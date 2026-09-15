package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/push/consumer"
	"github.com/sukko-dev/sukko/internal/push/provider"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/push/worker"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// ConfigCache provides push configuration data from the provisioning system.
// Implementations may cache data locally with periodic refresh from the
// provisioning gRPC service.
type ConfigCache interface {
	// GetPushPatterns returns the channel patterns for push notifications for a tenant.
	// Patterns use wildcard syntax (e.g., "acme.alerts.*", "acme.trade.*").
	GetPushPatterns(tenantID string) []string

	// GetTopics returns all Kafka topics for push-enabled tenants.
	GetTopics() []string

	// GetCredential returns provider credentials for a tenant and provider type.
	// The returned JSON structure depends on the provider (see provider.CredentialLookup).
	GetCredential(tenantID, provider string) (json.RawMessage, error)
}

// Prometheus metrics for the push service message pipeline.
var (
	messagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "push_messages_processed_total",
		Help: "Total number of Kafka messages processed by the push service.",
	})

	messagesMatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "push_messages_matched_total",
		Help: "Total number of messages that matched at least one push channel pattern.",
	})

	devicesTargeted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "push_devices_targeted_total",
		Help: "Total number of push notification jobs enqueued for device delivery.",
	})

	dryRunMode = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "push_dry_run_mode",
		Help: "1 if push service is running in dry-run mode (PUSH_DRY_RUN=true), 0 otherwise.",
	})
)

// SetDryRunMode sets the push_dry_run_mode gauge to 1 when dry-run is active, 0 otherwise.
// Called at startup from cmd/push/main.go after config is loaded.
func SetDryRunMode(enabled bool) {
	if enabled {
		dryRunMode.Set(1)
	} else {
		dryRunMode.Set(0)
	}
}

// ServiceConfig configures the push notification service.
type ServiceConfig struct {
	// ConsumerPool is the Kafka consumer pool configuration.
	ConsumerPool consumer.PoolConfig

	// WorkerPool is the bounded worker pool configuration.
	WorkerPool worker.PoolConfig

	// WorkerCount is the number of worker goroutines to launch.
	WorkerCount int

	// Repo is the subscription repository for looking up device subscriptions.
	Repo repository.SubscriptionRepository

	// Cache provides push patterns, topics, and credentials from provisioning.
	Cache ConfigCache

	// DefaultTTL is the default TTL for push notifications in seconds.
	DefaultTTL int

	// DefaultUrgency is the default Web Push urgency header value.
	DefaultUrgency string

	// RefreshInterval controls how often topics are refreshed from config cache.
	// Default: 30 seconds.
	RefreshInterval time.Duration

	// Logger for structured logging.
	Logger zerolog.Logger
}

// Service orchestrates the push notification pipeline:
// Kafka consumption -> channel matching -> subscription lookup -> job dispatch.
//
// Architecture:
//
//	ConfigCache (provisioning data)
//	    |
//	ConsumerPool (Kafka) --[message]--> handleMessage
//	    |                                   |
//	    |                              MatchChannel (pattern matching)
//	    |                                   |
//	    |                              Repo.FindByTenant (subscription lookup)
//	    |                                   |
//	    |                              WorkerPool.Enqueue (job dispatch)
//	    v                                   |
//	Provider.Send (Web Push / FCM / APNs)  <--
//
// Thread Safety: All public methods are safe for concurrent use.
type Service struct {
	consumerPool    *consumer.Pool
	workerPool      *worker.Pool
	repo            repository.SubscriptionRepository
	cache           ConfigCache
	workerCount     int
	defaultTTL      int
	defaultUrg      string
	refreshInterval time.Duration
	logger          zerolog.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// defaultRefreshInterval is the default topic refresh interval when not configured.
const defaultRefreshInterval = 30 * time.Second

// NewService creates a new push notification service. Call Start to begin processing.
// Fails fast if required dependencies are missing — prevents silent runtime panics.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.Repo == nil {
		return nil, errors.New("subscription repository is required")
	}
	if cfg.Cache == nil {
		return nil, errors.New("config cache is required")
	}
	if cfg.WorkerCount < 1 {
		return nil, fmt.Errorf("worker count must be >= 1, got %d", cfg.WorkerCount)
	}

	pool, err := consumer.NewPool(cfg.ConsumerPool)
	if err != nil {
		return nil, fmt.Errorf("create consumer pool: %w", err)
	}

	wp, err := worker.NewPool(cfg.WorkerPool)
	if err != nil {
		return nil, fmt.Errorf("create worker pool: %w", err)
	}

	refreshInterval := cfg.RefreshInterval
	if refreshInterval == 0 {
		refreshInterval = defaultRefreshInterval
	}

	return &Service{
		consumerPool:    pool,
		workerPool:      wp,
		repo:            cfg.Repo,
		cache:           cfg.Cache,
		workerCount:     cfg.WorkerCount,
		defaultTTL:      cfg.DefaultTTL,
		defaultUrg:      cfg.DefaultUrgency,
		refreshInterval: refreshInterval,
		logger:          cfg.Logger,
	}, nil
}

// Start begins the push notification pipeline: starts the consumer pool
// with the message handler and launches the worker pool.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Start worker pool first so it's ready to receive jobs.
	// s.ctx is derived from ctx via context.WithCancel above.
	s.workerPool.Start(s.ctx) //nolint:contextcheck // s.ctx derived from ctx on line 167
	s.workerPool.StartWorkers(s.workerCount)

	// Get initial topics from config cache
	topics := s.cache.GetTopics()

	// Start consumer pool with our message handler
	if err := s.consumerPool.Start(s.ctx, s.handleMessage, topics); err != nil { //nolint:contextcheck // s.ctx derived from ctx on line 167
		s.workerPool.Stop()
		return fmt.Errorf("start consumer pool: %w", err)
	}

	// Launch periodic topic refresh
	s.wg.Go(s.refreshTopicsLoop)

	s.logger.Info().
		Int("initial_topics", len(topics)).
		Int("workers", s.workerCount).
		Msg("Push service started")

	return nil
}

// Stop gracefully shuts down the push service.
// Shutdown order: cancel context -> stop consumer (no new messages) -> stop workers (drain queue).
func (s *Service) Stop() {
	if s.cancel != nil {
		s.cancel()
	}

	// Stop consumer first to prevent new messages
	s.consumerPool.Stop()

	// Wait for refresh loop to exit
	s.wg.Wait()

	// Stop workers — they drain remaining jobs before exiting
	s.workerPool.Stop()

	s.logger.Info().Msg("Push service stopped")
}

// handleMessage is the core message handler called by the consumer pool for each
// Kafka record. It matches the channel against push patterns, looks up subscriptions,
// and enqueues push jobs.
func (s *Service) handleMessage(tenantID, channelKey string, data []byte) {
	messagesProcessed.Inc()

	// Get push patterns for this tenant
	patterns := s.cache.GetPushPatterns(tenantID)
	if len(patterns) == 0 {
		// LOG-001: No push patterns configured for tenant
		if s.logger.Debug().Enabled() {
			s.logger.Debug().Str(logging.LogKeyTenantSlug, tenantID).Str("channel_key", channelKey).
				Msg("no push patterns configured for tenant")
		}
		return
	}

	// Check if this channel matches any push pattern
	if !consumer.MatchChannel(channelKey, patterns) {
		// LOG-002: Channel did not match push patterns
		if s.logger.Debug().Enabled() {
			s.logger.Debug().Str(logging.LogKeyTenantSlug, tenantID).Str("channel_key", channelKey).
				Int("pattern_count", len(patterns)).
				Msg("channel did not match push patterns")
		}
		return
	}
	messagesMatched.Inc()

	// Look up subscriptions for this tenant
	subs, err := s.repo.FindByTenant(s.ctx, tenantID)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str(logging.LogKeyTenantSlug, tenantID).
			Msg("Failed to find subscriptions for tenant")
		return
	}

	if len(subs) == 0 {
		// LOG-003: No push subscriptions for tenant
		if s.logger.Debug().Enabled() {
			s.logger.Debug().Str(logging.LogKeyTenantSlug, tenantID).Str("channel_key", channelKey).
				Int("subscription_count", 0).
				Msg("no push subscriptions for tenant")
		}
		return
	}

	// Filter subscriptions by channel match and build push jobs
	var jobsEnqueued int
	var platforms []string
	for _, sub := range subs {
		// Each subscription has its own channel patterns — filter in Go
		if !consumer.MatchChannel(channelKey, sub.Channels) {
			continue
		}

		job := provider.PushJob{
			TenantID:   tenantID,
			Principal:  sub.Principal,
			Platform:   sub.Platform,
			Token:      sub.Token,
			Endpoint:   sub.Endpoint,
			P256dhKey:  sub.P256dhKey,
			AuthSecret: sub.AuthSecret,
			Body:       string(data),
			TTL:        s.defaultTTL,
			Urgency:    s.defaultUrg,
			Channel:    channelKey,
		}

		devicesTargeted.Inc()
		s.workerPool.Enqueue(job)
		jobsEnqueued++
		if s.logger.Debug().Enabled() {
			platforms = append(platforms, sub.Platform)
		}
	}

	// LOG-004: Push jobs enqueued
	if s.logger.Debug().Enabled() && jobsEnqueued > 0 {
		s.logger.Debug().Str(logging.LogKeyTenantSlug, tenantID).Str("channel_key", channelKey).
			Int("jobs_enqueued", jobsEnqueued).Strs("platforms", platforms).
			Msg("push jobs enqueued")
	}
}

// refreshTopicsLoop periodically refreshes the consumer topic list from the config cache.
func (s *Service) refreshTopicsLoop() {
	defer logging.RecoverPanic(s.logger, "push_topic_refresh", nil)

	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()

	lastTopicCount := -1 // unknown until first refresh

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			topics := s.cache.GetTopics()
			newCount := len(topics)

			// LOG-006: Warn if topics dropped from >0 to 0
			if lastTopicCount > 0 && newCount == 0 {
				s.logger.Warn().Int("old_count", lastTopicCount).
					Msg("push topics dropped to zero — possible config loss")
			}

			// LOG-005: Debug topic refresh delta
			s.logger.Debug().Int("old_count", lastTopicCount).Int("new_count", newCount).
				Msg("push topics refreshed")

			s.consumerPool.UpdateTopics(topics)
			lastTopicCount = newCount
		}
	}
}
