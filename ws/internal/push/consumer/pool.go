// Package consumer provides Kafka message consumption for the push notification service.
// It connects to Kafka/Redpanda, consumes messages from tenant topics, and dispatches
// them to a message handler for push notification matching and delivery.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// MessageHandler is called for each consumed Kafka record.
// Parameters: tenantID extracted from topic, channelKey from record key, raw message data.
type MessageHandler func(tenantID, channelKey string, data []byte)

// PoolConfig configures the push consumer pool.
type PoolConfig struct {
	// Brokers is the list of Kafka/Redpanda broker addresses (required).
	Brokers []string

	// Namespace is the topic namespace prefix (e.g., "prod", "dev").
	// Used to extract tenantID from topic names: {namespace}.{tenantID}.{suffix}.
	Namespace string

	// ConsumerGroup is the Kafka consumer group ID.
	// Default: "push-service" (set by caller).
	ConsumerGroup string

	// Logger for structured logging.
	Logger zerolog.Logger

	// SASL authentication (nil = no auth, for local dev).
	SASL *kafkashared.SASLConfig

	// TLS encryption (nil = no TLS, for local dev).
	TLS *kafkashared.TLSConfig
}

// Pool manages a single Kafka consumer group for the push notification service.
// It consumes messages from all push-enabled tenant topics and dispatches them
// to a MessageHandler. Topics can be dynamically updated without consumer restart
// via UpdateTopics.
//
// Thread Safety: All public methods are safe for concurrent use.
type Pool struct {
	config  PoolConfig
	handler MessageHandler
	logger  zerolog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu     sync.Mutex
	client *kgo.Client
	topics map[string]bool // Currently consumed topics

	// Metrics — lock-free atomic counters
	messagesConsumed atomic.Uint64
	messagesRouted   atomic.Uint64
	parseErrors      atomic.Uint64
}

// NewPool creates a new push consumer pool. Call Start to begin consuming.
func NewPool(cfg PoolConfig) (*Pool, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if cfg.ConsumerGroup == "" {
		return nil, errors.New("consumer group is required")
	}

	return &Pool{
		config: cfg,
		logger: cfg.Logger,
		topics: make(map[string]bool),
	}, nil
}

// Start begins consuming messages from the configured topics. The handler is called
// for each record with the extracted tenantID, channelKey, and raw data.
// Start launches a consumer goroutine and returns immediately.
func (p *Pool) Start(ctx context.Context, handler MessageHandler, initialTopics []string) error {
	if handler == nil {
		return errors.New("message handler is required")
	}
	if len(initialTopics) == 0 {
		p.logger.Warn().Msg("Push consumer pool started with no topics — waiting for UpdateTopics")
	}

	p.handler = handler
	p.ctx, p.cancel = context.WithCancel(ctx)

	// Track initial topics
	for _, t := range initialTopics {
		p.topics[t] = true
	}

	client, err := p.createClient(initialTopics)
	if err != nil {
		p.cancel()
		return fmt.Errorf("create kafka client: %w", err)
	}

	p.mu.Lock()
	p.client = client
	p.mu.Unlock()

	p.wg.Go(p.consumeLoop)

	p.logger.Info().
		Int("topic_count", len(initialTopics)).
		Str("consumer_group", p.config.ConsumerGroup).
		Msg("Push consumer pool started")

	return nil
}

// UpdateTopics dynamically adds or removes topics from the consumer.
// Topics are compared against the current set; only changes trigger a client update.
func (p *Pool) UpdateTopics(topics []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		// Not started yet — just update the topic set
		p.topics = make(map[string]bool, len(topics))
		for _, t := range topics {
			p.topics[t] = true
		}
		return
	}

	// Build new topic set
	newTopics := make(map[string]bool, len(topics))
	for _, t := range topics {
		newTopics[t] = true
	}

	// Find additions and removals
	var toAdd, toRemove []string
	for t := range newTopics {
		if !p.topics[t] {
			toAdd = append(toAdd, t)
		}
	}
	for t := range p.topics {
		if !newTopics[t] {
			toRemove = append(toRemove, t)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return
	}

	// Apply changes via franz-go's AddConsumeTopics / RemoveConsumeTopics
	if len(toAdd) > 0 {
		p.client.AddConsumeTopics(toAdd...)
	}
	if len(toRemove) > 0 {
		p.client.PurgeTopicsFromConsuming(toRemove...)
	}

	p.topics = newTopics

	p.logger.Info().
		Int("added", len(toAdd)).
		Int("removed", len(toRemove)).
		Int("total", len(newTopics)).
		Msg("Push consumer topics updated")
}

// Stop gracefully shuts down the consumer pool.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()

	p.mu.Lock()
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
	p.mu.Unlock()

	p.logger.Info().
		Uint64("messages_consumed", p.messagesConsumed.Load()).
		Uint64("messages_routed", p.messagesRouted.Load()).
		Uint64("parse_errors", p.parseErrors.Load()).
		Msg("Push consumer pool stopped")
}

// consumeLoop is the main consumer goroutine. It polls Kafka for records
// and dispatches them to the message handler.
func (p *Pool) consumeLoop() {
	defer logging.RecoverPanic(p.logger, "push_consumer_loop", nil)

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		p.mu.Lock()
		client := p.client
		p.mu.Unlock()

		if client == nil {
			return
		}

		fetches := client.PollFetches(p.ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return
				}
				p.logger.Error().
					Err(e.Err).
					Str("topic", e.Topic).
					Int32("partition", e.Partition).
					Msg("Kafka fetch error")
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			p.messagesConsumed.Add(1)

			tenantID := p.extractTenantFromTopic(record.Topic)
			if tenantID == "" {
				p.parseErrors.Add(1)
				p.logger.Warn().
					Str("topic", record.Topic).
					Msg("Failed to extract tenant ID from topic")
				return
			}

			channelKey := string(record.Key)
			if channelKey == "" {
				p.parseErrors.Add(1)
				p.logger.Warn().
					Str("topic", record.Topic).
					Msg("Record has empty key (channel key)")
				return
			}

			p.messagesRouted.Add(1)
			p.handler(tenantID, channelKey, record.Value)
		})
	}
}

// extractTenantFromTopic extracts the tenant ID from a topic name.
// Topic format: {namespace}.{tenantID}.{suffix}
// Returns empty string if the topic doesn't match the expected format.
func (p *Pool) extractTenantFromTopic(topic string) string {
	// Expected: {namespace}.{tenantID}.{suffix}
	// The namespace is the configured prefix, so we strip it and take the next segment.
	prefix := p.config.Namespace + "."
	if !strings.HasPrefix(topic, prefix) {
		return ""
	}

	remainder := topic[len(prefix):]
	// remainder = "{tenantID}.{suffix}" — take the first segment
	tenantID, _, ok := strings.Cut(remainder, ".")
	if !ok || tenantID == "" {
		return ""
	}
	return tenantID
}

// createClient builds a franz-go client with the pool's configuration.
func (p *Pool) createClient(topics []string) (*kgo.Client, error) {
	opts, err := kafkashared.BuildKgoOpts(p.config.Brokers, p.config.SASL, p.config.TLS)
	if err != nil {
		return nil, fmt.Errorf("build kafka options: %w", err)
	}

	opts = append(opts,
		kgo.ConsumerGroup(p.config.ConsumerGroup),
		// §XVIII deviation from the server consumers' AfterMilli reset (ADR-0010):
		// the push pool consumes only push-ENABLED tenants' topics (GetTopics
		// filters to pushEnabledTenants), so a tenant enabling push late has its
		// topics hot-added long after boot with a post-boot backlog already in the
		// log. AfterMilli(boot) would replay that entire backlog as a notification
		// storm; AtEnd starts at the log end, delivering only new records. The
		// server pool consumes every provisioned topic continuously so it never
		// falls behind — that premise does not hold here, so AtEnd is retained.
		// The narrow fresh-topic race it leaves (a tenant push-enabled AT topic
		// creation, first record inside the ListOffsets window) is tracked as a
		// follow-up (needs an app-level freshness guard, not a reset-policy swap).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			p.logger.Info().
				Interface("partitions", assigned).
				Msg("Push consumer partitions assigned")
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			p.logger.Info().
				Interface("partitions", revoked).
				Msg("Push consumer partitions revoked")
		}),
	)

	if len(topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(topics...))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return client, nil
}
