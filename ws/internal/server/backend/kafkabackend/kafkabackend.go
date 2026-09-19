// Package kafkabackend provides the Kafka/Redpanda implementation of the
// MessageBackend interface. It wraps the existing multi-tenant consumer pool
// and producer for full persistence, offset-based replay, and tenant isolation.
package kafkabackend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/kafka"
	"github.com/sukko-dev/sukko/internal/server/limits"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/orchestration"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// backendName identifies this backend in metrics labels and logging.
const backendName = "kafka"

// KafkaBackend wraps the existing Kafka/Redpanda consumer pool and producer
// behind the MessageBackend interface. It provides full persistence,
// offset-based replay, and multi-tenant consumer isolation.
type KafkaBackend struct {
	pool          *orchestration.MultiTenantConsumerPool
	producer      *kafka.Producer
	adminClient   *kadm.Client
	kgoClient     *kgo.Client // underlying kgo client for admin (closed separately)
	topicRegistry *provapi.StreamTopicRegistry
	rulesProvider kafka.RoutingRulesSource // rule-based channel→ingress-topic resolution (ChannelTopic, ADR-0018)
	logger        zerolog.Logger
	healthy       atomic.Bool
	wg            sync.WaitGroup // tracks in-flight topic update goroutines

	defaultPartitions        int
	defaultReplicationFactor int
	namespace                string        // topic namespace for registry queries
	topicCreationTimeout     time.Duration // timeout for admin topic creation
}

// Config contains all configuration for the Kafka backend.
// Fields are populated by the wiring code in main.go from the top-level server config.
type Config struct {
	// Kafka/Redpanda broker addresses
	Brokers []string

	// Topic namespace (e.g., "prod", "dev")
	Namespace string

	// Deployment environment (e.g., "dev", "stg", "prod")
	Environment string

	// SASL authentication
	SASLEnabled   bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	// TLS encryption
	TLSEnabled  bool
	TLSInsecure bool
	TLSCAPath   string

	// Consumer toggle (false = connection-only mode for loadtesting)
	KafkaConsumerEnabled bool

	// Topic refresh interval for periodic discovery
	TopicRefreshInterval time.Duration

	// TopicCreationTimeout is the timeout for admin topic creation operations.
	TopicCreationTimeout time.Duration

	// Kafka admin topic defaults
	DefaultPartitions        int
	DefaultReplicationFactor int
	DefaultRetentionMs       int64

	// ResourceGuard fields
	MaxKafkaMessagesPerSec   int
	MaxBroadcastsPerSec      int
	RateLimitBurstMultiplier int
	CPUPauseThreshold        float64
	CPUPauseThresholdLower   float64
	CPURejectThreshold       float64
	CPURejectThresholdLower  float64

	// Broadcast bus for message distribution
	BroadcastBus broadcast.Bus

	// Logger is the structured logger passed by the caller.
	Logger zerolog.Logger

	// Kafka producer tuning
	ProducerBatchMaxBytes             int
	ProducerMaxBufferedRecs           int
	ProducerRecordRetries             int
	ProducerCircuitBreakerTimeout     time.Duration
	ProducerCircuitBreakerMaxFailures int
	ProducerCircuitBreakerHalfOpen    int
	ProducerShutdownTimeout           time.Duration

	// Kafka consumer batch tuning
	KafkaBatchSize    int
	KafkaBatchTimeout time.Duration

	// Kafka producer tuning
	KafkaMetadataMinAge time.Duration // 0 = franz-go default; lower shortens new-tenant produce latency (producer-scoped).

	// Kafka consumer transport tuning
	KafkaFetchMaxWait              time.Duration
	KafkaFetchMinBytes             int32
	KafkaFetchMaxBytes             int32
	KafkaSessionTimeout            time.Duration
	KafkaRebalanceTimeout          time.Duration
	KafkaReplayFetchMaxBytes       int32
	KafkaBackpressureCheckInterval time.Duration

	// Kafka partition-revoke commit tuning
	KafkaCommitOnRevokeTimeout time.Duration // max time for CommitMarkedOffsets in revoke callback
	KafkaAutoCommitInterval    time.Duration // background auto-commit interval (0 = franz-go default)

	// Provisioning gRPC connection
	ProvisioningGRPCAddr  string
	GRPCReconnectDelay    time.Duration
	GRPCReconnectMaxDelay time.Duration

	// RulesProvider resolves per-tenant routing rules on the produce path. REQUIRED —
	// New returns an error if nil (a kafka-mode server without a rules source would reject
	// every publish; #179).
	RulesProvider kafka.RoutingRulesSource

	// Fan-out / DLQ pool sizing (#179 P1b), threaded to the producer so multi-topic routing
	// rules fan out. All REQUIRED > 0 — New rejects non-positive values (the config layer
	// validates the env-backed WS_ROUTING_* fields, but New is a defense-in-depth boundary).
	RoutingFanoutWorkers   int
	RoutingFanoutQueueSize int
	DLQMaxRetries          int
	DLQBaseDelay           time.Duration
	DLQMaxDelay            time.Duration
	DLQRetryWorkers        int
}

// New creates a new Kafka backend with the provided configuration.
// It creates the ResourceGuard, StreamTopicRegistry, MultiTenantConsumerPool,
// and Producer. Returns an error on any initialization failure (fail fast).
func New(cfg Config) (*KafkaBackend, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka backend: at least one broker is required")
	}
	if cfg.KafkaConsumerEnabled && cfg.BroadcastBus == nil {
		return nil, errors.New("kafka backend: broadcast bus is required when consumer is enabled")
	}
	// A rules source is required on the produce path (#179): a kafka-mode server without
	// one would reject every publish. Validate here — Config is a hand-assembled struct
	// copy (main.go), an unvalidated source (§I/§II).
	if cfg.RulesProvider == nil {
		return nil, errors.New("kafka backend: rules provider is required")
	}
	// Fan-out / DLQ pool sizing must be positive (#179 P1b) — a zero would leave the producer
	// with no fan-out pool (rejecting every multi-topic rule) or an undrained DLQ queue.
	// Defense in depth (§II): server_config validates the env-backed values, this is the boundary.
	if cfg.RoutingFanoutWorkers <= 0 || cfg.RoutingFanoutQueueSize <= 0 ||
		cfg.DLQMaxRetries <= 0 || cfg.DLQBaseDelay <= 0 || cfg.DLQMaxDelay <= 0 || cfg.DLQRetryWorkers <= 0 {
		return nil, errors.New("kafka backend: routing fan-out/DLQ sizing (WS_ROUTING_*) must all be > 0")
	}

	logger := cfg.Logger.With().Str("component", "kafka-backend").Logger()

	// Topic namespace is resolved + normalized upstream (server config) and passed in verbatim.
	// The constructor never re-derives it from Environment (§I/§XV).
	topicNamespace := cfg.Namespace

	// Build SASL config if enabled
	var saslConfig *kafkashared.SASLConfig
	if cfg.SASLEnabled {
		saslConfig = &kafkashared.SASLConfig{
			Mechanism: cfg.SASLMechanism,
			Username:  cfg.SASLUsername,
			Password:  cfg.SASLPassword,
		}
	}

	// Build TLS config if enabled
	var tlsConfig *kafkashared.TLSConfig
	if cfg.TLSEnabled {
		tlsConfig = &kafkashared.TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: cfg.TLSInsecure,
			CAPath:             cfg.TLSCAPath,
		}
	}

	defaultPartitions := max(cfg.DefaultPartitions, 1)
	defaultReplicationFactor := max(cfg.DefaultReplicationFactor, 1)

	kb := &KafkaBackend{
		rulesProvider:            cfg.RulesProvider,
		logger:                   logger,
		defaultPartitions:        defaultPartitions,
		defaultReplicationFactor: defaultReplicationFactor,
		namespace:                topicNamespace,
		topicCreationTimeout:     cfg.TopicCreationTimeout,
	}

	// Create Kafka admin client for on-demand topic creation (uses same brokers/auth)
	adminKgoOpts, err := kafkashared.BuildKgoOpts(cfg.Brokers, saslConfig, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("kafka backend: build admin options: %w", err)
	}
	adminKgoClient, err := kgo.NewClient(adminKgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka backend: create admin client: %w", err)
	}
	kb.kgoClient = adminKgoClient
	kb.adminClient = kadm.NewClient(adminKgoClient)

	// Create Kafka producer for client message publishing
	producerLogger := cfg.Logger

	kb.producer, err = kafka.NewProducer(kafka.ProducerConfig{
		Brokers:                    cfg.Brokers,
		TopicNamespace:             topicNamespace,
		MetadataMinAge:             cfg.KafkaMetadataMinAge,
		Logger:                     &producerLogger,
		SASL:                       saslConfig,
		TLS:                        tlsConfig,
		BatchMaxBytes:              int32(min(cfg.ProducerBatchMaxBytes, math.MaxInt32)), //nolint:gosec // Bounds validated in ServerConfig.Validate()
		MaxBufferedRecs:            cfg.ProducerMaxBufferedRecs,
		RecordRetries:              cfg.ProducerRecordRetries,
		CircuitBreakerTimeout:      cfg.ProducerCircuitBreakerTimeout,
		CircuitBreakerMaxFailures:  uint32(min(cfg.ProducerCircuitBreakerMaxFailures, math.MaxUint32)), //nolint:gosec // Bounds validated in ServerConfig.Validate()
		CircuitBreakerHalfOpenReqs: uint32(min(cfg.ProducerCircuitBreakerHalfOpen, math.MaxUint32)),    //nolint:gosec // Bounds validated in ServerConfig.Validate()
		RulesProvider:              cfg.RulesProvider,
		FanoutWorkers:              cfg.RoutingFanoutWorkers,
		FanoutQueueSize:            cfg.RoutingFanoutQueueSize,
		DLQMaxRetries:              cfg.DLQMaxRetries,
		DLQBaseDelay:               cfg.DLQBaseDelay,
		DLQMaxDelay:                cfg.DLQMaxDelay,
		DLQRetryWorkers:            cfg.DLQRetryWorkers,
		ShutdownTimeout:            cfg.ProducerShutdownTimeout,
	})
	if err != nil {
		kb.kgoClient.Close()
		return nil, fmt.Errorf("kafka backend: create producer: %w", err)
	}

	logger.Info().
		Str("namespace", topicNamespace).
		Msg("Kafka producer initialized")

	if cfg.KafkaConsumerEnabled {
		// Create resource guard for CPU brake (shared across pool)
		poolLogger := cfg.Logger
		resourceGuard := limits.NewResourceGuard(limits.ResourceGuardConfig{
			MaxKafkaMessagesPerSec:   cfg.MaxKafkaMessagesPerSec,
			MaxBroadcastsPerSec:      cfg.MaxBroadcastsPerSec,
			RateLimitBurstMultiplier: cfg.RateLimitBurstMultiplier,
			CPUPauseThreshold:        cfg.CPUPauseThreshold,
			CPUPauseThresholdLower:   cfg.CPUPauseThresholdLower,
			CPURejectThreshold:       cfg.CPURejectThreshold,
			CPURejectThresholdLower:  cfg.CPURejectThresholdLower,
		}, poolLogger, &atomic.Int64{})

		// Create gRPC stream-backed topic registry for tenant topic discovery
		kb.topicRegistry, err = provapi.NewStreamTopicRegistry(provapi.StreamTopicRegistryConfig{
			GRPCAddr:          cfg.ProvisioningGRPCAddr,
			Namespace:         topicNamespace,
			ReconnectDelay:    cfg.GRPCReconnectDelay,
			ReconnectMaxDelay: cfg.GRPCReconnectMaxDelay,
			MetricPrefix:      "ws",
			Logger:            poolLogger,
		})
		if err != nil {
			_ = kb.producer.Close() // Best-effort cleanup; already returning constructor error
			kb.kgoClient.Close()
			return nil, fmt.Errorf("kafka backend: create topic registry: %w", err)
		}

		// Create multi-tenant consumer pool
		kb.pool, err = orchestration.NewMultiTenantConsumerPool(orchestration.MultiTenantPoolConfig{
			Brokers:           cfg.Brokers,
			Namespace:         topicNamespace,
			Environment:       strings.ToLower(strings.TrimSpace(cfg.Environment)),
			Registry:          kb.topicRegistry,
			BroadcastBus:      cfg.BroadcastBus,
			ResourceGuard:     resourceGuard,
			Logger:            poolLogger,
			SASL:              saslConfig,
			TLS:               tlsConfig,
			RefreshInterval:   cfg.TopicRefreshInterval,
			KafkaBatchSize:    cfg.KafkaBatchSize,
			KafkaBatchTimeout: cfg.KafkaBatchTimeout,
			// Consumer transport tuning
			KafkaFetchMaxWait:              cfg.KafkaFetchMaxWait,
			KafkaFetchMinBytes:             cfg.KafkaFetchMinBytes,
			KafkaFetchMaxBytes:             cfg.KafkaFetchMaxBytes,
			KafkaSessionTimeout:            cfg.KafkaSessionTimeout,
			KafkaRebalanceTimeout:          cfg.KafkaRebalanceTimeout,
			KafkaReplayFetchMaxBytes:       cfg.KafkaReplayFetchMaxBytes,
			KafkaBackpressureCheckInterval: cfg.KafkaBackpressureCheckInterval,
			// Partition-revoke commit tuning
			KafkaCommitOnRevokeTimeout: cfg.KafkaCommitOnRevokeTimeout,
			KafkaAutoCommitInterval:    cfg.KafkaAutoCommitInterval,
			Metrics:                    &metrics.MultiTenantPoolMetricsAdapter{},
		})
		if err != nil {
			_ = kb.topicRegistry.Close() // Best-effort cleanup; already returning constructor error
			_ = kb.producer.Close()      // Best-effort cleanup; already returning constructor error
			kb.kgoClient.Close()
			return nil, fmt.Errorf("kafka backend: create consumer pool: %w", err)
		}

		// Wire gRPC stream topic registry to trigger on-demand pool refresh
		// and on-demand topic creation via kadm
		kb.topicRegistry.SetOnUpdate(func() {
			// Run asynchronously to avoid blocking the gRPC stream receive goroutine.
			// ensureTopicsExist has its own 30s timeout; RefreshTopics is non-blocking.
			kb.wg.Go(func() {
				defer logging.RecoverPanic(kb.logger, "topic_update", nil)
				kb.ensureTopicsExist()
				kb.pool.RefreshTopics()
			})
		})

		logger.Info().
			Bool("consumer_enabled", true).
			Msg("Kafka consumer pool created (gRPC topic streaming)")
	} else {
		logger.Info().
			Bool("consumer_enabled", false).
			Msg("Kafka consumer DISABLED — connection-only mode for loadtesting")
	}

	return kb, nil
}

// Start begins the Kafka backend's consumption loop.
// For the consumer pool, this starts topic discovery and message consumption.
func (kb *KafkaBackend) Start(_ context.Context) error {
	if kb.pool != nil {
		if err := kb.pool.Start(); err != nil {
			return fmt.Errorf("kafka backend start: %w", err)
		}
		metrics.SetKafkaConnected(true)
		kb.logger.Info().Msg("Kafka consumer pool started")
	}

	kb.healthy.Store(true)
	metrics.SetBackendHealthy(backendName, true)
	return nil
}

// Publish sends a client-published message through the Kafka producer.
// tenantID is accepted for interface compliance but not used by the Kafka backend —
// routing is handled by the topic partitioning scheme.
func (kb *KafkaBackend) Publish(ctx context.Context, clientID int64, _, channel string, data []byte) (string, error) {
	if channel == "" {
		return "", fmt.Errorf("%w: channel is required", backend.ErrPublishFailed)
	}
	if kb.producer == nil {
		return "", fmt.Errorf("%w: kafka producer not initialized", backend.ErrPublishFailed)
	}
	start := time.Now()
	mid, err := kb.producer.Publish(ctx, clientID, channel, data)
	metrics.RecordBackendPublishLatency(backendName, time.Since(start).Seconds())
	if err != nil {
		// Reject-class errors (no applicable routing rule) are client-caused, not
		// infrastructure failures — keep them out of the error alert series (§VI).
		if errors.Is(err, backend.ErrPublishNotRoutable) {
			metrics.RecordBackendPublishRejected(backendName)
		} else {
			metrics.RecordBackendPublishError(backendName)
		}
		return "", fmt.Errorf("kafka backend publish: %w", err)
	}
	metrics.RecordBackendPublish(backendName)
	return mid, nil
}

// Replay returns messages from the specified offsets for client reconnection.
// Delegates to the shared consumer's ReplayFromOffsets method.
func (kb *KafkaBackend) Replay(ctx context.Context, req backend.ReplayRequest) ([]backend.ReplayMessage, error) {
	metrics.RecordBackendReplayRequest(backendName)

	if kb.pool == nil {
		return nil, nil
	}

	sharedConsumer := kb.pool.GetSharedConsumer()
	if sharedConsumer == nil {
		return nil, nil
	}

	kafkaMessages, err := sharedConsumer.ReplayFromOffsets(ctx, req.Positions, req.MaxMessages, req.Subscriptions)
	if err != nil {
		return nil, fmt.Errorf("kafka replay: %w", err)
	}

	messages := convertReplayMessages(kafkaMessages)

	metrics.RecordBackendReplayMessages(backendName, len(messages))
	return messages, nil
}

// convertReplayMessages converts kafka.ReplayMessage → backend.ReplayMessage.
// The mid is recomputed from each record's coordinates — the same derivation
// the consumer applies on live delivery — so the replayed copy carries the
// identical identity (ADR-0008 cross-copy equality).
func convertReplayMessages(kafkaMessages []kafka.ReplayMessage) []backend.ReplayMessage {
	messages := make([]backend.ReplayMessage, len(kafkaMessages))
	for i, m := range kafkaMessages {
		messages[i] = backend.ReplayMessage{
			Subject: m.Subject,
			Data:    m.Data,
			Pos:     m.Pos,
			Mid:     kafkashared.MessageID(m.Topic, m.Partition, m.Offset),
		}
	}
	return messages
}

// IsHealthy returns true if the Kafka backend is operational.
func (kb *KafkaBackend) IsHealthy() bool {
	return kb.healthy.Load()
}

// Ready reports readiness: the backend is ready once the topic-registry routing snapshot has been
// applied, so the consumer pool never broadcasts before it can resolve topic->tenant (#179 P3). A nil
// registry means the consumer is disabled (produce-only), which is ready immediately.
func (kb *KafkaBackend) Ready() bool {
	return kb.topicRegistry == nil || kb.topicRegistry.SnapshotReceived()
}

// Shutdown gracefully stops the Kafka backend.
// Stops pool, closes producer, closes topic registry (continues on individual failures per Constitution IV).
func (kb *KafkaBackend) Shutdown(ctx context.Context) error {
	kb.healthy.Store(false)
	metrics.SetBackendHealthy(backendName, false)

	// Close topic registry FIRST to stop gRPC stream and prevent new SetOnUpdate callbacks.
	// This must happen before wg.Wait to prevent wg.Add after wg.Wait returns.
	if kb.topicRegistry != nil {
		if err := kb.topicRegistry.Close(); err != nil {
			kb.logger.Error().Err(err).Msg("Error closing topic registry")
		}
	}

	// Wait for in-flight topic update goroutines to finish
	done := make(chan struct{})
	go func() {
		defer logging.RecoverPanic(kb.logger, "shutdown_wait", nil)
		kb.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Topic update goroutines exited cleanly
	case <-ctx.Done():
		kb.logger.Warn().Msg("Shutdown timed out waiting for topic update goroutines")
	}

	var errs []error

	// Stop consumer pool
	if kb.pool != nil {
		if err := kb.pool.Stop(); err != nil {
			kb.logger.Error().Err(err).Msg("Error stopping consumer pool")
			errs = append(errs, fmt.Errorf("stop consumer pool: %w", err))
		}
	}

	// Close producer
	if kb.producer != nil {
		if err := kb.producer.Close(); err != nil {
			kb.logger.Error().Err(err).Msg("Error closing Kafka producer")
			errs = append(errs, fmt.Errorf("close producer: %w", err))
		}
	}

	// Close admin kgo client
	if kb.kgoClient != nil {
		kb.kgoClient.Close()
	}

	metrics.SetKafkaConnected(false)
	kb.logger.Info().Msg("Kafka backend shut down")

	return errors.Join(errs...)
}

// ensureTopicsExist reads from the topic registry and creates missing Kafka topics
// via the kadm admin client. TopicAlreadyExists is treated as a no-op.
func (kb *KafkaBackend) ensureTopicsExist() {
	if kb.adminClient == nil || kb.topicRegistry == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), kb.topicCreationTimeout)
	defer cancel()

	// Collect all topics from registry
	sharedTopics, err := kb.topicRegistry.GetSharedTenantTopics(ctx, kb.namespace)
	if err != nil {
		kb.logger.Error().Err(err).Msg("Failed to get shared topics for topic creation")
		return
	}

	dedicatedTenants, err := kb.topicRegistry.GetDedicatedTenants(ctx, kb.namespace)
	if err != nil {
		kb.logger.Error().Err(err).Msg("Failed to get dedicated tenants for topic creation")
		return
	}

	var allTopics []string
	allTopics = append(allTopics, sharedTopics...)
	for _, dt := range dedicatedTenants {
		allTopics = append(allTopics, dt.Topics...)
	}
	// Egress topics: created on the broker, never consumed (ADR-0018). The DLQ
	// precedent — written to, never subscribed. They must never join a consumer's
	// topic list; they are appended here for CREATION only.
	allTopics = append(allTopics, kb.topicRegistry.CreateOnlyTopics()...)

	if len(allTopics) == 0 {
		return
	}

	// Create topics (kadm handles TopicAlreadyExists gracefully)
	partitions := int32(kb.defaultPartitions)               //nolint:gosec // G115: partition count is validated at config load time and always small
	replicationFactor := int16(kb.defaultReplicationFactor) //nolint:gosec // G115: replication factor is validated at config load time and always small

	resp, err := kb.adminClient.CreateTopics(ctx, partitions, replicationFactor, nil, allTopics...)
	if err != nil {
		kb.logger.Error().Err(err).Msg("Failed to create topics")
		return
	}

	for _, topic := range resp.Sorted() {
		if topic.Err != nil {
			// kadm returns *kadm.TopicError — check for "already exists"
			if isTopicAlreadyExistsError(topic.Err) {
				continue
			}
			kb.logger.Error().
				Err(topic.Err).
				Str("topic", topic.Topic).
				Msg("Failed to create topic")
		} else {
			kb.logger.Info().
				Str("topic", topic.Topic).
				Msg("Created Kafka topic")
		}
	}
}

// isTopicAlreadyExistsError checks if the error indicates the topic already exists.
func isTopicAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, kerr.TopicAlreadyExists)
}

// SplitBrokers splits a comma-separated broker string into individual addresses.
func SplitBrokers(brokers string) []string {
	var result []string
	for b := range strings.SplitSeq(brokers, ",") {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// ChannelTopic resolves a channel to its INGRESS topic from the tenant's routing
// rules — deterministically, on every pod, with no dependence on observed traffic
// (ADR-0018; the previous last-writer-wins traffic cache could not resolve any
// channel on a pod that had routed nothing, failing replay/history there).
//
// Resolution: the first matching rule's ingress topic; a tenant with no rules or
// no matching rule resolves to its default topic — where rule-less (Community
// ingest) and externally-produced records live. Returns ok=false only when the
// mapping is UNKNOWN: consumer disabled, no rules source, or the rules snapshot
// not yet synced (degraded — callers report not-available rather than guessing).
func (kb *KafkaBackend) ChannelTopic(channel string) (string, bool) {
	if kb.pool == nil || kb.rulesProvider == nil || !kb.rulesProvider.SnapshotReceived() {
		return "", false
	}
	tenant, err := kafka.ExtractTenant(channel)
	if err != nil {
		return "", false
	}
	if snap, ok := kb.rulesProvider.GetRoutingSnapshot(tenant); ok {
		for _, rule := range snap.Rules {
			matched, matchErr := routing.MatchRoutingPattern(rule.Pattern, channel)
			if matchErr != nil || !matched {
				continue
			}
			if rule.IngressTopic == "" {
				continue // defense in depth (§II): validated at provisioning time
			}
			return kafkashared.BuildTopicName(kb.namespace, tenant, rule.IngressTopic), true
		}
	}
	return kafkashared.BuildTopicName(kb.namespace, tenant, routing.DefaultTopicSuffix), true
}

// Compile-time interface check.
var _ backend.MessageBackend = (*KafkaBackend)(nil)
