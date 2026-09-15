// Package kafka provides Kafka/Redpanda consumer integration with resource-aware
// message processing. It supports batching, rate limiting, and CPU-based backpressure
// to prevent message loss during high load.
//
// Key features:
//   - Franz-go based consumer with consumer group support
//   - Message batching for reduced overhead (default: 50 messages, 10ms timeout)
//   - Integration with ResourceGuard for CPU-based throttling
//   - Offset replay for client reconnection scenarios
package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// consumerRoutingMetrics holds Prometheus metrics shared across all Consumer instances.
// Registered once at package level to avoid duplicate registration panics when the
// orchestration pool creates one Consumer per active tenant.
var consumerRoutingMetrics = struct {
	deadLetterCounter   *prometheus.CounterVec
	unknownTopicCounter prometheus.Counter
	once                sync.Once
}{}

// consumerDeletedMetrics holds the broker-deleted-topic counter registered once
// at package level. Tests bypass this singleton via ConsumerConfig.Registerer.
var consumerDeletedMetrics = struct {
	counterVec *prometheus.CounterVec
	once       sync.Once
}{}

// consumerRevokeCommitMetrics holds the partition-revoke commit counter and latency
// histogram registered once at package level. Tests bypass this singleton via
// ConsumerConfig.Registerer.
var consumerRevokeCommitMetrics = struct {
	counterVec        *prometheus.CounterVec
	durationHistogram prometheus.Histogram
	once              sync.Once
}{}

// topicPauser is the minimal interface for pausing Kafka topic fetching.
// *kgo.Client satisfies this interface in production; mockPauser is used in tests.
type topicPauser interface {
	PauseFetchTopics(topics ...string) []string
}

// committer is the minimal interface for committing Kafka offsets.
// *kgo.Client satisfies this interface in production; mockCommitter is used in tests.
type committer interface {
	CommitMarkedOffsets(ctx context.Context) error
	MarkCommitRecords(rs ...*kgo.Record)
}

// TokenEvent represents an event from Redpanda
type TokenEvent struct {
	Type      kafkashared.EventType `json:"type"`
	Timestamp int64                 `json:"timestamp"`
	Data      map[string]any        `json:"data"`
}

// BroadcastFunc is called when a message is received.
// Parameters: subject (broadcast routing key), message (raw JSON), topicName, partition, offset.
// The extra fields allow the history writer to populate TenantID/StreamID/Channel on broadcast.Message.
type BroadcastFunc func(subject string, message []byte, topicName string, partition int32, offset int64)

// ResourceGuard interface for rate limiting and CPU emergency brake
type ResourceGuard interface {
	AllowKafkaMessage(ctx context.Context) (allow bool, waitDuration time.Duration)
	ShouldPauseKafka() bool
}

// Consumer wraps franz-go client for consuming from Kafka/Redpanda.
// It provides three-layer protection against overload:
//  1. Rate limiting - caps consumption at configured rate (e.g., 25 msg/sec)
//  2. CPU emergency brake - pauses when CPU exceeds threshold (e.g., 80%)
//  3. Direct broadcast - non-blocking message delivery to clients
//
// Thread Safety: All public methods are safe for concurrent use.
//
// Lifecycle: Create with NewConsumer, start with Start, stop with Stop.
type Consumer struct {
	client        *kgo.Client     // Franz-go Kafka client
	logger        *zerolog.Logger // Structured logger
	broadcast     BroadcastFunc   // Callback for message delivery to clients
	resourceGuard ResourceGuard   // Rate limiting and CPU brake
	ctx           context.Context // Root context for consumer goroutines
	cancel        context.CancelFunc
	wg            sync.WaitGroup // Tracks consumer goroutines

	// Batching configuration - reduces overhead by processing multiple messages together
	batchSize    int           // Max messages per batch (default: 50)
	batchTimeout time.Duration // Max time to wait for batch (default: 10ms)
	batchEnabled bool          // Enable batching (default: true for performance)

	// Transport tuning (stored from config for replay consumer and backpressure)
	fetchMaxWait              time.Duration
	fetchMinBytes             int32
	fetchMaxBytes             int32
	replayFetchMaxBytes       int32
	backpressureCheckInterval time.Duration

	// Routing — resolves the channel from the record's HeaderChannel and the registry-supplied tenant.
	namespace      string
	tenantResolver func(topic string) (string, bool) // topic → tenant from the registry map (#179 P3)
	dlq            *DLQPool

	// Metrics - lock-free atomic counters (Constitution VII)
	messagesProcessed atomic.Uint64 // Successfully broadcast messages
	messagesFailed    atomic.Uint64 // Messages with invalid format
	messagesDropped   atomic.Uint64 // Rate limited or CPU paused
	batchesSent       atomic.Uint64 // Number of batches sent
	unknownTopicDrops atomic.Uint64 // Records dropped because the topic→tenant map had no entry (registry-miss)
	consumerGroup     string        // Consumer group ID for metrics labels

	deadLetterCounter   *prometheus.CounterVec
	unknownTopicCounter prometheus.Counter

	// Broker-deleted-topic handling
	pauser         topicPauser  // client in production; mockPauser in tests
	onUnknownTopic func(string) // callback dispatched when a topic is deleted at the broker
	deletedCounter prometheus.Counter

	// Partition-revoke commit handling (explicit-mark pattern)
	committer             committer              // client in production; mockCommitter in tests
	commitOnRevokeTimeout time.Duration          // max time for CommitMarkedOffsets in revoke callback
	revokeCommitCounter   *prometheus.CounterVec // ws_consumer_revoke_commit_total{result}
	revokeCommitDuration  prometheus.Observer    // ws_consumer_revoke_commit_duration_seconds (never nil after NewConsumer)

	// Security config retained for the replay client (ReplayFromOffsets creates a separate kgo.Client).
	sasl *kafkashared.SASLConfig
	tls  *kafkashared.TLSConfig
}

// ConsumerConfig holds configuration for creating a Kafka consumer.
// Required fields: Brokers, ConsumerGroup, Topics, Logger, Broadcast, ResourceGuard.
type ConsumerConfig struct {
	Brokers       []string        // Kafka/Redpanda broker addresses (required)
	ConsumerGroup string          // Consumer group ID for offset tracking (required)
	Topics        []string        // Topics to consume from (required)
	Logger        *zerolog.Logger // Structured logger for consumer events
	Broadcast     BroadcastFunc   // Callback for message delivery (required)
	ResourceGuard ResourceGuard   // Rate limiting and CPU brake (required)

	// Security configuration for managed Kafka/Redpanda services
	SASL *kafkashared.SASLConfig // SASL authentication (nil = no auth, for local dev)
	TLS  *kafkashared.TLSConfig  // TLS encryption (nil = no TLS, for local dev)

	// Optional batching configuration - improves throughput by reducing per-message overhead
	BatchSize    int           // Max messages per batch (default: 50, 0 = disabled)
	BatchTimeout time.Duration // Max wait for batch (default: 10ms)

	// Transport tuning (source of truth: envDefault tags in ServerConfig)
	FetchMaxWait     time.Duration // Max time broker waits before responding (default: 500ms)
	FetchMinBytes    int32         // Min bytes before broker responds (default: 1)
	FetchMaxBytes    int32         // Max bytes per fetch response (default: 10MB)
	SessionTimeout   time.Duration // Consumer group heartbeat timeout (default: 30s)
	RebalanceTimeout time.Duration // Max time for rebalance to complete (default: 60s)

	// Replay tuning
	ReplayFetchMaxBytes int32 // Max bytes for replay fetch (default: 5MB)

	// Backpressure tuning
	BackpressureCheckInterval time.Duration // CPU brake poll interval (default: 100ms)

	// Partition-revoke commit tuning
	CommitOnRevokeTimeout time.Duration // max time for CommitMarkedOffsets in revoke callback (must be > 0)
	AutoCommitInterval    time.Duration // background auto-commit interval (0 = use franz-go default)

	// ConsumeResetAfterMillis sets the reset offset for topics the group has never
	// committed: > 0 → AfterMilli(that) (end of pre-existing partitions, start of
	// partitions created after this timestamp — see ADR-0010); 0 → AtEnd. The pool
	// sets this to the process start time; direct Consumer construction (tests)
	// leaves it 0 for the historical AtEnd behavior.
	ConsumeResetAfterMillis int64

	// Routing — the channel comes from the record's HeaderChannel; the tenant from TenantResolver.
	Namespace string // Topic namespace (e.g. "prod") — used to build DLQ topic names.

	// TenantResolver resolves a record's topic to its owning tenant via the registry map (#179 P3).
	// Set by the pool; used by extractChannel to avoid reverse-parsing the topic name.
	TenantResolver func(topic string) (string, bool)
	DLQ            *DLQPool // Dead-letter queue pool (nil = drop on invalid channel).

	// OnUnknownTopic is called when the broker returns UnknownTopicOrPartition for a topic; nil is safe.
	OnUnknownTopic func(topic string)
	// ConsumerType labels the deleted-topic counter ("shared" or "dedicated").
	ConsumerType string
	// Registerer overrides the Prometheus registry; nil uses the promauto singleton (production default).
	Registerer prometheus.Registerer
}

// NewConsumer creates a new Kafka consumer with the provided configuration.
// It validates required fields and initializes the franz-go client with
// optimal settings for trading workloads (500ms fetch wait, 10MB max fetch).
//
// The consumer starts at the latest offset (no historical replay on creation).
// Use Start to begin consuming and Stop for graceful shutdown.
//
// Returns an error if any required field is missing or client creation fails.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if cfg.ConsumerGroup == "" {
		return nil, errors.New("consumer group is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("at least one topic is required")
	}
	if cfg.Broadcast == nil {
		return nil, errors.New("broadcast function is required")
	}
	if cfg.ResourceGuard == nil {
		return nil, errors.New("resource guard is required")
	}
	if cfg.Logger == nil {
		return nil, errors.New("logger is required")
	}
	if cfg.ConsumerType == "" {
		return nil, errors.New("consumer type is required")
	}
	if cfg.CommitOnRevokeTimeout <= 0 {
		return nil, errors.New("CommitOnRevokeTimeout must be > 0")
	}
	// When batching is enabled (BatchSize > 1) the consume loop bounds each poll to BatchTimeout;
	// a non-positive value would make the poll return instantly and busy-spin. Fail fast rather than
	// silently substituting a default — KAFKA_BATCH_TIMEOUT's envDefault is the single source of truth (§I).
	if cfg.BatchSize > 1 && cfg.BatchTimeout <= 0 {
		return nil, errors.New("BatchTimeout must be > 0 when batching is enabled (BatchSize > 1)")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Pre-declare consumer pointer so OnPartitionsRevoked closure can capture it.
	// The closure resolves consumer.committer at invocation time (after struct allocation).
	var consumer *Consumer

	// Build base client options (SeedBrokers, SASL, TLS)
	opts, err := kafkashared.BuildKgoOpts(cfg.Brokers, cfg.SASL, cfg.TLS)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to build kafka options: %w", err)
	}

	// Consumer-specific options
	opts = append(opts,
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(consumeResetOffset(cfg.ConsumeResetAfterMillis)),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			cfg.Logger.Info().
				Interface(LogFieldPartitions, assigned).
				Msg("Partitions assigned")
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			consumer.handleRevoke(ctx, revoked)
		}),
	)

	// Transport tuning (apply only if non-zero; omitting uses franz-go defaults)
	if cfg.FetchMaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(cfg.FetchMaxWait))
	}
	if cfg.FetchMinBytes > 0 {
		opts = append(opts, kgo.FetchMinBytes(cfg.FetchMinBytes))
	}
	if cfg.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(cfg.FetchMaxBytes))
	}
	if cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(cfg.SessionTimeout))
	}
	if cfg.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(cfg.RebalanceTimeout))
	}

	// Explicit-mark commit pattern: auto-commit fires on timer but only commits
	// records explicitly marked via MarkCommitRecords. This prevents committing
	// records that have been polled but not yet processed during rebalance.
	opts = append(opts, kgo.AutoCommitMarks())
	if cfg.AutoCommitInterval > 0 {
		opts = append(opts, kgo.AutoCommitInterval(cfg.AutoCommitInterval))
	}

	// Create franz-go client with all options
	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// Batching configuration (values provided by env-parsed config; BatchTimeout > 0 is validated
	// above when batching is enabled).
	batchSize := cfg.BatchSize
	batchTimeout := cfg.BatchTimeout
	batchEnabled := batchSize > 1

	// Backpressure check interval (validated > 0 by platform config layer)
	backpressureInterval := cfg.BackpressureCheckInterval

	consumer = &Consumer{
		client:        client,
		logger:        cfg.Logger,
		broadcast:     cfg.Broadcast,
		resourceGuard: cfg.ResourceGuard,
		ctx:           ctx,
		cancel:        cancel,
		batchSize:     batchSize,
		batchTimeout:  batchTimeout,
		batchEnabled:  batchEnabled,
		consumerGroup: cfg.ConsumerGroup,

		// Transport tuning (stored for replay consumer and backpressure)
		fetchMaxWait:              cfg.FetchMaxWait,
		fetchMinBytes:             cfg.FetchMinBytes,
		fetchMaxBytes:             cfg.FetchMaxBytes,
		replayFetchMaxBytes:       cfg.ReplayFetchMaxBytes,
		backpressureCheckInterval: backpressureInterval,

		// Routing
		namespace:      cfg.Namespace,
		tenantResolver: cfg.TenantResolver,
		dlq:            cfg.DLQ,

		// Partition-revoke commit handling
		committer:             client,
		commitOnRevokeTimeout: cfg.CommitOnRevokeTimeout,
	}
	consumer.sasl = cfg.SASL
	consumer.tls = cfg.TLS

	// Register routing Prometheus metrics once across all Consumer instances.
	// The orchestration pool creates one Consumer per active tenant, so
	// per-instance promauto calls would panic on the second registration.
	consumerRoutingMetrics.once.Do(func() {
		consumerRoutingMetrics.deadLetterCounter = promauto.NewCounterVec(prometheus.CounterOpts{
			Name: MetricDeadLetterTotal,
			Help: "Total records routed to the dead-letter topic",
		}, []string{LabelTenant, LabelReason})
		consumerRoutingMetrics.unknownTopicCounter = promauto.NewCounter(prometheus.CounterOpts{
			Name: MetricUnknownTopicTotal,
			Help: "Total records dropped (without commit) because the topic is not in the registry map",
		})
	})
	consumer.deadLetterCounter = consumerRoutingMetrics.deadLetterCounter
	consumer.unknownTopicCounter = consumerRoutingMetrics.unknownTopicCounter

	// Tests pass cfg.Registerer to bypass the package singleton; nil uses promauto.
	if cfg.Registerer != nil {
		counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricConsumerTopicDeletedTotal,
			Help: "Total topics paused after broker-reported deletion, by consumer type",
		}, []string{LabelConsumerType})
		if err := cfg.Registerer.Register(counterVec); err != nil {
			client.Close()
			cancel()
			return nil, fmt.Errorf("failed to register deleted topic counter: %w", err)
		}
		consumer.deletedCounter = counterVec.With(prometheus.Labels{LabelConsumerType: cfg.ConsumerType})
	} else {
		consumerDeletedMetrics.once.Do(func() {
			consumerDeletedMetrics.counterVec = promauto.NewCounterVec(prometheus.CounterOpts{
				Name: MetricConsumerTopicDeletedTotal,
				Help: "Total topics paused after broker-reported deletion, by consumer type",
			}, []string{LabelConsumerType})
		})
		consumer.deletedCounter = consumerDeletedMetrics.counterVec.With(prometheus.Labels{LabelConsumerType: cfg.ConsumerType})
	}
	consumer.pauser = client
	consumer.onUnknownTopic = cfg.OnUnknownTopic

	// Register partition-revoke commit metrics (counter + histogram).
	// Tests pass cfg.Registerer to bypass the singleton; nil uses promauto.
	if cfg.Registerer != nil {
		revokeCounterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricRevokeCommitTotal,
			Help: "Total partition-revoke commit attempts, by result",
		}, []string{LabelResult})
		if err := cfg.Registerer.Register(revokeCounterVec); err != nil {
			client.Close()
			cancel()
			return nil, fmt.Errorf("failed to register revoke commit counter: %w", err)
		}
		revokeHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    MetricRevokeCommitDurationSeconds,
			Help:    "Duration of CommitMarkedOffsets calls during partition revoke, in seconds",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		})
		if err := cfg.Registerer.Register(revokeHistogram); err != nil {
			client.Close()
			cancel()
			return nil, fmt.Errorf("failed to register revoke commit duration histogram: %w", err)
		}
		consumer.revokeCommitCounter = revokeCounterVec
		consumer.revokeCommitDuration = revokeHistogram
	} else {
		// Register both counter and histogram in a single once.Do for atomicity:
		// either both are registered or neither (concurrent NewConsumer calls are safe).
		consumerRevokeCommitMetrics.once.Do(func() {
			consumerRevokeCommitMetrics.counterVec = promauto.NewCounterVec(prometheus.CounterOpts{
				Name: MetricRevokeCommitTotal,
				Help: "Total partition-revoke commit attempts, by result",
			}, []string{LabelResult})
			consumerRevokeCommitMetrics.durationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
				Name:    MetricRevokeCommitDurationSeconds,
				Help:    "Duration of CommitMarkedOffsets calls during partition revoke, in seconds",
				Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
			})
		})
		consumer.revokeCommitCounter = consumerRevokeCommitMetrics.counterVec
		consumer.revokeCommitDuration = consumerRevokeCommitMetrics.durationHistogram
	}

	if batchEnabled {
		cfg.Logger.Info().
			Int("batch_size", batchSize).
			Dur("batch_timeout", batchTimeout).
			Msg("Kafka message batching enabled")
	}

	return consumer, nil
}

// Start begins consuming messages from Kafka in a background goroutine.
// It returns immediately after launching the consumer loop.
// Call Stop to gracefully shut down the consumer.
func (c *Consumer) Start() error {
	c.logger.Info().Msg("Starting Kafka consumer")

	// Start consumer loop
	c.wg.Go(c.consumeLoop)

	return nil
}

// Stop gracefully stops the consumer and waits for pending messages to complete.
// It cancels the consumer context, waits for the consume loop to exit, then
// closes the Kafka client. Final metrics are logged before returning.
func (c *Consumer) Stop() error {
	c.logger.Info().Msg("Stopping Kafka consumer")

	// Cancel context to stop consume loop
	c.cancel()

	// Wait for consumer loop to finish
	c.wg.Wait()

	// Close client
	c.client.Close()

	c.logger.Info().
		Uint64("messages_processed", c.messagesProcessed.Load()).
		Uint64("messages_failed", c.messagesFailed.Load()).
		Msg("Kafka consumer stopped")

	return nil
}

// handleRevoke commits all explicitly marked offsets within the configured timeout
// before the broker reassigns the revoked partitions. It is called from the
// OnPartitionsRevoked closure registered in NewConsumer.
//
// The revokeCtx parameter is the context supplied by franz-go to OnPartitionsRevoked.
// It is accepted for contextcheck compliance but intentionally NOT used as the commit
// timeout base — see research.md Decision 3: during graceful shutdown, Stop() calls
// c.cancel() before client.Close(), so c.ctx (and any child context) is already
// canceled by the time this runs. context.Background() is the only correct base.
func (c *Consumer) handleRevoke(_ context.Context, revoked map[string][]int32) {
	// context.Background() is intentional — the caller's ctx (franz-go's rebalance ctx)
	// is already canceled during graceful shutdown because Stop() calls c.cancel() before
	// client.Close(), and OnPartitionsRevoked fires synchronously inside client.Close().
	// Using the caller's ctx would make CommitMarkedOffsets fail immediately on shutdown.
	// See research.md Decision 3 for the full rationale.
	commitCtx, commitCancel := context.WithTimeout(context.Background(), c.commitOnRevokeTimeout)
	defer commitCancel()

	start := time.Now()
	err := c.committer.CommitMarkedOffsets(commitCtx) //nolint:contextcheck // context.Background() is intentional — caller ctx is already canceled during shutdown (Stop cancels c.ctx before client.Close); see Decision 3 in research.md
	c.revokeCommitDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		c.revokeCommitCounter.WithLabelValues(ResultFailure).Inc()
		c.logger.Error().
			Err(err).
			Interface(LogFieldPartitions, revoked).
			Dur(LogFieldTimeout, c.commitOnRevokeTimeout).
			Msg(MsgCommitOnRevokeFailed)
		return
	}

	c.revokeCommitCounter.WithLabelValues(ResultSuccess).Inc()
	c.logger.Info().
		Interface(LogFieldPartitions, revoked).
		Msg(MsgCommitOnRevokeSuccess)
}

// AddConsumeTopics dynamically adds topics to the consumer without triggering
// a full consumer group rebalance. This uses franz-go's AddConsumeTopics()
// which implements incremental cooperative rebalancing (KIP-429, Kafka 2.4+).
//
// Benefits over recreating the consumer:
//   - No stop-the-world rebalance (5-30s pause avoided)
//   - Existing partition assignments remain stable
//   - New topics are assigned incrementally
//   - Zero message loss during topic addition
//
// Thread Safety: Safe for concurrent use.
func (c *Consumer) AddConsumeTopics(topics ...string) {
	if len(topics) == 0 {
		return
	}

	c.client.AddConsumeTopics(topics...)

	c.logger.Info().
		Strs("topics", topics).
		Msg("Added topics to consumer (incremental assignment)")
}

// PauseFetchTopics stops the consumer from fetching the given topics.
// This is used to stop consuming deprovisioned tenant topics without
// triggering a rebalance. Paused topics persist until the consumer restarts.
//
// Note: franz-go does not support removing topics from a group consumer.
// PauseFetchTopics is the best available alternative — it eliminates
// fetch bandwidth waste while keeping the consumer group stable.
//
// Thread Safety: Safe for concurrent use.
func (c *Consumer) PauseFetchTopics(topics ...string) {
	if len(topics) == 0 {
		return
	}

	c.client.PauseFetchTopics(topics...)

	c.logger.Info().
		Strs("topics", topics).
		Msg("Paused fetching from deprovisioned topics")
}

// consumeLoop continuously polls for messages
func (c *Consumer) consumeLoop() {
	// CRITICAL: Panic recovery must be FIRST defer (executes LAST in LIFO order)
	defer logging.RecoverPanic(*c.logger, "consumeLoop", nil)

	// If batching is disabled, use the old one-by-one processing
	if !c.batchEnabled {
		c.consumeLoopUnbatched()
		return
	}

	// Batching enabled. A dedicated poller goroutine BLOCKS on PollFetches (letting each broker fetch
	// run to completion — bounding the poll instead would cancel in-flight fetches before they return)
	// and feeds raw records to this loop over a channel. This loop owns the batch + commit state and
	// selects over {records, flush timer, ctx}, so the flush timer fires every batchTimeout — even on a
	// quiet topic — and a partial batch (< batchSize) is delivered within batchTimeout rather than being
	// stranded until the next record happens to arrive (Constitution §VII: the message pipeline MUST NOT
	// stall; the earlier single-goroutine loop blocked in PollFetches so its flush timer never fired,
	// stranding burst tails indefinitely). §VII also prefers this goroutine-owns-state + channel design
	// over shared state. (The one exception to the batchTimeout flush cadence is an in-progress CPU
	// emergency brake in prepareMessage, which intentionally blocks this loop as backpressure until CPU
	// recovers — bounded, not a stall.)
	recordCh := make(chan *kgo.Record, c.batchSize)

	c.wg.Go(func() {
		defer logging.RecoverPanic(*c.logger, "consumePoller", nil)
		for {
			fetches := c.client.PollFetches(c.ctx)
			if c.ctx.Err() != nil {
				return // shutdown: c.ctx canceled (fetches carries only the cancel error)
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				if paused := c.handleFetchErrors(errs); len(paused) > 0 {
					c.deletedCounter.Add(float64(len(paused)))
				}
			}
			stopped := false
			fetches.EachRecord(func(record *kgo.Record) {
				if stopped {
					return
				}
				select {
				case recordCh <- record:
				case <-c.ctx.Done():
					stopped = true
				}
			})
			if stopped {
				return
			}
		}
	})

	batch := make([]preparedMessage, 0, c.batchSize)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		// Send all messages in batch
		for _, msg := range batch {
			c.broadcast(msg.subject, msg.message, msg.record.Topic, msg.record.Partition, msg.record.Offset)
			c.committer.MarkCommitRecords(msg.record)
			c.incrementProcessed(msg.topic)
		}

		c.incrementBatches()

		// Log batching metrics periodically (only when debug enabled)
		// IMPORTANT: Guard with level check to avoid expensive mutex lock in getBatchCount()
		// Cost when disabled: ~1ns (level check) vs ~100ns (modulo + mutex lock)
		if c.logger.GetLevel() <= zerolog.DebugLevel {
			if batches := c.getBatchCount(); batches%100 == 0 {
				c.logger.Debug().
					Uint64("batches_sent", batches).
					Int("last_batch_size", len(batch)).
					Msg("Kafka batching metrics")
			}
		}

		// Clear batch (reuse backing array)
		batch = batch[:0]
	}

	flushTimer := time.NewTimer(c.batchTimeout)
	defer flushTimer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			flushBatch()
			return

		case <-flushTimer.C:
			// Fires every batchTimeout regardless of traffic — resetting even on an empty flush keeps
			// it firing, so an accumulated partial batch is never left waiting.
			flushBatch()
			flushTimer.Reset(c.batchTimeout)

		case record := <-recordCh:
			msg, ctxCanceled := c.prepareMessage(record)
			if msg != nil {
				batch = append(batch, *msg)
				if len(batch) >= c.batchSize {
					flushBatch() // full-batch flush; the timer keeps its own cadence (may fire on an empty batch — a no-op)
				}
			} else if !ctxCanceled {
				// Deliberate drop (rate-limit, malformed, DLQ): mark so the offset is not
				// re-delivered after rebalance. CPU-brake ctx cancel is not marked — the record
				// must be re-delivered after rebalance.
				c.committer.MarkCommitRecords(record)
			}
		}
	}
}

// consumeLoopUnbatched is the original implementation without batching (for backward compatibility)
func (c *Consumer) consumeLoopUnbatched() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Poll for messages
			fetches := c.client.PollFetches(c.ctx)

			// Check for errors
			if errs := fetches.Errors(); len(errs) > 0 {
				if paused := c.handleFetchErrors(errs); len(paused) > 0 {
					c.deletedCounter.Add(float64(len(paused)))
				}
			}

			// Process records one by one
			fetches.EachRecord(func(record *kgo.Record) {
				c.processRecord(record)
			})
		}
	}
}

// preparedMessage holds a validated, ready-to-broadcast Kafka message.
type preparedMessage struct {
	topic   string
	subject string
	message []byte
	record  *kgo.Record // original record, used for MarkCommitRecords at broadcast site
}

// prepareMessage validates and prepares a message for batching.
// Returns (msg, ctxCanceled):
//   - (nil, false): deliberate drop (rate-limit, malformed, DLQ) — caller MUST mark the record
//   - (nil, true): CPU-brake interrupted by ctx.Done() — caller MUST NOT mark (re-delivery expected)
//   - (msg, false): ready to broadcast — caller MUST mark after broadcast
func (c *Consumer) prepareMessage(record *kgo.Record) (*preparedMessage, bool) {
	// ============================================================================
	// LAYER 1: RATE LIMITING
	// ============================================================================
	allow, waitDuration := c.resourceGuard.AllowKafkaMessage(c.ctx)
	if !allow {
		c.incrementDropped(record.Topic)

		// Log every 100th drop to avoid log spam
		dropped := c.getDroppedCount()
		if dropped%100 == 0 {
			c.logger.Warn().
				Uint64("dropped_count", dropped).
				Dur("would_wait", waitDuration).
				Str(LabelTopic, record.Topic).
				Msg("Kafka rate limit exceeded - dropping messages")
		}
		return nil, false // deliberate drop — caller must mark
	}

	// ============================================================================
	// LAYER 2: CPU EMERGENCY BRAKE - BACKPRESSURE MODE (zero message loss!)
	// ============================================================================
	// If CPU is critically high (>80% by default), WAIT instead of dropping
	// This creates natural backpressure - messages are never lost
	if c.resourceGuard.ShouldPauseKafka() {
		pauseStart := time.Now()
		pauseLogged := false

		// Wait until CPU recovers (with context cancellation support)
		for c.resourceGuard.ShouldPauseKafka() {
			if !pauseLogged {
				c.logger.Warn().
					Str(LabelTopic, record.Topic).
					Msg("CPU emergency brake - entering backpressure mode (waiting for CPU recovery)")
				pauseLogged = true
			}

			select {
			case <-c.ctx.Done():
				return nil, true // context canceled during brake — do NOT mark; record will be re-delivered
			case <-time.After(c.backpressureCheckInterval):
				// Check CPU again
			}
		}

		if pauseLogged {
			c.logger.Info().
				Dur("pause_duration", time.Since(pauseStart)).
				Str(LabelTopic, record.Topic).
				Msg("CPU emergency brake released - resuming consumption")
		}
	}

	channel, reason, tenant := c.extractChannel(record)
	if reason == ReasonUnknownTopic {
		if c.unknownTopicCounter != nil {
			c.unknownTopicCounter.Inc()
		}
		c.incrementFailed()
		c.logUnknownTopicDrop(record)
		return nil, true // registry miss — do NOT mark; redeliver once the registry catches up
	}
	if reason != "" {
		c.routeToDLQ(record, tenant, reason)
		c.incrementFailed()
		return nil, false // deliberate drop — caller must mark
	}

	return &preparedMessage{
		topic:   record.Topic,
		subject: channel,
		message: record.Value,
		record:  record,
	}, false
}

// processRecord handles a single Kafka record with three-layer protection
//
// LAYER 1: Rate limiting (caps consumption at configured rate, e.g., 25 msg/sec)
// LAYER 2: CPU emergency brake (pauses when CPU exceeds threshold, e.g., 80%)
// LAYER 3: Direct broadcast (worker pool removed - broadcast is already non-blocking)
//
// This achieves 12K connections @ 30% CPU with minimal overhead.
// Without these protections, Kafka consumer blocks synchronously, causing plateau at 2.2K connections.
func (c *Consumer) processRecord(record *kgo.Record) {
	// ============================================================================
	// LAYER 1: RATE LIMITING
	// ============================================================================
	// Check if we're allowed to process this message based on configured rate limit
	// If rate limit exceeded, drop message and let Kafka handle redelivery
	allow, waitDuration := c.resourceGuard.AllowKafkaMessage(c.ctx)
	if !allow {
		c.incrementDropped(record.Topic)

		// Log every 100th drop to avoid log spam
		dropped := c.getDroppedCount()
		if dropped%100 == 0 {
			c.logger.Warn().
				Uint64("dropped_count", dropped).
				Dur("would_wait", waitDuration).
				Str(LabelTopic, record.Topic).
				Msg("Kafka rate limit exceeded - dropping messages")
		}
		// Deliberate drop — mark so offset is not re-delivered after rebalance.
		c.committer.MarkCommitRecords(record)
		return
	}

	// ============================================================================
	// LAYER 2: CPU EMERGENCY BRAKE - BACKPRESSURE MODE (zero message loss!)
	// ============================================================================
	// If CPU is critically high (>80% by default), WAIT instead of dropping
	// This creates natural backpressure:
	// - Consumer pauses, Kafka retains messages
	// - When CPU recovers (drops below lower threshold), processing resumes
	// - Zero messages are lost - critical for trading platforms!
	//
	// Trade-off: Consumer lag increases during high CPU, but all messages are delivered
	if c.resourceGuard.ShouldPauseKafka() {
		pauseStart := time.Now()
		pauseLogged := false

		// Wait until CPU recovers (with context cancellation support)
		for c.resourceGuard.ShouldPauseKafka() {
			// Log once when entering pause state
			if !pauseLogged {
				c.logger.Warn().
					Str(LabelTopic, record.Topic).
					Msg("CPU emergency brake - entering backpressure mode (waiting for CPU recovery)")
				pauseLogged = true
			}

			select {
			case <-c.ctx.Done():
				// Context canceled during CPU brake — do NOT mark; record will be re-delivered
				// after rebalance (same constraint as ctxCanceled=true path in prepareMessage).
				return
			case <-time.After(c.backpressureCheckInterval):
				// Check CPU again after short wait
			}
		}

		// Log recovery
		if pauseLogged {
			c.logger.Info().
				Dur("pause_duration", time.Since(pauseStart)).
				Str(LabelTopic, record.Topic).
				Msg("CPU emergency brake released - resuming consumption")
		}
	}

	channel, reason, tenant := c.extractChannel(record)
	if reason == ReasonUnknownTopic {
		if c.unknownTopicCounter != nil {
			c.unknownTopicCounter.Inc()
		}
		c.incrementFailed()
		c.logUnknownTopicDrop(record)
		// Registry miss — do NOT mark; the record redelivers once the registry catches up.
		return
	}
	if reason != "" {
		c.routeToDLQ(record, tenant, reason)
		c.incrementFailed()
		// Deliberate drop — mark so offset is not re-delivered after rebalance.
		c.committer.MarkCommitRecords(record)
		return
	}

	c.broadcast(channel, record.Value, record.Topic, record.Partition, record.Offset)
	c.committer.MarkCommitRecords(record)
	c.incrementProcessed(record.Topic)

	c.logger.Debug().
		Str("channel", channel).
		Str(LabelTopic, record.Topic).
		Msg("Consumed Kafka message")
}

// routeToDLQ submits a record to the dead-letter queue using a non-blocking send.
// If the DLQ is nil or the channel is full, the record is dropped and counted.
// tenant is resolved by the caller from the registry map (#179 P3) — no reverse-parse here.
func (c *Consumer) routeToDLQ(record *kgo.Record, tenant, reason string) {

	if c.deadLetterCounter != nil {
		c.deadLetterCounter.WithLabelValues(tenant, reason).Inc()
	}

	if c.dlq == nil {
		return
	}

	dlqTopic := kafkashared.BuildTopicName(c.namespace, tenant, routing.DeadLetterTopicSuffix)
	dlqRec := &kgo.Record{
		Topic: dlqTopic,
		Key:   record.Key,
		Value: record.Value,
		Headers: append(slices.Clone(record.Headers),
			kgo.RecordHeader{Key: HeaderReason, Value: []byte(reason)},
		),
	}
	// Non-blocking submit; drop silently if queue full (deadLetterCounter already incremented above).
	c.dlq.TrySubmit(dlqJob{record: dlqRec, tenant: tenant, reason: reason})
}

// findHeader returns the value of the first header matching key, or nil.
func findHeader(record *kgo.Record, key string) []byte {
	for _, h := range record.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}

// extractChannel resolves the broadcast channel for a record. Returns (channel, reason, tenant):
//   - reason=ReasonUnknownTopic: topic not in the registry map; caller drops WITHOUT commit (redeliver).
//   - reason!="" (other): channel is invalid/missing; caller routes to DLQ (tenant supplied for the DLQ).
//   - otherwise: channel is valid.
//
// The topic's tenant is resolved from the registry map (#179 P3), never reverse-parsed.
func (c *Consumer) extractChannel(record *kgo.Record) (channel, reason, tenant string) {
	if c.tenantResolver == nil {
		return "", ReasonUnknownTopic, ""
	}
	tenant, ok := c.tenantResolver(record.Topic)
	if !ok {
		return "", ReasonUnknownTopic, ""
	}

	// The kafka backend runs on every edition (ADR-0009), so every producer stamps the channel header
	// (first-party producer.go and the tester's kafka publisher both do). The header is a protocol
	// invariant here — there is no record.Key fallback. A headerless record on a known topic is a
	// permanent producer bug, so it routes to the DLQ (caller marks the offset); it is NOT the
	// transient ReasonUnknownTopic redeliver leg.
	headerVal := findHeader(record, kafkashared.HeaderChannel)
	if headerVal == nil {
		return "", ReasonMissingChannelHeader, tenant
	}

	// Header present: validate it's well-formed, then check its tenant prefix matches the topic tenant —
	// a record on prod.acme.* carrying channel "evil.secret" must never broadcast cross-tenant (§IX).
	ch := string(headerVal)
	dot := strings.IndexByte(ch, '.')
	if dot <= 0 {
		return "", ReasonInvalidChannelKey, tenant
	}
	if ch[:dot] != tenant {
		return "", ReasonTenantPrefixMismatch, tenant
	}
	return ch, "", tenant
}

// GetMetrics returns current consumer metrics: messages processed successfully,
// messages that failed validation, and messages dropped due to rate limiting.
// Thread-safe for concurrent access.
func (c *Consumer) GetMetrics() (processed, failed, dropped uint64) {
	return c.messagesProcessed.Load(), c.messagesFailed.Load(), c.messagesDropped.Load()
}

// UnknownTopicDrops returns the count of records dropped because the topic→tenant
// map had no entry for the record's topic (registry-miss). A non-zero value means
// records were silently lost in-session — see logUnknownTopicDrop.
func (c *Consumer) UnknownTopicDrops() uint64 {
	return c.unknownTopicDrops.Load()
}

func (c *Consumer) incrementProcessed(topic string) {
	c.messagesProcessed.Add(1)
	metrics.IncrementKafkaMessages(topic, c.consumerGroup)
}

func (c *Consumer) incrementFailed() {
	c.messagesFailed.Add(1)
}

func (c *Consumer) incrementDropped(topic string) {
	c.messagesDropped.Add(1)
	metrics.IncrementKafkaDropped(topic, c.consumerGroup)
}

func (c *Consumer) getDroppedCount() uint64 {
	return c.messagesDropped.Load()
}

func (c *Consumer) incrementBatches() {
	c.batchesSent.Add(1)
}

func (c *Consumer) getBatchCount() uint64 {
	return c.batchesSent.Load()
}

// handleFetchErrors processes fetch errors from a single poll cycle.
// It deduplicates by topic: for UnknownTopicOrPartition errors it pauses
// the topic and dispatches the callback once per topic; all other errors
// are logged at error level. Returns the list of newly paused topics so
// the caller can increment the deleted-topic counter atomically.
//
// PauseFetchTopics is called BEFORE invokeUnknownTopicCallback to guarantee
// the topic is paused even if the callback panics.
func (c *Consumer) handleFetchErrors(errs []kgo.FetchError) []string {
	seen := make(map[string]struct{})
	var paused []string

	for _, fe := range errs {
		if errors.Is(fe.Err, kerr.UnknownTopicOrPartition) {
			if _, ok := seen[fe.Topic]; ok {
				continue // deduplicate: multiple partitions may report the same topic
			}
			seen[fe.Topic] = struct{}{}
			c.pauser.PauseFetchTopics(fe.Topic)
			paused = append(paused, fe.Topic)
			c.logger.Warn().
				Str(LabelTopic, fe.Topic).
				Msg(MsgTopicDeletedAtBroker)
			invokeUnknownTopicCallback(*c.logger, c.onUnknownTopic, fe.Topic)
		} else {
			c.logger.Error().
				Err(fe.Err).
				Str(LabelTopic, fe.Topic).
				Int32(LogFieldPartition, fe.Partition).
				Msg(MsgFetchError)
		}
	}
	return paused
}

// logUnknownTopicDrop records and surfaces a registry-miss drop: a record was
// fetched from a topic the topic→tenant map cannot resolve, so it is dropped
// without being marked. The "redeliver once the registry catches up" contract
// only holds across a rebalance — within a session the fetch position has moved
// past this offset, so the record is effectively lost. That makes the drop a
// silent-data-loss risk (§V), so it MUST be observable: the count is exported
// via UnknownTopicDrops() and each drop logs topic/partition/offset. Cadence is first-hit
// plus every 100th — a single-record loss (the fresh-tenant provisioning race)
// is always visible, while a persistent misconfiguration cannot spam the log.
func (c *Consumer) logUnknownTopicDrop(record *kgo.Record) {
	n := c.unknownTopicDrops.Add(1)
	if n == 1 || n%100 == 0 {
		c.logger.Warn().
			Str(LabelTopic, record.Topic).
			Int32(LogFieldPartition, record.Partition).
			Int64("offset", record.Offset).
			Uint64("unknown_topic_drops", n).
			Msg("record dropped: topic not in tenant registry map — not redelivered in-session")
	}
}

// consumeResetOffset selects the reset offset for topics the group has never
// committed. afterMillis > 0 → AfterMilli(afterMillis): consume from the end of
// partitions that existed at that time but from the start (offset 0) of any
// partition created after it — eliminating the fresh-topic AtEnd offset-list
// race while preserving no-history-replay for pre-existing topics (ADR-0010).
// afterMillis == 0 → AtEnd (historical default; used by direct-construction tests).
func consumeResetOffset(afterMillis int64) kgo.Offset {
	if afterMillis > 0 {
		return kgo.NewOffset().AfterMilli(afterMillis)
	}
	return kgo.NewOffset().AtEnd()
}

// invokeUnknownTopicCallback calls cb(topic) with panic recovery.
// A panicking callback must not abort the handleFetchErrors loop or leave
// remaining topics unprocessed.
func invokeUnknownTopicCallback(logger zerolog.Logger, cb func(string), topic string) {
	defer logging.RecoverPanic(logger, "unknownTopicCallback", nil)
	if cb != nil {
		cb(topic)
	}
}

// ReplayMessage represents a message replayed from Kafka
type ReplayMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Subject   string // The Kafka Key = broadcast subject
	Data      []byte
	Pos       string // Encoded Kafka position "(partition+1)-offset" for replay cursor
}

// ReplayFromOffsets creates a temporary direct consumer and replays messages
// from exact per-partition offsets. This serves both recovery surfaces: the
// reconnect flow (exclusive last_pos cursors) and the live replay request
// (inclusive from_pos cursor).
//
// Parameters:
//   - ctx: Context for cancellation (should have timeout, e.g. 5 seconds)
//   - positions: topic → partition → start offset. The partition comes from the
//     client's pos cursor and is consumed exactly — never fanned out to the
//     topic's other partitions (their logs are unrelated; an exact offset valid
//     on the anchor partition can be out of range on them)
//   - maxMessages: Maximum number of messages to replay (prevents overwhelming client)
//   - subscriptions: Filter to only include messages for these channels (e.g. ["BTC.trade", "ETH.trade"])
//
// Returns:
//   - []ReplayMessage: Messages from [start → log end at call time] filtered by subscriptions
//   - error: kerr.OffsetOutOfRange when a cursor lies outside a partition's log
//     (stale/invalid cursor — all-or-nothing, no partial delivery); otherwise
//     consumer-creation/metadata failures
//
// Implementation notes:
//   - Group-less direct consumer: kgo rejects ConsumePartitions combined with
//     ConsumerGroup, and isolation from the main group is structural — a direct
//     consumer never joins the group protocol or commits offsets
//   - NoResetOffset so an out-of-range cursor SURFACES instead of silently
//     resetting to the partition start (the kgo default), which would replay
//     records the client never asked for
//   - Termination is deterministic: log-end offsets are captured up front and
//     the poll loop stops once every requested partition is caught up. Without
//     this, PollFetches never wakes on empty fetches and every replay would
//     burn its full context budget
//
// Usage example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	messages, err := consumer.ReplayFromOffsets(ctx, map[string]map[int32]int64{
//	    "sukko.trades": {2: 12345}, // partition 2 from offset 12345
//	}, 100, []string{"BTC.trade", "ETH.trade"})
func (c *Consumer) ReplayFromOffsets(
	ctx context.Context,
	positions map[string]map[int32]int64,
	maxMessages int,
	subscriptions []string,
) ([]ReplayMessage, error) {
	if len(positions) == 0 {
		return nil, errors.New("at least one topic offset is required")
	}

	c.logger.Info().
		Interface("positions", positions).
		Int("max_messages", maxMessages).
		Strs("subscriptions", subscriptions).
		Msg("Starting Kafka replay from offsets")

	// Build the exact-partition offset map for kgo. Only the cursor's own
	// partition is consumed: records are keyed by channel (one partition per
	// channel), and an exact offset valid on the anchor partition can be out of
	// range on the topic's other partitions.
	startOffsets := make(map[string]map[int32]kgo.Offset, len(positions))
	for topic, partitionOffsets := range positions {
		offsets := make(map[int32]kgo.Offset, len(partitionOffsets))
		for partition, offset := range partitionOffsets {
			offsets[partition] = kgo.NewOffset().At(offset)
		}
		startOffsets[topic] = offsets
	}

	brokers, ok := c.client.OptValue(kgo.SeedBrokers).([]string)
	if !ok || len(brokers) == 0 {
		return nil, errors.New("replay: unable to extract broker addresses from consumer")
	}

	replayOpts, err := kafkashared.BuildKgoOpts(brokers, c.sasl, c.tls)
	if err != nil {
		return nil, fmt.Errorf("replay: build kafka options: %w", err)
	}
	// Direct partition consumer, deliberately WITHOUT a consumer group: kgo
	// rejects ConsumePartitions combined with ConsumerGroup, and isolation from
	// the main group is structural — a direct consumer never commits offsets.
	// NoResetOffset makes an out-of-range exact offset surface as
	// kerr.OffsetOutOfRange instead of silently resetting to the partition
	// start (the kgo default), which would replay from the beginning and
	// misreport the gap.
	replayOpts = append(replayOpts,
		kgo.ConsumePartitions(startOffsets),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)
	if c.fetchMaxWait > 0 {
		replayOpts = append(replayOpts, kgo.FetchMaxWait(c.fetchMaxWait))
	}
	if c.fetchMinBytes > 0 {
		replayOpts = append(replayOpts, kgo.FetchMinBytes(c.fetchMinBytes))
	}
	if c.replayFetchMaxBytes > 0 {
		replayOpts = append(replayOpts, kgo.FetchMaxBytes(c.replayFetchMaxBytes))
	} else if c.fetchMaxBytes > 0 {
		replayOpts = append(replayOpts, kgo.FetchMaxBytes(c.fetchMaxBytes))
	}

	tempClient, err := kgo.NewClient(replayOpts...)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to create replay consumer")
		return nil, fmt.Errorf("failed to create replay consumer: %w", err)
	}
	defer tempClient.Close()

	// Capture each partition's log-end offset up front so the poll loop can
	// terminate deterministically once every requested partition is caught up.
	// PollFetches never wakes on empty fetch responses — without a known end,
	// a caught-up replay blocks until the context expires, burning the full
	// timeout budget on every reconnect and leaving the live-replay handler's
	// send loop racing an already-expired context.
	topics := make([]string, 0, len(positions))
	for topic := range positions {
		topics = append(topics, topic)
	}
	endOffsets, err := kadm.NewClient(tempClient).ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("replay: list end offsets: %w", err)
	}
	// pending[topic][partition] = end offset (exclusive, "one past the last
	// record" at call time). Records produced AFTER this snapshot are live-path;
	// replay terminates at the snapshot (one may still ride along if it lands in
	// the same fetch batch as the last pre-snapshot record — benign, clients
	// dedupe overlapping live/replay copies by mid).
	pending := make(map[string]map[int32]int64, len(positions))
	pendingCount := 0
	for topic, partitionOffsets := range positions {
		for partition, start := range partitionOffsets {
			end, ok := endOffsets.Lookup(topic, partition)
			if !ok {
				return nil, fmt.Errorf("replay: no end offset listed for %s[%d]", topic, partition)
			}
			if end.Err != nil {
				return nil, fmt.Errorf("replay: list end offset for %s[%d]: %w", topic, partition, end.Err)
			}
			if start > end.Offset {
				// The cursor claims a record beyond the log end — stale/invalid,
				// same contract as the broker's fetch-level OOR (all-or-nothing).
				return nil, kerr.OffsetOutOfRange
			}
			if start == end.Offset {
				continue // fully caught up — nothing to replay on this partition
			}
			if pending[topic] == nil {
				pending[topic] = make(map[int32]int64)
			}
			pending[topic][partition] = end.Offset
			pendingCount++
		}
	}
	if pendingCount == 0 {
		c.logger.Info().Msg("Kafka replay completed (no gap — all cursors at log end)")
		return nil, nil
	}

	// Create subscription filter set for O(1) lookup
	subSet := make(map[string]struct{}, len(subscriptions))
	for _, sub := range subscriptions {
		subSet[sub] = struct{}{}
	}

	// Collect replayed messages
	var messages []ReplayMessage
	messagesRead := 0

	// Poll until every requested partition reaches its captured log end, the
	// message cap is hit, or the context expires.
	for messagesRead < maxMessages && pendingCount > 0 {
		select {
		case <-ctx.Done():
			c.logger.Warn().
				Int("messages_read", messagesRead).
				Msg("Replay canceled by context timeout")
			return messages, fmt.Errorf("replay canceled: %w", ctx.Err())
		default:
		}

		fetches := tempClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		// intentionally not handleFetchErrors: replay consumer is short-lived; pausing or firing the pool callback would be wrong.
		// Check for errors; propagate OffsetOutOfRange so callers can surface it to clients.
		// Log all errors first, then return OOR — prevents a leading OOR from silently
		// swallowing non-OOR errors on other partitions in the same fetch (Constitution III).
		if errs := fetches.Errors(); len(errs) > 0 {
			var oor bool
			for _, fe := range errs {
				if errors.Is(fe.Err, kerr.OffsetOutOfRange) {
					oor = true
					continue
				}
				c.logger.Warn().
					Err(fe.Err).
					Str(LabelTopic, fe.Topic).
					Int32(LogFieldPartition, fe.Partition).
					Msg("Replay fetch error")
			}
			if oor {
				// Intentionally all-or-nothing: return nil (not partial messages) so the
				// handler can surface a clean "offset out of range" error to the client.
				// Unlike ctx-cancel (which delivers a partial set), OOR means the requested
				// position never existed — partial delivery would be misleading.
				// §III intentional exception: non-OOR errors on other partitions have already
				// been logged in the loop above; we return OOR as the primary actionable error.
				return nil, kerr.OffsetOutOfRange
			}
		}

		// Fallback only: with deterministic end-offset termination the loop
		// normally exits via pendingCount; an empty poll here means the context
		// expired (PollFetches never returns empty while records are pending).
		if fetches.NumRecords() == 0 {
			break
		}

		// Process records
		fetches.EachRecord(func(record *kgo.Record) {
			// Caught-up bookkeeping FIRST — every consumed record advances its
			// partition toward the captured end, whether or not it is appended
			// (filtered/dropped records still move the cursor).
			if end, tracked := pending[record.Topic][record.Partition]; tracked && record.Offset >= end-1 {
				delete(pending[record.Topic], record.Partition)
				pendingCount--
			}
			if messagesRead >= maxMessages {
				return // Stop if we've hit the limit
			}

			// Parse message to get subject.
			// IMPORTANT: must NOT call MarkCommitRecords here — replay uses a temporary
			// client and marking via c.committer would contaminate main consumer offset state.
			msg, _ := c.prepareMessage(record)
			if msg == nil {
				return
			}

			// Filter by subscriptions (only return messages client is subscribed to)
			// The subject IS the channel (e.g., "BTC.trade", "BTC.balances.user123")
			if len(subSet) > 0 {
				if _, subscribed := subSet[msg.subject]; !subscribed {
					return // Skip messages for unsubscribed channels
				}
			}

			// Add to replay results
			messages = append(messages, ReplayMessage{
				Topic:     record.Topic,
				Partition: record.Partition,
				Offset:    record.Offset,
				Subject:   msg.subject,
				Data:      msg.message,
				Pos:       history.EncodePos(record.Partition, record.Offset),
			})
			messagesRead++
		})
	}

	c.logger.Info().
		Int("messages_replayed", len(messages)).
		Int("messages_read", messagesRead).
		Msg("Kafka replay completed")

	return messages, nil
}
