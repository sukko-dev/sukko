// Package kafka provides Kafka/Redpanda integration for the WebSocket server.
// This file contains the Producer for publishing client messages to Kafka.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/server/backend"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// Re-export sentinel errors from protocol package for backward compatibility.
// These aliases allow existing code that imports kafka.ErrXxx to continue working.
var (
	ErrInvalidChannel      = protocol.ErrInvalidChannel
	ErrTopicNotProvisioned = protocol.ErrTopicNotProvisioned
	ErrServiceUnavailable  = protocol.ErrServiceUnavailable
)

// ErrProducerClosed is defined locally — it's a producer concern, not a shared protocol type.
var ErrProducerClosed = errors.New("producer is closed")

// defaultProducerShutdownTimeout is the fallback for ProducerConfig.ShutdownTimeout when it is
// unset (test path / direct construction). Production sets it via KAFKA_PRODUCER_SHUTDOWN_TIMEOUT.
const defaultProducerShutdownTimeout = 10 * time.Second

// errFanoutAllFailed marks a multi-topic publish where every target topic write failed. It
// wraps backend.ErrPublishFailed (→ gateway 500, §III: the client is NOT told a dropped
// message succeeded) but is classified breaker-NEUTRAL (isBreakerNeutralErr): a genuine broker
// outage trips the SHARED breaker via any tenant's single-topic produce path, so fan-out
// 0-success needn't — and mustn't, or one tenant's all-unroutable multi-topic rule would 503
// every tenant (#179 P1b-C1).
var errFanoutAllFailed = errors.New("all fan-out topic writes failed")

// ProducerStats holds metrics for the producer.
type ProducerStats struct {
	MessagesPublished atomic.Int64 // Successfully published messages
	MessagesFailed    atomic.Int64 // Failed publish attempts
}

// RoutingSnapshotProvider provides per-tenant routing snapshots used to resolve
// channel → topic(s) mappings at publish time. The consumer implements against this
// narrow view; do NOT widen it (§XVIII — producer-specific needs go on RoutingRulesSource).
type RoutingSnapshotProvider interface {
	GetRoutingSnapshot(tenantID string) (provapi.TenantRoutingSnapshot, bool)
}

// RoutingRulesSource is the producer's view of the routing provider: snapshot lookup plus
// readiness. SnapshotReceived() distinguishes "no rules synced yet" (server degraded,
// retryable → 503) from "tenant has no rules" (reject → 409). StreamChannelRulesProvider
// satisfies it.
type RoutingRulesSource interface {
	RoutingSnapshotProvider
	SnapshotReceived() bool
}

// ProducerConfig holds configuration for creating a Kafka producer.
type ProducerConfig struct {
	Brokers        []string        // Kafka/Redpanda broker addresses (required)
	TopicNamespace string          // Topic namespace: "local", "dev", "staging", "prod" (required)
	ClientID       string          // Client ID for Kafka connection (optional, defaults to "sukko-producer-{hostname}")
	MetadataMinAge time.Duration   // Min interval before refreshing topic metadata (0 = franz-go default). Lower shortens produce latency to a just-created tenant topic.
	Logger         *zerolog.Logger // Structured logger (optional)

	// Security configuration for managed Kafka/Redpanda services
	SASL *kafkashared.SASLConfig // SASL authentication (nil = no auth, for local dev)
	TLS  *kafkashared.TLSConfig  // TLS encryption (nil = no TLS, for local dev)

	// Routing — resolves channel → topic(s) using per-tenant routing rules.
	// Required in kafka mode (kafkabackend.New rejects nil). Publish rejects when the
	// provider yields no applicable rule — there is no community fallback.
	RulesProvider RoutingRulesSource

	// Fan-out / DLQ pool sizing (#179 P1b). When FanoutWorkers > 0, NewProducer constructs
	// the fan-out + DLQ pools so multi-topic routing rules fan out; the DLQ pool retries
	// failed fan-out writes. Production always sets FanoutWorkers >= 1 (validated at startup),
	// so the pools are always built; FanoutWorkers == 0 (no pool) is a test-only affordance
	// (P1b-C8 — not an operator "disable" mode).
	FanoutWorkers   int
	FanoutQueueSize int
	DLQMaxRetries   int
	DLQBaseDelay    time.Duration
	DLQMaxDelay     time.Duration
	DLQRetryWorkers int

	// Registerer overrides the Prometheus registry for the pool metrics; nil uses the
	// package singleton (production default). Tests pass a fresh registry for isolation.
	Registerer prometheus.Registerer

	// Circuit breaker settings (optional - sensible defaults provided)
	CircuitBreakerTimeout      time.Duration // Time before half-open (default: 30s)
	CircuitBreakerMaxFailures  uint32        // Consecutive failures to trip (default: 5)
	CircuitBreakerHalfOpenReqs uint32        // Requests allowed in half-open (default: 1)

	// Producer tuning (optional - sensible defaults provided)
	BatchMaxBytes   int32         // Max batch size in bytes (default: 1MB)
	MaxBufferedRecs int           // Max buffered records (default: 10000)
	RecordRetries   int           // Number of retries (default: 8)
	ProduceTimeout  time.Duration // Timeout for produce operations (default: 10s)

	// ShutdownTimeout bounds how long Close waits for the fan-out/DLQ pool workers to exit
	// before force-closing the client (default: 10s). franz-go ignores ctx cancellation once a
	// record is buffered, so a worker producing to an unresponsive broker would otherwise block
	// Close indefinitely; after this timeout Close closes the client anyway (which aborts the
	// in-flight produce and unblocks the workers). Only used when FanoutWorkers > 0.
	ShutdownTimeout time.Duration
}

// Producer wraps franz-go client for publishing to Kafka/Redpanda.
// It provides a simple interface for WebSocket clients to publish messages.
//
// Thread Safety: All public methods are safe for concurrent use.
//
// Lifecycle: Create with NewProducer, use Publish to send messages, Close when done.
type Producer struct {
	client         *kgo.Client
	topicNamespace string
	logger         *zerolog.Logger
	stats          ProducerStats
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	closed         bool

	// Circuit breaker for Kafka connection
	circuitBreaker *gobreaker.CircuitBreaker[any]

	// Routing dependencies (validated non-nil by kafkabackend.New in production).
	rulesProvider RoutingRulesSource

	// Fan-out pool (constructed in NewProducer when FanoutWorkers > 0). nil → multi-topic
	// rules are rejected before the breaker. wg tracks the pool worker goroutines; Close
	// cancels ctx then waits on wg (bounded by shutdownTimeout) before closing the client.
	fanout          *FanoutPool
	wg              sync.WaitGroup
	shutdownTimeout time.Duration
	dlqDropped      *prometheus.CounterVec // {tenant} — DLQ enqueue-full drop (producer-side leg)
}

// NewProducer creates a new Kafka producer with the provided configuration.
// It validates required fields and initializes the franz-go client with
// optimal settings for client message publishing.
//
// Returns an error if any required field is missing or client creation fails.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("at least one broker is required")
	}
	if cfg.TopicNamespace == "" {
		return nil, errors.New("topic namespace is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Set defaults
	// Auto-generate client ID with hostname for per-instance observability
	// Example: "sukko-producer-sukko-7f8d9c4b5-abc123" in Kubernetes
	clientID := cfg.ClientID
	if clientID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		clientID = "sukko-producer-" + hostname
	}
	// Producer tuning (values provided by env-parsed config)
	batchMaxBytes := cfg.BatchMaxBytes
	maxBufferedRecs := cfg.MaxBufferedRecs
	recordRetries := cfg.RecordRetries

	// Circuit breaker settings (values provided by env-parsed config)
	cbTimeout := cfg.CircuitBreakerTimeout
	cbMaxFailures := cfg.CircuitBreakerMaxFailures
	cbHalfOpenReqs := cfg.CircuitBreakerHalfOpenReqs

	// Build base client options (SeedBrokers, SASL, TLS)
	opts, err := kafkashared.BuildKgoOpts(cfg.Brokers, cfg.SASL, cfg.TLS)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to build kafka options: %w", err)
	}

	// Producer-specific options
	opts = append(opts,
		kgo.ClientID(clientID),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			// Exponential backoff: 100ms, 200ms, 400ms, 800ms, ...
			return time.Duration(100*(1<<attempt)) * time.Millisecond
		}),
	)

	// Producer tuning (apply only if non-zero; omitting uses franz-go defaults)
	if batchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(batchMaxBytes))
	}
	if maxBufferedRecs > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(maxBufferedRecs))
	}
	if recordRetries > 0 {
		opts = append(opts, kgo.RecordRetries(recordRetries))
	}
	// Lower MetadataMinAge shortens the window before the producer re-fetches metadata for a
	// just-created tenant topic (kgo defaults to 5s), so a publish to a freshly-provisioned tenant
	// succeeds sooner instead of stalling on the unknown topic.
	if cfg.MetadataMinAge > 0 {
		opts = append(opts, kgo.MetadataMinAge(cfg.MetadataMinAge))
	}

	// Create franz-go client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create kafka producer client: %w", err)
	}

	// Create circuit breaker for Kafka connection
	cbSettings := gobreaker.Settings{
		Name:        "kafka-producer",
		MaxRequests: cbHalfOpenReqs,
		Interval:    0, // No periodic reset
		Timeout:     cbTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cbMaxFailures
		},
		IsSuccessful: func(err error) bool {
			// The breaker exists to protect against broker/transport unavailability — NOT
			// tenant misconfiguration (#179 P1b-C1). Misconfiguration-class produce errors (a
			// routing/topic problem that waiting will not fix) and shutdown are per-request
			// failures that MUST NOT count toward the shared breaker; otherwise one tenant's
			// bad multi-topic rule (UNKNOWN_TOPIC on every publish) trips the breaker and 503s
			// every tenant. Genuine broker-unavailable errors stay breaker-eligible.
			return err == nil || isBreakerNeutralErr(err)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			if cfg.Logger != nil {
				cfg.Logger.Warn().
					Str("name", name).
					Str("from", from.String()).
					Str("to", to.String()).
					Msg("Circuit breaker state changed")
			}
		},
	}
	cb := gobreaker.NewCircuitBreaker[any](cbSettings)

	producer := &Producer{
		client:         client,
		topicNamespace: strings.ToLower(strings.TrimSpace(cfg.TopicNamespace)),
		logger:         cfg.Logger,
		ctx:            ctx,
		cancel:         cancel,
		circuitBreaker: cb,
		rulesProvider:  cfg.RulesProvider,
	}

	// Construct the fan-out + DLQ pools so multi-topic routing rules fan out (#179 P1b).
	// Production validates FanoutWorkers >= 1, so the pools are always built; FanoutWorkers
	// == 0 is a test-only affordance leaving p.fanout nil (multi-topic rules then reject).
	// Metrics are registered once via newPoolMetrics (P1b-C2 — no per-pool promauto).
	// Env-wired via WS/KAFKA_PRODUCER_SHUTDOWN_TIMEOUT (envDefault 10s is the production source
	// of truth). The <=0 guard is the fallback for the test path / direct ProducerConfig use.
	producer.shutdownTimeout = cfg.ShutdownTimeout
	if producer.shutdownTimeout <= 0 {
		producer.shutdownTimeout = defaultProducerShutdownTimeout
	}

	if cfg.FanoutWorkers > 0 {
		pm := newPoolMetrics(cfg.Registerer)
		producer.dlqDropped = pm.dlqDropped
		dlq := NewDLQPool(DLQConfig{
			MaxRetries: cfg.DLQMaxRetries,
			BaseDelay:  cfg.DLQBaseDelay,
			MaxDelay:   cfg.DLQMaxDelay,
			Workers:    cfg.DLQRetryWorkers,
			Namespace:  producer.topicNamespace,
		}, client, loggerOrNop(cfg.Logger), pm.dlqWriteFailed)
		fanout := NewFanoutPool(cfg.FanoutWorkers, cfg.FanoutQueueSize, client, dlq, loggerOrNop(cfg.Logger), pm.fanoutWriteFailed, pm.fanoutDropped)
		// Start DLQ workers before fan-out workers so a fan-out failure always has a live
		// DLQ consumer. Both are tracked by producer.wg; Close cancels ctx then waits.
		dlq.Start(producer.ctx, &producer.wg)
		fanout.Start(producer.ctx, &producer.wg)
		producer.fanout = fanout
	}

	if cfg.Logger != nil {
		cfg.Logger.Info().
			Strs("brokers", cfg.Brokers).
			Str("namespace", producer.topicNamespace).
			Str("client_id", clientID).
			Dur("circuit_breaker_timeout", cbTimeout).
			Msg("Kafka producer initialized")
	}

	return producer, nil
}

// Publish sends a client message to Kafka.
//
// The target topic(s) come from the tenant's per-tenant routing rules (resolveTargets) —
// there is NO convention/last-segment fallback. The record uses:
//   - Key: full channel string (for partitioning — same channel = same partition = ordering)
//   - Value: raw JSON from the client (no parsing/validation)
//   - Headers: metadata (client_id, source, timestamp)
//
// The channel must already be in internal format (tenant-prefixed) after gateway mapping.
//
// Callers branch on these sentinels (via errors.Is):
//   - backend.ErrNoMatchingRoute    — rules present, none match this channel (reject → 409;
//     wraps ErrPublishNotRoutable, so the check below it also fires)
//   - backend.ErrPublishNotRoutable — no rules provisioned / routing unavailable (reject → 409)
//   - ErrInvalidChannel             — malformed channel (→ 400)
//   - ErrServiceUnavailable         — rules not yet synced or breaker open (retryable → 503)
//   - other (e.g. ErrPublishFailed) — genuine produce failure (→ 500)
//
// A rule with no configured topics is treated as a non-match (skipped); if no rule matches,
// the publish is REJECTED (ErrNoMatchingRoute) — never silently dead-lettered (C1 revised,
// #179).
func (p *Producer) Publish(ctx context.Context, clientID int64, channel string, data []byte) (string, error) {
	closed := func() bool {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.closed
	}()
	if closed {
		return "", ErrProducerClosed
	}

	// Resolve routing BEFORE the circuit breaker. Rejects and the not-synced/unavailable
	// signal are pure in-memory decisions with no Kafka I/O — they MUST NOT count toward
	// breaker failure state (§IX: otherwise one rules-less tenant's retries open the shared
	// breaker and 503 every tenant). Only genuine produce attempts enter the breaker.
	tenant, err := extractTenant(channel)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidChannel, err)
	}
	topics, resolveErr := p.resolveTargets(channel, tenant)
	if resolveErr != nil {
		return "", resolveErr
	}
	// Multi-topic rule with no fan-out pool. Production always builds the pool
	// (FanoutWorkers >= 1, validated at startup), so this is the FanoutWorkers == 0 test-only
	// config (#179 P1b-C8). Reject HERE, before the breaker — like the other in-memory
	// decisions above, it has no Kafka I/O and MUST NOT count toward breaker failure state
	// (§IX: otherwise a tenant with a valid multi-topic rule opens the shared breaker and
	// 503s every tenant).
	if len(topics) > 1 && p.fanout == nil {
		p.stats.MessagesFailed.Add(1)
		if p.logger != nil {
			p.logger.Error().
				Str("channel", channel).
				Int("rule_topics", len(topics)).
				Msg("multi-topic routing rule requires the fanout pool (not configured)")
		}
		return "", fmt.Errorf("%w: multi-topic routing requires the fanout pool", backend.ErrPublishFailed)
	}

	// The mid rides the breaker's any-typed result: single-topic produces
	// return the coordinate-derived identity; multi-topic fan-out returns ""
	// (N records → N mids — the ack carries none, documented on the interface).
	cbResult, cbErr := p.circuitBreaker.Execute(func() (any, error) {
		return p.doProduce(ctx, clientID, channel, tenant, topics, data)
	})

	if errors.Is(cbErr, gobreaker.ErrOpenState) || errors.Is(cbErr, gobreaker.ErrTooManyRequests) {
		return "", fmt.Errorf("%w: kafka circuit breaker open", ErrServiceUnavailable)
	}

	if cbErr != nil {
		return "", fmt.Errorf("circuit breaker execute: %w", cbErr)
	}
	mid, _ := cbResult.(string)
	return mid, nil
}

// doProduce performs the actual Kafka produce for a pre-resolved plan (topics already
// computed by resolveTargets, guaranteed non-empty — no-match rejects before this point).
// It runs inside the circuit breaker, so only genuine produce failures count toward breaker
// state. On single-topic success it returns the stable message identity derived from the
// coordinates franz-go fills into the record on ack (kafkashared.MessageID) — the same
// derivation the consumer and replay apply, which is what makes the ack'd mid equal the
// delivered one. Fan-out success returns "".
func (p *Producer) doProduce(ctx context.Context, clientID int64, channel, tenant string, topics []string, data []byte) (string, error) {
	baseRecord := &kgo.Record{
		Key:   []byte(channel),
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: HeaderClientID, Value: []byte(strconv.FormatInt(clientID, 10))},
			{Key: kafkashared.HeaderSource, Value: []byte(SourceWSClient)},
			{Key: kafkashared.HeaderTimestamp, Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))},
			{Key: kafkashared.HeaderChannel, Value: []byte(channel)},
		},
	}

	// Single-topic: synchronous, error-propagating, breaker-protected — regardless of whether
	// a fanout pool exists (the pool is for multi-topic fan-out only). Multi-topic with a nil
	// pool is already rejected before the breaker in Publish, so here len(topics) > 1 implies
	// p.fanout != nil.
	if len(topics) == 1 {
		rec := *baseRecord // struct copy — independent from baseRecord
		rec.Topic = topics[0]
		results := p.client.ProduceSync(ctx, &rec)
		if err := results.FirstErr(); err != nil {
			p.stats.MessagesFailed.Add(1)
			if p.logger != nil {
				p.logger.Error().Err(err).Str("channel", channel).Str(LabelTopic, topics[0]).Msg("kafka produce failed")
			}
			return "", fmt.Errorf("kafka produce failed: %w", err)
		}
		mid := kafkashared.MessageID(rec.Topic, rec.Partition, rec.Offset)
		p.stats.MessagesPublished.Add(1)
		if p.logger != nil {
			p.logger.Debug().
				Int64("client_id", clientID).
				Str("channel", channel).
				Str(LabelTopic, topics[0]).
				Int("data_size", len(data)).
				Msg("Published message to Kafka")
		}
		return mid, nil
	}

	// Fan-out path: submit to multiple topics via the fanout pool. Reached only when
	// len(topics) > 1, which implies p.fanout != nil (nil-pool multi-topic is rejected in
	// Publish before the breaker).
	n := len(topics)
	successCh := make(chan string, n)
	failCh := make(chan string, n)

	var succeeded, failed []string
	for _, topic := range topics {
		job := fanoutJob{
			topic:     topic,
			record:    baseRecord,
			channel:   channel,
			tenant:    tenant,
			successCh: successCh,
			failCh:    failCh,
		}
		if !p.fanout.Submit(job) {
			// Queue full — count as immediate failure so aggregator gets N results.
			failCh <- topic
		}
	}

	// Collect exactly N results. Two cancel arms (#179 P1b-C3): the request ctx (client
	// disconnected mid-publish) and the producer ctx (Close() fired — a fanout worker may
	// have exited before draining all jobs, so we must not block forever). ErrProducerClosed
	// is breaker-neutral (isBreakerNeutralErr), so a shutdown mid-fanout does not trip the breaker.
	for range n {
		select {
		case t := <-successCh:
			succeeded = append(succeeded, t)
		case t := <-failCh:
			failed = append(failed, t)
		case <-ctx.Done():
			return "", fmt.Errorf("fanout canceled: %w", ctx.Err())
		case <-p.ctx.Done():
			return "", ErrProducerClosed
		}
	}

	if len(failed) > 0 {
		dlqTopic := kafkashared.BuildTopicName(p.topicNamespace, tenant, routing.DeadLetterTopicSuffix)
		dlqRec := &kgo.Record{
			Topic: dlqTopic,
			Key:   baseRecord.Key,
			Value: baseRecord.Value,
			Headers: append(slices.Clone(baseRecord.Headers),
				kgo.RecordHeader{Key: HeaderReason, Value: []byte(ReasonFanoutTopicWriteFailed)},
				kgo.RecordHeader{Key: HeaderFailedTopics, Value: []byte(strings.Join(failed, ","))},
				kgo.RecordHeader{Key: HeaderSucceededTopics, Value: []byte(strings.Join(succeeded, ","))},
			),
		}
		if p.fanout.dlq != nil {
			if !p.fanout.dlq.TrySubmit(dlqJob{record: dlqRec, tenant: tenant, reason: ReasonFanoutTopicWriteFailed}) {
				// DLQ queue full — the record is terminally lost (write never attempted).
				// Count it so this silent-loss leg is observable (#179 P1b-C4, §VII).
				if p.dlqDropped != nil {
					p.dlqDropped.WithLabelValues(tenant).Inc()
				}
				if p.logger != nil {
					p.logger.Warn().Str(logging.LogKeyTenantSlug, tenant).Msg("DLQ queue full — dropping failed fanout record")
				}
			}
		}
	}

	if len(succeeded) > 0 {
		// ≥1 target topic accepted the record → publish succeeds (P1b-C9). Failed topics were
		// best-effort dead-lettered above. No mid: N records → N identities.
		p.stats.MessagesPublished.Add(1)
		return "", nil
	}
	// Every fan-out topic write failed. Return an error so the client sees 500 rather than a
	// false 2xx for a fully-dropped message (§III / spec outcome table). Breaker-neutral
	// (errFanoutAllFailed) so it cannot 503 other tenants — see errFanoutAllFailed doc.
	p.stats.MessagesFailed.Add(1)
	return "", fmt.Errorf("%w: %d fan-out topics: %w", backend.ErrPublishFailed, n, errFanoutAllFailed)
}

// resolveTargets computes the produce plan for a channel from the tenant's routing rules.
// There is NO community fallback (deleted in #179) and NO silent dead-letter on no-match
// (C1 revised, #179): a channel with no applicable rule is REJECTED, never silently routed
// to a convention-derived or dead-letter topic.
//
// Outcomes (checked in this order — not-synced MUST precede tenant-absent so a degraded
// server returns a retryable 503, not a misleading 409):
//   - err != nil      → REJECT (ErrPublishNotRoutable / ErrNoMatchingRoute) or UNAVAILABLE
//     (ErrServiceUnavailable); do NOT produce
//   - topics (len>=1) → produce to these rule topic(s)
//
// Routing rules are ungated on every edition (ADR-0014); the per-edition rule COUNT is
// enforced at provisioning time, so publish-time routing needs no license check.
func (p *Producer) resolveTargets(channel, tenant string) (topics []string, err error) {
	if p.rulesProvider == nil {
		return nil, fmt.Errorf("%w: routing rules provider not configured", backend.ErrPublishNotRoutable)
	}
	if !p.rulesProvider.SnapshotReceived() {
		// The rules stream has not delivered its first snapshot — the server is degraded
		// (cold start / provisioning outage), not the tenant misconfigured. Retryable.
		return nil, fmt.Errorf("%w: routing rules not yet synced", ErrServiceUnavailable)
	}
	snap, ok := p.rulesProvider.GetRoutingSnapshot(tenant)
	if !ok || len(snap.Rules) == 0 {
		return nil, fmt.Errorf("%w: no routing rules provisioned for tenant", backend.ErrPublishNotRoutable)
	}

	for _, rule := range snap.Rules {
		matched, matchErr := routing.MatchRoutingPattern(rule.Pattern, channel)
		if matchErr != nil {
			// Patterns are validated at provisioning time, so a match error here means the
			// snapshot is corrupt or skewed — skip the rule (it can't route reliably) but
			// surface it: silently dropping a provisioned rule is an operator-invisible gap.
			if p.logger != nil {
				p.logger.Warn().Err(matchErr).
					Str(logging.LogKeyTenantSlug, tenant).
					Str("pattern", rule.Pattern).
					Str("channel", channel).
					Msg("routing rule pattern failed to match; skipping rule")
			}
			continue
		}
		if !matched {
			continue
		}
		if len(rule.Topics) == 0 {
			continue // zero-topic rule — treat as no-match (defense in depth §II)
		}
		// Dedupe: duplicate topics within one rule would double-deliver.
		fullTopics := make([]string, 0, len(rule.Topics))
		seen := make(map[string]struct{}, len(rule.Topics))
		for _, suffix := range rule.Topics {
			name := kafkashared.BuildTopicName(p.topicNamespace, tenant, suffix)
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			fullTopics = append(fullTopics, name)
		}
		return fullTopics, nil
	}
	// Rules present but none matched — REJECT (C1 revised, #179). The WS
	// handler emits protocol.ErrCodeNoMatchingRoute; the gateway maps it to 409. This is a
	// synchronous reject, NOT a silent dead-letter (§III/§XV): a rules-bearing tenant
	// publishing to an unmatched channel gets an immediate, actionable error.
	return nil, fmt.Errorf("%w for channel %q", backend.ErrNoMatchingRoute, channel)
}

// extractTenant returns the tenant ID from an internal channel (first segment).
// Channel format: {tenant}.{rest...} (minimum 2 parts).
func extractTenant(channel string) (string, error) {
	dot := strings.IndexByte(channel, '.')
	if dot <= 0 {
		return "", fmt.Errorf("channel must have at least 2 dot-separated parts, got %q", channel)
	}
	tenant := channel[:dot]
	if tenant == "" {
		return "", errors.New("tenant (first segment) cannot be empty")
	}
	return tenant, nil
}

// isBreakerNeutralErr reports whether a produce error MUST NOT count toward the shared
// circuit breaker (#179 P1b-C1). These are per-request conditions — tenant misconfiguration
// (a routing/topic problem that waiting will not fix) and shutdown — as opposed to genuine
// broker/transport unavailability, which the breaker exists to detect. Kept deliberately
// narrow: unknown-topic/authorization errors and ErrProducerClosed. Everything else defaults
// to breaker-eligible so real outages still trip the breaker.
func isBreakerNeutralErr(err error) bool {
	return errors.Is(err, ErrProducerClosed) ||
		errors.Is(err, errFanoutAllFailed) ||
		// Client disconnected mid-publish (request ctx canceled) — a per-request condition,
		// not broker unavailability. NOTE: context.DeadlineExceeded is deliberately NOT here —
		// a PublishTimeout expiry is a slow-broker signal the breaker SHOULD see.
		errors.Is(err, context.Canceled) ||
		errors.Is(err, kerr.UnknownTopicOrPartition) ||
		errors.Is(err, kerr.TopicAuthorizationFailed)
}

// waitWithTimeout waits for wg up to d, returning true if the group completed and false on
// timeout. Used by Close to bound the wait for pool workers that may be blocked in a produce
// on an unresponsive broker (§VII — wg.Wait must not hang shutdown indefinitely).
func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// loggerOrNop returns the pointed-to logger, or a no-op logger when nil, so pool
// constructors (which take zerolog.Logger by value) never dereference a nil *Logger.
func loggerOrNop(l *zerolog.Logger) zerolog.Logger {
	if l == nil {
		return zerolog.Nop()
	}
	return *l
}

// Stats returns a snapshot of the current producer statistics.
func (p *Producer) Stats() ProducerStatsSnapshot {
	return ProducerStatsSnapshot{
		MessagesPublished: p.stats.MessagesPublished.Load(),
		MessagesFailed:    p.stats.MessagesFailed.Load(),
	}
}

// ProducerStatsSnapshot is a plain snapshot of producer statistics (safe to copy).
type ProducerStatsSnapshot struct {
	MessagesPublished int64
	MessagesFailed    int64
}

// Namespace returns the topic namespace this producer uses.
func (p *Producer) Namespace() string {
	return p.topicNamespace
}

// CircuitBreakerState returns the current state of the circuit breaker.
func (p *Producer) CircuitBreakerState() gobreaker.State {
	return p.circuitBreaker.State()
}

// Close gracefully shuts down the producer.
// It flushes any buffered messages and closes the Kafka client.
func (p *Producer) Close() error {
	alreadyClosed := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed {
			return true
		}
		p.closed = true
		return false
	}()
	if alreadyClosed {
		return nil
	}

	// Records still buffered in the fan-out/DLQ queues at shutdown are intentionally NOT counted
	// in the _dropped_total metrics (#179 P1b-C4, de-scoped): a buffered fan-out job belongs to a
	// Publish that already returned ErrProducerClosed, and a buffered DLQ job is a best-effort
	// copy whose failure is already counted and whose message is durable in Kafka — no signal is
	// lost, so a bounded shutdown drain is unwarranted complexity (§XV).
	//
	// §VII shutdown ordering: cancel the producer ctx → wait for the fan-out/DLQ pool workers
	// to exit → close the client. The wait is BOUNDED (shutdownTimeout): franz-go ignores ctx
	// cancellation once a record is buffered, so a worker producing to an unresponsive broker
	// would block wg.Wait indefinitely. On timeout we close the client anyway — client.Close
	// aborts the in-flight produce, unblocking the workers — so shutdown never hangs. In-flight
	// Publish collect loops unblock via their p.ctx.Done() arm (P1b-C3); those are caller
	// goroutines, not tracked by p.wg.
	p.cancel()
	if !waitWithTimeout(&p.wg, p.shutdownTimeout) && p.logger != nil {
		p.logger.Warn().Dur("timeout", p.shutdownTimeout).
			Msg("kafka producer: fan-out/DLQ workers did not exit within shutdown timeout; force-closing client")
	}
	p.client.Close()

	if p.logger != nil {
		stats := p.Stats()
		p.logger.Info().
			Int64("messages_published", stats.MessagesPublished).
			Int64("messages_failed", stats.MessagesFailed).
			Msg("Kafka producer closed")
	}

	return nil
}
