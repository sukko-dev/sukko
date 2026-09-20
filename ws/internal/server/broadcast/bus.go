// Package broadcast provides an abstraction for inter-instance message broadcasting.
// It provides Valkey-based inter-instance message broadcasting.
//
// Usage:
//
//	bus, err := broadcast.NewBus(cfg, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer bus.Shutdown()
//
//	// Subscribe to a tenant's channel (safe to call at any time after construction)
//	ch, err := bus.Subscribe("tenant-id")
//
//	// Start receiving
//	bus.Run()
//
//	// Publish messages (fail-fast: the returned error tells the caller the
//	// message was NOT delivered; retry policy belongs to the caller)
//	if err := bus.Publish(&broadcast.Message{Subject: "BTC.trade", TenantID: "tenant-id", Payload: data}); err != nil {
//	    // handle: retry, drop-and-count, or surface to the client
//	}
//
//	// Unsubscribe when done
//	bus.Unsubscribe("tenant-id", ch)
package broadcast

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// Bus is the interface for inter-instance message broadcasting.
// Implementations must be safe for concurrent use from multiple goroutines.
//
// Lifecycle: Create with NewBus, call Run to start receiving, call Subscribe
// for each consumer (safe at any time after construction — before or after Run),
// and Shutdown to stop.
type Bus interface {
	// Publish sends a message to all pods subscribed to the tenant's channel.
	// Validates that msg.TenantID is non-empty and contains no reserved separator.
	//
	// Fail-fast: Publish never blocks on retries and never buffers. A non-nil
	// error means the message was NOT delivered to the backend — retry policy
	// is the caller's responsibility. ErrPublishUnavailable (wrapped with the
	// cause) marks the transient backend-down class and is the only error worth
	// retrying; ErrEmptyTenantID, ErrInvalidTenantID, and serialization errors
	// are permanent rejects. Errors are also logged internally and tracked via
	// metrics, so callers that deliberately drop need only count the drop.
	// Safe for concurrent use.
	Publish(msg *Message) error

	// Subscribe returns a new independent buffered channel for the given tenant.
	// Multiple callers with the same tenantID each receive their own channel and
	// each get a copy of every message for that tenant.
	// Safe to call at any time after construction — before or after Run().
	Subscribe(tenantID string) (<-chan *Message, error)

	// SubscribeAll returns a channel that receives messages for ALL tenants via
	// Valkey PSUBSCRIBE. Used exclusively by history/writer.go.
	// Buffer size = min(BROADCAST_BUFFER_SIZE × license.MaxTenants, BroadcastBufferSizeMax).
	SubscribeAll() (<-chan *Message, error)

	// Unsubscribe removes the subscriber entry for (tenantID, ch).
	// Returns ErrSubscriberNotFound if no matching entry exists.
	// The channel is NOT closed — the shard owns channel lifetime.
	Unsubscribe(tenantID string, ch <-chan *Message) error

	// UnsubscribeAll removes the SubscribeAll subscriber entry.
	// Issues PUNSUBSCRIBE when subscribeAllRefCount drops to 0.
	// Returns ErrSubscriberNotFound if the channel is not registered.
	UnsubscribeAll(ch <-chan *Message) error

	// Run starts the subscription management loop and health monitoring.
	// Returns immediately after launching background goroutines.
	Run()

	// Shutdown gracefully stops the bus with a default timeout.
	Shutdown()

	// ShutdownWithContext gracefully stops the bus using the provided context.
	ShutdownWithContext(ctx context.Context)

	// IsHealthy returns true if the bus connection is operational.
	IsHealthy() bool

	// GetMetrics returns current bus metrics for monitoring.
	GetMetrics() Metrics
}

// Message represents a broadcast message sent between instances.
type Message struct {
	// Subject is the routing key (e.g., "BTC.trade")
	Subject string `json:"subject"`

	// Payload is the raw message data (typically JSON)
	Payload []byte `json:"payload"`

	// TenantID identifies the tenant that owns this message.
	// Required: Publish rejects messages with empty or invalid TenantID.
	TenantID string `json:"tenant_id,omitempty"`

	// Pos is the encoded Kafka position "(partition+1)-offset" used as the XADD stream ID
	// and as the durable replay cursor returned to clients in HistoryMessageEnvelope.Pos.
	Pos string `json:"pos,omitempty"`

	// Mid is the stable message identity (kafka.MessageID on the kafka backend,
	// a UUID on the direct backend) — carried pod-to-pod so every instance fans
	// out the same identity. omitempty keeps the bus JSON rolling-deploy safe.
	Mid string `json:"mid,omitempty"`

	// Channel is the bare channel name (distinct from the namespace-qualified Subject).
	// Required for per-channel stream key routing; populated by the Kafka consumer.
	Channel string `json:"channel,omitempty"`
}

// Metrics contains operational metrics for the broadcast bus.
type Metrics struct {
	// Type identifies the backend ("valkey")
	Type string `json:"type"`

	// Healthy indicates if the backend connection is operational:
	// PublishHealthy AND SubscriptionsConverged (ADR-0016)
	Healthy bool `json:"healthy"`

	// PublishHealthy indicates the publish connection is operational
	// (last PUBLISH / health-check ping succeeded)
	PublishHealthy bool `json:"publish_healthy"`

	// SubscriptionsConverged indicates the backend subscriptions this pod has
	// established match what its subscribers require. False means some
	// broadcasts published elsewhere are not being received here — a DEGRADED
	// condition, never a readiness/liveness failure (ADR-0016).
	SubscriptionsConverged bool `json:"subscriptions_converged"`

	// SubscriptionsDesired is the number of backend subscriptions this pod wants
	// (active tenant channels + the all-tenant pattern subscription when in use)
	SubscriptionsDesired int `json:"subscriptions_desired"`

	// SubscriptionsEstablished is the number of backend subscriptions confirmed
	// on the current pub/sub connection
	SubscriptionsEstablished int `json:"subscriptions_established"`

	// ChannelPrefix is the Valkey pub/sub channel name prefix (full channel = prefix:tenantID)
	ChannelPrefix string `json:"channel_prefix"`

	// Subscribers is the total number of local subscriber channels (all tenants + SubscribeAll)
	Subscribers int `json:"subscribers"`

	// PublishErrors is the count of failed publish attempts
	PublishErrors uint64 `json:"publish_errors"`

	// MessagesReceived is the count of messages received from backend
	MessagesReceived uint64 `json:"messages_received"`

	// LastPublishAgo is seconds since last successful publish (-1 if never)
	LastPublishAgo float64 `json:"last_publish_ago"`

	// LastPublishTime is the time of the last successful publish
	LastPublishTime time.Time `json:"last_publish_time,omitzero"`
}

// Config holds configuration for creating a Bus.
type Config struct {
	// Type selects the backend: "valkey"
	Type string

	// BufferSize is the per-tenant subscriber channel buffer capacity.
	BufferSize int

	// Limits contains edition license limits used for SubscribeAll buffer sizing.
	// Set from editionManager.Limits() in main.go after license initialization.
	Limits license.Limits

	// ShutdownTimeout is the maximum time to wait for graceful shutdown
	ShutdownTimeout time.Duration

	// Registerer overrides the Prometheus registry; nil uses the default
	// registerer (production). Tests pass an isolated registry so multiple bus
	// instances in one binary do not collide on registration (same pattern as
	// kafka.ConsumerConfig.Registerer, §XVIII).
	Registerer prometheus.Registerer

	// Valkey-specific configuration
	Valkey ValkeyConfig
}

// ValkeyConfig holds Valkey/Redis-specific configuration.
type ValkeyConfig struct {
	// Addrs is the list of Valkey addresses.
	// Single address: direct connection mode
	// Multiple addresses: Sentinel failover mode
	Addrs []string

	// MasterName is the Sentinel master name (default: "mymaster")
	MasterName string

	// Password for authentication
	Password string

	// DB is the database number (default: 0)
	DB int

	// Channel is the pub/sub channel name prefix (default: "ws.broadcast")
	Channel string

	// Timeouts
	WriteTimeout time.Duration // Timeout for write operations (maps to valkey-go ConnWriteTimeout)

	// Publish
	PublishTimeout time.Duration // Timeout for publish operations

	// Startup
	StartupPingTimeout time.Duration // Timeout for initial connectivity check

	// Reconnection
	ReconnectInitialBackoff time.Duration // Initial backoff for reconnection attempts
	ReconnectMaxBackoff     time.Duration // Maximum backoff for reconnection attempts
	ReconnectMaxAttempts    int           // Maximum number of reconnection attempts

	// Health checks
	HealthCheckInterval time.Duration // Interval between health checks
	HealthCheckTimeout  time.Duration // Timeout for each health check

	// Staleness detection
	PublishStalenessThreshold time.Duration // Log warning if no publish within this window

	// Recovery gate (ADR-0019): how long a zero-subscriber PUBLISH is held for
	// retry after a subscribe-connection disruption before it is flushed under
	// plain pub/sub semantics. Bounds the consume-loop block so a genuinely
	// empty channel cannot livelock.
	ZeroSubscriberWindow time.Duration

	// TLS for managed Valkey/Redis services (ElastiCache, Memorystore, Upstash, etc.)
	TLSEnabled  bool
	TLSInsecure bool   // Skip TLS verification (not for production)
	TLSCAPath   string // Custom CA certificate path
}
