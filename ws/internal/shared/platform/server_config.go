// Package platform provides environment configuration and container resource
// detection. It handles loading configuration from environment variables and
// detecting cgroup-based resource limits for containers.
//
// Key features:
//   - Environment variable parsing with defaults via caarlos0/env
//   - Optional .env file loading via godotenv
//   - Cgroup v1/v2 CPU and memory limit detection
//   - Container-aware GOMAXPROCS (Go 1.25+ runtime)
package platform

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/alerting"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// WebSocket ping/pong validation constants.
// These define the minimum acceptable values to prevent misconfiguration.
const (
	// MinPongWait is the minimum allowed timeout for receiving a pong response.
	// Values under 10s cause spurious disconnects because the ping must traverse
	// the full proxy chain (8 hops in local dev: client → gateway → ws-server →
	// shard and back) within this window.
	MinPongWait = 10 * time.Second

	// MinPingPeriod is the minimum allowed interval for sending pings.
	// Values under 5s create excessive ping traffic (>20% of connection
	// lifetime becomes ping/pong overhead) and provide no reliability benefit.
	MinPingPeriod = 5 * time.Second

	// MinAutoCommitInterval is the minimum allowed Kafka auto-commit interval.
	// Franz-go enforces this floor internally; pre-validating here produces a
	// clear env-var-named error per Constitution §I.
	MinAutoCommitInterval = 100 * time.Millisecond

	// MsgCommitOnRevokeTimeoutWarning is emitted by LoadServerConfig when
	// KAFKA_COMMIT_ON_REVOKE_TIMEOUT exceeds KafkaSessionTimeout/3.
	// Defined here (not in kafka/constants.go) because platform MUST NOT import kafka.
	MsgCommitOnRevokeTimeoutWarning = "KAFKA_COMMIT_ON_REVOKE_TIMEOUT exceeds recommended threshold relative to KAFKA_SESSION_TIMEOUT; large values may cause rebalance delays in multi-pod deployments"

	// MsgHistoryValkeyMemoryWarning is emitted at startup when WS_HISTORY_ENABLED=true.
	// Valkey OOM is silent until writes start failing; this gives operators a sizing
	// estimate before load hits. Assumes 1KB average message size.
	MsgHistoryValkeyMemoryWarning = "WS_HISTORY_ENABLED=true: size Valkey maxmemory for channels × WS_HISTORY_BUFFER_DEPTH × avg_msg_size × tenants; ensure Valkey maxmemory-policy=allkeys-lru and maxmemory < container memory limit"

	// MsgRegistryValkeyMemoryInfo is emitted at startup when WS_CONNECTIONS_REGISTRY_ENABLED=true.
	// Mirrors MsgHistoryValkeyMemoryWarning — both data features warn about Valkey footprint at startup.
	// Assumes 200 channels × 32 bytes per channel name per connection (MaxRegistryChannelsPerConnection).
	MsgRegistryValkeyMemoryInfo = "WS_CONNECTIONS_REGISTRY_ENABLED=true: size Valkey maxmemory for active_connections × MaxRegistryChannelsPerConnection × avg_channel_name_size; ensure maxmemory-policy=allkeys-lru and maxmemory < container memory limit"

	// BroadcastType constants for BROADCAST_TYPE env var.
	BroadcastTypeValkey = "valkey"

	// History writer bound constants.
	// Defined here (not in history/) to avoid import cycle: history → platform is fine,
	// but platform MUST NOT import history; these bounds are used in platform.Validate().
	MaxHistoryBufferDepth             = 10000
	MaxHistoryMaxLimit                = 1000
	MaxHistoryWriterBuffer            = 50000
	MaxHistoryPipelineBatch           = 500
	MaxHistoryTTL                     = 90 * 24 * time.Hour
	MaxHistoryDeliveryTimeout         = 5 * time.Minute
	MaxHistoryWriterRestartMaxBackoff = 5 * time.Minute
	MaxHistoryWriterLockTTL           = 60 * time.Second
	MaxChannelsPerClientLimit         = 1000
	HistoryWriterLockRenewDivisor     = 3
	HistoryKafkaFetchMultiplier       = 3
	MinHistoryWriterHeartbeatInterval = 500 * time.Millisecond
	MinHistoryWriterLockTTL           = 1 * time.Second

	// BroadcastBufferSizeMax is the upper bound for BROADCAST_BUFFER_SIZE.
	// Prevents unbounded per-tenant memory allocation:
	// worst-case = MaxTenants × NumShards × BroadcastBufferSizeMax × 8 bytes per pointer.
	BroadcastBufferSizeMax = 65536

	// MaxGapNotifyBufferSize is the upper bound for WS_GAP_NOTIFY_BUFFER_SIZE.
	// Gap notifications are low-frequency out-of-band events; a small ceiling caps per-client
	// goroutine stack pressure. Drops beyond this limit are counted via GapDropReasonBufferFull.
	MaxGapNotifyBufferSize = 64

	// MaxReplayMessagesLimit is the upper bound for WS_MAX_REPLAY_MESSAGES.
	// Caps per-goroutine memory use during live replay: each replayed message is buffered
	// in memory before streaming; an uncapped value lets an operator accidentally cause
	// unbounded heap growth with a single config change.
	MaxReplayMessagesLimit = 10_000

	// logFieldChannelPrefix is the zerolog field key for the Valkey channel prefix.
	// Named "prefix" because ValkeyChannel is now a prefix; the full channel is "{prefix}:{tenantID}".
	logFieldChannelPrefix = "valkey_channel_prefix"

	// Registry (connections) bound constants.
	// Defined here (not in registry/) to avoid import cycle: registry → platform is fine,
	// but platform MUST NOT import registry; these bounds are used in platform.Validate().
	RegistryValkeyDB = 0 // Valkey database index for all registry and provisioning clients

	MinConnectionsRegistryRestartInitialBackoff = 10 * time.Millisecond
	MaxConnectionsRegistryRestartInitialBackoff = 60 * time.Second
	// MinConnectionsRegistryRestartMaxBackoff = 10ms — equal to the initial backoff floor.
	// Named separately (not shared with MinConnectionsRegistryRestartInitialBackoff) to make
	// the lower bound on MaxBackoff explicit and enforceable via named constant.
	MinConnectionsRegistryRestartMaxBackoff = 10 * time.Millisecond
	MaxConnectionsRegistryRestartMaxBackoff = 5 * time.Minute
	MinConnectionsRegistryFlushInterval     = 50 * time.Millisecond
	MaxConnectionsRegistryFlushInterval     = 10 * time.Second
	MinConnectionsRegistryHeartbeatInterval = 5 * time.Second
	MaxConnectionsRegistryHeartbeatInterval = 5 * time.Minute
	MinConnectionsRegistryTTL               = 1 * time.Second
	MaxConnectionsRegistryTTL               = 3600 * time.Second
	RegistryHeartbeatTTLDivisor             = 3
	RegistryStalenessDivisor                = 2
	MaxRegistryChannelsPerConnection        = 200
	MinConnectionsRegistryBuffer            = 1
	MaxConnectionsRegistryBuffer            = 8192
	MinRegistryWriterShutdownDrainTimeout   = 0 * time.Second // 0 = skip drain entirely (useful for tests)
	MaxRegistryWriterShutdownDrainTimeout   = 10 * time.Second
	RetryAfterRegistryUnavailable           = 30 * time.Second // used as int seconds in HTTP Retry-After header
	MinAdminChannelSubscribeTimeout         = 1 * time.Second
	MaxAdminChannelSubscribeTimeout         = 60 * time.Second

	// DefaultConnectionsRegistryHeartbeatInterval is the default value of
	// WS_CONNECTIONS_REGISTRY_HEARTBEAT_INTERVAL. Used by provisioning reader to compute
	// stale-connection thresholds without hardcoding a magic number.
	DefaultConnectionsRegistryHeartbeatInterval = 30 * time.Second
)

// ServerConfig holds all server configuration
// Tags:
//
//	env: Environment variable name
//	envDefault: Default value if not set
//	required: Must be provided (no default)
type ServerConfig struct {
	BaseConfig
	MessageBackendConfig
	ProvisioningClientConfig
	KafkaNamespaceConfig
	HTTPTimeoutConfig
	AnalyticsConfig
	PodIdentityConfig

	// Server basics
	Addr      string `env:"WS_ADDR" envDefault:":3002"`         // HTTP listen address for the WebSocket load balancer.
	NumShards int    `env:"WS_NUM_SHARDS" envDefault:"1"`       // Number of independent connection-routing shards. Higher values reduce lock contention; increase WS_BASE_PORT range accordingly.
	BasePort  int    `env:"WS_BASE_PORT" envDefault:"3002"`     // Base port for shard binding (3002, 3003, ...)
	LBAddr    string `env:"WS_LB_ADDR" envDefault:":3005"`      // Load balancer listen address
	GRPCPort  int    `env:"SERVER_GRPC_PORT" envDefault:"3006"` // gRPC server port for RealtimeService (SSE Subscribe + REST Publish)

	// KafkaConsumerEnabled controls whether the Kafka consumer pool is started.
	// When false, the consumer pool is not created — no consumer group join, no message
	// consumption. WebSocket connections still work (connection-only mode for loadtesting).
	// Default: true
	KafkaConsumerEnabled bool `env:"KAFKA_CONSUMER_ENABLED" envDefault:"true"` // When false, the Kafka consumer pool is disabled and no messages are consumed from Kafka topics. Useful for debug deployments or when using an alternative message backend.

	// Kafka Topic Defaults (for on-demand topic creation by KafkaBackend)
	KafkaDefaultPartitions        int `env:"KAFKA_DEFAULT_PARTITIONS" envDefault:"1"`         // Default partition count for Kafka topics created on demand by ws-server.
	KafkaDefaultReplicationFactor int `env:"KAFKA_DEFAULT_REPLICATION_FACTOR" envDefault:"1"` // Default replication factor for Kafka topics created on demand by ws-server.

	// Resource limits
	// Note: CPU limit is detected automatically by Go runtime reading cgroup (Go 1.25+)
	// WS_CPU_LIMIT env var is only used by Docker to set the container limit
	MemoryLimit int64 `env:"WS_MEMORY_LIMIT" envDefault:"536870912"` // Memory limit in bytes. New connections are rejected (HTTP 503) when exceeded. Default: 512MB.

	// Capacity
	MaxConnections int `env:"WS_MAX_CONNECTIONS" envDefault:"500"` // Maximum simultaneous WebSocket client connections per pod. New connections are rejected with HTTP 503 when this limit is reached.

	// Rate limiting
	MaxKafkaMessagesPerSec   int `env:"WS_MAX_KAFKA_RATE" envDefault:"1000"`           // Maximum Kafka messages per second the server consumes across all topics; exceeding it paces (backpressures) consumption rather than dropping, so sustained overload becomes consumer lag, not message loss.
	MaxBroadcastsPerSec      int `env:"WS_MAX_BROADCAST_RATE" envDefault:"25"`         // Maximum Valkey broadcast fan-out rate in messages per second. Messages above this rate are dropped and counted in ws_broadcast_bus_dropped_total.
	MaxGoroutines            int `env:"WS_MAX_GOROUTINES" envDefault:"100000"`         // Maximum number of goroutines allowed per pod. New connections are rejected when this limit is reached.
	RateLimitBurstMultiplier int `env:"WS_RATE_LIMIT_BURST_MULTIPLIER" envDefault:"2"` // Burst capacity as a multiple of the steady-state rate

	// Per-client message rate limiting (token bucket algorithm)
	//
	// Controls how fast each WebSocket client can send messages.
	// Uses a token bucket: burst allows instant requests, rate is the sustained limit.
	//
	// Industry comparison:
	//   - Coinbase: 10 orders/second
	//   - Binance: 10 orders/second per symbol, 100/sec total
	//   - Interactive Brokers: 50 orders/second
	ClientMsgBurstLimit int     `env:"WS_CLIENT_MSG_BURST_LIMIT" envDefault:"100"` // Per-client inbound message burst capacity (token bucket). Clients exceeding this are disconnected.
	ClientMsgRatePerSec float64 `env:"WS_CLIENT_MSG_RATE_PER_SEC" envDefault:"10"` // Per-client sustained rate/sec

	// Connection rate limiting (DoS protection)
	ConnectionRateLimitEnabled bool    `env:"CONN_RATE_LIMIT_ENABLED" envDefault:"true"`     // When true, connection rate limiting is enforced per source IP and globally to protect against connection flood attacks. Controlled by CONN_RATE_LIMIT_IP_RATE, CONN_RATE_LIMIT_IP_BURST, CONN_RATE_LIMIT_GLOBAL_RATE, and CONN_RATE_LIMIT_GLOBAL_BURST.
	ConnRateLimitIPBurst       int     `env:"CONN_RATE_LIMIT_IP_BURST" envDefault:"100"`     // Burst allowance for per-source-IP connection rate limiting.
	ConnRateLimitIPRate        float64 `env:"CONN_RATE_LIMIT_IP_RATE" envDefault:"100.0"`    // Sustained new-connection rate limit per source IP address, in connections per second.
	ConnRateLimitGlobalBurst   int     `env:"CONN_RATE_LIMIT_GLOBAL_BURST" envDefault:"300"` // Burst allowance for the global (all-IP combined) new-connection rate limit.
	ConnRateLimitGlobalRate    float64 `env:"CONN_RATE_LIMIT_GLOBAL_RATE" envDefault:"50.0"` // Sustained new-connection rate limit across all source IPs combined, in connections per second.

	// CPU Safety Thresholds (Container-Aware) with Hysteresis
	//
	// These thresholds are relative to CONTAINER CPU ALLOCATION, not host CPU.
	// The system uses container-aware cgroup measurement when running in Docker/K8s.
	//
	// Example with 1.0 CPU allocation (docker: cpus: "1.0"):
	//   - 75% = reject when using 0.75 of allocated 1.0 CPU
	//   - Container can use up to 100% before being throttled by cgroup
	//
	// Example with 4.0 CPU allocation (docker: cpus: "4.0"):
	//   - 75% = reject when using 3.0 of allocated 4.0 CPUs
	//   - Container can use up to 400% (4.0 cores) before throttling
	//
	// In non-containerized environments, falls back to host CPU percentage.
	//
	// Hysteresis prevents rapid oscillation when CPU hovers near thresholds.
	// Uses two thresholds: upper (to enter state) and lower (to exit state).
	//
	// Example with default values (reject: 60% upper, 50% lower):
	//   - CPU rises to 61% → start rejecting connections
	//   - CPU drops to 55% → still rejecting (in deadband)
	//   - CPU drops to 49% → stop rejecting, accept connections
	//   - CPU rises to 55% → still accepting (in deadband)
	//
	// The 10% gap is the "hysteresis band" that provides stability.
	//
	CPURejectThreshold      float64 `env:"WS_CPU_REJECT_THRESHOLD" envDefault:"60.0"`    // CPU percentage above which new connections are rejected with HTTP 503. Hysteresis: stop rejecting below WS_CPU_REJECT_THRESHOLD_LOWER.
	CPURejectThresholdLower float64 `env:"WS_CPU_REJECT_THRESHOLD_LOWER" envDefault:"0"` // CPU percentage below which connection rejection stops (hysteresis lower bound). 0 = auto: WS_CPU_REJECT_THRESHOLD minus 10.
	CPUPauseThreshold       float64 `env:"WS_CPU_PAUSE_THRESHOLD" envDefault:"70.0"`     // CPU percentage above which Kafka consumption is paused. Hysteresis: resume below WS_CPU_PAUSE_THRESHOLD_LOWER.
	CPUPauseThresholdLower  float64 `env:"WS_CPU_PAUSE_THRESHOLD_LOWER" envDefault:"0"`  // CPU percentage below which the Kafka consumer pauses end (hysteresis lower bound). 0 = auto: WS_CPU_PAUSE_THRESHOLD minus 10.
	CPUEWMABeta             float64 `env:"WS_CPU_EWMA_BETA" envDefault:"0.8"`            // EWMA decay factor for CPU smoothing (0-1, higher = smoother)

	// TCP/Network Tuning (Burst Tolerance)
	//
	// These settings improve tolerance to connection bursts by increasing buffers and timeouts.
	// Defaults are conservative - increase for high-burst workloads.
	//
	// REVERT: Set TCP_LISTEN_BACKLOG=0 to disable custom backlog (use Go defaults)
	//         Set HTTP_*_TIMEOUT to lower values if needed
	//
	TCPListenBacklog int `env:"TCP_LISTEN_BACKLOG" envDefault:"2048"` // TCP accept queue size (0 = Go default ~128)

	// Monitoring
	MetricsInterval time.Duration `env:"METRICS_INTERVAL" envDefault:"15s"` // Prometheus metrics collection interval for system-level metrics (CPU usage, memory, connection counts).

	// CPU Polling Interval (for protection decisions)
	// Separate from MetricsInterval to allow faster CPU spike detection
	// while keeping full metrics reporting (memory, goroutines) at a reasonable interval.
	//
	// CPU polling is used by:
	// - ShouldPauseKafka() - backpressure when CPU > 80%
	// - ShouldAcceptConnection() - reject connections when CPU > 75%
	//
	// Trade-off: 1s polling = 0.1% CPU overhead, but 15x faster spike detection
	CPUPollInterval time.Duration `env:"CPU_POLL_INTERVAL" envDefault:"1s"` // Interval for CPU usage sampling used by backpressure and connection rejection decisions. Lower values detect spikes faster at marginally higher overhead.

	// Alerting
	AlertEnabled         bool          `env:"ALERT_ENABLED" envDefault:"false"`           // When true, ws-server sends operational alerts to the configured destinations (ALERT_SLACK_WEBHOOK_URL, ALERT_CONSOLE_ENABLED).
	AlertMinLevel        string        `env:"ALERT_MIN_LEVEL" envDefault:"WARNING"`       // Minimum alert severity to send. One of: DEBUG, INFO, WARNING, ERROR, CRITICAL.
	AlertSlackWebhookURL string        `env:"ALERT_SLACK_WEBHOOK_URL" redact:"true"`      // Slack incoming webhook URL for alert delivery.
	AlertSlackChannel    string        `env:"ALERT_SLACK_CHANNEL"`                        // Slack channel to post alerts to (e.g. #ops-alerts).
	AlertSlackUsername   string        `env:"ALERT_SLACK_USERNAME" envDefault:"AlertBot"` // Display name for the Slack alert bot.
	AlertSlackTimeout    time.Duration `env:"ALERT_SLACK_TIMEOUT" envDefault:"5s"`        // HTTP request timeout for each Slack webhook delivery attempt.
	AlertRateLimitWindow time.Duration `env:"ALERT_RATE_LIMIT_WINDOW" envDefault:"5m"`    // Window duration for alert rate limiting. At most ALERT_RATE_LIMIT_MAX alerts are sent per window.
	AlertRateLimitMax    int           `env:"ALERT_RATE_LIMIT_MAX" envDefault:"3"`        // Maximum alerts sent per ALERT_RATE_LIMIT_WINDOW. Excess alerts are suppressed to prevent notification floods.
	AlertConsoleEnabled  bool          `env:"ALERT_CONSOLE_ENABLED" envDefault:"false"`   // When true, alerts are written to the structured log output in addition to any remote destinations.

	// Valkey Configuration (for BroadcastBus when BROADCAST_TYPE=valkey)
	// Supports both self-hosted Sentinel (3 addresses) and single instance (1 address)
	ValkeyAddrs      []string `env:"VALKEY_ADDRS" envSeparator:"," envDefault:"localhost:6379"` // Valkey server addresses, comma-separated. For Sentinel high-availability, provide all Sentinel addresses (e.g. sentinel1:26379,sentinel2:26379,sentinel3:26379). For single-instance, provide one address.
	ValkeyMasterName string   `env:"VALKEY_MASTER_NAME" envDefault:"mymaster"`                  // Valkey Sentinel master name. Only used when VALKEY_ADDRS contains Sentinel addresses.
	ValkeyPassword   string   `env:"VALKEY_PASSWORD" redact:"true"`                             // Valkey authentication password.
	ValkeyDB         int      `env:"VALKEY_DB" envDefault:"0"`                                  // Valkey database index. Use 0 (the default) unless your deployment requires database isolation.
	ValkeyChannel    string   `env:"VALKEY_CHANNEL" envDefault:"ws.broadcast"`                  // Valkey pub/sub channel prefix. The full per-tenant channel is {VALKEY_CHANNEL}:{tenantID} (e.g. ws.broadcast:acme-corp). All ws-server pods must use the same prefix. If Valkey ACLs restrict pub/sub by channel pattern, update patterns from ws.broadcast to ws.broadcast:*.

	// Slow Client Detection
	//
	// When a client's send buffer is full, the server tracks consecutive failed send attempts.
	// After SlowClientMaxAttempts consecutive failures, the client is disconnected.
	//
	// Industry comparison:
	//   - Coinbase: 2 attempts (aggressive)
	//   - Binance: No automatic disconnect (relies on ping timeout)
	//   - FIX protocol: 5 second timeout (lenient)
	//
	// Default: 3 (balanced - tolerates brief hiccups, disconnects persistent slow clients)
	// Range: 1-10 (1 = aggressive, 10 = very lenient)
	SlowClientMaxAttempts int `env:"WS_SLOW_CLIENT_MAX_ATTEMPTS" envDefault:"3"` // Number of consecutive failed send attempts before a slow client is disconnected. A send attempt fails when the client's outbound buffer is full.

	// Client Send Buffer Size
	// Controls the per-client send channel buffer (memory vs slow-client tolerance trade-off)
	//
	// Buffer sizing at 125 msg/sec broadcast rate (25 msg/sec × 5 channel subscriptions):
	// - 512 slots: ~256KB/client, 4.1s buffer, ~3.5GB heap at 13.5K clients
	// - 1024 slots: ~512KB/client, 8.2s buffer, ~6.9GB heap at 13.5K clients
	//
	// Trade-off:
	// - Smaller buffer = less memory = less GC pressure = lower CPU spikes
	// - Larger buffer = more tolerance for slow clients and network hiccups
	//
	// Default: 512 (reduced from 1024 to cut heap size by 50%, reduce GC pressure)
	// Production guidance: Start with 512, increase to 768 or 1024 if cascade disconnects occur
	ClientSendBufferSize int `env:"WS_CLIENT_SEND_BUFFER_SIZE" envDefault:"512"` // Per-client outbound message buffer size in messages. Larger buffers tolerate temporary client slowness at the cost of higher memory usage per connection.

	// GapNotifyBufferSize is the per-client buffer for low-priority gap notifications.
	// Must be 1-MaxGapNotifyBufferSize (64); 0 would make the channel unbuffered (blocks Broadcast hot path).
	// Keep small relative to ClientSendBufferSize — gaps are low-frequency out-of-band events.
	// Drops beyond capacity are counted via ws_gap_notifications_dropped_total{reason="buffer_full"}.
	GapNotifyBufferSize int `env:"WS_GAP_NOTIFY_BUFFER_SIZE" envDefault:"8"` // Per-client buffer for gap notification messages. Gap notifications are low-priority; this buffer absorbs bursts without blocking the main message pipeline.

	// ReplayRateLimitInterval is the minimum time between live replay requests
	// per (client, channel) pair. Prevents unbounded Kafka reads per client.
	ReplayRateLimitInterval time.Duration `env:"WS_REPLAY_RATE_LIMIT_INTERVAL" envDefault:"10s"` // Minimum interval between history replay requests per client per channel. Prevents replay storms from a single connection.

	// Broadcast Bus Configuration
	//
	// BroadcastType: Backend for inter-instance messaging ("valkey")
	// - valkey: Redis-compatible Pub/Sub (default, ~1-2ms latency, required for message history)
	BroadcastType string `env:"BROADCAST_TYPE" envDefault:"valkey"` // Backend for inter-instance message broadcasting. Only valkey is supported — Valkey pub/sub is required for all ws-server deployments.

	// Valkey Broadcast Bus TLS (for managed Valkey/Redis: ElastiCache, Memorystore, Upstash, etc.)
	ValkeyTLSEnabled  bool   `env:"VALKEY_TLS_ENABLED" envDefault:"false"`  // When true, Valkey connections use TLS encryption. Required for managed Valkey/Redis services (ElastiCache, Memorystore, Upstash).
	ValkeyTLSInsecure bool   `env:"VALKEY_TLS_INSECURE" envDefault:"false"` // When true, TLS certificate verification is skipped for Valkey connections. Do not use in production.
	ValkeyTLSCAPath   string `env:"VALKEY_TLS_CA_PATH"`                     // Path to a custom CA certificate file for Valkey TLS connections. Required when Valkey uses a private certificate authority.

	// TopicRefreshInterval is how often to re-sync tenant topics from the registry.
	// Safety net — primary updates come from the gRPC stream.
	TopicRefreshInterval time.Duration `env:"TOPIC_REFRESH_INTERVAL" envDefault:"60s"` // How often ws-server re-syncs the tenant topic list from the provisioning registry. This is a safety net; the primary update path is event-driven.

	// WebSocket Ping/Pong Configuration
	//
	// These settings control the keep-alive mechanism between the shard and clients.
	// In a proxied architecture (gateway → ws-server → shard), pings travel through
	// multiple hops, requiring a larger buffer than direct connections.
	//
	// Buffer = PongWait - PingPeriod
	// The buffer must accommodate the round-trip time through all proxies.
	// With 8 network hops (local Kind + port-forward), 15+ seconds is recommended.
	//
	// Example with defaults (60s/45s):
	//   - Shard sends ping at t=0
	//   - Ping travels: Shard → ShardProxy → ws-server → Gateway → Client
	//   - Client sends pong
	//   - Pong travels: Client → Gateway → ws-server → ShardProxy → Shard
	//   - If pong arrives by t=60s, connection is healthy
	//   - 15 second buffer for network latency, GC pauses, etc.
	//
	// Common issues:
	//   - Buffer too small (<5s): Connections drop during GC pauses or network hiccups
	//   - PingPeriod >= PongWait: Invalid, ping would always timeout
	//
	PongWait   time.Duration `env:"WS_PONG_WAIT" envDefault:"60s"`   // Timeout for receiving a pong response after sending a ping. Clients that do not respond within this window are disconnected. Must be greater than WS_PING_PERIOD.
	PingPeriod time.Duration `env:"WS_PING_PERIOD" envDefault:"45s"` // Interval between WebSocket ping frames sent to connected clients. Must be less than WS_PONG_WAIT.

	// WriteWait is the timeout for WebSocket write operations.
	// 5s is sufficient for local writes; network latency is handled by TCP.
	WriteWait time.Duration `env:"WS_WRITE_WAIT" envDefault:"5s"` // Timeout for a single WebSocket write operation. Increase for high-latency networks.

	// Handler timeouts
	ReplayTimeout     time.Duration `env:"WS_REPLAY_TIMEOUT" envDefault:"5s"`       // Timeout for a single history replay handler to complete.
	PublishTimeout    time.Duration `env:"WS_PUBLISH_TIMEOUT" envDefault:"5s"`      // Timeout for a client publish operation (client to gateway to ws-server to Kafka).
	MaxReplayMessages int           `env:"WS_MAX_REPLAY_MESSAGES" envDefault:"100"` // Maximum number of messages returned per history replay request.

	// Topic/backend timeouts
	TopicCreationTimeout time.Duration `env:"WS_TOPIC_CREATION_TIMEOUT" envDefault:"30s"` // Timeout for on-demand Kafka topic creation.

	// Orchestration
	ShardDialTimeout           time.Duration `env:"WS_SHARD_DIAL_TIMEOUT" envDefault:"10s"`          // Timeout for the load balancer to dial a shard when proxying a new connection.
	ShardMessageTimeout        time.Duration `env:"WS_SHARD_MESSAGE_TIMEOUT" envDefault:"60s"`       // Timeout for forwarding a single message between the load balancer and a shard.
	MetricsAggregationInterval time.Duration `env:"WS_METRICS_AGGREGATION_INTERVAL" envDefault:"5s"` // Interval for aggregating per-shard metrics into cluster-level summaries.

	// Shutdown
	ShutdownGracePeriod   time.Duration `env:"WS_SHUTDOWN_GRACE_PERIOD" envDefault:"30s"`  // Maximum time ws-server waits for in-flight connections and consumers to drain during graceful shutdown.
	ShutdownCheckInterval time.Duration `env:"WS_SHUTDOWN_CHECK_INTERVAL" envDefault:"1s"` // Polling interval for checking whether all connections have closed during graceful shutdown.

	// Internal monitoring
	MetricsCollectInterval      time.Duration `env:"WS_METRICS_COLLECT_INTERVAL" envDefault:"2s"`       // Interval for collecting internal runtime metrics (goroutine counts, buffer saturation).
	MemoryMonitorInterval       time.Duration `env:"WS_MEMORY_MONITOR_INTERVAL" envDefault:"30s"`       // Interval for polling memory usage against WS_MEMORY_LIMIT.
	MemoryWarningPercent        int           `env:"WS_MEMORY_WARNING_PERCENT" envDefault:"80"`         // Memory usage percentage of WS_MEMORY_LIMIT at which a warning log is emitted.
	MemoryCriticalPercent       int           `env:"WS_MEMORY_CRITICAL_PERCENT" envDefault:"90"`        // Memory usage percentage of WS_MEMORY_LIMIT at which new connections are rejected.
	BufferSampleInterval        time.Duration `env:"WS_BUFFER_SAMPLE_INTERVAL" envDefault:"10s"`        // Interval for sampling client send buffer saturation metrics.
	BufferMaxSamples            int           `env:"WS_BUFFER_MAX_SAMPLES" envDefault:"100"`            // Rolling window size in samples for buffer saturation statistics.
	BufferHighSaturationPercent int           `env:"WS_BUFFER_HIGH_SATURATION_PERCENT" envDefault:"90"` // Per-client send buffer fill percentage above which the client is considered saturated.
	BufferPopulationWarnPercent int           `env:"WS_BUFFER_POPULATION_WARN_PERCENT" envDefault:"25"` // Percentage of clients considered saturated above which a warning alert is triggered.

	// Connection rate limiter internals
	ConnRateLimitIPTTL           time.Duration `env:"CONN_RATE_LIMIT_IP_TTL" envDefault:"5m"`           // How long per-IP connection rate limit state is retained after the last connection attempt from that IP.
	ConnRateLimitCleanupInterval time.Duration `env:"CONN_RATE_LIMIT_CLEANUP_INTERVAL" envDefault:"1m"` // Interval for purging expired per-IP rate limit state.

	// Broadcast bus
	BroadcastBufferSize      int           `env:"BROADCAST_BUFFER_SIZE" envDefault:"256"`     // Per-tenant per-shard broadcast message buffer size. When this buffer is full, messages for that tenant are dropped and counted in ws_broadcast_bus_dropped_total. Maximum: 65,536 (BROADCAST_BUFFER_SIZE_MAX); values above this cause a startup failure. Operators running high-throughput single-tenant workloads should increase this value.
	BroadcastShutdownTimeout time.Duration `env:"BROADCAST_SHUTDOWN_TIMEOUT" envDefault:"5s"` // Timeout for draining the broadcast bus during graceful shutdown.

	// Valkey broadcast tuning
	ValkeyWriteTimeout              time.Duration `env:"VALKEY_WRITE_TIMEOUT" envDefault:"3s"`                // Timeout for Valkey write operations (SET, EXPIRE) from the broadcast bus.
	ValkeyPublishTimeout            time.Duration `env:"VALKEY_PUBLISH_TIMEOUT" envDefault:"100ms"`           // Timeout for each Valkey PUBLISH command in the broadcast hot path.
	ValkeyStartupPingTimeout        time.Duration `env:"VALKEY_STARTUP_PING_TIMEOUT" envDefault:"5s"`         // Timeout for the Valkey connectivity check during ws-server startup.
	ValkeyReconnectInitialBackoff   time.Duration `env:"VALKEY_RECONNECT_INITIAL_BACKOFF" envDefault:"100ms"` // Starting backoff delay for Valkey reconnection attempts after a connection failure.
	ValkeyReconnectMaxBackoff       time.Duration `env:"VALKEY_RECONNECT_MAX_BACKOFF" envDefault:"30s"`       // Maximum backoff delay between Valkey reconnection attempts.
	ValkeyReconnectMaxAttempts      int           `env:"VALKEY_RECONNECT_MAX_ATTEMPTS" envDefault:"10"`       // Maximum number of Valkey reconnection attempts before the broadcast bus is marked unhealthy.
	ValkeyHealthCheckInterval       time.Duration `env:"VALKEY_HEALTH_CHECK_INTERVAL" envDefault:"10s"`       // Interval for periodic Valkey health checks.
	ValkeyHealthCheckTimeout        time.Duration `env:"VALKEY_HEALTH_CHECK_TIMEOUT" envDefault:"5s"`         // Timeout for each Valkey health check PING.
	ValkeyPublishStalenessThreshold time.Duration `env:"VALKEY_PUBLISH_STALENESS_THRESHOLD" envDefault:"60s"` // Staleness window for the Valkey broadcast publisher — a warning is logged if no message is published within this interval (indicates silent consumer failures).
	ValkeyZeroSubscriberWindow      time.Duration `env:"VALKEY_ZERO_SUBSCRIBER_WINDOW" envDefault:"30s"`      // How long a broadcast that reaches zero subscribers is held for retry after a Valkey pub/sub disruption before being flushed — bounds consume-loop backpressure so a genuinely empty channel cannot block ingestion indefinitely.

	// Kafka consumer tuning
	KafkaBatchSize    int           `env:"KAFKA_BATCH_SIZE" envDefault:"50"`      // Maximum number of Kafka messages processed per consumer batch.
	KafkaBatchTimeout time.Duration `env:"KAFKA_BATCH_TIMEOUT" envDefault:"10ms"` // Maximum time to wait to fill a batch before processing it.

	// KafkaMetadataMinAge caps how quickly the producer refreshes topic metadata: a message published to a
	// just-provisioned tenant topic cannot be produced until the client re-fetches metadata, so lower values
	// shorten new-tenant produce latency at the cost of more metadata traffic. Set to 0 to use the franz-go default.
	KafkaMetadataMinAge time.Duration `env:"KAFKA_METADATA_MIN_AGE" envDefault:"5s"` // Minimum interval before the Kafka producer refreshes topic metadata (lower = faster new-topic produce, more metadata traffic).

	// Kafka consumer transport tuning
	KafkaFetchMaxWait              time.Duration `env:"KAFKA_FETCH_MAX_WAIT" envDefault:"500ms"`              // Maximum time the Kafka broker waits before returning a fetch response, even if KAFKA_FETCH_MIN_BYTES has not been met. Lower values reduce latency; higher values improve throughput.
	KafkaFetchMinBytes             int32         `env:"KAFKA_FETCH_MIN_BYTES" envDefault:"1"`                 // Minimum bytes a broker should accumulate before returning a fetch response.
	KafkaFetchMaxBytes             int32         `env:"KAFKA_FETCH_MAX_BYTES" envDefault:"10485760"`          // Maximum bytes returned per fetch request.
	KafkaSessionTimeout            time.Duration `env:"KAFKA_SESSION_TIMEOUT" envDefault:"30s"`               // Kafka consumer group session timeout. If the consumer does not heartbeat within this window, it is declared dead and a rebalance is triggered. Must be within the broker's configured range.
	KafkaRebalanceTimeout          time.Duration `env:"KAFKA_REBALANCE_TIMEOUT" envDefault:"60s"`             // Maximum time allowed for a consumer group rebalance to complete. Must be greater than KAFKA_COMMIT_ON_REVOKE_TIMEOUT.
	KafkaReplayFetchMaxBytes       int32         `env:"KAFKA_REPLAY_FETCH_MAX_BYTES" envDefault:"5242880"`    // Maximum bytes per fetch during history replay. Kept separate from KAFKA_FETCH_MAX_BYTES to prevent replay reads from saturating the live consumer path.
	KafkaBackpressureCheckInterval time.Duration `env:"KAFKA_BACKPRESSURE_CHECK_INTERVAL" envDefault:"100ms"` // Polling interval for the Kafka backpressure circuit breaker (CPU and goroutine pressure check).

	// KafkaCommitOnRevokeTimeout controls how long OnPartitionsRevoked waits for
	// CommitMarkedOffsets to complete before returning. Must be > 0 and < KafkaRebalanceTimeout.
	KafkaCommitOnRevokeTimeout time.Duration `env:"KAFKA_COMMIT_ON_REVOKE_TIMEOUT" envDefault:"5s"` // How long ws-server waits to commit processed Kafka offsets during a consumer group rebalance (triggered on rolling deploys, pod restarts, or scale events). If the commit completes within this window, duplicate delivery to clients by the new consumer is minimized. If the commit times out, offsets since the last background auto-commit (see KAFKA_AUTO_COMMIT_INTERVAL) may be re-delivered.

	// KafkaAutoCommitInterval controls the background auto-commit interval.
	// Must be >= MinAutoCommitInterval (100ms). Franz-go enforces this floor;
	// pre-validation here produces a clear error per Constitution §I.
	KafkaAutoCommitInterval time.Duration `env:"KAFKA_AUTO_COMMIT_INTERVAL" envDefault:"5s"` // Interval between background Kafka offset commits. Controls the crash-path duplicate window: if a pod crashes without a clean shutdown, messages processed in the final KAFKA_AUTO_COMMIT_INTERVAL before the crash may be re-delivered after failover. Lower values reduce the crash-path window at the cost of more frequent broker commits. Minimum: 100ms — values below this cause a startup validation error.

	// Kafka producer tuning
	KafkaProducerBatchMaxBytes      int           `env:"KAFKA_PRODUCER_BATCH_MAX_BYTES" envDefault:"1048576"`    // Maximum bytes per Kafka producer batch.
	KafkaProducerMaxBufferedRecords int           `env:"KAFKA_PRODUCER_MAX_BUFFERED_RECORDS" envDefault:"10000"` // Maximum number of records buffered in the Kafka producer before backpressure is applied to callers.
	KafkaProducerRecordRetries      int           `env:"KAFKA_PRODUCER_RECORD_RETRIES" envDefault:"8"`           // Number of retries for failed Kafka produce operations before the record is discarded.
	KafkaProducerCBTimeout          time.Duration `env:"KAFKA_PRODUCER_CB_TIMEOUT" envDefault:"30s"`             // Circuit breaker observation window for Kafka producer failures.
	KafkaProducerCBMaxFailures      int           `env:"KAFKA_PRODUCER_CB_MAX_FAILURES" envDefault:"5"`          // Maximum producer failures within the circuit breaker window before the producer circuit opens (pauses).
	KafkaProducerCBHalfOpenReqs     int           `env:"KAFKA_PRODUCER_CB_HALF_OPEN_REQS" envDefault:"1"`        // Number of test requests permitted when the Kafka producer circuit breaker is in half-open state.
	KafkaProducerShutdownTimeout    time.Duration `env:"KAFKA_PRODUCER_SHUTDOWN_TIMEOUT" envDefault:"10s"`       // Maximum time to wait for in-flight Kafka fan-out/DLQ writes to finish during shutdown before force-closing the producer connection.

	// Kafka multi-topic produce fan-out worker pool
	RoutingFanoutWorkers   int `env:"WS_ROUTING_FANOUT_WORKERS"    envDefault:"4"`   // Number of parallel workers that produce a record to each Kafka topic when a routing rule fans a channel out to multiple topics.
	RoutingFanoutQueueSize int `env:"WS_ROUTING_FANOUT_QUEUE_SIZE" envDefault:"256"` // Queue depth for the Kafka multi-topic fan-out worker pool; when full, the overflowing topic write is routed to the dead-letter path.

	// Dead-letter topic retry pool (retries failed fan-out topic writes)
	DLQMaxRetries   int           `env:"WS_ROUTING_DLQ_MAX_RETRIES"   envDefault:"3"`     // Maximum retry attempts for a failed dead-letter topic write before the record is discarded.
	DLQBaseDelay    time.Duration `env:"WS_ROUTING_DLQ_BASE_DELAY"    envDefault:"100ms"` // Initial backoff delay before retrying a failed dead-letter topic write (exponential backoff starting point).
	DLQMaxDelay     time.Duration `env:"WS_ROUTING_DLQ_MAX_DELAY"     envDefault:"5s"`    // Maximum backoff delay cap for dead-letter topic write retries.
	DLQRetryWorkers int           `env:"WS_ROUTING_DLQ_RETRY_WORKERS" envDefault:"4"`     // Number of parallel workers retrying failed dead-letter topic writes.

	// PodID is the resolved pod identity, populated by Normalize() from PodIdentityConfig.PodID().
	// Not an env var — configure via PodIdentityConfig (SUKKO_POD_ID / WS_POD_ID).
	// Kept as a plain string so hot-path code (history writer, registry) avoids repeated method calls.
	PodID string `env:"-"`

	// MaxChannelsPerClient limits the number of concurrent channel subscriptions per client.
	// Prevents channel-fan-out attacks. Validated against MaxChannelsPerClientLimit (1000).
	MaxChannelsPerClient int `env:"WS_MAX_CHANNELS_PER_CLIENT" envDefault:"100"` // Maximum concurrent channel subscriptions per client connection. Subscription requests beyond this limit are rejected.

	// Message History (two-tier: Valkey Streams ring buffer + Kafka fallback)
	HistoryEnabled bool `env:"WS_HISTORY_ENABLED" envDefault:"false"` // When true, message history is retained in a Valkey Streams ring buffer with Kafka as a fallback for deeper replay. Enables client history replay requests.

	// HistoryBufferDepth is the Valkey Streams MAXLEN ~ per-channel (XADD MAXLEN ~).
	HistoryBufferDepth int `env:"WS_HISTORY_BUFFER_DEPTH" envDefault:"1000"` // Maximum number of messages retained per channel in the Valkey Streams ring buffer (XADD MAXLEN ~). Older messages are evicted when the limit is reached.

	// HistoryTTL is the per-stream key expiry (EXPIRE) applied on every write.
	HistoryTTL time.Duration `env:"WS_HISTORY_TTL" envDefault:"24h"` // Per-stream Valkey key expiry. Streams inactive longer than this duration are deleted to free memory.

	// HistoryWriterBuffer is the historyWriter workChan capacity (messages in-flight before drops).
	HistoryWriterBuffer int `env:"WS_HISTORY_WRITER_BUFFER" envDefault:"10000"` // Internal message queue depth for the history writer. Messages are dropped when this buffer is full.

	// HistoryMaxLimit is the maximum number of entries a client may request per history call.
	HistoryMaxLimit int `env:"WS_HISTORY_MAX_LIMIT" envDefault:"100"` // Maximum number of messages a client may request in a single history replay call.

	// HistoryDeliveryTimeout is the per-request deadline for XREVRANGE + Kafka fallback.
	HistoryDeliveryTimeout time.Duration `env:"WS_HISTORY_DELIVERY_TIMEOUT" envDefault:"30s"` // Deadline for completing a single history replay request, including Valkey XREVRANGE and optional Kafka fallback reads.

	// HistoryWriterPipelineBatch is the max number of XADD commands per DoMulti pipeline.
	HistoryWriterPipelineBatch int `env:"WS_HISTORY_WRITER_PIPELINE_BATCH" envDefault:"50"` // Maximum number of Valkey XADD commands per pipeline batch for history writes.

	// HistoryWriterLockTTL is the distributed lock TTL for the active historyWriter pod.
	HistoryWriterLockTTL time.Duration `env:"WS_HISTORY_WRITER_LOCK_TTL" envDefault:"10s"` // TTL for the distributed history writer lock in Valkey. The active writer pod renews this lock on each heartbeat interval.

	// HistoryWriterLockTTLMs is pre-computed from HistoryWriterLockTTL in Normalize().
	// Used by Valkey SET PX (milliseconds). Not an env var.
	HistoryWriterLockTTLMs int64 `env:"-"`

	// HistoryWriterRestartInitialBackoff is the starting delay before re-entering runOnce after a failure.
	HistoryWriterRestartInitialBackoff time.Duration `env:"WS_HISTORY_WRITER_RESTART_INITIAL_BACKOFF" envDefault:"500ms"` // Initial delay before restarting the history writer after a failure.

	// HistoryWriterRestartMaxBackoff caps the exponential backoff for historyWriter restarts.
	HistoryWriterRestartMaxBackoff time.Duration `env:"WS_HISTORY_WRITER_RESTART_MAX_BACKOFF" envDefault:"30s"` // Maximum delay between history writer restart attempts.

	// HistoryValkeyCmdTimeout is the per-command timeout for history Valkey operations (XADD, XREVRANGE, EXPIRE).
	HistoryValkeyCmdTimeout time.Duration `env:"WS_HISTORY_VALKEY_CMD_TIMEOUT" envDefault:"2s"` // Per-command timeout for history Valkey operations (XADD, XREVRANGE, EXPIRE).

	// HistoryMaxConsecutiveLockFailures is the number of consecutive heartbeat lock failures
	// before the historyWriter exits runOnce and re-enters the supervised restart loop.
	HistoryMaxConsecutiveLockFailures int `env:"WS_HISTORY_MAX_CONSECUTIVE_LOCK_FAILURES" envDefault:"3"` // Number of consecutive heartbeat lock renewal failures before the history writer steps down and triggers a restart.

	// HistoryWriterHeartbeatInterval is the ticker interval for lock renewal and bus health checks.
	HistoryWriterHeartbeatInterval time.Duration `env:"WS_HISTORY_WRITER_HEARTBEAT_INTERVAL" envDefault:"3s"` // Interval for history writer lock renewal and broadcast bus health checks.

	// Connections Registry (Connections Management API — Pro edition)
	// When false, registryWriter and adminListeners are skipped entirely. X-Sukko-Tenant-ID is
	// still read unconditionally (tenantHooks and channel subscription require it regardless).
	ConnectionsRegistryEnabled bool `env:"WS_CONNECTIONS_REGISTRY_ENABLED" envDefault:"false"` // When true, ws-server tracks active connections in Valkey and exposes them via the Connections Management API (Pro edition). Requires Valkey memory headroom proportional to active connections x subscriptions x average channel name length.

	// ConnectionsRegistryTTL is the Valkey EXPIRE duration for each connection hash.
	// Heartbeats renew the TTL; if a pod dies without heartbeat, entries expire after this.
	ConnectionsRegistryTTL time.Duration `env:"WS_CONNECTIONS_REGISTRY_TTL" envDefault:"120s"` // Valkey EXPIRE duration for each connection entry in the registry. Heartbeats renew the TTL; connections that fail to heartbeat within this window are treated as expired.

	// ConnectionsRegistryBuffer is the internal channel buffer for registry events
	// (connect, disconnect, subscribe, unsubscribe, heartbeat).
	ConnectionsRegistryBuffer int `env:"WS_CONNECTIONS_REGISTRY_BUFFER" envDefault:"1024"` // Internal channel buffer for registry events (connect, disconnect, subscribe). Events are dropped when this buffer is full.

	// ConnectionsRegistryFlushInterval is the tick interval for flushing buffered events to Valkey.
	ConnectionsRegistryFlushInterval time.Duration `env:"WS_CONNECTIONS_REGISTRY_FLUSH_INTERVAL" envDefault:"5s"` // Interval for flushing buffered registry events to Valkey.

	// ConnectionsRegistryHeartbeatInterval is how often the registry writer renews TTLs and writes health keys.
	ConnectionsRegistryHeartbeatInterval time.Duration `env:"WS_CONNECTIONS_REGISTRY_HEARTBEAT_INTERVAL" envDefault:"30s"` // Interval for renewing active connection TTLs in the registry.

	// ConnectionsRegistryRestartInitialBackoff is the initial restart delay after a registryWriter failure.
	ConnectionsRegistryRestartInitialBackoff time.Duration `env:"WS_CONNECTIONS_REGISTRY_RESTART_INITIAL_BACKOFF" envDefault:"100ms"` // Initial delay before restarting the registry writer after a failure.

	// ConnectionsRegistryRestartMaxBackoff is the maximum restart delay after repeated failures.
	ConnectionsRegistryRestartMaxBackoff time.Duration `env:"WS_CONNECTIONS_REGISTRY_RESTART_MAX_BACKOFF" envDefault:"30s"` // Maximum delay between registry writer restart attempts.

	// ConnectionsRegistryShutdownDrainTimeout is how long to wait draining buffered events on shutdown.
	// 0 = skip drain entirely (useful for tests).
	ConnectionsRegistryShutdownDrainTimeout time.Duration `env:"WS_CONNECTIONS_REGISTRY_SHUTDOWN_DRAIN_TIMEOUT" envDefault:"100ms"` // Time allowed to flush buffered registry events during graceful shutdown. Set to 0 to skip draining.

	// AdminChannelSubscribeTimeout is the timeout for each shard's synchronous SUBSCRIBE call at startup.
	AdminChannelSubscribeTimeout time.Duration `env:"WS_ADMIN_CHANNEL_SUBSCRIBE_TIMEOUT" envDefault:"10s"` // Timeout for each shard's SUBSCRIBE call to the admin broadcast channel at startup.

	// InternalSecretEnabled controls whether ws-server validates X-Sukko-Internal-Secret.
	// When true, WS_INTERNAL_SECRET must be non-empty or startup fails.
	InternalSecretEnabled bool `env:"WS_INTERNAL_SECRET_ENABLED" envDefault:"false"` // When true, ws-server validates the X-Sukko-Internal-Secret header on all incoming connections, rejecting any that bypass the gateway.

	// InternalSecret is the shared secret forwarded by the gateway as X-Sukko-Internal-Secret.
	// redact:"true" prevents the /config endpoint from exposing this value.
	InternalSecret string `env:"WS_INTERNAL_SECRET" redact:"true"` // Shared secret validated as X-Sukko-Internal-Secret on incoming connections. Must match the value configured on the gateway. Required when WS_INTERNAL_SECRET_ENABLED is true.

	// editionManager holds the license-resolved edition and limits.
	// Set by LoadServerConfig() before Validate(). Not an env var — derived from SUKKO_LICENSE_KEY.
	editionManager *license.Manager
}

// EditionManager returns the license manager for this config.
// Used by cmd/server/main.go to access edition at startup and pass to service constructors.
func (c *ServerConfig) EditionManager() *license.Manager {
	return c.editionManager
}

// LoadServerConfig reads server configuration from .env file and environment variables
// Priority: ENV vars > .env file > defaults
func LoadServerConfig(logger zerolog.Logger) (*ServerConfig, error) {
	// Load .env file (optional - OK if it doesn't exist)
	// In production (Docker), we use environment variables directly
	// In development, .env file provides convenience
	if err := godotenv.Load(); err != nil {
		// Only log, don't fail - we can run without .env file
		logger.Info().Msg("No .env file found (using environment variables only)")
	} else {
		logger.Info().Msg("Loaded configuration from .env file")
	}

	cfg := &ServerConfig{}

	// Parse environment variables into struct
	// This validates types and applies defaults
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Create license manager before validation — Validate() uses edition gates.
	mgr, err := license.NewManager(cfg.LicenseKey, logger)
	if err != nil {
		return nil, fmt.Errorf("license: %w", err)
	}
	cfg.editionManager = mgr

	// Normalize derived fields before validation
	cfg.Normalize()

	// Validation (now edition-aware)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	logger.Info().Msg("Configuration loaded and validated successfully")

	// Warn if CommitOnRevokeTimeout may cause rebalance delay pressure in large clusters.
	if cfg.KafkaCommitOnRevokeTimeout > cfg.KafkaSessionTimeout/3 {
		logger.Warn().
			Dur("commit_on_revoke_timeout", cfg.KafkaCommitOnRevokeTimeout).
			Dur("session_timeout", cfg.KafkaSessionTimeout).
			Msg(MsgCommitOnRevokeTimeoutWarning)
	}

	// Warn on estimated Valkey memory pressure from history.
	// Gives operators a concrete sizing estimate at startup so they can right-size
	// Valkey maxmemory before load hits. Assumes 1KB average message size.
	if cfg.HistoryEnabled {
		logger.Warn().
			Int("history_buffer_depth", cfg.HistoryBufferDepth).
			Dur("history_ttl", cfg.HistoryTTL).
			Str("valkey_memory_formula", "channels × WS_HISTORY_BUFFER_DEPTH × avg_msg_size × tenants").
			Str("example_1k_msg_100ch_10tenants", fmt.Sprintf("~%dMB",
				cfg.HistoryBufferDepth*100*10*1024/(1024*1024))).
			Msg(MsgHistoryValkeyMemoryWarning)
	}

	// Inform operators of estimated Valkey memory footprint from the connections registry.
	// Assumes MaxRegistryChannelsPerConnection channels × ~30 bytes per channel name.
	if cfg.ConnectionsRegistryEnabled {
		logger.Info().
			Int("registry_ttl_seconds", int(cfg.ConnectionsRegistryTTL.Seconds())).
			Int("max_channels_per_connection", MaxRegistryChannelsPerConnection).
			Str("example_10k_connections", fmt.Sprintf("~%dMB",
				10000*MaxRegistryChannelsPerConnection*30/(1024*1024)+10)).
			Msg(MsgRegistryValkeyMemoryInfo)
	}

	return cfg, nil
}

// Normalize computes derived configuration values from user-provided settings.
// It MUST be called before Validate(). LoadServerConfig calls this automatically.
//
// Currently handles:
//   - Canonicalizing KAFKA_TOPIC_NAMESPACE (trim + lowercase) via KafkaNamespaceConfig.Normalize().
//   - Auto-computing CPU hysteresis lower thresholds (upper - 10) when not set explicitly (0 = auto).
func (c *ServerConfig) Normalize() {
	// Canonicalize the explicit topic namespace (trim + lowercase) before Validate() so the
	// allowlist check and every BuildTopicName consumer read the same value. Called here (inside
	// the outer Normalize) rather than as a bare embedded call, which the method promotion shadows.
	c.KafkaNamespaceConfig.Normalize()

	if c.CPURejectThresholdLower == 0 {
		c.CPURejectThresholdLower = c.CPURejectThreshold - 10.0
	}
	if c.CPUPauseThresholdLower == 0 {
		c.CPUPauseThresholdLower = c.CPUPauseThreshold - 10.0
	}

	// Pre-compute lock TTL in milliseconds for Valkey SET PX (int64 required).
	c.HistoryWriterLockTTLMs = c.HistoryWriterLockTTL.Milliseconds()

	// Resolve and cache pod identity — used by hot-path code (history writer, registry).
	c.PodID = c.PodIdentityConfig.PodID()
}

// Validate checks server configuration for errors.
// Normalize() MUST be called before Validate() to compute derived fields.
func (c *ServerConfig) Validate() error {
	// Required fields (no sensible defaults)
	if c.Addr == "" {
		return errors.New("WS_ADDR is required")
	}

	// Resource limit checks
	if c.MemoryLimit < MinMemoryLimit {
		return fmt.Errorf("WS_MEMORY_LIMIT must be >= %d (64MB), got %d", MinMemoryLimit, c.MemoryLimit)
	}

	// Shard configuration checks
	if c.NumShards < 1 {
		return fmt.Errorf("WS_NUM_SHARDS must be > 0, got %d", c.NumShards)
	}
	if c.BasePort < 1 || c.BasePort > MaxPort {
		return fmt.Errorf("WS_BASE_PORT must be 1-%d, got %d", MaxPort, c.BasePort)
	}
	if maxPort := c.BasePort + c.NumShards - 1; maxPort > MaxPort {
		return fmt.Errorf("WS_BASE_PORT (%d) + WS_NUM_SHARDS (%d) exceeds max port %d (last port would be %d)", c.BasePort, c.NumShards, MaxPort, maxPort)
	}
	if c.LBAddr == "" {
		return errors.New("WS_LB_ADDR must not be empty")
	}
	if c.GRPCPort < 1 || c.GRPCPort > MaxPort {
		return fmt.Errorf("SERVER_GRPC_PORT must be 1-%d, got %d", MaxPort, c.GRPCPort)
	}

	// Range checks
	if c.MaxConnections < 1 {
		return fmt.Errorf("WS_MAX_CONNECTIONS must be > 0, got %d", c.MaxConnections)
	}
	if c.MaxKafkaMessagesPerSec < 1 {
		return fmt.Errorf("WS_MAX_KAFKA_RATE must be > 0, got %d", c.MaxKafkaMessagesPerSec)
	}
	if c.MaxBroadcastsPerSec < 1 {
		return fmt.Errorf("WS_MAX_BROADCAST_RATE must be > 0, got %d", c.MaxBroadcastsPerSec)
	}
	if c.MaxGoroutines < MinMaxGoroutines {
		return fmt.Errorf("WS_MAX_GOROUTINES must be >= %d, got %d", MinMaxGoroutines, c.MaxGoroutines)
	}
	if c.RateLimitBurstMultiplier < 1 {
		return fmt.Errorf("WS_RATE_LIMIT_BURST_MULTIPLIER must be >= 1, got %d", c.RateLimitBurstMultiplier)
	}
	if c.CPURejectThreshold < 0 || c.CPURejectThreshold > 100 {
		return fmt.Errorf("WS_CPU_REJECT_THRESHOLD must be 0-100, got %.1f", c.CPURejectThreshold)
	}
	if c.CPURejectThresholdLower < 0 || c.CPURejectThresholdLower > 100 {
		return fmt.Errorf("WS_CPU_REJECT_THRESHOLD_LOWER must be 0-100, got %.1f", c.CPURejectThresholdLower)
	}
	if c.CPUPauseThreshold < 0 || c.CPUPauseThreshold > 100 {
		return fmt.Errorf("WS_CPU_PAUSE_THRESHOLD must be 0-100, got %.1f", c.CPUPauseThreshold)
	}
	if c.CPUPauseThresholdLower < 0 || c.CPUPauseThresholdLower > 100 {
		return fmt.Errorf("WS_CPU_PAUSE_THRESHOLD_LOWER must be 0-100, got %.1f", c.CPUPauseThresholdLower)
	}
	if c.CPUEWMABeta <= 0 || c.CPUEWMABeta >= 1 {
		return fmt.Errorf("WS_CPU_EWMA_BETA must be between 0 and 1 exclusive, got %.2f", c.CPUEWMABeta)
	}

	// Post-Normalize() range validation (Normalize computes lower from upper - 10 when 0)
	if c.CPURejectThresholdLower < 0 {
		return fmt.Errorf("[CONFIG ERROR] auto-computed WS_CPU_REJECT_THRESHOLD_LOWER is negative (%.1f); set WS_CPU_REJECT_THRESHOLD >= 10 or set lower explicitly", c.CPURejectThresholdLower)
	}
	if c.CPUPauseThresholdLower < 0 {
		return fmt.Errorf("[CONFIG ERROR] auto-computed WS_CPU_PAUSE_THRESHOLD_LOWER is negative (%.1f); set WS_CPU_PAUSE_THRESHOLD >= 10 or set lower explicitly", c.CPUPauseThresholdLower)
	}

	// Logical checks
	if c.CPUPauseThreshold < c.CPURejectThreshold {
		return fmt.Errorf("WS_CPU_PAUSE_THRESHOLD (%.1f) must be >= WS_CPU_REJECT_THRESHOLD (%.1f)",
			c.CPUPauseThreshold, c.CPURejectThreshold)
	}

	// Hysteresis validation: lower must be < upper
	if c.CPURejectThresholdLower >= c.CPURejectThreshold {
		return fmt.Errorf("WS_CPU_REJECT_THRESHOLD_LOWER (%.1f) must be < WS_CPU_REJECT_THRESHOLD (%.1f)",
			c.CPURejectThresholdLower, c.CPURejectThreshold)
	}
	if c.CPUPauseThresholdLower >= c.CPUPauseThreshold {
		return fmt.Errorf("WS_CPU_PAUSE_THRESHOLD_LOWER (%.1f) must be < WS_CPU_PAUSE_THRESHOLD (%.1f)",
			c.CPUPauseThresholdLower, c.CPUPauseThreshold)
	}

	// Connection rate limit validation (conditional on enabled)
	if c.ConnectionRateLimitEnabled {
		if c.ConnRateLimitIPBurst < 1 {
			return fmt.Errorf("CONN_RATE_LIMIT_IP_BURST must be > 0 when rate limiting enabled, got %d", c.ConnRateLimitIPBurst)
		}
		if c.ConnRateLimitIPRate <= 0 {
			return fmt.Errorf("CONN_RATE_LIMIT_IP_RATE must be > 0 when rate limiting enabled, got %f", c.ConnRateLimitIPRate)
		}
		if c.ConnRateLimitGlobalBurst < 1 {
			return fmt.Errorf("CONN_RATE_LIMIT_GLOBAL_BURST must be > 0 when rate limiting enabled, got %d", c.ConnRateLimitGlobalBurst)
		}
		if c.ConnRateLimitGlobalRate <= 0 {
			return fmt.Errorf("CONN_RATE_LIMIT_GLOBAL_RATE must be > 0 when rate limiting enabled, got %f", c.ConnRateLimitGlobalRate)
		}
	}

	// TCP listen backlog validation
	if c.TCPListenBacklog < 0 {
		return fmt.Errorf("TCP_LISTEN_BACKLOG must be >= 0, got %d", c.TCPListenBacklog)
	}

	// HTTP timeout validation
	if err := c.HTTPTimeoutConfig.Validate(); err != nil {
		return err
	}

	// Metrics interval validation
	if c.MetricsInterval < time.Second {
		return fmt.Errorf("METRICS_INTERVAL must be >= 1s, got %v", c.MetricsInterval)
	}

	// Shared field validation (LogLevel, LogFormat, Environment)
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := c.AnalyticsConfig.Validate(); err != nil {
		return err
	}

	// Broadcast bus configuration validation
	if c.BroadcastType != BroadcastTypeValkey {
		return fmt.Errorf("[CONFIG ERROR] BROADCAST_TYPE=%q is invalid (valid: %s)", c.BroadcastType, BroadcastTypeValkey)
	}

	// Valkey-specific validation
	if len(c.ValkeyAddrs) == 0 || !slices.ContainsFunc(c.ValkeyAddrs, func(s string) bool { return strings.TrimSpace(s) != "" }) {
		return fmt.Errorf("VALKEY_ADDRS is required when BROADCAST_TYPE=%s (must contain at least one non-empty address)", c.BroadcastType)
	}
	if c.ValkeyDB < 0 {
		return fmt.Errorf("VALKEY_DB must be >= 0, got %d", c.ValkeyDB)
	}
	if c.ValkeyChannel == "" {
		return errors.New("VALKEY_CHANNEL cannot be empty")
	}

	// Slow client threshold validation
	if c.SlowClientMaxAttempts < 1 || c.SlowClientMaxAttempts > 10 {
		return fmt.Errorf("WS_SLOW_CLIENT_MAX_ATTEMPTS must be 1-10, got %d", c.SlowClientMaxAttempts)
	}

	// Client send buffer size validation
	if c.ClientSendBufferSize < 64 {
		return fmt.Errorf("WS_CLIENT_SEND_BUFFER_SIZE must be >= 64, got %d", c.ClientSendBufferSize)
	}
	if c.ClientSendBufferSize > 4096 {
		return fmt.Errorf("WS_CLIENT_SEND_BUFFER_SIZE must be <= 4096 (4096 = ~2MB per client), got %d", c.ClientSendBufferSize)
	}

	// Gap notify buffer and replay rate limit (unconditional — must not be inside HistoryEnabled gate)
	if c.GapNotifyBufferSize < 1 || c.GapNotifyBufferSize > MaxGapNotifyBufferSize {
		return fmt.Errorf("WS_GAP_NOTIFY_BUFFER_SIZE must be 1-%d, got %d", MaxGapNotifyBufferSize, c.GapNotifyBufferSize)
	}
	if c.ReplayRateLimitInterval <= 0 {
		return fmt.Errorf("WS_REPLAY_RATE_LIMIT_INTERVAL must be > 0, got %v", c.ReplayRateLimitInterval)
	}

	// CPU poll interval validation
	if c.CPUPollInterval < 100*time.Millisecond {
		return fmt.Errorf("CPU_POLL_INTERVAL must be >= 100ms, got %v", c.CPUPollInterval)
	}
	if c.CPUPollInterval > c.MetricsInterval {
		return fmt.Errorf("CPU_POLL_INTERVAL (%v) should be <= METRICS_INTERVAL (%v)", c.CPUPollInterval, c.MetricsInterval)
	}

	// NOTE: Authentication is now handled by ws-gateway
	// No auth config validation needed in ws-server

	// Message backend validation (type, Kafka SASL/TLS)
	if err := c.MessageBackendConfig.Validate(); err != nil {
		return err
	}

	// Topic refresh interval validation (applies to kafka backend)
	if c.MessageBackend == MessageBackendKafka && c.TopicRefreshInterval < 1*time.Second {
		return fmt.Errorf("TOPIC_REFRESH_INTERVAL must be >= 1s, got %v", c.TopicRefreshInterval)
	}

	// Kafka topic defaults validation (only relevant when MESSAGE_BACKEND=kafka)
	if c.MessageBackend == MessageBackendKafka {
		if c.KafkaDefaultPartitions < 1 {
			return fmt.Errorf("KAFKA_DEFAULT_PARTITIONS must be >= 1, got %d", c.KafkaDefaultPartitions)
		}
		if c.KafkaDefaultReplicationFactor < 1 {
			return fmt.Errorf("KAFKA_DEFAULT_REPLICATION_FACTOR must be >= 1, got %d", c.KafkaDefaultReplicationFactor)
		}
	}

	// Provisioning gRPC validation (required for topic discovery)
	if err := c.ProvisioningClientConfig.Validate(); err != nil {
		return err
	}

	// WebSocket ping/pong validation
	// Values come from environment config with sensible defaults via envDefault.
	// See MinPongWait and MinPingPeriod constants for rationale on minimum values.
	// PingPeriod < PongWait is required for the buffer to exist.
	if c.PongWait < MinPongWait {
		return fmt.Errorf("WS_PONG_WAIT must be >= %v, got %v", MinPongWait, c.PongWait)
	}
	if c.PingPeriod < MinPingPeriod {
		return fmt.Errorf("WS_PING_PERIOD must be >= %v, got %v", MinPingPeriod, c.PingPeriod)
	}
	if c.PingPeriod >= c.PongWait {
		return fmt.Errorf("WS_PING_PERIOD (%v) must be < WS_PONG_WAIT (%v)", c.PingPeriod, c.PongWait)
	}
	// WriteWait should be reasonable (1-30 seconds)
	if c.WriteWait < time.Second || c.WriteWait > 30*time.Second {
		return fmt.Errorf("WS_WRITE_WAIT must be 1s-30s, got %v", c.WriteWait)
	}

	// --- Externalized constant validation ---

	// Duration fields (reject non-positive)
	if c.ReplayTimeout <= 0 {
		return fmt.Errorf("WS_REPLAY_TIMEOUT must be > 0, got %v", c.ReplayTimeout)
	}
	if c.PublishTimeout <= 0 {
		return fmt.Errorf("WS_PUBLISH_TIMEOUT must be > 0, got %v", c.PublishTimeout)
	}
	if c.TopicCreationTimeout <= 0 {
		return fmt.Errorf("WS_TOPIC_CREATION_TIMEOUT must be > 0, got %v", c.TopicCreationTimeout)
	}
	if c.ShardDialTimeout <= 0 {
		return fmt.Errorf("WS_SHARD_DIAL_TIMEOUT must be > 0, got %v", c.ShardDialTimeout)
	}
	if c.ShardMessageTimeout <= 0 {
		return fmt.Errorf("WS_SHARD_MESSAGE_TIMEOUT must be > 0, got %v", c.ShardMessageTimeout)
	}
	if c.MetricsAggregationInterval <= 0 {
		return fmt.Errorf("WS_METRICS_AGGREGATION_INTERVAL must be > 0, got %v", c.MetricsAggregationInterval)
	}
	if c.ShutdownGracePeriod <= 0 {
		return fmt.Errorf("WS_SHUTDOWN_GRACE_PERIOD must be > 0, got %v", c.ShutdownGracePeriod)
	}
	if c.ShutdownCheckInterval <= 0 {
		return fmt.Errorf("WS_SHUTDOWN_CHECK_INTERVAL must be > 0, got %v", c.ShutdownCheckInterval)
	}
	if c.MetricsCollectInterval <= 0 {
		return fmt.Errorf("WS_METRICS_COLLECT_INTERVAL must be > 0, got %v", c.MetricsCollectInterval)
	}
	if c.MemoryMonitorInterval <= 0 {
		return fmt.Errorf("WS_MEMORY_MONITOR_INTERVAL must be > 0, got %v", c.MemoryMonitorInterval)
	}
	if c.BufferSampleInterval <= 0 {
		return fmt.Errorf("WS_BUFFER_SAMPLE_INTERVAL must be > 0, got %v", c.BufferSampleInterval)
	}
	if c.ConnRateLimitIPTTL <= 0 {
		return fmt.Errorf("CONN_RATE_LIMIT_IP_TTL must be > 0, got %v", c.ConnRateLimitIPTTL)
	}
	if c.ConnRateLimitCleanupInterval <= 0 {
		return fmt.Errorf("CONN_RATE_LIMIT_CLEANUP_INTERVAL must be > 0, got %v", c.ConnRateLimitCleanupInterval)
	}
	if c.BroadcastShutdownTimeout <= 0 {
		return fmt.Errorf("BROADCAST_SHUTDOWN_TIMEOUT must be > 0, got %v", c.BroadcastShutdownTimeout)
	}
	if c.ValkeyWriteTimeout <= 0 {
		return fmt.Errorf("VALKEY_WRITE_TIMEOUT must be > 0, got %v", c.ValkeyWriteTimeout)
	}
	if c.ValkeyPublishTimeout <= 0 {
		return fmt.Errorf("VALKEY_PUBLISH_TIMEOUT must be > 0, got %v", c.ValkeyPublishTimeout)
	}
	if c.ValkeyStartupPingTimeout <= 0 {
		return fmt.Errorf("VALKEY_STARTUP_PING_TIMEOUT must be > 0, got %v", c.ValkeyStartupPingTimeout)
	}
	if c.ValkeyReconnectInitialBackoff <= 0 {
		return fmt.Errorf("VALKEY_RECONNECT_INITIAL_BACKOFF must be > 0, got %v", c.ValkeyReconnectInitialBackoff)
	}
	if c.ValkeyReconnectMaxBackoff <= 0 {
		return fmt.Errorf("VALKEY_RECONNECT_MAX_BACKOFF must be > 0, got %v", c.ValkeyReconnectMaxBackoff)
	}
	if c.ValkeyHealthCheckInterval <= 0 {
		return fmt.Errorf("VALKEY_HEALTH_CHECK_INTERVAL must be > 0, got %v", c.ValkeyHealthCheckInterval)
	}
	if c.ValkeyHealthCheckTimeout <= 0 {
		return fmt.Errorf("VALKEY_HEALTH_CHECK_TIMEOUT must be > 0, got %v", c.ValkeyHealthCheckTimeout)
	}
	if c.ValkeyZeroSubscriberWindow <= 0 {
		return fmt.Errorf("VALKEY_ZERO_SUBSCRIBER_WINDOW must be > 0, got %v", c.ValkeyZeroSubscriberWindow)
	}
	if c.KafkaBatchTimeout <= 0 {
		return fmt.Errorf("KAFKA_BATCH_TIMEOUT must be > 0, got %v", c.KafkaBatchTimeout)
	}
	if c.KafkaMetadataMinAge < 0 {
		return fmt.Errorf("KAFKA_METADATA_MIN_AGE must be >= 0 (0 = library default), got %v", c.KafkaMetadataMinAge)
	}
	if c.KafkaFetchMaxWait <= 0 {
		return fmt.Errorf("KAFKA_FETCH_MAX_WAIT must be > 0, got %v", c.KafkaFetchMaxWait)
	}
	if c.KafkaSessionTimeout <= 0 {
		return fmt.Errorf("KAFKA_SESSION_TIMEOUT must be > 0, got %v", c.KafkaSessionTimeout)
	}
	if c.KafkaRebalanceTimeout <= 0 {
		return fmt.Errorf("KAFKA_REBALANCE_TIMEOUT must be > 0, got %v", c.KafkaRebalanceTimeout)
	}
	if c.KafkaCommitOnRevokeTimeout <= 0 {
		return fmt.Errorf("KAFKA_COMMIT_ON_REVOKE_TIMEOUT must be > 0, got %v", c.KafkaCommitOnRevokeTimeout)
	}
	if c.KafkaCommitOnRevokeTimeout >= c.KafkaRebalanceTimeout {
		return fmt.Errorf("KAFKA_COMMIT_ON_REVOKE_TIMEOUT (%v) must be < KAFKA_REBALANCE_TIMEOUT (%v)",
			c.KafkaCommitOnRevokeTimeout, c.KafkaRebalanceTimeout)
	}
	if c.KafkaRebalanceTimeout <= c.KafkaSessionTimeout {
		return fmt.Errorf("KAFKA_REBALANCE_TIMEOUT (%v) must be > KAFKA_SESSION_TIMEOUT (%v)",
			c.KafkaRebalanceTimeout, c.KafkaSessionTimeout)
	}
	if c.KafkaAutoCommitInterval < MinAutoCommitInterval {
		return fmt.Errorf("KAFKA_AUTO_COMMIT_INTERVAL must be >= %v, got %v",
			MinAutoCommitInterval, c.KafkaAutoCommitInterval)
	}
	if c.KafkaBackpressureCheckInterval <= 0 {
		return fmt.Errorf("KAFKA_BACKPRESSURE_CHECK_INTERVAL must be > 0, got %v", c.KafkaBackpressureCheckInterval)
	}
	if c.KafkaProducerShutdownTimeout <= 0 {
		return fmt.Errorf("KAFKA_PRODUCER_SHUTDOWN_TIMEOUT must be > 0, got %v", c.KafkaProducerShutdownTimeout)
	}
	if c.KafkaProducerCBTimeout <= 0 {
		return fmt.Errorf("KAFKA_PRODUCER_CB_TIMEOUT must be > 0, got %v", c.KafkaProducerCBTimeout)
	}

	// Int fields (must be > 0)
	if c.MaxReplayMessages < 1 || c.MaxReplayMessages > MaxReplayMessagesLimit {
		return fmt.Errorf("WS_MAX_REPLAY_MESSAGES must be 1-%d, got %d", MaxReplayMessagesLimit, c.MaxReplayMessages)
	}
	if c.BufferMaxSamples < 1 {
		return fmt.Errorf("WS_BUFFER_MAX_SAMPLES must be > 0, got %d", c.BufferMaxSamples)
	}
	if c.BroadcastBufferSize < 1 {
		return fmt.Errorf("BROADCAST_BUFFER_SIZE must be > 0, got %d", c.BroadcastBufferSize)
	}
	if c.BroadcastBufferSize > BroadcastBufferSizeMax {
		return fmt.Errorf("BROADCAST_BUFFER_SIZE must be <= %d (BROADCAST_BUFFER_SIZE_MAX), got %d", BroadcastBufferSizeMax, c.BroadcastBufferSize)
	}
	if c.ValkeyReconnectMaxAttempts < 1 {
		return fmt.Errorf("VALKEY_RECONNECT_MAX_ATTEMPTS must be > 0, got %d", c.ValkeyReconnectMaxAttempts)
	}
	if c.KafkaBatchSize < 1 {
		return fmt.Errorf("KAFKA_BATCH_SIZE must be > 0, got %d", c.KafkaBatchSize)
	}
	if c.KafkaFetchMinBytes < 1 {
		return fmt.Errorf("KAFKA_FETCH_MIN_BYTES must be > 0, got %d", c.KafkaFetchMinBytes)
	}
	if c.KafkaFetchMaxBytes < 1 {
		return fmt.Errorf("KAFKA_FETCH_MAX_BYTES must be > 0, got %d", c.KafkaFetchMaxBytes)
	}
	if c.KafkaReplayFetchMaxBytes < 1 {
		return fmt.Errorf("KAFKA_REPLAY_FETCH_MAX_BYTES must be > 0, got %d", c.KafkaReplayFetchMaxBytes)
	}
	if c.KafkaProducerBatchMaxBytes < 1 || c.KafkaProducerBatchMaxBytes > math.MaxInt32 {
		return fmt.Errorf("KAFKA_PRODUCER_BATCH_MAX_BYTES must be 1-%d, got %d", math.MaxInt32, c.KafkaProducerBatchMaxBytes)
	}
	if c.KafkaProducerMaxBufferedRecords < 1 {
		return fmt.Errorf("KAFKA_PRODUCER_MAX_BUFFERED_RECORDS must be > 0, got %d", c.KafkaProducerMaxBufferedRecords)
	}
	if c.KafkaProducerRecordRetries < 1 {
		return fmt.Errorf("KAFKA_PRODUCER_RECORD_RETRIES must be > 0, got %d", c.KafkaProducerRecordRetries)
	}
	if c.KafkaProducerCBMaxFailures < 1 || c.KafkaProducerCBMaxFailures > math.MaxUint32 {
		return fmt.Errorf("KAFKA_PRODUCER_CB_MAX_FAILURES must be 1-%d, got %d", math.MaxUint32, c.KafkaProducerCBMaxFailures)
	}
	if c.KafkaProducerCBHalfOpenReqs < 1 || c.KafkaProducerCBHalfOpenReqs > math.MaxUint32 {
		return fmt.Errorf("KAFKA_PRODUCER_CB_HALF_OPEN_REQS must be 1-%d, got %d", math.MaxUint32, c.KafkaProducerCBHalfOpenReqs)
	}
	if c.RoutingFanoutWorkers < 1 {
		return fmt.Errorf("WS_ROUTING_FANOUT_WORKERS must be > 0, got %d", c.RoutingFanoutWorkers)
	}
	if c.RoutingFanoutQueueSize < 1 {
		return fmt.Errorf("WS_ROUTING_FANOUT_QUEUE_SIZE must be > 0, got %d", c.RoutingFanoutQueueSize)
	}
	if c.DLQMaxRetries < 1 {
		return fmt.Errorf("WS_ROUTING_DLQ_MAX_RETRIES must be > 0, got %d", c.DLQMaxRetries)
	}
	if c.DLQRetryWorkers < 1 {
		return fmt.Errorf("WS_ROUTING_DLQ_RETRY_WORKERS must be > 0, got %d", c.DLQRetryWorkers)
	}
	if c.DLQBaseDelay <= 0 {
		return fmt.Errorf("WS_ROUTING_DLQ_BASE_DELAY must be > 0, got %v", c.DLQBaseDelay)
	}
	if c.DLQMaxDelay <= 0 {
		return fmt.Errorf("WS_ROUTING_DLQ_MAX_DELAY must be > 0, got %v", c.DLQMaxDelay)
	}
	if c.DLQBaseDelay >= c.DLQMaxDelay {
		return fmt.Errorf("WS_ROUTING_DLQ_BASE_DELAY (%v) must be < WS_ROUTING_DLQ_MAX_DELAY (%v)", c.DLQBaseDelay, c.DLQMaxDelay)
	}

	// Percentage fields (must be 1-100)
	if c.MemoryWarningPercent < 1 || c.MemoryWarningPercent > 100 {
		return fmt.Errorf("WS_MEMORY_WARNING_PERCENT must be 1-100, got %d", c.MemoryWarningPercent)
	}
	if c.MemoryCriticalPercent < 1 || c.MemoryCriticalPercent > 100 {
		return fmt.Errorf("WS_MEMORY_CRITICAL_PERCENT must be 1-100, got %d", c.MemoryCriticalPercent)
	}
	if c.BufferHighSaturationPercent < 1 || c.BufferHighSaturationPercent > 100 {
		return fmt.Errorf("WS_BUFFER_HIGH_SATURATION_PERCENT must be 1-100, got %d", c.BufferHighSaturationPercent)
	}
	if c.BufferPopulationWarnPercent < 1 || c.BufferPopulationWarnPercent > 100 {
		return fmt.Errorf("WS_BUFFER_POPULATION_WARN_PERCENT must be 1-100, got %d", c.BufferPopulationWarnPercent)
	}

	// Ordered pairs (lower < upper)
	if c.MemoryWarningPercent >= c.MemoryCriticalPercent {
		return fmt.Errorf("WS_MEMORY_WARNING_PERCENT (%d) must be < WS_MEMORY_CRITICAL_PERCENT (%d)",
			c.MemoryWarningPercent, c.MemoryCriticalPercent)
	}
	if c.ValkeyReconnectInitialBackoff >= c.ValkeyReconnectMaxBackoff {
		return fmt.Errorf("VALKEY_RECONNECT_INITIAL_BACKOFF (%v) must be < VALKEY_RECONNECT_MAX_BACKOFF (%v)",
			c.ValkeyReconnectInitialBackoff, c.ValkeyReconnectMaxBackoff)
	}

	// Kafka namespace validation (allowlist boundary; non-empty enforced per mode below)
	if err := c.KafkaNamespaceConfig.Validate(); err != nil {
		return err
	}
	// Namespace is required only in kafka mode — topics (and thus the prefix) exist only then.
	// Mirrors the KAFKA_BROKERS required-in-kafka-mode pattern. Reads the normalized field.
	if c.MessageBackend == MessageBackendKafka && c.KafkaTopicNamespace == "" {
		return errors.New("KAFKA_TOPIC_NAMESPACE is required when MESSAGE_BACKEND=kafka")
	}

	// Alerting validation (conditional on enabled)
	if c.AlertEnabled {
		if c.AlertSlackWebhookURL == "" && !c.AlertConsoleEnabled {
			return errors.New("ALERT_ENABLED is true but no alerter configured (set ALERT_SLACK_WEBHOOK_URL or ALERT_CONSOLE_ENABLED)")
		}
		if c.AlertSlackTimeout <= 0 {
			return fmt.Errorf("ALERT_SLACK_TIMEOUT must be > 0, got %s", c.AlertSlackTimeout)
		}
		if c.AlertRateLimitWindow <= 0 {
			return fmt.Errorf("ALERT_RATE_LIMIT_WINDOW must be > 0, got %s", c.AlertRateLimitWindow)
		}
		if c.AlertRateLimitMax < 1 {
			return fmt.Errorf("ALERT_RATE_LIMIT_MAX must be >= 1, got %d", c.AlertRateLimitMax)
		}
	}

	// MaxChannelsPerClient validation (independent of history gate)
	if c.MaxChannelsPerClient < 1 || c.MaxChannelsPerClient > MaxChannelsPerClientLimit {
		return fmt.Errorf("WS_MAX_CHANNELS_PER_CLIENT must be 1-%d, got %d", MaxChannelsPerClientLimit, c.MaxChannelsPerClient)
	}

	// History validation gate
	if c.HistoryEnabled {
		// Backend prerequisites
		if c.BroadcastType != BroadcastTypeValkey {
			return fmt.Errorf("WS_HISTORY_ENABLED requires BROADCAST_TYPE=%s, got %s (history writer uses Valkey Streams)", BroadcastTypeValkey, c.BroadcastType)
		}
		if len(c.ValkeyAddrs) == 0 || !slices.ContainsFunc(c.ValkeyAddrs, func(s string) bool { return strings.TrimSpace(s) != "" }) {
			return errors.New("WS_HISTORY_ENABLED requires VALKEY_ADDRS to be set (must contain at least one non-empty address)")
		}
		if c.PodIdentityConfig.PodID() == "unknown-pod" {
			return errors.New("WS_HISTORY_ENABLED requires SUKKO_POD_ID or WS_POD_ID or a resolvable hostname (pod identity required for distributed lock)")
		}

		// Numeric bounds
		if c.HistoryBufferDepth < 1 || c.HistoryBufferDepth > MaxHistoryBufferDepth {
			return fmt.Errorf("WS_HISTORY_BUFFER_DEPTH must be 1-%d, got %d", MaxHistoryBufferDepth, c.HistoryBufferDepth)
		}
		if c.HistoryMaxLimit < 1 || c.HistoryMaxLimit > MaxHistoryMaxLimit {
			return fmt.Errorf("WS_HISTORY_MAX_LIMIT must be 1-%d, got %d", MaxHistoryMaxLimit, c.HistoryMaxLimit)
		}
		if c.HistoryWriterBuffer < 1 || c.HistoryWriterBuffer > MaxHistoryWriterBuffer {
			return fmt.Errorf("WS_HISTORY_WRITER_BUFFER must be 1-%d, got %d", MaxHistoryWriterBuffer, c.HistoryWriterBuffer)
		}
		if c.HistoryWriterPipelineBatch < 1 || c.HistoryWriterPipelineBatch > MaxHistoryPipelineBatch {
			return fmt.Errorf("WS_HISTORY_WRITER_PIPELINE_BATCH must be 1-%d, got %d", MaxHistoryPipelineBatch, c.HistoryWriterPipelineBatch)
		}
		if c.HistoryTTL <= 0 || c.HistoryTTL > MaxHistoryTTL {
			return fmt.Errorf("WS_HISTORY_TTL must be > 0 and <= %v, got %v", MaxHistoryTTL, c.HistoryTTL)
		}
		if c.HistoryDeliveryTimeout <= 0 || c.HistoryDeliveryTimeout > MaxHistoryDeliveryTimeout {
			return fmt.Errorf("WS_HISTORY_DELIVERY_TIMEOUT must be > 0 and <= %v, got %v", MaxHistoryDeliveryTimeout, c.HistoryDeliveryTimeout)
		}
		if c.HistoryWriterLockTTL < MinHistoryWriterLockTTL || c.HistoryWriterLockTTL > MaxHistoryWriterLockTTL {
			return fmt.Errorf("WS_HISTORY_WRITER_LOCK_TTL must be %v-%v, got %v", MinHistoryWriterLockTTL, MaxHistoryWriterLockTTL, c.HistoryWriterLockTTL)
		}
		if c.HistoryWriterRestartInitialBackoff <= 0 {
			return fmt.Errorf("WS_HISTORY_WRITER_RESTART_INITIAL_BACKOFF must be > 0, got %v", c.HistoryWriterRestartInitialBackoff)
		}
		if c.HistoryWriterRestartMaxBackoff <= 0 || c.HistoryWriterRestartMaxBackoff > MaxHistoryWriterRestartMaxBackoff {
			return fmt.Errorf("WS_HISTORY_WRITER_RESTART_MAX_BACKOFF must be > 0 and <= %v, got %v", MaxHistoryWriterRestartMaxBackoff, c.HistoryWriterRestartMaxBackoff)
		}
		if c.HistoryWriterRestartInitialBackoff >= c.HistoryWriterRestartMaxBackoff {
			return fmt.Errorf("WS_HISTORY_WRITER_RESTART_INITIAL_BACKOFF (%v) must be < WS_HISTORY_WRITER_RESTART_MAX_BACKOFF (%v)", c.HistoryWriterRestartInitialBackoff, c.HistoryWriterRestartMaxBackoff)
		}
		if c.HistoryValkeyCmdTimeout <= 0 {
			return fmt.Errorf("WS_HISTORY_VALKEY_CMD_TIMEOUT must be > 0, got %v", c.HistoryValkeyCmdTimeout)
		}
		if c.HistoryMaxConsecutiveLockFailures < 1 {
			return fmt.Errorf("WS_HISTORY_MAX_CONSECUTIVE_LOCK_FAILURES must be >= 1, got %d", c.HistoryMaxConsecutiveLockFailures)
		}
		if c.HistoryWriterHeartbeatInterval < MinHistoryWriterHeartbeatInterval {
			return fmt.Errorf("WS_HISTORY_WRITER_HEARTBEAT_INTERVAL must be >= %v, got %v", MinHistoryWriterHeartbeatInterval, c.HistoryWriterHeartbeatInterval)
		}
		// Heartbeat must renew the lock before it expires: interval must be <= TTL/HistoryWriterLockRenewDivisor.
		if lockRenewThreshold := c.HistoryWriterLockTTL / HistoryWriterLockRenewDivisor; c.HistoryWriterHeartbeatInterval > lockRenewThreshold {
			return fmt.Errorf("WS_HISTORY_WRITER_HEARTBEAT_INTERVAL (%v) must be <= WS_HISTORY_WRITER_LOCK_TTL / %d (%v) to ensure lock renewal before expiry",
				c.HistoryWriterHeartbeatInterval, HistoryWriterLockRenewDivisor, lockRenewThreshold)
		}
	}

	// Registry validation — UNCONDITIONAL per.
	// Validates bounds regardless of ConnectionsRegistryEnabled so misconfigurations are
	// caught before the feature is turned on. Also validates InternalSecret.
	if c.ConnectionsRegistryBuffer < MinConnectionsRegistryBuffer || c.ConnectionsRegistryBuffer > MaxConnectionsRegistryBuffer {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_BUFFER must be %d-%d, got %d", MinConnectionsRegistryBuffer, MaxConnectionsRegistryBuffer, c.ConnectionsRegistryBuffer)
	}
	if c.ConnectionsRegistryTTL < MinConnectionsRegistryTTL || c.ConnectionsRegistryTTL > MaxConnectionsRegistryTTL {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_TTL must be %v-%v, got %v", MinConnectionsRegistryTTL, MaxConnectionsRegistryTTL, c.ConnectionsRegistryTTL)
	}
	if c.ConnectionsRegistryFlushInterval < MinConnectionsRegistryFlushInterval || c.ConnectionsRegistryFlushInterval > MaxConnectionsRegistryFlushInterval {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_FLUSH_INTERVAL must be %v-%v, got %v", MinConnectionsRegistryFlushInterval, MaxConnectionsRegistryFlushInterval, c.ConnectionsRegistryFlushInterval)
	}
	if c.ConnectionsRegistryHeartbeatInterval < MinConnectionsRegistryHeartbeatInterval || c.ConnectionsRegistryHeartbeatInterval > MaxConnectionsRegistryHeartbeatInterval {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_HEARTBEAT_INTERVAL must be %v-%v, got %v", MinConnectionsRegistryHeartbeatInterval, MaxConnectionsRegistryHeartbeatInterval, c.ConnectionsRegistryHeartbeatInterval)
	}
	if c.ConnectionsRegistryRestartInitialBackoff < MinConnectionsRegistryRestartInitialBackoff || c.ConnectionsRegistryRestartInitialBackoff > MaxConnectionsRegistryRestartInitialBackoff {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_RESTART_INITIAL_BACKOFF must be %v-%v, got %v", MinConnectionsRegistryRestartInitialBackoff, MaxConnectionsRegistryRestartInitialBackoff, c.ConnectionsRegistryRestartInitialBackoff)
	}
	if c.ConnectionsRegistryRestartMaxBackoff < MinConnectionsRegistryRestartMaxBackoff || c.ConnectionsRegistryRestartMaxBackoff > MaxConnectionsRegistryRestartMaxBackoff {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_RESTART_MAX_BACKOFF must be %v-%v, got %v", MinConnectionsRegistryRestartMaxBackoff, MaxConnectionsRegistryRestartMaxBackoff, c.ConnectionsRegistryRestartMaxBackoff)
	}
	if c.ConnectionsRegistryShutdownDrainTimeout < MinRegistryWriterShutdownDrainTimeout || c.ConnectionsRegistryShutdownDrainTimeout > MaxRegistryWriterShutdownDrainTimeout {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_SHUTDOWN_DRAIN_TIMEOUT must be %v-%v, got %v", MinRegistryWriterShutdownDrainTimeout, MaxRegistryWriterShutdownDrainTimeout, c.ConnectionsRegistryShutdownDrainTimeout)
	}
	if c.AdminChannelSubscribeTimeout < MinAdminChannelSubscribeTimeout || c.AdminChannelSubscribeTimeout > MaxAdminChannelSubscribeTimeout {
		return fmt.Errorf("WS_ADMIN_CHANNEL_SUBSCRIBE_TIMEOUT must be %v-%v, got %v", MinAdminChannelSubscribeTimeout, MaxAdminChannelSubscribeTimeout, c.AdminChannelSubscribeTimeout)
	}
	// Cross-field checks
	if c.ConnectionsRegistryTTL <= c.ConnectionsRegistryHeartbeatInterval*RegistryHeartbeatTTLDivisor {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_TTL (%v) must be > WS_CONNECTIONS_REGISTRY_HEARTBEAT_INTERVAL × %d (%v) to ensure TTL renewal before expiry",
			c.ConnectionsRegistryTTL, RegistryHeartbeatTTLDivisor, c.ConnectionsRegistryHeartbeatInterval*RegistryHeartbeatTTLDivisor)
	}
	if c.ConnectionsRegistryFlushInterval > c.ConnectionsRegistryHeartbeatInterval {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_FLUSH_INTERVAL (%v) must be <= WS_CONNECTIONS_REGISTRY_HEARTBEAT_INTERVAL (%v)",
			c.ConnectionsRegistryFlushInterval, c.ConnectionsRegistryHeartbeatInterval)
	}
	if c.ConnectionsRegistryRestartInitialBackoff >= c.ConnectionsRegistryRestartMaxBackoff {
		return fmt.Errorf("WS_CONNECTIONS_REGISTRY_RESTART_INITIAL_BACKOFF (%v) must be < WS_CONNECTIONS_REGISTRY_RESTART_MAX_BACKOFF (%v)",
			c.ConnectionsRegistryRestartInitialBackoff, c.ConnectionsRegistryRestartMaxBackoff)
	}
	if c.InternalSecretEnabled && c.InternalSecret == "" {
		return errors.New("WS_INTERNAL_SECRET must be set when WS_INTERNAL_SECRET_ENABLED=true")
	}

	// Edition gates — checked after all other validation passes.
	// Uses startup-resolved Edition()/Limits(), NOT expiry-aware CurrentEdition()/CurrentLimits().
	// Config validation runs once at startup; runtime gates use CurrentLimits() for mid-flight expiry.
	if c.editionManager != nil {
		edition := c.editionManager.Edition()
		limits := c.editionManager.Limits()

		// Feature gates — fail at startup if config uses gated features
		if c.AlertEnabled && !license.EditionHasFeature(edition, license.Alerting) {
			return license.NewFeatureError(license.Alerting, edition)
		}
		if c.OTELTracingEnabled && !license.EditionHasFeature(edition, license.ConnectionTracing) {
			return license.NewFeatureError(license.ConnectionTracing, edition)
		}
		if c.ConnectionsRegistryEnabled && !license.EditionHasFeature(edition, license.ConnectionsAPI) {
			return license.NewFeatureError(license.ConnectionsAPI, edition)
		}

		// Hard limit gates — validate configured capacity doesn't exceed edition limits
		if err := limits.CheckShards(c.NumShards); err != nil {
			return fmt.Errorf("edition limit: %w", err)
		}
		// WS_MAX_CONNECTIONS is the TOTAL capacity (divided across shards in cmd/server/main.go).
		// Check it directly — do NOT multiply by NumShards.
		if err := limits.CheckTotalConnections(c.MaxConnections); err != nil {
			return fmt.Errorf("edition limit: %w", err)
		}
	}

	return nil
}

// ValidateEditionLimits checks whether the broadcast buffer configuration is safe
// for the given edition's tenant limits. Returns an error if the worst-case memory
// usage would exceed WS_MEMORY_LIMIT. Enterprise (unlimited tenants) is always valid.
func (c *ServerConfig) ValidateEditionLimits(limits license.Limits) error {
	if license.IsUnlimited(limits.MaxTenants) {
		return nil // Enterprise: unlimited tenants — skip static formula
	}
	worstCaseBytes := int64(limits.MaxTenants) * int64(c.NumShards) * int64(c.BroadcastBufferSize) * 8
	if worstCaseBytes > c.MemoryLimit {
		return fmt.Errorf(
			"BROADCAST_BUFFER_SIZE=%d with %d max tenants and %d shards would use ~%d bytes per pod, "+
				"exceeding WS_MEMORY_LIMIT=%d; reduce BROADCAST_BUFFER_SIZE or increase WS_MEMORY_LIMIT",
			c.BroadcastBufferSize, limits.MaxTenants, c.NumShards, worstCaseBytes, c.MemoryLimit,
		)
	}
	return nil
}

// AlertConfig returns an alerting.Config derived from the server configuration.
// serviceName identifies this service instance in alert messages.
func (c *ServerConfig) AlertConfig(serviceName string) alerting.Config {
	return alerting.Config{
		Enabled:         c.AlertEnabled,
		MinLevel:        alerting.ParseLevel(c.AlertMinLevel),
		SlackWebhookURL: c.AlertSlackWebhookURL,
		SlackChannel:    c.AlertSlackChannel,
		SlackUsername:   c.AlertSlackUsername,
		ServiceName:     serviceName,
		Environment:     c.Environment,
		RateLimitWindow: c.AlertRateLimitWindow,
		RateLimitMax:    c.AlertRateLimitMax,
		SlackTimeout:    c.AlertSlackTimeout,
		ConsoleEnabled:  c.AlertConsoleEnabled,
	}
}

// Print logs server configuration for debugging (human-readable format)
// For production, use LogConfig() with structured logging
func (c *ServerConfig) Print() {
	edition := "community"
	if c.editionManager != nil {
		edition = c.editionManager.Edition().String()
	}

	_, _ = fmt.Fprintln(os.Stdout, "=== Server Configuration ===")
	_, _ = fmt.Fprintf(os.Stdout, "Edition:         %s\n", edition)
	_, _ = fmt.Fprintf(os.Stdout, "Environment:     %s\n", c.Environment)
	if c.KafkaTopicNamespace != "" {
		_, _ = fmt.Fprintf(os.Stdout, "Topic Namespace: %s\n", c.KafkaTopicNamespace)
	}
	_, _ = fmt.Fprintf(os.Stdout, "Address:         %s\n", c.Addr)
	_, _ = fmt.Fprintf(os.Stdout, "Message Backend: %s\n", c.MessageBackend)
	_, _ = fmt.Fprintf(os.Stdout, "Kafka Brokers:   %s\n", c.KafkaBrokers)
	_, _ = fmt.Fprintf(os.Stdout, "Consumer Enabled: %v\n", c.KafkaConsumerEnabled)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Kafka Security ===")
	_, _ = fmt.Fprintf(os.Stdout, "SASL Enabled:    %v\n", c.KafkaSASLEnabled)
	if c.KafkaSASLEnabled {
		_, _ = fmt.Fprintf(os.Stdout, "SASL Mechanism:  %s\n", c.KafkaSASLMechanism)
		_, _ = fmt.Fprintf(os.Stdout, "SASL Username:   %s\n", c.KafkaSASLUsername)
		_, _ = fmt.Fprintf(os.Stdout, "SASL Password:   %s\n", "****")
	}
	_, _ = fmt.Fprintf(os.Stdout, "TLS Enabled:     %v\n", c.KafkaTLSEnabled)
	if c.KafkaTLSEnabled {
		_, _ = fmt.Fprintf(os.Stdout, "TLS Insecure:    %v\n", c.KafkaTLSInsecure)
		_, _ = fmt.Fprintf(os.Stdout, "TLS CA Path:     %s\n", c.KafkaTLSCAPath)
	}
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Resource Limits ===")
	_, _ = fmt.Fprintf(os.Stdout, "GOMAXPROCS:      %d (container-aware, Go runtime)\n", runtime.GOMAXPROCS(0))
	_, _ = fmt.Fprintf(os.Stdout, "Memory Limit:    %d MB\n", c.MemoryLimit/(1024*1024))
	_, _ = fmt.Fprintf(os.Stdout, "Max Connections: %d\n", c.MaxConnections)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Rate Limits ===")
	_, _ = fmt.Fprintf(os.Stdout, "Kafka Messages:  %d/sec\n", c.MaxKafkaMessagesPerSec)
	_, _ = fmt.Fprintf(os.Stdout, "Broadcasts:      %d/sec\n", c.MaxBroadcastsPerSec)
	_, _ = fmt.Fprintf(os.Stdout, "Max Goroutines:  %d\n", c.MaxGoroutines)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Connection Rate Limiting (DoS Protection) ===")
	_, _ = fmt.Fprintf(os.Stdout, "Enabled:         %v\n", c.ConnectionRateLimitEnabled)
	_, _ = fmt.Fprintf(os.Stdout, "IP Burst:        %d connections\n", c.ConnRateLimitIPBurst)
	_, _ = fmt.Fprintf(os.Stdout, "IP Rate:         %.1f conn/sec\n", c.ConnRateLimitIPRate)
	_, _ = fmt.Fprintf(os.Stdout, "Global Burst:    %d connections\n", c.ConnRateLimitGlobalBurst)
	_, _ = fmt.Fprintf(os.Stdout, "Global Rate:     %.1f conn/sec\n", c.ConnRateLimitGlobalRate)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Safety Thresholds (with Hysteresis) ===")
	_, _ = fmt.Fprintf(os.Stdout, "CPU Reject (upper):    %.1f%%\n", c.CPURejectThreshold)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Reject (lower):    %.1f%%\n", c.CPURejectThresholdLower)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Reject Band:       %.1f%%\n", c.CPURejectThreshold-c.CPURejectThresholdLower)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Pause (upper):     %.1f%%\n", c.CPUPauseThreshold)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Pause (lower):     %.1f%%\n", c.CPUPauseThresholdLower)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Pause Band:        %.1f%%\n", c.CPUPauseThreshold-c.CPUPauseThresholdLower)
	_, _ = fmt.Fprintf(os.Stdout, "CPU EWMA Beta:         %.2f (~%.0f sample window)\n", c.CPUEWMABeta, 1/(1-c.CPUEWMABeta))
	_, _ = fmt.Fprintln(os.Stdout, "\n=== TCP/Network Tuning ===")
	_, _ = fmt.Fprintf(os.Stdout, "Listen Backlog:  %d\n", c.TCPListenBacklog)
	_, _ = fmt.Fprintf(os.Stdout, "Read Timeout:    %s\n", c.HTTPReadTimeout)
	_, _ = fmt.Fprintf(os.Stdout, "Write Timeout:   %s\n", c.HTTPWriteTimeout)
	_, _ = fmt.Fprintf(os.Stdout, "Idle Timeout:    %s\n", c.HTTPIdleTimeout)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Logging ===")
	_, _ = fmt.Fprintf(os.Stdout, "Level:           %s\n", c.LogLevel)
	_, _ = fmt.Fprintf(os.Stdout, "Format:          %s\n", c.LogFormat)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Authentication ===")
	_, _ = fmt.Fprintln(os.Stdout, "Auth:            Handled by ws-gateway")
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Broadcast Bus ===")
	_, _ = fmt.Fprintf(os.Stdout, "Type:            %s\n", c.BroadcastType)
	_, _ = fmt.Fprintf(os.Stdout, "Valkey Addrs:    %v\n", c.ValkeyAddrs)
	_, _ = fmt.Fprintf(os.Stdout, "Master Name:     %s\n", c.ValkeyMasterName)
	_, _ = fmt.Fprintf(os.Stdout, "Channel prefix:  %s\n", c.ValkeyChannel)
	_, _ = fmt.Fprintf(os.Stdout, "Database:        %d\n", c.ValkeyDB)
	if c.ValkeyTLSEnabled {
		_, _ = fmt.Fprintf(os.Stdout, "Valkey TLS:      %v\n", c.ValkeyTLSEnabled)
	}
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Client Buffers ===")
	_, _ = fmt.Fprintf(os.Stdout, "Send Buffer:     %d slots (~%dKB/client)\n", c.ClientSendBufferSize, c.ClientSendBufferSize/2)
	_, _ = fmt.Fprintf(os.Stdout, "Gap Notify Buffer: %d (WS_GAP_NOTIFY_BUFFER_SIZE)\n", c.GapNotifyBufferSize)
	_, _ = fmt.Fprintf(os.Stdout, "Replay Rate Limit: %v (WS_REPLAY_RATE_LIMIT_INTERVAL)\n", c.ReplayRateLimitInterval)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Slow Client Detection ===")
	_, _ = fmt.Fprintf(os.Stdout, "Max Attempts:    %d\n", c.SlowClientMaxAttempts)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Multi-Tenant Consumer ===")
	_, _ = fmt.Fprintf(os.Stdout, "Provisioning gRPC:   %s\n", c.ProvisioningGRPCAddr)
	_, _ = fmt.Fprintf(os.Stdout, "Reconnect Delay:     %s\n", c.GRPCReconnectDelay)
	_, _ = fmt.Fprintf(os.Stdout, "Reconnect Max Delay: %s\n", c.GRPCReconnectMaxDelay)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== WebSocket Ping/Pong ===")
	_, _ = fmt.Fprintf(os.Stdout, "Pong Wait:           %s\n", c.PongWait)
	_, _ = fmt.Fprintf(os.Stdout, "Ping Period:         %s\n", c.PingPeriod)
	_, _ = fmt.Fprintf(os.Stdout, "Write Wait:          %s\n", c.WriteWait)
	_, _ = fmt.Fprintf(os.Stdout, "Buffer:              %s\n", c.PongWait-c.PingPeriod)
	_, _ = fmt.Fprintln(os.Stdout, "\n=== Monitoring ===")
	_, _ = fmt.Fprintf(os.Stdout, "Metrics Interval: %s\n", c.MetricsInterval)
	_, _ = fmt.Fprintf(os.Stdout, "CPU Poll Interval: %s\n", c.CPUPollInterval)
	_, _ = fmt.Fprintln(os.Stdout, "============================")
}

// LogConfig logs server configuration using structured logging (Loki-compatible)
func (c *ServerConfig) LogConfig(logger zerolog.Logger) {
	edition := "community"
	if c.editionManager != nil {
		edition = c.editionManager.Edition().String()
	}

	logger.Info().
		Str("edition", edition).
		Str("environment", c.Environment).
		Str("kafka_topic_namespace", c.KafkaTopicNamespace).
		Str("addr", c.Addr).
		Str("message_backend", c.MessageBackend).
		Str("kafka_brokers", c.KafkaBrokers).
		Bool("kafka_consumer_enabled", c.KafkaConsumerEnabled).
		Bool("kafka_sasl_enabled", c.KafkaSASLEnabled).
		Str("kafka_sasl_mechanism", c.KafkaSASLMechanism).
		Bool("kafka_sasl_username_set", c.KafkaSASLUsername != "").
		Bool("kafka_tls_enabled", c.KafkaTLSEnabled).
		Bool("kafka_tls_insecure", c.KafkaTLSInsecure).
		Str("kafka_tls_ca_path", c.KafkaTLSCAPath).
		Int("gomaxprocs", runtime.GOMAXPROCS(0)).
		Int64("memory_limit_mb", c.MemoryLimit/(1024*1024)).
		Int("max_connections", c.MaxConnections).
		Int("max_kafka_rate", c.MaxKafkaMessagesPerSec).
		Int("max_broadcast_rate", c.MaxBroadcastsPerSec).
		Int("max_goroutines", c.MaxGoroutines).
		Bool("conn_rate_limit_enabled", c.ConnectionRateLimitEnabled).
		Int("conn_rate_limit_ip_burst", c.ConnRateLimitIPBurst).
		Float64("conn_rate_limit_ip_rate", c.ConnRateLimitIPRate).
		Int("conn_rate_limit_global_burst", c.ConnRateLimitGlobalBurst).
		Float64("conn_rate_limit_global_rate", c.ConnRateLimitGlobalRate).
		Float64("cpu_reject_threshold", c.CPURejectThreshold).
		Float64("cpu_reject_threshold_lower", c.CPURejectThresholdLower).
		Float64("cpu_pause_threshold", c.CPUPauseThreshold).
		Float64("cpu_pause_threshold_lower", c.CPUPauseThresholdLower).
		Int("tcp_listen_backlog", c.TCPListenBacklog).
		Dur("http_read_timeout", c.HTTPReadTimeout).
		Dur("http_write_timeout", c.HTTPWriteTimeout).
		Dur("http_idle_timeout", c.HTTPIdleTimeout).
		Dur("metrics_interval", c.MetricsInterval).
		Dur("cpu_poll_interval", c.CPUPollInterval).
		Str("log_level", c.LogLevel).
		Str("log_format", c.LogFormat).
		Str("auth", "handled by ws-gateway").
		Str("broadcast_type", c.BroadcastType).
		Strs("valkey_addrs", c.ValkeyAddrs).
		Str("valkey_master_name", c.ValkeyMasterName).
		Str(logFieldChannelPrefix, c.ValkeyChannel).
		Int("valkey_db", c.ValkeyDB).
		Bool("valkey_tls_enabled", c.ValkeyTLSEnabled).
		Int("client_send_buffer_size", c.ClientSendBufferSize).
		Int("gap_notify_buffer_size", c.GapNotifyBufferSize).
		Dur("replay_rate_limit_interval", c.ReplayRateLimitInterval).
		Int("slow_client_max_attempts", c.SlowClientMaxAttempts).
		Int("kafka_default_partitions", c.KafkaDefaultPartitions).
		Int("kafka_default_replication_factor", c.KafkaDefaultReplicationFactor).
		Dur("topic_refresh_interval", c.TopicRefreshInterval).
		Str("provisioning_grpc_addr", c.ProvisioningGRPCAddr).
		Dur("grpc_reconnect_delay", c.GRPCReconnectDelay).
		Dur("grpc_reconnect_max_delay", c.GRPCReconnectMaxDelay).
		Dur("ws_pong_wait", c.PongWait).
		Dur("ws_ping_period", c.PingPeriod).
		Dur("ws_write_wait", c.WriteWait).
		Msg("Server configuration loaded")
}
