package metrics

// Error severity levels for metrics and logging.
const (
	SeverityCritical = "critical" // Critical but recoverable
)

// Error types for categorization.
const (
	ErrorTypeKafka         = "kafka"
	ErrorTypeBroadcast     = "broadcast"
	ErrorTypeSerialization = "serialization"
	ErrorTypeConnection    = "connection"
)

// Disconnect reasons - standardized constants for categorization.
const (
	DisconnectReadError         = "read_error"          // Client stopped reading (network issue, crash)
	DisconnectWriteTimeout      = "write_timeout"       // Slow client (send buffer full)
	DisconnectPingTimeout       = "ping_timeout"        // Client didn't respond to ping
	DisconnectRateLimitExceeded = "rate_limit_exceeded" // Client sent too many messages
	DisconnectServerShutdown    = "server_shutdown"     // Graceful shutdown
	DisconnectClientInitiated   = "client_initiated"    // Normal close from client
	DisconnectSubscriptionError = "subscription_error"  // Invalid subscription
	DisconnectSendChannelClosed = "send_channel_closed" // Server closed send channel
)

// Disconnect initiators - who initiated the disconnect.
const (
	InitiatedByClient = "client"
	InitiatedByServer = "server"
)

// Drop reasons - why broadcast messages were dropped.
const (
	DropReasonSendTimeout        = "send_timeout"        // Timed out trying to send to client
	DropReasonBufferFull         = "buffer_full"         // Client send buffer is full
	DropReasonClientDisconnected = "client_disconnected" // Client already disconnected
)

// Auth status values for authentication metrics.
const (
	AuthStatusSuccess = "success"
	AuthStatusFailed  = "failed"
	AuthStatusSkipped = "skipped"
)

// Operation result values for general operation metrics.
const (
	ResultSuccess = "success"
	ResultError   = "error"
	ResultFailed  = "failed"
)
