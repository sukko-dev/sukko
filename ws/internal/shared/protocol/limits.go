package protocol

// Default size limits for WebSocket messages.
// These can be overridden via environment variables in each component's config.
const (
	// DefaultMaxFrameSize is the default maximum WebSocket frame size (1MB).
	// Applied at the proxy level before payload allocation to prevent OOM.
	// Gateway: GATEWAY_MAX_FRAME_SIZE
	DefaultMaxFrameSize = 1024 * 1024

	// DefaultMaxPublishSize is the default maximum size for publish payloads (64KB).
	// Gateway: GATEWAY_MAX_PUBLISH_SIZE
	DefaultMaxPublishSize = 64 * 1024

	// DefaultSendBufferSize is the default size for client send buffers.
	DefaultSendBufferSize = 256
)

// Default rate limits for publish operations.
// These can be overridden via environment variables in each component's config.
// Gateway: GATEWAY_PUBLISH_RATE_LIMIT, GATEWAY_PUBLISH_BURST
const (
	// DefaultPublishRateLimit is the default messages per second limit.
	DefaultPublishRateLimit = 10.0

	// DefaultPublishBurst is the default burst capacity.
	DefaultPublishBurst = 100
)

// Channel format constants define the minimum number of parts in channel names.
// With explicit channels, clients always include the tenant prefix.
//
// Changed from 3 to 2 as part of the routing-rules migration: channels now use
// the format {tenant_id}.{suffix} instead of {tenant_id}.{identifier}.{category}.
// The old 3-part format ({tenant_id}.{token_id}.{category}) was tied to the
// removed category-based topic system. The new format allows flexible topic
// suffixes defined by routing rules (e.g., "sukko.BTC.trade", "sukko.metadata").
const (
	// MinInternalChannelParts is the minimum parts for channels: {tenant_id}.{suffix}
	MinInternalChannelParts = 2
)
