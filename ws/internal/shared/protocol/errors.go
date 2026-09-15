package protocol

import "errors"

// ErrorCode represents machine-readable error codes for all error response types.
// Used across error, subscribe_error, unsubscribe_error, publish_error,
// and reconnect_error responses.
type ErrorCode string

// Error codes used by both gateway and server.
const (
	// General error codes (used across multiple response types).

	// ErrCodeInvalidRequest indicates a malformed request.
	// Used by subscribe_error, unsubscribe_error, reconnect_error, publish_error.
	ErrCodeInvalidRequest ErrorCode = "invalid_request"

	// ErrCodeNotAvailable indicates the requested feature is not available.
	// Used by publish_error and reconnect_error.
	ErrCodeNotAvailable ErrorCode = "not_available"

	// Publish-specific error codes.

	// ErrCodeInvalidChannel indicates invalid channel format.
	// Channel must have format: {tenant_id}.{suffix} (minimum 2 parts).
	ErrCodeInvalidChannel ErrorCode = "invalid_channel"

	// ErrCodeMessageTooLarge indicates payload exceeds size limit.
	ErrCodeMessageTooLarge ErrorCode = "message_too_large"

	// ErrCodeRateLimited indicates publish rate limit exceeded.
	ErrCodeRateLimited ErrorCode = "rate_limited"

	// ErrCodeForbidden indicates not authorized to publish to channel.
	ErrCodeForbidden ErrorCode = "forbidden"

	// ErrCodeTopicNotProvisioned indicates the topic doesn't exist for the tenant.
	ErrCodeTopicNotProvisioned ErrorCode = "topic_not_provisioned"

	// ErrCodeServiceUnavailable indicates Kafka is unavailable (circuit open).
	ErrCodeServiceUnavailable ErrorCode = "service_unavailable"

	// ErrCodeNoRoutingRules indicates no topic routing rules are configured for the tenant.
	ErrCodeNoRoutingRules ErrorCode = "no_routing_rules"

	// ErrCodeNoMatchingRoute indicates no routing rule matched the channel suffix.
	ErrCodeNoMatchingRoute ErrorCode = "no_matching_route"

	// Server-originated error codes (used only by ws-server, but typed as
	// shared ErrorCode and referenced by the shared error message map).

	// ErrCodeInvalidJSON indicates a client message is not valid JSON.
	ErrCodeInvalidJSON ErrorCode = "invalid_json"

	// ErrCodePublishFailed indicates backend publish failed.
	ErrCodePublishFailed ErrorCode = "publish_failed"

	// ErrCodeReplayFailed indicates backend message replay failed.
	ErrCodeReplayFailed ErrorCode = "replay_failed"

	// ErrCodeSubscribeLimitExceeded indicates the per-client channel subscription limit was reached.
	ErrCodeSubscribeLimitExceeded ErrorCode = "subscribe_limit_exceeded"

	// Live replay error codes (used only by ws-server, but typed as shared ErrorCode
	// so that replay error responses use the same ErrorCode type as other protocol errors).
	// §X documented exception: ErrorCode is a protocol-level type; constants live here
	// even when only one service emits them, to keep all error codes in a single registry.

	// ErrCodeReplayInProgress indicates a replay is already in progress for this channel.
	ErrCodeReplayInProgress ErrorCode = "replay_in_progress"

	// ErrCodeNotSubscribed indicates the client is not subscribed to the requested channel.
	ErrCodeNotSubscribed ErrorCode = "not_subscribed"

	// ErrCodeOffsetOutOfRange indicates the requested position is outside available history.
	ErrCodeOffsetOutOfRange ErrorCode = "offset_out_of_range"

	// ErrCodeReplayRateLimited indicates replay requests are too frequent for this channel.
	ErrCodeReplayRateLimited ErrorCode = "replay_rate_limited"
)

// publishErrorMessages maps error codes to human-readable messages for publish errors.
var publishErrorMessages = map[ErrorCode]string{
	ErrCodeNotAvailable:        "Publishing is not enabled on this server",
	ErrCodeInvalidRequest:      "Invalid publish request format",
	ErrCodeInvalidChannel:      "Channel must have format: {tenant_id}.{suffix}",
	ErrCodeMessageTooLarge:     "Message exceeds maximum size limit",
	ErrCodeRateLimited:         "Publish rate limit exceeded",
	ErrCodePublishFailed:       "Failed to publish message",
	ErrCodeForbidden:           "Not authorized to publish to this channel",
	ErrCodeTopicNotProvisioned: "Topic is not provisioned for your tenant",
	ErrCodeServiceUnavailable:  "Service temporarily unavailable, please retry",
	ErrCodeNoRoutingRules:      "No topic routing rules configured for tenant",
	ErrCodeNoMatchingRoute:     "No matching topic routing rule for channel",
}

// PublishErrorMessage returns the human-readable message for a publish error code.
// Returns an empty string for unknown codes.
func PublishErrorMessage(code ErrorCode) string {
	return publishErrorMessages[code]
}

// Sentinel errors for internal use.
// These are used internally and mapped to ErrorCode for client responses.
var (
	// ErrInvalidChannel indicates the channel format is invalid.
	ErrInvalidChannel = errors.New("invalid channel format")

	// ErrTopicNotProvisioned indicates the topic doesn't exist.
	ErrTopicNotProvisioned = errors.New("topic not provisioned")

	// ErrServiceUnavailable indicates Kafka is unavailable.
	ErrServiceUnavailable = errors.New("service unavailable")
)
