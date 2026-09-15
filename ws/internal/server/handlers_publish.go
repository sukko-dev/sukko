package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// handleClientPublish processes a client publish request.
// It validates the request, checks rate limits, and publishes via the message backend.
//
// Message format:
//
//	{
//	  "type": "publish",
//	  "data": {
//	    "channel": "acme.BTC.trade",  // Internal format (tenant-prefixed from gateway)
//	    "data": {"message": "Hello", "sender": "user456"}
//	  }
//	}
//
// The channel must already be in internal format (tenant-prefixed) after gateway mapping.
func (s *Server) handleClientPublish(c *Client, data json.RawMessage) {
	// Check if backend is available
	if s.backend == nil {
		s.sendPublishError(c, protocol.ErrCodeNotAvailable, protocol.PublishErrorMessage(protocol.ErrCodeNotAvailable))
		return
	}

	// Parse the publish request
	var pubReq protocol.PublishData

	if err := json.Unmarshal(data, &pubReq); err != nil {
		s.logger.Warn().
			Int64("client_id", c.id).
			Err(err).
			Msg("Client sent invalid publish request")
		s.sendPublishError(c, protocol.ErrCodeInvalidRequest, protocol.PublishErrorMessage(protocol.ErrCodeInvalidRequest))
		return
	}

	// Validate channel format ({tenant}.{suffix} — at least 2 dot-separated parts)
	if !auth.IsValidInternalChannel(pubReq.Channel) {
		s.logger.Warn().
			Int64("client_id", c.id).
			Str("channel", pubReq.Channel).
			Msg("Client sent invalid channel format")
		s.sendPublishError(c, protocol.ErrCodeInvalidChannel, protocol.PublishErrorMessage(protocol.ErrCodeInvalidChannel))
		return
	}

	// Validate message size
	if len(pubReq.Data) > protocol.DefaultMaxPublishSize {
		s.logger.Warn().
			Int64("client_id", c.id).
			Int("size", len(pubReq.Data)).
			Int("max_size", protocol.DefaultMaxPublishSize).
			Msg("Client publish message too large")
		s.sendPublishError(c, protocol.ErrCodeMessageTooLarge, protocol.PublishErrorMessage(protocol.ErrCodeMessageTooLarge))
		return
	}

	// Rate limiting check (uses the same rate limiter as other messages)
	if s.rateLimiter != nil && !s.rateLimiter.CheckLimit(c.id) {
		s.logger.Warn().
			Int64("client_id", c.id).
			Str("channel", pubReq.Channel).
			Msg("Client publish rate limited")
		s.sendPublishError(c, protocol.ErrCodeRateLimited, protocol.PublishErrorMessage(protocol.ErrCodeRateLimited))
		s.stats.RateLimitedMessages.Add(1)
		metrics.IncrementRateLimitedMessages()
		return
	}

	// Publish to backend with timeout
	ctx, cancel := context.WithTimeout(s.ctx, s.config.PublishTimeout)
	defer cancel()

	mid, err := s.backend.Publish(ctx, c.id, c.tenantID, pubReq.Channel, pubReq.Data)
	if err != nil {
		// Map backend errors to client error codes.
		code, reject := publishErrorToClientCode(err)

		evt := s.logger.Error()
		if reject {
			evt = s.logger.Warn()
		}
		evt.Err(err).
			Int64("client_id", c.id).
			Str("channel", pubReq.Channel).
			Str("error_code", string(code)).
			Msg("Failed to publish message to backend")

		s.sendPublishError(c, code, protocol.PublishErrorMessage(code))
		// Backend-agnostic publish error metrics are recorded inside each backend's Publish()
		return
	}

	// Success - send acknowledgment
	s.sendPublishAck(c, pubReq.Channel, mid)

	s.logger.Debug().
		Int64("client_id", c.id).
		Str("channel", pubReq.Channel).
		Int("data_size", len(pubReq.Data)).
		Msg("Client message published to backend")

	// Backend-agnostic publish metrics are recorded inside each backend's Publish()
}

// publishErrorToClientCode maps a backend publish error to the client-facing WS error code
// and whether it is a reject/transient class (expected → Warn log per §V, vs a genuine
// failure → Error). ErrNoMatchingRoute wraps ErrPublishNotRoutable, so it MUST be checked
// first to emit the more-specific no_matching_route rather than no_routing_rules (#179 C1).
func publishErrorToClientCode(err error) (protocol.ErrorCode, bool) {
	switch {
	case errors.Is(err, backend.ErrNoMatchingRoute):
		return protocol.ErrCodeNoMatchingRoute, true
	case errors.Is(err, backend.ErrPublishNotRoutable):
		return protocol.ErrCodeNoRoutingRules, true
	case errors.Is(err, protocol.ErrInvalidChannel):
		return protocol.ErrCodeInvalidChannel, true
	case errors.Is(err, protocol.ErrTopicNotProvisioned):
		return protocol.ErrCodeTopicNotProvisioned, false
	case errors.Is(err, protocol.ErrServiceUnavailable):
		return protocol.ErrCodeServiceUnavailable, true
	default:
		return protocol.ErrCodePublishFailed, false
	}
}

// sendPublishAck sends a publish acknowledgment to the client. mid is the
// stable message identity assigned by the backend — echoed so the publisher
// can correlate/dedup against delivered copies; omitted when empty (multi-topic
// fan-out publishes carry no single identity).
func (s *Server) sendPublishAck(c *Client, channel, mid string) {
	ack := map[string]any{
		"type":    protocol.RespTypePublishAck,
		"channel": channel,
		"status":  "accepted",
	}
	if mid != "" {
		ack["mid"] = mid
	}

	if data, err := json.Marshal(ack); err == nil {
		select {
		case c.send <- RawMsg(data):
			// Ack sent successfully
		default:
			// Client buffer full - skip ack (not critical, message was published)
		}
	}
}

// sendPublishError sends a publish error response to the client.
func (s *Server) sendPublishError(c *Client, code protocol.ErrorCode, message string) {
	s.sendErrorToClient(c, protocol.RespTypePublishError, code, message)
}
