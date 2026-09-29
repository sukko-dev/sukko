package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// Client message handlers
func (s *Server) handleClientMessage(c *Client, data []byte) {
	// Parse outer message structure
	var req protocol.ClientMessage

	if err := json.Unmarshal(data, &req); err != nil {
		s.logger.Warn().
			Int64("client_id", c.id).
			Err(err).
			Msg("Client sent invalid JSON")
		s.sendErrorToClient(c, MsgTypeError, protocol.ErrCodeInvalidJSON, "Message is not valid JSON")
		return
	}

	switch req.Type {
	case MsgTypeReconnect:
		// BACKEND-BASED RECONNECTION
		// Client reconnecting after disconnect, requesting missed messages from message backend.
		// Supports Kafka offset-based replay. Direct mode returns immediately (no replay).
		s.handleReconnect(c, req.Data)

	case MsgTypeHeartbeat:
		// Client keep-alive ping
		// Some older browsers/libraries don't support WebSocket ping/pong
		// So they send application-level heartbeats
		//
		// We respond with current server time (helps detect clock skew)
		pong := map[string]any{
			"type": MsgTypePong,
			"ts":   time.Now().UnixMilli(),
		}

		if data, err := json.Marshal(pong); err == nil {
			select {
			case c.send <- RawMsg(data):
				// Sent
			default:
				// If can't send pong, client buffer is full
				// Don't worry about it, regular heartbeat will fail eventually
			}
		}

	case protocol.MsgTypeSubscribe:
		// Client subscribing to hierarchical channels (tenant.symbol.eventType)
		// Message format: {"type": "subscribe", "data": {"channels": ["sukko.BTC.trade", "sukko.ETH.trade", "sukko.BTC.analytics"]}}
		//
		// Channel format: "{TENANT}.{SYMBOL}.{EVENT_TYPE}"
		// - TENANT: Tenant identifier (sukko, acme, etc.)
		// - SYMBOL: Token symbol (BTC, ETH, SOL, etc.)
		// - EVENT_TYPE: One of 8 types (trade, liquidity, metadata, social, favorites, creation, analytics, balances)
		//
		// Benefits:
		// - Granular control: Subscribe only to event types you need
		// - Reduced bandwidth: Trading clients don't receive social/metadata events
		// - Reduced CPU: 8x fewer messages per subscribed symbol vs symbol-only subscription
		// - Better UX: Clients see only relevant updates
		//
		// Example use cases:
		// - Trading client: ["sukko.BTC.trade", "sukko.ETH.trade"] - Only price updates
		// - Dashboard: ["sukko.BTC.trade", "sukko.BTC.analytics", "sukko.ETH.trade", "sukko.ETH.analytics"] - Prices + metrics
		// - Social app: ["sukko.BTC.social", "sukko.ETH.social"] - Only comments/reactions
		//
		// Performance impact (10K clients, 200 tokens, 12 msg/sec):
		// - Without filtering: 12 × 8 events × 10K = 960K writes/sec (CPU overload)
		// - With hierarchical: 12 × avg 2K subscribers = 24K writes/sec (CPU <30%)
		// - Result: 40x reduction in broadcast overhead
		var subReq protocol.SubscribeData

		if err := json.Unmarshal(req.Data, &subReq); err != nil {
			s.logger.Warn().
				Int64("client_id", c.id).
				Err(err).
				Msg("Client sent invalid subscribe request")
			s.sendErrorToClient(c, RespTypeSubscribeError, protocol.ErrCodeInvalidRequest, "Invalid subscribe request format")
			return
		}

		// Determine the channels to subscribe to.
		channels := subReq.Channels
		if subReq.History != nil {
			// Subscribe-with-history: single channel.
			if subReq.Channel == "" {
				s.sendErrorToClient(c, RespTypeSubscribeError, protocol.ErrCodeInvalidRequest, "channel is required for subscribe-with-history")
				return
			}
			channels = []string{subReq.Channel}

			// Check (a): history feature must be enabled.
			if !s.config.HistoryEnabled || s.historyWriter == nil {
				sendHistoryError(c, subReq.Channel, history.ErrCodeHistoryDisabled, "message history is not enabled")
				return
			}
			// Check (b) removed — MessageHistory is Community per ADR-0009.
		}

		if len(channels) == 0 {
			s.sendErrorToClient(c, RespTypeSubscribeError, protocol.ErrCodeInvalidRequest, "no channels specified")
			return
		}

		// Enforce per-client channel limit before subscribing. Count only channels NOT already
		// subscribed: re-subscribing a channel the client already holds is idempotent and must not
		// consume limit budget. This makes the resume `subscribe` after a reconnect (which re-sends
		// the whole desired set, already registered by handleReconnect per ADR-0026) a free re-add,
		// and still rejects a batch that would push the resulting set past the cap.
		newChannels := 0
		for _, ch := range channels {
			if !c.subscriptions.Has(ch) {
				newChannels++
			}
		}
		if c.subscriptions.Count()+newChannels > s.config.MaxChannelsPerClient {
			s.logger.Warn().
				Int64("client_id", c.id).
				Int("limit", s.config.MaxChannelsPerClient).
				Int("requested_new", newChannels).
				Int("current", c.subscriptions.Count()).
				Msg("Client exceeded channel subscription limit")
			s.sendErrorToClient(c, RespTypeSubscribeError, protocol.ErrCodeSubscribeLimitExceeded, "channel subscription limit reached")
			return
		}

		// Add subscriptions to client's local set
		c.subscriptions.AddMultiple(channels)

		// Add to global subscription index for fast broadcast targeting
		s.subscriptionIndex.AddMultiple(channels, c)

		// Push subscribe event to registry (snapshot taken after add, non-blocking).
		if s.config != nil && s.config.ConnectionsRegistryEnabled && s.registryWriter != nil && c.connID != "" {
			snapshot := c.subscriptions.List()
			capped := len(snapshot) >= s.config.MaxChannelsPerClient
			s.registryWriter.PushSubscribe(c.connID, snapshot, capped)
		}

		s.logger.Info().
			Int64("client_id", c.id).
			Int("channel_count", len(channels)).
			Strs("channels", channels).
			Msg("Client subscribed")

		// Send acknowledgment to client
		ack := map[string]any{
			"type":       RespTypeSubscriptionAck,
			"subscribed": channels,
			"count":      c.subscriptions.Count(),
		}

		if data, err := json.Marshal(ack); err == nil {
			select {
			case c.send <- RawMsg(data):
				// Ack sent successfully
			default:
				// Client buffer full - skip ack (not critical)
			}
		}

		// After subscribe, trigger history delivery if requested.
		if subReq.History != nil {
			s.handleHistoryRequest(c, HistoryRequest{
				Channel: subReq.Channel,
				Limit:   subReq.History.Limit,
			})
		}

	case protocol.MsgTypeUnsubscribe:
		// Client unsubscribing from channels
		// Message format: {"type": "unsubscribe", "data": {"channels": ["BTC"]}}
		var unsubReq UnsubscribeData

		if err := json.Unmarshal(req.Data, &unsubReq); err != nil {
			s.logger.Warn().
				Int64("client_id", c.id).
				Err(err).
				Msg("Client sent invalid unsubscribe request")
			s.sendErrorToClient(c, RespTypeUnsubscribeError, protocol.ErrCodeInvalidRequest, "Invalid unsubscribe request format")
			return
		}

		// Remove subscriptions from client's local set
		c.subscriptions.RemoveMultiple(unsubReq.Channels)

		// Remove from global subscription index
		s.subscriptionIndex.RemoveMultiple(unsubReq.Channels, c)

		// Push unsubscribe event to registry (snapshot taken after removal, non-blocking).
		if s.config != nil && s.config.ConnectionsRegistryEnabled && s.registryWriter != nil && c.connID != "" {
			snapshot := c.subscriptions.List()
			capped := len(snapshot) >= s.config.MaxChannelsPerClient
			s.registryWriter.PushUnsubscribe(c.connID, snapshot, capped)
		}

		s.logger.Info().
			Int64("client_id", c.id).
			Int("channel_count", len(unsubReq.Channels)).
			Strs("channels", unsubReq.Channels).
			Msg("Client unsubscribed")

		// Send acknowledgment to client
		ack := map[string]any{
			"type":         RespTypeUnsubscriptionAck,
			"unsubscribed": unsubReq.Channels,
			"count":        c.subscriptions.Count(),
		}

		if data, err := json.Marshal(ack); err == nil {
			select {
			case c.send <- RawMsg(data):
				// Ack sent successfully
			default:
				// Client buffer full - skip ack (not critical)
			}
		}

	case MsgTypeReplay:
		// Live gap recovery — replay missed messages on the existing connection without reconnect.
		// Message format: {"type": "replay", "data": {"channel": "...", "from_pos": "..."}}
		// See handler_replay.go for the full implementation.
		s.handleReplayRequest(c, req.Data)

	case MsgTypeHistory:
		// Standalone history request (client already subscribed separately).
		// Message format: {"type": "history", "data": {"channel": "acme.BTC.trade", "limit": 50}}
		var histReq HistoryRequest
		if err := json.Unmarshal(req.Data, &histReq); err != nil {
			s.logger.Warn().
				Int64("client_id", c.id).
				Err(err).
				Msg("Client sent invalid history request")
			// Best-effort: extract channel so the client can correlate the error.
			var partial struct {
				Channel string `json:"channel"`
			}
			_ = json.Unmarshal(req.Data, &partial)
			sendHistoryError(c, partial.Channel, history.ErrCodeHistoryInvalidRequest, "invalid history request format")
			return
		}
		s.handleHistoryRequest(c, histReq)

	case protocol.MsgTypePublish:
		// Client publishing a message via the message backend
		// Message format: {"type": "publish", "data": {"channel": "community.group123.chat", "data": {...}}}
		//
		// This enables bidirectional WebSocket messaging:
		// - Clients can publish messages through the configured backend
		// - Messages are routed to subscribers via the backend's consume loop
		//
		// Security: Authentication is handled by ws-gateway before requests reach here.
		s.handleClientPublish(c, req.Data)

	default:
		// Unknown message type
		// Log but don't disconnect (might be future feature we haven't implemented yet)
		s.logger.Warn().
			Int64("client_id", c.id).
			Str("message_type", req.Type).
			Msg("Client sent unknown message type")
	}
}

// handleReconnect handles client reconnection using the message backend's replay capability.
func (s *Server) handleReconnect(c *Client, data []byte) {
	var reconnectReq struct {
		ClientID string            `json:"client_id"` // Persistent client identifier
		LastPos  map[string]string `json:"last_pos"`  // Last pos per channel: {channel: "(partition+1)-offset"}
	}

	if err := json.Unmarshal(data, &reconnectReq); err != nil {
		s.logger.Warn().
			Int64("client_id", c.id).
			Err(err).
			Msg("Client sent invalid reconnect request")

		s.sendErrorToClient(c, RespTypeReconnectError, protocol.ErrCodeInvalidRequest, "Invalid reconnect request format")
		return
	}

	s.logger.Info().
		Int64("client_id", c.id).
		Str("persistent_id", reconnectReq.ClientID).
		Interface("last_pos", reconnectReq.LastPos).
		Msg("Client requesting message replay")

	// Check if backend is available for replay
	if s.backend == nil {
		s.logger.Warn().
			Int64("client_id", c.id).
			Msg("Message replay requested but no backend available")

		s.sendErrorToClient(c, RespTypeReconnectError, protocol.ErrCodeNotAvailable, "Message replay not available (no backend configured)")
		return
	}

	// Validate the client's last_pos cursor: tenant-scope each channel (ADR-0020),
	// decode positions, map to Kafka topics, and cap new channels (§IX). Shared
	// with the SSE Subscribe-with-last_pos path (ADR-0030).
	positions, authorizedChannels := s.authorizeLastPos(c, reconnectReq.LastPos)

	// Nothing left to replay after authorization/decoding (e.g. every last_pos
	// channel was denied or undecodable): send a clean zero-message ack, not a
	// REPLAY_FAILED. The backend rejects an empty positions map with "at least
	// one topic offset is required", which would misrepresent an authz denial as
	// a server failure.
	if len(positions) == 0 {
		ackMsg := map[string]any{
			"type":              RespTypeReconnectAck,
			"status":            "completed",
			"messages_replayed": 0,
			"message":           "No replayable channels",
		}
		if ackData, err := json.Marshal(ackMsg); err == nil {
			select {
			case c.send <- RawMsg(ackData):
			default:
			}
		}
		return
	}

	// ADR-0026: register live delivery for the tenant-validated last_pos channels BEFORE replaying,
	// closing the replay↔live seam. On the reconnect-then-subscribe protocol, the replay is a Kafka
	// snapshot up to now and live delivery would otherwise begin only when a later `subscribe`
	// registers the client — so a message reaching the bus after the snapshot but before that
	// subscribe is lost by both. Registering first means the two windows overlap; the client dedupes
	// the overlap by `mid` (ADR-0008), exactly as it already does for mid-session gap replay. The
	// client's subsequent explicit subscribe is an idempotent re-add. Both calls are non-blocking
	// map operations and live fan-out is already non-blocking, so the hot path is unaffected (§VII).
	c.subscriptions.AddMultiple(authorizedChannels)
	s.subscriptionIndex.AddMultiple(authorizedChannels, c)

	// Push subscribe event to registry so the connections view reflects the live subscriptions
	// registered on reconnect — not only those from a later explicit subscribe (§XVIII: same
	// pairing as the subscribe path; snapshot after add, non-blocking).
	if s.config != nil && s.config.ConnectionsRegistryEnabled && s.registryWriter != nil && c.connID != "" {
		snapshot := c.subscriptions.List()
		capped := len(snapshot) >= s.config.MaxChannelsPerClient
		s.registryWriter.PushSubscribe(c.connID, snapshot, capped)
	}

	replayedCount, _, err := s.replayAuthorizedToClient(c, positions, authorizedChannels)
	if err != nil {
		s.logger.Error().
			Int64("client_id", c.id).
			Err(err).
			Msg("Failed to replay messages from backend")
		s.sendErrorToClient(c, RespTypeReconnectError, protocol.ErrCodeReplayFailed, "Message replay failed")
		return
	}

	// Send acknowledgment with replay statistics
	ackMsg := map[string]any{
		"type":              RespTypeReconnectAck,
		"status":            "completed",
		"messages_replayed": replayedCount,
		"message":           fmt.Sprintf("Replayed %d missed messages", replayedCount),
	}

	if ackData, err := json.Marshal(ackMsg); err == nil {
		select {
		case c.send <- RawMsg(ackData):
		default:
		}
	}

	s.logger.Info().
		Int64("client_id", c.id).
		Int("messages_replayed", replayedCount).
		Msg("Message replay completed successfully")

	// Increment reconnect counter for monitoring
	s.stats.MessageReplayRequests.Add(1)
	metrics.IncrementReplayRequests()
}

// sendErrorToClient sends an error response to the client.
// Used for all error types (message parsing, subscribe, unsubscribe, reconnect, publish).
func (s *Server) sendErrorToClient(c *Client, errType string, code protocol.ErrorCode, message string) {
	errResp := map[string]any{
		"type":    errType,
		"code":    code,
		"message": message,
	}

	if data, err := json.Marshal(errResp); err == nil {
		select {
		case c.send <- RawMsg(data):
			// Error sent
		default:
			// Client buffer full - non-blocking per graceful degradation guidelines
		}
	}
}
