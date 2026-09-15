package server

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// handleReplayRequest handles a "replay" client message for live gap recovery.
// Guards: (a) unmarshal, (b) subscribed, (g) edition gate, (c) from_pos parse, (d) rate limit,
// (e) in-progress, (f) backend topic. Then launches an async delivery goroutine.
func (s *Server) handleReplayRequest(c *Client, data []byte) {
	var req liveReplayRequest
	if err := json.Unmarshal(data, &req); err != nil {
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed).Inc()
		s.sendErrorToClient(c, MsgTypeError, protocol.ErrCodeInvalidRequest, "invalid replay request")
		return
	}

	// (a) Client must be subscribed to the channel.
	if !c.subscriptions.Has(req.Channel) {
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedNotSubscribed).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeNotSubscribed, "subscribe before replaying")
		return
	}

	// (g) removed — LiveGapRecovery is Community per ADR-0009; no edition gate.

	// (b) Parse from_pos into a Kafka partition + offset. The partition is
	// preserved: the cursor identifies a record on ONE partition (records are
	// keyed by channel), and the exact offset may be out of range on the
	// topic's other partitions.
	partition, offset, ok := history.DecodePos(req.FromPos)
	if !ok {
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeInvalidRequest, "invalid from_pos format; expected (partition+1)-offset")
		return
	}

	// (c) In-progress guard — checked first so a concurrent replay returns the accurate error.
	// Must be under historyMu (§VII: protects replayInProgress and replayLastAt maps).
	c.historyMu.Lock()
	if _, running := c.replayInProgress[req.Channel]; running {
		c.historyMu.Unlock()
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedInProgress).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeReplayInProgress, "replay already in progress for this channel")
		return
	}

	// (d) Rate limit — checked second so in-progress takes precedence over rate-limit semantics.
	if last, hasLast := c.replayLastAt[req.Channel]; hasLast && time.Since(last) < s.config.ReplayRateLimitInterval {
		c.historyMu.Unlock()
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedRateLimited).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeReplayRateLimited, "replay rate limit exceeded; please wait before retrying")
		return
	}
	c.replayInProgress[req.Channel] = struct{}{}
	c.historyMu.Unlock()

	// (e) Backend must know about this channel's topic.
	if s.backend == nil {
		clearReplayInProgress(c, req.Channel)
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeNotAvailable, "replay not available (no backend configured)")
		return
	}
	topic, ok := s.backend.ChannelTopic(req.Channel)
	if !ok {
		clearReplayInProgress(c, req.Channel)
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed).Inc()
		sendReplayError(c, req.Channel, protocol.ErrCodeNotAvailable, "channel not available for replay")
		return
	}

	c.clientWg.Go(func() {
		// pos1: panic recovery — runs last in LIFO, catches everything else.
		defer logging.RecoverPanic(s.logger, "liveReplay", map[string]any{"channel": req.Channel})

		// pos2: clear in-progress flag so subsequent requests can proceed.
		// §VII: defer the clear so any exit path (error, timeout, disconnect) releases the flag.
		defer clearReplayInProgress(c, req.Channel)

		deliveryCtx, cancel := context.WithTimeout(c.clientCtx, s.config.ReplayTimeout)
		defer cancel()

		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeStarted).Inc()
		replayStart := time.Now()
		defer func() { metrics.LiveReplayDurationSeconds.Observe(time.Since(replayStart).Seconds()) }()

		replayedMsgs, err := s.backend.Replay(deliveryCtx, backend.ReplayRequest{
			Positions:     map[string]map[int32]int64{topic: {partition: offset}}, // replay from the anchor (the dropped message itself)
			MaxMessages:   s.config.MaxReplayMessages,
			Subscriptions: []string{req.Channel},
		})

		if err != nil {
			if errors.Is(err, kerr.OffsetOutOfRange) {
				metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedOutOfRange).Inc()
				sendReplayError(c, req.Channel, protocol.ErrCodeOffsetOutOfRange, "position is outside available history")
				return
			}
			s.logger.Error().Err(err).
				Str("channel", req.Channel).
				Int64("client_id", c.id).
				Msg("live replay: backend error")
			metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed).Inc()
			sendReplayError(c, req.Channel, protocol.ErrCodeReplayFailed, "replay failed")
			return
		}

		// nil or empty result: DirectBackend no-op or empty range.
		if len(replayedMsgs) == 0 {
			s.logger.Debug().
				Str("channel", req.Channel).
				Int64("client_id", c.id).
				Msg("live replay: nil/empty result from backend (DirectBackend or empty range)")
			sendReplayComplete(c, req.Channel, 0, false)
			metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeCompleted).Inc()
			return
		}

		// Stream replay_message envelopes.
		count := 0
		for _, msg := range replayedMsgs {
			// Validate msg.Data before consuming a sequence number so that malformed
			// Kafka payloads do not create phantom seq-gaps in the client's stream.
			// json.RawMessage causes json.Marshal to fail if the bytes are not valid JSON.
			if !json.Valid(msg.Data) {
				s.logger.Error().
					Str("channel", req.Channel).
					Str("pos", msg.Pos).
					Msg("live replay: skipping message with invalid JSON payload")
				continue
			}
			env := replayMessageEnvelope{
				Type:    RespTypeReplayMessage,
				Seq:     c.seqGen.Next(),
				Channel: msg.Subject,
				Ts:      time.Now().UnixMilli(),
				Data:    json.RawMessage(msg.Data),
				Pos:     msg.Pos,
				Mid:     msg.Mid,
			}
			envData, err := json.Marshal(env)
			if err != nil {
				// Unreachable: msg.Data validated above; all other fields are JSON-safe.
				s.logger.Error().Err(err).
					Str("channel", req.Channel).
					Str("pos", msg.Pos).
					Msg("live replay: failed to marshal replay_message envelope")
				continue
			}
			// Blocking send (no default): replay is ordered and reliable.
			// Unlike history (which drops if c.send is full), we hold until the buffer
			// drains or deliveryCtx times out (ReplayTimeout bound). Goroutine lifetime
			// is bounded; slow-client pressure is tracked via LiveReplayOutcomeTimeout.
			// §XV: explicit design choice documented here, not a missing default case.
			select {
			case c.send <- RawMsg(envData):
				count++
			case <-deliveryCtx.Done():
				// Delivery cut short — send truncated replay_complete.
				sendReplayComplete(c, req.Channel, count, true)
				metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeTimeout).Inc()
				return
			}
		}

		// SUCCESS PATH: update rate-limit timestamp only here (not in defer).
		// A failed replay must not count against the rate limit.
		c.historyMu.Lock()
		c.replayLastAt[req.Channel] = time.Now()
		c.historyMu.Unlock()

		sendReplayComplete(c, req.Channel, count, false)
		metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeCompleted).Inc()
	})
}

// sendReplayComplete sends a non-blocking replay_complete envelope to the client.
// truncated must be true when delivery was cut short by timeout or client disconnect.
// Parallel to sendHistoryComplete in handler_history.go.
func sendReplayComplete(c *Client, channel string, count int, truncated bool) {
	env := replayCompleteEnvelope{
		Type:             RespTypeReplayComplete,
		Channel:          channel,
		MessagesReplayed: count,
		Truncated:        truncated,
	}
	data, err := json.Marshal(env)
	if err != nil {
		// Unreachable: replayCompleteEnvelope contains only JSON-safe types (string, int, bool).
		return
	}
	select {
	case c.send <- RawMsg(data):
	default:
		metrics.RecordDroppedReplayResponse(ReplayResponseKindComplete)
	}
}

// sendReplayError sends a non-blocking error envelope to the client.
// Uses MsgTypeError per acceptance criteria.
// §XVIII documented exception: differs from sendHistoryError (which uses "history_error");
// replay errors use the current standard generic error type.
func sendReplayError(c *Client, channel string, code protocol.ErrorCode, message string) {
	env := replayErrorEnvelope{
		Type:    MsgTypeError,
		Channel: channel,
		Code:    string(code),
		Message: message,
	}
	data, err := json.Marshal(env)
	if err != nil {
		// Unreachable: replayErrorEnvelope contains only JSON-safe types (string).
		return
	}
	select {
	case c.send <- RawMsg(data):
	default:
		metrics.RecordDroppedReplayResponse(ReplayResponseKindError)
	}
}

// clearReplayInProgress removes the channel from the client's in-progress replay map.
// §XVIII: parallel to clearInProgress in handler_history.go.
func clearReplayInProgress(c *Client, channel string) {
	c.historyMu.Lock()
	delete(c.replayInProgress, channel)
	c.historyMu.Unlock()
}
