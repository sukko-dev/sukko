package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// authorizeLastPos validates a per-channel last_pos cursor for replay and
// returns the Kafka replay positions (topic→partition→startOffset) plus the
// authorized channel filter. For each channel it enforces tenant ownership
// (ADR-0020, fail-closed on empty tenant), decodes the pos, maps the channel to
// its Kafka topic, and caps the number of genuinely-new channels at
// MaxChannelsPerClient (§IX) — a held channel adds zero fan-out and is always
// authorized. Denied/undecodable/over-cap channels are skipped, logged, and
// counted; they never reach the backend. Shared by the WebSocket reconnect path
// and the SSE Subscribe-with-last_pos path (ADR-0030).
func (s *Server) authorizeLastPos(c *Client, lastPos map[string]string) (positions map[string]map[int32]int64, authorized []string) {
	positions = make(map[string]map[int32]int64, len(lastPos))
	authorized = make([]string, 0, len(lastPos))
	newChannels := 0 // channels not already held; only these grow the subscription set toward the cap
	for channel, posStr := range lastPos {
		// L1 authorization: a client may only replay channels owned by its
		// authenticated tenant (§IX). ValidateChannelTenant fails closed on an
		// empty tenant, so a forged or cross-tenant channel replays nothing.
		if !auth.ValidateChannelTenant(channel, c.TenantID()) {
			metrics.ReconnectChannelDenied.Inc()
			s.logger.Warn().
				Int64("client_id", c.id).
				Str("channel", channel).
				Str(logging.LogKeyTenantSlug, c.TenantID()).
				Msg("replay: channel not owned by tenant — denied")
			continue
		}
		partition, offset, ok := history.DecodePos(posStr)
		if !ok {
			metrics.ReconnectPosDecodeFailures.Inc()
			s.logger.Warn().
				Int64("client_id", c.id).
				Str("channel", channel).
				Str("pos", posStr).
				Msg("replay: undecodable pos — channel skipped")
			continue
		}
		topic, ok := s.backend.ChannelTopic(channel)
		if !ok {
			metrics.ReconnectPosDecodeFailures.Inc()
			s.logger.Warn().
				Int64("client_id", c.id).
				Str("channel", channel).
				Msg("replay: no topic mapping for channel — channel skipped")
			continue
		}
		// Cap only genuinely-new channels: a held channel adds zero live fan-out
		// and must still be replayed even at the limit (counting it twice would
		// starve a client re-homing its own channels). Skip the over-budget new
		// channel and keep scanning so held channels later in map order survive.
		if !c.subscriptions.Has(channel) {
			if c.subscriptions.Count()+newChannels >= s.config.MaxChannelsPerClient {
				metrics.ReconnectChannelLimitExceeded.Inc()
				s.logger.Warn().
					Int64("client_id", c.id).
					Str("channel", channel).
					Int("limit", s.config.MaxChannelsPerClient).
					Msg("replay: new channel over per-client limit — channel skipped")
				continue
			}
			newChannels++
		}
		nextOffset := offset + 1 // replay from the message AFTER the last received
		if positions[topic] == nil {
			positions[topic] = make(map[int32]int64)
		}
		if existing, alreadySet := positions[topic][partition]; !alreadySet || nextOffset < existing {
			positions[topic][partition] = nextOffset
		}
		authorized = append(authorized, channel)
	}
	return positions, authorized
}

// replayAuthorizedToClient replays the Kafka positions (scoped to authorized —
// the tenant-validated channel filter, ADR-0020) and forwards each replayed
// message to the client's send queue as a sequenced MessageEnvelope, returning
// the count forwarded and whether the send buffer filled (truncated). The caller
// MUST have registered live delivery for these channels BEFORE calling, so the
// replay and live windows overlap (ADR-0026) and the client dedupes the overlap
// by mid (ADR-0008). Shared by WebSocket reconnect and SSE Subscribe (ADR-0030).
func (s *Server) replayAuthorizedToClient(c *Client, positions map[string]map[int32]int64, authorized []string) (replayedCount int, truncated bool, err error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.config.ReplayTimeout)
	defer cancel()

	replayedMsgs, err := s.backend.Replay(ctx, backend.ReplayRequest{
		Positions:     positions,
		MaxMessages:   s.config.MaxReplayMessages,
		Subscriptions: authorized,
	})
	if err != nil {
		return 0, false, fmt.Errorf("history backend replay: %w", err)
	}

	for _, msg := range replayedMsgs {
		envelope := &messaging.MessageEnvelope{
			Type:      MsgTypeMessage,
			Seq:       c.seqGen.Next(),
			Timestamp: time.Now().UnixMilli(),
			Channel:   msg.Subject,
			Priority:  messaging.PriorityNormal,
			Data:      json.RawMessage(msg.Data),
			Pos:       msg.Pos,
			Mid:       msg.Mid,
		}
		envelopeData, serErr := envelope.Serialize()
		if serErr != nil {
			s.logger.Warn().
				Err(serErr).
				Int64("client_id", c.id).
				Str("channel", msg.Subject).
				Msg("Failed to serialize replay message")
			continue
		}
		select {
		case c.send <- RawMsg(envelopeData):
			replayedCount++
		default:
			s.logger.Warn().
				Int64("client_id", c.id).
				Msg("Client send buffer full during replay, stopping")
			return replayedCount, true, nil
		}
	}
	return replayedCount, false, nil
}

// replayControlEnvelope is a server→client control frame reporting a reconnect-replay outcome the
// client cannot infer from the message stream (ADR-0030 §XV). For no_replay, Channels names the
// affected cursor channels (treat as a possible gap); for replay_truncated, Replayed is the count
// delivered before the cut.
type replayControlEnvelope struct {
	Type     string   `json:"type"`
	Channels []string `json:"channels,omitempty"`
	// Replayed has NO omitempty: replay_truncated{replayed:0} is reachable (the send buffer was
	// full at the very first replay message), and the contract documents the field unconditionally.
	Replayed int `json:"replayed"`
}

// replayOutcome decides the explicit reconnect-recovery signals (ADR-0030 §XV) from a replay
// attempt: the cursor channels to report as no_replay, and whether the replay was truncated. A
// replay that errored delivered nothing, so every cursor channel is a gap; otherwise the no_replay
// set is the cursor channels that weren't replay-eligible (unauthorized / no Kafka mapping), and
// truncation is reported as-is. The two are independent and may co-occur.
func replayOutcome(lastPos map[string]string, authorized []string, truncated bool, replayErr error) (noReplay []string, isTruncated bool) {
	if replayErr != nil {
		return sortedCursorChannels(lastPos), false
	}
	return unreplayableCursorChannels(lastPos, authorized), truncated
}

// sendReplayControl delivers one replay-outcome control frame, blocking (bounded by ctx) until the
// write pump drains a send slot. Unlike the replay messages' non-blocking send, this signal MUST
// NOT be dropped — and for replay_truncated the send buffer is full by definition, so a
// non-blocking send would drop exactly the message that matters. A ctx cancel (client gone)
// abandons it.
func (s *Server) sendReplayControl(ctx context.Context, c *Client, env replayControlEnvelope) {
	data, _ := json.Marshal(env) // only JSON-safe types; Marshal cannot fail
	select {
	case c.send <- RawMsg(data):
	case <-ctx.Done():
	}
}

// unreplayableCursorChannels returns, sorted, the cursor channels that were NOT replay-eligible —
// the keys of lastPos not present in authorized (unauthorized, or no Kafka mapping on a direct
// backend). Channels the gateway's cursor∩channels intersect (#34) already dropped never appear in
// lastPos, so they are correctly not reported (the client unsubscribed them).
func unreplayableCursorChannels(lastPos map[string]string, authorized []string) []string {
	ok := make(map[string]struct{}, len(authorized))
	for _, ch := range authorized {
		ok[ch] = struct{}{}
	}
	var out []string
	for ch := range lastPos {
		if _, found := ok[ch]; !found {
			out = append(out, ch)
		}
	}
	slices.Sort(out)
	return out
}

// sortedCursorChannels returns all cursor channels sorted (the no_replay set when replay failed
// outright — nothing was replayed).
func sortedCursorChannels(lastPos map[string]string) []string {
	out := make([]string, 0, len(lastPos))
	for ch := range lastPos {
		out = append(out, ch)
	}
	slices.Sort(out)
	return out
}
