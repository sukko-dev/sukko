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

	serializeFailed := false
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
			serializeFailed = true // a record did not reach the client — recovery is incomplete
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
	// The Kafka replay caps at MaxReplayMessages (a cross-channel total): a full batch means more may
	// remain unrecovered, and a replay-timeout mid-fetch returns a short batch with the replay ctx
	// expired. Either is an incomplete recovery (ADR-0031) — report truncated conservatively so the
	// client treats the affected channels as a possible gap. A false positive at the exact-cap
	// boundary is the safe direction versus silent loss. A serialize failure likewise means a record
	// did not reach the client.
	truncated = len(replayedMsgs) >= s.config.MaxReplayMessages || ctx.Err() != nil || serializeFailed
	return replayedCount, truncated, nil
}

// replayControlEnvelope is a server→client SSE reconnect-recovery control frame the client cannot
// infer from the message stream (ADR-0031, extends ADR-0030 §XV). For no_replay, Channels is the
// possible-gap set (requested − recovered); for recovery_complete, only Type is set; for
// replay_truncated, Replayed carries the delivered count.
type replayControlEnvelope struct {
	Type     string   `json:"type"`
	Channels []string `json:"channels,omitempty"`
	// Replayed is a pointer so it is emitted ONLY on replay_truncated — a present zero
	// (replay_truncated{replayed:0}) is reachable when the send buffer was full at the first replay
	// message. no_replay and recovery_complete leave it nil, so the field is omitted.
	Replayed *int `json:"replayed,omitempty"`
}

// recoveryOutcome computes the SSE reconnect-recovery verdict (ADR-0031): the subscribed channels
// that are a POSSIBLE GAP (requested − recovered) and whether the replay truncated. A channel is
// recovered only if it was in the cursor, authorized, AND fully replayed — no replay error and no
// truncation. Everything else the client subscribed to is a possible gap: unreplayable cursor
// channels, "quiet" channels requested with no cursor baseline (requested − cursor), and — because
// MaxReplayMessages caps a cross-channel total, so truncation cannot be attributed per channel —
// every authorized cursor channel when the replay truncated or errored. The result is deduplicated
// and sorted. With the recovery_complete sentinel, the client treats exactly these as possible gaps
// and every other subscribed channel as recovered.
func recoveryOutcome(requested, authorized []string, truncated bool, replayErr error) (possibleGap []string, isTruncated bool) {
	if replayErr != nil {
		// A failed replay delivered nothing and did not truncate: report no replay_truncated frame,
		// just the full possible-gap set below. (replayAuthorizedToClient upholds this, but normalize
		// defensively so a future caller cannot emit replay_truncated with a count from a failed replay.)
		truncated = false
	}
	recovered := make(map[string]struct{}, len(authorized))
	if replayErr == nil && !truncated {
		for _, ch := range authorized {
			recovered[ch] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(requested))
	for _, ch := range requested {
		if _, ok := recovered[ch]; ok {
			continue
		}
		if _, dup := seen[ch]; dup {
			continue
		}
		seen[ch] = struct{}{}
		possibleGap = append(possibleGap, ch)
	}
	slices.Sort(possibleGap)
	return possibleGap, truncated
}

// sendReplayControl delivers one reconnect-recovery control frame, blocking (bounded by ctx) until
// the write pump drains a send slot. Unlike the replay messages' non-blocking send, this signal MUST
// NOT be dropped — and for replay_truncated the send buffer is full by definition, so a non-blocking
// send would drop exactly the message that matters. A ctx cancel (client gone) abandons it.
func (s *Server) sendReplayControl(ctx context.Context, c *Client, env replayControlEnvelope) {
	data, _ := json.Marshal(env) // only JSON-safe types; Marshal cannot fail
	select {
	case c.send <- RawMsg(data):
	case <-ctx.Done():
	}
}
