package runner

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// SuiteGapRecovery proves BOTH gap-recovery legs of the ADR-0009 free data path
// end to end on the kafka backend — the benchmark story's core mechanism:
//   - reconnect replay ("under kill"): a client that died mid-stream reconnects
//     with reconnect{client_id, last_pos} and receives everything it missed as
//     plain message frames before reconnect_ack
//   - live replay: replay{channel, from_pos} re-delivers records after the
//     cursor as replay_message frames terminated by replay_complete
//
// Both legs also assert mid identity against a control subscriber's live copies
// — completing the ADR-0008 triple (live == history == replay) together with
// the history suite.
const SuiteGapRecovery = "gap-recovery"

func validateGapRecovery(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	if pre := recoverySetupError(run, SuiteGapRecovery); pre != nil {
		return pre, nil
	}
	if fail := setupRecoveryChannel(ctx, run, logger); fail != nil {
		return []metrics.CheckResult{*fail}, nil
	}
	tenantID := run.authResult.TenantID
	channel := tenantID + "." + recoveryChannelSuffix

	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
		Timeout:    kafkaIngestDeliveryTimeout,
	})

	// Control subscriber C: connected throughout, receives every record live —
	// the mid reference both replay legs compare against.
	control, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "gap-control", ConnIndex: 0})
	if err != nil {
		return []metrics.CheckResult{{Name: "create control subscriber", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() { _ = control.Client.Close() }() // best-effort test cleanup
	if err := control.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe control", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	// Victim subscriber A1: will be hard-killed mid-stream.
	victim, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "gap-victim", ConnIndex: 1})
	if err != nil {
		return []metrics.CheckResult{{Name: "create victim subscriber", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	victimOpen := true
	defer func() {
		if victimOpen {
			_ = victim.Client.Close() // best-effort test cleanup
		}
	}()
	if err := victim.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe victim", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	pub, err := recoveryPublisher(run, tenantID)
	if err != nil {
		return []metrics.CheckResult{{Name: "kafka-connect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("could not reach Kafka at %s: %v", run.kafkaBrokers, err)}}, nil
	}
	defer func() { _ = pub.Close() }() // best-effort test cleanup

	// M1: both receive live; the victim's pos on M1 is the recovery anchor.
	m1 := ingestTracked(ctx, engine, pub, channel, []*TestUser{control, victim}, []*TestUser{control, victim})
	if !m1.Delivered {
		return []metrics.CheckResult{deliveryCheck("ingest m1", m1)}, nil
	}
	checks := make([]metrics.CheckResult, 0, 8)
	_, anchorPos, _, _ := victim.ReceivedMeta(m1.MessageID)
	if anchorPos == "" {
		checks = append(checks, metrics.CheckResult{Name: "pos present", Status: metrics.CheckStatusFail,
			Error: "live message carries no pos cursor — is the server on MESSAGE_BACKEND=kafka?"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "pos present", Status: metrics.CheckStatusPass})

	// Kill the victim mid-stream, then ingest the records it will miss.
	_ = victim.Client.Close()
	victimOpen = false
	m2 := ingestTracked(ctx, engine, pub, channel, []*TestUser{control}, []*TestUser{control})
	if !m2.Delivered {
		return append(checks, deliveryCheck("ingest m2", m2)), nil
	}
	m3 := ingestTracked(ctx, engine, pub, channel, []*TestUser{control}, []*TestUser{control})
	if !m3.Delivered {
		return append(checks, deliveryCheck("ingest m3", m3)), nil
	}
	gapIDs := []string{m2.MessageID, m3.MessageID}

	// ── Leg 1: reconnect replay (under kill) ──────────────────────────────────
	// New connection, re-subscribe (server guard), then reconnect{client_id,
	// last_pos}: everything after the anchor replays as plain message frames
	// before reconnect_ack. client_id is any client-chosen string (stateless).
	revived, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "gap-victim", ConnIndex: 2})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "reconnect", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	defer func() { _ = revived.Client.Close() }() // best-effort test cleanup
	if err := revived.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "reconnect resubscribe", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	if err := revived.Client.Reconnect("gap-victim-1", map[string]string{channel: anchorPos}); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "reconnect", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	if _, ok := revived.WaitControlFrame(ctx, "reconnect_ack", recoveryFrameTimeout, recoveryWaitInterval); !ok {
		if errFrame, isErr := revived.ControlFrame("reconnect_error"); isErr {
			checks = append(checks, metrics.CheckResult{Name: "reconnect ack", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("reconnect_error: code=%s message=%s", errFrame.Code, errFrame.ErrMessage)})
		} else {
			checks = append(checks, metrics.CheckResult{Name: "reconnect ack", Status: metrics.CheckStatusFail,
				Error: "reconnect_ack never arrived"})
		}
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "reconnect ack", Status: metrics.CheckStatusPass})

	// Replayed records land before the ack (protocol ordering) — but allow the
	// tracked-delivery poll its own bound rather than assuming, terminator-first.
	if !revived.WaitForMessages(ctx, gapIDs, recoveryFrameTimeout, recoveryWaitInterval) {
		checks = append(checks, metrics.CheckResult{Name: "gap replay delivery", Status: metrics.CheckStatusFail,
			Error: "missed records were not replayed after reconnect{last_pos}"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "gap replay delivery", Status: metrics.CheckStatusPass})

	// The anchor record must NOT be re-delivered (reconnect's last_pos is an
	// exclusive cursor — unlike live replay's inclusive from_pos) …
	if _, _, _, dup := revived.ReceivedMeta(m1.MessageID); dup {
		checks = append(checks, metrics.CheckResult{Name: "no duplicate replay", Status: metrics.CheckStatusFail,
			Error: "the anchor record was re-delivered — last_pos must be exclusive"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "no duplicate replay", Status: metrics.CheckStatusPass})

	// … order is preserved …
	if revived.ReceivedOrderIndex(m2.MessageID) > revived.ReceivedOrderIndex(m3.MessageID) {
		checks = append(checks, metrics.CheckResult{Name: "replay order", Status: metrics.CheckStatusFail,
			Error: "replayed records arrived out of order"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "replay order", Status: metrics.CheckStatusPass})

	// … and mids match the control's live copies (ADR-0008 identity).
	for _, id := range gapIDs {
		liveMid, _, _, _ := control.ReceivedMeta(id)
		replayMid, _, _, _ := revived.ReceivedMeta(id)
		if liveMid == "" || liveMid != replayMid {
			checks = append(checks, metrics.CheckResult{Name: "replay mid equality", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("msg %s: live mid %q vs reconnect-replay mid %q", id, liveMid, replayMid)})
			return checks, nil
		}
	}
	checks = append(checks, metrics.CheckResult{Name: "replay mid equality", Status: metrics.CheckStatusPass})

	// ── Leg 2: live replay (in-connection) ────────────────────────────────────
	// A fresh subscriber with none of the records requests replay{from_pos}.
	// Unlike reconnect's exclusive last_pos, replay's from_pos is INCLUSIVE
	// (handler_replay.go: "replay from the anchor (the dropped message itself)")
	// — so the anchor AND the gap records arrive as replay_message frames,
	// terminated by a flat replay_complete{messages_replayed}. Server-side rate
	// limit is 1 per 10s per channel — this is the run's only Replay call.
	liveReader, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "gap-live-reader", ConnIndex: 3})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "create live-replay reader", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	defer func() { _ = liveReader.Client.Close() }() // best-effort test cleanup
	if err := liveReader.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "subscribe live-replay reader", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	if err := liveReader.Client.Replay(channel, anchorPos); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "live replay request", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	frame, ok := liveReader.WaitControlFrame(ctx, "replay_complete", recoveryFrameTimeout, recoveryWaitInterval)
	if !ok {
		// Replay rejections arrive as a generic type:"error" frame with a
		// top-level code (replay_protocol.go replayErrorEnvelope).
		if errFrame, isErr := liveReader.ControlFrame("error"); isErr {
			checks = append(checks, metrics.CheckResult{Name: "live replay complete", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("replay rejected: code=%s channel=%s message=%s", errFrame.Code, errFrame.Channel, errFrame.ErrMessage)})
		} else {
			checks = append(checks, metrics.CheckResult{Name: "live replay complete", Status: metrics.CheckStatusFail,
				Error: "replay_complete never arrived"})
		}
		return checks, nil
	}
	// Inclusive cursor: the anchor replays too — 1 (anchor) + the gap records.
	replayIDs := append([]string{m1.MessageID}, gapIDs...)
	if frame.MessagesReplayed != len(replayIDs) {
		checks = append(checks, metrics.CheckResult{Name: "live replay complete", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("messages_replayed = %d, want %d (inclusive from_pos: anchor + gap)", frame.MessagesReplayed, len(replayIDs))})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "live replay complete", Status: metrics.CheckStatusPass})

	if !liveReader.HasAllMessages(replayIDs) {
		checks = append(checks, metrics.CheckResult{Name: "live replay delivery", Status: metrics.CheckStatusFail,
			Error: "replayed records absent after replay_complete"})
		return checks, nil
	}
	// The copies must have arrived AS replay_message frames with matching mids.
	for _, id := range replayIDs {
		liveMid, _, _, _ := control.ReceivedMeta(id)
		replayMid, _, frameType, _ := liveReader.ReceivedMeta(id)
		if frameType != "replay_message" {
			checks = append(checks, metrics.CheckResult{Name: "live replay delivery", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("msg %s arrived as %q, want replay_message", id, frameType)})
			return checks, nil
		}
		if liveMid == "" || liveMid != replayMid {
			checks = append(checks, metrics.CheckResult{Name: "live replay delivery", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("msg %s: live mid %q vs replay_message mid %q", id, liveMid, replayMid)})
			return checks, nil
		}
	}
	checks = append(checks, metrics.CheckResult{Name: "live replay delivery", Status: metrics.CheckStatusPass})

	return checks, nil
}
