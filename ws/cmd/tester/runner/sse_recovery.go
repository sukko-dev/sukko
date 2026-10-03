package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/sse"
)

// SuiteSSERecovery proves the SSE reconnect-recovery leg of the ADR-0009 free data
// path end to end on the kafka backend (ADR-0030): an SSE subscriber that dies
// mid-stream reconnects with the opaque Last-Event-ID cursor and receives exactly
// the records it missed — the same lossless, exclusive-cursor replay the WebSocket
// reconnect leg gives, delivered inside the Subscribe stream. Unlike gap-recovery,
// SSE has no in-band live replay ("replay{from_pos}") — recovery is only the
// Last-Event-ID path — so this suite is the reconnect leg alone. It asserts
// delivery, exclusive-cursor de-duplication, order, and mid identity against a
// control subscriber (completing the ADR-0008 triple for the SSE transport).
const SuiteSSERecovery = "sse-recovery"

// sseParsedEnvelope holds the fields the recovery suite reads from a delivered SSE
// envelope: the top-level channel/pos/mid (broadcast_envelope.go) and the inner
// .data.msg_id the tester tracks messages by.
type sseParsedEnvelope struct {
	Channel string
	Pos     string
	Mid     string
	MsgID   string
}

// parseSSEEnvelope decodes one SSE `data:` line into the fields the recovery suite
// asserts on. The SSE transport delivers the FULL broadcast envelope, so mid/pos/
// channel are top-level and msg_id lives one level deeper at .data.msg_id (the same
// dig sseEventMsgID performs). Returns ok=false when the payload is not a delivery
// envelope carrying a msg_id.
func parseSSEEnvelope(data string) (sseParsedEnvelope, bool) {
	var env struct {
		Channel string `json:"channel"`
		Pos     string `json:"pos"`
		Mid     string `json:"mid"`
		Data    struct {
			MsgID string `json:"msg_id"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(data), &env) != nil || env.Data.MsgID == "" {
		return sseParsedEnvelope{}, false
	}
	return sseParsedEnvelope{
		Channel: env.Channel,
		Pos:     env.Pos,
		Mid:     env.Mid,
		MsgID:   env.Data.MsgID,
	}, true
}

// collectSSEByMsgID reads SSE events until every msg_id in want has arrived (or ctx's
// deadline elapses). It returns every delivery envelope seen keyed by msg_id, plus the
// arrival order of those msg_ids — so callers can assert both which records replayed
// AND that unwanted records (e.g. the exclusive-cursor anchor) did NOT, relying on
// replay's pos-ascending order: any wrongly-included earlier record arrives before the
// wanted ones, so it is already in the map by the time all wanted ids are seen.
func collectSSEByMsgID(ctx context.Context, c *sse.Client, want []string) (seen map[string]sseParsedEnvelope, order []string, err error) {
	remaining := make(map[string]struct{}, len(want))
	for _, id := range want {
		remaining[id] = struct{}{}
	}
	seen = make(map[string]sseParsedEnvelope, len(want))
	order = make([]string, 0, len(want))

	for len(remaining) > 0 {
		event, readErr := c.ReadEvent(ctx)
		if readErr != nil {
			return seen, order, fmt.Errorf("sse read: %w", readErr)
		}
		env, ok := parseSSEEnvelope(event.Data)
		if !ok {
			continue
		}
		if _, dup := seen[env.MsgID]; !dup {
			seen[env.MsgID] = env
			order = append(order, env.MsgID)
		}
		delete(remaining, env.MsgID)
	}
	return seen, order, nil
}

// readUntilRecoveryComplete reads control frames from the reconnected stream until the
// recovery_complete sentinel (ADR-0031), returning the channels reported in any no_replay frames
// seen before it. The recovery control frames ride event:message with a data.type discriminator and
// no msg_id (so collectSSEByMsgID skips them); delivery messages and gaps are ignored here. Returns
// ok=false if the context deadline elapses before the sentinel.
func readUntilRecoveryComplete(ctx context.Context, c *sse.Client) (noReplay []string, ok bool) {
	for {
		event, err := c.ReadEvent(ctx)
		if err != nil {
			return noReplay, false
		}
		var env struct {
			Type     string   `json:"type"`
			Channels []string `json:"channels"`
		}
		if json.Unmarshal([]byte(event.Data), &env) != nil {
			continue
		}
		switch env.Type {
		case "no_replay":
			noReplay = append(noReplay, env.Channels...)
		case "recovery_complete":
			return noReplay, true
		}
	}
}

func validateSSERecovery(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	if pre := recoverySetupError(run, SuiteSSERecovery); pre != nil {
		return pre, nil
	}
	if fail := setupRecoveryChannel(ctx, run, logger); fail != nil {
		return []metrics.CheckResult{*fail}, nil
	}
	tenantID := run.authResult.TenantID
	channel := tenantID + "." + recoveryChannelSuffix
	gwHTTP := httpURL(run.Config.GatewayURL)

	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
		Timeout:    kafkaIngestDeliveryTimeout,
	})

	// Control subscriber C (WebSocket): connected throughout, receives every record
	// live — the mid reference the SSE replay is compared against. Set up first so it is
	// subscribed before M1 is ingested (it must receive M1's live copy). The victim's
	// first-message cursor emission is safe for a different reason: M1 is the first — and
	// only — record published to this channel before the kill, and neither
	// SubscribeConfirmed nor the ingest path emits any probe/warmup traffic, so nothing
	// precedes M1 to steal the emission (ADR-0030: the gateway emits id: on the first
	// pos-bearing message per connection, then only every SSE_CURSOR_EVERY_N).
	control, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{Subject: "sse-gap-control", ConnIndex: 0})
	if err != nil {
		return []metrics.CheckResult{{Name: "create control subscriber", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() { _ = control.Client.Close() }() // best-effort test cleanup
	if err := control.SubscribeConfirmed(ctx, []string{channel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe control", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	token := run.authResult.TokenFunc(1)
	victim, _, err := sse.Connect(ctx, sse.ConnectConfig{
		GatewayURL: gwHTTP,
		Channels:   []string{channel},
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "sse victim connect", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	victimOpen := true
	defer func() {
		if victimOpen {
			_ = victim.Close() // best-effort test cleanup
		}
	}()
	// The 200 returns before server-side subscription registration completes; a missed
	// M1 is unrecoverable (live-only, and it is the recovery anchor), so let the
	// subscription propagate before ingesting — mirrors validate_sse.go.
	time.Sleep(500 * time.Millisecond)

	pub, err := recoveryPublisher(run, tenantID)
	if err != nil {
		return []metrics.CheckResult{{Name: "kafka-connect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("could not reach Kafka at %s: %v", run.kafkaBrokers, err)}}, nil
	}
	defer func() { _ = pub.Close() }() // best-effort test cleanup

	// M1: control receives it live; the SSE victim also receives it, and its event
	// carries the opaque id: cursor — the recovery anchor.
	m1 := ingestTracked(ctx, engine, pub, channel, []*TestUser{control}, []*TestUser{control})
	if !m1.Delivered {
		return []metrics.CheckResult{deliveryCheck("ingest m1", m1)}, nil
	}
	checks := make([]metrics.CheckResult, 0, 6)

	readCtx, cancel := context.WithTimeout(ctx, recoveryFrameTimeout)
	m1Event, readErr := readSSEUntilMsgID(readCtx, victim, m1.MessageID)
	cancel()
	if readErr != nil {
		checks = append(checks, metrics.CheckResult{Name: "sse victim receives m1", Status: metrics.CheckStatusFail, Error: readErr.Error()})
		return checks, nil
	}
	// The anchor is the id: the gateway emitted on M1's event. A blank id here means
	// no cursor was minted — the server is not on MESSAGE_BACKEND=kafka, or a rare
	// orphan record (ingestRepublish after a publish error) stole the first-message
	// emission. Fail loudly rather than reconnect with an empty cursor.
	anchorCursor := m1Event.ID
	if anchorCursor == "" {
		checks = append(checks, metrics.CheckResult{Name: "sse cursor present", Status: metrics.CheckStatusFail,
			Error: "M1 SSE event carried no id: cursor — is the server on MESSAGE_BACKEND=kafka?"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "sse cursor present", Status: metrics.CheckStatusPass})

	// Kill the victim mid-stream, then ingest the records it will miss.
	_ = victim.Close()
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

	// Reconnect with the opaque Last-Event-ID cursor. The server registers the live
	// subscription first, then replays anchor..now for each tenant-validated channel
	// (ADR-0026 ordering, ADR-0030) — M2/M3 are already in Kafka, so replay is server-
	// driven with no propagation sleep needed.
	revived, _, err := sse.Connect(ctx, sse.ConnectConfig{
		GatewayURL:  gwHTTP,
		Channels:    []string{channel},
		Token:       token,
		LastEventID: anchorCursor,
		Logger:      logger,
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "sse reconnect", Status: metrics.CheckStatusFail, Error: err.Error()})
		return checks, nil
	}
	defer func() { _ = revived.Close() }() // best-effort test cleanup

	collectCtx, collectCancel := context.WithTimeout(ctx, recoveryFrameTimeout)
	seen, order, collectErr := collectSSEByMsgID(collectCtx, revived, gapIDs)
	collectCancel()
	if collectErr != nil {
		checks = append(checks, metrics.CheckResult{Name: "sse gap replay delivery", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("missed records were not replayed after Last-Event-ID reconnect: %v", collectErr)})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "sse gap replay delivery", Status: metrics.CheckStatusPass})

	// ADR-0031: after the replayed records the server emits the recovery_complete sentinel. The
	// victim's single channel was fully recovered (M2/M3 replayed), so no_replay must be empty and
	// recovery_complete must arrive. This proves precise-recovery emission end-to-end.
	rcCtx, rcCancel := context.WithTimeout(ctx, recoveryFrameTimeout)
	noReplay, sentinel := readUntilRecoveryComplete(rcCtx, revived)
	rcCancel()
	switch {
	case !sentinel:
		checks = append(checks, metrics.CheckResult{Name: "sse recovery_complete sentinel", Status: metrics.CheckStatusFail,
			Error: "no recovery_complete frame after replay (ADR-0031)"})
		return checks, nil
	case len(noReplay) != 0:
		checks = append(checks, metrics.CheckResult{Name: "sse recovery_complete sentinel", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("a fully-recovered channel was reported as no_replay: %v", noReplay)})
		return checks, nil
	default:
		checks = append(checks, metrics.CheckResult{Name: "sse recovery_complete sentinel", Status: metrics.CheckStatusPass})
	}

	// The anchor record must NOT be re-delivered — Last-Event-ID is an exclusive
	// cursor. Replay is pos-ascending, so a wrongly-included M1 would already be in
	// `seen` by the time M2 and M3 arrived.
	if _, dup := seen[m1.MessageID]; dup {
		checks = append(checks, metrics.CheckResult{Name: "sse no duplicate replay", Status: metrics.CheckStatusFail,
			Error: "the anchor record was re-delivered — Last-Event-ID must be an exclusive cursor"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "sse no duplicate replay", Status: metrics.CheckStatusPass})

	// Order is preserved (M2 before M3).
	if slices.Index(order, m2.MessageID) > slices.Index(order, m3.MessageID) {
		checks = append(checks, metrics.CheckResult{Name: "sse replay order", Status: metrics.CheckStatusFail,
			Error: "replayed records arrived out of order"})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{Name: "sse replay order", Status: metrics.CheckStatusPass})

	// Mids match the control's live copies (ADR-0008 identity: live == replay).
	for _, id := range gapIDs {
		liveMid, _, _, _ := control.ReceivedMeta(id)
		replayMid := seen[id].Mid
		if liveMid == "" || liveMid != replayMid {
			checks = append(checks, metrics.CheckResult{Name: "sse replay mid equality", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("msg %s: live mid %q vs sse-replay mid %q", id, liveMid, replayMid)})
			return checks, nil
		}
	}
	checks = append(checks, metrics.CheckResult{Name: "sse replay mid equality", Status: metrics.CheckStatusPass})

	return checks, nil
}
