package server

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sukko-dev/sukko/internal/server/metrics"
)

// reconnectJSON builds the data payload for handleReconnect (client_id + last_pos).
func reconnectJSON(t *testing.T, clientID string, lastPos map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"client_id": clientID, "last_pos": lastPos})
	if err != nil {
		t.Fatalf("marshal reconnect: %v", err)
	}
	return b
}

// TestHandleReconnect_ScopesReplayToTenantValidatedLastPos is the ADR-0020 red
// test. On the documented reconnect-before-subscribe order the live subscription
// set is empty, so the replay must be authorized against the connection's tenant
// and scoped to the channels the client named in last_pos:
//   - a cross-tenant channel in last_pos MUST NOT be replayed (its topic must not
//     reach the backend request);
//   - the own-tenant channel MUST be replayed (recovery preserved);
//   - the backend's subscription filter MUST be the tenant-validated last_pos
//     channels, not the (empty) live subscription set.
//
//nolint:paralleltest // mutates a shared test Client; not parallel-safe
func TestHandleReconnect_ScopesReplayToTenantValidatedLastPos(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{
		"acme.md-1": "acme-topic",
		"evil.md-0": "evil-topic",
	}}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.tenantID = "acme" // authenticated tenant

	// reconnect BEFORE subscribe: live subscription set is empty (the real protocol).
	data := reconnectJSON(t, "cid", map[string]string{
		"acme.md-1": "2-5", // own tenant — must replay
		"evil.md-0": "2-5", // other tenant — must be denied
	})
	s.handleReconnect(c, data)

	req := mb.lastReplayReq

	// Cross-tenant channel must never reach the replay request.
	if _, ok := req.Positions["evil-topic"]; ok {
		t.Errorf("cross-tenant channel replayed: positions include evil-topic: %+v", req.Positions)
	}
	// Own-tenant channel must be replayed (recovery preserved).
	if _, ok := req.Positions["acme-topic"]; !ok {
		t.Errorf("own-tenant channel not replayed: positions missing acme-topic: %+v", req.Positions)
	}
	// The filter must be the tenant-validated last_pos channels, not the empty live set.
	if len(req.Subscriptions) != 1 || req.Subscriptions[0] != "acme.md-1" {
		t.Errorf("replay filter = %v, want [acme.md-1] (tenant-validated last_pos)", req.Subscriptions)
	}
}

// TestHandleReconnect_EmptyTenantDeniesAll pins the fail-closed default: a
// connection with no authenticated tenant must not replay anything.
//
//nolint:paralleltest // mutates a shared test Client; not parallel-safe
func TestHandleReconnect_EmptyTenantDeniesAll(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{"acme.md-1": "acme-topic"}}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(2)
	c.tenantID = "" // no authenticated tenant

	s.handleReconnect(c, reconnectJSON(t, "cid", map[string]string{"acme.md-1": "2-5"}))

	if len(mb.lastReplayReq.Positions) != 0 {
		t.Errorf("empty tenant must deny all replay, got positions: %+v", mb.lastReplayReq.Positions)
	}
	if len(mb.lastReplayReq.Subscriptions) != 0 {
		t.Errorf("empty tenant must yield empty filter, got: %v", mb.lastReplayReq.Subscriptions)
	}

	// All channels denied → a clean reconnect_ack with 0 replayed, not an error.
	var gotAck bool
	for len(c.send) > 0 {
		var m map[string]any
		if json.Unmarshal((<-c.send).Bytes(), &m) == nil && m["type"] == RespTypeReconnectAck {
			gotAck = true
			if r, _ := m["messages_replayed"].(float64); r != 0 {
				t.Errorf("messages_replayed = %v, want 0", m["messages_replayed"])
			}
		}
	}
	if !gotAck {
		t.Errorf("expected a clean reconnect_ack (0 replayed), got none")
	}
}

// TestHandleReconnect_RegistersLiveBeforeReplay is the ADR-0026 red test for the
// replay↔live seam. On reconnect the server MUST register the client for live delivery
// on its tenant-authorized last_pos channels BEFORE it replays — so a message that hits
// the bus during the replay is captured live rather than falling into a gap between the
// replay's snapshot and a later subscribe. Asserts (a) the client was already subscribed
// when Replay ran (ordering), and (b) it is registered in both the client set and the
// global subscriptionIndex (live fan-out targets it) after reconnect.
//
//nolint:paralleltest // mutates a shared test Client; not parallel-safe
func TestHandleReconnect_RegistersLiveBeforeReplay(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{"acme.md-1": "acme-topic"}}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.tenantID = "acme"

	// Capture BOTH the client set and the global index at replay time. Live fan-out targets ONLY
	// s.subscriptionIndex.Get (broadcast.go), never c.subscriptions — so the index add is the one
	// that closes the seam. Asserting the index here fails if the index registration is ever moved
	// below the replay (the exact reordering ADR-0026 must prevent), which a client-set-only check
	// would miss.
	var subscribedAtReplay, indexedAtReplay bool
	mb.onReplay = func() {
		subscribedAtReplay = c.subscriptions.Has("acme.md-1")
		indexedAtReplay = len(s.subscriptionIndex.Get("acme.md-1")) > 0
	}

	s.handleReconnect(c, reconnectJSON(t, "cid", map[string]string{"acme.md-1": "2-5"}))

	if !indexedAtReplay {
		t.Error("live fan-out index must be registered BEFORE replay (seam): acme.md-1 not in subscriptionIndex when Replay ran")
	}
	if !subscribedAtReplay {
		t.Error("client subscription set must be registered BEFORE replay: acme.md-1 not in c.subscriptions when Replay ran")
	}
	if !c.subscriptions.Has("acme.md-1") {
		t.Error("after reconnect the client must be subscribed to acme.md-1 for live delivery")
	}
	if got := s.subscriptionIndex.Get("acme.md-1"); len(got) == 0 {
		t.Error("after reconnect the client must be in subscriptionIndex[acme.md-1] so live fan-out targets it")
	}
	// The replay itself must still be scoped to the tenant-validated last_pos channel (ADR-0020 preserved).
	if _, ok := mb.lastReplayReq.Positions["acme-topic"]; !ok {
		t.Errorf("replay must still cover acme-topic: %+v", mb.lastReplayReq.Positions)
	}
	// A cross-tenant channel must be registered for NEITHER live nor replay.
	if s.subscriptionIndex.Get("evil.md-0") != nil {
		t.Error("cross-tenant channel must not be live-registered")
	}
}

// TestHandleReconnect_ResumeSubscribeNotRejectedByLimit is the ADR-0026 finding-1 regression:
// handleReconnect pre-registers the last_pos channels, so the client's resume `subscribe` (which
// batch-re-sends the whole desired set) must NOT be rejected by the per-client channel limit —
// re-subscribing already-held channels is idempotent and must not consume limit budget. Before the
// fix, a batching client with more than half the limit's worth of channels got subscribe_limit_exceeded.
//
//nolint:paralleltest // mutates a shared test Client; not parallel-safe
func TestHandleReconnect_ResumeSubscribeNotRejectedByLimit(t *testing.T) {
	// Limit = 4; resume 3 channels via reconnect, then batch-subscribe the same 3. 3 > (4-3)=1, so
	// the pre-fix "remaining" check would reject; the dedup check must accept (resulting set = 3 ≤ 4).
	topics := map[string]string{"acme.md-1": "t1", "acme.md-2": "t2", "acme.md-3": "t3"}
	mb := &mockBackend{channelTopics: topics}
	s := newReplayTestServer(t, mb)
	s.config.MaxChannelsPerClient = 4
	c := newReplayTestClient(7)
	c.tenantID = "acme"

	s.handleReconnect(c, reconnectJSON(t, "cid", map[string]string{"acme.md-1": "2-1", "acme.md-2": "2-1", "acme.md-3": "2-1"}))
	if c.subscriptions.Count() != 3 {
		t.Fatalf("reconnect should have registered 3 channels, got %d", c.subscriptions.Count())
	}

	// Resume subscribe: the full set again, in one batch.
	s.handleClientMessage(c, []byte(`{"type":"subscribe","data":{"channels":["acme.md-1","acme.md-2","acme.md-3"]}}`))

	// Must NOT have received a subscribe_limit_exceeded error.
	for len(c.send) > 0 {
		var m map[string]any
		if json.Unmarshal((<-c.send).Bytes(), &m) == nil {
			if m["type"] == RespTypeSubscribeError {
				t.Fatalf("resume subscribe of already-registered channels was rejected: %v", m)
			}
		}
	}
	if c.subscriptions.Count() != 3 {
		t.Errorf("after idempotent resume subscribe, channel count = %d, want 3", c.subscriptions.Count())
	}
}

// TestHandleReconnect_NewChannelsCappedAtLimit is the ADR-0026 finding-2 test: reconnect
// registers live delivery for last_pos channels, so the per-client channel cap
// (WS_MAX_CHANNELS_PER_CLIENT) must apply — a client naming more NEW channels than the limit
// must have exactly the limit registered+replayed, the excess skipped and counted, so it cannot
// bypass MaxChannelsPerClient via reconnect (fan-out amplification).
//
//nolint:paralleltest // mutates a shared test Client and a process-global metric; not parallel-safe
func TestHandleReconnect_NewChannelsCappedAtLimit(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{
		"acme.md-1": "t1", "acme.md-2": "t2", "acme.md-3": "t3", "acme.md-4": "t4",
	}}
	s := newReplayTestServer(t, mb)
	s.config.MaxChannelsPerClient = 2
	c := newReplayTestClient(11)
	c.tenantID = "acme"

	before := testutil.ToFloat64(metrics.ReconnectChannelLimitExceeded)
	s.handleReconnect(c, reconnectJSON(t, "cid", map[string]string{
		"acme.md-1": "2-1", "acme.md-2": "2-1", "acme.md-3": "2-1", "acme.md-4": "2-1",
	}))
	after := testutil.ToFloat64(metrics.ReconnectChannelLimitExceeded)

	// Exactly the limit registered for live delivery — the excess NEW channels skipped.
	if c.subscriptions.Count() != 2 {
		t.Errorf("registered channel count = %d, want 2 (capped at MaxChannelsPerClient)", c.subscriptions.Count())
	}
	// The replay filter/positions must match the capped set (2 topics), not all 4.
	if len(mb.lastReplayReq.Positions) != 2 {
		t.Errorf("replay positions = %d topics, want 2 (capped): %+v", len(mb.lastReplayReq.Positions), mb.lastReplayReq.Positions)
	}
	// The 2 over-budget NEW channels must be counted as skipped.
	if delta := after - before; delta != 2 {
		t.Errorf("ReconnectChannelLimitExceeded delta = %v, want 2 (channels 3 and 4 skipped)", delta)
	}
}

// TestHandleReconnect_HeldChannelsExemptFromCap is the ADR-0026 finding-2 regression guard: a
// channel already held by the client adds zero new live fan-out, so it must NOT count toward the
// cap. Otherwise a client at the limit that re-homes its own K channels (e.g. after a replay error
// left it registered and it retries reconnect on the same connection) would be double-counted,
// break on the first channel, replay nothing, and receive a false "completed" ack for an
// unrecoverable gap. All held channels must be replayed regardless of the cap.
//
//nolint:paralleltest // mutates a shared test Client and a process-global metric; not parallel-safe
func TestHandleReconnect_HeldChannelsExemptFromCap(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{
		"acme.md-1": "t1", "acme.md-2": "t2", "acme.md-3": "t3",
	}}
	s := newReplayTestServer(t, mb)
	s.config.MaxChannelsPerClient = 3
	c := newReplayTestClient(12)
	c.tenantID = "acme"
	// Client already holds all 3 channels (at the limit) — the retry-reconnect state.
	c.subscriptions.AddMultiple([]string{"acme.md-1", "acme.md-2", "acme.md-3"})

	before := testutil.ToFloat64(metrics.ReconnectChannelLimitExceeded)
	s.handleReconnect(c, reconnectJSON(t, "cid", map[string]string{
		"acme.md-1": "2-1", "acme.md-2": "2-1", "acme.md-3": "2-1",
	}))
	after := testutil.ToFloat64(metrics.ReconnectChannelLimitExceeded)

	// All 3 held channels must be replayed — not skipped, not a zero-positions false-"completed".
	if len(mb.lastReplayReq.Positions) != 3 {
		t.Errorf("held channels must all be replayed: positions = %d topics, want 3: %+v", len(mb.lastReplayReq.Positions), mb.lastReplayReq.Positions)
	}
	if len(mb.lastReplayReq.Subscriptions) != 3 {
		t.Errorf("replay filter = %d channels, want 3 (all held): %v", len(mb.lastReplayReq.Subscriptions), mb.lastReplayReq.Subscriptions)
	}
	if c.subscriptions.Count() != 3 {
		t.Errorf("channel count = %d, want 3 (unchanged; held channels re-added idempotently)", c.subscriptions.Count())
	}
	// No channel is a NEW channel over budget, so the limit-exceeded metric must not move.
	if delta := after - before; delta != 0 {
		t.Errorf("ReconnectChannelLimitExceeded delta = %v, want 0 (held channels are exempt)", delta)
	}
}
