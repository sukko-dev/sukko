package server

import (
	"testing"

	"github.com/sukko-dev/sukko/internal/server/backend"
)

// TestAuthorizeLastPos_TenantScopeAndCap pins the security-critical behavior of
// the shared replay authorization (§IX/ADR-0020): only own-tenant channels are
// authorized, cross-tenant channels never reach the replay positions, and new
// channels are capped at MaxChannelsPerClient.
func TestAuthorizeLastPos_TenantScopeAndCap(t *testing.T) {
	mb := &mockBackend{channelTopics: map[string]string{
		"acme.md-1": "t1", "acme.md-2": "t2", "acme.md-3": "t3", "evil.md-0": "evil-topic",
	}}
	s := newReplayTestServer(t, mb)
	s.config.MaxChannelsPerClient = 2
	c := newReplayTestClient(1)
	c.tenantID = "acme"

	positions, authorized := s.authorizeLastPos(c, map[string]string{
		"acme.md-1": "2-5", // own tenant → authorized
		"acme.md-2": "2-5", // own tenant → authorized
		"acme.md-3": "2-5", // own tenant, but over the cap of 2 → skipped
		"evil.md-0": "2-5", // other tenant → denied
	})

	for _, ch := range authorized {
		if ch == "evil.md-0" {
			t.Fatalf("cross-tenant channel authorized: %v", authorized)
		}
	}
	if _, ok := positions["evil-topic"]; ok {
		t.Errorf("cross-tenant topic reached replay positions: %+v", positions)
	}
	if len(authorized) != 2 {
		t.Errorf("authorized = %d (%v), want 2 (capped at MaxChannelsPerClient)", len(authorized), authorized)
	}
	if len(positions) != 2 {
		t.Errorf("positions = %d topics, want 2 (capped)", len(positions))
	}
}

// TestReplayAuthorizedToClient_ForwardsWithFilter verifies the shared replay
// forwards each replayed message to the client and passes the authorized set as
// the backend replay filter (ADR-0020), reporting the count with no truncation.
func TestReplayAuthorizedToClient_ForwardsWithFilter(t *testing.T) {
	mb := &mockBackend{
		channelTopics: map[string]string{"acme.md-1": "t1"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "acme.md-1", Data: []byte(`{"a":1}`), Pos: "2-6", Mid: "m1"},
			{Subject: "acme.md-1", Data: []byte(`{"a":2}`), Pos: "2-7", Mid: "m2"},
		},
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.tenantID = "acme"

	positions, authorized := s.authorizeLastPos(c, map[string]string{"acme.md-1": "2-5"})
	count, truncated, err := s.replayAuthorizedToClient(c, positions, authorized)
	if err != nil {
		t.Fatalf("replay err: %v", err)
	}
	if truncated {
		t.Error("unexpected truncation with a large send buffer")
	}
	if count != 2 {
		t.Errorf("replayed count = %d, want 2", count)
	}
	if len(mb.lastReplayReq.Subscriptions) != 1 || mb.lastReplayReq.Subscriptions[0] != "acme.md-1" {
		t.Errorf("replay filter = %v, want [acme.md-1] (the tenant-validated set)", mb.lastReplayReq.Subscriptions)
	}
}

// TestReplayAuthorizedToClient_TruncatesOnFullBuffer verifies the shared replay
// reports truncation (rather than blocking) when the client's send buffer fills.
func TestReplayAuthorizedToClient_TruncatesOnFullBuffer(t *testing.T) {
	mb := &mockBackend{
		channelTopics: map[string]string{"acme.md-1": "t1"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "acme.md-1", Data: []byte(`{"a":1}`), Pos: "2-6", Mid: "m1"},
			{Subject: "acme.md-1", Data: []byte(`{"a":2}`), Pos: "2-7", Mid: "m2"},
		},
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.tenantID = "acme"
	c.send = make(chan OutgoingMsg, 1) // capacity 1: fills after the first message

	positions, authorized := s.authorizeLastPos(c, map[string]string{"acme.md-1": "2-5"})
	count, truncated, err := s.replayAuthorizedToClient(c, positions, authorized)
	if err != nil {
		t.Fatalf("replay err: %v", err)
	}
	if !truncated {
		t.Error("expected truncation when the send buffer fills")
	}
	if count != 1 {
		t.Errorf("replayed count = %d, want 1 (buffer capacity 1)", count)
	}
}
