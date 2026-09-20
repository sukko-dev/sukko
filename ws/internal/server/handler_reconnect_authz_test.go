package server

import (
	"encoding/json"
	"testing"
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
