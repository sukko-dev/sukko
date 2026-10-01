package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func TestUnreplayableCursorChannels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		lastPos    map[string]string
		authorized []string
		want       []string
	}{
		{"all authorized → none", map[string]string{"t.a": "1-5", "t.b": "1-9"}, []string{"t.a", "t.b"}, nil},
		{"partial → the unauthorized ones", map[string]string{"t.a": "1-5", "t.b": "1-9"}, []string{"t.a"}, []string{"t.b"}},
		{"none authorized → all", map[string]string{"t.a": "1-5", "t.b": "1-9"}, nil, []string{"t.a", "t.b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := unreplayableCursorChannels(tt.lastPos, tt.authorized)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestSendReplayControl_DeliversNoReplayEnvelope(t *testing.T) {
	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	c.send = make(chan OutgoingMsg, 2)

	s.sendReplayControl(context.Background(), c, replayControlEnvelope{
		Type: MsgTypeNoReplay, Channels: []string{"acme.x", "acme.y"},
	})

	select {
	case msg := <-c.send:
		var env struct {
			Type     string   `json:"type"`
			Channels []string `json:"channels"`
		}
		if err := json.Unmarshal(msg.Bytes(), &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if env.Type != MsgTypeNoReplay || len(env.Channels) != 2 {
			t.Fatalf("envelope = %+v, want no_replay with 2 channels", env)
		}
	default:
		t.Fatal("no control frame delivered on c.send")
	}
}

func TestSendReplayControl_AbandonsOnContextCancel(t *testing.T) {
	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	c.send = make(chan OutgoingMsg) // unbuffered + no reader → send would block

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled → the blocking send must abandon, not hang
	done := make(chan struct{})
	go func() {
		s.sendReplayControl(ctx, c, replayControlEnvelope{Type: MsgTypeReplayTruncated, Replayed: 3})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendReplayControl hung on a full buffer with a canceled ctx")
	}
}

func TestReplayOutcome(t *testing.T) {
	t.Parallel()
	cur := map[string]string{"t.a": "1-5", "t.b": "1-9"}
	tests := []struct {
		name          string
		authorized    []string
		truncated     bool
		replayErr     error
		wantNoReplay  []string
		wantTruncated bool
	}{
		{"all authorized, clean", []string{"t.a", "t.b"}, false, nil, nil, false},
		{"partial auth → the unauthorized one", []string{"t.a"}, false, nil, []string{"t.b"}, false},
		{"truncated only", []string{"t.a", "t.b"}, true, nil, nil, true},
		{"partial auth AND truncated (co-occur)", []string{"t.a"}, true, nil, []string{"t.b"}, true},
		{"replay error → all channels, never truncated", []string{"t.a"}, true, errors.New("boom"), []string{"t.a", "t.b"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			noReplay, isTrunc := replayOutcome(cur, tt.authorized, tt.truncated, tt.replayErr)
			if isTrunc != tt.wantTruncated {
				t.Errorf("isTruncated = %v, want %v", isTrunc, tt.wantTruncated)
			}
			if len(noReplay) != len(tt.wantNoReplay) {
				t.Fatalf("noReplay = %v, want %v", noReplay, tt.wantNoReplay)
			}
			for i := range tt.wantNoReplay {
				if noReplay[i] != tt.wantNoReplay[i] {
					t.Fatalf("noReplay = %v, want %v", noReplay, tt.wantNoReplay)
				}
			}
		})
	}
}
