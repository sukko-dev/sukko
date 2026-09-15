package runner

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// testUserWith builds an in-package TestUser whose received-tracker is pre-seeded with the given
// message IDs. buildResult only inspects presence (via HasReceived) and Subject, so a zero-value
// receivedMsg is sufficient.
func testUserWith(subject string, receivedIDs ...string) *TestUser {
	u := &TestUser{Subject: subject, received: make(map[string]receivedMsg)}
	for _, id := range receivedIDs {
		u.received[id] = receivedMsg{}
	}
	return u
}

// TestBuildResult pins the pure leak-detection logic buildResult computes from the users' received
// state — the committed, race-detector-runnable guard for the tenant-isolation NEGATIVE assertion
// The load-bearing row is "leak": when a must-NOT-receive user receives the
// measured message, MisroutedTo MUST be populated (→ fail) while Delivered STAYS true (the expected
// receiver still got it — Delivered only flips false on a genuine miss).
func TestBuildResult(t *testing.T) {
	t.Parallel()
	const msgID = "msg-1"
	e := NewPubSubEngine(PubSubEngineConfig{Logger: zerolog.Nop()})

	tests := []struct {
		name        string
		expected    []string // subjects that SHOULD receive
		allUsers    []*TestUser
		wantDeliver bool
		wantMisrout []string
		wantMissing []string
	}{
		{
			name:        "clean delivery — expected got it, other did not",
			expected:    []string{"A"},
			allUsers:    []*TestUser{testUserWith("A", msgID), testUserWith("B")},
			wantDeliver: true,
			wantMisrout: nil,
			wantMissing: nil,
		},
		{
			name:        "leak — expected got it AND other also got it (Delivered stays true)",
			expected:    []string{"A"},
			allUsers:    []*TestUser{testUserWith("A", msgID), testUserWith("B", msgID)},
			wantDeliver: true, // expected receiver still got it; the FAIL is driven by MisroutedTo, not Delivered
			wantMisrout: []string{"B"},
			wantMissing: nil,
		},
		{
			name:        "miss — expected did NOT receive (Delivered false)",
			expected:    []string{"A"},
			allUsers:    []*TestUser{testUserWith("A"), testUserWith("B")},
			wantDeliver: false,
			wantMisrout: nil,
			wantMissing: []string{"A"},
		},
		{
			name:        "miss + leak — expected missed AND other leaked",
			expected:    []string{"A"},
			allUsers:    []*TestUser{testUserWith("A"), testUserWith("B", msgID)},
			wantDeliver: false,
			wantMisrout: []string{"B"},
			wantMissing: []string{"A"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expectedSet := make(map[string]bool, len(tt.expected))
			for _, s := range tt.expected {
				expectedSet[s] = true
			}
			got := e.buildResult("tenant.general.test", msgID, time.Now(), expectedSet, tt.allUsers)

			if got.Delivered != tt.wantDeliver {
				t.Errorf("Delivered = %v, want %v", got.Delivered, tt.wantDeliver)
			}
			if !slices.Equal(got.MisroutedTo, tt.wantMisrout) {
				t.Errorf("MisroutedTo = %v, want %v", got.MisroutedTo, tt.wantMisrout)
			}
			if !slices.Equal(got.Missing, tt.wantMissing) {
				t.Errorf("Missing = %v, want %v", got.Missing, tt.wantMissing)
			}
		})
	}
}

// newCaptureTestUser builds a TestUser with initialized delivery + error state for driving
// onMessage directly (no live connection).
func newCaptureTestUser() *TestUser {
	return &TestUser{received: make(map[string]receivedMsg)}
}

// TestTestUser_ErrorCapture covers subscribe_error/publish_error frames are captured with
// their top-level code and matched jointly; delivery envelopes are unaffected; error frames never
// leak into delivery tracking even when carrying a msg_id-shaped payload.
func TestTestUser_ErrorCapture(t *testing.T) {
	t.Parallel()

	t.Run("captures subscribe_error and publish_error with code", func(t *testing.T) {
		t.Parallel()
		u := newCaptureTestUser()
		u.onMessage(testerws.Message{Type: respTypeSubscribeError, Code: "invalid_request"})
		u.onMessage(testerws.Message{Type: respTypePublishError, Code: "forbidden"})
		if !u.HasErrorMatching(respTypeSubscribeError, "invalid_request") {
			t.Error("subscribe_error/invalid_request not captured")
		}
		if !u.HasErrorMatching(respTypePublishError, "forbidden") {
			t.Error("publish_error/forbidden not captured")
		}
		// Error frames must NOT be counted as delivered.
		if got := u.ReceivedCount(); got != 0 {
			t.Errorf("ReceivedCount = %d, want 0 (error frames must not reach delivery tracking)", got)
		}
	})

	t.Run("joint (type,code) discrimination", func(t *testing.T) {
		t.Parallel()
		u := newCaptureTestUser()
		u.onMessage(testerws.Message{Type: respTypeSubscribeError, Code: "invalid_request"})
		// right type, wrong code
		if u.HasErrorMatching(respTypeSubscribeError, "forbidden") {
			t.Error("matched on wrong code")
		}
		// wrong type, right code
		if u.HasErrorMatching(respTypePublishError, "invalid_request") {
			t.Error("matched on wrong type")
		}
	})

	t.Run("publish_error with msg_id-shaped payload does not reach delivery tracking", func(t *testing.T) {
		t.Parallel()
		u := newCaptureTestUser()
		// A publish_error whose Data happens to carry a msg_id-shaped field MUST still be an error,
		// never a delivered message (disjoint-types + first-branch-return guarantee).
		u.onMessage(testerws.Message{Type: respTypePublishError, Code: "forbidden", Data: []byte(`{"msg_id":"m1"}`)})
		if u.ReceivedCount() != 0 {
			t.Error("publish_error with msg_id leaked into delivery tracking")
		}
		if u.HasReceived("m1") {
			t.Error("publish_error msg_id tracked as delivered")
		}
		if !u.HasErrorMatching(respTypePublishError, "forbidden") {
			t.Error("publish_error not captured")
		}
	})

	t.Run("delivery envelopes still tracked; error frames disjoint", func(t *testing.T) {
		t.Parallel()
		u := newCaptureTestUser()
		u.onMessage(testerws.Message{Type: "message", Data: []byte(`{"msg_id":"d1"}`)})
		u.onMessage(testerws.Message{Type: "publish", Data: []byte(`{"msg_id":"d2"}`)})
		if !u.HasReceived("d1") || !u.HasReceived("d2") {
			t.Error("delivery envelopes not tracked after adding error-capture branch")
		}
		if u.ReceivedCount() != 2 {
			t.Errorf("ReceivedCount = %d, want 2", u.ReceivedCount())
		}
	})

	t.Run("ClearErrors resets errors but not delivery, and vice-versa", func(t *testing.T) {
		t.Parallel()
		u := newCaptureTestUser()
		u.onMessage(testerws.Message{Type: "message", Data: []byte(`{"msg_id":"d1"}`)})
		u.onMessage(testerws.Message{Type: respTypeSubscribeError, Code: "invalid_request"})

		u.ClearErrors()
		if u.HasErrorMatching(respTypeSubscribeError, "invalid_request") {
			t.Error("ClearErrors did not reset error frames")
		}
		if !u.HasReceived("d1") {
			t.Error("ClearErrors wrongly cleared delivery state")
		}

		u.onMessage(testerws.Message{Type: respTypePublishError, Code: "forbidden"})
		u.ClearReceived()
		if u.HasReceived("d1") {
			t.Error("ClearReceived did not reset delivery state")
		}
		if !u.HasErrorMatching(respTypePublishError, "forbidden") {
			t.Error("ClearReceived wrongly cleared error frames")
		}
	})
}

// A subscription_ack must mark its channels confirmed on the user — the signal
// subscribeConfirmed polls. A gateway that silently filtered a channel (rules
// not yet propagated, #242) yields an ack WITHOUT that channel, which must not
// confirm it.
func TestOnMessage_SubscriptionAckTracked(t *testing.T) {
	t.Parallel()

	u := &TestUser{Subject: "s", received: make(map[string]receivedMsg)}
	u.onMessage(testerws.Message{Type: "subscription_ack", Subscribed: []string{"t.a.x", "t.b.y"}})

	if !u.SubscribedAll([]string{"t.a.x"}) || !u.SubscribedAll([]string{"t.a.x", "t.b.y"}) {
		t.Error("acked channels must be confirmed")
	}
	if u.SubscribedAll([]string{"t.c.z"}) {
		t.Error("unacked channel must not be confirmed (silently-filtered subscribe, #242)")
	}
}

// waitSubscribeConfirmed must re-send the subscribe until the confirmation
// predicate holds (rules propagating), and fail loudly at the bound.
func TestWaitSubscribeConfirmed(t *testing.T) {
	t.Parallel()

	t.Run("confirms after retries", func(t *testing.T) {
		t.Parallel()
		var sends int
		confirmedAt := 3
		err := waitSubscribeConfirmed(context.Background(),
			func() error { sends++; return nil },
			func() bool { return sends >= confirmedAt },
			2*time.Second, 5*time.Millisecond)
		if err != nil {
			t.Fatalf("want confirmation, got %v", err)
		}
		if sends < confirmedAt {
			t.Errorf("sends = %d, want ≥ %d (must re-send while unconfirmed)", sends, confirmedAt)
		}
	})

	t.Run("times out loudly when never confirmed", func(t *testing.T) {
		t.Parallel()
		err := waitSubscribeConfirmed(context.Background(),
			func() error { return nil },
			func() bool { return false },
			50*time.Millisecond, 5*time.Millisecond)
		if err == nil {
			t.Fatal("want timeout error for never-confirmed subscribe")
		}
	})

	t.Run("subscribe send error surfaces", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("ws closed")
		err := waitSubscribeConfirmed(context.Background(),
			func() error { return boom },
			func() bool { return false },
			50*time.Millisecond, 5*time.Millisecond)
		if err == nil || !errors.Is(err, boom) {
			t.Fatalf("want send error surfaced, got %v", err)
		}
	})
}

// The recovery suites depend on identity metadata and control-frame capture:
// mid/pos/frameType recorded per FIRST copy, replay_message accepted as a
// delivery envelope, terminators captured verbatim.
func TestOnMessage_RecoveryTracking(t *testing.T) {
	t.Parallel()
	u := &TestUser{received: make(map[string]receivedMsg)}

	// Live copy first.
	u.onMessage(testerws.Message{Type: "message", Channel: "t.c", Mid: "mid-1", Pos: "3-100",
		Data: []byte(`{"msg_id":"id-1"}`)})
	// A replayed duplicate must NOT overwrite the first copy's metadata.
	u.onMessage(testerws.Message{Type: "replay_message", Channel: "t.c", Mid: "mid-1", Pos: "3-100",
		Data: []byte(`{"msg_id":"id-1"}`)})
	// A replay-only record IS tracked (the live-replay leg's delivery signal).
	u.onMessage(testerws.Message{Type: "replay_message", Channel: "t.c", Mid: "mid-2", Pos: "3-101",
		Data: []byte(`{"msg_id":"id-2"}`)})
	// Terminators are captured, not treated as deliveries.
	u.onMessage(testerws.Message{Type: "replay_complete", Channel: "t.c", MessagesReplayed: 1})
	u.onMessage(testerws.Message{Type: "history_complete", Channel: "t.c"})

	mid, pos, ft, ok := u.ReceivedMeta("id-1")
	if !ok || mid != "mid-1" || pos != "3-100" || ft != "message" {
		t.Errorf("id-1 meta = (%q,%q,%q,%v), want (mid-1,3-100,message,true) — first receipt must win", mid, pos, ft, ok)
	}
	if _, _, ft2, ok2 := u.ReceivedMeta("id-2"); !ok2 || ft2 != "replay_message" {
		t.Errorf("id-2 must be tracked via replay_message, got ft=%q ok=%v", ft2, ok2)
	}
	if u.ReceivedOrderIndex("id-1") != 0 || u.ReceivedOrderIndex("id-2") != 1 {
		t.Errorf("order = (%d,%d), want (0,1)", u.ReceivedOrderIndex("id-1"), u.ReceivedOrderIndex("id-2"))
	}
	if _, ok := u.ControlFrame("replay_complete"); !ok {
		t.Error("replay_complete not captured")
	}
	if _, ok := u.ControlFrame("history_complete"); !ok {
		t.Error("history_complete not captured")
	}
	if !u.HasAllMessages([]string{"id-1", "id-2"}) || u.HasAllMessages([]string{"id-3"}) {
		t.Error("HasAllMessages wrong")
	}
}
