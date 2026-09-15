package runner

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// TestClassifyRateLimitFrame covers the inbound-frame classifier against interleaved
// frame types, including publish_error of OTHER codes (must NOT count as enforcement). §VIII.
func TestClassifyRateLimitFrame(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  testerws.Message
		want rateLimitFrameClass
	}{
		{"rate_limited publish_error", testerws.Message{Type: "publish_error", Code: "rate_limited"}, frameRateLimited},
		{"publish_error other code invalid_channel", testerws.Message{Type: "publish_error", Code: "invalid_channel"}, frameOther},
		{"publish_error other code forbidden", testerws.Message{Type: "publish_error", Code: "forbidden"}, frameOther},
		{"publish_error no code", testerws.Message{Type: "publish_error"}, frameOther},
		{"delivered message envelope", testerws.Message{Type: "message"}, frameDelivery},
		{"delivered publish envelope", testerws.Message{Type: "publish"}, frameDelivery},
		{"subscribe_ack", testerws.Message{Type: "subscribe_ack"}, frameOther},
		{"pong", testerws.Message{Type: "pong"}, frameOther},
		{"unknown type", testerws.Message{Type: "something_else"}, frameOther},
		{"empty", testerws.Message{}, frameOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyRateLimitFrame(tc.msg); got != tc.want {
				t.Errorf("classifyRateLimitFrame(%+v) = %d, want %d", tc.msg, got, tc.want)
			}
		})
	}
}

// TestDeliveredMsgID covers msg_id extraction edge cases.
func TestDeliveredMsgID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data json.RawMessage
		want string
	}{
		{"well-formed", json.RawMessage(`{"msg_id":"rl-probe-abc"}`), "rl-probe-abc"},
		{"extra fields", json.RawMessage(`{"msg_id":"m1","ts":123}`), "m1"},
		{"missing msg_id", json.RawMessage(`{"ts":123}`), ""},
		{"empty msg_id", json.RawMessage(`{"msg_id":""}`), ""},
		{"malformed json", json.RawMessage(`not json`), ""},
		{"empty data", json.RawMessage(``), ""},
		{"null", json.RawMessage(`null`), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := deliveredMsgID(tc.data); got != tc.want {
				t.Errorf("deliveredMsgID(%s) = %q, want %q", tc.data, got, tc.want)
			}
		})
	}
}

// TestWaitForAtLeast covers the bounded-wait polling helper: already-satisfied, reached-before-timeout,
// never-reached (timeout), and ctx-cancellation. §VIII (no untested new code).
func TestWaitForAtLeast(t *testing.T) {
	t.Parallel()

	t.Run("already satisfied returns true immediately", func(t *testing.T) {
		t.Parallel()
		var c atomic.Int64
		c.Store(5)
		if !waitForAtLeast(context.Background(), &c, 3, time.Second) {
			t.Error("want true when counter already >= target")
		}
	})

	t.Run("reached before timeout returns true", func(t *testing.T) {
		t.Parallel()
		var c atomic.Int64
		go func() {
			time.Sleep(20 * time.Millisecond)
			c.Add(2)
		}()
		if !waitForAtLeast(context.Background(), &c, 2, 2*time.Second) {
			t.Error("want true when counter reaches target before timeout")
		}
	})

	t.Run("never reached returns false on timeout", func(t *testing.T) {
		t.Parallel()
		var c atomic.Int64
		if waitForAtLeast(context.Background(), &c, 1, 50*time.Millisecond) {
			t.Error("want false when target is never reached")
		}
	})

	t.Run("ctx cancellation returns current state", func(t *testing.T) {
		t.Parallel()
		var c atomic.Int64
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if waitForAtLeast(ctx, &c, 1, time.Second) {
			t.Error("want false when ctx is canceled and target unmet")
		}
	})
}

// TestDecideRateLimitChecks covers the pure pass/fail decision over an assembled observation:
// all-pass, zero-signal, conn-closed, write-error, probe-undelivered, straggler-after-snapshot,
// and the boundary count==1. §VIII edge cases.
func TestDecideRateLimitChecks(t *testing.T) {
	t.Parallel()

	// A fully-passing observation: signal seen, connection alive, probe delivered, no stragglers.
	base := rateLimitObservation{
		RateLimitedCount:         400,
		ConnClosed:               false,
		WriteErrors:              0,
		Sent:                     500,
		ProbeDelivered:           true,
		RateLimitedAfterSnapshot: 0,
	}

	statusByName := func(checks []metrics.CheckResult, name string) string {
		for _, c := range checks {
			if c.Name == name {
				return c.Status
			}
		}
		return "<absent>"
	}

	cases := []struct {
		name         string
		mutate       func(o *rateLimitObservation)
		wantEnforce  string
		wantSurvives string
		wantRecovers string
	}{
		{
			name:         "all pass",
			mutate:       func(o *rateLimitObservation) {},
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusPass,
			wantRecovers: metrics.CheckStatusPass,
		},
		{
			name:         "boundary count==1 still passes enforcement",
			mutate:       func(o *rateLimitObservation) { o.RateLimitedCount = 1 },
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusPass,
			wantRecovers: metrics.CheckStatusPass,
		},
		{
			name:         "zero signal fails enforcement (no skip)",
			mutate:       func(o *rateLimitObservation) { o.RateLimitedCount = 0 },
			wantEnforce:  metrics.CheckStatusFail,
			wantSurvives: metrics.CheckStatusPass,
			wantRecovers: metrics.CheckStatusPass,
		},
		{
			name:         "connection closed fails survives",
			mutate:       func(o *rateLimitObservation) { o.ConnClosed = true },
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusFail,
			wantRecovers: metrics.CheckStatusPass,
		},
		{
			name:         "write error fails survives",
			mutate:       func(o *rateLimitObservation) { o.WriteErrors = 3 },
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusFail,
			wantRecovers: metrics.CheckStatusPass,
		},
		{
			name:         "probe undelivered fails recovery",
			mutate:       func(o *rateLimitObservation) { o.ProbeDelivered = false },
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusPass,
			wantRecovers: metrics.CheckStatusFail,
		},
		{
			name:         "straggler rate_limited after snapshot fails recovery",
			mutate:       func(o *rateLimitObservation) { o.RateLimitedAfterSnapshot = 2 },
			wantEnforce:  metrics.CheckStatusPass,
			wantSurvives: metrics.CheckStatusPass,
			wantRecovers: metrics.CheckStatusFail,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obs := base
			tc.mutate(&obs)
			checks := decideRateLimitChecks(obs)

			if len(checks) != 3 {
				t.Fatalf("got %d checks, want 3", len(checks))
			}
			// No skip is ever emitted.
			for _, c := range checks {
				if c.Status == metrics.CheckStatusSkip {
					t.Errorf("check %q is skip — the suite must never skip", c.Name)
				}
			}
			if got := statusByName(checks, "rate limit enforcement"); got != tc.wantEnforce {
				t.Errorf("enforcement = %q, want %q", got, tc.wantEnforce)
			}
			if got := statusByName(checks, "connection survives rate limit"); got != tc.wantSurvives {
				t.Errorf("survives = %q, want %q", got, tc.wantSurvives)
			}
			if got := statusByName(checks, "publish recovers after refill"); got != tc.wantRecovers {
				t.Errorf("recovers = %q, want %q", got, tc.wantRecovers)
			}
		})
	}
}
