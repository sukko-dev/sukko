package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// TestWaitForDeny_DenyWinsProbe_Deterministic pins the exact deny-wins probe (migrated from
// the api-key sibling's TestWaitForPrivateDeny_DenyWinsProbe_Deterministic): an issue transport
// error with a deny already observed MUST resolve denied (pass). Driven at the helper level via a
// pre-buffered errCh + pollDenyFromErrCh, because the end-to-end mock cannot deterministically reach
// "transport error with pending deny".
func TestWaitForDeny_DenyWinsProbe_Deterministic(t *testing.T) {
	t.Parallel()

	errCh := make(chan testerws.Message, 1)
	errCh <- testerws.Message{Type: respTypeSubscribeError, Code: wsErrCodeInvalidRequest} // buffered deny

	issue := func() error { return errors.New("write ws message: connection reset") }

	outcome, err := waitForDeny(context.Background(), issue,
		pollDenyFromErrCh(errCh, respTypeSubscribeError, wsErrCodeInvalidRequest),
		time.Second, 100*time.Millisecond, zerolog.Nop())
	if outcome != denyOutcomeDenied {
		t.Errorf("outcome = %d, want denyOutcomeDenied (buffered deny wins over transport error)", outcome)
	}
	if err != nil {
		t.Errorf("err = %v, want nil on deny-wins", err)
	}
}

// TestWaitForDeny_DenyWinsAtDeadline pins the deadline-boundary deny-wins guard (migrated): a deny
// observable when the deadline fires MUST resolve denied, never timedOut — otherwise select's
// pseudo-random tie-break would reintroduce a narrow #216. A tiny deadline makes denyCtx.Done() and
// the buffered deny both ready in the loop select; a huge interval prevents an intervening retry.
func TestWaitForDeny_DenyWinsAtDeadline(t *testing.T) {
	t.Parallel()

	errCh := make(chan testerws.Message, 1)
	errCh <- testerws.Message{Type: respTypeSubscribeError, Code: wsErrCodeInvalidRequest}

	issue := func() error { return nil } // succeeds → enter the select loop

	outcome, err := waitForDeny(context.Background(), issue,
		pollDenyFromErrCh(errCh, respTypeSubscribeError, wsErrCodeInvalidRequest),
		time.Millisecond, time.Hour, zerolog.Nop())
	if outcome != denyOutcomeDenied {
		t.Errorf("outcome = %d, want denyOutcomeDenied (deny buffered at the deadline must win, not time out)", outcome)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

// TestWaitForDeny_TransportError_ParentCancelled pins the transport-path guard (migrated): an
// issue transport error with NO pending deny and a canceled parent ctx MUST resolve canceled (clean
// short-circuit), never transportError. Uses a pre-canceled ctx (no wall-clock sleep).
func TestWaitForDeny_TransportError_ParentCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // parent already canceled

	issue := func() error { return errors.New("write ws message: use of closed connection") }
	poll := func() bool { return false } // no deny pending

	outcome, err := waitForDeny(ctx, issue, poll,
		time.Second, 100*time.Millisecond, zerolog.Nop())
	if outcome != denyOutcomeCancelled {
		t.Errorf("outcome = %d, want denyOutcomeCancelled (canceled parent must not record a failure)", outcome)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled (informational ctx error on the canceled outcome)", err)
	}
}

// TestWaitForDeny_NeverDenied_HardFail proves the strengthened check reds when no
// deny frame ever arrives: a stub poll that never matches → denyOutcomeTimedOut, which
// denyCheckResult maps to Status == fail. Also asserts the windowed-send bound: the loop stops
// re-issuing one interval before the deadline, so issue-count stays strictly below deadline/interval.
func TestWaitForDeny_NeverDenied_HardFail(t *testing.T) {
	t.Parallel()

	const (
		interval = 100 * time.Millisecond
		deadline = 300 * time.Millisecond
	)
	var issueCount atomic.Int64
	issue := func() error { issueCount.Add(1); return nil }
	poll := func() bool { return false } // never matches

	outcome, err := waitForDeny(context.Background(), issue, poll,
		deadline, interval, zerolog.Nop())
	if outcome != denyOutcomeTimedOut {
		t.Errorf("outcome = %d, want denyOutcomeTimedOut (never-denied must time out)", outcome)
	}
	if err != nil {
		t.Errorf("err = %v, want nil on timedOut", err)
	}

	result := denyCheckResult("publish auth rejection", outcome, err)
	if result.Status != metrics.CheckStatusFail {
		t.Errorf("denyCheckResult status = %q, want fail (never-denied must red the check)", result.Status)
	}

	// Windowed-send bound: strictly fewer issues than deadline/interval proves the final-interval
	// re-issue was suppressed. MUST be float division — integer Duration division (300ms/100ms == 3)
	// would collapse the bound to 3 < 3.
	bound := float64(deadline) / float64(interval) // 3.0
	if got := float64(issueCount.Load()); got >= bound {
		t.Errorf("issue invoked %v times, want < %v (loop must stop re-issuing one interval before the deadline)", got, bound)
	}
}

// TestWaitForDeny_TransportError_HardFail proves the publish-write-errors variant:
// a stub issue that always errors + a never-matching poll and a live parent ctx →
// denyOutcomeTransportError, which denyCheckResult maps to Status == fail.
func TestWaitForDeny_TransportError_HardFail(t *testing.T) {
	t.Parallel()

	issue := func() error { return errors.New("write ws message: broken pipe") }
	poll := func() bool { return false } // no deny pending, live parent ctx

	outcome, err := waitForDeny(context.Background(), issue, poll,
		time.Second, 100*time.Millisecond, zerolog.Nop())
	if outcome != denyOutcomeTransportError {
		t.Errorf("outcome = %d, want denyOutcomeTransportError (issue error + no deny must fail transport)", outcome)
	}
	if err == nil {
		t.Error("err = nil, want the underlying transport error")
	}

	result := denyCheckResult("publish auth rejection", outcome, err)
	if result.Status != metrics.CheckStatusFail {
		t.Errorf("denyCheckResult status = %q, want fail (transport error must red the check)", result.Status)
	}
}

// TestWaitForDeny_RetryReFires proves the bounded retry actually re-issues: a poll that
// stays false until issue has been invoked ≥2 times, then true → denied; assert issue fired >1×.
// Without a working re-issue loop this would time out (issue never reaches 2).
func TestWaitForDeny_RetryReFires(t *testing.T) {
	t.Parallel()

	var issueCount atomic.Int64
	issue := func() error { issueCount.Add(1); return nil }
	poll := func() bool { return issueCount.Load() >= 2 }

	outcome, err := waitForDeny(context.Background(), issue, poll,
		2*time.Second, 50*time.Millisecond, zerolog.Nop())
	if outcome != denyOutcomeDenied {
		t.Errorf("outcome = %d, want denyOutcomeDenied (deny arrives after a retry)", outcome)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got := issueCount.Load(); got <= 1 {
		t.Errorf("issue invoked %d times, want > 1 (retry must actually re-fire)", got)
	}
}

// TestWaitForDeny_JointMatchDiscrimination proves pollDenyFromErrCh discriminates on
// the FULL (type, code) pair: a right-type/wrong-code frame and a wrong-type/right-code frame each
// buffered on their own channel do NOT satisfy the wait → timedOut. Tiny deadline + huge interval so
// the single buffered frame is consumed by the first poll and no retry intervenes.
func TestWaitForDeny_JointMatchDiscrimination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  testerws.Message
	}{
		{"right type, wrong code", testerws.Message{Type: respTypeSubscribeError, Code: wsErrCodeForbidden}},
		{"wrong type, right code", testerws.Message{Type: respTypePublishError, Code: wsErrCodeInvalidRequest}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			errCh := make(chan testerws.Message, 1)
			errCh <- tt.msg
			issue := func() error { return nil }

			outcome, err := waitForDeny(context.Background(), issue,
				pollDenyFromErrCh(errCh, respTypeSubscribeError, wsErrCodeInvalidRequest),
				150*time.Millisecond, time.Hour, zerolog.Nop())
			if outcome != denyOutcomeTimedOut {
				t.Errorf("outcome = %d, want denyOutcomeTimedOut (a partial (type,code) match must NOT satisfy)", outcome)
			}
			if err != nil {
				t.Errorf("err = %v, want nil on timedOut", err)
			}
		})
	}
}

// TestWaitForDeny_DenyWinsOnRetry proves deny-wins on a later attempt: a store-backed
// poll yields the match only after the second issue (post the single pre-wait clear the caller owns).
// waitForDeny NEVER re-clears, so the accumulated match on the later attempt satisfies the wait.
func TestWaitForDeny_DenyWinsOnRetry(t *testing.T) {
	t.Parallel()

	// Simulates a TestUser-style accumulating store: the deny "arrives" only after the retry.
	var denied atomic.Bool
	var issueCount atomic.Int64
	issue := func() error {
		if issueCount.Add(1) >= 2 {
			denied.Store(true) // the platform's deny lands on the second attempt
		}
		return nil
	}
	poll := denied.Load

	outcome, err := waitForDeny(context.Background(), issue, poll,
		2*time.Second, 50*time.Millisecond, zerolog.Nop())
	if outcome != denyOutcomeDenied {
		t.Errorf("outcome = %d, want denyOutcomeDenied (deny observed on a later attempt must win)", outcome)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got := issueCount.Load(); got < 2 {
		t.Errorf("issue invoked %d times, want >= 2 (deny landed only after the retry)", got)
	}
}
