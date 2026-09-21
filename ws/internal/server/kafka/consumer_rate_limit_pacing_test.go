package kafka

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Rate-limit pacing tests (ADR-0022)
//
// The consume-loop rate limiter MUST pace (bounded-block until a token is
// available), NOT drop-and-mark. Drop-and-mark permanently loses records during
// recovery catch-up: when a partition is reassigned after an owner death, the
// survivor drains the accumulated backlog faster than WS_MAX_KAFKA_RATE, so the
// old drop-and-mark arm shed the excess AND committed past it — a silent
// at-least-once violation (found by the sukko-dev/bench fault matrix as ~700
// permanent holes on a deterministic owner-kill). Pacing mirrors the CPU
// emergency brake (LAYER 2), which already backpressures for zero loss.
//
// These tests assert, on BOTH consume paths, that a record denied by the rate
// limiter is retried in place until admitted and delivered — never dropped,
// never marked before broadcast — and that a context cancel during the pacing
// wait abandons the record UNMARKED (redelivered after restart/rebalance).
// =============================================================================

// mockResourceGuardPacing denies the first denyCalls AllowKafkaMessage calls
// (returning a non-zero waitDuration, as the real limiter does when a token is
// not yet available), then admits every subsequent call. denyForever overrides
// denyCalls and never admits — used to exercise the ctx-cancel-during-pace path.
type mockResourceGuardPacing struct {
	mu          sync.Mutex
	denyCalls   int
	denyForever bool
	calls       int
}

func (m *mockResourceGuardPacing) AllowKafkaMessage(_ context.Context) (bool, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.denyForever || m.calls <= m.denyCalls {
		return false, time.Millisecond // token not yet available; caller must wait then retry
	}
	return true, 0
}

func (m *mockResourceGuardPacing) ShouldPauseKafka() bool { return false }

func (m *mockResourceGuardPacing) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// newPacingConsumer builds a consumer whose broadcast always succeeds, sharing
// the event log with a recording committer, and driven by the given guard.
func newPacingConsumer(t *testing.T, log *broadcastEventLog, guard ResourceGuard) *Consumer {
	t.Helper()
	consumer, _, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		Broadcast:     scriptedBroadcast(log, nil), // nil failures → every broadcast succeeds
		ResourceGuard: guard,
	})
	consumer.broadcastRetryInitial = time.Millisecond
	consumer.broadcastRetryMax = 4 * time.Millisecond
	consumer.committer = &markLogger{log: log}
	return consumer
}

// TestRateLimitPacing_UnbatchedDeliversAllUnderRateLimit: on the unbatched path,
// records denied by the limiter are paced (not dropped) and every one is
// broadcast then marked, in order. The old drop-and-mark arm fails this: denied
// records produce "mark:" with no "ok:" (dropped, committed past).
func TestRateLimitPacing_UnbatchedDeliversAllUnderRateLimit(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	guard := &mockResourceGuardPacing{denyCalls: 3} // the first record paces through 3 denials
	consumer := newPacingConsumer(t, log, guard)

	for _, offset := range []int64{1, 2, 3, 4, 5} {
		record := makeRecord("sukko.test.trade")
		record.Offset = offset
		if abort := consumer.processRecord(record); abort {
			t.Fatalf("processRecord(offset=%d) aborted unexpectedly (ctx not canceled)", offset)
		}
	}

	want := []string{"ok:1", "mark:1", "ok:2", "mark:2", "ok:3", "mark:3", "ok:4", "mark:4", "ok:5", "mark:5"}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Errorf("events: got %v, want %v (denied records must be paced+delivered, not dropped)", got, want)
	}
	// Every record broadcast then marked, in order (the want above) proves nothing
	// was dropped — the drop counter was removed with the drop path (ADR-0022).
	if guard.callCount() < 8 { // >=3 denials + 5 admits
		t.Errorf("guard called %d times, want >=8 (pacing must retry the denied record)", guard.callCount())
	}
}

// TestRateLimitPacing_BatchedDeliversAllUnderRateLimit: on the batched path,
// prepareMessage paces internally (never returns the deliberate-drop result for
// a rate-limited record), so every record joins the batch and is delivered.
func TestRateLimitPacing_BatchedDeliversAllUnderRateLimit(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	guard := &mockResourceGuardPacing{denyCalls: 3}
	consumer := newPacingConsumer(t, log, guard)

	batch := make([]preparedMessage, 0, 5)
	for _, offset := range []int64{1, 2, 3, 4, 5} {
		record := makeRecord("sukko.test.trade")
		record.Offset = offset
		msg, ctxCanceled := consumer.prepareMessage(record)
		if ctxCanceled {
			t.Fatalf("prepareMessage(offset=%d) reported ctx-cancel unexpectedly", offset)
		}
		if msg == nil {
			t.Fatalf("prepareMessage(offset=%d) returned nil (rate-limited record must be paced, not dropped)", offset)
		}
		batch = append(batch, *msg)
	}

	if !consumer.deliverBatch(batch) {
		t.Fatal("deliverBatch returned false, want true (context never canceled)")
	}

	want := []string{"ok:1", "mark:1", "ok:2", "mark:2", "ok:3", "mark:3", "ok:4", "mark:4", "ok:5", "mark:5"}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Errorf("events: got %v, want %v", got, want)
	}
}

// TestRateLimitPacing_CtxCancelDuringPaceUnbatched: when the consumer context is
// canceled while a record is stuck pacing (limiter permanently denying), the
// unbatched path returns abort=true and never marks the record — it redelivers
// after restart/rebalance, preserving at-least-once.
func TestRateLimitPacing_CtxCancelDuringPaceUnbatched(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	guard := &mockResourceGuardPacing{denyForever: true}
	consumer := newPacingConsumer(t, log, guard)

	record := makeRecord("sukko.test.trade")
	record.Offset = 42

	done := make(chan bool, 1)
	go func() { done <- consumer.processRecord(record) }()

	// Wait until the record is provably pacing (guard called at least twice),
	// then cancel.
	deadline := time.After(3 * time.Second)
	for guard.callCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("record never entered the pacing loop")
		case <-time.After(time.Millisecond):
		}
	}
	consumer.cancel()

	select {
	case abort := <-done:
		if !abort {
			t.Error("processRecord returned abort=false, want true (ctx canceled during pace)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("processRecord did not return after cancellation — pacing loop leaked")
	}

	if n := log.countPrefix("mark:"); n != 0 {
		t.Errorf("marks after cancellation: got %d (%v), want 0 — record must redeliver", n, log.snapshot())
	}
}

// TestRateLimitPacing_CtxCancelDuringPaceBatched: the batched path's
// prepareMessage returns (nil, true) — the no-mark abort result — when ctx is
// canceled during pacing, so the caller stops consuming without marking.
func TestRateLimitPacing_CtxCancelDuringPaceBatched(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	guard := &mockResourceGuardPacing{denyForever: true}
	consumer := newPacingConsumer(t, log, guard)

	record := makeRecord("sukko.test.trade")
	record.Offset = 42

	type result struct {
		msg       *preparedMessage
		ctxCancel bool
	}
	done := make(chan result, 1)
	go func() {
		msg, ctxCancel := consumer.prepareMessage(record)
		done <- result{msg, ctxCancel}
	}()

	deadline := time.After(3 * time.Second)
	for guard.callCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("record never entered the pacing loop")
		case <-time.After(time.Millisecond):
		}
	}
	consumer.cancel()

	select {
	case r := <-done:
		if r.msg != nil {
			t.Error("prepareMessage returned a message, want nil (ctx canceled during pace)")
		}
		if !r.ctxCancel {
			t.Error("prepareMessage returned noMark=false, want true (ctx-cancel abort)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("prepareMessage did not return after cancellation — pacing loop leaked")
	}

	if n := log.countPrefix("mark:"); n != 0 {
		t.Errorf("marks after cancellation: got %d, want 0", n)
	}
}
