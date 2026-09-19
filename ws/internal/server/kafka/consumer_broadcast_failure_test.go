package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// =============================================================================
// Broadcast Failure / Redelivery Tests
//
// These tests pin the at-least-once invariant at the consumer's mark-for-commit
// boundary: a record whose broadcast transiently fails MUST be retried in place
// until it is broadcast successfully, and MUST be marked for commit only after
// that success. They deliberately assert REDELIVERY WITHIN THE SESSION — not
// merely "no mark on failure" — because a pause-and-resume implementation that
// skips the failed record would pass the weaker assertion while still losing
// the message (franz-go does not redeliver polled-but-unmarked records within
// a live session; redelivery happens only on rebalance/restart/seek).
//
// Found by the sukko-dev/bench fault matrix: killing Valkey mid-run produced a
// permanent 28-sequence delivery hole per (subscriber, channel) because the
// consumer committed past records whose broadcast had failed.
// =============================================================================

// broadcastEventLog is a mutex-guarded, order-preserving event trace shared by
// the scripted broadcast function and the recording committer, so tests can
// assert the exact interleaving of broadcast attempts and commit marks.
type broadcastEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *broadcastEventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *broadcastEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

// countEvents returns how many events in the snapshot match prefix.
func (l *broadcastEventLog) countPrefix(prefix string) int {
	n := 0
	for _, e := range l.snapshot() {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// markLogger implements committer, appending "mark:<offset>" events to the
// shared log so mark placement relative to broadcast attempts is provable.
type markLogger struct {
	log *broadcastEventLog
}

func (c *markLogger) CommitMarkedOffsets(_ context.Context) error { return nil }

func (c *markLogger) MarkCommitRecords(rs ...*kgo.Record) {
	for _, r := range rs {
		c.log.add(fmt.Sprintf("mark:%d", r.Offset))
	}
}

// scriptedBroadcast returns a BroadcastFunc that fails the first failures[offset]
// attempts for each offset (recording "fail:<offset>") and then succeeds
// (recording "ok:<offset>").
func scriptedBroadcast(log *broadcastEventLog, failures map[int64]int) BroadcastFunc {
	var mu sync.Mutex
	remaining := maps.Clone(failures)
	return func(_ string, _ []byte, _ string, _ int32, offset int64) error {
		mu.Lock()
		defer mu.Unlock()
		if remaining[offset] > 0 {
			remaining[offset]--
			log.add(fmt.Sprintf("fail:%d", offset))
			return errors.New("broadcast bus unavailable (test)")
		}
		log.add(fmt.Sprintf("ok:%d", offset))
		return nil
	}
}

// newBroadcastFailureConsumer builds a consumer with a scripted broadcast, a
// recording committer sharing the same event log, and short retry backoffs.
func newBroadcastFailureConsumer(t *testing.T, log *broadcastEventLog, failures map[int64]int) *Consumer {
	t.Helper()
	consumer, _, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		Broadcast:     scriptedBroadcast(log, failures),
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true},
	})
	// Shrink backoffs so failure cases retry in milliseconds (fields exist for
	// exactly this; production always uses the package constants).
	consumer.broadcastRetryInitial = time.Millisecond
	consumer.broadcastRetryMax = 4 * time.Millisecond
	consumer.committer = &markLogger{log: log}
	return consumer
}

// TestBroadcastFailure_RetriedInSessionThenMarked asserts the core invariant on
// the unbatched path: a record whose broadcast transiently fails is retried IN
// SESSION until it broadcasts successfully, and is marked only after that
// success. A pause-and-lose implementation (skip the failed record, keep going)
// fails this test because "ok:<offset>" never appears.
func TestBroadcastFailure_RetriedInSessionThenMarked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		failures int
	}{
		{name: "single transient failure", failures: 1},
		{name: "several transient failures", failures: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := &broadcastEventLog{}
			consumer := newBroadcastFailureConsumer(t, log, map[int64]int{7: tt.failures})

			record := makeRecord("sukko.test.trade")
			record.Offset = 7
			consumer.processRecord(record)

			want := make([]string, 0, tt.failures+2)
			for range tt.failures {
				want = append(want, "fail:7")
			}
			want = append(want, "ok:7", "mark:7")

			if got := log.snapshot(); !slices.Equal(got, want) {
				t.Errorf("event order: got %v, want %v (record must be re-broadcast in session and marked only after success)", got, want)
			}
		})
	}
}

// TestBroadcastFailure_BatchWithholdsLaterMarks asserts the batched-path
// invariant: when record i fails to broadcast, no record after i on the batch
// is broadcast or marked until i has been broadcast successfully — order is
// preserved and no mark ever jumps ahead of an undelivered record.
func TestBroadcastFailure_BatchWithholdsLaterMarks(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	consumer := newBroadcastFailureConsumer(t, log, map[int64]int{2: 2})

	batch := make([]preparedMessage, 0, 3)
	for _, offset := range []int64{1, 2, 3} {
		record := makeRecord("sukko.test.trade")
		record.Offset = offset
		msg, ctxCanceled := consumer.prepareMessage(record)
		if msg == nil || ctxCanceled {
			t.Fatalf("prepareMessage(offset=%d) should succeed", offset)
		}
		batch = append(batch, *msg)
	}

	if !consumer.deliverBatch(batch) {
		t.Fatal("deliverBatch returned false, want true (context never canceled)")
	}

	want := []string{
		"ok:1", "mark:1",
		"fail:2", "fail:2", "ok:2", "mark:2",
		"ok:3", "mark:3",
	}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Errorf("event order: got %v, want %v (later records must wait behind the failing one)", got, want)
	}
}

// TestBroadcastFailure_CtxCancelDuringRetry asserts the shutdown contract: when
// the consumer context is canceled while a record is stuck in broadcast retry,
// deliverBatch exits promptly (no goroutine leak), the stuck record and every
// record after it are NOT marked, and no later record is broadcast. Those
// records redeliver after restart/rebalance — at-least-once preserved.
func TestBroadcastFailure_CtxCancelDuringRetry(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	// Effectively permanent failure for offset 1: the outage outlives the test.
	consumer := newBroadcastFailureConsumer(t, log, map[int64]int{1: 1 << 30})

	batch := make([]preparedMessage, 0, 2)
	for _, offset := range []int64{1, 2} {
		record := makeRecord("sukko.test.trade")
		record.Offset = offset
		msg, ctxCanceled := consumer.prepareMessage(record)
		if msg == nil || ctxCanceled {
			t.Fatalf("prepareMessage(offset=%d) should succeed", offset)
		}
		batch = append(batch, *msg)
	}

	done := make(chan bool, 1)
	go func() { done <- consumer.deliverBatch(batch) }()

	// Wait until the first record is provably stuck in retry, then cancel.
	deadline := time.After(3 * time.Second)
	for log.countPrefix("fail:1") == 0 {
		select {
		case <-deadline:
			t.Fatal("broadcast was never attempted")
		case <-time.After(time.Millisecond):
		}
	}
	consumer.cancel()

	select {
	case ok := <-done:
		if ok {
			t.Error("deliverBatch returned true, want false (canceled mid-retry)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deliverBatch did not return after context cancellation — retry loop leaked")
	}

	events := log.snapshot()
	if n := log.countPrefix("mark:"); n != 0 {
		t.Errorf("marks after cancellation: got %d (%v), want 0 — records must redeliver, not commit", n, events)
	}
	if n := log.countPrefix("ok:"); n != 0 {
		t.Errorf("successful broadcasts: got %d (%v), want 0", n, events)
	}
	if n := log.countPrefix("fail:2"); n != 0 {
		t.Errorf("record 2 was broadcast while record 1 was undelivered (%v)", events)
	}
}

// TestBroadcastFailure_DeliberateDropsStillMark asserts that the deliberate-drop
// sites (rate limit, DLQ route) still mark the record for commit even while the
// broadcast bus is failing — those records never reach the bus, so bus health
// must not affect their commit behavior.
func TestBroadcastFailure_DeliberateDropsStillMark(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		allowKafka bool
		emptyKey   bool // empty channel key routes to DLQ
	}{
		{name: "rate-limited record is marked", allowKafka: false, emptyKey: false},
		{name: "DLQ-routed record is marked", allowKafka: true, emptyKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log := &broadcastEventLog{}
			consumer, _, _ := newRebalanceTestConsumer(t, ConsumerConfig{
				// Bus permanently down: every broadcast attempt would fail.
				Broadcast: func(_ string, _ []byte, _ string, _ int32, offset int64) error {
					log.add(fmt.Sprintf("fail:%d", offset))
					return errors.New("broadcast bus unavailable (test)")
				},
				ResourceGuard: &mockResourceGuardFixed{allowKafka: tt.allowKafka},
			})
			consumer.broadcastRetryInitial = time.Millisecond
			consumer.broadcastRetryMax = 4 * time.Millisecond
			consumer.committer = &markLogger{log: log}

			record := makeRecord("sukko.test.trade")
			record.Offset = 9
			if tt.emptyKey {
				record.Key = nil
				record.Headers = nil
			}
			consumer.processRecord(record)

			if n := log.countPrefix("fail:"); n != 0 {
				t.Errorf("broadcast attempted %d times for a deliberately dropped record, want 0", n)
			}
			if got, want := log.snapshot(), []string{"mark:9"}; !slices.Equal(got, want) {
				t.Errorf("events: got %v, want %v (deliberate drop must still mark)", got, want)
			}
		})
	}
}

// makeFetches wraps records into a single-partition kgo.Fetches, in order —
// the shape consumeFetchRecords receives from PollFetches.
func makeFetches(topic string, records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      topic,
			Partitions: []kgo.FetchPartition{{Records: records}},
		}},
	}}
}

// TestBroadcastFailure_NoMarkAfterAbandonedRecord pins the post-abort commit
// hole on the unbatched path: when record N is abandoned unmarked (consumer
// context canceled while N was stuck in broadcast retry), NO later record in
// the same fetch may be processed — in particular a deliberate-drop record
// K > N on the same partition must NOT mark. Under kgo.AutoCommitMarks
// cumulative commit, marking K would commit K+1 and silently cover the
// abandoned, undelivered N: the exact defect class this fix exists to remove,
// one select-arm later. consumeFetchRecords must report the abort so the
// consume loop stops consuming entirely.
func TestBroadcastFailure_NoMarkAfterAbandonedRecord(t *testing.T) {
	t.Parallel()
	log := &broadcastEventLog{}
	// Bus permanently down for the test's lifetime: offset 1 never broadcasts.
	consumer := newBroadcastFailureConsumer(t, log, map[int64]int{1: 1 << 30})

	recN := makeRecord("sukko.test.trade")
	recN.Offset = 1
	recK := makeRecord("sukko.test.trade") // empty key → DLQ → deliberate drop
	recK.Offset = 2
	recK.Key = nil
	recK.Headers = nil

	fetches := makeFetches("sukko.test.trade", recN, recK)

	done := make(chan bool, 1)
	go func() { done <- consumer.consumeFetchRecords(fetches) }()

	// Wait until record 1 is provably stuck in retry, then cancel the consumer.
	deadline := time.After(3 * time.Second)
	for log.countPrefix("fail:1") == 0 {
		select {
		case <-deadline:
			t.Fatal("broadcast was never attempted for record 1")
		case <-time.After(time.Millisecond):
		}
	}
	consumer.cancel()

	var aborted bool
	select {
	case aborted = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumeFetchRecords did not return after context cancellation")
	}

	if !aborted {
		t.Error("consumeFetchRecords returned false, want true (abort must stop the consume loop)")
	}
	if n := log.countPrefix("mark:"); n != 0 {
		t.Errorf("marks after abandoned record: got %d (%v), want 0 — a late mark commits past the abandoned offset", n, log.snapshot())
	}
}
