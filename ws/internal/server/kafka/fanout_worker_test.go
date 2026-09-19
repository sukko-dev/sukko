package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestFanoutPool builds a FanoutPool without Prometheus registration.
func newTestFanoutPool(workers, queueSize int, client *kgo.Client) *FanoutPool {
	return &FanoutPool{
		handoff:        make(chan fanoutJob, queueSize),
		client:         client,
		logger:         zerolog.Nop(),
		workers:        workers,
		topicNamespace: fanoutTestNamespace,
		// counters intentionally nil — nil-guarded in Submit/process/deadLetter
	}
}

// captureDLQ returns a DLQPool whose (undrained, buffered) jobs channel captures
// TrySubmit calls without any worker goroutines or broker.
func captureDLQ(capacity int) *DLQPool {
	return &DLQPool{jobs: make(chan dlqJob, capacity), logger: zerolog.Nop()}
}

// TestFanoutPool_Submit_QueueFull_DeadLetters: when the handoff queue is full the
// egress write was never attempted — Submit must dead-letter the job immediately
// (ADR-0018: an egress miss is surfaced, never silent).
func TestFanoutPool_Submit_QueueFull_DeadLetters(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	// queueSize=1 with no workers draining: the first job parks in the queue,
	// the second must overflow into the DLQ.
	pool := newTestFanoutPool(0, 1, client)
	dlq := captureDLQ(1)
	pool.dlq = dlq

	job := fanoutJob{
		topic:  "test.acme.orders",
		tenant: "acme",
		record: &kgo.Record{Value: []byte("payload")},
	}

	pool.Submit(job) // fills the queue
	pool.Submit(job) // overflows → dead-letter

	select {
	case dj := <-dlq.jobs:
		if dj.reason != ReasonFanoutTopicWriteFailed {
			t.Errorf("DLQ reason = %q, want %q", dj.reason, ReasonFanoutTopicWriteFailed)
		}
		if got := headerValue(dj.record, HeaderFailedTopics); got != "test.acme.orders" {
			t.Errorf("DLQ failed-topics header = %q, want %q", got, "test.acme.orders")
		}
		// Queue overflow means the write was NEVER attempted — the triage header
		// must say so, not claim a retry story (ADR-0018).
		if got := headerValue(dj.record, HeaderFailureKind); got != FailureKindNotAttempted {
			t.Errorf("DLQ failure-kind header = %q, want %q", got, FailureKindNotAttempted)
		}
	default:
		t.Fatal("queue-full Submit did not dead-letter the job")
	}
}

// TestFanoutPool_Process_FailPath_DeadLetters: a failed egress produce is routed
// to the tenant DLQ with the failure reason and the missed topic.
func TestFanoutPool_Process_FailPath_DeadLetters(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(1, 16, client)
	dlq := captureDLQ(1)
	pool.dlq = dlq

	ctx := t.Context()

	pool.Start(ctx)

	pool.Submit(fanoutJob{
		topic:  "test.acme.audit",
		tenant: "acme",
		record: &kgo.Record{Value: []byte("payload")},
	})

	select {
	case dj := <-dlq.jobs:
		if dj.tenant != "acme" {
			t.Errorf("DLQ tenant = %q, want %q", dj.tenant, "acme")
		}
		if got := headerValue(dj.record, HeaderFailedTopics); got != "test.acme.audit" {
			t.Errorf("DLQ failed-topics header = %q, want %q", got, "test.acme.audit")
		}
		if dj.record.Topic != "test.acme.dead-letter" {
			t.Errorf("DLQ record topic = %q, want %q", dj.record.Topic, "test.acme.dead-letter")
		}
		// An unreachable broker exhausts the client's retry policy — the record
		// must carry that classification plus the terminal error for triage
		// (ADR-0018; franz-go exposes no attempt count, so the kind is the fact).
		if got := headerValue(dj.record, HeaderFailureKind); got != FailureKindRetriesExhausted {
			t.Errorf("DLQ failure-kind header = %q, want %q", got, FailureKindRetriesExhausted)
		}
		if headerValue(dj.record, HeaderFailureCause) == "" {
			t.Error("DLQ failure-cause header empty — a failing artifact must carry its own triage")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed egress write was not dead-lettered within timeout")
	}
}

func TestFanoutPool_Process_OriginalRecordNotMutated(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(1, 16, client)
	dlq := captureDLQ(1)
	pool.dlq = dlq

	ctx := t.Context()

	pool.Start(ctx)

	original := &kgo.Record{
		Value:   []byte("data"),
		Headers: []kgo.RecordHeader{{Key: "x-existing", Value: []byte("v")}},
	}

	pool.Submit(fanoutJob{
		topic:  "test.acme.orders",
		tenant: "acme",
		record: original,
	})

	select {
	case <-dlq.jobs: // failing client → the job lands in the DLQ once processed
	case <-time.After(3 * time.Second):
		t.Fatal("no DLQ result within timeout")
	}

	// Original record must not be mutated (the DLQ record clones headers).
	if len(original.Headers) != 1 {
		t.Errorf("original record Headers len = %d, want 1 (must not be mutated)", len(original.Headers))
	}
}

func TestFanoutPool_ContextCancelled_ShutsDown(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(2, 16, client)

	ctx, cancel := context.WithCancel(context.Background())

	pool.Start(ctx)

	cancel()

	done := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fanout pool did not shut down within timeout after context cancel")
	}
}
