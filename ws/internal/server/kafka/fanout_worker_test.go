package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestFanoutPool builds a FanoutPool without Prometheus registration.
func newTestFanoutPool(workers, queueSize int, client *kgo.Client) *FanoutPool {
	return &FanoutPool{
		handoff: make(chan fanoutJob, queueSize),
		client:  client,
		logger:  zerolog.Nop(),
		workers: workers,
		// counters intentionally nil — nil-guarded in Submit/process
	}
}

func TestFanoutPool_Submit_QueueFull_ReturnsFalse(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	// queueSize=1; fill it with one job (no workers draining), second submit must fail.
	pool := newTestFanoutPool(0, 1, client)

	successCh := make(chan string, 1)
	failCh := make(chan string, 1)

	job := fanoutJob{
		topic:   "test.acme.orders",
		channel: "acme.orders",

		tenant: "acme",
		record: &kgo.Record{
			Value: []byte("payload"),
		},
		successCh: successCh,
		failCh:    failCh,
	}

	first := pool.Submit(job)
	if !first {
		t.Error("first Submit should succeed (queue has capacity)")
	}

	second := pool.Submit(job)
	if second {
		t.Error("second Submit should fail (queue full)")
	}
}

func TestFanoutPool_Submit_QueueEmpty_ReturnsTrue(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(0, 10, client)

	successCh := make(chan string, 1)
	failCh := make(chan string, 1)

	job := fanoutJob{
		topic:   "test.acme.quotes",
		channel: "acme.quotes",

		tenant:    "acme",
		record:    &kgo.Record{Value: []byte("msg")},
		successCh: successCh,
		failCh:    failCh,
	}

	if !pool.Submit(job) {
		t.Error("Submit should return true when queue has capacity")
	}
}

func TestFanoutPool_Process_FailPath_ReportsToFailCh(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(1, 16, client)

	ctx := t.Context()

	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	successCh := make(chan string, 1)
	failCh := make(chan string, 1)

	pool.Submit(fanoutJob{
		topic:   "test.acme.dead-letter",
		channel: "acme.orders",

		tenant:    "acme",
		record:    &kgo.Record{Value: []byte("payload")},
		successCh: successCh,
		failCh:    failCh,
	})

	select {
	case topic := <-failCh:
		if topic != "test.acme.dead-letter" {
			t.Errorf("failCh topic = %q, want %q", topic, "test.acme.dead-letter")
		}
	case <-successCh:
		t.Error("expected failCh, got successCh (unreachable broker should always fail)")
	case <-time.After(3 * time.Second):
		t.Fatal("no result on failCh within timeout")
	}
}

func TestFanoutPool_Process_OriginalRecordNotMutated(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(1, 16, client)

	ctx := t.Context()

	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	original := &kgo.Record{
		Value:   []byte("data"),
		Headers: []kgo.RecordHeader{{Key: "x-existing", Value: []byte("v")}},
	}

	successCh := make(chan string, 1)
	failCh := make(chan string, 1)

	pool.Submit(fanoutJob{
		topic:   "test.acme.orders",
		channel: "acme.orders",

		tenant:    "acme",
		record:    original,
		successCh: successCh,
		failCh:    failCh,
	})

	select {
	case <-failCh:
	case <-successCh:
	case <-time.After(3 * time.Second):
		t.Fatal("no result within timeout")
	}

	// Original record must not be mutated.
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

	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fanout pool did not shut down within timeout after context cancel")
	}
}
