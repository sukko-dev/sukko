package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newFailingKgoClient(t *testing.T) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:1"), // unreachable — all produces fail
		kgo.RecordRetries(0),
		kgo.ProduceRequestTimeout(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("newFailingKgoClient: %v", err)
	}
	return client
}

// newTestDLQPool builds a DLQPool without Prometheus registration.
func newTestDLQPool(cfg DLQConfig, client *kgo.Client) *DLQPool {
	return &DLQPool{
		jobs:   make(chan dlqJob, max(cfg.Workers*16, 1)),
		client: client,
		cfg:    cfg,
		logger: zerolog.Nop().With().Str("component", "dlq_pool").Logger(),
		// writeFailedCounter intentionally nil — nil-guarded in process()
	}
}

func TestDLQPool_ExhaustsRetries_IncrementsCounter(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	cfg := DLQConfig{
		MaxRetries: 1,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   20 * time.Millisecond,
		Workers:    1,
		Namespace:  "test",
	}

	pool := newTestDLQPool(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	if !pool.TrySubmit(dlqJob{
		record: &kgo.Record{
			Topic: "test.acme.dead-letter",
			Value: []byte("payload"),
		},
		tenant: "acme",
		reason: ReasonNoRoutingRuleMatched,
	}) {
		t.Fatal("TrySubmit returned false on empty queue")
	}

	// Allow worker to finish processing (1 retry + backoff).
	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestDLQPool_ContextCancelledDuringBackoff_ShutsDown(t *testing.T) {
	t.Parallel()

	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	cfg := DLQConfig{
		MaxRetries: 5,
		BaseDelay:  500 * time.Millisecond, // long enough to be canceled mid-backoff
		MaxDelay:   2 * time.Second,
		Workers:    1,
		Namespace:  "test",
	}

	pool := newTestDLQPool(cfg, client)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	pool.TrySubmit(dlqJob{
		record: &kgo.Record{
			Topic: "test.acme.dead-letter",
			Value: []byte("payload"),
		},
		tenant: "acme",
		reason: ReasonFanoutTopicWriteFailed,
	})

	// Cancel while worker is in the first retry backoff.
	time.Sleep(60 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DLQ pool did not shut down within timeout after context cancel")
	}
}

// TestDLQPool_DrainAbandoned_CountsQueuedJobs pins the ADR-0018 "surfaced, never
// silent" rule for shutdown: when the pool stops with jobs still queued, each is
// counted (and logged), not dropped invisibly. The producer's Close cancels the
// DLQ context only after the fan-out pool has drained, so no sender races this.
func TestDLQPool_DrainAbandoned_CountsQueuedJobs(t *testing.T) {
	t.Parallel()

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_dlq_drop_total"},
		[]string{LabelTenant, LabelReason},
	)
	pool := &DLQPool{
		jobs:               make(chan dlqJob, 8),
		logger:             zerolog.Nop(),
		writeFailedCounter: counter,
	}

	// Queue jobs that will never be processed — the worker is shutting down.
	pool.jobs <- dlqJob{record: &kgo.Record{Topic: "t"}, tenant: "acme", reason: ReasonFanoutTopicWriteFailed}
	pool.jobs <- dlqJob{record: &kgo.Record{Topic: "t"}, tenant: "acme", reason: ReasonFanoutTopicWriteFailed}
	pool.jobs <- dlqJob{record: &kgo.Record{Topic: "t"}, tenant: "beta", reason: ReasonFanoutTopicWriteFailed}

	pool.drainAbandoned()

	if got := testutil.ToFloat64(counter.WithLabelValues("acme", ReasonFanoutTopicWriteFailed)); got != 2 {
		t.Errorf("acme drop count = %v, want 2 — abandoned jobs must be counted", got)
	}
	if got := testutil.ToFloat64(counter.WithLabelValues("beta", ReasonFanoutTopicWriteFailed)); got != 1 {
		t.Errorf("beta drop count = %v, want 1", got)
	}
	// Channel must be fully drained (non-blocking receive returns nothing).
	select {
	case j := <-pool.jobs:
		t.Errorf("drainAbandoned left a job queued: %+v", j)
	default:
	}
}
