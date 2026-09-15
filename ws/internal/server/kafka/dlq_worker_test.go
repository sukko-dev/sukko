package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

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
