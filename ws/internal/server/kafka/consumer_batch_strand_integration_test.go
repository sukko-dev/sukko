//go:build integration

package kafka

import (
	"context"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// TestConsumeLoop_PartialBatchNotStranded is the regression guard for the batched-consume-loop
// stranding bug: the loop's `default:` case blocked in PollFetches, so `case <-flushTimer.C` was
// never serviced and an accumulated partial batch (< batchSize) sat in memory until the next record
// happened to arrive on any topic — silent, unbounded delivery latency (Constitution §VII).
//
// Repro shape: once the consumer has joined, produce a discrete burst of N records (N < batchSize)
// and then produce NOTHING more. All N must be broadcast within a few × batchTimeout. batchSize is
// set very high (1000) so the burst can ONLY be flushed by the timer, never by a full-batch flush —
// making this a clean discriminator: against the pre-fix loop the burst strands and the assertion
// times out; with the fix (a dedicated poller goroutine feeds records over a channel, so the select's
// flush-timer case is always serviced) the timer drains it within batchTimeout.
func TestConsumeLoop_PartialBatchNotStranded(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is not available — skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.1.1")
	if err != nil {
		t.Skipf("failed to start Redpanda container (Docker required): %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("get broker address: %v", err)
	}

	const (
		tenant    = "acme"
		topicName = "local.acme.strand"
		groupID   = "test-strand-group"
	)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create admin kgo client: %v", err)
	}
	adm := kadm.NewClient(adminClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topicName); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	adminClient.Close()

	var broadcastCount atomic.Int32
	firstBroadcast := make(chan struct{}, 1)
	broadcastFn := func(_ string, _ []byte, _ string, _ int32, _ int64) error {
		broadcastCount.Add(1)
		select {
		case firstBroadcast <- struct{}{}:
		default:
		}
		return nil
	}

	logger := zerolog.Nop()
	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{broker},
		ConsumerGroup:         groupID,
		Topics:                []string{topicName},
		Logger:                &logger,
		Broadcast:             broadcastFn,
		ResourceGuard:         &mockResourceGuardFixed{allowKafka: true},
		TenantResolver:        func(string) (string, bool) { return tenant, true }, // topic → tenant (registry stand-in)
		ConsumerType:          ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 10 * time.Second,
		AutoCommitInterval:    1 * time.Second,
		// Batching ON (BatchSize > 1). BatchSize deliberately huge so a small burst can only be
		// delivered by the flush timer — never by the full-batch path that would mask the strand.
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
		Registerer:   prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)

	// Phase 1 — join: produce continuously until the consumer broadcasts its first record. The
	// consumer starts AtEnd, so records produced before assignment are invisible; continuous
	// production removes the partition-assignment timing race.
	stopWarmup := make(chan struct{})
	warmupDone := make(chan struct{})
	go func() {
		defer close(warmupDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopWarmup:
				return
			default:
			}
			_ = producer.ProduceSync(ctx, &kgo.Record{
				Topic:   topicName,
				Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte(tenant + ".warmup")}},
				Value:   []byte(`{"w":1}`),
			})
		}
	}()
	select {
	case <-firstBroadcast:
	case <-ctx.Done():
		t.Fatal("timeout waiting for first broadcast — consumer may not have joined the group")
	}
	close(stopWarmup)
	<-warmupDone

	// Let warmup records drain so the measured burst is a clean partial batch. With the fix the
	// whole warmup drains; against the pre-fix loop the warmup's own tail also strands here (which is
	// fine — we assert on the DELTA from a settled baseline).
	time.Sleep(1 * time.Second)
	base := broadcastCount.Load()

	// Phase 2 — a discrete burst of N < batchSize, then NOTHING more.
	const burst = 10
	for i := range burst {
		rec := &kgo.Record{
			Topic:   topicName,
			Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte(tenant + ".burst")}},
			Value:   []byte(`{"b":1}`),
		}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce burst record %d: %v", i, err)
		}
	}

	// Phase 3 — all N must be broadcast within a few × batchTimeout. Pre-fix: the burst joins the
	// stranded batch and is never flushed (no further records arrive), so this times out.
	deadline := time.After(5 * time.Second)
	for broadcastCount.Load() < base+burst { // exits when the partial batch has been flushed
		select {
		case <-deadline:
			t.Fatalf("partial batch stranded: got %d broadcasts, want >= %d (baseline %d + burst %d) — the burst tail was never flushed",
				broadcastCount.Load(), base+burst, base, burst)
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Logf("burst delivered: baseline=%d final=%d", base, broadcastCount.Load())
}
