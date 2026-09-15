package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sukko-dev/sukko/internal/server/history"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// =============================================================================
// ReplayFromOffsets — kfake regression tests.
//
// The replay consumer was DEAD in production until the community-ingest e2e
// exercised it: its options combined kgo.ConsumerGroup + kgo.ConsumeTopics +
// kgo.ConsumePartitions, which kgo rejects at client creation ("invalid
// direct-partition consuming option when consuming as a group"), and the
// partition map used -1 as an invented "all partitions" wildcard. These tests
// pin the repaired contract end to end against an in-process broker:
// exact-partition consumption, subscription filtering, pos/mid coordinates,
// and OffsetOutOfRange surfacing (NoResetOffset — the kgo default would
// silently reset to the partition start instead).
// =============================================================================

const (
	replayTestTenant  = "acme"
	replayTestTopic   = "local.acme.replaysrc"
	replayTestChannel = "acme.BTC.trade"
)

// newReplayTestConsumer builds an unstarted Consumer against the kfake cluster.
// ReplayFromOffsets never touches the group consume loop — it only needs the
// client (for broker addresses), fetch tuning, and prepareMessage.
func newReplayTestConsumer(t *testing.T, cluster *kfake.Cluster) *Consumer {
	t.Helper()
	logger := zerolog.Nop()
	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               cluster.ListenAddrs(),
		ConsumerGroup:         "replay-test-group",
		Topics:                []string{replayTestTopic},
		Logger:                &logger,
		Broadcast:             func(string, []byte, string, int32, int64) {},
		ResourceGuard:         &mockResourceGuardFixed{allowKafka: true},
		TenantResolver:        func(string) (string, bool) { return replayTestTenant, true },
		ConsumerType:          ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 10 * time.Second,
		AutoCommitInterval:    1 * time.Second,
		FetchMaxWait:          250 * time.Millisecond,
		Registerer:            prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })
	return consumer
}

// produceAt synchronously produces one record to an explicit partition with the
// channel header the real ingest pipeline sets (prepareMessage drops
// headerless records before the subscription filter — a headerless test record
// would make an empty-result assertion pass vacuously).
func produceAt(ctx context.Context, t *testing.T, producer *kgo.Client, partition int32, channel string, value []byte) {
	t.Helper()
	res := producer.ProduceSync(ctx, &kgo.Record{
		Topic:     replayTestTopic,
		Partition: partition,
		Headers:   []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte(channel)}},
		Value:     value,
	})
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce to partition %d: %v", partition, err)
	}
}

func newReplayTestCluster(t *testing.T) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(3, replayTestTopic))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)
	return cluster, producer
}

// TestReplayFromOffsets_ExactPartitionCursor proves the repaired semantics: the
// cursor's own partition is consumed from the exact offset, other partitions of
// the topic are untouched, and unsubscribed channels are filtered out.
//
// The partition-0 record is load-bearing: partition 0 holds ONE record (log end
// offset 1), so the old all-partitions-at-cursor-offset fan-out (offset 1 on
// every partition) would have consumed from partition 0's log end — and any
// cursor beyond it would OOR the whole replay. Only exact-partition consumption
// returns partition-1 records with no interference.
func TestReplayFromOffsets_ExactPartitionCursor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster, producer := newReplayTestCluster(t)

	// Partition 1: the replayed log — offsets 0,1,2 for the subscribed channel,
	// offset 3 for a channel the client is NOT subscribed to.
	produceAt(ctx, t, producer, 1, replayTestChannel, []byte(`{"n":0}`))
	produceAt(ctx, t, producer, 1, replayTestChannel, []byte(`{"n":1}`))
	produceAt(ctx, t, producer, 1, replayTestChannel, []byte(`{"n":2}`))
	produceAt(ctx, t, producer, 1, "acme.SOL.trade", []byte(`{"other":true}`))
	// Partition 0: a single unrelated record (short log — see doc comment).
	produceAt(ctx, t, producer, 0, "acme.ETH.trade", []byte(`{"eth":true}`))

	consumer := newReplayTestConsumer(t, cluster)

	start := time.Now()
	msgs, err := consumer.ReplayFromOffsets(ctx,
		map[string]map[int32]int64{replayTestTopic: {1: 1}}, 100, []string{replayTestChannel})
	if err != nil {
		t.Fatalf("ReplayFromOffsets: %v", err)
	}
	// Caught-up termination must be deterministic (end-offsets), not ctx-expiry:
	// without it PollFetches blocks until the context dies and every replay
	// burns its full timeout budget (production: the live-replay handler's send
	// loop then races its already-expired context and truncates randomly). The
	// bound is generous — a terminating replay completes in milliseconds; the
	// broken one takes the full 30s ctx.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("ReplayFromOffsets took %v — caught-up termination is broken (ran to ctx expiry)", elapsed)
	}

	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (offsets 1,2 on partition 1, subscribed channel only): %+v", len(msgs), msgs)
	}
	for i, msg := range msgs {
		wantOffset := int64(i + 1)
		if msg.Partition != 1 || msg.Offset != wantOffset {
			t.Errorf("msgs[%d] at partition %d offset %d, want partition 1 offset %d", i, msg.Partition, msg.Offset, wantOffset)
		}
		if msg.Subject != replayTestChannel {
			t.Errorf("msgs[%d] subject = %q, want %q (subscription filter)", i, msg.Subject, replayTestChannel)
		}
		if wantPos := history.EncodePos(1, wantOffset); msg.Pos != wantPos {
			t.Errorf("msgs[%d] pos = %q, want %q", i, msg.Pos, wantPos)
		}
	}
}

// TestReplayFromOffsets_EmptyGapFastPath pins the caught-up contract: a cursor
// exactly at the log end is a VALID empty gap (the client saw everything) and
// must return immediately with no messages and no error — not OOR, and not a
// poll that waits out its context.
func TestReplayFromOffsets_EmptyGapFastPath(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster, producer := newReplayTestCluster(t)
	produceAt(ctx, t, producer, 0, replayTestChannel, []byte(`{"only":true}`)) // log end offset = 1

	consumer := newReplayTestConsumer(t, cluster)

	start := time.Now()
	msgs, err := consumer.ReplayFromOffsets(ctx,
		map[string]map[int32]int64{replayTestTopic: {0: 1}}, 100, []string{replayTestChannel})
	if err != nil {
		t.Fatalf("ReplayFromOffsets: %v (cursor at log end is a valid empty gap, not an error)", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("got %d messages, want 0 (nothing after the cursor)", len(msgs))
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("empty gap took %v — fast path missing (polled to ctx expiry)", elapsed)
	}
}

// TestReplayFromOffsets_OutOfRangeSurfaces pins the NoResetOffset contract: an
// exact offset beyond the partition's log end must surface OffsetOutOfRange to
// the caller. With kgo's default reset (AtStart) the client would silently
// restart from the beginning of the partition — replaying records the client
// never asked for and misreporting the gap.
func TestReplayFromOffsets_OutOfRangeSurfaces(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster, producer := newReplayTestCluster(t)
	produceAt(ctx, t, producer, 0, replayTestChannel, []byte(`{"only":true}`)) // log end offset = 1

	consumer := newReplayTestConsumer(t, cluster)

	_, err := consumer.ReplayFromOffsets(ctx,
		map[string]map[int32]int64{replayTestTopic: {0: 99}}, 100, []string{replayTestChannel})
	if !errors.Is(err, kerr.OffsetOutOfRange) {
		t.Fatalf("err = %v, want kerr.OffsetOutOfRange (stale cursor must fail loudly, not replay from start)", err)
	}
}
