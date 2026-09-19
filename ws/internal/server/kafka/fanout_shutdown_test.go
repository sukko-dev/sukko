package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFanoutPool_ShutdownDrainsQueuedEgress pins the graceful-shutdown drain
// (ADR-0018): egress copies already submitted MUST be written before Shutdown
// returns — a copy belongs to a publish that was already ACKED, so abandoning
// the queue on shutdown would be silent loss with no trace. The hard cancel
// immediately after Shutdown is the discriminator: a no-op drain leaves the
// backlog to die with the workers.
func TestFanoutPool_ShutdownDrainsQueuedEgress(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, fullTopic("audit")))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(client.Close)

	pool := newTestFanoutPool(1, 16, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	const n = 5
	for i := range n {
		pool.Submit(fanoutJob{
			topic:  fullTopic("audit"),
			tenant: fanoutTestTenant,
			record: &kgo.Record{Value: []byte{byte(i)}},
		})
	}

	undrained := pool.Shutdown(10 * time.Second)
	cancel() // a no-op drain would strand the backlog right here

	if undrained != 0 {
		t.Fatalf("Shutdown() undrained = %d, want 0 (healthy broker, everything must drain)", undrained)
	}
	recs := consumeTopic(t, cluster.ListenAddrs(), fullTopic("audit"), n, 5*time.Second)
	if len(recs) != n {
		t.Fatalf("egress topic has %d records after Shutdown, want %d — queued copies were abandoned", len(recs), n)
	}
}

// TestFanoutPool_Shutdown_BoundedTimeout_CountsUndrained pins the §VII bound:
// against a broker that never answers, Shutdown must return within its timeout
// (never hang the pod's shutdown), and every egress copy it could not drain
// must be counted in ws_routing_fanout_dropped_total — terminal loss, observable.
func TestFanoutPool_Shutdown_BoundedTimeout_CountsUndrained(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, fullTopic("audit")))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	release := make(chan struct{})
	cluster.ControlKey(kmsg.Produce.Int16(), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		cluster.SleepControl(func() { <-release }) // block every produce
		return nil, nil, false
	})
	defer close(release)

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(client.Close)

	pm := newPoolMetrics(prometheus.NewRegistry())
	pool := newTestFanoutPool(1, 16, client)
	pool.droppedCounter = pm.fanoutDropped
	// t.Context() is canceled before cleanups run, so the worker blocked in the
	// produce exits once close(release) (deferred) unblocks the broker.
	pool.Start(t.Context())

	// 3 jobs: the single worker blocks on the first produce; 2 stay queued.
	for i := range 3 {
		pool.Submit(fanoutJob{
			topic:  fullTopic("audit"),
			tenant: fanoutTestTenant,
			record: &kgo.Record{Value: []byte{byte(i)}},
		})
	}
	time.Sleep(200 * time.Millisecond) // let the worker pick up job 1 and block

	start := time.Now()
	undrained := pool.Shutdown(500 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("Shutdown took %v — the drain must be bounded by its timeout (§VII)", elapsed)
	}
	if undrained != 2 {
		t.Errorf("Shutdown() undrained = %d, want 2 (one in-flight, two abandoned in the queue)", undrained)
	}
	if got := testutil.ToFloat64(pm.fanoutDropped.WithLabelValues(fanoutTestTenant, FanoutDropReasonShutdown)); got != 2 {
		t.Errorf("ws_routing_fanout_dropped_total{tenant,reason=shutdown_undrained} = %v, want 2 — undrained copies must be counted", got)
	}
}

// TestFanoutPool_SubmitAfterShutdown_CountedNotPanicking: a Submit racing past
// shutdown must never panic on the closed handoff (§VII send-on-closed) and the
// lost copy must be counted.
func TestFanoutPool_SubmitAfterShutdown_CountedNotPanicking(t *testing.T) {
	t.Parallel()
	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	pm := newPoolMetrics(prometheus.NewRegistry())
	pool := newTestFanoutPool(0, 4, client)
	pool.droppedCounter = pm.fanoutDropped

	_ = pool.Shutdown(100 * time.Millisecond)

	pool.Submit(fanoutJob{
		topic:  fullTopic("audit"),
		tenant: fanoutTestTenant,
		record: &kgo.Record{Value: []byte("late")},
	})

	if got := testutil.ToFloat64(pm.fanoutDropped.WithLabelValues(fanoutTestTenant, FanoutDropReasonPostShutdown)); got != 1 {
		t.Errorf("ws_routing_fanout_dropped_total{tenant,reason=post_shutdown} = %v, want 1 (post-shutdown Submit is a counted loss)", got)
	}
}
