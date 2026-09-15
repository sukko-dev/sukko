package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// =============================================================================
// Fan-out pool tests (#179 P1b). The delivery + singleton paths use a kfake
// in-process broker; the failure + breaker-classification paths use the
// fast-failing client / pure-function patterns (deterministic, no retry loops).
// All run in the DEFAULT -race gate (no //go:build integration tag) so the
// fan-out happy path is covered in CI (P1b-C7).
// =============================================================================

const (
	fanoutTestNamespace = "test"
	fanoutTestTenant    = "acme"
	fanoutTestChannel   = "acme.BTC.trade"
)

func fullTopic(suffix string) string {
	return fanoutTestNamespace + "." + fanoutTestTenant + "." + suffix
}

// newKfakeProducer builds a Producer wired to a kfake cluster with a fan-out pool and an
// isolated Prometheus registry (so parallel tests / repeated constructions never collide).
func newKfakeProducer(t *testing.T, cluster *kfake.Cluster, workers int) *Producer {
	t.Helper()
	logger := zerolog.Nop()
	p, err := NewProducer(ProducerConfig{
		Brokers:         cluster.ListenAddrs(),
		TopicNamespace:  fanoutTestNamespace,
		Logger:          &logger,
		RulesProvider:   syncedProvider(rule("acme.**", "trades", "audit")),
		FanoutWorkers:   workers,
		FanoutQueueSize: 16,
		DLQMaxRetries:   1,
		DLQBaseDelay:    time.Millisecond,
		DLQMaxDelay:     10 * time.Millisecond,
		DLQRetryWorkers: 1,
		ShutdownTimeout: 2 * time.Second, // bound Close if a worker is stuck on a sleeping broker
		Registerer:      prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// consumeTopic reads up to want records from a topic within timeout.
func consumeTopic(t *testing.T, brokers []string, topic string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var got []*kgo.Record
	for len(got) < want {
		fetches := cl.PollRecords(ctx, want-len(got))
		if ctx.Err() != nil || len(fetches.Errors()) > 0 {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	return got
}

// TestFanout_DeliversToAllTopics: a matched multi-topic rule fans a single publish out to
// every rule topic; each target gets exactly one record and the breaker stays Closed (P1b-C3).
func TestFanout_DeliversToAllTopics(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1,
		fullTopic("trades"), fullTopic("audit"), fullTopic(routing.DeadLetterTopicSuffix)))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	p := newKfakeProducer(t, cluster, 2)
	fanoutMid, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if fanoutMid != "" {
		t.Errorf("fan-out Publish mid = %q, want empty (N records → N mids; the ack carries none)", fanoutMid)
	}

	for _, suffix := range []string{"trades", "audit"} {
		if recs := consumeTopic(t, cluster.ListenAddrs(), fullTopic(suffix), 1, 3*time.Second); len(recs) != 1 {
			t.Errorf("topic %s: got %d records, want 1", fullTopic(suffix), len(recs))
		}
	}
	if got := p.CircuitBreakerState(); got != gobreaker.StateClosed {
		t.Errorf("breaker = %v, want Closed", got)
	}
}

// TestNewProducer_TwiceWithFanout_NoPanic proves the metric singleton (P1b-C2): constructing
// two producers WITH fan-out pools against the DEFAULT registry does not panic on duplicate
// registration. (FanoutWorkers > 0 required, else the test is vacuous — no pool.)
func TestNewProducer_TwiceWithFanout_NoPanic(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, fullTopic("trades")))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	mk := func() *Producer {
		logger := zerolog.Nop()
		p, err := NewProducer(ProducerConfig{
			Brokers:         cluster.ListenAddrs(),
			TopicNamespace:  fanoutTestNamespace,
			Logger:          &logger,
			RulesProvider:   syncedProvider(rule("acme.**", "trades")),
			FanoutWorkers:   2,
			FanoutQueueSize: 16,
			DLQMaxRetries:   1,
			DLQBaseDelay:    time.Millisecond,
			DLQMaxDelay:     10 * time.Millisecond,
			DLQRetryWorkers: 1,
			// NO Registerer → both use the package singleton against the default registry.
		})
		if err != nil {
			t.Fatalf("NewProducer: %v", err)
		}
		return p
	}
	p1 := mk()
	t.Cleanup(func() { _ = p1.Close() })
	p2 := mk() // must not panic on duplicate promauto registration
	t.Cleanup(func() { _ = p2.Close() })
}

// failProduceForTopics installs a kfake control hook that fails every produce to a named topic
// with a non-retriable TopicAuthorizationFailed. Produce v13+ identifies topics by UUID (not
// name), so we resolve names→IDs up front and match/echo by TopicID (matching by name fails —
// the name field is empty on the wire). Produces to other topics (incl. the DLQ) pass through.
func failProduceForTopics(t *testing.T, cluster *kfake.Cluster, failNames ...string) {
	t.Helper()
	admCl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	td, err := kadm.NewClient(admCl).ListTopics(context.Background(), failNames...)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	failIDs := make(map[[16]byte]bool, len(failNames))
	for _, name := range failNames {
		failIDs[[16]byte(td[name].ID)] = true
	}

	cluster.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl() // persist across every produce request
		pr := req.(*kmsg.ProduceRequest)
		hit := false
		for _, tp := range pr.Topics {
			if failIDs[tp.TopicID] {
				hit = true
			}
		}
		if !hit {
			return nil, nil, false // let kfake serve it (success)
		}
		resp := pr.ResponseKind().(*kmsg.ProduceResponse)
		for _, tp := range pr.Topics {
			rt := kmsg.ProduceResponseTopic{Topic: tp.Topic, TopicID: tp.TopicID}
			for _, part := range tp.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition: part.Partition,
					ErrorCode: kerr.TopicAuthorizationFailed.Code,
				})
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return resp, nil, true
	})
}

// headerValue returns the value of the named record header, or "" if absent.
func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// TestFanout_PartialFailure_DeadLetters: when one fan-out topic write fails but another
// succeeds, Publish still returns nil (2xx, P1b-C9) and a record lands on the tenant DLQ topic
// carrying the fan-out-failure reason + failed/succeeded headers (P1b-C3/C4). The breaker stays
// Closed — a partial failure is not broker unavailability (P1b-C1).
func TestFanout_PartialFailure_DeadLetters(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1,
		fullTopic("trades"), fullTopic("audit"), fullTopic(routing.DeadLetterTopicSuffix)))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	failProduceForTopics(t, cluster, fullTopic("audit")) // audit fails, trades + DLQ succeed

	// Single fan-out worker → the two topic produces are sequential (separate ProduceRequests),
	// so the control hook can fail "audit" in isolation. With >1 worker franz-go may batch both
	// topics into one request, which the hook would fail wholesale.
	p := newKfakeProducer(t, cluster, 1)
	if _, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Publish: partial failure must still return nil (>=1 topic ok), got %v", err)
	}

	if recs := consumeTopic(t, cluster.ListenAddrs(), fullTopic("trades"), 1, 3*time.Second); len(recs) != 1 {
		t.Errorf("trades: got %d records, want 1", len(recs))
	}
	dlq := consumeTopic(t, cluster.ListenAddrs(), fullTopic(routing.DeadLetterTopicSuffix), 1, 3*time.Second)
	if len(dlq) != 1 {
		t.Fatalf("DLQ: got %d records, want 1", len(dlq))
	}
	if r := headerValue(dlq[0], HeaderReason); r != ReasonFanoutTopicWriteFailed {
		t.Errorf("DLQ reason header = %q, want %q", r, ReasonFanoutTopicWriteFailed)
	}
	if headerValue(dlq[0], HeaderFailedTopics) != fullTopic("audit") {
		t.Errorf("DLQ failed_topics = %q, want %q", headerValue(dlq[0], HeaderFailedTopics), fullTopic("audit"))
	}
	if headerValue(dlq[0], HeaderSucceededTopics) != fullTopic("trades") {
		t.Errorf("DLQ succeeded_topics = %q, want %q", headerValue(dlq[0], HeaderSucceededTopics), fullTopic("trades"))
	}
	if got := p.CircuitBreakerState(); got != gobreaker.StateClosed {
		t.Errorf("breaker = %v, want Closed (partial failure must not trip)", got)
	}
}

// TestIsBreakerNeutralErr pins the breaker classification (P1b-C1): which produce errors are
// treated as per-request (neutral) vs genuine broker unavailability (breaker-eligible).
func TestIsBreakerNeutralErr(t *testing.T) {
	t.Parallel()
	neutral := []struct {
		name string
		err  error
	}{
		{"producer closed", ErrProducerClosed},
		{"all fan-out failed", errFanoutAllFailed},
		{"client canceled (disconnect)", context.Canceled},
		{"unknown topic (misconfig)", kerr.UnknownTopicOrPartition},
		{"topic auth failed (misconfig)", kerr.TopicAuthorizationFailed},
		{"wrapped unknown topic", errors.Join(errors.New("produce"), kerr.UnknownTopicOrPartition)},
	}
	for _, tc := range neutral {
		if !isBreakerNeutralErr(tc.err) {
			t.Errorf("%s: isBreakerNeutralErr = false, want true (breaker-neutral)", tc.name)
		}
	}

	eligible := []struct {
		name string
		err  error
	}{
		{"deadline exceeded (slow broker)", context.DeadlineExceeded},
		{"generic produce failure", backend.ErrPublishFailed},
		{"broker not available", kerr.BrokerNotAvailable},
	}
	for _, tc := range eligible {
		if isBreakerNeutralErr(tc.err) {
			t.Errorf("%s: isBreakerNeutralErr = true, want false (breaker-eligible)", tc.name)
		}
	}
}

// TestDoProduce_DLQQueueFull_IncrementsDropped: when a failed fan-out topic cannot be enqueued
// to the DLQ pool (queue full), the producer counts it in ws_routing_dlq_dropped_total (P1b-C4,
// §VII — the terminal-loss leg must be observable). Uses an undrained (unbuffered, no-worker)
// DLQ queue so TrySubmit always returns false.
func TestDoProduce_DLQQueueFull_IncrementsDropped(t *testing.T) {
	t.Parallel()
	client := newFailingKgoClient(t) // all fan-out writes fail → all route to DLQ
	t.Cleanup(client.Close)

	pm := newPoolMetrics(prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	fanout := newTestFanoutPool(2, 8, client)
	fanout.dlq = &DLQPool{jobs: make(chan dlqJob), logger: zerolog.Nop()} // unbuffered, undrained → TrySubmit false
	fanout.Start(ctx, &wg)

	p := &Producer{topicNamespace: fanoutTestNamespace, ctx: ctx, fanout: fanout, dlqDropped: pm.dlqDropped}
	_, _ = p.doProduce(context.Background(), 1, fanoutTestChannel, fanoutTestTenant,
		[]string{fullTopic("trades"), fullTopic("audit")}, []byte(`{"x":1}`))

	cancel()
	wg.Wait()

	if got := testutil.ToFloat64(pm.dlqDropped.WithLabelValues(fanoutTestTenant)); got != 1 {
		t.Errorf("ws_routing_dlq_dropped_total{tenant} = %v, want 1", got)
	}
}

// TestFanout_CloseDuringInFlight_Unblocks: Close() during an in-flight multi-topic Publish must
// return promptly (cancel → wg.Wait → client.Close, P1b-C3) — the fan-out workers abort their
// blocked ProduceSync on ctx cancel and the in-flight Publish unblocks via the p.ctx collect-loop
// arm. A hang here would mean a goroutine leak.
//
//nolint:paralleltest // timing-sensitive: blocks a produce via SleepControl then races Close
func TestFanout_CloseDuringInFlight_Unblocks(t *testing.T) {
	// Not parallel: timing-sensitive (blocks a produce, then races Close against it).
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1,
		fullTopic("trades"), fullTopic("audit"), fullTopic(routing.DeadLetterTopicSuffix)))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	release := make(chan struct{})
	cluster.ControlKey(kmsg.Produce.Int16(), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		cluster.SleepControl(func() { <-release }) // block every produce until released
		return nil, nil, false
	})

	p := newKfakeProducer(t, cluster, 2)

	pubDone := make(chan error, 1)
	go func() {
		_, pubErr := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`))
		pubDone <- pubErr
	}()
	time.Sleep(300 * time.Millisecond) // let the produce reach the sleeping broker

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()

	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Close() hung — wg.Wait did not complete (goroutine leak)")
	}
	select {
	case err := <-pubDone:
		// ErrProducerClosed (p.ctx arm) is the expected verdict; a fan-out aggregate error is
		// also acceptable if the workers' ProduceSync aborted first. The point is: it RETURNED.
		t.Logf("in-flight Publish returned: %v", err)
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("in-flight Publish() hung after Close")
	}
	close(release)
}

// TestDoProduce_ZeroSuccessFanout: when every fan-out topic write fails, doProduce returns an
// error wrapping both backend.ErrPublishFailed (→ 500, §III — not a false 2xx) and
// errFanoutAllFailed (breaker-neutral, P1b-C1). Uses a fast-failing client (all produces fail
// in 100ms) so no retry loop / broker needed.
func TestDoProduce_ZeroSuccessFanout(t *testing.T) {
	t.Parallel()
	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	fanout := newTestFanoutPool(2, 8, client) // dlq nil → DLQ submission skipped
	fanout.Start(ctx, &wg)

	p := &Producer{topicNamespace: fanoutTestNamespace, ctx: ctx, fanout: fanout}
	_, err := p.doProduce(context.Background(), 1, fanoutTestChannel, fanoutTestTenant,
		[]string{fullTopic("trades"), fullTopic("audit")}, []byte(`{"x":1}`))

	cancel()
	wg.Wait()

	if !errors.Is(err, errFanoutAllFailed) {
		t.Errorf("err = %v, want errors.Is(errFanoutAllFailed)", err)
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("err = %v, want errors.Is(ErrPublishFailed) (→ gateway 500)", err)
	}
}

// TestPublish_SingleTopicMid: a single-topic publish returns the stable
// message identity derived from the coordinates franz-go fills on the produce
// ack — and it equals the derivation applied to the record as later consumed
// (the ack↔delivery identity the tester's "rest publish mid equality" check
// asserts end-to-end).
func TestPublish_SingleTopicMid(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, fullTopic("trades")))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	logger := zerolog.Nop()
	p, err := NewProducer(ProducerConfig{
		Brokers:         cluster.ListenAddrs(),
		TopicNamespace:  fanoutTestNamespace,
		Logger:          &logger,
		RulesProvider:   syncedProvider(rule("acme.**", "trades")),
		FanoutWorkers:   1,
		FanoutQueueSize: 16,
		DLQMaxRetries:   1,
		DLQBaseDelay:    time.Millisecond,
		DLQMaxDelay:     10 * time.Millisecond,
		DLQRetryWorkers: 1,
		ShutdownTimeout: 2 * time.Second,
		Registerer:      prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	mid, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if mid == "" {
		t.Fatal("single-topic Publish returned empty mid")
	}

	recs := consumeTopic(t, cluster.ListenAddrs(), fullTopic("trades"), 1, 3*time.Second)
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	want := kafkashared.MessageID(recs[0].Topic, recs[0].Partition, recs[0].Offset)
	if mid != want {
		t.Errorf("ack mid = %q, consumed-record derivation = %q — must match (ADR-0008)", mid, want)
	}

	// Second publish must yield a DIFFERENT mid (offset advanced) — catches a
	// regression that derives the mid before ProduceSync fills the record's
	// coordinates, which the first record's degenerate (0,0) would mask.
	mid2, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":2}`))
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	if mid2 == mid {
		t.Errorf("second publish returned the same mid %q — coordinates not read from the acked record", mid2)
	}
}
