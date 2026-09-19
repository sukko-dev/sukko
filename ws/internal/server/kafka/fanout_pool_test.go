package kafka

import (
	"context"
	"errors"
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

// TestFanout_DeliversToAllTopics: a matched rule with egress topics writes one record to the
// ingress topic (synchronously — its coordinates name the mid) and one egress copy per egress
// topic; each target gets exactly one record and the breaker stays Closed (P1b-C3).
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
	if fanoutMid == "" {
		t.Error("Publish with egress topics returned empty mid — the ack must always carry the ingress record's identity (ADR-0018)")
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

// TestFanout_EgressFailure_AcksAndDeadLetters pins the ADR-0018 partial-failure
// semantics: the INGRESS write alone determines the ack, so a failed egress write
// still returns success + mid, and the missed egress copy is dead-lettered with
// the fan-out-failure reason + failed-topic header (surfaced, never silent). The
// breaker stays Closed — an egress failure is not broker unavailability (P1b-C1).
func TestFanout_EgressFailure_AcksAndDeadLetters(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1,
		fullTopic("trades"), fullTopic("audit"), fullTopic(routing.DeadLetterTopicSuffix)))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	failProduceForTopics(t, cluster, fullTopic("audit")) // egress fails; ingress + DLQ succeed

	// Single fan-out worker → produces are sequential (separate ProduceRequests),
	// so the control hook can fail "audit" in isolation.
	p := newKfakeProducer(t, cluster, 1)
	mid, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("Publish: egress failure must not fail the ack (ingress succeeded), got %v", err)
	}
	if mid == "" {
		t.Error("Publish mid empty — the ingress record's identity must be returned even when egress fails")
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
	// TopicAuthorizationFailed is non-retriable — the client fails fast rather
	// than exhausting retries, and the triage header must say which (ADR-0018).
	if got := headerValue(dlq[0], HeaderFailureKind); got != FailureKindNonRetryable {
		t.Errorf("DLQ failure-kind header = %q, want %q", got, FailureKindNonRetryable)
	}
	if headerValue(dlq[0], HeaderFailureCause) == "" {
		t.Error("DLQ failure-cause header empty — want the terminal produce error")
	}
	if got := p.CircuitBreakerState(); got != gobreaker.StateClosed {
		t.Errorf("breaker = %v, want Closed (egress failure must not trip)", got)
	}
}

// TestPublish_IngressFailure_Errors pins the other half of ADR-0018: when the
// INGRESS write fails, Publish returns an error (→ 500, §III — never a false 2xx)
// even though the egress topic is healthy, and no egress copy is written (egress
// is submitted only after ingress success, so an egress topic never holds a copy
// of an un-acked message).
func TestPublish_IngressFailure_Errors(t *testing.T) {
	t.Parallel()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1,
		fullTopic("trades"), fullTopic("audit"), fullTopic(routing.DeadLetterTopicSuffix)))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	failProduceForTopics(t, cluster, fullTopic("trades")) // ingress fails; egress would succeed

	p := newKfakeProducer(t, cluster, 1)
	mid, err := p.Publish(context.Background(), 1, fanoutTestChannel, []byte(`{"x":1}`))
	if err == nil {
		t.Fatal("Publish: ingress write failure must error, got nil")
	}
	if mid != "" {
		t.Errorf("mid = %q, want empty on ingress failure", mid)
	}
	// The healthy egress topic must NOT have received a copy of the failed publish.
	if recs := consumeTopic(t, cluster.ListenAddrs(), fullTopic("audit"), 1, time.Second); len(recs) != 0 {
		t.Errorf("audit: got %d records, want 0 — egress must only be written after ingress success", len(recs))
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

// TestFanout_DLQQueueFull_IncrementsDropped: when a failed egress write cannot be enqueued
// to the DLQ pool (queue full), the pool counts it in ws_routing_dlq_dropped_total (P1b-C4,
// §VII — the terminal-loss leg must be observable). Uses an undrained (unbuffered, no-worker)
// DLQ queue so TrySubmit always returns false.
func TestFanout_DLQQueueFull_IncrementsDropped(t *testing.T) {
	t.Parallel()
	client := newFailingKgoClient(t) // every egress write fails → routes to DLQ
	t.Cleanup(client.Close)

	pm := newPoolMetrics(prometheus.NewRegistry())
	fanout := newTestFanoutPool(0, 8, client)
	fanout.dlq = &DLQPool{jobs: make(chan dlqJob), logger: zerolog.Nop()} // unbuffered, undrained → TrySubmit false
	fanout.dlqDropped = pm.dlqDropped

	// Process synchronously (no workers): the produce fails, dead-letter is
	// attempted, the full DLQ queue rejects it — the drop must be counted.
	fanout.process(context.Background(), fanoutJob{
		topic:  fullTopic("audit"),
		tenant: fanoutTestTenant,
		record: &kgo.Record{Value: []byte(`{"x":1}`)},
	})

	if got := testutil.ToFloat64(pm.dlqDropped.WithLabelValues(fanoutTestTenant)); got != 1 {
		t.Errorf("ws_routing_dlq_dropped_total{tenant} = %v, want 1", got)
	}
}

// TestFanout_CloseDuringInFlight_Unblocks: Close() during an in-flight Publish must return
// promptly (cancel → wg.Wait (bounded) → client.Close, P1b-C3) — closing the client aborts the
// blocked ingress ProduceSync so the in-flight Publish unblocks, and the egress workers exit on
// ctx cancel. A hang here would mean a goroutine leak.
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
		// Any error verdict is acceptable (client-closed abort of the ingress
		// ProduceSync is typical). The point is: it RETURNED.
		t.Logf("in-flight Publish returned: %v", err)
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("in-flight Publish() hung after Close")
	}
	close(release)
}

// TestDoProduce_IngressFailure_NoEgressSubmitted: when the ingress write fails,
// doProduce errors (→ 500, §III — not a false 2xx) and does NOT submit any egress
// job — an egress topic must never hold a copy of an un-acked message (ADR-0018).
// Uses a fast-failing client (all produces fail in 100ms) so no broker is needed.
func TestDoProduce_IngressFailure_NoEgressSubmitted(t *testing.T) {
	t.Parallel()
	client := newFailingKgoClient(t)
	t.Cleanup(client.Close)

	fanout := newTestFanoutPool(0, 8, client) // no workers: submitted jobs would park in handoff

	p := &Producer{topicNamespace: fanoutTestNamespace, ctx: context.Background(), client: client, fanout: fanout}
	_, err := p.doProduce(context.Background(), 1, fanoutTestChannel, fanoutTestTenant,
		producePlan{ingressTopic: fullTopic("trades"), egressTopics: []string{fullTopic("audit")}}, []byte(`{"x":1}`))

	if err == nil {
		t.Fatal("doProduce: ingress failure must error, got nil")
	}
	if len(fanout.handoff) != 0 {
		t.Errorf("egress jobs submitted after ingress failure: %d, want 0", len(fanout.handoff))
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
