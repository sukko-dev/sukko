package kafka

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// =============================================================================
// Test Mocks
// =============================================================================

// mockCommitter tracks MarkCommitRecords calls per-record and controls
// CommitMarkedOffsets return value and latency.
type mockCommitter struct {
	mu sync.Mutex

	// Controls CommitMarkedOffsets behavior
	commitErr   error         // error to return from CommitMarkedOffsets
	commitDelay time.Duration // latency to inject into CommitMarkedOffsets

	// Observation tracking
	markedRecords []*kgo.Record // records passed to MarkCommitRecords, in order
	commitCalls   int           // number of CommitMarkedOffsets calls
	callOrder     []string      // interleaved "mark" and "commit" entries for ordering assertions
}

func (m *mockCommitter) CommitMarkedOffsets(ctx context.Context) error {
	if m.commitDelay > 0 {
		// Respect context cancellation during the simulated delay
		select {
		case <-time.After(m.commitDelay):
		case <-ctx.Done():
			m.mu.Lock()
			m.commitCalls++
			m.callOrder = append(m.callOrder, "commit")
			m.mu.Unlock()
			return ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitCalls++
	m.callOrder = append(m.callOrder, "commit")
	return m.commitErr
}

func (m *mockCommitter) MarkCommitRecords(rs ...*kgo.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markedRecords = append(m.markedRecords, rs...)
	for range rs {
		m.callOrder = append(m.callOrder, "mark")
	}
}

func (m *mockCommitter) markCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.markedRecords)
}

func (m *mockCommitter) commitCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitCalls
}

// mockHistogram implements prometheus.Observer for test assertions.
// The consumer field revokeCommitDuration is typed as prometheus.Observer
// to allow simple mocking — production code assigns a real prometheus.Histogram
// (which satisfies prometheus.Observer).
type mockHistogram struct {
	mu           sync.Mutex
	observations []float64
}

func (h *mockHistogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observations = append(h.observations, v)
}

func (h *mockHistogram) observationCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.observations)
}

// mockResourceGuardFixed is a resource guard where allowKafka is set per-call.
type mockResourceGuardFixed struct {
	allowKafka  bool
	shouldPause bool
}

func (m *mockResourceGuardFixed) AllowKafkaMessage(_ context.Context) (bool, time.Duration) {
	return m.allowKafka, 0
}

func (m *mockResourceGuardFixed) ShouldPauseKafka() bool {
	return m.shouldPause
}

// =============================================================================
// Test Helpers
// =============================================================================

// newRebalanceTestConsumer constructs a Consumer via NewConsumer with a test-safe config.
// It overwrites consumer.committer and consumer.revokeCommitDuration with mocks
// before returning, so all assertions go through the injected mocks.
//
// Note: kgo.NewClient() connects lazily — a fake broker address is sufficient
// since no Poll() is called in unit tests.
func newRebalanceTestConsumer(t *testing.T, cfg ConsumerConfig) (*Consumer, *mockCommitter, *mockHistogram) {
	t.Helper()
	logger := zerolog.Nop()

	// Apply defaults for required fields not set by caller
	if cfg.Logger == nil {
		cfg.Logger = &logger
	}
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9999"} // fake — connects lazily
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "test-rebalance-group"
	}
	if len(cfg.Topics) == 0 {
		cfg.Topics = []string{"sukko.test.trade"}
	}
	if cfg.Broadcast == nil {
		cfg.Broadcast = func(_ string, _ []byte, _ string, _ int32, _ int64) {}
	}
	if cfg.ResourceGuard == nil {
		cfg.ResourceGuard = &mockResourceGuardFixed{allowKafka: true}
	}
	if cfg.TenantResolver == nil {
		cfg.TenantResolver = parseResolver // tenant = 2nd topic segment (#179 P3)
	}
	if cfg.ConsumerType == "" {
		cfg.ConsumerType = ConsumerTypeKindShared
	}
	if cfg.CommitOnRevokeTimeout == 0 {
		cfg.CommitOnRevokeTimeout = 5 * time.Second
	}
	// Use an isolated registry to avoid singleton pollution across test cases
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}

	consumer, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("newTestConsumer: NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })

	// Overwrite committer with mock so assertions work without a live broker
	mock := &mockCommitter{}
	consumer.committer = mock

	// Overwrite histogram with mock for observation-count assertions
	hist := &mockHistogram{}
	consumer.revokeCommitDuration = hist

	return consumer, mock, hist
}

// testRevoked is a sample partition map used across multiple tests.
var testRevoked = map[string][]int32{
	"sukko.test.trade": {0, 1},
}

// makeRecord builds a minimal kgo.Record for testing. The channel key carries the topic's tenant
// prefix so it passes the §IX tenant-prefix check in extractChannel.
func makeRecord(topic string) *kgo.Record {
	tenant := "test"
	if parts := strings.SplitN(topic, ".", 3); len(parts) >= 3 {
		tenant = parts[1]
	}
	channel := tenant + ".BTC.trade"
	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(channel),
		Value:   []byte(`{"price":"50000"}`),
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte(channel)}},
	}
}

// =============================================================================
// handleRevoke Unit Tests
// =============================================================================

func TestOnPartitionsRevoked_CommitSuccess(t *testing.T) {
	t.Parallel()
	consumer, mock, hist := newRebalanceTestConsumer(t, ConsumerConfig{})

	consumer.handleRevoke(context.Background(), testRevoked)

	if mock.commitCount() != 1 {
		t.Errorf("CommitMarkedOffsets called %d times, want 1", mock.commitCount())
	}
	if hist.observationCount() != 1 {
		t.Errorf("histogram observed %d times, want 1", hist.observationCount())
	}
}

func TestOnPartitionsRevoked_CommitFailure(t *testing.T) {
	t.Parallel()
	consumer, mock, hist := newRebalanceTestConsumer(t, ConsumerConfig{})
	mock.commitErr = errors.New("broker unavailable")

	// Capture logs to verify error message using a bytes.Buffer
	var buf strings.Builder
	logger := zerolog.New(&buf)
	consumer.logger = &logger

	// Set up a real counter for the failure label
	reg := prometheus.NewRegistry()
	revokeCounterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricRevokeCommitTotal,
		Help: "test",
	}, []string{LabelResult})
	_ = reg.Register(revokeCounterVec)
	consumer.revokeCommitCounter = revokeCounterVec

	consumer.handleRevoke(context.Background(), testRevoked)

	if mock.commitCount() != 1 {
		t.Errorf("CommitMarkedOffsets called %d times, want 1", mock.commitCount())
	}
	if hist.observationCount() != 1 {
		t.Errorf("histogram must observe on failure path; got %d observations", hist.observationCount())
	}
	if !strings.Contains(buf.String(), MsgCommitOnRevokeFailed) {
		t.Errorf("expected log %q not found in: %s", MsgCommitOnRevokeFailed, buf.String())
	}
}

func TestOnPartitionsRevoked_CommitTimeout(t *testing.T) {
	t.Parallel()
	timeout := 100 * time.Millisecond
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		CommitOnRevokeTimeout: timeout,
	})
	// Make commit take longer than the timeout
	mock.commitDelay = timeout * 3

	start := time.Now()
	consumer.handleRevoke(context.Background(), testRevoked)
	elapsed := time.Since(start)

	// Elapsed should be ≥ timeout (context expired) and well under commitDelay
	if elapsed < timeout {
		t.Errorf("elapsed %v < timeout %v; commit returned too early", elapsed, timeout)
	}
	maxExpected := timeout + 200*time.Millisecond
	if elapsed > maxExpected {
		t.Errorf("elapsed %v > %v; commit did not respect timeout", elapsed, maxExpected)
	}
}

func TestOnPartitionsRevoked_MultiplePartitions(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	revoked := map[string][]int32{
		"sukko.test.trade":    {0, 1, 2},
		"sukko.test.balances": {0},
	}
	consumer.handleRevoke(context.Background(), revoked)

	if mock.commitCount() != 1 {
		t.Errorf("expected 1 CommitMarkedOffsets call for multiple partitions, got %d", mock.commitCount())
	}
}

func TestOnPartitionsRevoked_EmptyBatch(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	// Simulate all records flushed before revoke
	consumer.handleRevoke(context.Background(), testRevoked)

	if mock.commitCount() != 1 {
		t.Errorf("CommitMarkedOffsets must be called even for empty batch, got %d calls", mock.commitCount())
	}
	if mock.commitErr != nil {
		t.Errorf("unexpected commit error: %v", mock.commitErr)
	}
}

func TestOnPartitionsRevoked_ZeroMarks(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	consumer.handleRevoke(context.Background(), nil)

	if mock.commitCount() != 1 {
		t.Errorf("CommitMarkedOffsets must be called even with zero marks, got %d calls", mock.commitCount())
	}
	if mock.markCount() != 0 {
		t.Errorf("no MarkCommitRecords calls expected before handleRevoke, got %d", mock.markCount())
	}
}

// TestShutdown_CommitOnStop verifies Research.md Decision 3:
// handleRevoke uses context.Background() as timeout base, so CommitMarkedOffsets
// succeeds even though c.ctx is canceled (simulating the shutdown sequence
// where Stop() calls c.cancel() before client.Close()).
func TestShutdown_CommitOnStop(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	// Simulate the shutdown state: Stop() calls c.cancel() before client.Close(),
	// which in turn fires OnPartitionsRevoked.
	consumer.cancel()

	// handleRevoke must still succeed because it uses context.Background() base
	consumer.handleRevoke(context.Background(), nil)

	if mock.commitCount() != 1 {
		t.Errorf("CommitMarkedOffsets must succeed with canceled c.ctx; got %d calls", mock.commitCount())
	}
	if mock.commitErr != nil {
		t.Errorf("unexpected commit error after cancel: %v", mock.commitErr)
	}
}

// TestShutdown_NoHangOnStop verifies Stop() completes within a 3-second deadline
// even against an unreachable broker. franz-go's LeaveGroup RPC uses its own
// internal retry context (not c.ctx), so without this guard the test would hang.
func TestShutdown_NoHangOnStop(t *testing.T) {
	t.Parallel()
	consumer, _, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	done := make(chan struct{})
	go func() {
		_ = consumer.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK — Stop completed before deadline
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() blocked for >3s; possible goroutine leak or stuck RPC")
	}
}

// TestShutdown_BatchFlushBeforeRevoke verifies that CommitMarkedOffsets in
// handleRevoke covers records that were previously marked via MarkCommitRecords.
// flushBatch is an inner closure in consumeLoop and not directly callable from
// tests; instead we simulate by calling MarkCommitRecords directly (as flushBatch
// would), then verify that CommitMarkedOffsets follows in call order.
func TestShutdown_BatchFlushBeforeRevoke(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	r1 := makeRecord("sukko.test.trade")
	r2 := makeRecord("sukko.test.trade")

	// Simulate what flushBatch does: mark each broadcast record
	consumer.committer.MarkCommitRecords(r1)
	consumer.committer.MarkCommitRecords(r2)

	consumer.handleRevoke(context.Background(), nil)

	// Verify ordering: both marks came before the commit
	mock.mu.Lock()
	order := slices.Clone(mock.callOrder)
	mock.mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 calls (mark, mark, commit), got %d: %v", len(order), order)
	}
	if order[0] != "mark" || order[1] != "mark" || order[2] != "commit" {
		t.Errorf("unexpected call order: %v (want [mark mark commit])", order)
	}
}

// =============================================================================
// MarkCommitRecords Placement Tests
// =============================================================================

func TestExplicitMark_BroadcastMarked_Unbatched(t *testing.T) {
	t.Parallel()
	broadcastCalled := 0
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		Broadcast:     func(_ string, _ []byte, _ string, _ int32, _ int64) { broadcastCalled++ },
		BatchSize:     1, // batching disabled (batchEnabled = batchSize > 1)
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true},
	})

	record := &kgo.Record{
		Topic:   "sukko.test.trade",
		Key:     []byte("test.BTC.trade"),
		Value:   []byte(`{"price":"50000"}`),
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte("test.BTC.trade")}},
	}
	consumer.processRecord(record)

	if mock.markCount() != 1 {
		t.Errorf("MarkCommitRecords called %d times after broadcast, want 1", mock.markCount())
	}
	if broadcastCalled != 1 {
		t.Errorf("broadcast called %d times, want 1", broadcastCalled)
	}
}

func TestExplicitMark_BroadcastMarked_Batched(t *testing.T) {
	t.Parallel()
	broadcastCalled := 0
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		Broadcast:     func(_ string, _ []byte, _ string, _ int32, _ int64) { broadcastCalled++ },
		BatchSize:     10,
		BatchTimeout:  10 * time.Millisecond,
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true},
	})

	record := &kgo.Record{
		Topic:   "sukko.test.trade",
		Key:     []byte("test.BTC.trade"),
		Value:   []byte(`{"price":"50000"}`),
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte("test.BTC.trade")}},
	}

	// Use prepareMessage to prepare the record, then simulate flushBatch
	msg, ctxCanceled := consumer.prepareMessage(record)
	if msg == nil || ctxCanceled {
		t.Fatal("prepareMessage should succeed")
	}

	// Simulate what flushBatch does
	consumer.broadcast(msg.subject, msg.message, msg.topic, msg.record.Partition, msg.record.Offset)
	consumer.committer.MarkCommitRecords(msg.record)

	if mock.markCount() != 1 {
		t.Errorf("MarkCommitRecords called %d times after batch broadcast, want 1", mock.markCount())
	}
}

func TestExplicitMark_RateLimitedMarked(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: false},
	})

	record := makeRecord("sukko.test.trade")
	consumer.processRecord(record)

	if mock.markCount() != 1 {
		t.Errorf("rate-limited record: MarkCommitRecords called %d times, want 1", mock.markCount())
	}
}

func TestExplicitMark_UnknownTopicNotMarked(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true},
	})

	// Unknown topic: the resolver has no tenant for it (fewer than 3 segments here).
	// Tri-state: an unknown topic is dropped WITHOUT committing the offset so the record
	// redelivers once the registry snapshot resolves the topic — it MUST NOT be marked.
	record := &kgo.Record{
		Topic: "malformed",
		Key:   []byte("test.BTC.trade"),
		Value: []byte(`{}`),
	}
	consumer.processRecord(record)

	if mock.markCount() != 0 {
		t.Errorf("unknown-topic record: MarkCommitRecords called %d times, want 0 (redeliver, do not commit)", mock.markCount())
	}
}

func TestExplicitMark_DLQRoutedMarked(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true},
	})

	// Record with invalid channel key (empty key → DLQ route in Community fallback)
	record := &kgo.Record{
		Topic: "sukko.test.trade",
		Key:   []byte(""), // empty key → ReasonInvalidChannelKey
		Value: []byte(`{}`),
	}
	consumer.processRecord(record)

	if mock.markCount() != 1 {
		t.Errorf("DLQ-routed record: MarkCommitRecords called %d times, want 1", mock.markCount())
	}
}

func TestExplicitMark_CPUBrakeNoMark(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true, shouldPause: true},
	})
	// Replace the consumer's context with one we control
	consumer.ctx = ctx

	record := makeRecord("sukko.test.trade")

	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.processRecord(record)
	}()

	// Cancel the context to simulate ctx.Done() during CPU brake
	cancel()
	<-done

	if mock.markCount() != 0 {
		t.Errorf("CPU-brake ctx cancel: MarkCommitRecords called %d times, want 0 (record must not be marked)", mock.markCount())
	}
}

// =============================================================================
// CPU Brake Marking Tests (prepareMessage-level invariants)
// =============================================================================

// TestCPUBrakeMarking_BrakeAlwaysActive verifies that when the CPU brake is
// permanently engaged and the context is canceled, prepareMessage returns
// (nil, true) and NO MarkCommitRecords call is made. The record will be
// re-delivered after rebalance — it must not be marked.
func TestCPUBrakeMarking_BrakeAlwaysActive(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true, shouldPause: true},
	})
	consumer.ctx = ctx

	record := makeRecord("sukko.test.trade")

	done := make(chan struct{})
	var msg *preparedMessage
	var ctxCanceled bool
	go func() {
		defer close(done)
		msg, ctxCanceled = consumer.prepareMessage(record)
	}()

	// Cancel context to unblock the brake loop
	cancel()
	<-done

	if msg != nil {
		t.Errorf("BrakeAlwaysActive: expected nil msg, got %+v", msg)
	}
	if !ctxCanceled {
		t.Error("BrakeAlwaysActive: expected ctxCanceled=true, got false")
	}
	if mock.markCount() != 0 {
		t.Errorf("BrakeAlwaysActive: MarkCommitRecords called %d times, want 0 (re-delivery expected)", mock.markCount())
	}
}

// TestCPUBrakeMarking_BrakeNeverActive verifies that when the CPU brake is not
// engaged, a valid record is broadcast and exactly one MarkCommitRecords call is
// made at the processRecord call site — not inside prepareMessage.
func TestCPUBrakeMarking_BrakeNeverActive(t *testing.T) {
	t.Parallel()
	broadcastCalled := 0
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		Broadcast:     func(_ string, _ []byte, _ string, _ int32, _ int64) { broadcastCalled++ },
		BatchSize:     1, // unbatched mode
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true, shouldPause: false},
	})

	record := &kgo.Record{
		Topic:   "sukko.test.trade",
		Key:     []byte("test.BTC.trade"),
		Value:   []byte(`{"price":"50000"}`),
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte("test.BTC.trade")}},
	}
	consumer.processRecord(record)

	if mock.markCount() != 1 {
		t.Errorf("BrakeNeverActive: MarkCommitRecords called %d times after broadcast, want 1", mock.markCount())
	}
	if broadcastCalled != 1 {
		t.Errorf("BrakeNeverActive: broadcast called %d times, want 1", broadcastCalled)
	}
}

// TestCPUBrakeMarking_NoPreFlushMarks guards Decision 6 (research.md):
// prepareMessage MUST NOT call MarkCommitRecords internally — marking MUST only
// happen at the flushBatch/processRecord call site, AFTER broadcast. Moving marks
// into prepareMessage would re-introduce duplicate delivery because replay uses
// prepareMessage via a different (temporary) kgo.Client, and any mark would advance
// the main consumer's committed offsets for records polled by that other client.
//
// This test calls prepareMessage directly and verifies that even when it returns a
// valid (non-nil) msg, the mock committer's mark count is still zero.
func TestCPUBrakeMarking_NoPreFlushMarks(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{
		ResourceGuard: &mockResourceGuardFixed{allowKafka: true, shouldPause: false},
	})

	record := &kgo.Record{
		Topic:   "sukko.test.trade",
		Key:     []byte("test.BTC.trade"),
		Value:   []byte(`{"price":"50000"}`),
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte("test.BTC.trade")}},
	}

	msg, ctxCanceled := consumer.prepareMessage(record)

	if msg == nil {
		t.Fatal("NoPreFlushMarks: prepareMessage returned nil on valid record — test precondition failed")
	}
	if ctxCanceled {
		t.Fatal("NoPreFlushMarks: prepareMessage returned ctxCanceled=true on valid record — test precondition failed")
	}
	// The invariant: prepareMessage MUST NOT call MarkCommitRecords.
	// Marks belong at the flushBatch site (batched) or processRecord site (unbatched).
	if mock.markCount() != 0 {
		t.Errorf("NoPreFlushMarks: MarkCommitRecords called %d times inside prepareMessage, want 0 "+
			"(marks must happen at flushBatch/processRecord site only)", mock.markCount())
	}
}

// =============================================================================
// NewConsumer Validation Tests
// =============================================================================

func TestNewConsumer_CommitOnRevokeTimeout_Zero(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	guard := &mockResourceGuardFixed{allowKafka: true}
	reg := prometheus.NewRegistry()

	_, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{"localhost:9999"},
		ConsumerGroup:         "test-group",
		Topics:                []string{"sukko.test.trade"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) {},
		ResourceGuard:         guard,
		ConsumerType:          ConsumerTypeKindShared,
		Registerer:            reg,
		CommitOnRevokeTimeout: 0, // invalid
	})
	if err == nil {
		t.Fatal("expected error for zero CommitOnRevokeTimeout, got nil")
	}
}

func TestNewConsumer_CommitOnRevokeTimeout_Negative(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	guard := &mockResourceGuardFixed{allowKafka: true}
	reg := prometheus.NewRegistry()

	_, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{"localhost:9999"},
		ConsumerGroup:         "test-group",
		Topics:                []string{"sukko.test.trade"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) {},
		ResourceGuard:         guard,
		ConsumerType:          ConsumerTypeKindShared,
		Registerer:            reg,
		CommitOnRevokeTimeout: -1 * time.Second, // invalid
	})
	if err == nil {
		t.Fatal("expected error for negative CommitOnRevokeTimeout, got nil")
	}
}

// =============================================================================
// ReplayFromOffsets No-Mark Test
// =============================================================================

// TestReplayFromOffsets_NoMarkCalls verifies that ReplayFromOffsets never calls
// MarkCommitRecords on the main consumer's committer, even if records are
// successfully prepared. The replay client is isolated from the main consumer
// group offset state.
func TestReplayFromOffsets_NoMarkCalls(t *testing.T) {
	t.Parallel()
	consumer, mock, _ := newRebalanceTestConsumer(t, ConsumerConfig{})

	// Use a short deadline so the call returns quickly against the fake broker
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// It's expected to fail or return empty — we only care about mark count
	_, _ = consumer.ReplayFromOffsets(ctx, map[string]map[int32]int64{"sukko.test.trade": {0: 0}}, 10, nil)

	if mock.markCount() != 0 {
		t.Errorf("ReplayFromOffsets: MarkCommitRecords called %d times, want 0", mock.markCount())
	}
}

// =============================================================================
// AutoCommitInterval Test
// =============================================================================

// TestKafkaAutoCommitInterval_Override verifies that AutoCommitInterval is passed
// through to kgo without error. We cannot inspect kgo's internal interval from
// outside, but a non-error construction with a valid interval is sufficient.
func TestKafkaAutoCommitInterval_Override(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	guard := &mockResourceGuardFixed{allowKafka: true}
	reg := prometheus.NewRegistry()

	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{"localhost:9999"},
		ConsumerGroup:         "test-autocommit-group",
		Topics:                []string{"sukko.test.trade"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) {},
		ResourceGuard:         guard,
		ConsumerType:          ConsumerTypeKindShared,
		Registerer:            reg,
		CommitOnRevokeTimeout: 5 * time.Second,
		AutoCommitInterval:    1 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewConsumer with AutoCommitInterval=1s: %v", err)
	}
	_ = consumer.Stop()
}
