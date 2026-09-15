package kafka

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// =============================================================================
// Helpers
// =============================================================================

// mockPauser records which topics were paused.
type mockPauser struct {
	mu     sync.Mutex
	paused []string
}

func (m *mockPauser) PauseFetchTopics(topics ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paused = append(m.paused, topics...)
	return m.paused
}

func (m *mockPauser) getPaused() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.paused)
}

// newTestConsumer builds a Consumer with only the fields needed for handleFetchErrors
// testing. It does not create a kgo.Client — safe to call without a real broker.
// The caller is responsible for calling consumer.Stop() if needed (t.Cleanup).
func newTestConsumer(t *testing.T, cb func(string), consumerType string) (*Consumer, *mockPauser, *prometheus.Registry) {
	t.Helper()

	logger := zerolog.Nop()
	p := &mockPauser{}
	reg := prometheus.NewRegistry()

	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricConsumerTopicDeletedTotal,
		Help: "test",
	}, []string{LabelConsumerType})
	if err := reg.Register(counterVec); err != nil {
		t.Fatalf("register counter: %v", err)
	}

	return &Consumer{
		logger:         &logger,
		pauser:         p,
		onUnknownTopic: cb,
		deletedCounter: counterVec.With(prometheus.Labels{LabelConsumerType: consumerType}),
	}, p, reg
}

// counterValue reads the current value of the deleted-topic counter.
// The counter label matches what was set in newTestConsumer.
func counterValue(t *testing.T, c *Consumer) float64 {
	t.Helper()
	return testutil.ToFloat64(c.deletedCounter)
}

// =============================================================================
// handleFetchErrors Tests
// =============================================================================

func TestHandleFetchErrors_SingleUnknownTopic(t *testing.T) {
	t.Parallel()

	var cbTopics []string
	c, p, _ := newTestConsumer(t, func(topic string) { cbTopics = append(cbTopics, topic) }, ConsumerTypeKindShared)

	errs := []kgo.FetchError{
		{Topic: "sukko.tenant1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	if got := p.getPaused(); len(got) != 1 || got[0] != "sukko.tenant1.trade" {
		t.Fatalf("paused = %v, want [sukko.tenant1.trade]", got)
	}
	if len(cbTopics) != 1 || cbTopics[0] != "sukko.tenant1.trade" {
		t.Fatalf("callbacks = %v, want [sukko.tenant1.trade]", cbTopics)
	}
	if v := counterValue(t, c); v != 1.0 {
		t.Fatalf("counter = %v, want 1", v)
	}
}

func TestHandleFetchErrors_MultiPartitionDedup(t *testing.T) {
	t.Parallel()

	var cbCount int
	c, p, _ := newTestConsumer(t, func(_ string) { cbCount++ }, ConsumerTypeKindShared)

	// Three partitions of the same topic — callback and pause must fire once only.
	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
		{Topic: "sukko.t1.trade", Partition: 1, Err: kerr.UnknownTopicOrPartition},
		{Topic: "sukko.t1.trade", Partition: 2, Err: kerr.UnknownTopicOrPartition},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	if got := p.getPaused(); len(got) != 1 {
		t.Fatalf("paused = %v, want exactly 1 entry", got)
	}
	if cbCount != 1 {
		t.Fatalf("callback count = %d, want 1", cbCount)
	}
	if v := counterValue(t, c); v != 1.0 {
		t.Fatalf("counter = %v, want 1", v)
	}
}

func TestHandleFetchErrors_TwoUnknownTopics(t *testing.T) {
	t.Parallel()

	var cbTopics []string
	c, p, _ := newTestConsumer(t, func(topic string) { cbTopics = append(cbTopics, topic) }, ConsumerTypeKindShared)

	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
		{Topic: "sukko.t2.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	if got := p.getPaused(); len(got) != 2 {
		t.Fatalf("paused = %v, want 2 entries", got)
	}
	if len(cbTopics) != 2 {
		t.Fatalf("callback count = %d, want 2", len(cbTopics))
	}
	if v := counterValue(t, c); v != 2.0 {
		t.Fatalf("counter = %v, want 2", v)
	}
}

func TestHandleFetchErrors_MixedErrors(t *testing.T) {
	t.Parallel()

	var cbTopics []string
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf)
	p := &mockPauser{}
	reg := prometheus.NewRegistry()
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricConsumerTopicDeletedTotal,
		Help: "test",
	}, []string{LabelConsumerType})
	if err := reg.Register(counterVec); err != nil {
		t.Fatalf("register counter: %v", err)
	}
	c := &Consumer{
		logger:         &logger,
		pauser:         p,
		onUnknownTopic: func(topic string) { cbTopics = append(cbTopics, topic) },
		deletedCounter: counterVec.With(prometheus.Labels{LabelConsumerType: ConsumerTypeKindShared}),
	}

	otherErr := errors.New("some transient broker error")
	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
		{Topic: "sukko.t2.trade", Partition: 0, Err: otherErr},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	// Unknown topic: paused, callback fired, counter incremented.
	if got := p.getPaused(); len(got) != 1 || got[0] != "sukko.t1.trade" {
		t.Fatalf("paused = %v, want [sukko.t1.trade]", got)
	}
	if len(cbTopics) != 1 || cbTopics[0] != "sukko.t1.trade" {
		t.Fatalf("callbacks = %v, want [sukko.t1.trade]", cbTopics)
	}
	if v := testutil.ToFloat64(c.deletedCounter); v != 1.0 {
		t.Fatalf("counter = %v, want 1", v)
	}

	// Unknown-topic: warn log; other error: error log.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, `"level":"warn"`) {
		t.Fatalf("expected warn log for unknown topic, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"level":"error"`) {
		t.Fatalf("expected error log for non-unknown-topic error, got: %s", logOutput)
	}
}

func TestHandleFetchErrors_NilCallback(t *testing.T) {
	t.Parallel()

	// nil OnUnknownTopic: must not panic; topic still paused; counter still incremented.
	c, p, _ := newTestConsumer(t, nil, ConsumerTypeKindShared)

	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
	}

	var panicked bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		paused := c.handleFetchErrors(errs)
		c.deletedCounter.Add(float64(len(paused)))
	}()

	if panicked {
		t.Fatal("nil callback must not cause panic")
	}
	if got := p.getPaused(); len(got) != 1 {
		t.Fatalf("paused = %v, want 1 entry", got)
	}
	if v := counterValue(t, c); v != 1.0 {
		t.Fatalf("counter = %v, want 1", v)
	}
}

func TestHandleFetchErrors_WrappedError(t *testing.T) {
	t.Parallel()

	var cbTopics []string
	c, p, _ := newTestConsumer(t, func(topic string) { cbTopics = append(cbTopics, topic) }, ConsumerTypeKindShared)

	wrappedErr := fmt.Errorf("wrap1: %w", fmt.Errorf("wrap2: %w", kerr.UnknownTopicOrPartition))
	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: wrappedErr},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	if got := p.getPaused(); len(got) != 1 {
		t.Fatalf("paused = %v, want 1 entry for wrapped error", got)
	}
	if len(cbTopics) != 1 {
		t.Fatalf("callbacks = %v, want 1 for wrapped error", cbTopics)
	}
	if v := counterValue(t, c); v != 1.0 {
		t.Fatalf("counter = %v, want 1", v)
	}
}

func TestHandleFetchErrors_PanickingCallback(t *testing.T) {
	t.Parallel()

	var cbOrder []string
	panicOnTopic := "sukko.t1.trade"
	safeTopic := "sukko.t2.trade"

	cb := func(topic string) {
		cbOrder = append(cbOrder, topic)
		if topic == panicOnTopic {
			panic("intentional test panic in callback")
		}
	}
	c, p, _ := newTestConsumer(t, cb, ConsumerTypeKindShared)

	errs := []kgo.FetchError{
		{Topic: panicOnTopic, Partition: 0, Err: kerr.UnknownTopicOrPartition},
		{Topic: safeTopic, Partition: 0, Err: kerr.UnknownTopicOrPartition},
	}
	paused := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused)))

	// Both topics must be paused — panic in callback must not abort the loop.
	got := p.getPaused()
	if len(got) != 2 {
		t.Fatalf("paused = %v, want both topics paused after panic", got)
	}

	// Both callbacks must have been invoked — safeTopic callback must fire after panic recovery.
	if len(cbOrder) != 2 || cbOrder[0] != panicOnTopic || cbOrder[1] != safeTopic {
		t.Fatalf("cbOrder = %v, want [%s, %s] (loop must continue after panic)", cbOrder, panicOnTopic, safeTopic)
	}

	// Counter incremented for all paused topics.
	if v := counterValue(t, c); v != 2.0 {
		t.Fatalf("counter = %v, want 2", v)
	}
}

func TestHandleFetchErrors_IdempotentPerCall(t *testing.T) {
	t.Parallel()

	var cbCount int
	c, _, _ := newTestConsumer(t, func(_ string) { cbCount++ }, ConsumerTypeKindShared)

	errs := []kgo.FetchError{
		{Topic: "sukko.t1.trade", Partition: 0, Err: kerr.UnknownTopicOrPartition},
	}

	// First call
	paused1 := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused1)))

	// Second call with same topic — fresh seen map per call → callback fires again.
	paused2 := c.handleFetchErrors(errs)
	c.deletedCounter.Add(float64(len(paused2)))

	if cbCount != 2 {
		t.Fatalf("callback count = %d, want 2 (once per call, seen map is fresh)", cbCount)
	}
	if v := counterValue(t, c); v != 2.0 {
		t.Fatalf("counter = %v, want 2", v)
	}
}

// =============================================================================
// Registry-miss silent-drop observability (prepareMessage ReasonUnknownTopic)
// =============================================================================

// A record fetched from a topic the tenant resolver cannot map is dropped
// without being marked. That drop was previously silent; it must now increment
// an observable counter and emit a warn with topic/partition/offset — the drop
// is a §V silent-data-loss risk (the record is not redelivered in-session).
func TestPrepareMessage_UnknownTopic_ObservableDrop(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf)
	c := &Consumer{
		logger:         &logger,
		resourceGuard:  newMockResourceGuard(),                           // allow=true, pause=false
		tenantResolver: func(string) (string, bool) { return "", false }, // registry miss
	}

	rec := &kgo.Record{Topic: "local.tester-abc.default", Partition: 0, Offset: 7, Value: []byte("x")}

	msg, skipMark := c.prepareMessage(rec)

	// Behavior preserved: dropped (nil msg) and NOT marked (skipMark true → redeliver leg).
	if msg != nil {
		t.Fatalf("expected nil preparedMessage on registry miss, got %+v", msg)
	}
	if !skipMark {
		t.Fatal("registry-miss drop must return skipMark=true (do NOT mark the offset)")
	}
	// Observable: counter incremented.
	if got := c.UnknownTopicDrops(); got != 1 {
		t.Fatalf("UnknownTopicDrops() = %d, want 1", got)
	}
	// Observable: first drop is always logged (single-record loss must never be silent),
	// with topic, partition, and offset.
	out := logBuf.String()
	if !strings.Contains(out, `"level":"warn"`) {
		t.Fatalf("expected warn log for registry-miss drop, got: %s", out)
	}
	for _, want := range []string{`"topic":"local.tester-abc.default"`, `"partition":0`, `"offset":7`} {
		if !strings.Contains(out, want) {
			t.Fatalf("warn log missing %s, got: %s", want, out)
		}
	}
}

// =============================================================================
// NewConsumer Wiring Tests
// =============================================================================

func TestNewConsumer_WiresFields(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	guard := newMockResourceGuard()
	broadcast := func(_ string, _ []byte, _ string, _ int32, _ int64) {}
	reg := prometheus.NewRegistry()

	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{"localhost:1"}, // offline — franz-go connects lazily
		ConsumerGroup:         "test-wires-group",
		Topics:                []string{"sukko.tenant1.trade"},
		Logger:                &logger,
		Broadcast:             broadcast,
		ResourceGuard:         guard,
		Registerer:            reg,
		ConsumerType:          ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 5 * time.Second,
		OnUnknownTopic:        func(_ string) {},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })

	if consumer.pauser == nil {
		t.Error("consumer.pauser must not be nil")
	}
	if consumer.onUnknownTopic == nil {
		t.Error("consumer.onUnknownTopic must not be nil")
	}
	if consumer.deletedCounter == nil {
		t.Error("consumer.deletedCounter must not be nil")
	}
	if v := testutil.ToFloat64(consumer.deletedCounter); v != 0.0 {
		t.Fatalf("initial counter = %v, want 0", v)
	}
}

func TestNewConsumer_NilRegisterer_UsesSingleton(t *testing.T) { //nolint:paralleltest // nil-Registerer path uses the package-level promauto singleton; concurrent once.Do with other non-parallel tests could race.
	// No t.Parallel() — nil-Registerer path uses the package-level promauto singleton;
	// concurrent once.Do execution with other non-parallel tests could race.

	logger := zerolog.Nop()
	guard := newMockResourceGuard()
	broadcast := func(_ string, _ []byte, _ string, _ int32, _ int64) {}

	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:               []string{"localhost:1"},
		ConsumerGroup:         "test-singleton-group",
		Topics:                []string{"sukko.tenant1.trade"},
		Logger:                &logger,
		Broadcast:             broadcast,
		ResourceGuard:         guard,
		ConsumerType:          ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewConsumer (nil Registerer): %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop() })

	if consumer.deletedCounter == nil {
		t.Error("consumer.deletedCounter must not be nil when Registerer is nil")
	}
	if v := testutil.ToFloat64(consumer.deletedCounter); v != 0.0 {
		t.Fatalf("initial counter = %v, want 0", v)
	}
}

func TestNewConsumer_EmptyConsumerType_ReturnsError(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	guard := newMockResourceGuard()
	broadcast := func(_ string, _ []byte, _ string, _ int32, _ int64) {}
	reg := prometheus.NewRegistry()

	_, err := NewConsumer(ConsumerConfig{
		Brokers:       []string{"localhost:1"},
		ConsumerGroup: "test-empty-type-group",
		Topics:        []string{"sukko.tenant1.trade"},
		Logger:        &logger,
		Broadcast:     broadcast,
		ResourceGuard: guard,
		Registerer:    reg,
		// ConsumerType intentionally omitted
	})
	if err == nil {
		t.Fatal("NewConsumer must return an error when ConsumerType is empty")
	}
	if !strings.Contains(err.Error(), "consumer type") {
		t.Fatalf("error message must mention 'consumer type', got: %v", err)
	}
}
