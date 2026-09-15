package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
)

// =============================================================================
// extractChannel tests
// =============================================================================

// parseResolver is a test tenant resolver: tenant = the topic's second segment. Production wires the
// registry topic→tenant map; tests parse for convenience. A topic with <3 segments is "unknown".
func parseResolver(topic string) (string, bool) {
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) < 3 {
		return "", false
	}
	return parts[1], true
}

// newExtractConsumer builds a Consumer wired with parseResolver for extractChannel tests.
func newExtractConsumer() *Consumer {
	return &Consumer{tenantResolver: parseResolver}
}

func TestExtractChannel_UnknownTopic_TwoSegments(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{Topic: "prod.acme"} // only 2 parts → not resolvable
	_, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonUnknownTopic {
		t.Errorf("reason = %q, want %q for a topic not in the registry", reason, ReasonUnknownTopic)
	}
}

func TestExtractChannel_UnknownTopic_OneSegment(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{Topic: "prod"}
	_, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonUnknownTopic {
		t.Errorf("reason = %q, want %q", reason, ReasonUnknownTopic)
	}
}

// An unknown topic is checked BEFORE the header, so a headerless record on an unknown topic is the
// transient redeliver leg (ReasonUnknownTopic), not the permanent missing-header leg.
func TestExtractChannel_UnknownTopic_BeforeHeaderCheck(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{Topic: "prod.acme"} // unresolvable → unknown before header inspection
	_, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonUnknownTopic {
		t.Errorf("reason = %q, want %q", reason, ReasonUnknownTopic)
	}
}

func TestExtractChannel_NilResolver_UnknownTopic(t *testing.T) {
	t.Parallel()

	// No resolver wired → every topic is unknown (fail-closed).
	c := &Consumer{}
	rec := &kgo.Record{Topic: "prod.acme.orders", Headers: []kgo.RecordHeader{
		{Key: kafkashared.HeaderChannel, Value: []byte("acme.BTC.orders")},
	}}
	_, reason, _ := c.extractChannel(rec)
	if reason != ReasonUnknownTopic {
		t.Errorf("reason = %q, want %q", reason, ReasonUnknownTopic)
	}
}

func TestExtractChannel_ValidHeader_TenantMatch(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Topic: "prod.acme.orders",
		Headers: []kgo.RecordHeader{
			{Key: kafkashared.HeaderChannel, Value: []byte("acme.BTC.orders")},
		},
	}

	channel, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != "" {
		t.Errorf("reason = %q, want empty", reason)
	}
	if channel != "acme.BTC.orders" {
		t.Errorf("channel = %q, want %q", channel, "acme.BTC.orders")
	}
}

// Surviving guarantee: a header whose tenant prefix ≠ the topic tenant MUST be rejected —
// this is now the sole cross-tenant boundary since the record.Key fallback is gone.
func TestExtractChannel_ValidHeader_TenantMismatch(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Topic: "prod.acme.orders",
		Headers: []kgo.RecordHeader{
			{Key: kafkashared.HeaderChannel, Value: []byte("globex.BTC.orders")}, // wrong tenant
		},
	}

	channel, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonTenantPrefixMismatch {
		t.Errorf("reason = %q, want %q", reason, ReasonTenantPrefixMismatch)
	}
	if channel != "" {
		t.Errorf("channel = %q, want empty — cross-tenant record must NOT be broadcast", channel)
	}
}

func TestExtractChannel_Header_NoDot_InvalidKey(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Topic:   "prod.acme.orders",
		Headers: []kgo.RecordHeader{{Key: kafkashared.HeaderChannel, Value: []byte("nodot")}},
	}
	_, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonInvalidChannelKey {
		t.Errorf("reason = %q, want %q", reason, ReasonInvalidChannelKey)
	}
}

// A headerless record on a KNOWN topic is a permanent producer bug → ReasonMissingChannelHeader (the
// caller DLQs + marks). There is no edition branch and no record.Key fallback (#179 P3).
func TestExtractChannel_NoHeader_KnownTopic_MissingHeader(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{Topic: "prod.acme.orders", Key: []byte("acme.BTC.orders")} // Key is ignored now
	channel, reason, _ := newExtractConsumer().extractChannel(rec)
	if reason != ReasonMissingChannelHeader {
		t.Errorf("reason = %q, want %q", reason, ReasonMissingChannelHeader)
	}
	if channel != "" {
		t.Errorf("channel = %q, want empty — record.Key is no longer a channel source", channel)
	}
}

func TestFindHeader_ReturnsNilWhenAbsent(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Headers: []kgo.RecordHeader{{Key: "other-key", Value: []byte("v")}},
	}
	val := findHeader(rec, kafkashared.HeaderChannel)
	if val != nil {
		t.Errorf("findHeader = %q, want nil", val)
	}
}

func TestFindHeader_ReturnsFirstMatch(t *testing.T) {
	t.Parallel()

	rec := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: kafkashared.HeaderChannel, Value: []byte("first")},
			{Key: kafkashared.HeaderChannel, Value: []byte("second")},
		},
	}
	val := findHeader(rec, kafkashared.HeaderChannel)
	if string(val) != "first" {
		t.Errorf("findHeader = %q, want %q", val, "first")
	}
}

// =============================================================================
// routeToDLQ tests — tenant is now passed by the caller (registry-resolved), not reverse-parsed.
// =============================================================================

func TestRouteToDLQ_NilDLQ_DoesNotPanic(t *testing.T) {
	t.Parallel()

	c := &Consumer{namespace: "prod", dlq: nil}
	rec := &kgo.Record{Topic: "prod.acme.orders", Value: []byte("payload")}
	c.routeToDLQ(rec, "acme", ReasonMissingChannelHeader)
}

func TestRouteToDLQ_ValidTopic_SubmitsJobWithReasonHeader(t *testing.T) {
	t.Parallel()

	pool := &DLQPool{
		jobs:   make(chan dlqJob, 4),
		cfg:    DLQConfig{Workers: 0},
		logger: zerolog.Nop(),
	}
	c := &Consumer{namespace: "prod", dlq: pool}
	rec := &kgo.Record{
		Topic:   "prod.acme.orders",
		Key:     []byte("acme.BTC.orders"),
		Value:   []byte("payload"),
		Headers: []kgo.RecordHeader{{Key: "existing", Value: []byte("val")}},
	}
	c.routeToDLQ(rec, "acme", ReasonMissingChannelHeader)

	select {
	case job := <-pool.jobs:
		if job.tenant != "acme" {
			t.Errorf("tenant = %q, want %q", job.tenant, "acme")
		}
		if job.reason != ReasonMissingChannelHeader {
			t.Errorf("reason = %q, want %q", job.reason, ReasonMissingChannelHeader)
		}
		found := false
		for _, h := range job.record.Headers {
			if h.Key == HeaderReason && string(h.Value) == ReasonMissingChannelHeader {
				found = true
			}
		}
		if !found {
			t.Error("DLQ record missing HeaderReason header with expected value")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected DLQ job to be submitted")
	}
}

func TestRouteToDLQ_QueueFull_DropsJobSilently(t *testing.T) {
	t.Parallel()

	// Capacity-0 channel: TrySubmit always returns false.
	pool := &DLQPool{
		jobs:   make(chan dlqJob),
		cfg:    DLQConfig{Workers: 0},
		logger: zerolog.Nop(),
	}
	c := &Consumer{namespace: "prod", dlq: pool}
	rec := &kgo.Record{Topic: "prod.acme.orders", Value: []byte("payload")}
	c.routeToDLQ(rec, "acme", ReasonNoRoutingRuleMatched) // must not block or panic
}
