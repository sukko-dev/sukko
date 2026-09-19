package kafkabackend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/kafka"
	"github.com/sukko-dev/sukko/internal/server/orchestration"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// Compile-time interface check.
var _ backend.MessageBackend = (*KafkaBackend)(nil)

// TestKafkaBackend_Ready_NilRegistry: a nil topic registry means the consumer is disabled
// (produce-only), which is ready immediately (#179 P3). The snapshot-gated transition itself is
// covered by provapi.TestTopicRegistry_SnapshotReceived, which Ready() delegates to.
func TestKafkaBackend_Ready_NilRegistry(t *testing.T) {
	t.Parallel()
	if !(&KafkaBackend{}).Ready() {
		t.Error("Ready() with a nil topic registry must be true (consumer disabled = ready)")
	}
}

// stubRulesSource is a minimal kafka.RoutingRulesSource for New() wiring tests.
type stubRulesSource struct{}

func (stubRulesSource) GetRoutingSnapshot(string) (provapi.TenantRoutingSnapshot, bool) {
	return provapi.TenantRoutingSnapshot{}, false
}
func (stubRulesSource) SnapshotReceived() bool { return true }

// validRoutingDeps returns a provider + positive fan-out/DLQ sizing that pass New()'s
// required-field validation, so a test can reach later code paths (SASL/TLS, etc.).
func validRoutingDeps(c Config) Config {
	c.RulesProvider = stubRulesSource{}
	c.RoutingFanoutWorkers = 4
	c.RoutingFanoutQueueSize = 256
	c.DLQMaxRetries = 3
	c.DLQBaseDelay = 100 * time.Millisecond
	c.DLQMaxDelay = 5 * time.Second
	c.DLQRetryWorkers = 4
	return c
}

func TestNew_RequiresRoutingDeps(t *testing.T) {
	t.Parallel()

	base := Config{Brokers: []string{"localhost:19092"}, Environment: "test"}

	if _, err := New(base); err == nil || !strings.Contains(err.Error(), "rules provider is required") {
		t.Fatalf("nil RulesProvider: got err=%v, want 'rules provider is required'", err)
	}

	// Provider set but sizing left zero → sizing validation rejects (#179 P1b).
	withDeps := base
	withDeps.RulesProvider = stubRulesSource{}
	if _, err := New(withDeps); err == nil || !strings.Contains(err.Error(), "sizing (WS_ROUTING_*) must all be > 0") {
		t.Fatalf("zero sizing: got err=%v, want 'sizing (WS_ROUTING_*) must all be > 0'", err)
	}
}

// =============================================================================
// SplitBrokers Tests
// =============================================================================

func TestSplitBrokers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "comma-separated brokers",
			input: "broker1:9092,broker2:9092,broker3:9092",
			want:  []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:  "single broker",
			input: "broker1:9092",
			want:  []string{"broker1:9092"},
		},
		{
			name:  "empty string",
			input: "",
			want:  nil,
		},
		{
			name:  "whitespace only",
			input: "   ",
			want:  nil,
		},
		{
			name:  "brokers with whitespace",
			input: " broker1:9092 , broker2:9092 , broker3:9092 ",
			want:  []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:  "trailing comma",
			input: "broker1:9092,broker2:9092,",
			want:  []string{"broker1:9092", "broker2:9092"},
		},
		{
			name:  "leading comma",
			input: ",broker1:9092",
			want:  []string{"broker1:9092"},
		},
		{
			name:  "multiple commas",
			input: "broker1:9092,,broker2:9092",
			want:  []string{"broker1:9092", "broker2:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SplitBrokers(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("SplitBrokers(%q) = %v (len %d), want %v (len %d)", tt.input, got, len(got), tt.want, len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("SplitBrokers(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// =============================================================================
// isTopicAlreadyExistsError Tests
// =============================================================================

func TestIsTopicAlreadyExistsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "topic already exists",
			err:  kerr.TopicAlreadyExists,
			want: true,
		},
		{
			name: "wrapped topic already exists",
			err:  fmt.Errorf("create topic failed: %w", kerr.TopicAlreadyExists),
			want: true,
		},
		{
			name: "other error",
			err:  kerr.UnknownServerError,
			want: false,
		},
		{
			name: "connection refused",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "empty error message",
			err:  errors.New(""),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := isTopicAlreadyExistsError(tt.err)
			if got != tt.want {
				t.Errorf("isTopicAlreadyExistsError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestNew_NamespacePassthrough guards the constructor uses cfg.Namespace verbatim and does
// NOT re-derive it from Environment (the deleted double-resolution). Namespace and Environment are set
// to different values to prove the backend takes the passed namespace, not the environment label.
func TestNew_NamespacePassthrough(t *testing.T) {
	t.Parallel()

	b, err := New(validRoutingDeps(Config{
		Brokers:     []string{"localhost:19092"},
		Namespace:   "prod",
		Environment: "dev",
	}))
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
	if b.namespace != "prod" {
		t.Errorf("backend namespace = %q, want %q (verbatim cfg.Namespace, not Environment)", b.namespace, "prod")
	}
}

// =============================================================================
// New — SASL/TLS passthrough Tests
// =============================================================================

func TestNew_SASLAndTLSPassthrough(t *testing.T) {
	t.Parallel()

	// An unsupported SASL mechanism must propagate as a config error, not a broker error.
	// "oauthbearer" is a real SASL mechanism we deliberately do not support (supported:
	// plain, scram-sha-256, scram-sha-512), so it must hit the unsupported-mechanism path.
	_, err := New(validRoutingDeps(Config{
		Brokers:       []string{"localhost:19092"},
		Environment:   "test",
		SASLEnabled:   true,
		SASLMechanism: "oauthbearer",
	}))
	if err == nil {
		t.Fatal("expected error for unsupported SASL mechanism, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Fatalf("expected SASL mechanism error, got: %v", err)
	}

	// A missing CA file must propagate as a config error, not a broker error.
	_, err = New(validRoutingDeps(Config{
		Brokers:     []string{"localhost:19092"},
		Environment: "test",
		TLSEnabled:  true,
		TLSCAPath:   "/nonexistent/ca.pem",
	}))
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read CA certificate") {
		t.Fatalf("expected CA certificate error, got: %v", err)
	}
}

// =============================================================================
// Publish Channel Validation Tests
// =============================================================================

func TestPublish_EmptyChannel(t *testing.T) {
	t.Parallel()

	// Construct a minimal KafkaBackend with nil producer.
	// The empty channel validation runs before producer delegation.
	kb := &KafkaBackend{}

	_, err := kb.Publish(context.Background(), 1, "test-tenant", "", []byte("data"))
	if err == nil {
		t.Fatal("expected error for empty channel, got nil")
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("error = %v, want wrapping %v", err, backend.ErrPublishFailed)
	}
}

func TestPublish_NilProducer(t *testing.T) {
	t.Parallel()

	// Producer is nil but channel is valid — should hit the producer nil check.
	kb := &KafkaBackend{}

	_, err := kb.Publish(context.Background(), 1, "test-tenant", "test.channel", []byte("data"))
	if err == nil {
		t.Fatal("expected error for nil producer, got nil")
	}
	if !errors.Is(err, backend.ErrPublishFailed) {
		t.Errorf("error = %v, want wrapping %v", err, backend.ErrPublishFailed)
	}
}

// =============================================================================
// IsHealthy Tests
// =============================================================================

func TestIsHealthy_Default(t *testing.T) {
	t.Parallel()

	kb := &KafkaBackend{}
	if kb.IsHealthy() {
		t.Error("expected false for zero-value KafkaBackend, got true")
	}
}

// =============================================================================
// Test Certificate
// =============================================================================

// convertReplayMessages must recompute the mid from record coordinates with
// the SAME derivation the consumer applies on live delivery — the property
// that makes a replayed copy carry the identical identity (ADR-0008).
func TestConvertReplayMessages_MidRecompute(t *testing.T) {
	t.Parallel()

	in := []kafka.ReplayMessage{
		{Topic: "prod.acme.odds", Partition: 3, Offset: 42, Subject: "acme.BTC.trade", Data: []byte(`{"p":1}`), Pos: "4-42"},
		{Topic: "prod.acme.odds", Partition: 3, Offset: 43, Subject: "acme.BTC.trade", Data: []byte(`{"p":2}`), Pos: "4-43"},
	}
	out := convertReplayMessages(in)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	for i, m := range out {
		want := kafkashared.MessageID(in[i].Topic, in[i].Partition, in[i].Offset)
		if m.Mid != want {
			t.Errorf("[%d] Mid = %q, want %q (live-copy derivation)", i, m.Mid, want)
		}
		if m.Pos != in[i].Pos || m.Subject != in[i].Subject {
			t.Errorf("[%d] Pos/Subject not passed through", i)
		}
	}
}

// =============================================================================
// ChannelTopic — deterministic rule-based resolution (ADR-0018)
// =============================================================================

// routingStub is a configurable RoutingRulesSource for ChannelTopic tests.
type routingStub struct {
	snap   provapi.TenantRoutingSnapshot
	found  bool
	synced bool
}

func (s *routingStub) GetRoutingSnapshot(string) (provapi.TenantRoutingSnapshot, bool) {
	return s.snap, s.found
}
func (s *routingStub) SnapshotReceived() bool { return s.synced }

// TestChannelTopic_ResolvesFromRules pins the ADR-0018 replacement of the
// observed-traffic channel→topic cache: resolution comes from the tenant's
// routing rules DETERMINISTICALLY — critically, on a pod that has routed no
// traffic at all (the two-replica deployment where one replica owned all
// partitions failed 242 of 480 replay requests under the old cache).
func TestChannelTopic_ResolvesFromRules(t *testing.T) {
	t.Parallel()

	pool := &orchestration.MultiTenantConsumerPool{} // consumer present; has routed NOTHING

	tests := []struct {
		name    string
		rules   *routingStub
		channel string
		want    string
		wantOK  bool
	}{
		{
			name: "first matching rule's ingress topic wins",
			rules: &routingStub{synced: true, found: true, snap: provapi.TenantRoutingSnapshot{Rules: []types.RoutingRule{
				{Pattern: "acme.**.trade", IngressTopic: "trades", EgressTopics: []string{"audit"}, Priority: 1},
				{Pattern: "acme.**", IngressTopic: "other", Priority: 2},
			}}},
			channel: "acme.BTC.trade",
			want:    "prod.acme.trades",
			wantOK:  true,
		},
		{
			name: "no matching rule resolves to the tenant default topic",
			rules: &routingStub{synced: true, found: true, snap: provapi.TenantRoutingSnapshot{Rules: []types.RoutingRule{
				{Pattern: "acme.**.orders", IngressTopic: "orders", Priority: 1},
			}}},
			channel: "acme.BTC.trade",
			want:    "prod.acme.default",
			wantOK:  true,
		},
		{
			name:    "rule-less tenant resolves to the tenant default topic (Community ingest)",
			rules:   &routingStub{synced: true, found: false},
			channel: "acme.BTC.trade",
			want:    "prod.acme.default",
			wantOK:  true,
		},
		{
			name:    "rules snapshot not synced: mapping unknown, not guessed",
			rules:   &routingStub{synced: false},
			channel: "acme.BTC.trade",
			wantOK:  false,
		},
		{
			name:    "malformed channel (no tenant prefix) does not resolve",
			rules:   &routingStub{synced: true, found: false},
			channel: "nodots",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kb := &KafkaBackend{pool: pool, rulesProvider: tt.rules, namespace: "prod"}
			got, ok := kb.ChannelTopic(tt.channel)
			if ok != tt.wantOK {
				t.Fatalf("ChannelTopic(%q) ok = %v, want %v", tt.channel, ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("ChannelTopic(%q) = %q, want %q", tt.channel, got, tt.want)
			}
		})
	}
}

// TestChannelTopic_NoConsumer: connection-only mode (nil pool) has no topic mapping.
func TestChannelTopic_NoConsumer(t *testing.T) {
	t.Parallel()
	kb := &KafkaBackend{rulesProvider: &routingStub{synced: true}, namespace: "prod"}
	if _, ok := kb.ChannelTopic("acme.BTC.trade"); ok {
		t.Error("ChannelTopic with nil pool: want ok=false")
	}
}
