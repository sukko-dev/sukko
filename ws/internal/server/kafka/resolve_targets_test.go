package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sony/gobreaker/v2"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// =============================================================================
// resolveTargets tests (#179: reject semantics, no community fallback)
// =============================================================================

// stubRulesProvider implements RoutingRulesSource for producer tests and
// RoutingSnapshotProvider for consumer tests (shared with extract_channel_test.go).
type stubRulesProvider struct {
	snap   provapi.TenantRoutingSnapshot
	ok     bool
	synced bool
}

func (s *stubRulesProvider) GetRoutingSnapshot(_ string) (provapi.TenantRoutingSnapshot, bool) {
	return s.snap, s.ok
}

func (s *stubRulesProvider) SnapshotReceived() bool { return s.synced }

// resolveTargetsProducer builds a producer wired with a rules source. Routing rules are
// ungated on every edition (ADR-0014), so no license accessor is involved.
func resolveTargetsProducer(prov RoutingRulesSource) *Producer {
	return &Producer{
		topicNamespace: "prod",
		rulesProvider:  prov,
	}
}

func syncedProvider(rules ...types.RoutingRule) *stubRulesProvider {
	return &stubRulesProvider{
		ok:     true,
		synced: true,
		snap:   provapi.TenantRoutingSnapshot{Rules: rules},
	}
}

func rule(pattern, ingress string, egress ...string) types.RoutingRule {
	return types.RoutingRule{Pattern: pattern, IngressTopic: ingress, EgressTopics: egress, Priority: 1}
}

// TestPublish_EgressRejectDoesNotTripBreaker pins the invariant that a rule with egress
// topics but no fanout pool (the P1a state) is rejected BEFORE the circuit breaker — so one
// tenant's valid egress rule cannot open the shared breaker and 503 every tenant (§IX).
func TestPublish_EgressRejectDoesNotTripBreaker(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	p, err := NewProducer(ProducerConfig{
		Brokers:                   []string{"localhost:19092"}, // kgo does not dial at construction
		TopicNamespace:            "prod",
		Logger:                    &logger,
		CircuitBreakerMaxFailures: 3, // low, to prove even many rejects don't trip it
		CircuitBreakerTimeout:     time.Minute,
		RulesProvider:             syncedProvider(rule("acme.**.trade", "a", "b")),
		// Fanout intentionally nil (P1a).
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer func() { _ = p.Close() }()

	// The reject fires before any Kafka I/O, so no broker connection is needed.
	for i := range 10 {
		_, perr := p.Publish(context.Background(), 1, "acme.BTC.trade", []byte(`{"x":1}`))
		if !errors.Is(perr, backend.ErrPublishFailed) {
			t.Fatalf("publish %d: err = %v, want ErrPublishFailed", i, perr)
		}
	}
	if state := p.CircuitBreakerState(); state != gobreaker.StateClosed {
		t.Errorf("breaker state = %v, want Closed — egress rejects must not trip the breaker", state)
	}
}

func TestResolveTargets(t *testing.T) {
	t.Parallel()

	const channel = "acme.BTC.trade"

	tests := []struct {
		name        string
		producer    *Producer
		wantIngress string
		wantEgress  []string
		wantErrIs   error // nil = no error expected
	}{
		{
			name:      "nil provider rejects (no community fallback)",
			producer:  resolveTargetsProducer(nil),
			wantErrIs: backend.ErrPublishNotRoutable,
		},
		{
			name:      "not synced yet is unavailable (retryable), not a reject",
			producer:  resolveTargetsProducer(&stubRulesProvider{ok: false, synced: false}),
			wantErrIs: ErrServiceUnavailable,
		},
		{
			name:      "synced but tenant absent rejects",
			producer:  resolveTargetsProducer(&stubRulesProvider{ok: false, synced: true}),
			wantErrIs: backend.ErrPublishNotRoutable,
		},
		{
			name:      "synced but zero rules rejects",
			producer:  resolveTargetsProducer(syncedProvider()),
			wantErrIs: backend.ErrPublishNotRoutable,
		},
		{
			name:        "ingress-only rule match produces",
			producer:    resolveTargetsProducer(syncedProvider(rule("acme.*.trade", "trades"))),
			wantIngress: "prod.acme.trades",
		},
		{
			name:        "rule with egress returns ingress plus egress plan",
			producer:    resolveTargetsProducer(syncedProvider(rule("acme.**.trade", "trades", "all-data"))),
			wantIngress: "prod.acme.trades",
			wantEgress:  []string{"prod.acme.all-data"},
		},
		{
			// §II defense in depth: validation rejects this at write time, but a
			// skewed snapshot must not double-produce to the ingress topic.
			name:        "egress equal to ingress is dropped from the plan",
			producer:    resolveTargetsProducer(syncedProvider(rule("acme.**.trade", "trades", "trades"))),
			wantIngress: "prod.acme.trades",
		},
		{
			name:        "duplicate egress topics dedupe to a single copy",
			producer:    resolveTargetsProducer(syncedProvider(rule("acme.**.trade", "trades", "audit", "audit"))),
			wantIngress: "prod.acme.trades",
			wantEgress:  []string{"prod.acme.audit"},
		},
		{
			// C1 revised (#179): a rule with no ingress topic → no-match → REJECT (was DLQ).
			name:      "no-ingress rule treated as no-match rejects",
			producer:  resolveTargetsProducer(syncedProvider(rule("acme.*.trade", ""))),
			wantErrIs: backend.ErrNoMatchingRoute,
		},
		{
			// C1 revised (#179): rules present, none match → REJECT (was DLQ).
			name:      "no rule matches rejects (not dead-lettered)",
			producer:  resolveTargetsProducer(syncedProvider(rule("acme.*.orders", "orders"))),
			wantErrIs: backend.ErrNoMatchingRoute,
		},
		{
			name:        "first matching rule wins",
			producer:    resolveTargetsProducer(syncedProvider(rule("acme.**.trade", "all-trades"), rule("acme.BTC.trade", "btc-trades"))),
			wantIngress: "prod.acme.all-trades",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan, err := tt.producer.resolveTargets(channel, "acme")

			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tt.wantErrIs)
				}
				// ErrNoMatchingRoute wraps ErrPublishNotRoutable so the shared reject-class
				// handling (gRPC FailedPrecondition, reject metric, Warn log) catches it.
				if errors.Is(err, backend.ErrNoMatchingRoute) && !errors.Is(err, backend.ErrPublishNotRoutable) {
					t.Errorf("ErrNoMatchingRoute must also satisfy errors.Is(ErrPublishNotRoutable); err = %v", err)
				}
				if plan.ingressTopic != "" || plan.egressTopics != nil {
					t.Errorf("plan = %+v, want zero value on reject", plan)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err = %v", err)
			}
			if plan.ingressTopic != tt.wantIngress {
				t.Errorf("plan.ingressTopic = %q, want %q", plan.ingressTopic, tt.wantIngress)
			}
			if len(plan.egressTopics) != len(tt.wantEgress) {
				t.Fatalf("plan.egressTopics = %v, want %v", plan.egressTopics, tt.wantEgress)
			}
			for i, want := range tt.wantEgress {
				if plan.egressTopics[i] != want {
					t.Errorf("plan.egressTopics[%d] = %q, want %q", i, plan.egressTopics[i], want)
				}
			}
		})
	}
}
