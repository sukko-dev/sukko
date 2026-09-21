package provisioning_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// newEditionService builds a provisioning service with a fixed edition and a
// tenant "acme-corp" whose ingress/egress topics all pre-exist on the mock
// broker, so ONLY the edition gate can reject.
func newEditionService(t *testing.T, edition license.Edition) (*provisioning.Service, *testutil.MockRoutingRulesStore) {
	t.Helper()
	tenantStore := testutil.NewMockTenantStore()
	if err := tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp")); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	routingStore := testutil.NewMockRoutingRulesStore()
	kafkaAdmin := testutil.NewMockKafkaAdmin()
	for _, suffix := range []string{"default", "trades", "audit", "orders"} {
		if err := kafkaAdmin.CreateTopic(context.Background(), "test.acme-corp."+suffix, 1, 1, nil); err != nil {
			t.Fatalf("seed topic %s: %v", suffix, err)
		}
	}

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           routingStore,
		TopicStore:                  testutil.NewMockTopicStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		EditionManager:              license.NewTestManager(edition),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    100,
		MaxTopicsPerRule:            10,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  604800000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, routingStore
}

func isFeatureError(err error) bool {
	_, ok := errors.AsType[*license.EditionFeatureError](err)
	return ok
}

// TestEgressTopicsGate pins the ADR-0018 edition boundary on BOTH write paths:
// egress topics require Pro (license.EgressTopics); an ingress-only rule NEVER
// trips the gate — Community keeps full routing (ADR-0014), which is what keeps
// the unlicensed benchmark stack (`ingress_topic: "default"`, no egress) legal.
func TestEgressTopicsGate(t *testing.T) {
	t.Parallel()

	ingressOnly := provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", Priority: 1}
	withEgress := provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: []string{"audit"}, Priority: 1}

	tests := []struct {
		name     string
		edition  license.Edition
		rule     provisioning.TopicRoutingRule
		wantGate bool
	}{
		{"community ingress-only passes", license.Community, ingressOnly, false},
		{"community empty egress slice passes", license.Community,
			provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: []string{}, Priority: 1}, false},
		{"community egress gated", license.Community, withEgress, true},
		{"pro egress passes", license.Pro, withEgress, false},
		{"enterprise egress passes", license.Enterprise, withEgress, false},
	}

	for _, tt := range tests {
		t.Run("replace/"+tt.name, func(t *testing.T) {
			t.Parallel()
			svc, _ := newEditionService(t, tt.edition)
			err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", []provisioning.TopicRoutingRule{tt.rule})
			if tt.wantGate {
				if !isFeatureError(err) {
					t.Fatalf("ReplaceRoutingRules = %v, want EditionFeatureError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReplaceRoutingRules unexpected error: %v", err)
			}
		})
		t.Run("add/"+tt.name, func(t *testing.T) {
			t.Parallel()
			svc, _ := newEditionService(t, tt.edition)
			err := svc.AddRoutingRule(context.Background(), "acme-corp", tt.rule)
			if tt.wantGate {
				if !isFeatureError(err) {
					t.Fatalf("AddRoutingRule = %v, want EditionFeatureError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("AddRoutingRule unexpected error: %v", err)
			}
		})
	}
}

// TestEgressGate_ShapeValidationPrecedes pins the error-precedence contract the
// e2e battery relies on: a malformed rule (over the per-rule destination cap)
// returns the VALIDATION error on Community too — never the edition gate — so
// the `too many topics` probe keeps asserting 400 on every edition.
func TestEgressGate_ShapeValidationPrecedes(t *testing.T) {
	t.Parallel()
	svc, _ := newEditionService(t, license.Community)

	over := make([]string, 10) // 1 ingress + 10 egress > default cap 10
	for i := range over {
		over[i] = fmt.Sprintf("count-%d", i)
	}
	err := svc.ReplaceRoutingRules(context.Background(), "acme-corp", []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: over, Priority: 1},
	})
	if !errors.Is(err, provisioning.ErrTooManyTopics) {
		t.Fatalf("err = %v, want ErrTooManyTopics (validation before edition gate)", err)
	}
	if isFeatureError(err) {
		t.Fatalf("err = %v — edition gate fired before shape validation", err)
	}
}

// TestAddRoutingRule_CrossRuleEgressDisjoint pins the bidirectional tenant-wide
// invariant on the incremental write path (ADR-0018): a new rule's egress topic
// must not be an existing rule's ingress topic, and a new rule's ingress topic
// must not be an existing rule's egress topic — either overlap would pull an
// egress topic into the consume set and deliver its copies.
func TestAddRoutingRule_CrossRuleEgressDisjoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing provisioning.TopicRoutingRule
		add      provisioning.TopicRoutingRule
		wantErr  bool
	}{
		{
			name:     "new egress hits existing ingress",
			existing: provisioning.TopicRoutingRule{Pattern: "**.order", IngressTopic: "orders", Priority: 1},
			add:      provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: []string{"orders"}, Priority: 2},
			wantErr:  true,
		},
		{
			name:     "new ingress hits existing egress",
			existing: provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: []string{"orders"}, Priority: 1},
			add:      provisioning.TopicRoutingRule{Pattern: "**.order", IngressTopic: "orders", Priority: 2},
			wantErr:  true,
		},
		{
			name:     "disjoint rules pass",
			existing: provisioning.TopicRoutingRule{Pattern: "**.trade", IngressTopic: "trades", EgressTopics: []string{"audit"}, Priority: 1},
			add:      provisioning.TopicRoutingRule{Pattern: "**.order", IngressTopic: "orders", EgressTopics: []string{"audit"}, Priority: 2},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, store := newEditionService(t, license.Enterprise)
			tenantID := seededTenantUUID(t, svc)
			store.Seed(tenantID, []provisioning.TopicRoutingRule{tt.existing})

			err := svc.AddRoutingRule(context.Background(), "acme-corp", tt.add)
			if tt.wantErr && !errors.Is(err, provisioning.ErrEgressIngressOverlap) {
				t.Fatalf("AddRoutingRule = %v, want ErrEgressIngressOverlap", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("AddRoutingRule unexpected error: %v", err)
			}
		})
	}
}

// seededTenantUUID resolves the seeded tenant's UUID (stores key rules by UUID).
func seededTenantUUID(t *testing.T, svc *provisioning.Service) string {
	t.Helper()
	tenant, err := svc.GetTenantBySlug(context.Background(), "acme-corp")
	if err != nil {
		t.Fatalf("GetTenantBySlug: %v", err)
	}
	return tenant.ID
}
