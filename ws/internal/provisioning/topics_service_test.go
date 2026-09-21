package provisioning_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// newTopicsSvc builds a service wired for topics-API tests, returning the mock
// stores so tests can configure them. maxTopics sets the per-tenant quota.
func newTopicsSvc(t *testing.T, maxTopics int) (*provisioning.Service, *testutil.MockTopicStore, *testutil.MockRoutingRulesStore, *testutil.MockTenantStore) {
	t.Helper()
	topics := testutil.NewMockTopicStore()
	rr := testutil.NewMockRoutingRulesStore()
	ts := testutil.NewMockTenantStore()
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 ts,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           rr,
		TopicStore:                  topics,
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          maxTopics,
		MaxRoutingRulesPerTenant:    100,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("newTopicsSvc: %v", err)
	}
	_ = ts.Create(context.Background(), testutil.NewTestTenant("acme"))
	return svc, topics, rr, ts
}

func TestCreateTopic_Success(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)

	topic, err := svc.CreateTopic(context.Background(), "acme", "analytics")
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if topic.Suffix != "analytics" {
		t.Errorf("suffix = %q, want analytics", topic.Suffix)
	}
}

func TestCreateTopic_ReservedAndInvalid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		suffix  string
		wantErr error
	}{
		{"default reserved", "default", provisioning.ErrReservedTopicSuffix},
		{"dead-letter reserved", "dead-letter", provisioning.ErrReservedTopicSuffix},
		{"empty invalid", "", provisioning.ErrInvalidTopicSuffix},
		{"uppercase invalid", "Analytics", provisioning.ErrInvalidTopicSuffix},
		{"dot invalid", "a.b", provisioning.ErrInvalidTopicSuffix},
		{"space invalid", "a b", provisioning.ErrInvalidTopicSuffix},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc, _, _, _ := newTopicsSvc(t, 50)
			_, err := svc.CreateTopic(context.Background(), "acme", tt.suffix)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateTopic(%q) err = %v, want %v", tt.suffix, err, tt.wantErr)
			}
		})
	}
}

func TestCreateTopic_QuotaExceeded(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 1) // quota of 1

	if _, err := svc.CreateTopic(context.Background(), "acme", "first"); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	_, err := svc.CreateTopic(context.Background(), "acme", "second")
	if !errors.Is(err, provisioning.ErrTopicQuotaExceeded) {
		t.Errorf("second CreateTopic err = %v, want ErrTopicQuotaExceeded", err)
	}
}

func TestCreateTopic_Duplicate(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)

	if _, err := svc.CreateTopic(context.Background(), "acme", "analytics"); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	_, err := svc.CreateTopic(context.Background(), "acme", "analytics")
	if !errors.Is(err, provisioning.ErrTopicAlreadyExists) {
		t.Errorf("duplicate CreateTopic err = %v, want ErrTopicAlreadyExists", err)
	}
}

func TestListTopics_PrependsDefault(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)
	_, _ = svc.CreateTopic(context.Background(), "acme", "analytics")

	topics, err := svc.ListTopics(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics) != 2 || topics[0].Suffix != "default" || topics[1].Suffix != "analytics" {
		t.Errorf("topics = %+v, want [default, analytics]", topics)
	}
}

func TestDeleteTopic_Reserved(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)
	for _, suffix := range []string{"default", "dead-letter"} {
		if err := svc.DeleteTopic(context.Background(), "acme", suffix); !errors.Is(err, provisioning.ErrReservedTopicSuffix) {
			t.Errorf("DeleteTopic(%q) err = %v, want ErrReservedTopicSuffix", suffix, err)
		}
	}
}

func TestDeleteTopic_ReferencedByRule(t *testing.T) {
	t.Parallel()
	svc, topics, rr, ts := newTopicsSvc(t, 50)
	tenant, _ := ts.GetBySlug(context.Background(), "acme")
	_, _ = topics.Create(context.Background(), tenant.ID, "trade")
	// A routing rule references "trade" as ingress → delete must be rejected.
	_ = rr.Add(context.Background(), tenant.ID, provisioning.TopicRoutingRule{
		Pattern: "**.trade", IngressTopic: "trade", Priority: 1,
	})

	err := svc.DeleteTopic(context.Background(), "acme", "trade")
	if !errors.Is(err, provisioning.ErrTopicReferencedByRule) {
		t.Errorf("DeleteTopic err = %v, want ErrTopicReferencedByRule", err)
	}
}

func TestDeleteTopic_NotFound(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)
	err := svc.DeleteTopic(context.Background(), "acme", "ghost")
	if !errors.Is(err, provisioning.ErrTopicNotFound) {
		t.Errorf("DeleteTopic err = %v, want ErrTopicNotFound", err)
	}
}

func TestDeleteTopic_Success(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := newTopicsSvc(t, 50)
	if _, err := svc.CreateTopic(context.Background(), "acme", "analytics"); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := svc.DeleteTopic(context.Background(), "acme", "analytics"); err != nil {
		t.Errorf("DeleteTopic: %v", err)
	}
}

// newTopicsSvcCustom builds a topics service with an explicit edition and an
// optionally-seeded per-tenant quota row, for the edition-cap and quota-row
// walls. seedQuotaMax < 0 leaves the quota row unseeded (falls back to config).
func newTopicsSvcCustom(t *testing.T, configMax int, edition license.Edition, seedQuotaMax int) (*provisioning.Service, *testutil.MockTopicStore) {
	t.Helper()
	topics := testutil.NewMockTopicStore()
	ts := testutil.NewMockTenantStore()
	quotas := testutil.NewMockQuotaStore()
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 ts,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		TopicStore:                  topics,
		QuotaStore:                  quotas,
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		EditionManager:              license.NewTestManager(edition),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          configMax,
		MaxRoutingRulesPerTenant:    100,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("newTopicsSvcCustom: %v", err)
	}
	tenant := testutil.NewTestTenant("acme")
	_ = ts.Create(context.Background(), tenant)
	if seedQuotaMax >= 0 {
		_ = quotas.Create(context.Background(), &provisioning.TenantQuota{TenantID: tenant.ID, MaxTopics: seedQuotaMax})
	}
	return svc, topics
}

// TestCreateTopic_EditionCapBindsAtLimit pins the checkCount off-by-one: a
// Community tenant (edition cap 10) must be able to create exactly 10 topics,
// with the 11th rejected — not 9 with the 10th rejected. Config is set high so
// the edition cap is the binding wall. Mutating CheckTopicsPerTenant(count) back
// to (count+1) makes the 10th create fail, so this test red-verifies the fix.
func TestCreateTopic_EditionCapBindsAtLimit(t *testing.T) {
	t.Parallel()
	svc, _ := newTopicsSvcCustom(t, 50, license.Community, -1) // config 50, edition cap 10

	for i := range 10 {
		if _, err := svc.CreateTopic(context.Background(), "acme", "t"+string(rune('a'+i))); err != nil {
			t.Fatalf("create #%d (within edition cap 10): %v", i+1, err)
		}
	}
	_, err := svc.CreateTopic(context.Background(), "acme", "eleventh")
	if err == nil {
		t.Fatal("11th create should exceed the Community edition cap of 10")
	}
	if _, ok := errors.AsType[*license.EditionLimitError](err); !ok {
		t.Errorf("11th create err = %v, want *license.EditionLimitError", err)
	}
}

// TestCreateTopic_QuotaRowBinds pins ADR-0006's "enforced through the existing
// seeded quota": a per-tenant quota row of 2 binds even though the global config
// allows 50 and the edition is unlimited — so PATCH /quotas max_topics is not a
// no-op. The 3rd create is rejected with ErrTopicQuotaExceeded.
func TestCreateTopic_QuotaRowBinds(t *testing.T) {
	t.Parallel()
	svc, _ := newTopicsSvcCustom(t, 50, license.Enterprise, 2) // config 50, quota row 2

	for _, s := range []string{"one", "two"} {
		if _, err := svc.CreateTopic(context.Background(), "acme", s); err != nil {
			t.Fatalf("create %q (within quota row 2): %v", s, err)
		}
	}
	_, err := svc.CreateTopic(context.Background(), "acme", "three")
	if !errors.Is(err, provisioning.ErrTopicQuotaExceeded) {
		t.Errorf("3rd create err = %v, want ErrTopicQuotaExceeded (quota row binds over config)", err)
	}
}
