package provisioning_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// newRenameService creates a Service wired for rename tests.
// clock overrides the internal time source (nil = real time.Now).
// holdPeriod sets SlugRenameTopicHoldPeriod (0 = uses 168h default-equivalent, but callers should pass explicitly).
func newRenameService(clock func() time.Time, holdPeriod time.Duration) (
	*provisioning.Service,
	*testutil.MockTenantStore,
	*testutil.MockKafkaAdmin,
	*testutil.MockAuditStore,
	*testutil.MockQuotaStore,
) {
	tenantStore := testutil.NewMockTenantStore()
	kafkaAdmin := testutil.NewMockKafkaAdmin()
	auditStore := testutil.NewMockAuditStore()
	quotaStore := testutil.NewMockQuotaStore()

	if holdPeriod == 0 {
		holdPeriod = platform.MinSlugRenameHoldPeriod // 24h
	}

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  quotaStore,
		AuditStore:                  auditStore,
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		SlugRenameTopicHoldPeriod:   holdPeriod,
		Clock:                       clock,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newRenameService: " + err.Error())
	}
	return svc, tenantStore, kafkaAdmin, auditStore, quotaStore
}

func TestRenameTenant_HappyPath(t *testing.T) {
	t.Parallel()
	svc, tenantStore, kafka, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	updated, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("RenameTenant() error = %v", err)
	}
	if updated == nil {
		t.Fatal("expected non-nil updated tenant")
	}
	if updated.Slug != "new-corp" {
		t.Errorf("updated.Slug = %q, want %q", updated.Slug, "new-corp")
	}
	if updated.ID != tenant.ID {
		t.Errorf("updated.ID = %q, want %q (UUID must not change)", updated.ID, tenant.ID)
	}
	// New ACLs created, old ACLs deleted
	if len(kafka.CreateTopicACLsCalls) != 1 || kafka.CreateTopicACLsCalls[0] != "new-corp" {
		t.Errorf("CreateTopicACLsCalls = %v, want [new-corp]", kafka.CreateTopicACLsCalls)
	}
	if len(kafka.DeleteTopicACLsCalls) != 1 || kafka.DeleteTopicACLsCalls[0] != "acme-corp" {
		t.Errorf("DeleteTopicACLsCalls = %v, want [acme-corp]", kafka.DeleteTopicACLsCalls)
	}
}

func TestRenameTenant_SlugAlreadyTaken(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("taken-corp"))

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "taken-corp")
	if !errors.Is(err, provisioning.ErrSlugAlreadyTaken) {
		t.Errorf("error = %v, want ErrSlugAlreadyTaken", err)
	}
}

func TestRenameTenant_CASConflict_ReturnsRenameInProgress(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	// Simulate concurrent rename by putting store in CAS-fail state (pending state).
	tenantStore.SetRenameStateErr = provisioning.ErrCASFailed

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if !errors.Is(err, provisioning.ErrSlugRenameInProgress) {
		t.Errorf("error = %v, want ErrSlugRenameInProgress (CAS conflict must not expose ErrCASFailed)", err)
	}
}

func TestRenameTenant_WithinHoldPeriod_Blocked(t *testing.T) {
	t.Parallel()

	fixedNow := time.Now()
	renamedAt := fixedNow.Add(-12 * time.Hour) // 12 hours ago, hold is 24h

	svc, tenantStore, _, _, _ := newRenameService(
		func() time.Time { return fixedNow },
		24*time.Hour,
	)

	tenant := testutil.NewTestTenant("acme-corp")
	tenant.SlugRenameState = provisioning.SlugRenameStateComplete
	tenant.PreviousSlug = "old-corp"
	tenant.SlugRenamedAt = &renamedAt
	_ = tenantStore.Create(context.Background(), tenant)

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if !errors.Is(err, provisioning.ErrSlugRenameInProgress) {
		t.Errorf("error = %v, want ErrSlugRenameInProgress (rename within hold period)", err)
	}
}

func TestRenameTenant_SlugUnchanged(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "acme-corp")
	if !errors.Is(err, provisioning.ErrSlugUnchanged) {
		t.Errorf("error = %v, want ErrSlugUnchanged", err)
	}
}

func TestRenameTenant_SlugReserved(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "admin")
	if !errors.Is(err, provisioning.ErrSlugReserved) {
		t.Errorf("error = %v, want ErrSlugReserved", err)
	}
}

func TestRenameTenant_SlugInvalid(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("acme-corp"))

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "INVALID_SLUG")
	if !errors.Is(err, provisioning.ErrSlugInvalid) {
		t.Errorf("error = %v, want ErrSlugInvalid", err)
	}
}

func TestRenameTenant_TenantNotFound(t *testing.T) {
	t.Parallel()
	svc, _, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	_, err := svc.RenameTenant(context.Background(), "nonexistent", "new-corp")
	if err == nil {
		t.Error("expected error for nonexistent tenant, got nil")
	}
}

func TestRenameTenant_TenantNotActive(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	tenant.Status = provisioning.StatusSuspended
	_ = tenantStore.Create(context.Background(), tenant)

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if !errors.Is(err, provisioning.ErrTenantNotActive) {
		t.Errorf("error = %v, want ErrTenantNotActive", err)
	}
}

func TestRenameTenant_PostHoldPeriod_Allowed(t *testing.T) {
	t.Parallel()

	fixedNow := time.Now()
	renamedAt := fixedNow.Add(-48 * time.Hour) // 48h ago, hold is 24h → expired

	svc, tenantStore, kafka, _, _ := newRenameService(
		func() time.Time { return fixedNow },
		24*time.Hour,
	)

	tenant := testutil.NewTestTenant("acme-corp")
	tenant.SlugRenameState = provisioning.SlugRenameStateComplete
	tenant.PreviousSlug = "very-old-corp"
	tenant.SlugRenamedAt = &renamedAt
	_ = tenantStore.Create(context.Background(), tenant)

	updated, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("expected rename to succeed after hold period, got: %v", err)
	}
	if updated.Slug != "new-corp" {
		t.Errorf("Slug = %q, want new-corp", updated.Slug)
	}
	// Verify ACL lifecycle happened
	if len(kafka.CreateTopicACLsCalls) != 1 {
		t.Errorf("CreateTopicACLsCalls = %v, want 1 call", kafka.CreateTopicACLsCalls)
	}
}

func TestRenameTenant_LazyClear_HoldExpired(t *testing.T) {
	t.Parallel()

	fixedNow := time.Now()
	renamedAt := fixedNow.Add(-48 * time.Hour) // 48h ago, hold is 24h → expired

	svc, tenantStore, _, _, _ := newRenameService(
		func() time.Time { return fixedNow },
		24*time.Hour,
	)

	tenant := testutil.NewTestTenant("acme-corp")
	tenant.SlugRenameState = provisioning.SlugRenameStateComplete
	tenant.PreviousSlug = "old-corp"
	tenant.SlugRenamedAt = &renamedAt
	_ = tenantStore.Create(context.Background(), tenant)

	// Rename should succeed (hold expired), which means lazy clear ran.
	_, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v", err)
	}
}

func TestRenameTenant_AuditActor_WithAdminContext(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, auditStore, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	ctx := auth.WithActor(context.Background(), "kid-admin-123", provisioning.ActorTypeAdmin, "")
	_, err := svc.RenameTenant(ctx, "acme-corp", "renamed-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v", err)
	}

	entries := auditStore.GetEntries()
	var renameEntry *provisioning.AuditEntry
	for _, e := range entries {
		if e.Action == provisioning.ActionRenameTenant {
			renameEntry = e
			break
		}
	}
	if renameEntry == nil {
		t.Fatal("expected audit entry for ActionRenameTenant, got none")
	}
	if renameEntry.Actor != "kid-admin-123" {
		t.Errorf("Actor = %q, want %q", renameEntry.Actor, "kid-admin-123")
	}
	if renameEntry.ActorType != provisioning.ActorTypeAdmin {
		t.Errorf("ActorType = %q, want %q", renameEntry.ActorType, provisioning.ActorTypeAdmin)
	}
}

func TestRenameTenant_AuditActor_NoContextActor(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, auditStore, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "renamed-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v", err)
	}

	entries := auditStore.GetEntries()
	var renameEntry *provisioning.AuditEntry
	for _, e := range entries {
		if e.Action == provisioning.ActionRenameTenant {
			renameEntry = e
			break
		}
	}
	if renameEntry == nil {
		t.Fatal("expected audit entry for ActionRenameTenant, got none")
	}
	if renameEntry.Actor != auth.DefaultActor {
		t.Errorf("Actor = %q, want %q (default)", renameEntry.Actor, auth.DefaultActor)
	}
	if renameEntry.ActorType != provisioning.ActorTypeSystem {
		t.Errorf("ActorType = %q, want %q", renameEntry.ActorType, provisioning.ActorTypeSystem)
	}
}

func TestRenameTenant_QuotaFetchError_SetQuotaSkipped(t *testing.T) {
	t.Parallel()
	svc, tenantStore, kafka, _, quotaStore := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	quotaStore.GetErr = errors.New("quota db unavailable")

	// Rename should still succeed (quota failure is non-fatal).
	updated, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v (quota error must not fail rename)", err)
	}
	if updated.Slug != "new-corp" {
		t.Errorf("Slug = %q, want new-corp", updated.Slug)
	}
	// SetQuota must NOT have been called when quota fetch fails.
	if len(kafka.SetQuotaCalls) != 0 {
		t.Errorf("SetQuotaCalls = %v, want empty (SetQuota must not be called when quota fetch fails)", kafka.SetQuotaCalls)
	}
}

func TestRenameTenant_SetQuotaError_RenameSucceeds(t *testing.T) {
	t.Parallel()
	svc, tenantStore, kafka, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	kafka.SetQuotaErr = errors.New("kafka quota error")

	// Rename should still succeed (SetQuota failure is non-fatal).
	updated, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v (SetQuota error must not fail rename)", err)
	}
	if updated.Slug != "new-corp" {
		t.Errorf("Slug = %q, want new-corp", updated.Slug)
	}
}

func TestRenameTenant_DeleteQuotaError_SetQuotaStillAttempted(t *testing.T) {
	t.Parallel()
	svc, tenantStore, kafka, _, quotaStore := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	// Pre-populate quota so quotas.Get succeeds and the DeleteQuota/SetQuota path is reached.
	_ = quotaStore.Create(context.Background(), &provisioning.TenantQuota{TenantID: tenant.ID})
	kafka.DeleteQuotaErr = errors.New("kafka delete quota error")

	// Rename should succeed and SetQuota still attempted despite DeleteQuota error.
	updated, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if err != nil {
		t.Fatalf("RenameTenant: %v (DeleteQuota error must not fail rename)", err)
	}
	if updated.Slug != "new-corp" {
		t.Errorf("Slug = %q, want new-corp", updated.Slug)
	}
	// SetQuota MUST still have been called for the new slug.
	if len(kafka.SetQuotaCalls) != 1 || kafka.SetQuotaCalls[0] != "new-corp" {
		t.Errorf("SetQuotaCalls = %v, want [new-corp] (SetQuota must be attempted despite DeleteQuota failure)", kafka.SetQuotaCalls)
	}
}

func TestRenameTenant_TOCTOU_CompensationExecuted(t *testing.T) {
	t.Parallel()
	svc, tenantStore, kafka, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	tenant := testutil.NewTestTenant("acme-corp")
	_ = tenantStore.Create(context.Background(), tenant)
	tenantStore.UpdateSlugErr = provisioning.ErrSlugAlreadyTaken // simulate 23505 race

	_, err := svc.RenameTenant(context.Background(), "acme-corp", "new-corp")
	if !errors.Is(err, provisioning.ErrSlugAlreadyTaken) {
		t.Errorf("error = %v, want ErrSlugAlreadyTaken (TOCTOU must return slug taken)", err)
	}
	// Compensation: DeleteTopicACLs for the new slug must have been called.
	found := false
	for _, s := range kafka.DeleteTopicACLsCalls {
		if s == "new-corp" {
			found = true
		}
	}
	if !found {
		t.Errorf("DeleteTopicACLsCalls = %v, want to contain 'new-corp' (TOCTOU compensation)", kafka.DeleteTopicACLsCalls)
	}
	// Compensation: infra topics for the new slug must have been deleted.
	for _, topic := range []string{"test.new-corp.dead-letter", "test.new-corp.default"} {
		topicFound := slices.Contains(kafka.DeleteAttempts, topic)
		if !topicFound {
			t.Errorf("DeleteAttempts = %v, want to contain %q (TOCTOU compensation)", kafka.DeleteAttempts, topic)
		}
	}
}

func TestIsReservedSlug(t *testing.T) {
	t.Parallel()
	svc, tenantStore, _, _, _ := newRenameService(nil, platform.MinSlugRenameHoldPeriod)

	// All these must fail RenameTenant with ErrSlugReserved.
	reserved := []string{
		"admin", "api", "health", "ready", "metrics", "version", "edition",
		"config", "debug", "internal", "system", "sukko", "keys", "api-keys",
		"routing-rules", "quotas", "audit", "channel-rules", "tokens",
		"suspend", "reactivate", "rename", "test-access",
	}

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("base-tenant"))

	for _, slug := range reserved {
		t.Run(slug, func(t *testing.T) {
			t.Parallel()
			_, err := svc.RenameTenant(context.Background(), "base-tenant", slug)
			if !errors.Is(err, provisioning.ErrSlugReserved) {
				t.Errorf("RenameTenant(%q) = %v, want ErrSlugReserved", slug, err)
			}
		})
	}
}
