package repository_test

import (
	"context"
	"testing"

	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

const (
	tenantUUID1 = "00000000-0000-0000-0000-000000000001"
	tenantUUID2 = "00000000-0000-0000-0000-000000000002"
)

func TestSubscriptionRepository_Create(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewSubscriptionRepository(pool)
	ctx := context.Background()

	sub := &repository.PushSubscription{
		TenantID:   tenantUUID1,
		Principal:  "user-123",
		Platform:   "web",
		Endpoint:   "https://fcm.googleapis.com/fcm/send/test",
		P256dhKey:  "BNcRdreALRFXTkOOUHK1EtK2wt...",
		AuthSecret: "tBHItJI5svbpC7rN3fA...",
		Channels:   []string{"test-tenant.trades", "test-tenant.balances"},
	}

	id, err := repo.Create(ctx, sub)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if id == 0 {
		t.Fatal("Create() returned zero ID")
	}
}

func TestSubscriptionRepository_FindByTenant(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewSubscriptionRepository(pool)
	ctx := context.Background()

	// Create two subscriptions for tenantUUID1
	for i := range 2 {
		sub := &repository.PushSubscription{
			TenantID:  tenantUUID1,
			Principal: "user-123",
			Platform:  "web",
			Endpoint:  "https://example.com/push/" + string(rune('a'+i)),
			Channels:  []string{"tenant-aaa.trades"},
		}
		if _, err := repo.Create(ctx, sub); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
	}

	// Create one for a different tenant
	other := &repository.PushSubscription{
		TenantID:  tenantUUID2,
		Principal: "user-456",
		Platform:  "android",
		Token:     "fcm-token-abc",
		Channels:  []string{"tenant-bbb.trades"},
	}
	if _, err := repo.Create(ctx, other); err != nil {
		t.Fatalf("Create() other error = %v", err)
	}

	subs, err := repo.FindByTenant(ctx, tenantUUID1)
	if err != nil {
		t.Fatalf("FindByTenant() error = %v", err)
	}
	if len(subs) != 2 {
		t.Errorf("FindByTenant() returned %d subscriptions, want 2", len(subs))
	}
	for _, s := range subs {
		if s.TenantID != tenantUUID1 {
			t.Errorf("subscription tenant = %q, want %q", s.TenantID, tenantUUID1)
		}
	}
}

func TestSubscriptionRepository_Delete(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewSubscriptionRepository(pool)
	ctx := context.Background()

	sub := &repository.PushSubscription{
		TenantID:  "00000000-0000-0000-0000-000000000003",
		Principal: "user-789",
		Platform:  "ios",
		Token:     "apns-token-xyz",
		Channels:  []string{"tenant-del.alerts"},
	}
	id, err := repo.Create(ctx, sub)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := repo.Delete(ctx, id, sub.TenantID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	subs, err := repo.FindByTenant(ctx, sub.TenantID)
	if err != nil {
		t.Fatalf("FindByTenant() after delete error = %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("FindByTenant() after delete returned %d, want 0", len(subs))
	}
}

func TestSubscriptionRepository_DeleteByToken(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewSubscriptionRepository(pool)
	ctx := context.Background()

	sub := &repository.PushSubscription{
		TenantID:  "00000000-0000-0000-0000-000000000004",
		Principal: "user-111",
		Platform:  "android",
		Token:     "fcm-token-delete-me",
		Channels:  []string{"tenant-dbt.updates"},
	}
	if _, err := repo.Create(ctx, sub); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := repo.DeleteByToken(ctx, sub.TenantID, "fcm-token-delete-me"); err != nil {
		t.Fatalf("DeleteByToken() error = %v", err)
	}

	subs, err := repo.FindByTenant(ctx, sub.TenantID)
	if err != nil {
		t.Fatalf("FindByTenant() after DeleteByToken error = %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("FindByTenant() after DeleteByToken returned %d, want 0", len(subs))
	}
}

func TestSubscriptionRepository_UpdateLastSuccess(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewSubscriptionRepository(pool)
	ctx := context.Background()

	sub := &repository.PushSubscription{
		TenantID:  "00000000-0000-0000-0000-000000000005",
		Principal: "user-222",
		Platform:  "web",
		Endpoint:  "https://example.com/push/success",
		Channels:  []string{"tenant-uls.events"},
	}
	id, err := repo.Create(ctx, sub)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := repo.UpdateLastSuccess(ctx, id); err != nil {
		t.Fatalf("UpdateLastSuccess() error = %v", err)
	}

	subs, err := repo.FindByTenant(ctx, sub.TenantID)
	if err != nil {
		t.Fatalf("FindByTenant() error = %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("FindByTenant() returned %d, want 1", len(subs))
	}
	if subs[0].LastSuccessAt == nil {
		t.Error("LastSuccessAt should be set after UpdateLastSuccess")
	}
}
