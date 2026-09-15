package repository_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

func TestTenantRepository_CreateAndGet(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewTenantRepository(pool)
	ctx := context.Background()

	tenant := &provisioning.Tenant{
		ID:           "test-tenant",
		Slug:         "test-tenant",
		Name:         "Test Tenant",
		Status:       provisioning.StatusActive,
		ConsumerType: provisioning.ConsumerShared,
		Metadata:     provisioning.Metadata{"env": "test"},
	}

	if err := repo.Create(ctx, tenant); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.GetBySlug(ctx, "test-tenant")
	if err != nil {
		t.Fatalf("GetBySlug() error = %v", err)
	}
	if got.Name != "Test Tenant" {
		t.Errorf("Name = %q, want %q", got.Name, "Test Tenant")
	}
	if got.Status != provisioning.StatusActive {
		t.Errorf("Status = %q, want %q", got.Status, provisioning.StatusActive)
	}
}

func TestTenantRepository_CreateDuplicate(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewTenantRepository(pool)
	ctx := context.Background()

	tenant := &provisioning.Tenant{
		ID:           "dup-tenant",
		Slug:         "dup-tenant",
		Name:         "Dup Tenant",
		Status:       provisioning.StatusActive,
		ConsumerType: provisioning.ConsumerShared,
	}

	if err := repo.Create(ctx, tenant); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}

	err := repo.Create(ctx, tenant)
	if err == nil {
		t.Fatal("second Create() should return error for duplicate ID")
	}
}

func TestTenantRepository_GetNotFound(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewTenantRepository(pool)
	ctx := context.Background()

	_, err := repo.GetBySlug(ctx, "nonexistent")
	if err == nil {
		t.Fatal("GetBySlug() should return error for nonexistent tenant")
	}
}

func TestTenantRepository_CountActive(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewTenantRepository(pool)
	ctx := context.Background()

	// Empty DB: should return 0, not error.
	n, err := repo.CountActive(ctx)
	if err != nil {
		t.Fatalf("CountActive() on empty DB error = %v", err)
	}
	if n != 0 {
		t.Errorf("CountActive() on empty DB = %d, want 0", n)
	}

	newTenant := func(slug string) *provisioning.Tenant {
		return &provisioning.Tenant{
			ID:           uuid.NewString(),
			Slug:         slug,
			Name:         slug,
			Status:       provisioning.StatusActive,
			ConsumerType: provisioning.ConsumerShared,
			Metadata:     provisioning.Metadata{},
		}
	}

	// Create 2 active tenants.
	for _, slug := range []string{"cnt-active-1", "cnt-active-2"} {
		if err := repo.Create(ctx, newTenant(slug)); err != nil {
			t.Fatalf("Create(%s) error = %v", slug, err)
		}
	}
	// Create 1 suspended tenant (must not be counted).
	susp := newTenant("cnt-susp-1")
	if err := repo.Create(ctx, susp); err != nil {
		t.Fatalf("Create(suspended) error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, susp.ID, provisioning.StatusSuspended); err != nil {
		t.Fatalf("UpdateStatus(suspended) error = %v", err)
	}

	n, err = repo.CountActive(ctx)
	if err != nil {
		t.Fatalf("CountActive() error = %v", err)
	}
	if n != 2 {
		t.Errorf("CountActive() = %d, want 2 (active only, suspended excluded)", n)
	}
}

// TestTenantRepository_UpdateStatusDeleted covers the deleted-transition guard.
// Regression for the force-delete leak: UpdateStatus(StatusDeleted) only allowed
// deprovisioning→deleted, so DELETE ?force=true on an ACTIVE tenant affected 0 rows,
// returned ErrTenantNotFound, and the tenant silently kept counting against the
// edition MaxTenants limit (surfaced by the Community/direct e2e cell as
// EDITION_LIMIT_TENANTS 3/3 after three throwaway-tenant suites).
func TestTenantRepository_UpdateStatusDeleted(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewTenantRepository(pool)
	ctx := context.Background()

	// walk moves a fresh active tenant to the desired source status.
	walk := map[provisioning.TenantStatus][]provisioning.TenantStatus{
		provisioning.StatusActive:         {},
		provisioning.StatusSuspended:      {provisioning.StatusSuspended},
		provisioning.StatusDeprovisioning: {provisioning.StatusDeprovisioning},
		provisioning.StatusDeleted:        {provisioning.StatusDeprovisioning, provisioning.StatusDeleted},
	}

	tests := []struct {
		name    string
		from    provisioning.TenantStatus
		wantErr bool
	}{
		{name: "active to deleted (force-delete)", from: provisioning.StatusActive, wantErr: false},
		{name: "suspended to deleted (force-delete)", from: provisioning.StatusSuspended, wantErr: false},
		{name: "deprovisioning to deleted (sweeper)", from: provisioning.StatusDeprovisioning, wantErr: false},
		{name: "deleted to deleted rejected (no double delete)", from: provisioning.StatusDeleted, wantErr: true},
	}

	for i, tt := range tests { //nolint:paralleltest // sub-tests share a live DB fixture (and the final Count assertion needs all deletes applied); t.Parallel() on shared resources is forbidden by Constitution VIII
		t.Run(tt.name, func(t *testing.T) {
			tenant := &provisioning.Tenant{
				ID:           uuid.NewString(),
				Slug:         fmt.Sprintf("del-%d", i),
				Name:         tt.name,
				Status:       provisioning.StatusActive,
				ConsumerType: provisioning.ConsumerShared,
				Metadata:     provisioning.Metadata{},
			}
			if err := repo.Create(ctx, tenant); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			for _, step := range walk[tt.from] {
				if err := repo.UpdateStatus(ctx, tenant.ID, step); err != nil {
					t.Fatalf("walk to %s: UpdateStatus(%s) error = %v", tt.from, step, err)
				}
			}

			err := repo.UpdateStatus(ctx, tenant.ID, provisioning.StatusDeleted)
			if tt.wantErr {
				if !errors.Is(err, provisioning.ErrTenantNotFound) {
					t.Fatalf("UpdateStatus(deleted) from %s: error = %v, want ErrTenantNotFound", tt.from, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateStatus(deleted) from %s: error = %v, want nil", tt.from, err)
			}

			// Deleted tenants are invisible to slug lookups (GetBySlug filters
			// status='deleted') — not-found here proves the row actually flipped.
			if _, err := repo.GetBySlug(ctx, tenant.Slug); !errors.Is(err, provisioning.ErrTenantNotFound) {
				t.Errorf("GetBySlug() after delete: error = %v, want ErrTenantNotFound", err)
			}
		})
	}

	// The leak symptom: deleted tenants must not count against the edition tenant limit.
	n, err := repo.Count(ctx)
	if err != nil {
		t.Fatalf("Count() error = %v", err)
	}
	if n != 0 {
		t.Errorf("Count() after deleting all tenants = %d, want 0 (deleted tenants must free MaxTenants capacity)", n)
	}
}
