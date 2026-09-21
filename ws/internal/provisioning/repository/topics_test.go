package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// newTopicsFixture spins up a containerised Postgres (migrations applied), seeds
// a tenant row (topics.tenant_id has a FK → tenants.id), and returns a wired
// TopicRepository plus the pool (for seeding topic rows directly — slice 1 has
// no Create; rows arrive via the migration backfill or the slice-2 API).
func newTopicsFixture(t *testing.T, slug string) (*repository.TopicRepository, *pgxpool.Pool, string, context.Context) {
	t.Helper()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	tenantUUID := uuid.New().String()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata)
		 VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
		tenantUUID, slug, slug+"-name",
	); err != nil {
		t.Fatalf("seed tenant row: %v", err)
	}
	return repository.NewTopicRepository(pool), pool, tenantUUID, ctx
}

func TestTopicRepository_Exists_NotProvisioned(t *testing.T) {
	t.Parallel()
	repo, _, tenantID, ctx := newTopicsFixture(t, "topics-absent")

	exists, err := repo.Exists(ctx, tenantID, "analytics")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected Exists=false for an unprovisioned topic")
	}
}

func TestTopicRepository_Exists_Provisioned(t *testing.T) {
	t.Parallel()
	repo, pool, tenantID, ctx := newTopicsFixture(t, "topics-present")

	if _, err := pool.Exec(ctx,
		`INSERT INTO topics (tenant_id, suffix) VALUES ($1, $2)`, tenantID, "analytics"); err != nil {
		t.Fatalf("seed topic row: %v", err)
	}

	exists, err := repo.Exists(ctx, tenantID, "analytics")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected Exists=true for a provisioned topic")
	}
}

// TestTopicRepository_Exists_TenantScoped pins isolation: a topic provisioned for
// one tenant must not be visible to another (Constitution §IX tenant isolation).
func TestTopicRepository_Exists_TenantScoped(t *testing.T) {
	t.Parallel()
	repo, pool, tenantA, ctx := newTopicsFixture(t, "topics-tenant-a")

	tenantB := uuid.New().String()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata)
		 VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
		tenantB, "topics-tenant-b", "topics-tenant-b-name",
	); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO topics (tenant_id, suffix) VALUES ($1, $2)`, tenantA, "shared-name"); err != nil {
		t.Fatalf("seed topic for tenant A: %v", err)
	}

	// Same suffix, different tenant → must be invisible.
	exists, err := repo.Exists(ctx, tenantB, "shared-name")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("tenant B must not see tenant A's topic (isolation)")
	}
}

func TestTopicRepository_Create_ListCountDelete(t *testing.T) {
	t.Parallel()
	repo, _, tenantID, ctx := newTopicsFixture(t, "topics-crud")

	if n, err := repo.Count(ctx, tenantID); err != nil || n != 0 {
		t.Fatalf("initial Count = %d, %v; want 0, nil", n, err)
	}

	ca, err := repo.Create(ctx, tenantID, "analytics")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ca.IsZero() {
		t.Error("Create returned zero created_at")
	}
	if _, err := repo.Create(ctx, tenantID, "audit"); err != nil {
		t.Fatalf("Create audit: %v", err)
	}

	got, err := repo.List(ctx, tenantID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].Suffix != "analytics" || got[1].Suffix != "audit" {
		t.Errorf("List = %+v, want [analytics, audit] ordered", got)
	}
	if n, _ := repo.Count(ctx, tenantID); n != 2 {
		t.Errorf("Count = %d, want 2", n)
	}

	if err := repo.Delete(ctx, tenantID, "analytics"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n, _ := repo.Count(ctx, tenantID); n != 1 {
		t.Errorf("Count after delete = %d, want 1", n)
	}
}

func TestTopicRepository_Create_DuplicateRejected(t *testing.T) {
	t.Parallel()
	repo, _, tenantID, ctx := newTopicsFixture(t, "topics-dup")
	if _, err := repo.Create(ctx, tenantID, "analytics"); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := repo.Create(ctx, tenantID, "analytics")
	if !errors.Is(err, provisioning.ErrTopicAlreadyExists) {
		t.Errorf("duplicate Create err = %v, want ErrTopicAlreadyExists", err)
	}
}

func TestTopicRepository_Delete_NotFound(t *testing.T) {
	t.Parallel()
	repo, _, tenantID, ctx := newTopicsFixture(t, "topics-del-absent")
	err := repo.Delete(ctx, tenantID, "ghost")
	if !errors.Is(err, provisioning.ErrTopicNotFound) {
		t.Errorf("Delete absent err = %v, want ErrTopicNotFound", err)
	}
}
