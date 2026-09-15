package analytics

import (
	"context"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// TestPartitionManager_RunMaintenance verifies partition creation and that
// RunMaintenance is idempotent (calling twice does not error).
// Uses testcontainers for a real Postgres instance with the analytics schema.
func TestPartitionManager_RunMaintenance(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// Run the analytics migrations first.
	// testutil.NewTestPool already runs all migrations including 002_analytics_partition.sql

	pm := NewPartitionManager(pool, noopLogger())

	// First call — creates T+1 and T+2 day partitions.
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance (first call): %v", err)
	}

	// Second call — idempotent: CREATE TABLE IF NOT EXISTS must not error.
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance (second call, idempotency): %v", err)
	}

	// Verify named partitions were created for analytics_connections.
	tomorrow := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)
	partName := tableConnections + "_" + tomorrow.Format("20060102")
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_class WHERE relname = $1`, partName,
	).Scan(&count); err != nil {
		t.Fatalf("query pg_class for partition %s: %v", partName, err)
	}
	if count == 0 {
		t.Errorf("expected partition %s to exist after RunMaintenance, got count=0", partName)
	}
}

// TestPartitionManager_DropExpired verifies that partitions older than retention
// are dropped. Creates a fake old partition and checks it is removed.
func TestPartitionManager_DropExpired(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// testutil.NewTestPool already runs all migrations including 002_analytics_partition.sql

	pm := NewPartitionManager(pool, noopLogger())

	// Create a very old daily partition manually — 5 days ago (> 2-day retention).
	oldDate := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -5)
	oldName := tableConnections + "_" + oldDate.Format("20060102")
	from := oldDate.Format("2006-01-02")
	to := oldDate.AddDate(0, 0, 1).Format("2006-01-02")
	if _, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+oldName+
		" PARTITION OF analytics_connections FOR VALUES FROM ('"+from+"') TO ('"+to+"')"); err != nil {
		t.Fatalf("create old partition: %v", err)
	}

	// RunMaintenance should drop it.
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_class WHERE relname = $1`, oldName,
	).Scan(&count); err != nil {
		t.Fatalf("query pg_class: %v", err)
	}
	if count != 0 {
		t.Errorf("expected partition %s to be dropped, still exists", oldName)
	}
}
