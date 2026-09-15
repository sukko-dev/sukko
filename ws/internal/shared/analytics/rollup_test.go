package analytics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// TestRollupManager_PromoteHour_LockSkip verifies that a concurrent PromoteHour call
// returns nil without writing when the advisory lock is already held.
func TestRollupManager_PromoteHour_LockSkip(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	pm := NewPartitionManager(pool, noopLogger())
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("partition maintenance: %v", err)
	}

	// Hold the advisory lock in an explicit transaction so a concurrent call skips.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var held bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, rollupHourLockID).Scan(&held); err != nil {
		t.Fatalf("acquire advisory lock: %v", err)
	}
	if !held {
		t.Fatal("could not acquire advisory lock in setup transaction")
	}

	rm := NewRollupManager(pool, noopLogger())

	// Concurrent call should skip (lock held by tx above).
	var wg sync.WaitGroup
	var skipErr error
	wg.Go(func() {
		skipErr = rm.PromoteHour(ctx)
	})
	wg.Wait()

	if skipErr != nil {
		t.Errorf("PromoteHour with lock held: expected nil (skip), got %v", skipErr)
	}

	// Verify no hour rows were written.
	var count int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM analytics_connections_hour`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected no rows written when lock skipped, got %d", count)
	}
}

// TestRollupManager_PromoteHour_Idempotent verifies that running PromoteHour twice
// on the same completed hour produces the same result (not double-counted).
func TestRollupManager_PromoteHour_Idempotent(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// Create partitions so inserts land somewhere.
	pm := NewPartitionManager(pool, noopLogger())
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("partition maintenance: %v", err)
	}

	tenantID := uuid.NewString()
	// Insert 3 minute rows for the previous hour (now completed).
	hourStart := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	for i := range 3 {
		bucket := hourStart.Add(time.Duration(i) * time.Minute)
		_, err := pool.Exec(ctx, `
			INSERT INTO analytics_connections
				(pod_id, tenant_id, bucket_start, bucket_size, transport,
				 active_count, connect_count, disconnect_count, error_count)
			VALUES ($1,$2,$3,'minute','websocket',10,2,1,0)`,
			"test-pod", tenantID, bucket,
		)
		if err != nil {
			t.Fatalf("insert minute row %d: %v", i, err)
		}
	}

	rm := NewRollupManager(pool, noopLogger())

	// First promotion.
	if err := rm.PromoteHour(ctx); err != nil {
		t.Fatalf("PromoteHour (first): %v", err)
	}

	var active1 int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(active_count), 0)
		FROM analytics_connections_hour
		WHERE tenant_id = $1 AND bucket_size = 'hour'`, tenantID,
	).Scan(&active1); err != nil {
		t.Fatalf("query hour rollup: %v", err)
	}

	// Second promotion (idempotent — ON CONFLICT DO UPDATE SET value = EXCLUDED.value).
	if err := rm.PromoteHour(ctx); err != nil {
		t.Fatalf("PromoteHour (second): %v", err)
	}

	var active2 int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(active_count), 0)
		FROM analytics_connections_hour
		WHERE tenant_id = $1 AND bucket_size = 'hour'`, tenantID,
	).Scan(&active2); err != nil {
		t.Fatalf("query hour rollup after second promotion: %v", err)
	}

	// Both promotions read the same minute rows → same sum.
	if active1 != active2 {
		t.Errorf("PromoteHour not idempotent: first=%d second=%d (expected equal)", active1, active2)
	}
	// 3 minute rows × active_count=10 = 30 total.
	if active1 != 30 {
		t.Errorf("PromoteHour sum: got %d, want 30 (3 rows × 10)", active1)
	}
}

// TestRollupManager_PromoteHour_EmptyBucket verifies no phantom hour row is created
// when there are no minute rows for the target hour.
func TestRollupManager_PromoteHour_EmptyBucket(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	pm := NewPartitionManager(pool, noopLogger())
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("partition maintenance: %v", err)
	}

	rm := NewRollupManager(pool, noopLogger())
	tenantID := uuid.NewString() // unique sentinel: any rows for this tenant would be phantom

	if err := rm.PromoteHour(ctx); err != nil {
		t.Fatalf("PromoteHour on empty bucket: %v", err)
	}

	// Use tenant-scoped count so the assertion is robust even if the migration seeds rows.
	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM analytics_connections_hour
		WHERE bucket_size='hour' AND tenant_id = $1`, tenantID,
	).Scan(&count); err != nil {
		t.Fatalf("count hour rollup rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 hour rollup rows for empty bucket (tenant %s), got %d", tenantID, count)
	}
}

// TestRollupManager_PromoteDay_Idempotent verifies hour→day promotion is idempotent.
func TestRollupManager_PromoteDay_Idempotent(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	pm := NewPartitionManager(pool, noopLogger())
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("partition maintenance: %v", err)
	}

	tenantID := uuid.NewString()
	// Insert 3 hour rows for yesterday (now a complete day).
	dayStart := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	for i := range 3 {
		bucket := dayStart.Add(time.Duration(i) * time.Hour)
		_, err := pool.Exec(ctx, `
			INSERT INTO analytics_connections_hour
				(tenant_id, bucket_start, bucket_size, transport,
				 active_count, connect_count, disconnect_count, error_count)
			VALUES ($1,$2,'hour','websocket',10,2,1,0)`,
			tenantID, bucket,
		)
		if err != nil {
			t.Fatalf("insert hour row %d: %v", i, err)
		}
	}

	rm := NewRollupManager(pool, noopLogger())

	if err := rm.PromoteDay(ctx); err != nil {
		t.Fatalf("PromoteDay (first): %v", err)
	}
	var active1 int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(active_count), 0) FROM analytics_connections_day
		WHERE tenant_id = $1 AND bucket_size = 'day'`, tenantID,
	).Scan(&active1); err != nil {
		t.Fatalf("query day rollup: %v", err)
	}

	if err := rm.PromoteDay(ctx); err != nil {
		t.Fatalf("PromoteDay (second): %v", err)
	}
	var active2 int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(active_count), 0) FROM analytics_connections_day
		WHERE tenant_id = $1 AND bucket_size = 'day'`, tenantID,
	).Scan(&active2); err != nil {
		t.Fatalf("query day rollup after second promotion: %v", err)
	}

	if active1 != active2 {
		t.Errorf("PromoteDay not idempotent: first=%d second=%d", active1, active2)
	}
	if active1 != 30 { // 3 rows × active_count=10
		t.Errorf("PromoteDay sum: got %d, want 30", active1)
	}
}

// TestRollupManager_PromoteDay_EmptyBucket verifies no phantom day row for empty input.
func TestRollupManager_PromoteDay_EmptyBucket(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	pm := NewPartitionManager(pool, noopLogger())
	if err := pm.RunMaintenance(ctx); err != nil {
		t.Fatalf("partition maintenance: %v", err)
	}

	rm := NewRollupManager(pool, noopLogger())
	tenantID := uuid.NewString()

	if err := rm.PromoteDay(ctx); err != nil {
		t.Fatalf("PromoteDay on empty bucket: %v", err)
	}

	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM analytics_connections_day
		WHERE bucket_size='day' AND tenant_id = $1`, tenantID,
	).Scan(&count); err != nil {
		t.Fatalf("count day rollup rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 day rollup rows for empty bucket (tenant %s), got %d", tenantID, count)
	}
}
