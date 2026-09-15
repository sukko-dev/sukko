package gateway

import (
	"sync"
	"testing"
)

func TestTenantConnectionTracker_TryAcquireRelease(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(10)

	// Acquire a connection
	if !tracker.TryAcquire("tenant1") {
		t.Error("expected TryAcquire to succeed")
	}

	// Check count
	if got := tracker.GetConnectionCount("tenant1"); got != 1 {
		t.Errorf("expected count 1, got %d", got)
	}

	// Release
	tracker.Release("tenant1")

	// Check count after release
	if got := tracker.GetConnectionCount("tenant1"); got != 0 {
		t.Errorf("expected count 0 after release, got %d", got)
	}
}

func TestTenantConnectionTracker_LimitEnforcement(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(3) // Low limit for testing

	// Acquire up to limit
	for i := range 3 {
		if !tracker.TryAcquire("tenant1") {
			t.Errorf("expected TryAcquire to succeed for connection %d", i+1)
		}
	}

	// Next acquisition should fail
	if tracker.TryAcquire("tenant1") {
		t.Error("expected TryAcquire to fail when limit reached")
	}

	// Release one and try again
	tracker.Release("tenant1")

	if !tracker.TryAcquire("tenant1") {
		t.Error("expected TryAcquire to succeed after release")
	}
}

func TestTenantConnectionTracker_PerTenantLimits(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(100) // High default

	// Set low limit for specific tenant
	tracker.SetLimit("limited_tenant", 2)

	// Acquire up to limit
	if !tracker.TryAcquire("limited_tenant") {
		t.Error("expected first TryAcquire to succeed")
	}
	if !tracker.TryAcquire("limited_tenant") {
		t.Error("expected second TryAcquire to succeed")
	}

	// Third should fail
	if tracker.TryAcquire("limited_tenant") {
		t.Error("expected TryAcquire to fail at tenant-specific limit")
	}

	// Other tenant should still work (uses default)
	if !tracker.TryAcquire("other_tenant") {
		t.Error("expected other tenant to succeed")
	}
}

func TestTenantConnectionTracker_Concurrent(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(1000)
	tenantID := "concurrent_tenant"

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent acquisitions
	for range numGoroutines {
		wg.Go(func() {
			if tracker.TryAcquire(tenantID) {
				defer tracker.Release(tenantID)
				// Simulate some work
			}
		})
	}

	wg.Wait()

	// After all goroutines complete, count should be 0
	if got := tracker.GetConnectionCount(tenantID); got != 0 {
		t.Errorf("expected count 0 after all releases, got %d", got)
	}
}

func TestTenantConnectionTracker_GetAllCounts(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(100)

	tracker.TryAcquire("tenant1")
	tracker.TryAcquire("tenant1")
	tracker.TryAcquire("tenant2")

	counts := tracker.GetAllCounts()

	if counts["tenant1"] != 2 {
		t.Errorf("expected tenant1 count 2, got %d", counts["tenant1"])
	}
	if counts["tenant2"] != 1 {
		t.Errorf("expected tenant2 count 1, got %d", counts["tenant2"])
	}
}

func TestTenantConnectionTracker_GetLimit(t *testing.T) {
	t.Parallel()
	tracker := NewTenantConnectionTracker(500)

	// Default limit
	if got := tracker.GetLimit("any_tenant"); got != 500 {
		t.Errorf("expected default limit 500, got %d", got)
	}

	// Set custom limit
	tracker.SetLimit("custom_tenant", 100)
	if got := tracker.GetLimit("custom_tenant"); got != 100 {
		t.Errorf("expected custom limit 100, got %d", got)
	}

	// Clear custom limit
	tracker.SetLimit("custom_tenant", 0)
	if got := tracker.GetLimit("custom_tenant"); got != 500 {
		t.Errorf("expected default limit after clear, got %d", got)
	}
}
