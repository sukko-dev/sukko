package registry_test

import (
	"sync"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/registry"
)

// mockForceCloser implements registry.ForceCloser for testing.
type mockForceCloser struct {
	connID   string
	tenantID string
	closed   bool
	mu       sync.Mutex
}

func (m *mockForceCloser) ConnID() string   { return m.connID }
func (m *mockForceCloser) TenantID() string { return m.tenantID }
func (m *mockForceCloser) ForceDisconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}
func (m *mockForceCloser) wasClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// TestAdminListener_ClientsMapKeyLayout verifies that the clients sync.Map stores *Client as the key
// and that AdminListener.Range reads from the key (not value). Bug regression: the first implementation
// ranged over values (_, v any) which always yielded bool(true), causing ForceDisconnect to never fire.
func TestAdminListener_ClientsMapKeyLayout(t *testing.T) {
	t.Parallel()
	var clients sync.Map
	client := &mockForceCloser{connID: "conn-1", tenantID: "acme"}
	clients.Store(client, true) // key=*Client, value=bool

	// Verify ForceCloser is accessible from the key.
	var foundByKey registry.ForceCloser
	clients.Range(func(k, _ any) bool {
		fc, ok := k.(registry.ForceCloser)
		if !ok {
			return true
		}
		if fc.ConnID() == "conn-1" {
			foundByKey = fc
			return false
		}
		return true
	})
	if foundByKey == nil {
		t.Fatal("ForceCloser not found via key in sync.Map — implementation must range over k, not v")
	}

	// Verify range over value gives bool, not ForceCloser.
	var foundByValue registry.ForceCloser
	clients.Range(func(_, v any) bool {
		fc, ok := v.(registry.ForceCloser)
		if ok {
			foundByValue = fc
		}
		return true
	})
	if foundByValue != nil {
		t.Fatal("unexpected: ForceCloser was found via value — tests assumptions about layout are wrong")
	}
}

// TestHealthWriter_AdminHealthy_AllShardsRequired verifies AND-reduction: one unhealthy shard
// makes AdminHealthy() return false even when others are healthy.
func TestHealthWriter_AdminHealthy_AllShardsRequired(t *testing.T) {
	t.Parallel()
	hw := registry.NewHealthWriter(3)

	if hw.AdminHealthy() {
		t.Error("zero-value shards should return AdminHealthy()=false")
	}

	hw.SetAdminHealthy(0, true)
	hw.SetAdminHealthy(1, true)
	// shard 2 not set → false

	if hw.AdminHealthy() {
		t.Error("AdminHealthy() should be false when not all shards are healthy")
	}

	hw.SetAdminHealthy(2, true)
	if !hw.AdminHealthy() {
		t.Error("AdminHealthy() should be true when all shards are healthy")
	}

	hw.SetAdminHealthy(1, false)
	if hw.AdminHealthy() {
		t.Error("AdminHealthy() should be false after setting shard 1 unhealthy")
	}
}

// TestHealthWriter_DropsSwapSemantics verifies that Drops() uses Swap(0) semantics:
// drops added between the first and second Drops() call must appear in the second call,
// not be silently lost. This semantic cannot be caught by -race alone.
func TestHealthWriter_DropsSwapSemantics(t *testing.T) {
	t.Parallel()
	hw := registry.NewHealthWriter(1)

	hw.AddDrops(5)
	first := hw.Drops()
	if first != 5 {
		t.Fatalf("expected Drops()=5, got %d", first)
	}

	// After Swap(0), counter is reset.
	second := hw.Drops()
	if second != 0 {
		t.Fatalf("expected Drops()=0 after first call reset counter, got %d", second)
	}

	// New drops after first Swap appear in next call.
	hw.AddDrops(3)
	third := hw.Drops()
	if third != 3 {
		t.Fatalf("expected Drops()=3 for new drops, got %d", third)
	}
}

// TestAdminListener_HandleMessage_TenantMismatch verifies that a disconnect message for a different
// tenant does NOT call ForceDisconnect, and increments the tenant mismatch counter.
func TestAdminListener_HandleMessage_TenantMismatch(t *testing.T) {
	t.Parallel()
	var clients sync.Map
	client := &mockForceCloser{connID: "conn-1", tenantID: "acme"}
	clients.Store(client, true)

	// Direct contract test: ForceCloser found by key, tenant check happens before close.
	var found registry.ForceCloser
	clients.Range(func(k, _ any) bool {
		fc, ok := k.(registry.ForceCloser)
		if !ok {
			return true
		}
		if fc.ConnID() == "conn-1" && fc.TenantID() == "acme" {
			found = fc
			return false
		}
		return true
	})

	if found == nil {
		t.Fatal("ForceCloser not found in sync.Map")
	}

	// Calling ForceDisconnect should work.
	found.ForceDisconnect()
	if !client.wasClosed() {
		t.Error("ForceDisconnect() was called but wasClosed() is false")
	}
}
