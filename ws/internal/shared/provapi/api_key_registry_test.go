package provapi

import (
	"testing"

	"github.com/rs/zerolog"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
)

// newTestAPIKeyRegistry creates a minimal StreamAPIKeyRegistry for unit testing
// without gRPC connections or Prometheus metrics.
func newTestAPIKeyRegistry() *StreamAPIKeyRegistry {
	return &StreamAPIKeyRegistry{
		keysByID: make(map[string]*APIKeyInfo),
		logger:   zerolog.Nop(),
	}
}

func TestStreamAPIKeyRegistry_Lookup_AfterSnapshot(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_abc", TenantUuid: "tenant1", Name: "production", IsActive: true},
			{KeyId: "pk_live_def", TenantUuid: "tenant2", Name: "staging", IsActive: true},
		},
	})

	t.Run("first key found", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_abc")
		if !ok {
			t.Fatal("Lookup(pk_live_abc) returned false, want true")
		}
		if info.KeyID != "pk_live_abc" {
			t.Errorf("KeyID = %q, want %q", info.KeyID, "pk_live_abc")
		}
		if info.TenantID != "tenant1" {
			t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant1")
		}
		if info.Name != "production" {
			t.Errorf("Name = %q, want %q", info.Name, "production")
		}
		if !info.IsActive {
			t.Error("IsActive = false, want true")
		}
	})

	t.Run("second key found", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_def")
		if !ok {
			t.Fatal("Lookup(pk_live_def) returned false, want true")
		}
		if info.KeyID != "pk_live_def" {
			t.Errorf("KeyID = %q, want %q", info.KeyID, "pk_live_def")
		}
		if info.TenantID != "tenant2" {
			t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant2")
		}
		if info.Name != "staging" {
			t.Errorf("Name = %q, want %q", info.Name, "staging")
		}
	})
}

func TestStreamAPIKeyRegistry_Lookup_AfterDelta(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	// Initial snapshot with one key.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_abc", TenantUuid: "tenant1", Name: "first", IsActive: true},
		},
	})

	// Delta adds a second key.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: false,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_xyz", TenantUuid: "tenant2", Name: "second", IsActive: true},
		},
	})

	t.Run("original key still exists", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_abc")
		if !ok {
			t.Fatal("Lookup(pk_live_abc) returned false, want true")
		}
		if info.TenantID != "tenant1" {
			t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant1")
		}
	})

	t.Run("delta-added key exists", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_xyz")
		if !ok {
			t.Fatal("Lookup(pk_live_xyz) returned false, want true")
		}
		if info.TenantID != "tenant2" {
			t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant2")
		}
		if info.Name != "second" {
			t.Errorf("Name = %q, want %q", info.Name, "second")
		}
	})
}

func TestStreamAPIKeyRegistry_Lookup_UnknownKey(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	info, ok := r.Lookup("nonexistent")
	if ok {
		t.Error("Lookup(nonexistent) returned true, want false")
	}
	if info != nil {
		t.Errorf("Lookup(nonexistent) returned %+v, want nil", info)
	}
}

func TestStreamAPIKeyRegistry_Lookup_InactiveKey(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_inactive", TenantUuid: "tenant1", Name: "disabled", IsActive: false},
		},
	})

	info, ok := r.Lookup("pk_live_inactive")
	if ok {
		t.Error("Lookup(inactive key) returned true, want false")
	}
	if info != nil {
		t.Errorf("Lookup(inactive key) returned %+v, want nil", info)
	}
}

func TestStreamAPIKeyRegistry_UpdateKeys_SnapshotReplacesAll(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	// First snapshot with key A.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_aaa", TenantUuid: "tenant1", Name: "key-a", IsActive: true},
		},
	})

	// Verify key A exists before replacement.
	if _, ok := r.Lookup("pk_live_aaa"); !ok {
		t.Fatal("pk_live_aaa should exist after first snapshot")
	}

	// Second snapshot with only key B — should fully replace.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_bbb", TenantUuid: "tenant2", Name: "key-b", IsActive: true},
		},
	})

	t.Run("old key removed", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_aaa")
		if ok {
			t.Error("pk_live_aaa should not exist after second snapshot")
		}
		if info != nil {
			t.Errorf("expected nil for replaced key, got %+v", info)
		}
	})

	t.Run("new key exists", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_bbb")
		if !ok {
			t.Fatal("pk_live_bbb should exist after second snapshot")
		}
		if info.TenantID != "tenant2" {
			t.Errorf("TenantID = %q, want %q", info.TenantID, "tenant2")
		}
		if info.Name != "key-b" {
			t.Errorf("Name = %q, want %q", info.Name, "key-b")
		}
	})
}

func TestStreamAPIKeyRegistry_UpdateKeys_RemovedKeys(t *testing.T) {
	t.Parallel()
	r := newTestAPIKeyRegistry()

	// Snapshot with two keys.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys: []*provisioningv1.APIKeyInfo{
			{KeyId: "pk_live_keep", TenantUuid: "tenant1", Name: "keeper", IsActive: true},
			{KeyId: "pk_live_remove", TenantUuid: "tenant1", Name: "doomed", IsActive: true},
		},
	})

	// Delta removes one key.
	r.updateKeys(&provisioningv1.WatchAPIKeysResponse{
		IsSnapshot:    false,
		RemovedKeyIds: []string{"pk_live_remove"},
	})

	t.Run("removed key is gone", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_remove")
		if ok {
			t.Error("pk_live_remove should not exist after removal")
		}
		if info != nil {
			t.Errorf("expected nil for removed key, got %+v", info)
		}
	})

	t.Run("kept key still exists", func(t *testing.T) {
		t.Parallel()
		info, ok := r.Lookup("pk_live_keep")
		if !ok {
			t.Fatal("pk_live_keep should still exist after delta removal of other key")
		}
		if info.Name != "keeper" {
			t.Errorf("Name = %q, want %q", info.Name, "keeper")
		}
	})
}
