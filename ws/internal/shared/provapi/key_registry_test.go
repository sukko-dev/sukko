package provapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// testPublicKeyPEM is a valid EC P-256 public key for tests.
const testPublicKeyPEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEEVs/o5+uQbTjL3chynL4wXgUg2R9
q9UU8I5mEovUf86QZ7kOBIjJwqnzD1omageEHWwHdBO6B+dFabmdT9POxg==
-----END PUBLIC KEY-----`

// newTestKeyRegistry creates a minimal StreamKeyRegistry for unit testing
// without gRPC connections or Prometheus metrics.
func newTestKeyRegistry() *StreamKeyRegistry {
	return &StreamKeyRegistry{
		keysByID:     make(map[string]*auth.KeyInfo),
		keysByTenant: make(map[string][]*auth.KeyInfo),
		logger:       zerolog.Nop(),
	}
}

func TestKeyRegistry_Snapshot(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:        "key-1",
				TenantUuid:   "tenant-a",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     true,
			},
			{
				KeyId:        "key-2",
				TenantUuid:   "tenant-a",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     true,
			},
		},
	})

	ctx := context.Background()

	t.Run("GetKey existing", func(t *testing.T) {
		t.Parallel()
		key, err := r.GetKey(ctx, "key-1")
		if err != nil {
			t.Fatalf("GetKey() error = %v", err)
		}
		if key.KeyID != "key-1" {
			t.Errorf("KeyID = %q, want %q", key.KeyID, "key-1")
		}
		if key.TenantID != "tenant-a" {
			t.Errorf("TenantID = %q, want %q", key.TenantID, "tenant-a")
		}
		if key.Algorithm != "ES256" {
			t.Errorf("Algorithm = %q, want %q", key.Algorithm, "ES256")
		}
	})

	t.Run("GetKey not found", func(t *testing.T) {
		t.Parallel()
		_, err := r.GetKey(ctx, "nonexistent")
		if !errors.Is(err, auth.ErrKeyNotFound) {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("GetKeysByTenantUUID", func(t *testing.T) {
		t.Parallel()
		keys, err := r.GetKeysByTenantUUID(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("GetKeysByTenantUUID() error = %v", err)
		}
		if len(keys) != 2 {
			t.Errorf("len(keys) = %d, want 2", len(keys))
		}
	})

	t.Run("GetKeysByTenantUUID empty", func(t *testing.T) {
		t.Parallel()
		keys, err := r.GetKeysByTenantUUID(ctx, "nonexistent")
		if err != nil {
			t.Fatalf("GetKeysByTenantUUID() error = %v", err)
		}
		if keys != nil {
			t.Errorf("expected nil for unknown tenant, got %v", keys)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		t.Parallel()
		stats := r.Stats()
		if stats.TotalKeys != 2 {
			t.Errorf("TotalKeys = %d, want 2", stats.TotalKeys)
		}
		if stats.ActiveKeys != 2 {
			t.Errorf("ActiveKeys = %d, want 2", stats.ActiveKeys)
		}
	})
}

func TestKeyRegistry_Delta(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	// Load initial snapshot
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:        "key-1",
				TenantUuid:   "tenant-a",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     true,
			},
		},
	})

	// Apply delta: add key-2, remove key-1
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: false,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:        "key-2",
				TenantUuid:   "tenant-b",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     true,
			},
		},
		RemovedKeyIds: []string{"key-1"},
	})

	ctx := context.Background()

	// key-1 should be removed
	_, err := r.GetKey(ctx, "key-1")
	if !errors.Is(err, auth.ErrKeyNotFound) {
		t.Errorf("key-1 should be removed, got %v", err)
	}

	// key-2 should exist
	key, err := r.GetKey(ctx, "key-2")
	if err != nil {
		t.Fatalf("GetKey(key-2) error = %v", err)
	}
	if key.TenantID != "tenant-b" {
		t.Errorf("TenantID = %q, want %q", key.TenantID, "tenant-b")
	}
}

func TestKeyRegistry_DeltaUpdate(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	// Load initial snapshot
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:        "key-1",
				TenantUuid:   "tenant-a",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     true,
			},
		},
	})

	// Delta update: update key-1 to inactive
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: false,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:        "key-1",
				TenantUuid:   "tenant-a",
				Algorithm:    "ES256",
				PublicKeyPem: testPublicKeyPEM,
				IsActive:     false,
			},
		},
	})

	ctx := context.Background()

	// GetKey should reject inactive keys (defense in depth)
	_, err := r.GetKey(ctx, "key-1")
	if !errors.Is(err, auth.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound for inactive key, got %v", err)
	}

	// GetKeysByTenantUUID should also filter inactive keys
	activeKeys, err := r.GetKeysByTenantUUID(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("GetKeysByTenantUUID() error = %v", err)
	}
	if len(activeKeys) != 0 {
		t.Errorf("expected 0 active keys, got %d", len(activeKeys))
	}
}

func TestKeyRegistry_SnapshotReplacesAll(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	// First snapshot with 2 keys
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{KeyId: "key-1", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true},
			{KeyId: "key-2", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true},
		},
	})

	// Second snapshot with only 1 key (should replace)
	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{KeyId: "key-3", TenantUuid: "t-b", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true},
		},
	})

	ctx := context.Background()

	// Old keys should be gone
	_, err := r.GetKey(ctx, "key-1")
	if !errors.Is(err, auth.ErrKeyNotFound) {
		t.Error("key-1 should not exist after snapshot replace")
	}

	// New key should exist
	key, err := r.GetKey(ctx, "key-3")
	if err != nil {
		t.Fatalf("GetKey(key-3) error = %v", err)
	}
	if key.TenantID != "t-b" {
		t.Errorf("TenantID = %q, want %q", key.TenantID, "t-b")
	}

	stats := r.Stats()
	if stats.TotalKeys != 1 {
		t.Errorf("TotalKeys = %d, want 1", stats.TotalKeys)
	}
}

func TestKeyRegistry_ExpiredKey(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	expired := time.Now().Add(-1 * time.Hour).Unix()

	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{
				KeyId:         "expired-key",
				TenantUuid:    "tenant-a",
				Algorithm:     "ES256",
				PublicKeyPem:  testPublicKeyPEM,
				IsActive:      true,
				ExpiresAtUnix: expired,
			},
		},
	})

	ctx := context.Background()

	_, err := r.GetKey(ctx, "expired-key")
	if !errors.Is(err, auth.ErrKeyExpired) {
		t.Errorf("expected ErrKeyExpired, got %v", err)
	}
}

func TestKeyRegistry_StatsActiveCount(t *testing.T) {
	t.Parallel()
	r := newTestKeyRegistry()

	expired := time.Now().Add(-1 * time.Hour).Unix()

	r.updateKeys(&provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys: []*provisioningv1.KeyInfo{
			{KeyId: "active-1", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true},
			{KeyId: "active-2", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true},
			{KeyId: "inactive", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: false},
			{KeyId: "expired", TenantUuid: "t-a", Algorithm: "ES256", PublicKeyPem: testPublicKeyPEM, IsActive: true, ExpiresAtUnix: expired},
		},
	})

	stats := r.Stats()
	if stats.TotalKeys != 4 {
		t.Errorf("TotalKeys = %d, want 4", stats.TotalKeys)
	}
	if stats.ActiveKeys != 2 {
		t.Errorf("ActiveKeys = %d, want 2 (should exclude inactive and expired)", stats.ActiveKeys)
	}
}

func TestParsePublicKeyViaAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pem     string
		algo    string
		wantErr bool
	}{
		{"valid EC key", testPublicKeyPEM, "ES256", false},
		{"invalid PEM", "not-a-pem", "ES256", true},
		{"empty", "", "ES256", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := auth.ParsePublicKey(tt.pem, tt.algo)
			if (err != nil) != tt.wantErr {
				t.Errorf("auth.ParsePublicKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current time.Duration
		max     time.Duration
		wantMin time.Duration
		wantMax time.Duration
	}{
		{
			name:    "doubles with jitter",
			current: 1 * time.Second,
			max:     30 * time.Second,
			wantMin: 1500 * time.Millisecond, // 2s * 0.75
			wantMax: 2 * time.Second,         // 2s * 1.0
		},
		{
			name:    "caps at max",
			current: 20 * time.Second,
			max:     30 * time.Second,
			wantMin: 22500 * time.Millisecond, // 30s * 0.75
			wantMax: 30 * time.Second,         // 30s * 1.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Run multiple times to test jitter
			for range 20 {
				result := backoff(tt.current, tt.max)
				if result < tt.wantMin || result > tt.wantMax {
					t.Errorf("backoff(%v, %v) = %v, want [%v, %v]",
						tt.current, tt.max, result, tt.wantMin, tt.wantMax)
				}
			}
		})
	}
}
