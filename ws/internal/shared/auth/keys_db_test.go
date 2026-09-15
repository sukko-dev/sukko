package auth_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// generateTestPEM generates an ECDSA P-256 key pair and returns the PEM-encoded public key.
func generateTestPEM(t *testing.T) string {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate EC key: %v", err)
	}
	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}))
}

// insertTestTenant inserts a tenant using the new UUID/slug schema and returns the generated UUID.
func insertTestTenant(t *testing.T, pool *pgxpool.Pool, slug, name string) string {
	t.Helper()
	ctx := t.Context()
	var tenantID string
	err := pool.QueryRow(ctx,
		`INSERT INTO tenants (slug, name, status, consumer_type) VALUES ($1, $2, 'active', 'shared') RETURNING id`,
		slug, name).Scan(&tenantID)
	if err != nil {
		t.Fatalf("insert tenant %s: %v", slug, err)
	}
	return tenantID
}

func TestDBKeyRegistry_GetKey(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := t.Context()

	pubPEM := generateTestPEM(t)

	tenantID := insertTestTenant(t, pool, "test-tenant", "Test Tenant")

	_, err := pool.Exec(ctx,
		`INSERT INTO tenant_keys (key_id, tenant_id, algorithm, public_key, is_active) VALUES ($1, $2, $3, $4, $5)`,
		"test-key-001", tenantID, "ES256", pubPEM, true)
	if err != nil {
		t.Fatalf("insert key: %v", err)
	}

	registry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: time.Minute,
		QueryTimeout:    5 * time.Second,
		Logger:          zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewKeyRegistry() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	key, err := registry.GetKey(ctx, "test-key-001")
	if err != nil {
		t.Fatalf("GetKey() error = %v", err)
	}
	if key.KeyID != "test-key-001" {
		t.Errorf("KeyID = %q, want %q", key.KeyID, "test-key-001")
	}
	// TenantID in KeyInfo is the stable tenant UUID (required by the tenant-UUID
	// binding in ValidateJWT), not the slug.
	if key.TenantID != tenantID {
		t.Errorf("TenantID = %q, want %q (tenant UUID)", key.TenantID, tenantID)
	}
	if key.PublicKey == nil {
		t.Error("PublicKey should be parsed")
	}
}

func TestDBKeyRegistry_GetKey_NotFound(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := t.Context()

	registry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: time.Minute,
		QueryTimeout:    5 * time.Second,
		Logger:          zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewKeyRegistry() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	_, err = registry.GetKey(ctx, "nonexistent-key")
	if err == nil {
		t.Fatal("GetKey() should return error for nonexistent key")
	}
}

func TestDBKeyRegistry_GetKeysByTenantUUID(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := t.Context()

	pubPEM := generateTestPEM(t)

	tenantID := insertTestTenant(t, pool, "multi-key-tenant", "Multi Key Tenant")

	for _, kid := range []string{"key-aaa-001", "key-aaa-002"} {
		_, err := pool.Exec(ctx,
			`INSERT INTO tenant_keys (key_id, tenant_id, algorithm, public_key, is_active) VALUES ($1, $2, $3, $4, $5)`,
			kid, tenantID, "ES256", pubPEM, true)
		if err != nil {
			t.Fatalf("insert key %s: %v", kid, err)
		}
	}

	registry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: time.Minute,
		QueryTimeout:    5 * time.Second,
		Logger:          zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewKeyRegistry() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	// GetKeysByTenantUUID is called with the tenant UUID (identity of record), the
	// same UUID the tenant_id claim resolves to during JWT tenant binding.
	keys, err := registry.GetKeysByTenantUUID(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetKeysByTenantUUID() error = %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("GetKeysByTenantUUID() returned %d keys, want 2", len(keys))
	}

	// An unknown tenant UUID returns no keys.
	empty, err := registry.GetKeysByTenantUUID(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("GetKeysByTenantUUID(unknown) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("GetKeysByTenantUUID(unknown) returned %d keys, want 0", len(empty))
	}
}

func TestDBKeyRegistry_RevokedKeyNotReturned(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := t.Context()

	pubPEM := generateTestPEM(t)
	past := time.Now().Add(-time.Hour)

	tenantID := insertTestTenant(t, pool, "revoked-tenant", "Revoked Tenant")

	_, err := pool.Exec(ctx,
		`INSERT INTO tenant_keys (key_id, tenant_id, algorithm, public_key, is_active, revoked_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		"revoked-key-01", tenantID, "ES256", pubPEM, true, past)
	if err != nil {
		t.Fatalf("insert revoked key: %v", err)
	}

	registry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: time.Minute,
		QueryTimeout:    5 * time.Second,
		Logger:          zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewKeyRegistry() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	_, err = registry.GetKey(ctx, "revoked-key-01")
	if err == nil {
		t.Fatal("GetKey() should return error for revoked key")
	}
}

func TestDBKeyRegistry_Refresh(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := t.Context()

	registry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: time.Minute,
		QueryTimeout:    5 * time.Second,
		Logger:          zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewKeyRegistry() error = %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	// Refresh should succeed even with empty database
	if err := registry.Refresh(ctx); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	stats := registry.Stats()
	if stats.TotalKeys != 0 {
		t.Errorf("TotalKeys = %d, want 0", stats.TotalKeys)
	}
}
