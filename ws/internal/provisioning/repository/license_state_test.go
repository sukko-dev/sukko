package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// testEncryptionKeyHex is a valid 64-char hex string (32 bytes) for AES-256.
const testEncryptionKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func newLicenseStateRepo(t *testing.T) *repository.LicenseStateRepository {
	t.Helper()
	pool := testutil.NewTestPool(t)
	key, err := crypto.ParseEncryptionKey(testEncryptionKeyHex)
	if err != nil {
		t.Fatalf("ParseEncryptionKey: %v", err)
	}
	return repository.NewLicenseStateRepository(pool, key)
}

func TestLicenseState_LoadEmpty(t *testing.T) {
	t.Parallel()
	repo := newLicenseStateRepo(t)

	got, err := repo.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != "" {
		t.Errorf("Load() = %q, want empty (no license stored)", got)
	}
}

func TestLicenseState_UpsertAndLoad(t *testing.T) {
	t.Parallel()
	repo := newLicenseStateRepo(t)
	ctx := context.Background()

	expires := time.Now().Add(365 * 24 * time.Hour).Truncate(time.Microsecond)
	if err := repo.Upsert(ctx, "license-key-v1", "pro", "Acme Corp", &expires); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	got, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != "license-key-v1" {
		t.Errorf("Load() = %q, want %q", got, "license-key-v1")
	}
}

func TestLicenseState_UpsertOverwrites(t *testing.T) {
	t.Parallel()
	repo := newLicenseStateRepo(t)
	ctx := context.Background()

	expires := time.Now().Add(365 * 24 * time.Hour).Truncate(time.Microsecond)

	// First insert
	if err := repo.Upsert(ctx, "key-v1", "pro", "Acme", &expires); err != nil {
		t.Fatalf("first Upsert() error = %v", err)
	}

	// Overwrite with new key
	if err := repo.Upsert(ctx, "key-v2", "enterprise", "Acme", &expires); err != nil {
		t.Fatalf("second Upsert() error = %v", err)
	}

	got, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != "key-v2" {
		t.Errorf("Load() = %q, want %q (should be overwritten)", got, "key-v2")
	}
}
