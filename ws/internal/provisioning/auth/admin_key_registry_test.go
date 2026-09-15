package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

func testKey(keyID string) *sharedauth.KeyInfo {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	return &sharedauth.KeyInfo{
		KeyID:     keyID,
		Algorithm: "EdDSA",
		PublicKey: pub,
		IsActive:  true,
	}
}

func revokedKey(keyID string) *sharedauth.KeyInfo {
	k := testKey(keyID)
	now := time.Now()
	k.RevokedAt = &now
	k.IsActive = false
	return k
}

func TestAdminKeyRegistry_GetKey_Hit(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()
	k := testKey("ak_test1")
	reg.Refresh([]*sharedauth.KeyInfo{k})

	got, err := reg.GetKey(context.Background(), "ak_test1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.KeyID != "ak_test1" {
		t.Errorf("key ID = %q, want ak_test1", got.KeyID)
	}
}

func TestAdminKeyRegistry_GetKey_Miss(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()

	_, err := reg.GetKey(context.Background(), "nonexistent")
	if !errors.Is(err, sharedauth.ErrKeyNotFound) {
		t.Errorf("error = %v, want ErrKeyNotFound", err)
	}
}

func TestAdminKeyRegistry_GetKey_Revoked(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()
	reg.Refresh([]*sharedauth.KeyInfo{revokedKey("ak_revoked")})

	_, err := reg.GetKey(context.Background(), "ak_revoked")
	if !errors.Is(err, sharedauth.ErrKeyRevoked) {
		t.Errorf("error = %v, want ErrKeyRevoked", err)
	}
}

func TestAdminKeyRegistry_Refresh_ReplacesAll(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()

	// Initial set
	reg.Refresh([]*sharedauth.KeyInfo{testKey("ak_old")})
	if reg.ActiveCount() != 1 {
		t.Fatalf("active = %d, want 1", reg.ActiveCount())
	}

	// Replace with new set
	reg.Refresh([]*sharedauth.KeyInfo{testKey("ak_new1"), testKey("ak_new2")})
	if reg.ActiveCount() != 2 {
		t.Errorf("active = %d, want 2", reg.ActiveCount())
	}

	// Old key gone
	_, err := reg.GetKey(context.Background(), "ak_old")
	if !errors.Is(err, sharedauth.ErrKeyNotFound) {
		t.Errorf("old key should be gone, got: %v", err)
	}
}

func TestAdminKeyRegistry_ActiveCount(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()

	reg.Refresh([]*sharedauth.KeyInfo{
		testKey("ak_active1"),
		testKey("ak_active2"),
		revokedKey("ak_revoked"),
	})

	if got := reg.ActiveCount(); got != 2 {
		t.Errorf("active = %d, want 2 (1 revoked)", got)
	}
}

func TestAdminKeyRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	reg := NewAdminKeyRegistry()
	reg.Refresh([]*sharedauth.KeyInfo{testKey("ak_concurrent")})

	var wg sync.WaitGroup
	// 10 readers + 5 writers concurrently
	for range 10 {
		wg.Go(func() {
			for range 100 {
				_, _ = reg.GetKey(context.Background(), "ak_concurrent")
			}
		})
	}
	for range 5 {
		wg.Go(func() {
			for range 20 {
				reg.Refresh([]*sharedauth.KeyInfo{testKey("ak_concurrent")})
			}
		})
	}
	wg.Wait()

	// If we get here without race detector complaints, concurrency is safe
	if reg.ActiveCount() < 1 {
		t.Error("expected at least 1 active key after concurrent operations")
	}
}
