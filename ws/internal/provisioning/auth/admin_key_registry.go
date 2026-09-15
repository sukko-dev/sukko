// Package auth provides admin-specific authentication for the provisioning service.
// This is separate from shared/auth which handles tenant JWT validation.
// Constitution X: admin auth is only used by provisioning — not shared.
package auth

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"

	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

// AdminKeyRegistry implements shared auth.KeyResolver for admin public keys.
// In-memory cache guarded by sync.RWMutex, refreshed directly by handlers
// on registration/revocation events (no background goroutine needed).
type AdminKeyRegistry struct {
	mu   sync.RWMutex
	keys map[string]*sharedauth.KeyInfo // keyed by key_id
}

// NewAdminKeyRegistry creates an empty AdminKeyRegistry.
func NewAdminKeyRegistry() *AdminKeyRegistry {
	return &AdminKeyRegistry{
		keys: make(map[string]*sharedauth.KeyInfo),
	}
}

// GetKey retrieves an admin key by key ID. Returns auth sentinel errors for
// not-found and revoked keys.
func (r *AdminKeyRegistry) GetKey(_ context.Context, keyID string) (*sharedauth.KeyInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key, ok := r.keys[keyID]
	if !ok {
		return nil, sharedauth.ErrKeyNotFound
	}
	if !key.IsValid() {
		if key.RevokedAt != nil {
			return nil, sharedauth.ErrKeyRevoked
		}
		return nil, sharedauth.ErrKeyExpired
	}

	return key, nil
}

// Refresh replaces all cached keys with the provided set.
// Called by handlers after registration or revocation.
func (r *AdminKeyRegistry) Refresh(keys []*sharedauth.KeyInfo) {
	m := make(map[string]*sharedauth.KeyInfo, len(keys))
	for _, k := range keys {
		m[k.KeyID] = k
	}

	r.mu.Lock()
	r.keys = m
	r.mu.Unlock()
}

// ActiveCount returns the number of active (valid) keys in the cache.
func (r *AdminKeyRegistry) ActiveCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, k := range r.keys {
		if k.IsValid() {
			count++
		}
	}
	return count
}

// AdminKeyToKeyInfo converts a repository AdminKey to a shared KeyInfo.
// The publicKey must be a parsed crypto.PublicKey (e.g., ed25519.PublicKey).
func AdminKeyToKeyInfo(keyID, name, algorithm string, publicKey crypto.PublicKey) *sharedauth.KeyInfo {
	return &sharedauth.KeyInfo{
		KeyID:     keyID,
		Algorithm: algorithm,
		PublicKey: publicKey,
		IsActive:  true,
	}
}

// ParsePublicKeyPEM parses a PEM-encoded public key. Used by both the admin
// keys handler (registration validation) and main.go (bootstrap cache loading).
func ParsePublicKeyPEM(pemEncoded string) (any, error) {
	block, _ := pem.Decode([]byte(pemEncoded))
	if block == nil {
		return nil, errors.New("invalid PEM encoding")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX public key: %w", err)
	}
	return key, nil
}

// Verify AdminKeyRegistry implements KeyResolver at compile time.
var _ sharedauth.KeyResolver = (*AdminKeyRegistry)(nil)
