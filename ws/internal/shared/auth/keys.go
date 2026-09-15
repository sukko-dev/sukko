// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Errors for key registry operations.
var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyRevoked      = errors.New("key revoked")
	ErrKeyExpired      = errors.New("key expired")
	ErrTenantInactive  = errors.New("tenant inactive")
	ErrInvalidKeyType  = errors.New("invalid key type for algorithm")
	ErrUnsupportedAlgo = errors.New("unsupported algorithm")

	// ErrTenantMismatch is returned when a token's tenant_id claim does not
	// resolve to the same tenant that owns the signing key (kid). This is the
	// canonical cross-tenant-forgery sentinel; the gateway maps it to a
	// tenant-mismatch response and callers route on it via errors.Is.
	ErrTenantMismatch = errors.New("tenant mismatch")

	// ErrTenantNotResolvable is returned when a tenant identity cannot be resolved
	// in EITHER direction — a slug that maps to no tenant UUID (JWT tenant binding),
	// or a tenant UUID that maps to no current slug (API-key data-plane scoping) —
	// because the tenant is unknown/deleted or the resolver cache is not yet warm.
	// Tenant-scoped validation treats this as a rejection (fail closed).
	ErrTenantNotResolvable = errors.New("tenant not resolvable")
)

// TenantResolver maps a JWT tenant_id claim (a tenant slug) to the stable
// tenant UUID, so validation can bind the claim to the signing key's owning
// tenant UUID. Implementations are per-service: the gateway resolves from the
// tenant-config stream cache (rename-aware, current + previous slug), the
// provisioning tenant path from a grace-aware DB lookup. Implementations MUST
// be thread-safe and return ErrTenantNotResolvable when the slug is unknown.
type TenantResolver interface {
	ResolveTenantUUID(ctx context.Context, slug string) (string, error)
}

// TenantMismatchError carries the details of a cross-tenant binding failure for
// server-side structured logging. Its Error() message is intentionally generic
// so the owning tenant is never leaked to clients (Constitution §IX); callers
// use errors.As to log the fields and errors.Is(err, ErrTenantMismatch) to route.
type TenantMismatchError struct {
	KID         string // signing key id from the token header
	ClaimedSlug string // tenant_id claim (slug) presented by the token
	ClaimedUUID string // tenant UUID the claim resolved to ("" if unresolved)
	KeyUUID     string // tenant UUID that actually owns the signing key
}

func (e *TenantMismatchError) Error() string { return ErrTenantMismatch.Error() }

func (e *TenantMismatchError) Unwrap() error { return ErrTenantMismatch }

// KeyInfo contains public key information for JWT validation.
type KeyInfo struct {
	// KeyID is the unique key identifier (kid in JWT header).
	KeyID string

	// TenantID is the stable UUID of the owning tenant. The tenant-UUID binding
	// (ValidateJWT) compares this against the tenant UUID that the tenant_id
	// claim resolves to, so a registry feeding a tenant-scoped validator MUST
	// populate this with the tenant UUID — never the slug. (StreamKeyRegistry
	// carries the UUID FK; DBKeyRegistry is normalized to the UUID for the same
	// reason.)
	TenantID string

	// Algorithm is the signing algorithm (ES256, RS256, EdDSA).
	Algorithm string

	// PublicKey is the parsed public key.
	PublicKey crypto.PublicKey

	// PublicKeyPEM is the raw PEM-encoded key (for caching).
	PublicKeyPEM string

	// IsActive indicates if the key is currently valid.
	IsActive bool

	// ExpiresAt is when the key expires (nil = no expiry).
	ExpiresAt *time.Time

	// RevokedAt is when the key was revoked (nil = not revoked).
	RevokedAt *time.Time
}

// IsValid checks if the key is currently valid (active, not expired, not revoked).
func (k *KeyInfo) IsValid() bool {
	if !k.IsActive {
		return false
	}
	if k.RevokedAt != nil {
		return false
	}
	if k.ExpiresAt != nil && k.ExpiresAt.Before(time.Now()) {
		return false
	}
	return true
}

// KeyResolver provides public key lookup by key ID.
// This is the minimal interface needed for JWT signature verification.
// Implementations must be thread-safe.
type KeyResolver interface {
	// GetKey retrieves a key by its ID.
	// Returns ErrKeyNotFound if the key doesn't exist.
	// Returns ErrKeyRevoked if the key has been revoked.
	// Returns ErrKeyExpired if the key has expired.
	GetKey(ctx context.Context, keyID string) (*KeyInfo, error)
}

// KeyRegistry provides public keys for JWT validation.
// Extends KeyResolver with tenant-scoped queries and lifecycle management.
// Implementations should be thread-safe.
type KeyRegistry interface {
	KeyResolver

	// GetKeysByTenantUUID retrieves all active keys for a tenant UUID.
	GetKeysByTenantUUID(ctx context.Context, tenantUUID string) ([]*KeyInfo, error)

	// Close releases any resources held by the registry.
	Close() error
}

// KeyRegistryWithRefresh extends KeyRegistry with refresh capabilities.
type KeyRegistryWithRefresh interface {
	KeyRegistry

	// Refresh forces a refresh of the key cache.
	Refresh(ctx context.Context) error

	// Stats returns cache statistics.
	Stats() KeyRegistryStats
}

// KeyRegistryStats contains cache statistics.
type KeyRegistryStats struct {
	// TotalKeys is the number of keys in the cache.
	TotalKeys int

	// ActiveKeys is the number of active, non-expired keys.
	ActiveKeys int

	// LastRefresh is when the cache was last refreshed.
	LastRefresh time.Time

	// RefreshErrors is the count of refresh errors.
	RefreshErrors int64

	// CacheHits is the count of cache hits.
	CacheHits int64

	// CacheMisses is the count of cache misses.
	CacheMisses int64
}

// ParsePublicKey parses a PEM-encoded public key and validates it matches the algorithm.
func ParsePublicKey(pemData, algorithm string) (crypto.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	// Validate key type matches algorithm
	switch algorithm {
	case "ES256":
		if _, ok := pub.(*ecdsa.PublicKey); !ok {
			return nil, fmt.Errorf("%w: ES256 requires ECDSA key", ErrInvalidKeyType)
		}
	case "RS256":
		if _, ok := pub.(*rsa.PublicKey); !ok {
			return nil, fmt.Errorf("%w: RS256 requires RSA key", ErrInvalidKeyType)
		}
	case "EdDSA":
		if _, ok := pub.(ed25519.PublicKey); !ok {
			return nil, fmt.Errorf("%w: EdDSA requires Ed25519 key", ErrInvalidKeyType)
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedAlgo, algorithm)
	}

	return pub, nil
}

// GetSigningMethod returns the jwt.SigningMethod for an algorithm string.
func GetSigningMethod(algorithm string) (jwt.SigningMethod, error) {
	switch algorithm {
	case "ES256":
		return jwt.SigningMethodES256, nil
	case "RS256":
		return jwt.SigningMethodRS256, nil
	case "EdDSA":
		return jwt.SigningMethodEdDSA, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedAlgo, algorithm)
	}
}

// StaticKeyRegistry is a simple in-memory key registry for testing.
// Thread-safe for concurrent use.
type StaticKeyRegistry struct {
	mu   sync.RWMutex
	keys map[string]*KeyInfo
}

// NewStaticKeyRegistry creates a new static key registry.
func NewStaticKeyRegistry() *StaticKeyRegistry {
	return &StaticKeyRegistry{
		keys: make(map[string]*KeyInfo),
	}
}

// AddKey adds a key to the registry.
func (r *StaticKeyRegistry) AddKey(key *KeyInfo) error {
	// Parse the public key if not already parsed
	if key.PublicKey == nil && key.PublicKeyPEM != "" {
		pub, err := ParsePublicKey(key.PublicKeyPEM, key.Algorithm)
		if err != nil {
			return err
		}
		key.PublicKey = pub
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[key.KeyID] = key
	return nil
}

// GetKey retrieves a key by ID.
func (r *StaticKeyRegistry) GetKey(_ context.Context, keyID string) (*KeyInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.keys[keyID]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if key.RevokedAt != nil {
		return nil, ErrKeyRevoked
	}
	if key.ExpiresAt != nil && key.ExpiresAt.Before(time.Now()) {
		return nil, ErrKeyExpired
	}
	if !key.IsActive {
		return nil, ErrKeyNotFound
	}
	return key, nil
}

// GetKeysByTenantUUID retrieves all active keys for a tenant UUID.
func (r *StaticKeyRegistry) GetKeysByTenantUUID(_ context.Context, tenantUUID string) ([]*KeyInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var keys []*KeyInfo
	for _, key := range r.keys {
		if key.TenantID == tenantUUID && key.IsValid() {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// Close is a no-op for static registry.
func (r *StaticKeyRegistry) Close() error {
	return nil
}

// Ensure StaticKeyRegistry implements KeyRegistry.
var _ KeyRegistry = (*StaticKeyRegistry)(nil)
