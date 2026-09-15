package gateway

import (
	"context"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// TokenValidator validates JWT tokens and returns claims.
// The existing auth.MultiTenantValidator satisfies this interface.
type TokenValidator interface {
	ValidateToken(ctx context.Context, tokenString string) (*auth.Claims, error)
}

// ChannelRulesProvider provides per-tenant channel rules for the gateway.
// Defined here (consumer) per coding guidelines: "accept interfaces, return concrete types"
type ChannelRulesProvider interface {
	// GetChannelRules returns the channel rules for a tenant.
	// Returns types.ErrChannelRulesNotFound if not configured.
	GetChannelRules(ctx context.Context, tenantID string) (*types.ChannelRules, error)

	// SnapshotReceived reports whether the initial rules snapshot has been
	// successfully applied. False means rules are unknown — callers fail
	// closed and readiness is withheld.
	SnapshotReceived() bool

	// State returns the stream connection state (provapi.StreamState*).
	State() int32

	// Close releases resources held by the provider.
	Close() error
}

// TenantSlugResolver maps a stable tenant UUID to its current slug. API keys
// carry only the tenant UUID (APIKeyInfo.TenantID), but the data plane scopes
// channels by slug — so API-key auth resolves the slug here. Implemented by
// provapi.StreamChannelRulesProvider; defined at the consumer for mock injection.
type TenantSlugResolver interface {
	// ResolveTenantSlug returns the current slug for a tenant UUID, or
	// auth.ErrTenantNotResolvable when the UUID is unknown.
	ResolveTenantSlug(ctx context.Context, tenantUUID string) (string, error)

	// TenantUUIDsPresent reports whether the tenant-config projection is warm
	// (delivering tenant UUIDs). False = cold: callers fail closed as retryable.
	TenantUUIDsPresent() bool
}

// APIKeyLookup provides O(1) API key validation for gateway connections.
// Implemented by StreamAPIKeyRegistry; defined here to enable mock injection in tests.
type APIKeyLookup interface {
	// Lookup returns the API key info for the given key string.
	// Returns false if the key is not found or not active.
	Lookup(apiKey string) (*provapi.APIKeyInfo, bool)

	// Close releases resources held by the registry.
	Close() error
}

// licenseWatcher is used by Gateway for health reporting and shutdown.
// Satisfied by *provapi.StreamLicenseWatcher; mock-injectable in tests.
type licenseWatcher interface {
	State() int32
	Close() error
}

// RevocationChecker answers token-revocation queries for the gateway (connect-time check
// and the post-register re-check). Satisfied by *provapi.StreamRevocationRegistry; defined
// at the consumer for mock injection (same pattern as APIKeyLookup / licenseWatcher).
type RevocationChecker interface {
	// IsRevoked reports whether the token is revoked. Lock-free snapshot read.
	IsRevoked(jti, sub, tenantID string, iat int64) bool
	// Close releases resources held by the checker.
	Close() error
}

// Compile-time assertion that the concrete stream registry satisfies RevocationChecker.
var _ RevocationChecker = (*provapi.StreamRevocationRegistry)(nil)
