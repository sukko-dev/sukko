package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Sentinel errors for authentication failures.
// Callers map these to HTTP status codes:
//   - ErrNoCredentials → 401
//   - ErrInvalidToken → 401
//   - ErrInvalidAPIKey → 401
//   - ErrTenantMismatch → 401 (WS close reason CloseReasonAPIKeyTenantMismatch)
var (
	ErrNoCredentials = errors.New("no credentials provided")
	ErrInvalidToken  = errors.New("invalid token")
	ErrInvalidAPIKey = errors.New("invalid API key")
	// ErrTenantMismatch aliases the shared sentinel (§X — single definition in
	// shared/auth). It covers both the JWT-claim-vs-signing-key binding failure
	// (from ValidateToken) and the JWT-tenant-vs-API-key-tenant mismatch below.
	ErrTenantMismatch = auth.ErrTenantMismatch
	ErrTokenRevoked   = errors.New("token revoked")
	ErrMissingJTI     = errors.New("missing jti claim")
	// ErrTenantUnavailable is returned when API-key auth cannot resolve the
	// tenant UUID to a slug because the tenant-config projection is not yet warm.
	// It is retryable → handlers map it to HTTP 503 (not 401), so a client
	// reconnects once the projection catches up. Fail-closed: the request is
	// still rejected, never served with an empty tenant slug.
	ErrTenantUnavailable = errors.New("tenant resolution temporarily unavailable")
)

// Auth-failure metric method labels for tenant-mismatch rejections (§I named
// constants; §XVIII branch-distinct, matching the jwt_revoked / jwt+api_key_revoked
// convention).
const (
	authMethodJWTTenantMismatch = "jwt_tenant_mismatch"
	// G101 false positive: this is a Prometheus metric label value (the "api_key"
	// substring trips gosec's hardcoded-credential heuristic), not a credential.
	authMethodJWTAPIKeyTenantMismatch = "jwt+api_key_tenant_mismatch" //nolint:gosec // G101: metric label, not a credential
)

// logTenantMismatch Warn-logs a tenant-binding rejection with the structured
// fields carried by *auth.TenantMismatchError (kid, claimed slug, resolved claim
// UUID, key owning UUID). The raw error is never returned to the client — the
// owning tenant must not leak (§IX) — so callers return the generic sentinel.
func logTenantMismatch(logger zerolog.Logger, err error, remoteAddr string) {
	evt := logger.Warn().Str("remote_addr", remoteAddr)
	var tmErr *auth.TenantMismatchError
	if errors.As(err, &tmErr) {
		evt = evt.
			Str("kid", tmErr.KID).
			Str("claimed_slug", tmErr.ClaimedSlug).
			Str("claimed_uuid", tmErr.ClaimedUUID).
			Str("key_uuid", tmErr.KeyUUID)
	}
	evt.Msg("Request rejected: JWT tenant does not match signing key")
}

// authErrorResponse maps an authenticateRequest error to an (HTTP status, error
// code) pair: tenant mismatch is a hard 403, a cold-projection resolution
// failure is a retryable 503, and everything else is 401.
func authErrorResponse(err error) (status int, code string) {
	switch {
	case errors.Is(err, ErrTenantMismatch):
		return http.StatusForbidden, "FORBIDDEN"
	case errors.Is(err, ErrTenantUnavailable):
		return http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE"
	default:
		return http.StatusUnauthorized, "UNAUTHORIZED"
	}
}

// resolveAPIKeyTenantSlug maps an API key's tenant UUID to the current slug via
// the reverse projection. It distinguishes a cold projection (transient →
// ErrTenantUnavailable, HTTP 503) from an unknown tenant against a warm
// projection (hard reject → ErrTenantMismatch, HTTP 403). It never returns an
// empty slug without an error, so an API-key request is never served with an
// empty tenant scope (§II/§IX fail closed).
func (gw *Gateway) resolveAPIKeyTenantSlug(ctx context.Context, tenantUUID string) (string, error) {
	if gw.tenantSlugResolver == nil {
		return "", ErrTenantUnavailable
	}
	slug, err := gw.tenantSlugResolver.ResolveTenantSlug(ctx, tenantUUID)
	if err != nil {
		if !gw.tenantSlugResolver.TenantUUIDsPresent() {
			return "", ErrTenantUnavailable
		}
		return "", ErrTenantMismatch
	}
	if slug == "" {
		return "", ErrTenantUnavailable
	}
	return slug, nil
}

// authResult holds the validated identity from authenticateRequest.
type authResult struct {
	Claims     *auth.Claims // nil for API-key-only or auth-disabled
	Principal  string       // User identity (JWT subject or "anon:uuid")
	TenantSlug string       // client-facing routing label — data-plane channel scoping; always populated (resolved for API-key auth)
	TenantUUID string       // stable tenant UUID (identity of record); may be empty when the resolver is cold for JWT-only auth
	APIKeyOnly bool         // True if only API key was provided (no JWT)
	// APIKeyTenantID is the API key's owning tenant UUID (for cross-validation).
	APIKeyTenantID string
	AuthMethod     string // "none", "api_key", "jwt", "jwt+api_key" — for metrics and logging
	APIKeyID       string // Stable database ID of the API key (APIKeyInfo.KeyID); empty if no API key was used
	UserID         string // JWT subject claim; empty for API-key-only auth
}

// authenticateRequest validates credentials from the HTTP request.
// Checks all credential sources: query param ?token=, Authorization header, X-API-Key header.
//
// This function MUST NOT write to http.ResponseWriter — it returns an error
// and lets the caller decide the HTTP response.
//
// Auth metrics (RecordAuthValidation) are recorded inside this function.
// Permission checking stays OUT — each handler applies its own permission logic.
func (gw *Gateway) authenticateRequest(ctx context.Context, r *http.Request) (*authResult, error) {
	authStart := time.Now()
	token := httputil.ExtractBearerToken(r)
	apiKey := httputil.ExtractAPIKey(r)

	switch {
	case token == "" && apiKey == "":
		RecordAuthValidation(pkgmetrics.AuthStatusFailed, "none", time.Since(authStart))
		gw.logger.Warn().
			Str("remote_addr", r.RemoteAddr).
			Msg("Request rejected: no credentials provided")
		return nil, ErrNoCredentials

	case apiKey != "" && token == "":
		// API key only — public channels, no publish
		info, ok := gw.apiKeyRegistry.Lookup(apiKey)
		if !ok {
			RecordAPIKeyAuth(APIKeyAuthInvalid)
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "api_key", time.Since(authStart))
			gw.logger.Warn().
				Str("remote_addr", r.RemoteAddr).
				Msg("Request rejected: invalid API key")
			return nil, ErrInvalidAPIKey
		}
		// API keys carry only the tenant UUID (info.TenantID). Resolve it to the
		// current slug for data-plane channel scoping (#161 B0). Fail closed — an
		// API-key request is never served with an empty tenant slug.
		tenantSlug, resolveErr := gw.resolveAPIKeyTenantSlug(ctx, info.TenantID)
		if resolveErr != nil {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "api_key", time.Since(authStart))
			gw.logger.Warn().
				Str(logging.LogKeyTenantUUID, info.TenantID).
				Str("remote_addr", r.RemoteAddr).
				Err(resolveErr).
				Msg("Request rejected: cannot resolve API-key tenant slug")
			return nil, resolveErr
		}
		RecordAPIKeyAuth(APIKeyAuthAccepted)
		RecordAuthValidation(pkgmetrics.AuthStatusSuccess, "api_key", time.Since(authStart))

		result := &authResult{
			TenantSlug:     tenantSlug,
			TenantUUID:     info.TenantID,
			Principal:      "anon:" + uuid.NewString(),
			APIKeyOnly:     true,
			APIKeyTenantID: info.TenantID,
			AuthMethod:     "api_key",
			APIKeyID:       info.KeyID,
			// UserID is empty for API-key-only auth (no JWT subject to forward).
		}
		gw.logger.Debug().
			Str("principal", result.Principal).
			Str(logging.LogKeyTenantSlug, result.TenantSlug).
			Str(logging.LogKeyTenantUUID, result.TenantUUID).
			Str("remote_addr", r.RemoteAddr).
			Msg("API key validated successfully")
		return result, nil

	case token != "" && apiKey == "":
		// JWT only
		claims, err := gw.validator.ValidateToken(ctx, token)
		if err != nil {
			if errors.Is(err, auth.ErrTenantMismatch) {
				// Cross-tenant token: signed by one tenant's key but claiming
				// another (#158). Distinct label + structured detail server-side.
				RecordAuthValidation(pkgmetrics.AuthStatusFailed, authMethodJWTTenantMismatch, time.Since(authStart))
				logTenantMismatch(gw.logger, err, r.RemoteAddr)
				return nil, ErrTenantMismatch
			}
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt", time.Since(authStart))
			gw.logger.Warn().
				Err(err).
				Str("remote_addr", r.RemoteAddr).
				Msg("Request rejected: invalid token")
			return nil, ErrInvalidToken
		}
		RecordAuthValidation(pkgmetrics.AuthStatusSuccess, "jwt", time.Since(authStart))

		// jti mandatory
		if claims.ID == "" {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt_missing_jti", time.Since(authStart))
			return nil, ErrMissingJTI
		}

		// Revocation check — skip if registry not configured
		if gw.revocationRegistry != nil && gw.revocationRegistry.IsRevoked(
			claims.ID, claims.Subject, claims.TenantID, claims.IssuedAt.Unix(),
		) {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt_revoked", time.Since(authStart))
			return nil, ErrTokenRevoked
		}

		result := &authResult{
			Claims:     claims,
			Principal:  claims.Subject,
			TenantSlug: claims.TenantID,           // JWT tenant_id claim is the slug
			TenantUUID: claims.ResolvedTenantUUID, // bound to the signing key's tenant (#158)
			AuthMethod: "jwt",
			UserID:     claims.Subject,
			// APIKeyID is empty for JWT-only auth.
		}
		gw.logger.Debug().
			Str("principal", result.Principal).
			Str(logging.LogKeyTenantSlug, result.TenantSlug).
			Str(logging.LogKeyTenantUUID, result.TenantUUID).
			Strs("groups", claims.Groups).
			Str("remote_addr", r.RemoteAddr).
			Msg("Token validated successfully")
		return result, nil

	default:
		// Both API key and JWT — validate both, verify tenant match
		info, ok := gw.apiKeyRegistry.Lookup(apiKey)
		if !ok {
			RecordAPIKeyAuth(APIKeyAuthInvalid)
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt+api_key", time.Since(authStart))
			gw.logger.Warn().
				Str("remote_addr", r.RemoteAddr).
				Msg("Request rejected: invalid API key")
			return nil, ErrInvalidAPIKey
		}
		RecordAPIKeyAuth(APIKeyAuthAccepted)

		claims, err := gw.validator.ValidateToken(ctx, token)
		if err != nil {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt+api_key", time.Since(authStart))
			gw.logger.Warn().
				Err(err).
				Str("remote_addr", r.RemoteAddr).
				Msg("Request rejected: invalid token")
			return nil, ErrInvalidToken
		}

		// Verify JWT tenant matches API key tenant. The core binding already bound
		// the claim to the signing key's owning tenant UUID (claims.ResolvedTenantUUID);
		// compare that UUID against the API key's tenant UUID (info.TenantID) — both
		// UUIDs, so this is a like-for-like check (the old claims.TenantID slug vs
		// info.TenantID UUID comparison never matched). Shared with the auth-refresh
		// path via MatchesTenantUUID so the two can't diverge again.
		if !claims.MatchesTenantUUID(info.TenantID) {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, authMethodJWTAPIKeyTenantMismatch, time.Since(authStart))
			gw.logger.Warn().
				Str("jwt_tenant_uuid", claims.ResolvedTenantUUID).
				Str("api_key_tenant", info.TenantID).
				Str("remote_addr", r.RemoteAddr).
				Msg("Request rejected: API key and JWT tenant mismatch")
			return nil, ErrTenantMismatch
		}
		RecordAuthValidation(pkgmetrics.AuthStatusSuccess, "jwt+api_key", time.Since(authStart))

		// jti mandatory
		if claims.ID == "" {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt+api_key_missing_jti", time.Since(authStart))
			return nil, ErrMissingJTI
		}

		// Revocation check — skip if registry not configured
		if gw.revocationRegistry != nil && gw.revocationRegistry.IsRevoked(
			claims.ID, claims.Subject, claims.TenantID, claims.IssuedAt.Unix(),
		) {
			RecordAuthValidation(pkgmetrics.AuthStatusFailed, "jwt+api_key_revoked", time.Since(authStart))
			return nil, ErrTokenRevoked
		}

		result := &authResult{
			Claims:         claims,
			Principal:      claims.Subject,
			TenantSlug:     claims.TenantID,           // JWT tenant_id claim is the slug
			TenantUUID:     claims.ResolvedTenantUUID, // bound to the signing key's tenant (#158)
			APIKeyTenantID: info.TenantID,
			AuthMethod:     "jwt+api_key",
			APIKeyID:       info.KeyID,
			UserID:         claims.Subject,
		}
		gw.logger.Debug().
			Str("principal", result.Principal).
			Str(logging.LogKeyTenantSlug, result.TenantSlug).
			Str(logging.LogKeyTenantUUID, result.TenantUUID).
			Str("remote_addr", r.RemoteAddr).
			Msg("API key and token validated successfully")
		return result, nil
	}
}
