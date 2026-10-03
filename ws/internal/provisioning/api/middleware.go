// Package api provides HTTP handlers and middleware for the provisioning service.
package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/provisioning"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Context key for actor information (tenant_id:user_id format).
type contextKey string

const (
	// ActorContextKey is the context key for the actor (tenant_id:user_id).
	ActorContextKey contextKey = "actor"
	// tenantUUIDContextKey holds the validated tenant's UUID (identity-of-record, control
	// plane), stashed by RequireTenant for handlers that write UUID-keyed records (webhooks,
	// audit log). Reuses the tenant record RequireTenant already fetched — no extra query.
	tenantUUIDContextKey contextKey = "tenant_uuid"
	// tenantSlugContextKey holds the effective data-path slug (registry keying), also stashed by
	// RequireTenant. It differs by caller: a tenant passes its own claims.TenantID — the
	// connect-time slug, correct for the connection registry even under a rename grace window; an
	// operator (no tenant claim) passes the tenant's current slug. Handlers read it via
	// getEffectiveTenantSlug.
	tenantSlugContextKey contextKey = "tenant_slug"
)

// LoggingMiddleware provides structured request logging.
func LoggingMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			// Inject logger into context so downstream middleware (RequireRole, RequireTenant) can log.
			r = r.WithContext(logger.WithContext(r.Context()))

			defer func() { //nolint:contextcheck // False positive: r.Context() is captured via closure from the enclosing HandlerFunc scope; the deferred func accesses the same request context used by next.ServeHTTP.
				elapsed := time.Since(start)

				logger.Info().
					Str("method", r.Method).
					Str("path", r.URL.Path).
					Str("remote_addr", r.RemoteAddr).
					Int("status", ww.Status()).
					Int("bytes", ww.BytesWritten()).
					Dur("duration", elapsed).
					Str("request_id", middleware.GetReqID(r.Context())).
					Msg("HTTP request")

				// Record API metrics using route pattern to avoid cardinality explosion
				routePattern := chi.RouteContext(r.Context()).RoutePattern()
				if routePattern == "" {
					routePattern = "unmatched"
				}
				statusStr := strconv.Itoa(ww.Status())
				RecordAPIRequest(routePattern, r.Method, statusStr)
				RecordAPILatency(routePattern, r.Method, elapsed.Seconds())
			}()

			next.ServeHTTP(ww, r)
		})
	}
}

// AdminJWTMiddleware validates admin JWTs (iss:"sukko-admin").
// If the token has the admin issuer, it validates via AdminValidator and sets context.
// If not an admin JWT (different or missing issuer), it falls through to the next middleware.
func AdminJWTMiddleware(validator *provauth.AdminValidator, logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip if already authenticated
			if auth.GetClaims(r.Context()) != nil {
				next.ServeHTTP(w, r)
				return
			}

			// Extract Bearer token
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				next.ServeHTTP(w, r)
				return
			}

			scheme, tokenString, found := strings.Cut(authHeader, " ")
			if !found || !strings.EqualFold(scheme, "Bearer") {
				next.ServeHTTP(w, r)
				return
			}

			// Quick check: is this an admin JWT? (peek at issuer without full validation)
			iss, _ := auth.ExtractIssuer(tokenString)
			if iss != provauth.AdminJWTIssuer {
				// Not an admin JWT — fall through to tenant JWT middleware
				next.ServeHTTP(w, r)
				return
			}

			// Validate admin JWT
			claims, err := validator.ValidateToken(r.Context(), tokenString)
			if err != nil {
				kid, _ := auth.ExtractKeyID(tokenString)
				clientIP := httputil.GetClientIP(r)
				logger.Warn().
					Err(err).
					Str("kid", kid).
					Str("client_ip", clientIP).
					Msg("admin JWT authentication failed")

				httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "invalid admin credentials")
				return
			}

			// Set admin identity in context
			sub, _ := claims.GetSubject()
			kid, _ := auth.ExtractKeyID(tokenString)
			ctx := auth.WithClaims(r.Context(), claims)
			// Bare IP, never RemoteAddr's host:port: the audit trail stores this in an
			// INET column, and a port suffix made every admin-action audit INSERT fail
			// (SQLSTATE 22P02) — silently losing the §IX-mandatory audit records.
			ctx = auth.WithActor(ctx, kid, provisioning.ActorTypeAdmin, httputil.GetClientIP(r))

			logger.Debug().
				Str("admin_key_id", kid).
				Str("admin_name", sub).
				Msg("admin JWT authenticated")

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AuthMiddleware validates JWT tokens and extracts actor information.
// Tokens are validated using the MultiTenantValidator which supports
// per-tenant asymmetric keys (ES256, RS256, EdDSA).
func AuthMiddleware(validator *auth.MultiTenantValidator, logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip if already authenticated (e.g., by admin token middleware)
			if auth.GetClaims(r.Context()) != nil {
				next.ServeHTTP(w, r)
				return
			}

			// Extract token from Authorization header
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				httputil.WriteError(w, http.StatusUnauthorized, errCodeMissingToken, "Authorization header required")
				return
			}

			// Expect "Bearer <token>"
			scheme, tokenString, found := strings.Cut(authHeader, " ")
			if !found || !strings.EqualFold(scheme, "Bearer") {
				httputil.WriteError(w, http.StatusUnauthorized, errCodeInvalidAuthHeader, "Authorization header must be 'Bearer <token>'")
				return
			}

			// Validate token
			claims, err := validator.ValidateToken(r.Context(), tokenString)
			if err != nil {
				logger.Warn().
					Err(err).
					Str("remote_addr", r.RemoteAddr).
					Str("path", r.URL.Path).
					Msg("Token validation failed")

				var reason string
				switch {
				case errors.Is(err, auth.ErrMissingToken):
					reason = authFailureReasonMissingToken
					httputil.WriteError(w, http.StatusUnauthorized, errCodeMissingToken, "Token required")
				case errors.Is(err, auth.ErrTokenExpired):
					reason = authFailureReasonTokenExpired
					httputil.WriteError(w, http.StatusUnauthorized, errCodeTokenExpired, "Token has expired")
				case errors.Is(err, auth.ErrKeyNotFound):
					reason = authFailureReasonKeyNotFound
					httputil.WriteError(w, http.StatusUnauthorized, errCodeKeyNotFound, "Signing key not found")
				case errors.Is(err, auth.ErrKeyRevoked):
					reason = authFailureReasonKeyRevoked
					httputil.WriteError(w, http.StatusUnauthorized, errCodeKeyRevoked, "Signing key has been revoked")
				case errors.Is(err, auth.ErrTenantMismatch):
					// Cross-tenant token: tenant_id claim does not match the signing
					// key's owning tenant (#158). Distinct reason for observability.
					reason = authFailureReasonTenantMismatch
					httputil.WriteError(w, http.StatusUnauthorized, errCodeInvalidToken, "Token validation failed")
				default:
					reason = authFailureReasonInvalidToken
					httputil.WriteError(w, http.StatusUnauthorized, errCodeInvalidToken, "Token validation failed")
				}
				RecordAuthAttempt(authResultFailure, reason)
				return
			}

			RecordAuthAttempt(pkgmetrics.AuthStatusSuccess, "")

			// Build actor string for audit logging
			actor := claims.TenantID + ":" + claims.Subject

			// Add claims and actor to context using shared helpers
			ctx := auth.WithClaims(r.Context(), claims)
			ctx = context.WithValue(ctx, ActorContextKey, actor)

			logger.Debug().
				Str(logging.LogKeyTenantSlug, claims.TenantID).
				Str("subject", claims.Subject).
				Str("path", r.URL.Path).
				Msg("Request authenticated")

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRole checks that the authenticated user has at least one of the required roles.
// Must be used after AuthMiddleware.
func RequireRole(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := GetClaimsFromContext(r.Context())
			if claims == nil {
				httputil.WriteError(w, http.StatusUnauthorized, errCodeNotAuthenticated, "Authentication required")
				return
			}

			// Check if user has any of the required roles
			if slices.ContainsFunc(roles, claims.HasRole) {
				next.ServeHTTP(w, r)
				return
			}

			// LOG-020: Role-based authorization denial
			zerolog.Ctx(r.Context()).Warn().
				Str(logging.LogKeyTenantSlug, claims.TenantID).Str("subject", claims.Subject).
				Strs("required_roles", roles).Str("path", r.URL.Path).
				Msg("authorization denied: insufficient role")
			RecordAuthorizationDenial(authzDenialInsufficientRole)
			httputil.WriteError(w, http.StatusForbidden, errCodeInsufficientRole, "Required role: "+strings.Join(roles, " or "))
		})
	}
}

// RequireTenant checks that the request is for the authenticated tenant's resources.
// It looks up the tenant by slug (URL param "tenantSlug") and validates the caller's
// JWT tenant_id claim matches — including a time-bounded grace-period window for
// recently-renamed tenants so clients can rotate tokens without an outage.
// holdPeriod bounds how long old-slug JWTs are accepted after a rename.
// Must be used after AuthMiddleware.
func RequireTenant(lookup provisioning.TenantLookupFunc, holdPeriod time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := GetClaimsFromContext(r.Context())
			if claims == nil {
				httputil.WriteError(w, http.StatusUnauthorized, errCodeNotAuthenticated, "Authentication required")
				return
			}

			tenantSlug := chi.URLParam(r, "tenantSlug")
			if tenantSlug == "" {
				next.ServeHTTP(w, r)
				return
			}

			// (1) Admin/system roles may act on ANY tenant — the ownership check is skipped — but the
			// tenant is still RESOLVED so its UUID is stashed for handlers keyed by it (webhooks,
			// connections), uniform with every other tenant resource (ADR-0033). Skipping the stash
			// here is what previously left those two resources unreachable by operators. A missing or
			// soft-deleted tenant 404s at the middleware, as it already does at the service layer
			// (every admin handler resolves via GetBySlug, which filters deleted_at IS NULL).
			if claims.HasRole(provisioning.ActorTypeAdmin) || claims.HasRole(provisioning.ActorTypeSystem) {
				tenant, err := lookup(r.Context(), tenantSlug)
				if err != nil {
					if errors.Is(err, provisioning.ErrTenantNotFound) {
						httputil.WriteError(w, http.StatusNotFound, errCodeTenantNotFound, "Tenant not found")
						return
					}
					zerolog.Ctx(r.Context()).Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("tenant lookup failed")
					httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, "Service temporarily unavailable")
					return
				}
				// Operator: no tenant claim — the effective registry slug is the tenant's current slug.
				next.ServeHTTP(w, r.WithContext(stashTenantIdentity(r.Context(), tenant, tenant.Slug)))
				return
			}

			// (2) Look up the tenant by slug. No caching — intentional for this low-frequency admin API.
			tenant, err := lookup(r.Context(), tenantSlug)
			if err != nil {
				if errors.Is(err, provisioning.ErrTenantNotFound) {
					httputil.WriteError(w, http.StatusNotFound, errCodeTenantNotFound, "Tenant not found")
					return
				}
				zerolog.Ctx(r.Context()).Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("tenant lookup failed")
				httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, "Service temporarily unavailable")
				return
			}

			// (3) Direct slug match.
			if claims.TenantID == tenant.Slug {
				next.ServeHTTP(w, r.WithContext(stashTenantIdentity(r.Context(), tenant, claims.TenantID)))
				return
			}

			// (4) Grace-period: accept old-slug tokens only within the configured hold window.
			// Requires StatusActive so deprovisioning/deleted tenants cannot use old tokens.
			// Requires SlugRenamedAt so the window is always time-bounded — a zero/nil timestamp
			// means no rename occurred and the grace period does not apply.
			if tenant.Status == provisioning.StatusActive &&
				tenant.SlugRenameState == provisioning.SlugRenameStateComplete &&
				tenant.PreviousSlug != "" &&
				tenant.SlugRenamedAt != nil &&
				time.Since(*tenant.SlugRenamedAt) < holdPeriod &&
				claims.TenantID == tenant.PreviousSlug {
				next.ServeHTTP(w, r.WithContext(stashTenantIdentity(r.Context(), tenant, claims.TenantID)))
				return
			}

			// (5) Mismatch — deny.
			zerolog.Ctx(r.Context()).Warn().
				Str("jwt_tenant", claims.TenantID).
				Str("requested_tenant", tenantSlug).
				Str("path", r.URL.Path).
				Msg("authorization denied: tenant mismatch")
			RecordAuthorizationDenial(authzDenialTenantMismatch)
			httputil.WriteError(w, http.StatusForbidden, errCodeTenantMismatch, "Cannot access other tenant's resources")
		})
	}
}

// GetClaimsFromContext retrieves JWT claims from context.
func GetClaimsFromContext(ctx context.Context) *auth.Claims {
	return auth.GetClaims(ctx)
}

// GetActorFromContext retrieves the actor string from context.
func GetActorFromContext(ctx context.Context) string {
	actor, _ := ctx.Value(ActorContextKey).(string)
	return actor
}

// stashTenantIdentity stores the validated tenant's UUID (identity-of-record, control plane) and
// its effective data-path slug (registry keying) in ctx. Called by RequireTenant after a successful
// match. effectiveSlug differs by caller: a tenant passes its own claims.TenantID (the connect-time
// slug, correct for the connection registry even during a rename grace window); an operator passes
// the tenant's current slug (operators carry no tenant claim). Handlers read these via
// getTenantUUIDFromContext / getEffectiveTenantSlug rather than from claims.
func stashTenantIdentity(ctx context.Context, tenant *provisioning.Tenant, effectiveSlug string) context.Context {
	ctx = context.WithValue(ctx, tenantUUIDContextKey, tenant.ID)
	return context.WithValue(ctx, tenantSlugContextKey, effectiveSlug)
}

// getTenantUUIDFromContext returns the validated tenant UUID stashed by RequireTenant, or "" when
// absent (no validated tenant context). Handlers that write UUID-keyed records (webhooks) MUST use
// this, not claims.TenantID.
func getTenantUUIDFromContext(r *http.Request) string {
	v, _ := r.Context().Value(tenantUUIDContextKey).(string)
	return v
}

// getEffectiveTenantSlug returns the effective data-path slug stashed by RequireTenant, or "" when
// absent. Handlers that read the connection registry (data plane) MUST use this: for a tenant
// caller it is the connect-time slug (claims.TenantID), and for an operator it is the tenant's
// current slug — so operators can manage any tenant's connections, which reading claims (empty for
// admin JWTs) could not support.
func getEffectiveTenantSlug(r *http.Request) string {
	v, _ := r.Context().Value(tenantSlugContextKey).(string)
	return v
}

// RateLimitMiddleware applies global and per-IP token-bucket rate limits.
// Global limit: requestsPerMinute/60 rps with burst = requestsPerMinute.
// Per-IP limit: 2x per-second rate of global, burst = requestsPerMinute/2.
// Per-IP entries are lazily created and bounded to perIPRateLimitMaxEntries.
func RateLimitMiddleware(requestsPerMinute int) func(http.Handler) http.Handler {
	globalLimiter := rate.NewLimiter(rate.Limit(float64(requestsPerMinute)/60.0), requestsPerMinute)

	perSecond := float64(requestsPerMinute) / 60.0
	ipRate := rate.Limit(perSecond * 2)
	ipBurst := max(requestsPerMinute/2, 1)
	var ipLimiters sync.Map // map[string]*rate.Limiter

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Global rate limit
			if !globalLimiter.Allow() {
				// Retry-After = refill time for one token (§IX; revocation-handler parallel).
				httputil.WriteRateLimited(w, time.Duration(float64(time.Second)*60.0/float64(requestsPerMinute)),
					errCodeRateLimited, "API rate limit exceeded")
				return
			}

			// Per-IP rate limit
			ip := httputil.GetClientIP(r)
			v, _ := ipLimiters.LoadOrStore(ip, rate.NewLimiter(ipRate, ipBurst))
			if limiter, ok := v.(*rate.Limiter); ok && !limiter.Allow() {
				// Per-IP rate is 2x the global per-second rate → half the refill time.
				httputil.WriteRateLimited(w, time.Duration(float64(time.Second)/(perSecond*2)),
					errCodeRateLimited, "Per-IP rate limit exceeded")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
