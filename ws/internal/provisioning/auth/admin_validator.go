package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

// Prometheus metrics for admin JWT authentication.
var adminAuthTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "provisioning_admin_jwt_auth_total",
	Help: "Total admin JWT authentication attempts by result",
}, []string{"result"})

// Auth result labels for Prometheus metrics.
const (
	resultSuccess          = "success"
	resultExpired          = "expired"
	resultMaxLifetime      = "max_lifetime"
	resultRevoked          = "revoked"
	resultUnknownKey       = "unknown_key"
	resultInvalidSignature = "invalid_signature"
	resultWrongIssuer      = "wrong_issuer"
	resultMissingClaims    = "missing_claims"
	resultInvalid          = "invalid"
)

// AdminJWTIssuer is the JWT issuer claim value for admin tokens.
// Must match the value used by the admin JWT middleware and tester auth provider.
const AdminJWTIssuer = "sukko-admin"

// BootstrapAdminKeyID is the default key ID for the admin bootstrap key.
const BootstrapAdminKeyID = "bootstrap-0"

// AdminValidator validates admin JWTs using the shared ValidateJWT core
// with admin-specific options (issuer check, max lifetime, leeway).
// Constitution X: lives in provisioning package — only provisioning validates admin JWTs.
type AdminValidator struct {
	registry *AdminKeyRegistry
	opts     sharedauth.ValidateOpts
}

// NewAdminValidator creates an AdminValidator wrapping the given key registry.
func NewAdminValidator(registry *AdminKeyRegistry) *AdminValidator {
	return &AdminValidator{
		registry: registry,
		opts: sharedauth.ValidateOpts{
			KeyResolver:       registry,
			AllowedIssuers:    []string{AdminJWTIssuer},
			AllowedAlgorithms: []string{"EdDSA", "RS256"},
			Leeway:            30 * time.Second,
			MaxLifetime:       24 * time.Hour,
			RequireClaims:     []string{"iss", "sub", "exp", "iat"},
			// Admin JWTs are not tenant-scoped (no tenant_id claim; admin keys
			// have no owning tenant), so the tenant-UUID binding must be disabled.
			DisableTenantBinding: true,
		},
	}
}

// ValidateToken validates an admin JWT and returns the claims.
// Records Prometheus metrics for all outcomes.
func (v *AdminValidator) ValidateToken(ctx context.Context, tokenString string) (*sharedauth.Claims, error) {
	claims, err := sharedauth.ValidateJWT(ctx, tokenString, v.opts)
	if err != nil {
		label := classifyError(err)
		adminAuthTotal.WithLabelValues(label).Inc()
		return nil, fmt.Errorf("admin JWT validation: %w", err)
	}

	adminAuthTotal.WithLabelValues(resultSuccess).Inc()
	return claims, nil
}

// classifyError maps a validation error to a Prometheus metric label.
func classifyError(err error) string {
	msg := err.Error()

	switch {
	case containsAny(msg, "issuer", "not allowed"):
		return resultWrongIssuer
	case containsAny(msg, "key revoked"):
		return resultRevoked
	case containsAny(msg, "key not found"):
		return resultUnknownKey
	case containsAny(msg, "expired"):
		return resultExpired
	case containsAny(msg, "lifetime", "exceeds maximum"):
		return resultMaxLifetime
	case containsAny(msg, "missing"):
		return resultMissingClaims
	case containsAny(msg, "signature", "algorithm"):
		return resultInvalidSignature
	default:
		return resultInvalid
	}
}

// containsAny returns true if s contains any of the substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
