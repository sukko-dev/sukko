// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// MultiTenantValidatorConfig configures the MultiTenantValidator.
type MultiTenantValidatorConfig struct {
	// KeyRegistry provides public keys for validation.
	KeyRegistry KeyRegistry

	// RequireTenantID requires tokens to have a tenant_id claim.
	RequireTenantID bool

	// AllowedAlgorithms restricts which algorithms are accepted.
	// Empty means all supported algorithms are allowed.
	AllowedAlgorithms []string

	// AllowedIssuers restricts which token issuers (iss claim) are accepted.
	// Empty means issuer verification is skipped (for backward compatibility).
	AllowedIssuers []string

	// TenantResolver maps a tenant_id claim (slug) to the stable tenant UUID for
	// tenant binding. Required — MultiTenantValidator is always tenant-scoped and
	// always binds. (Admin validation, which is not tenant-scoped, uses a
	// separate ValidateOpts with DisableTenantBinding set, not this constructor.)
	TenantResolver TenantResolver
}

// MultiTenantValidator validates JWTs using tenant-specific public keys.
// Thread-safe for concurrent use.
type MultiTenantValidator struct {
	requireTenantID bool
	opts            ValidateOpts
}

// NewMultiTenantValidator creates a new multi-tenant JWT validator.
func NewMultiTenantValidator(cfg MultiTenantValidatorConfig) (*MultiTenantValidator, error) {
	if cfg.KeyRegistry == nil {
		return nil, errors.New("key registry is required")
	}
	// MultiTenantValidator is always tenant-scoped and always binds the tenant_id
	// claim to the signing key's owning tenant UUID, so a resolver is mandatory.
	// Requiring it here (rather than only failing closed at validation time)
	// surfaces a misconfiguration loudly at startup.
	if cfg.TenantResolver == nil {
		return nil, errors.New("tenant resolver is required")
	}

	var requireClaims []string
	if cfg.RequireTenantID {
		requireClaims = append(requireClaims, "tenant_id")
	}

	return &MultiTenantValidator{
		requireTenantID: cfg.RequireTenantID,
		opts: ValidateOpts{
			KeyResolver:       cfg.KeyRegistry, // KeyRegistry embeds KeyResolver
			AllowedAlgorithms: cfg.AllowedAlgorithms,
			AllowedIssuers:    cfg.AllowedIssuers,
			RequireClaims:     requireClaims,
			TenantResolver:    cfg.TenantResolver,
			// DisableTenantBinding left false — binding enabled (secure default).
		},
	}, nil
}

// ValidateToken validates a JWT token string and returns the claims if valid.
// The token must have a 'kid' header that identifies the signing key in the tenant key registry.
func (v *MultiTenantValidator) ValidateToken(ctx context.Context, tokenString string) (*Claims, error) {
	return ValidateJWT(ctx, tokenString, v.opts)
}

// ValidateTokenForTenant validates a token and ensures it belongs to the specified tenant.
// This is useful for endpoint-specific validation where the tenant is known.
func (v *MultiTenantValidator) ValidateTokenForTenant(ctx context.Context, tokenString, expectedTenant string) (*Claims, error) {
	claims, err := v.ValidateToken(ctx, tokenString)
	if err != nil {
		return nil, err
	}

	if claims.TenantID != expectedTenant {
		// Generic message — do not leak tenant identifiers to clients (§IX);
		// detail belongs in server-side structured logs. errors.Is routes on the
		// shared sentinel, consistent with the binding path's TenantMismatchError.
		return nil, fmt.Errorf("%w", ErrTenantMismatch)
	}

	return claims, nil
}

// ExtractKeyID extracts the key ID from a token without validating it.
// Useful for logging or debugging.
func ExtractKeyID(tokenString string) (string, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, &Claims{})
	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	kidRaw, ok := token.Header["kid"]
	if !ok {
		return "", errors.New("missing kid header")
	}

	kid, ok := kidRaw.(string)
	if !ok {
		return "", errors.New("invalid kid header")
	}

	return kid, nil
}

// ExtractIssuer extracts the issuer from a token without validating it.
// Useful for routing tokens to the correct validator (admin vs tenant).
func ExtractIssuer(tokenString string) (string, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, &Claims{})
	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return "", errors.New("invalid claims type")
	}

	issuer, _ := claims.GetIssuer()
	return issuer, nil
}

// ExtractTenantID extracts the tenant ID from a token without validating it.
// Useful for routing or logging before validation.
func ExtractTenantID(tokenString string) (string, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, &Claims{})
	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return "", errors.New("invalid claims type")
	}

	return claims.TenantID, nil
}
