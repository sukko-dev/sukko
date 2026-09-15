package auth

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ValidateOpts configures the shared JWT validation behavior.
type ValidateOpts struct {
	// KeyResolver resolves signing keys by key ID (kid header). Required.
	KeyResolver KeyResolver

	// AllowedAlgorithms restricts accepted algorithms. Empty = allow ES256, RS256, EdDSA.
	AllowedAlgorithms []string

	// AllowedIssuers restricts accepted issuers (iss claim). Empty = skip issuer check.
	AllowedIssuers []string

	// Leeway is the clock skew tolerance for expiry validation. Zero = strict.
	Leeway time.Duration

	// MaxLifetime rejects tokens with exp - iat > MaxLifetime. Zero = no limit.
	MaxLifetime time.Duration

	// RequireClaims lists claim names that must be present (non-empty).
	// Supported: "tenant_id", "iss", "exp", "iat", "sub", "jti".
	RequireClaims []string

	// TenantResolver maps the tenant_id claim (slug) to the stable tenant UUID
	// so the token's claimed tenant can be bound to the signing key's owning
	// tenant UUID. Required for tenant-scoped validation (unless the binding is
	// disabled). See DisableTenantBinding.
	TenantResolver TenantResolver

	// DisableTenantBinding turns OFF the tenant-UUID binding. The zero value
	// (false) keeps the binding ENABLED — the secure default, so a caller that
	// forgets to configure it fails closed rather than open. Only the admin
	// validator (not tenant-scoped) sets this true.
	DisableTenantBinding bool
}

// resolvedAlgorithms returns the allowed algorithms map, using defaults if none specified.
func (o *ValidateOpts) resolvedAlgorithms() map[string]bool {
	if len(o.AllowedAlgorithms) > 0 {
		m := make(map[string]bool, len(o.AllowedAlgorithms))
		for _, alg := range o.AllowedAlgorithms {
			m[alg] = true
		}
		return m
	}
	return map[string]bool{"ES256": true, "RS256": true, "EdDSA": true}
}

// ValidateJWT parses, verifies, and validates a JWT token using the provided options.
// This is the shared core used by both MultiTenantValidator (tenant auth) and
// AdminValidator (admin auth). The KeyResolver abstracts the key lookup source.
func ValidateJWT(ctx context.Context, tokenString string, opts ValidateOpts) (*Claims, error) {
	if tokenString == "" {
		return nil, ErrMissingToken
	}
	if opts.KeyResolver == nil {
		return nil, errors.New("auth: KeyResolver is required")
	}

	allowedAlgos := opts.resolvedAlgorithms()

	// Build parser options
	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(slices.Collect(maps.Keys(allowedAlgos))),
	}
	if opts.Leeway > 0 {
		parserOpts = append(parserOpts, jwt.WithLeeway(opts.Leeway))
	}

	parser := jwt.NewParser(parserOpts...)

	// Check issuer from unverified token first (fast reject before key lookup)
	if len(opts.AllowedIssuers) > 0 {
		issuerMap := make(map[string]bool, len(opts.AllowedIssuers))
		for _, iss := range opts.AllowedIssuers {
			issuerMap[iss] = true
		}

		preparse := jwt.NewParser()
		unverified, _, err := preparse.ParseUnverified(tokenString, &Claims{})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
		}
		if claims, ok := unverified.Claims.(*Claims); ok {
			issuer, _ := claims.GetIssuer()
			if issuer == "" || !issuerMap[issuer] {
				return nil, fmt.Errorf("%w: issuer %q not allowed", ErrInvalidToken, issuer)
			}
		}
	}

	// keyTenantUUID / tokenKID capture the owning tenant UUID of the resolved
	// signing key and the token's kid from inside the keyfunc closure, so the
	// tenant binding below can compare against the (resolved) tenant_id claim
	// after signature verification.
	var keyTenantUUID, tokenKID string

	// Parse with signature verification via KeyResolver
	token, err := parser.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		kidRaw, ok := token.Header["kid"]
		if !ok {
			return nil, errors.New("missing kid header")
		}

		kid, ok := kidRaw.(string)
		if !ok || kid == "" {
			return nil, errors.New("invalid kid header")
		}

		alg, ok := token.Header["alg"].(string)
		if !ok {
			return nil, errors.New("missing alg header")
		}
		if !allowedAlgos[alg] {
			return nil, fmt.Errorf("algorithm %s not allowed", alg)
		}

		key, err := opts.KeyResolver.GetKey(ctx, kid)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return nil, fmt.Errorf("key not found: %s", kid)
			}
			if errors.Is(err, ErrKeyRevoked) {
				return nil, fmt.Errorf("key revoked: %s", kid)
			}
			if errors.Is(err, ErrKeyExpired) {
				return nil, fmt.Errorf("key expired: %s", kid)
			}
			return nil, fmt.Errorf("key lookup failed: %w", err)
		}

		if key.Algorithm != alg {
			return nil, fmt.Errorf("algorithm mismatch: token=%s, key=%s", alg, key.Algorithm)
		}

		keyTenantUUID = key.TenantID
		tokenKID = kid
		return key.PublicKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	// MaxLifetime check: reject tokens with exp - iat > MaxLifetime
	if opts.MaxLifetime > 0 {
		iat, _ := claims.GetIssuedAt()
		exp, _ := claims.GetExpirationTime()
		if iat == nil || exp == nil {
			return nil, fmt.Errorf("%w: max lifetime enforcement requires iat and exp claims", ErrInvalidToken)
		}
		lifetime := exp.Sub(iat.Time)
		if lifetime > opts.MaxLifetime {
			return nil, fmt.Errorf("%w: token lifetime %s exceeds maximum %s", ErrInvalidToken, lifetime, opts.MaxLifetime)
		}
	}

	// RequireClaims check
	for _, claim := range opts.RequireClaims {
		switch claim {
		case "tenant_id":
			if claims.TenantID == "" {
				return nil, fmt.Errorf("%w: missing tenant_id claim", ErrInvalidToken)
			}
		case "iss":
			if issuer, _ := claims.GetIssuer(); issuer == "" {
				return nil, fmt.Errorf("%w: missing iss claim", ErrInvalidToken)
			}
		case "sub":
			if sub, _ := claims.GetSubject(); sub == "" {
				return nil, fmt.Errorf("%w: missing sub claim", ErrInvalidToken)
			}
		case "exp":
			if exp, _ := claims.GetExpirationTime(); exp == nil {
				return nil, fmt.Errorf("%w: missing exp claim", ErrInvalidToken)
			}
		case "iat":
			if iat, _ := claims.GetIssuedAt(); iat == nil {
				return nil, fmt.Errorf("%w: missing iat claim", ErrInvalidToken)
			}
		case "jti":
			if claims.ID == "" {
				return nil, fmt.Errorf("%w: missing jti claim", ErrInvalidToken)
			}
		}
	}

	// Tenant binding (Constitution §IX): the tenant_id claim MUST resolve to the
	// same tenant that owns the signing key. Enabled by default (secure); only
	// the admin validator disables it. All failure modes reject (fail closed).
	//
	// Only runs when a tenant_id is present: with RequireTenantID the claim is
	// already mandatory (RequireClaims rejects an empty one above), so binding
	// always runs for tenant-scoped tokens; in single-tenant mode
	// (RequireTenantID=false) a tenant-less token is intentionally allowed and
	// has nothing to bind. A non-empty tenant_id is ALWAYS bound regardless of
	// RequireTenantID, so this never weakens multi-tenant isolation.
	if !opts.DisableTenantBinding && claims.TenantID != "" {
		if opts.TenantResolver == nil {
			return nil, fmt.Errorf("%w: tenant binding enabled but no resolver configured", ErrInvalidToken)
		}
		if keyTenantUUID == "" {
			return nil, fmt.Errorf("%w: signing key has no owning tenant", ErrInvalidToken)
		}
		claimUUID, resolveErr := opts.TenantResolver.ResolveTenantUUID(ctx, claims.TenantID)
		if resolveErr != nil {
			// Preserve the cause (fail closed, but distinguishable from forgery:
			// a transient resolver/DB failure is not a cross-tenant attack). This
			// keeps errors.Is(err, ErrTenantNotResolvable) working for callers.
			return nil, fmt.Errorf("resolve tenant for binding: %w", resolveErr)
		}
		if claimUUID == "" || claimUUID != keyTenantUUID {
			return nil, &TenantMismatchError{
				KID:         tokenKID,
				ClaimedSlug: claims.TenantID,
				ClaimedUUID: claimUUID,
				KeyUUID:     keyTenantUUID,
			}
		}
		claims.ResolvedTenantUUID = claimUUID
	}

	return claims, nil
}
