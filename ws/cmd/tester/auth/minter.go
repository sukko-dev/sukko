package auth

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// defaultJWTLifetime is the default JWT expiration duration.
const defaultJWTLifetime = 15 * time.Minute

// MinterConfig configures JWT minting for a test run.
type MinterConfig struct {
	// Keypair is the ES256 signing keypair.
	Keypair *Keypair

	// TenantID is the tenant identifier placed in the JWT claims.
	TenantID string

	// Lifetime is the JWT expiration duration. Defaults to 15m if zero.
	Lifetime time.Duration
}

// Minter creates signed JWTs for tester WebSocket connections.
// Each connection gets a unique sub claim for realistic simulation.
type Minter struct {
	keypair  *Keypair
	tenantID string
	lifetime time.Duration
}

// NewMinter creates a Minter from the given configuration.
func NewMinter(cfg MinterConfig) *Minter {
	lifetime := cfg.Lifetime
	if lifetime <= 0 {
		lifetime = defaultJWTLifetime
	}
	return &Minter{
		keypair:  cfg.Keypair,
		tenantID: cfg.TenantID,
		lifetime: lifetime,
	}
}

// Mint creates a signed JWT for the given connection index.
// Each connection gets a unique sub: "tester-{testID}-{connIndex:04d}".
func (m *Minter) Mint(connIndex int) (string, error) {
	return m.mintWithOverrides(connIndex, m.tenantID, m.keypair.KeyID, time.Now().Add(m.lifetime))
}

// TokenFunc returns a function that mints a JWT for a given connection index.
// Panics on signing error — acceptable for a test tool where key generation
// is validated at setup time.
func (m *Minter) TokenFunc() func(int) string {
	return func(connIndex int) string {
		token, err := m.Mint(connIndex)
		if err != nil {
			panic(fmt.Sprintf("mint JWT for conn %d: %v", connIndex, err))
		}
		return token
	}
}

// MintExpired creates a JWT with an expiration time in the past.
// Used for auth rejection testing.
func (m *Minter) MintExpired(connIndex int) (string, error) {
	return m.mintWithOverrides(connIndex, m.tenantID, m.keypair.KeyID, time.Now().Add(-1*time.Hour))
}

// MintWithKid creates a JWT with a custom kid header value.
// Used for auth rejection testing — wrong/nonexistent key.
func (m *Minter) MintWithKid(connIndex int, kid string) (string, error) {
	return m.mintWithOverrides(connIndex, m.tenantID, kid, time.Now().Add(m.lifetime))
}

// MintWithTenant creates a JWT with a custom tenant_id claim.
// Used for auth rejection testing — tenant mismatch.
func (m *Minter) MintWithTenant(connIndex int, tenantID string) (string, error) {
	return m.mintWithOverrides(connIndex, tenantID, m.keypair.KeyID, time.Now().Add(m.lifetime))
}

// MintOptions configures custom JWT claims for pub-sub testing.
// Zero values use defaults (auto-generated subject, minter's tenant, no groups/roles).
type MintOptions struct {
	ConnIndex int
	TenantID  string    // override tenant (empty = use minter's default)
	Groups    []string  // JWT groups claim (for group-scoped channel access)
	Roles     []string  // JWT roles claim (for RBAC)
	Subject   string    // override subject (empty = auto-generate from connIndex)
	JTI       string    // JWT ID claim (empty = auto-generated; gateway mandates a jti)
	IssuedAt  time.Time // override iat (zero = use now)
}

// MintWithClaims creates a JWT with custom groups, roles, and subject.
// Used by the pub-sub engine for multi-user scoping tests.
func (m *Minter) MintWithClaims(opts MintOptions) (string, error) {
	tenantID := opts.TenantID
	if tenantID == "" {
		tenantID = m.tenantID
	}
	subject := opts.Subject
	if subject == "" {
		subject = fmt.Sprintf("tester-%s-%04d", strings.TrimPrefix(m.keypair.KeyID, "tester-"), opts.ConnIndex)
	}

	now := time.Now()
	iat := now
	if !opts.IssuedAt.IsZero() {
		iat = opts.IssuedAt
	}
	// The gateway mandates a jti on tenant JWTs and rejects tokens without one
	// (ErrMissingJTI) — so default to a generated jti. Callers that need a specific jti
	// (e.g. replay/revocation tests) still override it via opts.JTI.
	jti := opts.JTI
	if jti == "" {
		jti = uuid.NewString()
	}
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			ExpiresAt: jwt.NewNumericDate(now.Add(m.lifetime)),
			IssuedAt:  jwt.NewNumericDate(iat),
			ID:        jti,
		},
		TenantID: tenantID,
		Groups:   opts.Groups,
		Roles:    opts.Roles,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = m.keypair.KeyID

	signed, err := token.SignedString(m.keypair.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}

	return signed, nil
}

func (m *Minter) mintWithOverrides(connIndex int, tenantID, kid string, exp time.Time) (string, error) {
	now := time.Now()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   fmt.Sprintf("tester-%s-%04d", strings.TrimPrefix(m.keypair.KeyID, "tester-"), connIndex),
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        uuid.NewString(), // gateway mandates a jti on tenant JWTs
		},
		TenantID: tenantID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = kid

	signed, err := token.SignedString(m.keypair.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("sign JWT: %w", err)
	}

	return signed, nil
}
