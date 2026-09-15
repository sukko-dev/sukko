package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
)

// AdminKeyName is the JWT sub claim value for admin tokens issued by this tester instance.
// Used by both NewEphemeralAuthProvider and NewKeypairAuthProvider calls in resolveAdminProvider.
const AdminKeyName = "tester"

// Provider signs HTTP requests with admin credentials.
// Every code path — CLI, tester, unit tests — uses KeypairAuthProvider.
// No noop implementation: every request exercises real JWT signing.
type Provider interface {
	// SignRequest adds an Authorization header with a signed admin JWT.
	SignRequest(req *http.Request)
	// KeyID returns the key ID embedded in admin JWTs signed by this provider.
	// Ephemeral providers return a key ID starting with "ephemeral-".
	KeyID() string
}

// KeypairAuthProvider signs requests with an Ed25519 admin JWT.
// Each call to SignRequest creates a fresh short-lived JWT (5 min expiry)
// with a unique jti.
type KeypairAuthProvider struct {
	privateKey ed25519.PrivateKey
	keyID      string // kid in JWT header
	keyName    string // sub in JWT claims
}

// NewKeypairAuthProvider creates a KeypairAuthProvider with the given keypair identity.
func NewKeypairAuthProvider(privateKey ed25519.PrivateKey, keyID, keyName string) *KeypairAuthProvider {
	return &KeypairAuthProvider{
		privateKey: privateKey,
		keyID:      keyID,
		keyName:    keyName,
	}
}

// NewEphemeralAuthProvider generates a random Ed25519 keypair and returns
// a KeypairAuthProvider for local dev mode only. The ephemeral keypair is
// not pre-registered with provisioning — use TESTER_ADMIN_KEY_FILE for remote mode.
func NewEphemeralAuthProvider() (*KeypairAuthProvider, ed25519.PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ephemeral admin keypair: %w", err)
	}
	provider := &KeypairAuthProvider{
		privateKey: priv,
		keyID:      "ephemeral-" + uuid.NewString()[:8],
		keyName:    AdminKeyName,
	}
	return provider, pub, nil
}

// SignRequest creates a short-lived admin JWT and sets it as the Authorization header.
func (p *KeypairAuthProvider) SignRequest(req *http.Request) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": provauth.AdminJWTIssuer,
		"sub": p.keyName,
		// Provisioning's RequireRole("admin","system") reads the "roles" claim (admin keys
		// carry no server-side role, and ValidateJWT never injects one). Without this, admin
		// operations like tenant creation — including the tester's own throwaway-tenant setup —
		// fail with 403 INSUFFICIENT_ROLE. Mirrors the CLI's admin signer (client/keypair_signer.go).
		"roles": []string{"admin"},
		"exp":   jwt.NewNumericDate(now.Add(5 * time.Minute)),
		"iat":   jwt.NewNumericDate(now),
		"jti":   uuid.NewString(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = p.keyID

	signed, err := token.SignedString(p.privateKey)
	if err != nil {
		return // Ed25519 signing cannot fail with a valid key; unauthenticated request will be rejected by server
	}

	req.Header.Set("Authorization", "Bearer "+signed)
}

// KeyID returns the admin key ID used in JWT headers.
func (p *KeypairAuthProvider) KeyID() string {
	return p.keyID
}

// PublicKey returns the Ed25519 public key (derived from the private key).
func (p *KeypairAuthProvider) PublicKey() ed25519.PublicKey {
	return p.privateKey.Public().(ed25519.PublicKey) //nolint:errcheck // ed25519.PrivateKey.Public() always returns ed25519.PublicKey
}
