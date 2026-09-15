package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

// signAdminJWT creates a signed admin JWT for testing.
func signAdminJWT(t *testing.T, kid, sub string, privateKey ed25519.PrivateKey, overrides map[string]any) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": "sukko-admin",
		"sub": sub,
		"exp": jwt.NewNumericDate(now.Add(5 * time.Minute)),
		"iat": jwt.NewNumericDate(now),
		"jti": "test-jti-123",
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = kid

	signed, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return signed
}

func setupValidator(t *testing.T) (*AdminValidator, ed25519.PublicKey, ed25519.PrivateKey, string) {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	keyID := "ak_testkey1"

	reg := NewAdminKeyRegistry()
	reg.Refresh([]*sharedauth.KeyInfo{{
		KeyID:     keyID,
		Algorithm: "EdDSA",
		PublicKey: pub,
		IsActive:  true,
	}})

	return NewAdminValidator(reg), pub, priv, keyID
}

func TestAdminValidator_ValidToken(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	token := signAdminJWT(t, kid, "alice", priv, nil)
	claims, err := v.ValidateToken(context.Background(), token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sub, _ := claims.GetSubject()
	if sub != "alice" {
		t.Errorf("sub = %q, want alice", sub)
	}
}

func TestAdminValidator_ExpiredToken(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	token := signAdminJWT(t, kid, "alice", priv, map[string]any{
		"exp": jwt.NewNumericDate(time.Now().Add(-10 * time.Minute)),
		"iat": jwt.NewNumericDate(time.Now().Add(-15 * time.Minute)),
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestAdminValidator_WrongIssuer(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	token := signAdminJWT(t, kid, "alice", priv, map[string]any{
		"iss": "wrong-issuer",
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestAdminValidator_MissingIssuer(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	token := signAdminJWT(t, kid, "alice", priv, map[string]any{
		"iss": nil, // remove iss
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for missing issuer")
	}
}

func TestAdminValidator_UnknownKey(t *testing.T) {
	t.Parallel()
	v, _, priv, _ := setupValidator(t)

	token := signAdminJWT(t, "ak_unknown", "alice", priv, nil)
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestAdminValidator_RevokedKey(t *testing.T) {
	t.Parallel()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	kid := "ak_revoked"
	now := time.Now()

	reg := NewAdminKeyRegistry()
	reg.Refresh([]*sharedauth.KeyInfo{{
		KeyID:     kid,
		Algorithm: "EdDSA",
		PublicKey: pub,
		IsActive:  false,
		RevokedAt: &now,
	}})

	v := NewAdminValidator(reg)
	token := signAdminJWT(t, kid, "alice", priv, nil)
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for revoked key")
	}
}

func TestAdminValidator_MaxLifetimeExceeded(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	now := time.Now()
	token := signAdminJWT(t, kid, "alice", priv, map[string]any{
		"iat": jwt.NewNumericDate(now),
		"exp": jwt.NewNumericDate(now.Add(48 * time.Hour)), // 48h > 24h max
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for max lifetime exceeded")
	}
}

func TestAdminValidator_TenantJWTRejected(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	// A tenant JWT has no iss or different iss — should be rejected
	token := signAdminJWT(t, kid, "tenant-app", priv, map[string]any{
		"iss":       "tenant-issuer",
		"tenant_id": "acme",
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error: tenant JWT should be rejected by admin validator")
	}
}

func TestAdminValidator_MissingSub(t *testing.T) {
	t.Parallel()
	v, _, priv, kid := setupValidator(t)

	token := signAdminJWT(t, kid, "", priv, map[string]any{
		"sub": nil, // remove sub
	})
	_, err := v.ValidateToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected error for missing sub claim")
	}
}

func TestClassifyError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		errMsg   string
		expected string
	}{
		{"wrong issuer", `invalid token: issuer "wrong" not allowed`, resultWrongIssuer},
		{"key revoked", "key revoked: ak_xxx", resultRevoked},
		{"key not found", "key not found: ak_xxx", resultUnknownKey},
		{"expired token", "token expired", resultExpired},
		{"max lifetime", "token lifetime 48h exceeds maximum 24h", resultMaxLifetime},
		{"missing claim", "missing iss claim", resultMissingClaims},
		{"invalid signature", "algorithm mismatch: token=RS256, key=EdDSA", resultInvalidSignature},
		{"unknown error", "something unexpected", resultInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := classifyError(fmt.Errorf("%s", tt.errMsg))
			if got != tt.expected {
				t.Errorf("classifyError(%q) = %q, want %q", tt.errMsg, got, tt.expected)
			}
		})
	}
}
