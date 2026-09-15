package auth

import (
	"crypto/ecdsa"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

func testKeypair(t *testing.T) *Keypair {
	t.Helper()
	kp, err := GenerateKeypair("abcd1234")
	if err != nil {
		t.Fatalf("GenerateKeypair: %v", err)
	}
	return kp
}

func TestMinter_Mint(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{
		Keypair:  kp,
		TenantID: "test-tenant",
		Lifetime: 15 * time.Minute,
	})

	tokenStr, err := m.Mint(42)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Parse and verify with the public key
	claims := &sharedauth.Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return &kp.PrivateKey.PublicKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse JWT: %v", err)
	}
	if !token.Valid {
		t.Fatal("token is not valid")
	}

	// Verify claims
	if claims.TenantID != "test-tenant" {
		t.Errorf("tenant_id = %q, want %q", claims.TenantID, "test-tenant")
	}
	if claims.Subject != "tester-abcd1234-0042" {
		t.Errorf("sub = %q, want %q", claims.Subject, "tester-abcd1234-0042")
	}
	if len(claims.Roles) != 0 {
		t.Errorf("roles = %v, want empty", claims.Roles)
	}

	// Verify kid header
	kid, ok := token.Header["kid"].(string)
	if !ok || kid != kp.KeyID {
		t.Errorf("kid = %q, want %q", kid, kp.KeyID)
	}

	// Verify alg
	if token.Method.Alg() != "ES256" {
		t.Errorf("alg = %q, want ES256", token.Method.Alg())
	}
}

func TestMinter_UniqueSub(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	t1, _ := m.Mint(0)
	t2, _ := m.Mint(1)

	if t1 == t2 {
		t.Error("two minted tokens are identical")
	}

	// Parse subs
	sub1 := parseSub(t, t1, &kp.PrivateKey.PublicKey)
	sub2 := parseSub(t, t2, &kp.PrivateKey.PublicKey)
	if sub1 == sub2 {
		t.Errorf("subs are identical: %q", sub1)
	}
	if !strings.HasSuffix(sub1, "-0000") {
		t.Errorf("sub1 = %q, want suffix -0000", sub1)
	}
	if !strings.HasSuffix(sub2, "-0001") {
		t.Errorf("sub2 = %q, want suffix -0001", sub2)
	}
}

func TestMinter_TokenFunc(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})
	fn := m.TokenFunc()

	token := fn(5)
	if token == "" {
		t.Fatal("TokenFunc returned empty string")
	}

	sub := parseSub(t, token, &kp.PrivateKey.PublicKey)
	if !strings.HasSuffix(sub, "-0005") {
		t.Errorf("sub = %q, want suffix -0005", sub)
	}
}

func TestMinter_MintExpired(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintExpired(0)
	if err != nil {
		t.Fatalf("MintExpired: %v", err)
	}

	// Parsing should fail with token expired error
	claims := &sharedauth.Claims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return &kp.PrivateKey.PublicKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
	if !strings.Contains(err.Error(), "token is expired") {
		t.Errorf("expected expired error, got: %v", err)
	}
}

func TestMinter_MintWithKid(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithKid(0, "custom-kid-value")
	if err != nil {
		t.Fatalf("MintWithKid: %v", err)
	}

	// Parse without validation to check kid header
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenStr, &sharedauth.Claims{})
	if err != nil {
		t.Fatalf("ParseUnverified: %v", err)
	}

	kid, ok := token.Header["kid"].(string)
	if !ok || kid != "custom-kid-value" {
		t.Errorf("kid = %q, want %q", kid, "custom-kid-value")
	}
}

func TestMinter_MintWithTenant(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "original-tenant"})

	tokenStr, err := m.MintWithTenant(0, "other-tenant")
	if err != nil {
		t.Fatalf("MintWithTenant: %v", err)
	}

	claims := &sharedauth.Claims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return &kp.PrivateKey.PublicKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse JWT: %v", err)
	}

	if claims.TenantID != "other-tenant" {
		t.Errorf("tenant_id = %q, want %q", claims.TenantID, "other-tenant")
	}
}

func TestMinter_DefaultLifetime(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	if m.lifetime != defaultJWTLifetime {
		t.Errorf("lifetime = %v, want %v", m.lifetime, defaultJWTLifetime)
	}
}

func TestMinter_MintWithClaims_CustomSubject(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "custom-user"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	sub := parseSub(t, tokenStr, &kp.PrivateKey.PublicKey)
	if sub != "custom-user" {
		t.Errorf("sub = %q, want %q", sub, "custom-user")
	}
}

func TestMinter_MintWithClaims_DefaultSubject(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithClaims(MintOptions{ConnIndex: 7})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	sub := parseSub(t, tokenStr, &kp.PrivateKey.PublicKey)
	if !strings.HasSuffix(sub, "-0007") {
		t.Errorf("sub = %q, want suffix -0007", sub)
	}
}

func TestMinter_MintWithClaims_TenantOverride(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "default-tenant"})

	tokenStr, err := m.MintWithClaims(MintOptions{TenantID: "override-tenant", Subject: "u1"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.TenantID != "override-tenant" {
		t.Errorf("tenant_id = %q, want %q", claims.TenantID, "override-tenant")
	}
}

func TestMinter_MintWithClaims_DefaultTenant(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "my-tenant"})

	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "u1"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.TenantID != "my-tenant" {
		t.Errorf("tenant_id = %q, want %q", claims.TenantID, "my-tenant")
	}
}

func TestMinter_MintWithClaims_GroupsAndRoles(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithClaims(MintOptions{
		Subject: "u1",
		Groups:  []string{"vip", "traders"},
		Roles:   []string{"admin"},
	})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if len(claims.Groups) != 2 || claims.Groups[0] != "vip" || claims.Groups[1] != "traders" {
		t.Errorf("groups = %v, want [vip traders]", claims.Groups)
	}
	if len(claims.Roles) != 1 || claims.Roles[0] != "admin" {
		t.Errorf("roles = %v, want [admin]", claims.Roles)
	}
}

func TestMinter_MintWithClaims_JTI(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "u1", JTI: "my-jti-123"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.ID != "my-jti-123" {
		t.Errorf("jti = %q, want %q", claims.ID, "my-jti-123")
	}
}

func TestMinter_MintWithClaims_JTI_DefaultsToGenerated(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "u1"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	// jti defaults to a generated value when not supplied — the gateway rejects tenant
	// tokens without one (ErrMissingJTI).
	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.ID == "" {
		t.Error("jti = empty, want a generated jti")
	}
}

func TestMinter_MintWithClaims_IssuedAt(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	customIAT := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "u1", IssuedAt: customIAT})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.IssuedAt == nil {
		t.Fatal("expected iat to be set")
	}
	if !claims.IssuedAt.Equal(customIAT) {
		t.Errorf("iat = %v, want %v", claims.IssuedAt, customIAT)
	}
}

func TestMinter_MintWithClaims_IssuedAt_Zero(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	before := time.Now().Add(-1 * time.Second)
	tokenStr, err := m.MintWithClaims(MintOptions{Subject: "u1"})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}
	after := time.Now().Add(1 * time.Second)

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.IssuedAt == nil {
		t.Fatal("expected iat to be set")
	}
	if claims.IssuedAt.Before(before) || claims.IssuedAt.After(after) {
		t.Errorf("iat = %v, expected ≈ now (between %v and %v)", claims.IssuedAt, before, after)
	}
}

func TestMinter_MintWithClaims_AllNewFields(t *testing.T) {
	t.Parallel()

	kp := testKeypair(t)
	m := NewMinter(MinterConfig{Keypair: kp, TenantID: "t1"})

	customIAT := time.Now().Add(-1 * time.Hour)
	tokenStr, err := m.MintWithClaims(MintOptions{
		Subject:  "alice",
		JTI:      "tok-456",
		IssuedAt: customIAT,
		TenantID: "t2",
		Groups:   []string{"vip"},
		Roles:    []string{"admin"},
	})
	if err != nil {
		t.Fatalf("MintWithClaims: %v", err)
	}

	claims := parseClaims(t, tokenStr, &kp.PrivateKey.PublicKey)
	if claims.Subject != "alice" {
		t.Errorf("sub = %q, want alice", claims.Subject)
	}
	if claims.ID != "tok-456" {
		t.Errorf("jti = %q, want tok-456", claims.ID)
	}
	if !claims.IssuedAt.Time.Truncate(time.Second).Equal(customIAT.Truncate(time.Second)) {
		t.Errorf("iat = %v, want %v", claims.IssuedAt.Time, customIAT)
	}
	if claims.TenantID != "t2" {
		t.Errorf("tenant_id = %q, want t2", claims.TenantID)
	}
}

func parseClaims(t *testing.T, tokenStr string, pubKey *ecdsa.PublicKey) *sharedauth.Claims {
	t.Helper()
	claims := &sharedauth.Claims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return pubKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse JWT: %v", err)
	}
	return claims
}

func parseSub(t *testing.T, tokenStr string, pubKey *ecdsa.PublicKey) string {
	t.Helper()
	claims := &sharedauth.Claims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return pubKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse JWT for sub: %v", err)
	}
	return claims.Subject
}
