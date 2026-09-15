package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// createTestToken creates a signed JWT token for testing.
func createTestToken(t *testing.T, key *KeyInfo, privateKey any, claims *Claims) string {
	t.Helper()

	method, err := GetSigningMethod(key.Algorithm)
	if err != nil {
		t.Fatalf("GetSigningMethod failed: %v", err)
	}

	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = key.KeyID

	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}

	return tokenString
}

func TestMultiTenantValidator_ValidateToken_ES256(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	gotClaims, err := validator.ValidateToken(ctx, tokenString)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if gotClaims.Subject != "user-123" {
		t.Errorf("Subject = %s, want user-123", gotClaims.Subject)
	}
	if gotClaims.TenantID != "acme" {
		t.Errorf("TenantID = %s, want acme", gotClaims.TenantID)
	}
}

func TestMultiTenantValidator_ValidateToken_RS256(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	rsaPEM, privateKey := generateTestRSAKey(t)

	key := &KeyInfo{
		KeyID:        "rsa-key-1",
		TenantID:     "globex",
		Algorithm:    "RS256",
		PublicKeyPEM: rsaPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "app-456",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "globex",
		Roles:    []string{"admin"},
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	gotClaims, err := validator.ValidateToken(ctx, tokenString)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if gotClaims.TenantID != "globex" {
		t.Errorf("TenantID = %s, want globex", gotClaims.TenantID)
	}
	if !gotClaims.HasRole("admin") {
		t.Error("Expected admin role")
	}
}

func TestMultiTenantValidator_ValidateToken_EdDSA(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	edPEM, privateKey := generateTestEdKey(t)

	key := &KeyInfo{
		KeyID:        "ed-key-1",
		TenantID:     "initech",
		Algorithm:    "EdDSA",
		PublicKeyPEM: edPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "service-789",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "initech",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	gotClaims, err := validator.ValidateToken(ctx, tokenString)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if gotClaims.TenantID != "initech" {
		t.Errorf("TenantID = %s, want initech", gotClaims.TenantID)
	}
}

func TestMultiTenantValidator_ValidateToken_ExpiredToken(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// Create expired token
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)), // expired
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	_, err = validator.ValidateToken(ctx, tokenString)
	if !errors.Is(err, ErrTokenExpired) {
		t.Errorf("Expected ErrTokenExpired, got %v", err)
	}
}

func TestMultiTenantValidator_ValidateToken_KeyNotFound(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	// Don't add key to registry
	key := &KeyInfo{
		KeyID:        "unknown-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	_, err = validator.ValidateToken(ctx, tokenString)
	if err == nil {
		t.Error("Expected error for unknown key")
	}
}

func TestMultiTenantValidator_ValidateToken_RevokedKey(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	past := time.Now().Add(-time.Hour)
	key := &KeyInfo{
		KeyID:        "revoked-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
		RevokedAt:    &past,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	_, err = validator.ValidateToken(ctx, tokenString)
	if err == nil {
		t.Error("Expected error for revoked key")
	}
}

func TestMultiTenantValidator_ValidateToken_MissingToken(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	ctx := context.Background()
	_, err = validator.ValidateToken(ctx, "")
	if !errors.Is(err, ErrMissingToken) {
		t.Errorf("Expected ErrMissingToken, got %v", err)
	}
}

func TestMultiTenantValidator_ValidateToken_RequireTenantID(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// Create token WITHOUT tenant_id
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		// No TenantID
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	ctx := context.Background()
	_, err = validator.ValidateToken(ctx, tokenString)
	if err == nil {
		t.Error("Expected error for missing tenant_id")
	}
}

func TestMultiTenantValidator_ValidateTokenForTenant(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)
	ctx := context.Background()

	t.Run("matching tenant", func(t *testing.T) {
		t.Parallel()
		_, err := validator.ValidateTokenForTenant(ctx, tokenString, "acme")
		if err != nil {
			t.Errorf("ValidateTokenForTenant failed: %v", err)
		}
	})

	t.Run("non-matching tenant", func(t *testing.T) {
		t.Parallel()
		_, err := validator.ValidateTokenForTenant(ctx, tokenString, "other")
		if err == nil {
			t.Error("Expected error for tenant mismatch")
		}
	})
}

func TestExtractKeyID(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "extract-test-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "acme",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	kid, err := ExtractKeyID(tokenString)
	if err != nil {
		t.Fatalf("ExtractKeyID failed: %v", err)
	}

	if kid != "extract-test-key" {
		t.Errorf("kid = %s, want extract-test-key", kid)
	}
}

func TestExtractTenantID(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	key := &KeyInfo{
		KeyID:        "test-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "extracted-tenant",
	}

	tokenString := createTestToken(t, key, privateKey, claims)

	tenantID, err := ExtractTenantID(tokenString)
	if err != nil {
		t.Fatalf("ExtractTenantID failed: %v", err)
	}

	if tenantID != "extracted-tenant" {
		t.Errorf("tenantID = %s, want extracted-tenant", tenantID)
	}
}
