package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"
)

// generateTestECKey generates an ECDSA P-256 key pair for testing.
func generateTestECKey(t *testing.T) (string, *ecdsa.PrivateKey) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate EC key: %v", err)
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("Failed to marshal public key: %v", err)
	}

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	return string(pem.EncodeToMemory(pemBlock)), privateKey
}

// generateTestRSAKey generates an RSA key pair for testing.
func generateTestRSAKey(t *testing.T) (string, *rsa.PrivateKey) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("Failed to marshal public key: %v", err)
	}

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	return string(pem.EncodeToMemory(pemBlock)), privateKey
}

// generateTestEdKey generates an Ed25519 key pair for testing.
func generateTestEdKey(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate Ed25519 key: %v", err)
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("Failed to marshal public key: %v", err)
	}

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	return string(pem.EncodeToMemory(pemBlock)), privateKey
}

func TestParsePublicKey_ES256(t *testing.T) {
	t.Parallel()
	pemData, _ := generateTestECKey(t)

	pub, err := ParsePublicKey(pemData, "ES256")
	if err != nil {
		t.Fatalf("ParsePublicKey failed: %v", err)
	}

	if _, ok := pub.(*ecdsa.PublicKey); !ok {
		t.Errorf("Expected *ecdsa.PublicKey, got %T", pub)
	}
}

func TestParsePublicKey_RS256(t *testing.T) {
	t.Parallel()
	pemData, _ := generateTestRSAKey(t)

	pub, err := ParsePublicKey(pemData, "RS256")
	if err != nil {
		t.Fatalf("ParsePublicKey failed: %v", err)
	}

	if _, ok := pub.(*rsa.PublicKey); !ok {
		t.Errorf("Expected *rsa.PublicKey, got %T", pub)
	}
}

func TestParsePublicKey_EdDSA(t *testing.T) {
	t.Parallel()
	pemData, _ := generateTestEdKey(t)

	pub, err := ParsePublicKey(pemData, "EdDSA")
	if err != nil {
		t.Fatalf("ParsePublicKey failed: %v", err)
	}

	if _, ok := pub.(ed25519.PublicKey); !ok {
		t.Errorf("Expected ed25519.PublicKey, got %T", pub)
	}
}

func TestParsePublicKey_Mismatch(t *testing.T) {
	t.Parallel()
	ecPEM, _ := generateTestECKey(t)

	// Try to parse EC key as RSA
	_, err := ParsePublicKey(ecPEM, "RS256")
	if err == nil {
		t.Error("Expected error for algorithm mismatch")
	}
}

func TestParsePublicKey_InvalidPEM(t *testing.T) {
	t.Parallel()
	_, err := ParsePublicKey("not valid pem", "ES256")
	if err == nil {
		t.Error("Expected error for invalid PEM")
	}
}

func TestParsePublicKey_UnsupportedAlgorithm(t *testing.T) {
	t.Parallel()
	ecPEM, _ := generateTestECKey(t)

	_, err := ParsePublicKey(ecPEM, "HS256")
	if err == nil {
		t.Error("Expected error for unsupported algorithm")
	}
}

func TestKeyInfo_IsValid(t *testing.T) {
	t.Parallel()
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	tests := []struct {
		name     string
		key      *KeyInfo
		expected bool
	}{
		{
			name:     "active key with no expiry",
			key:      &KeyInfo{IsActive: true},
			expected: true,
		},
		{
			name:     "active key with future expiry",
			key:      &KeyInfo{IsActive: true, ExpiresAt: &future},
			expected: true,
		},
		{
			name:     "active key with past expiry",
			key:      &KeyInfo{IsActive: true, ExpiresAt: &past},
			expected: false,
		},
		{
			name:     "inactive key",
			key:      &KeyInfo{IsActive: false},
			expected: false,
		},
		{
			name:     "revoked key",
			key:      &KeyInfo{IsActive: true, RevokedAt: &past},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.key.IsValid(); got != tt.expected {
				t.Errorf("KeyInfo.IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestStaticKeyRegistry_GetKey(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()

	ecPEM, _ := generateTestECKey(t)
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

	ctx := context.Background()

	t.Run("existing key", func(t *testing.T) {
		t.Parallel()
		got, err := registry.GetKey(ctx, "test-key-1")
		if err != nil {
			t.Fatalf("GetKey failed: %v", err)
		}
		if got.KeyID != "test-key-1" {
			t.Errorf("KeyID = %s, want test-key-1", got.KeyID)
		}
		if got.PublicKey == nil {
			t.Error("PublicKey should be parsed")
		}
	})

	t.Run("non-existing key", func(t *testing.T) {
		t.Parallel()
		_, err := registry.GetKey(ctx, "non-existent")
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})
}

func TestStaticKeyRegistry_GetKey_Revoked(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()

	ecPEM, _ := generateTestECKey(t)
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

	ctx := context.Background()
	_, err := registry.GetKey(ctx, "revoked-key")
	if !errors.Is(err, ErrKeyRevoked) {
		t.Errorf("Expected ErrKeyRevoked, got %v", err)
	}
}

func TestStaticKeyRegistry_GetKey_Expired(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()

	ecPEM, _ := generateTestECKey(t)
	past := time.Now().Add(-time.Hour)
	key := &KeyInfo{
		KeyID:        "expired-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
		ExpiresAt:    &past,
	}

	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	ctx := context.Background()
	_, err := registry.GetKey(ctx, "expired-key")
	if !errors.Is(err, ErrKeyExpired) {
		t.Errorf("Expected ErrKeyExpired, got %v", err)
	}
}

func TestStaticKeyRegistry_GetKeysByTenantUUID(t *testing.T) {
	t.Parallel()
	registry := NewStaticKeyRegistry()

	ecPEM, _ := generateTestECKey(t)

	// Add multiple keys for same tenant
	for i := 1; i <= 3; i++ {
		key := &KeyInfo{
			KeyID:        "acme-key-" + string(rune('0'+i)),
			TenantID:     "acme",
			Algorithm:    "ES256",
			PublicKeyPEM: ecPEM,
			IsActive:     true,
		}
		if err := registry.AddKey(key); err != nil {
			t.Fatalf("AddKey failed: %v", err)
		}
	}

	// Add key for different tenant
	key := &KeyInfo{
		KeyID:        "other-key",
		TenantID:     "other",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	ctx := context.Background()
	keys, err := registry.GetKeysByTenantUUID(ctx, "acme")
	if err != nil {
		t.Fatalf("GetKeysByTenantUUID failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 keys for acme, got %d", len(keys))
	}
}

func TestGetSigningMethod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		algorithm string
		wantErr   bool
	}{
		{"ES256", false},
		{"RS256", false},
		{"EdDSA", false},
		{"HS256", true},
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.algorithm, func(t *testing.T) {
			t.Parallel()
			_, err := GetSigningMethod(tt.algorithm)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSigningMethod(%s) error = %v, wantErr %v", tt.algorithm, err, tt.wantErr)
			}
		})
	}
}
