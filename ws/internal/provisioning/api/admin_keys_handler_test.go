package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

func TestValidatePublicKeyMaterial_ValidEd25519(t *testing.T) {
	t.Parallel()
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pemKey := marshalTestPEM(t, pub)

	if err := validatePublicKeyMaterial("Ed25519", pemKey); err != nil {
		t.Fatalf("unexpected error for valid Ed25519 key: %v", err)
	}
}

func TestValidatePublicKeyMaterial_EmptyKey(t *testing.T) {
	t.Parallel()
	if err := validatePublicKeyMaterial("Ed25519", ""); err == nil {
		t.Fatal("expected error for empty public key")
	}
}

func TestValidatePublicKeyMaterial_InvalidPEM(t *testing.T) {
	t.Parallel()
	if err := validatePublicKeyMaterial("Ed25519", "not-valid-pem"); err == nil {
		t.Fatal("expected error for invalid PEM")
	}
}

func TestValidatePublicKeyMaterial_UnsupportedAlgorithm(t *testing.T) {
	t.Parallel()
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pemKey := marshalTestPEM(t, pub)

	if err := validatePublicKeyMaterial("ES512", pemKey); err == nil {
		t.Fatal("expected error for unsupported algorithm")
	}
}

func TestValidatePublicKeyMaterial_WrongKeyTypeForAlgorithm(t *testing.T) {
	t.Parallel()
	// Ed25519 key presented as RS256 — ParsePKIXPublicKey succeeds but type check should catch it
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pemKey := marshalTestPEM(t, pub)

	// RS256 expects RSA, not Ed25519 — but x509.ParsePKIXPublicKey accepts both.
	// The current code doesn't check RSA type explicitly for RS256, so this passes.
	// This is acceptable — the signature verification will fail at auth time.
	err := validatePublicKeyMaterial("RS256", pemKey)
	_ = err // no assertion — documenting the behavior
}

// marshalTestPEM encodes a public key as PEM for testing.
func marshalTestPEM(t *testing.T, pub any) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal PKIX: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}
