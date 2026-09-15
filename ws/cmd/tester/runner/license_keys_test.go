package runner

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// pkcs8PEM PKCS#8-marshals a private key and wraps it in a PEM block.
func pkcs8PEM(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS#8: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func TestNewLicenseKeyGeneratorFromBytes_ValidP256(t *testing.T) {
	t.Parallel()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate P-256 key: %v", err)
	}
	pemBytes := pkcs8PEM(t, priv)

	gen, err := newLicenseKeyGeneratorFromBytes(pemBytes)
	if err != nil {
		t.Fatalf("newLicenseKeyGeneratorFromBytes() error = %v", err)
	}
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}

	// The generator must produce a signable, non-empty license token.
	token := gen.sign(license.Pro, "Test Org", 24*time.Hour)
	if token == "" {
		t.Fatal("expected non-empty signed token")
	}
}

func TestNewLicenseKeyGeneratorFromBytes_Invalid(t *testing.T) {
	t.Parallel()

	// Ed25519 PKCS#8 PEM — wrong key type.
	_, ed25519Priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	ed25519PEM := pkcs8PEM(t, ed25519Priv)

	// P-384 PKCS#8 PEM — ECDSA but wrong curve.
	p384Key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("generate P-384 key: %v", err)
	}
	p384PEM := pkcs8PEM(t, p384Key)

	tests := []struct {
		name string
		key  []byte
	}{
		{"not PEM", []byte("this is not a PEM block")},
		{"empty", []byte{}},
		{"nil", nil},
		{"Ed25519 PKCS#8 PEM", ed25519PEM},
		{"P-384 PKCS#8 PEM", p384PEM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gen, err := newLicenseKeyGeneratorFromBytes(tt.key)
			if err == nil {
				t.Fatalf("expected error, got nil (gen=%v)", gen)
			}
		})
	}
}
