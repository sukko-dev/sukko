package auth

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

// p256PKCS8PEM returns a freshly generated valid P-256 PKCS#8 PEM.
func p256PKCS8PEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate P-256 key: %v", err)
	}
	return marshalPKCS8PEM(t, key)
}

// marshalPKCS8PEM PKCS#8-marshals any private key and wraps it in a PEM block.
func marshalPKCS8PEM(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS#8: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func TestParseECDSAP256PrivateKeyPEM(t *testing.T) {
	t.Parallel()

	// Ed25519 PKCS#8 PEM — wrong key type.
	_, ed25519Priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	ed25519PEM := marshalPKCS8PEM(t, ed25519Priv)

	// P-384 PKCS#8 PEM — ECDSA but wrong curve.
	p384Key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("generate P-384 key: %v", err)
	}
	p384PEM := marshalPKCS8PEM(t, p384Key)

	tests := []struct {
		name    string
		pem     []byte
		wantErr bool
	}{
		{"valid P-256 PKCS#8 PEM", p256PKCS8PEM(t), false},
		{"not PEM", []byte("this is not a PEM block at all"), true},
		{"empty", []byte{}, true},
		{"Ed25519 PKCS#8 PEM", ed25519PEM, true},
		{"P-384 PKCS#8 PEM", p384PEM, true},
		{"truncated PEM header", []byte("-----BEGIN PRIVATE KEY-----\nnot-base64"), true},
		{"garbage inside PEM block", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("garbage-der")}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key, err := ParseECDSAP256PrivateKeyPEM(tt.pem)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (key=%v)", key)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if key == nil {
				t.Fatal("expected non-nil key")
			}
			if key.Curve != elliptic.P256() {
				t.Errorf("curve = %s, want P-256", key.Curve.Params().Name)
			}
		})
	}
}

// TestLoadECDSAP256PrivateKey covers the file-path loader: a valid P-256 PKCS#8 PEM
// file loads, a missing path errors, and a well-formed wrong-type key file (Ed25519)
// is rejected — mirroring the coverage the replaced Ed25519 loader had.
func TestLoadECDSAP256PrivateKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	validPath := filepath.Join(dir, "valid.pem")
	if err := os.WriteFile(validPath, p256PKCS8PEM(t), 0o600); err != nil {
		t.Fatal(err)
	}

	_, ed25519Priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	wrongTypePath := filepath.Join(dir, "ed25519.pem")
	if err := os.WriteFile(wrongTypePath, marshalPKCS8PEM(t, ed25519Priv), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid P-256 file", validPath, false},
		{"missing file", filepath.Join(dir, "does-not-exist.pem"), true},
		{"Ed25519 wrong-type file", wrongTypePath, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key, err := LoadECDSAP256PrivateKey(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (key=%v)", key)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if key == nil || key.Curve != elliptic.P256() {
				t.Fatalf("expected valid P-256 key, got %v", key)
			}
		})
	}
}
