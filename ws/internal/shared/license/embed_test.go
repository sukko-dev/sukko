package license

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"
)

// mustPKIXPEM marshals a public key to a PKIX ("PUBLIC KEY") PEM block for test fixtures.
func mustPKIXPEM(t *testing.T, pub any) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal PKIX: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// TestParsePublicKeyBundle_Rejects pins the embedded-key bundle parser must
// reject a malformed PEM, a wrong block type, a non-ECDSA key, and a wrong-curve key —
// each with a clear error — so a bad embedded key fails loudly at load, never a green
// boot that then rejects every license.
func TestParsePublicKeyBundle_Rejects(t *testing.T) {
	t.Parallel()

	// Valid P-256 fixture — must parse.
	p256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// P-384 (wrong curve) fixture.
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// Ed25519 (non-ECDSA) fixture.
	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		pem     []byte
		wantErr bool
	}{
		{"valid P-256", mustPKIXPEM(t, &p256.PublicKey), false},
		{"malformed - not PEM", []byte("this is not a pem block"), true},
		{"empty", nil, true},
		{"wrong block type", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{0x01, 0x02}}), true},
		{"non-ECDSA (Ed25519)", mustPKIXPEM(t, edPub), true},
		{"wrong curve (P-384)", mustPKIXPEM(t, &p384.PublicKey), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parsePublicKeyBundle(tt.pem)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected success, got %v", err)
			}
		})
	}
}

// TestEmbeddedKeySetTripwire pins the set-aware tripwire: the committed production/placeholder
// key BUNDLE (keys/sukko.pub) and its fingerprint MANIFEST (keys/sukko.fingerprints) MUST
// form an exact ordered bijection with valid set invariants (1..2 keys, no dups). This is
// the offline drift tripwire — no cloud dependency — and the same test validates the
// KMS-delivered production set once the delivery PR swaps the placeholder. It reads the
// committed files directly (not embeddedPublicKeyBytes), so it pins the committed production
// set regardless of build tag: under -tags sukko_e2e the embedded bytes are the regenerated
// dev key, which intentionally does not match the committed manifest.
func TestEmbeddedKeySetTripwire(t *testing.T) {
	t.Parallel()

	bundle, err := os.ReadFile("keys/sukko.pub")
	if err != nil {
		t.Fatalf("read committed public key bundle: %v", err)
	}
	manifestBytes, err := os.ReadFile("keys/sukko.fingerprints")
	if err != nil {
		t.Fatalf("read committed fingerprint manifest: %v", err)
	}
	keys, err := parsePublicKeyBundle(bundle)
	if err != nil {
		t.Fatalf("parse committed bundle: %v", err)
	}
	manifest, err := parseFingerprintManifest(manifestBytes)
	if err != nil {
		t.Fatalf("parse committed manifest: %v", err)
	}
	if err := validateEmbeddedKeySet(keys, manifest); err != nil {
		t.Fatalf("committed key set fails invariants (bundle/manifest drift): %v\n(if the embedded key set changed intentionally, the delivery PR updates keys/sukko.pub + keys/sukko.fingerprints in lockstep)", err)
	}
}
