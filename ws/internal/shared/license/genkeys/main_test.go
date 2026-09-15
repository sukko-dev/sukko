package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRun pins that genkeys emits a P-256 keypair — public key as PKIX PEM,
// private key as PKCS#8 PEM — to sukko.dev.pub/sukko.dev.key, and NEVER writes sukko.pub
// (which would clobber the committed embedded key during an e2e run).
func TestRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pubPath, privPath, fpPath, err := run(dir)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := filepath.Base(pubPath); got != "sukko.dev.pub" {
		t.Errorf("public key path = %s, want sukko.dev.pub", got)
	}
	if got := filepath.Base(privPath); got != "sukko.dev.key" {
		t.Errorf("private key path = %s, want sukko.dev.key", got)
	}
	if got := filepath.Base(fpPath); got != "sukko.dev.fingerprints" {
		t.Errorf("manifest path = %s, want sukko.dev.fingerprints", got)
	}

	// It must NOT have written the committed embedded key or manifest paths.
	for _, committed := range []string{"sukko.pub", "sukko.fingerprints"} {
		if _, err := os.Stat(filepath.Join(dir, committed)); !os.IsNotExist(err) {
			t.Fatalf("genkeys wrote %s (must never touch committed embedded files); stat err=%v", committed, err)
		}
	}

	// Public key: PKIX PEM, ECDSA P-256.
	pub := decodePEM(t, pubPath, "PUBLIC KEY")
	parsedPub, err := x509.ParsePKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	ecPub, ok := parsedPub.(*ecdsa.PublicKey)
	if !ok || ecPub.Curve != elliptic.P256() {
		t.Fatalf("public key is not ECDSA P-256: %T", parsedPub)
	}

	// Manifest: exactly one line "<fp> local" where <fp> is the SPKI-DER SHA-256 of the pub key.
	manifestRaw, err := os.ReadFile(fpPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(manifestRaw)))
	if len(fields) != 2 || fields[1] != "local" {
		t.Fatalf("manifest = %q, want '<fp> local'", string(manifestRaw))
	}
	sum := sha256.Sum256(pub)
	if want := hex.EncodeToString(sum[:]); fields[0] != want {
		t.Fatalf("manifest fingerprint = %s, want %s (SPKI-DER SHA-256 of dev pub key)", fields[0], want)
	}

	// Private key: PKCS#8 PEM, ECDSA P-256.
	priv := decodePEM(t, privPath, "PRIVATE KEY")
	parsedPriv, err := x509.ParsePKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	ecPriv, ok := parsedPriv.(*ecdsa.PrivateKey)
	if !ok || ecPriv.Curve != elliptic.P256() {
		t.Fatalf("private key is not ECDSA P-256: %T", parsedPriv)
	}
}

func decodePEM(t *testing.T, path, wantType string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatalf("%s: no PEM block", path)
	}
	if block.Type != wantType {
		t.Fatalf("%s: PEM type = %q, want %q", path, block.Type, wantType)
	}
	return block.Bytes
}
