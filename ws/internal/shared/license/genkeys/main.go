// Command genkeys generates an ECDSA P-256 key pair for the Sukko license DEV/E2E
// signer. It writes the public key to keys/sukko.dev.pub (PKIX PEM) and the private
// key to keys/sukko.dev.key (PKCS#8 PEM) — the same encodings the License Service's
// local signer and golden vectors use (Constitution XI / XVIII).
//
// It deliberately does NOT write keys/sukko.pub: that path holds the committed
// production/placeholder key the validator embeds by default, and an e2e run invokes
// genkeys before building from source — writing there would clobber the committed key
// . The e2e build embeds keys/sukko.dev.pub via the `sukko_e2e` build tag.
// Production signs via a non-exportable GCP Cloud KMS key whose public key replaces
// the committed keys/sukko.pub through the DK-1 cross-repo PR, never via genkeys.
//
// This is a standalone CLI tool, not a production service. It uses fmt for output
// instead of zerolog (no service context).
//
// Usage: go run ./internal/shared/license/genkeys
//
//nolint:forbidigo // standalone CLI tool — fmt output is intentional, not a logging violation
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// keysDir is the license package's key directory, relative to the ws/ module root
// (the directory `go run ./internal/shared/license/genkeys` is invoked from).
const keysDir = "internal/shared/license/keys"

func main() {
	pubPath, privPath, fpPath, err := run(keysDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "genkeys: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Public key:   %s (ECDSA P-256, PKIX PEM)\n", pubPath)
	fmt.Printf("Private key:  %s (ECDSA P-256, PKCS#8 PEM)\n", privPath)
	fmt.Printf("Fingerprints: %s (<fp> %s)\n", fpPath, license.SentinelLocalVersion)
	fmt.Println("IMPORTANT: The dev keys (sukko.dev.*) are gitignored. Keep the private key safe.")
}

// run generates a P-256 keypair and writes the dev public/private keys and the matching
// fingerprint manifest (sukko.dev.fingerprints, embedded by the sukko_e2e build) into dir,
// returning their paths. Split out from main so it is unit-testable (never writes sukko.pub
// or the committed manifest).
func run(dir string) (pubPath, privPath, fpPath string, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", fmt.Errorf("generate key pair: %w", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal public key: %w", err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal private key: %w", err)
	}

	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})
	// Fingerprint manifest matching sukko.dev.pub (lowercase SHA-256 hex over SPKI DER),
	// with the "local" sentinel version — the form embed_e2e.go embeds and license.init
	// validates as a 1-key set.
	sum := sha256.Sum256(pubDER)
	manifest := hex.EncodeToString(sum[:]) + " " + license.SentinelLocalVersion + "\n"

	pubPath = filepath.Join(dir, "sukko.dev.pub")
	privPath = filepath.Join(dir, "sukko.dev.key")
	fpPath = filepath.Join(dir, "sukko.dev.fingerprints")

	if err := os.WriteFile(pubPath, pubPEM, 0o600); err != nil {
		return "", "", "", fmt.Errorf("write public key: %w", err)
	}
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		return "", "", "", fmt.Errorf("write private key: %w", err)
	}
	if err := os.WriteFile(fpPath, []byte(manifest), 0o600); err != nil {
		return "", "", "", fmt.Errorf("write fingerprint manifest: %w", err)
	}
	return pubPath, privPath, fpPath, nil
}
