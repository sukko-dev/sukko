package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// ParseECDSAP256PrivateKeyPEM decodes a PEM-wrapped PKCS#8 ECDSA private key,
// asserts the curve is P-256, and performs a sign-then-verify round trip to
// confirm the key is internally consistent.
// Exported so runner/license_keys.go can use it on byte-slice keys (no file read needed).
// Ed25519 and other-curve keys are rejected — only P-256 is accepted.
func ParseECDSAP256PrivateKeyPEM(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found (expected PKCS#8 ECDSA P-256 private key)")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#8 private key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected ECDSA private key, got %T", parsed)
	}
	if key.Curve != elliptic.P256() {
		return nil, fmt.Errorf("expected P-256 curve, got %s", key.Curve.Params().Name)
	}
	if err := validateECDSAP256Key(key); err != nil {
		return nil, err
	}
	return key, nil
}

// LoadECDSAP256PrivateKey reads a PEM-wrapped PKCS#8 ECDSA P-256 private key
// from path and validates it via ParseECDSAP256PrivateKeyPEM (curve assertion
// plus a sign-then-verify round trip).
func LoadECDSAP256PrivateKey(path string) (*ecdsa.PrivateKey, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is from trusted config (env var), validated at startup
	if err != nil {
		return nil, fmt.Errorf("key file %q: %w", path, err)
	}
	key, err := ParseECDSAP256PrivateKeyPEM(b)
	if err != nil {
		return nil, fmt.Errorf("key file %q: %w", path, err)
	}
	return key, nil
}

// validateECDSAP256Key performs a sign-then-verify round trip to confirm the
// key's private scalar and public point are internally consistent.
func validateECDSAP256Key(key *ecdsa.PrivateKey) error {
	digest := sha256.Sum256([]byte("sukko-ecdsa-p256-consistency-check"))
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		return fmt.Errorf("sign failed (key likely inconsistent): %w", err)
	}
	if !ecdsa.VerifyASN1(&key.PublicKey, digest[:], sig) {
		return errors.New("ECDSA P-256 consistency check failed (private scalar and public point do not match)")
	}
	return nil
}
