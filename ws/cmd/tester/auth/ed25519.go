package auth

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
)

// ValidateEd25519Key performs a sign-then-verify round trip to confirm the key's
// seed and public key halves are internally consistent.
// Exported so runner/license_keys.go can use it on byte-slice keys (no file read needed).
func ValidateEd25519Key(key ed25519.PrivateKey) error {
	msg := []byte("sukko-ed25519-consistency-check")
	sig, err := key.Sign(rand.Reader, msg, crypto.Hash(0))
	if err != nil {
		return fmt.Errorf("sign failed (key likely inconsistent): %w", err)
	}
	if !ed25519.Verify(key.Public().(ed25519.PublicKey), msg, sig) { //nolint:errcheck // ed25519.PrivateKey.Public() always returns ed25519.PublicKey
		return errors.New("Ed25519 consistency check failed (seed and public key halves do not match)")
	}
	return nil
}

// LoadEd25519PrivateKey reads a raw 64-byte Ed25519 private key from path,
// validates its length, and performs a sign-then-verify round trip to ensure
// the seed and public key halves are internally consistent.
// ECDSA P-256 keys are implicitly rejected — they are 32 bytes, not 64.
func LoadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is from trusted config (env var), validated at startup
	if err != nil {
		return nil, fmt.Errorf("key file %q: %w", path, err)
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("key file %q: must be %d bytes (Ed25519), got %d", path, ed25519.PrivateKeySize, len(b))
	}
	key := ed25519.PrivateKey(b)
	if err := ValidateEd25519Key(key); err != nil {
		return nil, fmt.Errorf("key file %q: %w", path, err)
	}
	return key, nil
}
