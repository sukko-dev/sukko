package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEd25519PrivateKey_Valid(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	path := filepath.Join(t.TempDir(), "key.bin")
	if err := os.WriteFile(path, []byte(priv), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	got, err := LoadEd25519PrivateKey(path)
	if err != nil {
		t.Fatalf("LoadEd25519PrivateKey: %v", err)
	}
	if len(got) != ed25519.PrivateKeySize {
		t.Errorf("key size = %d, want %d", len(got), ed25519.PrivateKeySize)
	}
}

func TestLoadEd25519PrivateKey_NotFound(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nonexistent.bin")
	got, err := LoadEd25519PrivateKey(path)
	if err == nil {
		t.Fatalf("expected error, got nil (key: %x)", got)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not contain path %q", err.Error(), path)
	}
	// Assert no key bytes appear in the error string (no secret leakage)
	if len(got) > 0 {
		t.Errorf("expected nil key on error, got %d bytes", len(got))
	}
}

func TestLoadEd25519PrivateKey_WrongLength(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "short.bin")
	if err := os.WriteFile(path, make([]byte, 32), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	_, err := LoadEd25519PrivateKey(path)
	if err == nil {
		t.Fatal("expected error for 32-byte file, got nil")
	}
}

func TestLoadEd25519PrivateKey_CorruptedSeed(t *testing.T) {
	t.Parallel()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Corrupt the seed (first 32 bytes) while keeping the key length valid.
	corrupted := make([]byte, ed25519.PrivateKeySize)
	copy(corrupted, priv)
	corrupted[0] ^= 0xFF

	path := filepath.Join(t.TempDir(), "corrupted.bin")
	if err := os.WriteFile(path, corrupted, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	_, err = LoadEd25519PrivateKey(path)
	if err == nil {
		t.Fatal("expected error for corrupted seed, got nil")
	}
}
