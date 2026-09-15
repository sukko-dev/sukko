package auth

import (
	"crypto/x509"
	"encoding/pem"
	"regexp"
	"testing"
)

func TestGenerateKeypair(t *testing.T) {
	t.Parallel()

	kp, err := GenerateKeypair("a1b2c3d4")
	if err != nil {
		t.Fatalf("GenerateKeypair: %v", err)
	}

	if kp.PrivateKey == nil {
		t.Fatal("PrivateKey is nil")
	}
	if kp.PublicPEM == "" {
		t.Fatal("PublicPEM is empty")
	}
	if kp.KeyID != "tester-a1b2c3d4" {
		t.Errorf("KeyID = %q, want %q", kp.KeyID, "tester-a1b2c3d4")
	}
}

func TestGenerateKeypair_PEMValid(t *testing.T) {
	t.Parallel()

	kp, err := GenerateKeypair("abcd1234")
	if err != nil {
		t.Fatalf("GenerateKeypair: %v", err)
	}

	block, _ := pem.Decode([]byte(kp.PublicPEM))
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatalf("PEM decode: got block=%v, want non-nil PUBLIC KEY block", block)
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("ParsePKIXPublicKey: %v", err)
	}
	if pub == nil {
		t.Fatal("parsed public key is nil")
	}
}

func TestGenerateKeypair_KeyIDPattern(t *testing.T) {
	t.Parallel()

	pattern := regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)

	tests := []string{"a1b2c3d4", "deadbeef", "00112233"}
	for _, testID := range tests {
		kp, err := GenerateKeypair(testID)
		if err != nil {
			t.Fatalf("GenerateKeypair(%q): %v", testID, err)
		}
		if !pattern.MatchString(kp.KeyID) {
			t.Errorf("KeyID %q does not match tenant key pattern", kp.KeyID)
		}
	}
}

func TestGenerateKeypair_UniqueKeys(t *testing.T) {
	t.Parallel()

	kp1, err := GenerateKeypair("test0001")
	if err != nil {
		t.Fatalf("GenerateKeypair 1: %v", err)
	}
	kp2, err := GenerateKeypair("test0002")
	if err != nil {
		t.Fatalf("GenerateKeypair 2: %v", err)
	}

	if kp1.PublicPEM == kp2.PublicPEM {
		t.Error("two keypairs produced identical public keys")
	}
	if kp1.KeyID == kp2.KeyID {
		t.Error("two keypairs produced identical key IDs")
	}
}
