package license

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// MUST NOT use t.Parallel() — tests share package-level publicKey via SetPublicKeyForTesting.

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_Valid(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{
		Edition: Pro,
		Org:     "Acme Corp",
		Exp:     time.Now().Add(24 * time.Hour).Unix(),
		Nodes:   3,
	}
	key := SignTestLicense(claims, priv)

	got, err := ParseAndVerify(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Edition != Pro {
		t.Errorf("Edition = %q, want Pro", got.Edition)
	}
	if got.Org != "Acme Corp" {
		t.Errorf("Org = %q, want %q", got.Org, "Acme Corp")
	}
	if got.Nodes != 3 {
		t.Errorf("Nodes = %d, want 3", got.Nodes)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_Expired(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{
		Edition: Pro,
		Org:     "Expired Corp",
		Exp:     time.Now().Add(-1 * time.Hour).Unix(), // expired 1 hour ago
	}
	key := SignTestLicense(claims, priv)

	got, err := ParseAndVerify(key)
	if !errors.Is(err, ErrLicenseExpired) {
		t.Fatalf("expected ErrLicenseExpired, got: %v", err)
	}
	// Expired claims are still returned for inspection
	if got == nil {
		t.Fatal("expected non-nil claims for expired license")
	}
	if got.Org != "Expired Corp" {
		t.Errorf("Org = %q, want %q", got.Org, "Expired Corp")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_InvalidSignature(t *testing.T) {
	_, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// Sign with a DIFFERENT key
	otherPriv, _ := GenerateTestKeyPair()
	claims := Claims{Edition: Pro, Org: "Tampered", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, otherPriv)

	_, err := ParseAndVerify(key)
	if !errors.Is(err, ErrLicenseInvalidSignature) {
		t.Fatalf("expected ErrLicenseInvalidSignature, got: %v", err)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_InvalidFormat(t *testing.T) {
	_, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	tests := []struct {
		name string
		key  string
	}{
		{"no separator", "nodothere"},
		{"too many parts", "a.b.c"},
		{"empty string", ""},
		{"bad payload base64", "!!!invalid!!!." + base64.RawURLEncoding.EncodeToString([]byte("sig"))},
		{"bad signature base64", base64.RawURLEncoding.EncodeToString([]byte("{}")) + ".!!!invalid!!!"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseAndVerify(tt.key)
			if !errors.Is(err, ErrLicenseInvalidFormat) {
				t.Errorf("expected ErrLicenseInvalidFormat, got: %v", err)
			}
		})
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_InvalidJSON(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// Sign raw bytes that aren't valid JSON — a validly-signed payload that
	// passes signature verification but fails JSON unmarshal.
	payload := []byte("not json at all")
	digest := sha256.Sum256(payload)
	der, err := ecdsa.SignASN1(rand.Reader, priv, digest[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	sig, err := derToRawSignature(der)
	if err != nil {
		t.Fatalf("convert signature: %v", err)
	}
	key := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)

	_, err = ParseAndVerify(key)
	if !errors.Is(err, ErrLicenseInvalidFormat) {
		t.Errorf("expected ErrLicenseInvalidFormat for invalid JSON, got: %v", err)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_WithLimits(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{
		Edition: Pro,
		Org:     "Custom Limits Corp",
		Exp:     time.Now().Add(24 * time.Hour).Unix(),
		Limits: Limits{
			MaxTenants: 100,
		},
	}
	key := SignTestLicense(claims, priv)

	got, err := ParseAndVerify(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Limits.MaxTenants != 100 {
		t.Errorf("Limits.MaxTenants = %d, want 100", got.Limits.MaxTenants)
	}
}

// TestParseAndVerify_StrictRejection pins the rejection paths distinctly:
// a wrong-length signature is a format error, while a wrong-key signature and a
// legacy Ed25519-signed key both fail signature verification (no dual-algorithm
// acceptance). The wrong-key case shares intent with TestParseAndVerify_InvalidSignature
// but is kept here so the three rejection classes read as one contract.
//
//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_StrictRejection(t *testing.T) {
	_, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "Strict", Exp: time.Now().Add(time.Hour).Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	enc := base64.RawURLEncoding

	t.Run("wrong length signature -> format error", func(t *testing.T) {
		// 32-byte signature (not 64) must be rejected as a format error, not reach verify.
		key := enc.EncodeToString(payload) + "." + enc.EncodeToString(make([]byte, 32))
		if _, err := ParseAndVerify(key); !errors.Is(err, ErrLicenseInvalidFormat) {
			t.Fatalf("got %v, want ErrLicenseInvalidFormat", err)
		}
	})

	t.Run("wrong key signature -> signature error", func(t *testing.T) {
		otherPriv, _ := GenerateTestKeyPair()
		key := SignTestLicense(claims, otherPriv)
		if _, err := ParseAndVerify(key); !errors.Is(err, ErrLicenseInvalidSignature) {
			t.Fatalf("got %v, want ErrLicenseInvalidSignature", err)
		}
	})

	t.Run("legacy Ed25519 signed key -> signature error", func(t *testing.T) {
		// An Ed25519 signature is exactly 64 bytes, so it passes the length gate and
		// must be rejected at ECDSA verification (no dual-algorithm acceptance).
		_, edPriv, gerr := ed25519.GenerateKey(rand.Reader)
		if gerr != nil {
			t.Fatalf("ed25519 keygen: %v", gerr)
		}
		edSig := ed25519.Sign(edPriv, payload)
		if len(edSig) != rawSigLen {
			t.Fatalf("ed25519 sig len = %d, want %d (length gate would mask this)", len(edSig), rawSigLen)
		}
		key := enc.EncodeToString(payload) + "." + enc.EncodeToString(edSig)
		if _, err := ParseAndVerify(key); !errors.Is(err, ErrLicenseInvalidSignature) {
			t.Fatalf("got %v, want ErrLicenseInvalidSignature", err)
		}
	})
}

// TestParseAndVerify_FutureIatAccepted pins the validator MUST NOT reject a
// key whose iat is in the future — the license service does not guarantee iat <= now.
//
//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestParseAndVerify_FutureIatAccepted(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{
		Edition: Pro,
		Org:     "Future Iat",
		Exp:     time.Now().Add(24 * time.Hour).Unix(),
		Iat:     time.Now().Add(365 * 24 * time.Hour).Unix(), // one year in the future
	}
	key := SignTestLicense(claims, priv)

	got, err := ParseAndVerify(key)
	if err != nil {
		t.Fatalf("future-iat key must validate, got: %v", err)
	}
	if got.Org != "Future Iat" {
		t.Errorf("Org = %q, want %q", got.Org, "Future Iat")
	}
}
