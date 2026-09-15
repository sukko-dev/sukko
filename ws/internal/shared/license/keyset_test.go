package license

import (
	"crypto/ecdsa"
	"encoding/pem"
	"errors"
	"testing"
	"time"
)

// manifestFor builds a valid fingerprint manifest for the given keys (test helper).
func manifestFor(t *testing.T, keys ...*ecdsa.PublicKey) []manifestEntry {
	t.Helper()
	m := make([]manifestEntry, len(keys))
	for i, k := range keys {
		fp, err := publicKeyFingerprint(k)
		if err != nil {
			t.Fatal(err)
		}
		m[i] = manifestEntry{fingerprint: fp, keyVersion: "local"}
	}
	return m
}

// TestParseAndVerify_AnyOfKeySet pins the dual-key any-of behavior: a license
// verifies if its signature matches ANY key in the set; a key in neither, an empty set,
// and the retire transition ({B} rejecting an A-signed license) all fail.
func TestParseAndVerify_AnyOfKeySet(t *testing.T) {
	t.Parallel()

	privA, pubA := GenerateTestKeyPair()
	privB, pubB := GenerateTestKeyPair()
	privC, _ := GenerateTestKeyPair()
	claims := Claims{Edition: Pro, Org: "Set", Exp: time.Now().Add(24 * time.Hour).Unix(), Iat: 1}
	keyA := SignTestLicense(claims, privA)
	keyB := SignTestLicense(claims, privB)
	keyC := SignTestLicense(claims, privC)

	both := []*ecdsa.PublicKey{pubA, pubB}
	tests := []struct {
		name    string
		key     string
		set     []*ecdsa.PublicKey
		wantErr error // nil = valid
	}{
		{"A-signed vs {A,B}", keyA, both, nil},
		{"B-signed vs {A,B}", keyB, both, nil},
		{"C-signed vs {A,B} (neither)", keyC, both, ErrLicenseInvalidSignature},
		{"retire: A-signed vs {B}", keyA, []*ecdsa.PublicKey{pubB}, ErrLicenseInvalidSignature},
		{"steady-state: A-signed vs {A}", keyA, []*ecdsa.PublicKey{pubA}, nil},
		{"empty set never verifies", keyA, nil, ErrLicenseInvalidSignature},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseAndVerify(tt.key, tt.set)
			if tt.wantErr == nil && err != nil {
				t.Fatalf("got %v, want valid", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateEmbeddedKeySet pins the init-time set invariants: 1 ≤ |set| ≤ 2,
// exact ordered key↔manifest bijection, no duplicates — including the fail-fast negatives
// (empty, oversize, duplicate, count/order mismatch, orphan fingerprint).
func TestValidateEmbeddedKeySet(t *testing.T) {
	t.Parallel()

	_, pubA := GenerateTestKeyPair()
	_, pubB := GenerateTestKeyPair()
	_, pubC := GenerateTestKeyPair()
	fpA, _ := publicKeyFingerprint(pubA)
	fpB, _ := publicKeyFingerprint(pubB)

	tests := []struct {
		name     string
		keys     []*ecdsa.PublicKey
		manifest []manifestEntry
		wantErr  bool
	}{
		{"valid 1-key", []*ecdsa.PublicKey{pubA}, []manifestEntry{{fpA, "placeholder"}}, false},
		{"valid 2-key", []*ecdsa.PublicKey{pubA, pubB}, []manifestEntry{{fpA, "v1"}, {fpB, "v2"}}, false},
		{"empty set", nil, nil, true},
		{"oversize 3-key", []*ecdsa.PublicKey{pubA, pubB, pubC}, manifestFor(t, pubA, pubB, pubC), true},
		{"duplicate key", []*ecdsa.PublicKey{pubA, pubA}, []manifestEntry{{fpA, "v1"}, {fpA, "v2"}}, true},
		{"count mismatch (extra key)", []*ecdsa.PublicKey{pubA, pubB}, []manifestEntry{{fpA, "v1"}}, true},
		{"count mismatch (extra manifest)", []*ecdsa.PublicKey{pubA}, []manifestEntry{{fpA, "v1"}, {fpB, "v2"}}, true},
		{"order mismatch", []*ecdsa.PublicKey{pubA, pubB}, []manifestEntry{{fpB, "v1"}, {fpA, "v2"}}, true},
		{"orphan fingerprint", []*ecdsa.PublicKey{pubA}, []manifestEntry{{fpB, "v1"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateEmbeddedKeySet(tt.keys, tt.manifest)
			if tt.wantErr && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

// TestParsePublicKeyBundle covers the multi-block PEM bundle parser.
func TestParsePublicKeyBundle(t *testing.T) {
	t.Parallel()

	_, pubA := GenerateTestKeyPair()
	_, pubB := GenerateTestKeyPair()
	pemA := mustPKIXPEM(t, pubA)
	pemB := mustPKIXPEM(t, pubB)
	two := append(append([]byte{}, pemA...), pemB...)

	t.Run("1-block", func(t *testing.T) {
		t.Parallel()
		keys, err := parsePublicKeyBundle(pemA)
		if err != nil || len(keys) != 1 {
			t.Fatalf("got %d keys, err %v", len(keys), err)
		}
	})
	t.Run("2-block", func(t *testing.T) {
		t.Parallel()
		keys, err := parsePublicKeyBundle(two)
		if err != nil || len(keys) != 2 {
			t.Fatalf("got %d keys, err %v", len(keys), err)
		}
	})
	t.Run("2-block + trailing non-PEM", func(t *testing.T) {
		t.Parallel()
		keys, err := parsePublicKeyBundle(append(append([]byte{}, two...), []byte("trailing junk\n")...))
		if err != nil || len(keys) != 2 {
			t.Fatalf("got %d keys, err %v", len(keys), err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		if _, err := parsePublicKeyBundle(nil); err == nil {
			t.Fatal("want error for empty bundle")
		}
	})
	t.Run("wrong block type", func(t *testing.T) {
		t.Parallel()
		wrong := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{0x01, 0x02}})
		if _, err := parsePublicKeyBundle(wrong); err == nil {
			t.Fatal("want error for non-PUBLIC-KEY block")
		}
	})
}

// TestParseFingerprintManifest covers the manifest line-format parser.
func TestParseFingerprintManifest(t *testing.T) {
	t.Parallel()

	_, pubA := GenerateTestKeyPair()
	_, pubB := GenerateTestKeyPair()
	fpA, _ := publicKeyFingerprint(pubA)
	fpB, _ := publicKeyFingerprint(pubB)

	tests := []struct {
		name    string
		in      string
		wantN   int
		wantErr bool
	}{
		{"one entry", fpA + " placeholder", 1, false},
		{"two entries", fpA + " v1\n" + fpB + " v2", 2, false},
		{"blank lines skipped", "\n" + fpA + " local\n\n", 1, false},
		{"empty manifest", "", 0, true},
		{"one field", fpA, 0, true},
		{"three fields", fpA + " v1 extra", 0, true},
		{"short fingerprint", "abc local", 0, true},
		{"uppercase fingerprint", "0C4171EE7ED131EADCBD1A872DDEDBC5B7DC0FAD7A01B179063B34BC610F054A local", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseFingerprintManifest([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %d entries", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantN {
				t.Fatalf("got %d entries, want %d", len(got), tt.wantN)
			}
		})
	}
}
