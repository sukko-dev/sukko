package license

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The golden artifact under testdata/golden/ is the byte-compatibility
// contract, generated and owned by the sukko-license repo and copied here BYTE-IDENTICAL
// (verified by checksums.sha256). This test verifies every committed vector through the
// REAL validator's per-call seam (parseAndVerify) using the golden public key —
// no reimplementation, no package-global mutation — proving Sukko and the license service
// agree on the frozen byte format. If these files drift from sukko-license's, the two
// repos' checksums.sha256 diverge and this test (and sukko-license's) fail.

const goldenDir = "testdata/golden"

// goldenChecksumFiles are pinned by checksums.sha256 (must match sukko-license).
var goldenChecksumFiles = []string{"keypair.pem", "pub.pem", "vectors.json"}

type goldenVector struct {
	Name       string `json:"name"`
	PayloadB64 string `json:"payload_b64"`
	SigB64     string `json:"sig_b64"`
	Key        string `json:"key"`
	Expect     string `json:"expect"` // "valid" | "invalid_signature" | "expired"
}

// TestGoldenVectors verifies the committed golden artifact against the real validator
// : the checksum manifest matches on disk, and every vector's key
// assembles and verifies exactly as recorded — including the short-r/short-s pad cases,
// the wrong-key and tampered negatives, and the expired negative.
func TestGoldenVectors(t *testing.T) {
	t.Parallel()

	assertGoldenChecksums(t)
	pub := loadGoldenPub(t)

	raw, err := os.ReadFile(filepath.Join(goldenDir, "vectors.json"))
	if err != nil {
		t.Fatalf("read golden vectors: %v", err)
	}
	var gf struct {
		Vectors []goldenVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &gf); err != nil {
		t.Fatalf("decode golden vectors: %v", err)
	}
	if len(gf.Vectors) == 0 {
		t.Fatal("golden artifact has no vectors")
	}

	for _, v := range gf.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			t.Parallel()
			payload, err := base64.RawURLEncoding.DecodeString(v.PayloadB64)
			if err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			sig, err := base64.RawURLEncoding.DecodeString(v.SigB64)
			if err != nil {
				t.Fatalf("decode sig: %v", err)
			}

			// The recorded key must equal the canonical two-part assembly.
			assembled := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
			if assembled != v.Key {
				t.Fatalf("key mismatch:\n got %q\nwant %q", assembled, v.Key)
			}

			// Verify through the real validator's per-call seam.
			_, verr := parseAndVerify(v.Key, []*ecdsa.PublicKey{pub})
			switch v.Expect {
			case "valid":
				if verr != nil {
					t.Fatalf("expected valid, got %v", verr)
				}
			case "invalid_signature":
				if !errors.Is(verr, ErrLicenseInvalidSignature) {
					t.Fatalf("expected ErrLicenseInvalidSignature, got %v", verr)
				}
			case "expired":
				if !errors.Is(verr, ErrLicenseExpired) {
					t.Fatalf("expected ErrLicenseExpired, got %v", verr)
				}
			default:
				t.Fatalf("unknown expect %q", v.Expect)
			}
		})
	}
}

// assertGoldenChecksums verifies each artifact file matches the committed manifest —
// the intra-repo drift tripwire; cross-repo equality reduces to comparing this manifest
// with sukko-license's.
func assertGoldenChecksums(t *testing.T) {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(goldenDir, "checksums.sha256"))
	if err != nil {
		t.Fatalf("read checksum manifest: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(manifest)), "\n")
	if len(lines) != len(goldenChecksumFiles) {
		t.Fatalf("manifest has %d entries, want %d", len(lines), len(goldenChecksumFiles))
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("bad manifest line %q", line)
		}
		wantSum, name := fields[0], fields[1]
		data, err := os.ReadFile(filepath.Join(goldenDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != wantSum {
			t.Fatalf("%s checksum mismatch:\n got %s\nwant %s", name, got, wantSum)
		}
	}
}

func loadGoldenPub(t *testing.T) *ecdsa.PublicKey {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, "pub.pem"))
	if err != nil {
		t.Fatalf("read golden pub key: %v", err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatal("golden pub.pem: no PEM block")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse golden pub key: %v", err)
	}
	pub, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("golden pub key is %T, want *ecdsa.PublicKey", parsed)
	}
	return pub
}
