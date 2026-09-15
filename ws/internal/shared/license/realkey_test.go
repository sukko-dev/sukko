//go:build sukko_realkey

package license

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// realkeyFixture is the golden-vector JSON schema (reused from feat/license-p256) that the
// delivery PR writes to testdata/realkey/<fingerprint>.json.
type realkeyFixture struct {
	PayloadB64 string `json:"payload_b64"`
	SigB64     string `json:"sig_b64"`
	Key        string `json:"key"`
}

// TestRealKeyFixtures is the anti-circularity proof, armed only under -tags
// sukko_realkey (compiled out of default builds so main stays green in the placeholder
// era; the delivery PR arms CI by writing the fixtures + flipping ws/.ci-test-tags).
//
// For EACH embedded production key: its real-key fixture (testdata/realkey/<fp>.json) MUST
// exist (hard-fail, never skip), its signature MUST verify against that embedded
// key (co-provenance, the anti-circularity proof), and full ParseAndVerify MUST
// return ErrLicenseExpired (the fixture is inert/expired by design — a leaked fixture is
// never a usable credential).
func TestRealKeyFixtures(t *testing.T) {
	t.Parallel() // reads only the immutable embedded bundle + read-only testdata; no shared mutable state
	// Derive the embedded set directly from the embedded bytes rather than the mutable
	// package global publicKeys — other tests' SetPublicKey(s)ForTesting calls clobber
	// that global without restoring it, so reading it here would fingerprint a leftover
	// test key. The embedded bytes are the source of truth (init already validated them).
	embedded, err := parsePublicKeyBundle(embeddedPublicKeyBytes)
	if err != nil {
		t.Fatalf("parse embedded bundle: %v", err)
	}
	for i, pub := range embedded {
		fp, ferr := publicKeyFingerprint(pub)
		if ferr != nil {
			t.Fatalf("embedded key %d fingerprint: %v", i, ferr)
		}
		path := filepath.Join("testdata", "realkey", fp+".json")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("embedded key %d (%s): missing real-key fixture %s: %v (the delivery PR must add it)", i, fp, path, err)
		}
		var fx realkeyFixture
		if err := json.Unmarshal(data, &fx); err != nil {
			t.Fatalf("%s: decode fixture: %v", path, err)
		}
		payload, err := base64.RawURLEncoding.DecodeString(fx.PayloadB64)
		if err != nil {
			t.Fatalf("%s: payload b64: %v", path, err)
		}
		sig, err := base64.RawURLEncoding.DecodeString(fx.SigB64)
		if err != nil {
			t.Fatalf("%s: sig b64: %v", path, err)
		}
		// Anti-circularity proof: the KMS signature verifies against THIS embedded key.
		if !verifyRawSignature(pub, payload, sig) {
			t.Fatalf("%s: signature does not verify against embedded key %s (co-provenance)", path, fp)
		}
		// Inertness: full parse+verify against the embedded set returns ErrLicenseExpired
		// (fixed-past exp — a leaked fixture is never a usable credential).
		if _, verr := parseAndVerify(fx.Key, embedded); !errors.Is(verr, ErrLicenseExpired) {
			t.Fatalf("%s: parseAndVerify = %v, want ErrLicenseExpired (inert fixture)", path, verr)
		}
	}
}
