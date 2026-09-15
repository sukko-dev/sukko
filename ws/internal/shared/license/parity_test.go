package license

import (
	"os"
	"path/filepath"
	"testing"
)

// goldenPubFingerprint is the pinned SHA-256 (lowercase hex over SPKI DER) of the shared
// golden pub.pem — the cross-repo fingerprint-definition anchor. The SAME
// value is pinned in sukko-license (its parity Go test + the scripts/fingerprint shell
// helper key-sync.yml uses). A drift in either repo's fingerprint definition fails as a
// fast local test here rather than mid-delivery. testdata/golden/pub.pem is byte-identical
// across both repos (feat/license-p256).
const goldenPubFingerprint = "97f55f75297a718faa858e990288a40e565f513981287f58f44fd9032eeef173"

// TestFingerprintParityGolden pins that publicKeyFingerprint over the shared
// golden pub.pem MUST equal goldenPubFingerprint, so the two ends of the KMS delivery
// (this validator's tripwire and the workflow's shell helper) compare like for like.
func TestFingerprintParityGolden(t *testing.T) {
	t.Parallel()

	pemBytes, err := os.ReadFile(filepath.Join("testdata", "golden", "pub.pem"))
	if err != nil {
		t.Fatalf("read golden pub.pem: %v", err)
	}
	keys, err := parsePublicKeyBundle(pemBytes)
	if err != nil {
		t.Fatalf("parse golden pub.pem: %v", err)
	}
	fp, err := publicKeyFingerprint(keys[0])
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if fp != goldenPubFingerprint {
		t.Fatalf("golden fingerprint = %s, want %s (cross-repo parity pin)", fp, goldenPubFingerprint)
	}
}
