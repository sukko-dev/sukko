//go:build sukko_realkey

package license

import (
	"errors"
	"testing"
)

// TestRealKeyCoProvenanceNegative pins a fixture signed by a key NOT in the
// embedded set fails embedded-key verification — the reproducible offline negative that
// demonstrates the co-provenance guard rather than merely asserting it. Mints a fixture
// with a fresh throwaway key (never an embedded one) and asserts rejection.
func TestRealKeyCoProvenanceNegative(t *testing.T) {
	t.Parallel() // reads only the immutable embedded bundle; mints a throwaway key locally; no shared mutable state
	// Derive the embedded set from the embedded bytes (not the clobber-prone global).
	embedded, err := parsePublicKeyBundle(embeddedPublicKeyBytes)
	if err != nil {
		t.Fatalf("parse embedded bundle: %v", err)
	}
	wrongPriv, _ := GenerateTestKeyPair()
	claims := Claims{Edition: Enterprise, Org: "GOLDEN — not a real customer", Exp: 1000000000, Iat: 1}
	key := SignTestLicense(claims, wrongPriv)
	if _, verr := parseAndVerify(key, embedded); !errors.Is(verr, ErrLicenseInvalidSignature) {
		t.Fatalf("wrong-key fixture: got %v, want ErrLicenseInvalidSignature", verr)
	}
}
