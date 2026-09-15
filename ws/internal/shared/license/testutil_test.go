package license

import (
	"crypto/ecdsa"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

// TestSignTestLicense_ShortScalarRoundTrip pins the sukko-side signer emits a
// fixed-width 64-byte r||s (each half left-zero-padded), never DER on the wire, and a
// short (leading-zero) r or s still verifies. It mines both short-r and short-s cases
// out of random signing and validates each through the per-call verify seam — the
// imported golden vectors are license-service-minted, so they never exercise the sukko
// signer's own padding path.
func TestSignTestLicense_ShortScalarRoundTrip(t *testing.T) {
	t.Parallel()

	priv, pub := GenerateTestKeyPair()
	claims := Claims{Edition: Pro, Org: "Pad", Exp: time.Now().Add(24 * time.Hour).Unix(), Iat: 1}

	sawShortR, sawShortS := false, false
	for i := range 4000 {
		key := SignTestLicense(claims, priv)

		// Every signature must verify, short scalar or not.
		if _, err := parseAndVerify(key, []*ecdsa.PublicKey{pub}); err != nil {
			t.Fatalf("iter %d: verify failed: %v", i, err)
		}

		sig := decodeSignature(t, key)
		if sig[0] == 0 {
			sawShortR = true
		}
		if sig[32] == 0 {
			sawShortS = true
		}
		if sawShortR && sawShortS {
			break
		}
	}
	if !sawShortR || !sawShortS {
		t.Fatalf("did not observe short-r (%v) and short-s (%v) in 4000 signatures", sawShortR, sawShortS)
	}
}

// decodeSignature extracts and base64url-decodes the signature segment of a key,
// asserting it is exactly rawSigLen bytes.
func decodeSignature(t *testing.T, key string) []byte {
	t.Helper()
	_, sigB64, ok := strings.Cut(key, ".")
	if !ok {
		t.Fatal("key has no '.' separator")
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if len(sig) != rawSigLen {
		t.Fatalf("signature length = %d, want %d", len(sig), rawSigLen)
	}
	return sig
}
