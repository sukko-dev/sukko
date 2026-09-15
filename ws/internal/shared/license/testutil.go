package license

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
)

// GenerateTestKeyPair creates an ECDSA P-256 key pair for testing.
// Panics on failure — test-only, never used in production.
func GenerateTestKeyPair() (*ecdsa.PrivateKey, *ecdsa.PublicKey) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic("license: generate test key pair: " + err.Error())
	}
	return priv, &priv.PublicKey
}

// SignTestLicense creates a signed license key string for testing.
// Format: base64url(json_claims).base64url(signature), where the signature is
// ECDSA P-256 over SHA-256 of the payload, emitted as 64-byte raw r||s. This
// exercises the identical DER→r||s conversion the License Service's signer uses.
func SignTestLicense(claims Claims, privateKey *ecdsa.PrivateKey) string {
	payload, err := json.Marshal(claims)
	if err != nil {
		panic("license: marshal test claims: " + err.Error())
	}
	digest := sha256.Sum256(payload)
	der, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	if err != nil {
		panic("license: sign test claims: " + err.Error())
	}
	rawSig, err := derToRawSignature(der)
	if err != nil {
		panic("license: convert test signature: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(rawSig)
}

// ecdsaDERSig is the ASN.1 structure ecdsa.SignASN1 and GCP KMS both produce.
type ecdsaDERSig struct {
	R, S *big.Int
}

// derToRawSignature converts a DER-encoded ECDSA signature into the fixed
// 64-byte big-endian r||s form the validator verifies. Short r/s values (a
// leading zero byte, ~1-in-128 of signatures) are left-zero-padded to 32 bytes
// by big.Int.FillBytes. This mirrors the License Service's DERToRawSignature
// byte-for-byte (Constitution XI) so both repos share one conversion.
func derToRawSignature(der []byte) ([]byte, error) {
	var sig ecdsaDERSig
	if _, err := asn1.Unmarshal(der, &sig); err != nil {
		return nil, fmt.Errorf("asn1 unmarshal signature: %w", err)
	}
	raw := make([]byte, rawSigLen)
	sig.R.FillBytes(raw[:p256ScalarLen]) // left-zero-padded to exactly 32 bytes
	sig.S.FillBytes(raw[p256ScalarLen:])
	return raw, nil
}

// SetPublicKeysForTesting replaces the package-level embedded key SET for test
// isolation (dual-key any-of). Must be called before ParseAndVerify.
//
// Tests that call this (or SetPublicKeyForTesting) MUST NOT use t.Parallel() since
// they share the package-level publicKeys variable (Constitution VIII).
func SetPublicKeysForTesting(keys []*ecdsa.PublicKey) {
	publicKeys = keys
}

// SetPublicKeyForTesting is the single-key convenience wrapper over
// SetPublicKeysForTesting — kept with its original name+signature so existing
// callers across packages stay unchanged. Sets the embedded set to exactly {key}.
func SetPublicKeyForTesting(key *ecdsa.PublicKey) {
	publicKeys = []*ecdsa.PublicKey{key}
}
