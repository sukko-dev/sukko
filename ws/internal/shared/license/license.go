package license

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const (
	// rawSigLen is the length of a raw r||s ECDSA P-256 signature: two 32-byte
	// big-endian scalars. The License Service's signer emits DER and converts to
	// this fixed-width form; the validator splits it back into (r, s).
	rawSigLen = 64

	// p256ScalarLen is the byte width of a single P-256 scalar (r or s) — the split
	// point of a raw r||s signature.
	p256ScalarLen = rawSigLen / 2

	// pemTypePublicKey is the PEM block type for a PKIX (SubjectPublicKeyInfo)
	// public key — the form genkeys emits and the validator embeds.
	pemTypePublicKey = "PUBLIC KEY"

	// maxEmbeddedKeys bounds the embedded key set: exactly one key in steady state,
	// two during a rotation overlap (dual-key any-of model). An
	// embedded set larger than this is a build error caught at init.
	maxEmbeddedKeys = 2

	// SentinelPlaceholderVersion is the manifest key-version token for the committed
	// placeholder key (§I named-constant, data-model §2). The release gate refuses to
	// publish a build whose embedded manifest still contains it.
	SentinelPlaceholderVersion = "placeholder"
	// SentinelLocalVersion is the manifest key-version token for the dev/e2e key, keeping
	// licenses.signing_key_version total across the KMS and local signers.
	SentinelLocalVersion = "local"
)

// embeddedPublicKeyBytes (declared in embed_default.go / embed_e2e.go, build-tag
// selected) is the PKIX-PEM public-key BUNDLE compiled into the binary: one "PUBLIC
// KEY" block in steady state, two during a rotation overlap. embeddedFingerprintManifest
// (same build-tag selection) is the matching "<fingerprint> <key-version>" manifest.
// Both are owned by the cross-repo delivery PR; genkeys writes the dev
// variants (keys/sukko.dev.pub + keys/sukko.dev.fingerprints) for the sukko_e2e build.

// publicKeys is the ECDSA P-256 embedded key SET used for license verification:
// a license is valid if its signature matches ANY key in the set (dual-key any-of).
// Normally one key; two during a rotation overlap. Initialized and
// invariant-validated at package init. Tests override via SetPublicKeysForTesting()
// (or the single-key SetPublicKeyForTesting() convenience wrapper).
var publicKeys []*ecdsa.PublicKey

func init() {
	keys, err := parsePublicKeyBundle(embeddedPublicKeyBytes)
	if err != nil {
		panic("license: parse embedded public key bundle: " + err.Error())
	}
	manifest, err := parseFingerprintManifest(embeddedFingerprintManifest)
	if err != nil {
		panic("license: parse embedded fingerprint manifest: " + err.Error())
	}
	if err := validateEmbeddedKeySet(keys, manifest); err != nil {
		// A malformed, oversize, duplicate, or non-bijective embedded key set is a
		// build/deploy error — fail fast at load, never a green boot that then
		// mis-validates every license at runtime (§I/§II fail-fast).
		panic("license: invalid embedded key set: " + err.Error())
	}
	publicKeys = keys
}

// parsePKIXECDSAP256 decodes one PKIX (SubjectPublicKeyInfo) DER into an ECDSA P-256
// public key, with distinct wrapped errors per failure mode so a bad key fails clearly.
func parsePKIXECDSAP256(der []byte) (*ecdsa.PublicKey, error) {
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX public key: %w", err)
	}
	pub, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is %T, want *ecdsa.PublicKey", parsed)
	}
	if pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("key curve is %s, want P-256", pub.Curve.Params().Name)
	}
	return pub, nil
}

// parsePublicKeyBundle decodes a multi-block PKIX PEM bundle (one or two "PUBLIC KEY"
// blocks) into an ordered slice of ECDSA P-256 public keys. Each block's error is
// wrapped with its index. At least one block is required.
func parsePublicKeyBundle(pemBytes []byte) ([]*ecdsa.PublicKey, error) {
	var keys []*ecdsa.PublicKey
	rest := pemBytes
	for i := 0; ; i++ {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != pemTypePublicKey {
			return nil, fmt.Errorf("block %d: PEM type is %q, want %q", i, block.Type, pemTypePublicKey)
		}
		pub, err := parsePKIXECDSAP256(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", i, err)
		}
		keys = append(keys, pub)
	}
	if len(keys) == 0 {
		return nil, errors.New("no PEM public-key blocks found")
	}
	return keys, nil
}

// manifestEntry is one "<fingerprint> <key-version>" line of the embedded fingerprint
// manifest: the lowercase-hex SPKI-DER SHA-256 and the signing key's provenance
// (a KMS CryptoKeyVersion resource name, or a placeholder/local sentinel).
type manifestEntry struct {
	fingerprint string
	keyVersion  string
}

// parseFingerprintManifest parses the embedded manifest. Each non-empty line is
// "<fingerprint> <key-version>" (exactly two whitespace-separated fields); the
// fingerprint MUST be 64 lowercase hex chars. At least one entry is required.
func parseFingerprintManifest(b []byte) ([]manifestEntry, error) {
	var entries []manifestEntry
	sc := bufio.NewScanner(bytes.NewReader(b))
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("line %d: want '<fingerprint> <key-version>', got %d fields", line, len(fields))
		}
		fp := fields[0]
		if len(fp) != sha256.Size*2 || !isLowerHex(fp) {
			return nil, fmt.Errorf("line %d: fingerprint %q is not 64 lowercase hex chars", line, fp)
		}
		entries = append(entries, manifestEntry{fingerprint: fp, keyVersion: fields[1]})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("manifest has no entries")
	}
	return entries, nil
}

// isLowerHex reports whether s is entirely lowercase hexadecimal digits.
func isLowerHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// validateEmbeddedKeySet enforces the dual-key set invariants: 1 ≤ |set| ≤
// maxEmbeddedKeys, an exact ordered key↔manifest bijection (keys[i]'s SPKI fingerprint
// equals manifest[i].fingerprint), and no duplicate keys/fingerprints. It is the pure,
// unit-testable core of the init-time fail-fast check and the tripwire.
func validateEmbeddedKeySet(keys []*ecdsa.PublicKey, manifest []manifestEntry) error {
	if len(keys) == 0 {
		return errors.New("empty key set")
	}
	if len(keys) > maxEmbeddedKeys {
		return fmt.Errorf("%d keys, want at most %d", len(keys), maxEmbeddedKeys)
	}
	if len(keys) != len(manifest) {
		return fmt.Errorf("%d keys but %d manifest entries", len(keys), len(manifest))
	}
	seen := make(map[string]struct{}, len(keys))
	for i, k := range keys {
		fp, err := publicKeyFingerprint(k)
		if err != nil {
			return fmt.Errorf("key %d: %w", i, err)
		}
		if fp != manifest[i].fingerprint {
			return fmt.Errorf("key %d fingerprint %s != manifest entry %s", i, fp, manifest[i].fingerprint)
		}
		if _, dup := seen[fp]; dup {
			return fmt.Errorf("duplicate key fingerprint %s", fp)
		}
		seen[fp] = struct{}{}
	}
	return nil
}

// publicKeyFingerprint returns the SHA-256 (lowercase hex) of a public key's PKIX
// (SubjectPublicKeyInfo) DER — the identity token, matching the fingerprint
// the License Service's KMS adapter computes.
func publicKeyFingerprint(pub *ecdsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("marshal PKIX public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:]), nil
}

// Claims holds the decoded fields from a license key.
type Claims struct {
	// Edition is the licensed tier (pro, enterprise).
	Edition Edition `json:"edition"`

	// Org is the licensee organization name.
	Org string `json:"org"`

	// Exp is the expiration time as a Unix timestamp (seconds).
	Exp int64 `json:"exp"`

	// Iat is the issued-at time as a Unix timestamp (seconds).
	// Used for replay protection: reject keys with iat <= current key's iat.
	// Omitempty for backward compatibility with pre-existing keys.
	Iat int64 `json:"iat,omitempty"`

	// Nodes is the advisory maximum node count (contractual, not enforced).
	Nodes int `json:"nodes,omitempty"`

	// Limits contains per-customer limit overrides. Non-zero values override
	// the edition's DefaultLimits.
	Limits Limits `json:"limits,omitzero"`
}

// IsExpired returns true if the license has passed its expiration date.
func (c *Claims) IsExpired() bool {
	return time.Now().Unix() > c.Exp
}

// ParseAndVerify verifies and parses a license key using the embedded public key set.
func ParseAndVerify(key string) (*Claims, error) {
	return parseAndVerify(key, publicKeys)
}

// parseAndVerify is the testable inner function that accepts an explicit key SET — the
// per-call injectable seam the golden-vector and set-aware tests use,
// sharing no mutable package state with the embedded-key path. The signature is valid
// if it verifies against ANY key in the set (any-of, dual-key rotation overlap).
func parseAndVerify(key string, pubKeys []*ecdsa.PublicKey) (*Claims, error) {
	// Split into payload.signature (exactly 2 parts)
	parts := strings.SplitN(key, ".", 3)
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: expected 2 parts (payload.signature), got %d", ErrLicenseInvalidFormat, len(parts))
	}

	// Decode payload
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: payload base64 decode: %w", ErrLicenseInvalidFormat, err)
	}

	// Decode signature (64-byte raw r||s)
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: signature base64 decode: %w", ErrLicenseInvalidFormat, err)
	}

	// A signature that is not exactly 64 bytes is a malformed key (format error).
	// §XVIII: the sukko-license mirror (SelfVerify) classifies a wrong-length sig
	// under ErrInvalidSignature; the validator treats length as a format check
	// (spec edge case) — both still reject, only the error class differs.
	if len(sig) != rawSigLen {
		return nil, fmt.Errorf("%w: signature must be %d bytes, got %d", ErrLicenseInvalidFormat, rawSigLen, len(sig))
	}

	// Verify ECDSA P-256 signature over SHA-256 of the raw payload bytes against ANY
	// embedded key (dual-key any-of).
	if !verifyAnyKey(pubKeys, payload, sig) {
		return nil, ErrLicenseInvalidSignature
	}

	// Unmarshal claims
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("%w: claims JSON: %w", ErrLicenseInvalidFormat, err)
	}

	// Check expiry
	if claims.IsExpired() {
		return &claims, ErrLicenseExpired
	}

	return &claims, nil
}

// verifyAnyKey reports whether the 64-byte raw r||s signature verifies against ANY
// key in the set (dual-key any-of). An empty set never verifies.
func verifyAnyKey(pubKeys []*ecdsa.PublicKey, payload, sig []byte) bool {
	for _, k := range pubKeys {
		if verifyRawSignature(k, payload, sig) {
			return true
		}
	}
	return false
}

// verifyRawSignature checks a 64-byte raw r||s ECDSA P-256 signature against the
// SHA-256 digest of the payload. r and s are the two 32-byte big-endian halves;
// short (leading-zero) halves verify correctly because SetBytes is width-agnostic.
func verifyRawSignature(pubKey *ecdsa.PublicKey, payload, sig []byte) bool {
	if pubKey == nil || len(sig) != rawSigLen {
		return false
	}
	digest := sha256.Sum256(payload)
	r := new(big.Int).SetBytes(sig[:p256ScalarLen])
	s := new(big.Int).SetBytes(sig[p256ScalarLen:])
	return ecdsa.Verify(pubKey, digest[:], r, s)
}
