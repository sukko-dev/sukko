package runner

import (
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// licenseKeyGenerator manages monotonic iat and signs test license keys.
// The private key is read from a file path at runtime (never embedded).
type licenseKeyGenerator struct {
	privateKey *ecdsa.PrivateKey
	nextIat    int64
}

// newLicenseKeyGenerator reads the ECDSA P-256 private key from the given file path
// and returns a generator with monotonic iat starting from now.
func newLicenseKeyGenerator(keyFilePath string) (*licenseKeyGenerator, error) {
	key, err := auth.LoadECDSAP256PrivateKey(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("license signing key: %w", err)
	}
	return &licenseKeyGenerator{privateKey: key, nextIat: time.Now().Unix()}, nil
}

// newLicenseKeyGeneratorFromBytes creates a generator from a PEM-wrapped PKCS#8
// ECDSA P-256 private key. Used when the key is passed via the API request body
// (no file read needed).
func newLicenseKeyGeneratorFromBytes(keyBytes []byte) (*licenseKeyGenerator, error) {
	key, err := auth.ParseECDSAP256PrivateKeyPEM(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("license signing key: %w", err)
	}
	return &licenseKeyGenerator{privateKey: key, nextIat: time.Now().Unix()}, nil
}

// sign creates a valid license key for the given edition with a monotonically increasing iat.
func (g *licenseKeyGenerator) sign(edition license.Edition, org string, expireIn time.Duration) string {
	iat := g.nextIat
	g.nextIat++
	return license.SignTestLicense(license.Claims{
		Edition: edition,
		Org:     org,
		Exp:     time.Now().Add(expireIn).Unix(),
		Iat:     iat,
	}, g.privateKey)
}

// signExpired creates a key with an expiration in the past (for testing 400 rejection).
func (g *licenseKeyGenerator) signExpired(edition license.Edition) string {
	iat := g.nextIat
	g.nextIat++
	return license.SignTestLicense(license.Claims{
		Edition: edition,
		Org:     "Test Expired",
		Exp:     time.Now().Add(-1 * time.Hour).Unix(),
		Iat:     iat,
	}, g.privateKey)
}

// signWithIat creates a key with a specific iat (for replay testing).
// Does NOT increment nextIat — caller controls the iat value.
func (g *licenseKeyGenerator) signWithIat(edition license.Edition, iat int64) string {
	return license.SignTestLicense(license.Claims{
		Edition: edition,
		Org:     "Test Replay",
		Exp:     time.Now().Add(24 * time.Hour).Unix(),
		Iat:     iat,
	}, g.privateKey)
}
