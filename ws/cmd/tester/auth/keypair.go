// Package auth provides JWT authentication for the tester service.
// It generates ES256 keypairs, registers public keys with provisioning,
// mints per-connection JWTs, and handles test auth lifecycle.
package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// Keypair holds an ES256 (ECDSA P-256) signing keypair for JWT minting.
// The private key is held in memory for signing; the public key PEM is
// registered with the provisioning API.
type Keypair struct {
	// PrivateKey is the ECDSA private key used to sign JWTs.
	PrivateKey *ecdsa.PrivateKey

	// PublicPEM is the PEM-encoded public key for registration with provisioning.
	PublicPEM string

	// KeyID is the key identifier (kid in JWT header), formatted as "tester-{testID}".
	KeyID string
}

// GenerateKeypair creates a new ES256 keypair with a key ID derived from the test ID.
// The key ID follows the provisioning pattern: "tester-{testID}" which is compliant
// with the tenant key ID format ^[a-z][a-z0-9-]{2,62}$.
func GenerateKeypair(testID string) (*Keypair, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ES256 key: %w", err)
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal public key: %w", err)
	}

	pubPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	})

	return &Keypair{
		PrivateKey: privateKey,
		PublicPEM:  string(pubPEM),
		KeyID:      "tester-" + testID,
	}, nil
}
