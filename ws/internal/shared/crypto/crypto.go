// Package crypto provides AES-256-GCM encryption helpers shared across services.
// Used by the provisioning service (push credentials, license key, webhook secrets)
// and the webhook-worker (decrypting webhook secrets at HMAC computation time).
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// KeySize is the required key length for AES-256.
const KeySize = 32

// errInvalidKeyLength is returned when the parsed key is not exactly 32 bytes.
var errInvalidKeyLength = errors.New("encryption key must be exactly 32 bytes for AES-256")

// EncryptCredential encrypts plaintext using AES-256-GCM and returns a base64-encoded
// ciphertext with the nonce prepended.
func EncryptCredential(plaintext string, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", errInvalidKeyLength
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	// Seal appends the ciphertext to the nonce slice, so the result is nonce + ciphertext + tag.
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(sealed), nil
}

// DecryptCredential base64-decodes the ciphertext, extracts the prepended nonce,
// and decrypts using AES-256-GCM. Returns the plaintext as a string.
func DecryptCredential(ciphertext string, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", errInvalidKeyLength
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}

	b, err := decryptRaw(data, key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecryptRaw decrypts raw AES-256-GCM ciphertext bytes (nonce prepended, not base64-encoded)
// and returns the plaintext as a string. Used when the caller only needs a string result.
func DecryptRaw(data, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", errInvalidKeyLength
	}
	b, err := decryptRaw(data, key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecryptRawToBytes decrypts raw AES-256-GCM ciphertext bytes (nonce prepended, not base64-encoded)
// and returns the plaintext as []byte. Use when the caller must zero the secret after use
// (defer clear(secret)) — avoids an intermediate string allocation that would pin the secret
// in memory beyond the call site. See §IX.
func DecryptRawToBytes(data, key []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, errInvalidKeyLength
	}
	return decryptRaw(data, key)
}

// decryptRaw performs AES-256-GCM decryption on nonce-prepended ciphertext and
// returns the plaintext as []byte. Key must be KeySize bytes; caller validates.
func decryptRaw(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce := data[:nonceSize]
	encrypted := data[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

// ParseEncryptionKey accepts a hex-encoded (64 chars) or base64-encoded string
// and returns the 32-byte key. Returns an error if neither encoding works or
// the result is not exactly 32 bytes.
func ParseEncryptionKey(hexOrBase64 string) ([]byte, error) {
	// Try hex first (64 hex chars = 32 bytes).
	if len(hexOrBase64) == hex.EncodedLen(KeySize) {
		key, err := hex.DecodeString(hexOrBase64)
		if err == nil && len(key) == KeySize {
			return key, nil
		}
	}

	// Try base64.
	key, err := base64.StdEncoding.DecodeString(hexOrBase64)
	if err == nil && len(key) == KeySize {
		return key, nil
	}

	// Try base64 URL encoding as fallback.
	key, err = base64.URLEncoding.DecodeString(hexOrBase64)
	if err == nil && len(key) == KeySize {
		return key, nil
	}

	return nil, fmt.Errorf("encryption key must decode to exactly %d bytes (provide 64-char hex or base64)", KeySize)
}
