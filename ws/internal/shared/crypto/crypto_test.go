package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	t.Parallel()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "short string", plaintext: "hello"},
		{name: "json payload", plaintext: `{"project_id":"my-project","private_key":"-----BEGIN RSA..."}`},
		{name: "empty string", plaintext: ""},
		{name: "unicode", plaintext: "credentials with unicode: éàü"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encrypted, err := EncryptCredential(tt.plaintext, key)
			if err != nil {
				t.Fatalf("EncryptCredential() error = %v", err)
			}
			if encrypted == tt.plaintext && tt.plaintext != "" {
				t.Error("encrypted should differ from plaintext")
			}

			decrypted, err := DecryptCredential(encrypted, key)
			if err != nil {
				t.Fatalf("DecryptCredential() error = %v", err)
			}
			if decrypted != tt.plaintext {
				t.Errorf("DecryptCredential() = %q, want %q", decrypted, tt.plaintext)
			}
		})
	}
}

func TestEncryptDifferentNonce(t *testing.T) {
	t.Parallel()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	plaintext := "same plaintext for both encryptions"

	enc1, err := EncryptCredential(plaintext, key)
	if err != nil {
		t.Fatalf("first encrypt: %v", err)
	}
	enc2, err := EncryptCredential(plaintext, key)
	if err != nil {
		t.Fatalf("second encrypt: %v", err)
	}

	if enc1 == enc2 {
		t.Error("encrypting the same plaintext twice should produce different ciphertexts (random nonce)")
	}

	dec1, err := DecryptCredential(enc1, key)
	if err != nil {
		t.Fatalf("decrypt first: %v", err)
	}
	dec2, err := DecryptCredential(enc2, key)
	if err != nil {
		t.Fatalf("decrypt second: %v", err)
	}
	if dec1 != dec2 {
		t.Errorf("decrypted values differ: %q vs %q", dec1, dec2)
	}
}

func TestDecryptWrongKey(t *testing.T) {
	t.Parallel()

	keyA := make([]byte, 32)
	keyB := make([]byte, 32)
	if _, err := rand.Read(keyA); err != nil {
		t.Fatalf("generate key A: %v", err)
	}
	if _, err := rand.Read(keyB); err != nil {
		t.Fatalf("generate key B: %v", err)
	}

	encrypted, err := EncryptCredential("secret data", keyA)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	_, err = DecryptCredential(encrypted, keyB)
	if err == nil {
		t.Fatal("DecryptCredential() with wrong key should return an error")
	}
}

func TestParseEncryptionKey(t *testing.T) {
	t.Parallel()

	rawKey := make([]byte, 32)
	if _, err := rand.Read(rawKey); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "hex encoded 64 chars", input: hex.EncodeToString(rawKey), wantErr: false},
		{name: "base64 standard encoded", input: base64.StdEncoding.EncodeToString(rawKey), wantErr: false},
		{name: "base64 URL encoded", input: base64.URLEncoding.EncodeToString(rawKey), wantErr: false},
		{name: "too short", input: "abcd1234", wantErr: true},
		{name: "wrong length hex", input: hex.EncodeToString(rawKey[:16]), wantErr: true},
		{name: "invalid encoding", input: "not-a-valid-encoding-string-that-is-long-enough-to-test!!!!!!!!", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := ParseEncryptionKey(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseEncryptionKey() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseEncryptionKey() error = %v", err)
			}
			if len(key) != 32 {
				t.Errorf("key length = %d, want 32", len(key))
			}
		})
	}
}

func TestEncryptInvalidKeyLength(t *testing.T) {
	t.Parallel()

	shortKey := make([]byte, 16)
	if _, err := rand.Read(shortKey); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	_, err := EncryptCredential("test", shortKey)
	if err == nil {
		t.Fatal("EncryptCredential() with 16-byte key should return an error")
	}

	_, err = DecryptCredential("dGVzdA==", shortKey)
	if err == nil {
		t.Fatal("DecryptCredential() with 16-byte key should return an error")
	}
}

// TestDecryptRawToBytes covers happy path, tampered ciphertext, nil input,
// empty input, wrong key length — all verified under -race.
func TestDecryptRawToBytes(t *testing.T) {
	t.Parallel()

	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		plaintext := "webhook-secret-value"
		ct, err := EncryptCredential(plaintext, key)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		raw, err := base64.StdEncoding.DecodeString(ct)
		if err != nil {
			t.Fatalf("base64 decode: %v", err)
		}
		got, err := DecryptRawToBytes(raw, key)
		if err != nil {
			t.Fatalf("DecryptRawToBytes() error = %v", err)
		}
		if string(got) != plaintext {
			t.Errorf("got %q, want %q", string(got), plaintext)
		}
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		t.Parallel()
		ct, err := EncryptCredential("original", key)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		raw, _ := base64.StdEncoding.DecodeString(ct)
		// Flip the last byte to tamper the auth tag.
		raw[len(raw)-1] ^= 0xFF
		_, err = DecryptRawToBytes(raw, key)
		if err == nil {
			t.Error("tampered ciphertext should return an error")
		}
	})

	t.Run("nil input", func(t *testing.T) {
		t.Parallel()
		_, err := DecryptRawToBytes(nil, key)
		if err == nil {
			t.Error("nil input should return an error")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()
		_, err := DecryptRawToBytes([]byte{}, key)
		if err == nil {
			t.Error("empty input should return an error (too short)")
		}
	})

	t.Run("wrong key length", func(t *testing.T) {
		t.Parallel()
		ct, err := EncryptCredential("secret", key)
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		raw, _ := base64.StdEncoding.DecodeString(ct)
		shortKey := key[:16]
		_, err = DecryptRawToBytes(raw, shortKey)
		if err == nil {
			t.Error("wrong key length should return an error")
		}
	})
}
