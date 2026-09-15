package license

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// reloadTestPriv is the test ECDSA P-256 private key shared across reload tests.
// Tests in this file MUST NOT use t.Parallel() since they call SetPublicKeyForTesting.
var reloadTestPriv, reloadTestPub = GenerateTestKeyPair()

func setupReloadTest(t *testing.T) {
	t.Helper()
	SetPublicKeyForTesting(reloadTestPub)
}

func signKey(t *testing.T, edition Edition, org string, exp time.Time, iat int64) string {
	t.Helper()
	return SignTestLicense(Claims{
		Edition: edition,
		Org:     org,
		Exp:     exp.Unix(),
		Iat:     iat,
	}, reloadTestPriv)
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ValidEditionChange(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(key); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if got := m.CurrentEdition(); got != Pro {
		t.Errorf("edition = %s, want Pro", got)
	}
	if got := m.Org(); got != "Acme" {
		t.Errorf("org = %q, want Acme", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ReplayRejected_SameIat(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	iat := time.Now().Unix()
	key1 := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), iat)
	key2 := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), iat)

	if err := m.Reload(key1); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	err := m.Reload(key2)
	if !errors.Is(err, ErrReplayDetected) {
		t.Errorf("second Reload error = %v, want ErrReplayDetected", err)
	}

	if got := m.CurrentEdition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ReplayRejected_OlderIat(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	now := time.Now().Unix()
	key1 := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), now)
	key2 := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), now-100)

	if err := m.Reload(key1); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	err := m.Reload(key2)
	if !errors.Is(err, ErrReplayDetected) {
		t.Errorf("second Reload error = %v, want ErrReplayDetected", err)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_FirstReload_NoCurrentIat(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 12345)
	if err := m.Reload(key); err != nil {
		t.Fatalf("Reload: %v (should accept any iat when current has none)", err)
	}

	if got := m.CurrentEdition(); got != Pro {
		t.Errorf("edition = %s, want Pro", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_InvalidKeyRejected_CurrentPreserved(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(key); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	err := m.Reload("not-a-valid-license-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}

	if got := m.CurrentEdition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged after invalid reload)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ExpiredKeyRejected(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	key := signKey(t, Pro, "Acme", time.Now().Add(-24*time.Hour), time.Now().Unix())
	err := m.Reload(key)
	if err == nil {
		t.Fatal("expected error for expired key")
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_DowngradeAccepted(t *testing.T) {
	setupReloadTest(t)
	m, _ := NewManager("", zerolog.Nop())

	now := time.Now().Unix()

	key1 := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), now)
	if err := m.Reload(key1); err != nil {
		t.Fatalf("first Reload: %v", err)
	}
	if got := m.CurrentEdition(); got != Enterprise {
		t.Fatalf("edition = %s, want Enterprise", got)
	}

	key2 := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), now+1)
	if err := m.Reload(key2); err != nil {
		t.Fatalf("downgrade Reload: %v", err)
	}
	if got := m.CurrentEdition(); got != Pro {
		t.Errorf("edition = %s, want Pro (downgrade)", got)
	}
}
