package license

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// MUST NOT use t.Parallel() — tests share package-level publicKey via SetPublicKeyForTesting.

var testLogger = zerolog.Nop()

// TestNewManager_NoKey (ADR-0038): no license key → refuse to serve. Community is
// no longer keyless-by-default; an empty key fails construction so the service
// fails fast at startup rather than silently running as Community.
func TestNewManager_NoKey(t *testing.T) {
	t.Parallel()
	m, err := NewManager("", testLogger)
	if !errors.Is(err, ErrNoLicense) {
		t.Fatalf("NewManager(\"\") err = %v, want ErrNoLicense", err)
	}
	if m != nil {
		t.Error("manager should be nil when no license key is provided")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_ValidPro(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "Acme", Exp: time.Now().Add(time.Hour).Unix(), Nodes: 3}
	key := SignTestLicense(claims, priv)

	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Edition() != Pro {
		t.Errorf("Edition() = %q, want Pro", m.Edition())
	}
	if m.Org() != "Acme" {
		t.Errorf("Org() = %q, want Acme", m.Org())
	}
	if m.Claims() == nil {
		t.Fatal("Claims() should not be nil")
	}

	limits := m.Limits()
	if limits.MaxTenants != 50 {
		t.Errorf("MaxTenants = %d, want 50 (Pro default)", limits.MaxTenants)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_ValidEnterprise(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Enterprise, Org: "BigCo", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, priv)

	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Edition() != Enterprise {
		t.Errorf("Edition() = %q, want Enterprise", m.Edition())
	}

	limits := m.Limits()
	if limits.MaxTenants != 0 {
		t.Errorf("MaxTenants = %d, want 0 (unlimited)", limits.MaxTenants)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_ExpiredAtStartup(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "ExpiredCo", Exp: time.Now().Add(-time.Hour).Unix()}
	key := SignTestLicense(claims, priv)

	// Expired at boot → refuse (fail-fast), no degrade-to-Community (ADR-0038
	// increment B). Enforcement happens at boot/reload: an expired key is rejected
	// exactly like a missing one, forcing the operator to re-key.
	m, err := NewManager(key, testLogger)
	if !errors.Is(err, ErrLicenseExpired) {
		t.Fatalf("NewManager(expired) err = %v, want ErrLicenseExpired", err)
	}
	if m != nil {
		t.Error("manager should be nil for an expired-at-boot license")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_MidFlightExpiry(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// License expires in the past + 1 second buffer to ensure it's valid NOW
	// but will be expired after a short sleep. Use Unix() which has second precision,
	// so we set exp to "current second" — valid during this second, expired next second.
	claims := Claims{Edition: Pro, Org: "SoonExpired", Exp: time.Now().Unix()}
	key := SignTestLicense(claims, priv)

	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// At this moment, the license is valid (time.Now().Unix() == claims.Exp, and > check fails)
	// Edition() always returns startup-resolved value
	if m.Edition() != Pro {
		t.Errorf("Edition() = %q, want Pro (startup-resolved)", m.Edition())
	}

	// Wait for the next second so time.Now().Unix() > claims.Exp
	time.Sleep(1100 * time.Millisecond)

	// After expiry: the booted values are HELD until restart — no mid-flight
	// downgrade (ADR-0038). Edition, Limits, and features stay Pro; the only
	// signal of lapse is Claims().IsExpired(), which reporting surfaces (the
	// /edition `expired` flag) without lying about the enforced edition.
	if m.Edition() != Pro {
		t.Errorf("Edition() = %q, want Pro (booted value held after expiry)", m.Edition())
	}
	if m.Limits().MaxTenants != 50 {
		t.Errorf("Limits().MaxTenants = %d, want 50 (booted Pro limits held)", m.Limits().MaxTenants)
	}
	if !m.HasFeature(SSETransport) {
		t.Error("HasFeature(SSETransport) should stay true after expiry — features held until restart")
	}
	if c := m.Claims(); c == nil || !c.IsExpired() {
		t.Error("Claims().IsExpired() should be true — the honest expiry signal")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_InvalidSignature(t *testing.T) {
	_, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	otherPriv, _ := GenerateTestKeyPair()
	claims := Claims{Edition: Pro, Org: "Forged", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, otherPriv)

	_, err := NewManager(key, testLogger)
	if err == nil {
		t.Fatal("expected error for invalid signature")
	}
	if !errors.Is(err, ErrLicenseInvalidSignature) {
		t.Errorf("expected ErrLicenseInvalidSignature, got: %v", err)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_MalformedKey(t *testing.T) {
	_, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	_, err := NewManager("not-a-valid-key", testLogger)
	if err == nil {
		t.Fatal("expected error for malformed key")
	}
	if !errors.Is(err, ErrLicenseInvalidFormat) {
		t.Errorf("expected ErrLicenseInvalidFormat, got: %v", err)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_LimitsPrecedence(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// Pro key with custom tenants limit (100 instead of default 50)
	claims := Claims{
		Edition: Pro,
		Org:     "CustomLimits",
		Exp:     time.Now().Add(time.Hour).Unix(),
		Limits:  Limits{MaxTenants: 100},
	}
	key := SignTestLicense(claims, priv)

	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	limits := m.Limits()
	if limits.MaxTenants != 100 {
		t.Errorf("MaxTenants = %d, want 100 (custom override)", limits.MaxTenants)
	}
	// Other limits should be Pro defaults
	if limits.MaxTotalConnections != 10000 {
		t.Errorf("MaxTotalConnections = %d, want 10000 (Pro default)", limits.MaxTotalConnections)
	}
	if limits.MaxShards != 8 {
		t.Errorf("MaxShards = %d, want 8 (Pro default)", limits.MaxShards)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestManager_HasFeature(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// Pro manager
	claims := Claims{Edition: Pro, Org: "Test", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, priv)
	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !m.HasFeature(SSETransport) {
		t.Error("Pro should have SSETransport")
	}
	if !m.HasFeature(Alerting) {
		t.Error("Pro should have Alerting")
	}
	if m.HasFeature(MobilePush) {
		t.Error("Pro should NOT have MobilePush (Enterprise only)")
	}

	// Community manager
	cm := NewTestManager(Community)
	if !cm.HasFeature(KafkaBackend) {
		t.Error("Community should have KafkaBackend (data path is Community per ADR-0009)")
	}
	if cm.HasFeature(SSETransport) {
		t.Error("Community should NOT have SSETransport")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestHasFeature_HeldAfterMidFlightExpiry(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// License valid at construction (current Unix second), expires after a short sleep.
	// The booted Enterprise edition is held until restart (ADR-0038) — no mid-flight
	// downgrade — so features stay active even once the key has lapsed.
	claims := Claims{
		Edition: Enterprise,
		Org:     "MidFlightEnterprise",
		Exp:     time.Now().Unix(), // valid during current second boundary
	}
	key := SignTestLicense(claims, priv)

	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	if m.Edition() != Enterprise {
		t.Fatalf("Edition() = %q, want Enterprise — license not yet expired at construction", m.Edition())
	}

	// Wait for the next second so time.Now().Unix() > claims.Exp.
	time.Sleep(1100 * time.Millisecond)

	// Expired mid-flight, but the booted edition and its features are held.
	if c := m.Claims(); c == nil || !c.IsExpired() {
		t.Fatal("precondition: license should be expired mid-flight")
	}
	if m.Edition() != Enterprise {
		t.Errorf("Edition() = %q, want Enterprise (booted edition held after expiry)", m.Edition())
	}
	if !m.HasFeature(MobilePush) {
		t.Error("HasFeature(MobilePush) = false after mid-flight expiry, want true: features held until restart, not revoked")
	}
}
