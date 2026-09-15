package license

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// MUST NOT use t.Parallel() — tests share package-level publicKey via SetPublicKeyForTesting.

var testLogger = zerolog.Nop()

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestNewManager_NoKey(t *testing.T) {
	m, err := NewManager("", testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Edition() != Community {
		t.Errorf("Edition() = %q, want Community", m.Edition())
	}
	if m.CurrentEdition() != Community {
		t.Errorf("CurrentEdition() = %q, want Community", m.CurrentEdition())
	}
	if m.Claims() != nil {
		t.Error("Claims() should be nil for no-key Community")
	}
	if m.Org() != "" {
		t.Errorf("Org() = %q, want empty", m.Org())
	}

	// Limits should be Community defaults
	limits := m.Limits()
	if limits.MaxTenants != 3 {
		t.Errorf("MaxTenants = %d, want 3", limits.MaxTenants)
	}
	if limits.MaxShards != 1 {
		t.Errorf("MaxShards = %d, want 1", limits.MaxShards)
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
	if m.CurrentEdition() != Pro {
		t.Errorf("CurrentEdition() = %q, want Pro", m.CurrentEdition())
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

	// Expired → degrades to Community (NOT error)
	m, err := NewManager(key, testLogger)
	if err != nil {
		t.Fatalf("expected no error for expired license (should degrade), got: %v", err)
	}
	if m.Edition() != Community {
		t.Errorf("Edition() = %q, want Community (degraded)", m.Edition())
	}
	if m.CurrentEdition() != Community {
		t.Errorf("CurrentEdition() = %q, want Community", m.CurrentEdition())
	}

	// Claims still available for debugging
	if m.Claims() == nil {
		t.Fatal("Claims() should be non-nil for degraded license")
	}
	if m.Claims().Org != "ExpiredCo" {
		t.Errorf("Claims().Org = %q, want ExpiredCo", m.Claims().Org)
	}

	// Limits are Community
	if m.Limits().MaxTenants != 3 {
		t.Errorf("MaxTenants = %d, want 3 (Community)", m.Limits().MaxTenants)
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

	// After expiry: startup-resolved stays Pro, current-aware switches to Community
	if m.Edition() != Pro {
		t.Errorf("Edition() = %q, want Pro (startup-resolved, never changes)", m.Edition())
	}
	if m.CurrentEdition() != Community {
		t.Errorf("CurrentEdition() = %q, want Community (expired mid-flight)", m.CurrentEdition())
	}
	// HasFeature uses Edition() (startup-resolved), so it returns true even after expiry —
	// features remain active during the grace period; shutdown is the enforcement mechanism.
	if !m.HasFeature(SSETransport) {
		t.Error("HasFeature(SSETransport) should be true after expiry: Edition()=Pro (startup-resolved), grace period keeps features active")
	}

	// CurrentLimits should be Community defaults
	currentLimits := m.CurrentLimits()
	if currentLimits.MaxTenants != 3 {
		t.Errorf("CurrentLimits().MaxTenants = %d, want 3 (Community)", currentLimits.MaxTenants)
	}

	// Startup Limits should still be Pro
	startupLimits := m.Limits()
	if startupLimits.MaxTenants != 50 {
		t.Errorf("Limits().MaxTenants = %d, want 50 (Pro, startup-resolved)", startupLimits.MaxTenants)
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
	cm, err := NewManager("", testLogger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cm.HasFeature(KafkaBackend) {
		t.Error("Community should have KafkaBackend (data path is Community per ADR-0009)")
	}
	if cm.HasFeature(SSETransport) {
		t.Error("Community should NOT have SSETransport")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestHasFeature_UsesEditionNotCurrentEdition(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// License valid at construction (current Unix second), expires after a short sleep.
	// Edition() == Enterprise (startup-resolved), CurrentEdition() == Community (expiry-degraded).
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
		t.Fatalf("Edition() = %q, want Enterprise (startup-resolved) — license not yet expired at construction", m.Edition())
	}

	// Wait for the next second so time.Now().Unix() > claims.Exp
	time.Sleep(1100 * time.Millisecond)

	if m.CurrentEdition() != Community {
		t.Fatalf("CurrentEdition() = %q, want Community (expiry-degraded after sleep)", m.CurrentEdition())
	}

	// HasFeature uses Edition() — Enterprise features must remain active during grace period.
	if !m.HasFeature(MobilePush) {
		t.Error("HasFeature(MobilePush) = false for mid-flight expired Enterprise license, want true: grace period keeps features active until shutdown")
	}
}
