package license

import (
	"errors"
	"testing"
	"time"
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
	m := NewTestManager(Community)

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(key); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro", got)
	}
	if got := m.Org(); got != "Acme" {
		t.Errorf("org = %q, want Acme", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ReplayRejected_SameIat(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

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

	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ReplayRejected_OlderIat(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

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
	m := NewTestManager(Community)

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 12345)
	if err := m.Reload(key); err != nil {
		t.Fatalf("Reload: %v (should accept any iat when current has none)", err)
	}

	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_InvalidKeyRejected_CurrentPreserved(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	key := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(key); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	err := m.Reload("not-a-valid-license-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}

	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged after invalid reload)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_ExpiredKeyRejected(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	// Boot a live Pro edition first so the assertion that an expired reload does
	// NOT downgrade is meaningful (ADR-0038: expiry never acts on a running server).
	live := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(live); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	expired := signKey(t, Enterprise, "Acme", time.Now().Add(-24*time.Hour), time.Now().Unix()+1)
	if err := m.Reload(expired); err == nil {
		t.Fatal("expected error for expired key")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (expired reload must not change booted edition)", got)
	}
}

// TestReload_EmptyKeyRejected_CurrentPreserved pins the deliberate no-op on an
// empty key: WatchLicense streams an empty key when the provisioning DB key is
// cleared, and a running server MUST keep its booted edition rather than silently
// drop to Community (ADR-0038). The stream guards against this before calling
// Reload; this asserts Reload itself is also safe (§II defense in depth).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_EmptyKeyRejected_CurrentPreserved(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	live := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), time.Now().Unix())
	if err := m.Reload(live); err != nil {
		t.Fatalf("first Reload: %v", err)
	}

	if err := m.Reload(""); err == nil {
		t.Fatal("expected error for empty key")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (empty reload must not change booted edition)", got)
	}
}

// TestReload_DowngradeAccepted pins that the UNCONDITIONAL Reload (the boot-time
// path used by provisioning's DB-precedence resolve) applies a downgrade — boot is
// the restart at which a lower persisted key legitimately takes effect (ADR-0038).
// The runtime path (ReloadNoDowngrade) deliberately differs; see below.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReload_DowngradeAccepted(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	now := time.Now().Unix()

	key1 := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), now)
	if err := m.Reload(key1); err != nil {
		t.Fatalf("first Reload: %v", err)
	}
	if got := m.Edition(); got != Enterprise {
		t.Fatalf("edition = %s, want Enterprise", got)
	}

	key2 := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), now+1)
	if err := m.Reload(key2); err != nil {
		t.Fatalf("downgrade Reload: %v", err)
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (downgrade)", got)
	}
}

// --- ReloadNoDowngrade: the runtime path (ADR-0038 Model C) ---
// A live reload raises or renews the edition immediately but NEVER lowers a running
// server's edition; a downgrade is reported as not-applied (applied=false, nil err)
// and takes effect only at the next restart.

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_UpgradeAppliedLive(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	pro := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 10)
	applied, err := m.ReloadNoDowngrade(pro)
	if err != nil {
		t.Fatalf("ReloadNoDowngrade: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true (Community→Pro is an upgrade)")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_DowngradeNotAppliedLive(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	ent := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), 10)
	if err := m.Reload(ent); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	// A valid, strictly-newer (iat) Pro key — a genuine downgrade, not a replay.
	pro := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 20)
	applied, err := m.ReloadNoDowngrade(pro)
	if err != nil {
		t.Fatalf("ReloadNoDowngrade returned error, want nil: %v", err)
	}
	if applied {
		t.Error("applied = true, want false (Enterprise→Pro must not lower a running server)")
	}
	if got := m.Edition(); got != Enterprise {
		t.Errorf("edition = %s, want Enterprise (unchanged — downgrade deferred to restart)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_SameEditionRenewalAppliedLive(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	early := time.Now().Add(30 * 24 * time.Hour)
	pro1 := signKey(t, Pro, "Acme", early, 10)
	if err := m.Reload(pro1); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	later := time.Now().Add(365 * 24 * time.Hour)
	pro2 := signKey(t, Pro, "Acme", later, 20)
	applied, err := m.ReloadNoDowngrade(pro2)
	if err != nil {
		t.Fatalf("ReloadNoDowngrade: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true (same-edition renewal must apply live)")
	}
	if got := m.Claims().Exp; got != later.Unix() {
		t.Errorf("expiry = %d, want %d (renewal must extend expiry live)", got, later.Unix())
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_ReplayRejected_StatePreserved(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	pro := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 10)
	if err := m.Reload(pro); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	// An Enterprise (would-be upgrade) key but with an OLDER iat — replay must be
	// rejected before the edition is ever considered.
	stale := signKey(t, Enterprise, "Acme", time.Now().Add(365*24*time.Hour), 5)
	applied, err := m.ReloadNoDowngrade(stale)
	if !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("err = %v, want ErrReplayDetected", err)
	}
	if applied {
		t.Error("applied = true, want false on replay")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged after replay)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_ExpiredRejected_StatePreserved(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	pro := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 10)
	if err := m.Reload(pro); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	expired := signKey(t, Enterprise, "Acme", time.Now().Add(-time.Hour), 20)
	applied, err := m.ReloadNoDowngrade(expired)
	if !errors.Is(err, ErrLicenseExpired) {
		t.Fatalf("err = %v, want ErrLicenseExpired", err)
	}
	if applied {
		t.Error("applied = true, want false on expired key")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged after expired reload)", got)
	}
}

// TestReloadNoDowngrade_SameEditionCapacityDowngradeDeferred: a same-edition key that
// LOWERS a capacity dimension (a smaller Pro band) is not applied live — the running
// server keeps its higher capacity until restart (ADR-0038 Model C extends to limits).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_SameEditionCapacityDowngradeDeferred(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	exp := time.Now().Add(365 * 24 * time.Hour)
	hi := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: exp.Unix(), Iat: 10, Limits: Limits{MaxTotalConnections: 30000}}, reloadTestPriv)
	if err := m.Reload(hi); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}
	if got := m.Limits().MaxTotalConnections; got != 30000 {
		t.Fatalf("booted MaxTotalConnections = %d, want 30000", got)
	}

	lo := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: exp.Unix(), Iat: 20, Limits: Limits{MaxTotalConnections: 2000}}, reloadTestPriv)
	applied, err := m.ReloadNoDowngrade(lo)
	if err != nil {
		t.Fatalf("ReloadNoDowngrade returned error, want nil: %v", err)
	}
	if applied {
		t.Error("applied = true, want false (same-edition capacity downgrade must defer)")
	}
	if got := m.Limits().MaxTotalConnections; got != 30000 {
		t.Errorf("MaxTotalConnections = %d, want 30000 (capacity downgrade deferred to restart)", got)
	}
}

// TestReloadNoDowngrade_SameEditionCapacityUpgradeApplied: a same-edition key that RAISES
// capacity is applied live (the upgrade half of the rule).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_SameEditionCapacityUpgradeApplied(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	exp := time.Now().Add(365 * 24 * time.Hour)
	lo := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: exp.Unix(), Iat: 10, Limits: Limits{MaxTotalConnections: 2000}}, reloadTestPriv)
	if err := m.Reload(lo); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	hi := SignTestLicense(Claims{Edition: Pro, Org: "Acme", Exp: exp.Unix(), Iat: 20, Limits: Limits{MaxTotalConnections: 30000}}, reloadTestPriv)
	applied, err := m.ReloadNoDowngrade(hi)
	if err != nil {
		t.Fatalf("ReloadNoDowngrade: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true (same-edition capacity upgrade must apply live)")
	}
	if got := m.Limits().MaxTotalConnections; got != 30000 {
		t.Errorf("MaxTotalConnections = %d, want 30000 (capacity upgrade applied live)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestReloadNoDowngrade_InvalidRejected_StatePreserved(t *testing.T) {
	setupReloadTest(t)
	m := NewTestManager(Community)

	pro := signKey(t, Pro, "Acme", time.Now().Add(365*24*time.Hour), 10)
	if err := m.Reload(pro); err != nil {
		t.Fatalf("boot Reload: %v", err)
	}

	applied, err := m.ReloadNoDowngrade("not-a-valid-license-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}
	if applied {
		t.Error("applied = true, want false on invalid key")
	}
	if got := m.Edition(); got != Pro {
		t.Errorf("edition = %s, want Pro (unchanged after invalid reload)", got)
	}
}
