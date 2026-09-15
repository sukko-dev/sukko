package provapi

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
)

func newTestSnapshot() *RevocationSnapshot {
	return &RevocationSnapshot{
		JTIRevocations: make(map[string]*jtiEntry),
		SubRevocations: make(map[string]*subEntry),
	}
}

func newTestRegistryWithSnapshot(snap *RevocationSnapshot) *StreamRevocationRegistry {
	r := &StreamRevocationRegistry{
		mapEntriesGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_revocation_map_entries_" + time.Now().Format("150405.000000000"),
		}),
	}
	r.snapshot.Store(snap)
	return r
}

func TestIsRevoked_ByJTI(t *testing.T) {
	t.Parallel()
	snap := newTestSnapshot()
	snap.JTIRevocations["tok-123"] = &jtiEntry{
		TenantID:  "acme",
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	r := newTestRegistryWithSnapshot(snap)

	if !r.IsRevoked("tok-123", "alice", "acme", time.Now().Unix()) {
		t.Error("expected revoked by jti")
	}
}

func TestIsRevoked_ByJTI_WrongTenant(t *testing.T) {
	t.Parallel()
	snap := newTestSnapshot()
	snap.JTIRevocations["tok-123"] = &jtiEntry{
		TenantID:  "acme",
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	r := newTestRegistryWithSnapshot(snap)

	if r.IsRevoked("tok-123", "alice", "globex", time.Now().Unix()) {
		t.Error("should not be revoked — different tenant")
	}
}

func TestIsRevoked_BySub(t *testing.T) {
	t.Parallel()
	revokedAt := time.Now().Unix()
	snap := newTestSnapshot()
	snap.SubRevocations["acme:alice"] = &subEntry{
		RevokedAt: revokedAt,
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	r := newTestRegistryWithSnapshot(snap)

	// Token issued before revocation — revoked
	if !r.IsRevoked("tok-x", "alice", "acme", revokedAt-10) {
		t.Error("expected revoked — iat < revoked_at")
	}

	// Token issued after revocation — not revoked (re-enabled user)
	if r.IsRevoked("tok-y", "alice", "acme", revokedAt+10) {
		t.Error("should not be revoked — iat > revoked_at")
	}
}

// TestIsRevoked_BySub_IatBoundary pins the exact iat==RevokedAt boundary of the real predicate
// (the gateway post-register re-check mirrors this in its mock; this test binds the mock to the
// real implementation for the cross-path-agreement argument). The condition is `iat < RevokedAt`,
// so iat == RevokedAt is NOT revoked (re-enabled boundary), consistent with the fan-out's
// `iat >= RevokedAt` skip.
func TestIsRevoked_BySub_IatBoundary(t *testing.T) {
	t.Parallel()
	const revokedAt = int64(1000)
	snap := newTestSnapshot()
	snap.SubRevocations["acme:alice"] = &subEntry{
		RevokedAt: revokedAt,
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
	r := newTestRegistryWithSnapshot(snap)

	if !r.IsRevoked("tok-x", "alice", "acme", revokedAt-1) {
		t.Error("iat < RevokedAt must be revoked")
	}
	if r.IsRevoked("tok-x", "alice", "acme", revokedAt) {
		t.Error("iat == RevokedAt must NOT be revoked (boundary — re-enabled)")
	}
	if r.IsRevoked("tok-x", "alice", "acme", revokedAt+1) {
		t.Error("iat > RevokedAt must NOT be revoked")
	}
}

func TestIsRevoked_NotRevoked(t *testing.T) {
	t.Parallel()
	r := newTestRegistryWithSnapshot(newTestSnapshot())

	if r.IsRevoked("tok-unknown", "bob", "acme", time.Now().Unix()) {
		t.Error("should not be revoked — no entries")
	}
}

func TestIsRevoked_EmptyJTI(t *testing.T) {
	t.Parallel()
	snap := newTestSnapshot()
	snap.JTIRevocations["tok-123"] = &jtiEntry{TenantID: "acme", ExpiresAt: time.Now().Add(1 * time.Hour).Unix()}
	r := newTestRegistryWithSnapshot(snap)

	if r.IsRevoked("", "bob", "acme", time.Now().Unix()) {
		t.Error("should not be revoked — empty jti, no sub revocation")
	}
}

func TestApplySnapshot(t *testing.T) {
	t.Parallel()
	r := newTestRegistryWithSnapshot(newTestSnapshot())

	now := time.Now().Unix()
	revocations := []*provisioningv1.TokenRevocation{
		{TenantSlug: "acme", Type: "token", Jti: "tok-1", ExpiresAt: now + 3600},
		{TenantSlug: "acme", Type: "user", Sub: "alice", RevokedAt: now, ExpiresAt: now + 3600},
		{TenantSlug: "acme", Type: "token", Jti: "expired", ExpiresAt: now - 100},
	}

	r.applySnapshot(revocations)

	snap := r.snapshot.Load().(*RevocationSnapshot)
	if len(snap.JTIRevocations) != 1 {
		t.Errorf("JTI entries = %d, want 1", len(snap.JTIRevocations))
	}
	if len(snap.SubRevocations) != 1 {
		t.Errorf("Sub entries = %d, want 1", len(snap.SubRevocations))
	}
}

func TestApplyDelta_AddAndRemove(t *testing.T) {
	t.Parallel()

	now := time.Now().Unix()
	initial := &RevocationSnapshot{
		JTIRevocations: map[string]*jtiEntry{
			"tok-1": {TenantID: "acme", ExpiresAt: now + 3600},
		},
		SubRevocations: make(map[string]*subEntry),
	}
	r := newTestRegistryWithSnapshot(initial)

	delta := []*provisioningv1.TokenRevocation{
		{TenantSlug: "acme", Type: "token", Jti: "tok-2", ExpiresAt: now + 3600},
		{TenantSlug: "acme", Type: "token", Jti: "tok-1", Removed: true},
	}

	r.applyDelta(delta)

	snap := r.snapshot.Load().(*RevocationSnapshot)
	if _, ok := snap.JTIRevocations["tok-1"]; ok {
		t.Error("tok-1 should be removed")
	}
	if _, ok := snap.JTIRevocations["tok-2"]; !ok {
		t.Error("tok-2 should be added")
	}
}
