package license

import (
	"fmt"
	"time"
)

// Reload validates a new license key and atomically swaps the Manager's state,
// applying it UNCONDITIONALLY — including a key that lowers the edition. This is
// the boot-time path: provisioning's DB-precedence resolve uses it so a persisted
// key takes effect at startup even when it is lower, because "a downgrade takes
// effect at the next restart" (ADR-0038) and boot is that restart. Runtime reloads
// (the WatchLicense stream and the admin reload API) use ReloadNoDowngrade instead.
//
// If validation fails, the current state is preserved — no partial updates.
//
// Replay protection: if the current key has an iat > 0, the new key's iat must
// be strictly greater. If the current key has no iat (pre-dates the field),
// any valid key is accepted.
//
// Thread-safe: concurrent calls are serialized via the instance's reloadMu mutex.
func (m *Manager) Reload(key string) error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	claims, err := m.parseAndCheckReplayLocked(key)
	if err != nil {
		return err
	}
	m.applyLocked(claims)
	return nil
}

// ReloadNoDowngrade validates a new license key and applies it live ONLY when it
// does not lower the running edition (ADR-0038 Model C — a running server's edition
// is never lowered mid-flight). A key that RAISES the edition or RENEWS the same
// edition (e.g. extends expiry) is applied immediately and returns applied=true. A
// key that would LOWER the edition is NOT applied: the server keeps its current
// (higher) edition and the lower key takes effect only at the next restart; the
// method returns applied=false with a nil error. A downgrade is logged on every
// receipt (including stream reconnects) so the deferral stays visible to operators.
//
// This is the single rule for both runtime surfaces: the WatchLicense stream
// (ws-server/gateway/push clients) and provisioning's admin reload API. Callers
// that must still propagate a downgrade (persist it for the next restart, broadcast
// it to clients) do so themselves regardless of applied — this method governs only
// the LIVE edition of the calling process.
//
// Consequence worth noting: a server whose booted key has EXPIRED still holds its
// (now-expired) edition over a valid but lower key streamed by a restarted
// provisioning, until this server itself restarts — expiry never acts mid-flight
// (the daily license-expiry log line and the Grafana expiry gauge surface this
// instead of any automatic action).
//
// On validation failure (bad signature, expired, replay) the current state is
// preserved and the error is returned with applied=false.
//
// Thread-safe: serialized via reloadMu.
func (m *Manager) ReloadNoDowngrade(key string) (applied bool, err error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	claims, err := m.parseAndCheckReplayLocked(key)
	if err != nil {
		return false, err
	}

	newEdition := claims.Edition.normalize()
	newLimits := resolveLimits(newEdition, claims.Limits)
	// A reload is applied live only when it lowers NEITHER the edition NOR any capacity
	// dimension. An edition downgrade, or a same-edition capacity reduction (a smaller
	// Pro band), is retained until the next restart (ADR-0038 Model C).
	if !newEdition.IsAtLeast(m.Edition()) || !newLimits.CoversAtLeast(m.Limits()) {
		m.logger.Info().
			Str("current_edition", m.Edition().String()).
			Str("pushed_edition", newEdition.String()).
			Str("org", claims.Org).
			Msg("license reload would lower edition or capacity — retaining current license until restart")
		return false, nil
	}

	m.applyLocked(claims)
	return true, nil
}

// parseAndCheckReplayLocked parses and verifies key (ECDSA P-256 signature + expiry)
// and enforces iat replay protection. The caller MUST hold reloadMu. It returns the
// validated claims or an error and does not mutate Manager state.
func (m *Manager) parseAndCheckReplayLocked(key string) (*Claims, error) {
	claims, err := ParseAndVerify(key)
	if err != nil {
		return nil, fmt.Errorf("reload: %w", err)
	}

	current := m.loadClaims()
	if current != nil && current.Iat > 0 && claims.Iat <= current.Iat {
		return nil, ErrReplayDetected
	}
	return claims, nil
}

// applyLocked atomically swaps edition/limits/claims to the given claims, updates
// the expiry gauge, and logs the transition. The caller MUST hold reloadMu.
func (m *Manager) applyLocked(claims *Claims) {
	oldEdition := m.Edition()
	oldClaims := m.loadClaims()

	edition := claims.Edition.normalize()
	limits := resolveLimits(edition, claims.Limits)

	m.edition.Store(&editionHolder{edition})
	m.limits.Store(&limitsHolder{limits})
	m.claims.Store(claims)

	licenseExpiryTimestamp.Set(float64(claims.Exp))

	event := m.logger.Info().
		Str("old_edition", oldEdition.String()).
		Str("new_edition", edition.String()).
		Str("org", claims.Org).
		Time("expires_at", time.Unix(claims.Exp, 0))

	if oldClaims != nil {
		event = event.Time("old_expires_at", time.Unix(oldClaims.Exp, 0))
	}

	event.Msg("license reloaded")
}
