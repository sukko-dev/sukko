package license

import (
	"fmt"
	"time"
)

// Reload validates a new license key and atomically swaps the Manager's state.
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

	// 1. Parse and verify (ECDSA P-256 signature check)
	claims, err := ParseAndVerify(key)
	if err != nil {
		return fmt.Errorf("reload: %w", err)
	}

	// 2. Replay protection: check iat
	current := m.loadClaims()
	if current != nil && current.Iat > 0 && claims.Iat <= current.Iat {
		return ErrReplayDetected
	}

	// 3. Capture old state for transition logging
	oldEdition := m.CurrentEdition()
	oldClaims := current

	// 4. Resolve new state
	edition := claims.Edition.normalize()
	limits := resolveLimits(edition, claims.Limits)

	// 5. Atomic swap — all three stores happen under the mutex
	m.edition.Store(&editionHolder{edition})
	m.limits.Store(&limitsHolder{limits})
	m.claims.Store(claims)

	// 6. Update Prometheus metric
	licenseExpiryTimestamp.Set(float64(claims.Exp))

	// 7. Log transition
	event := m.logger.Info().
		Str("old_edition", oldEdition.String()).
		Str("new_edition", edition.String()).
		Str("org", claims.Org).
		Time("expires_at", time.Unix(claims.Exp, 0))

	if oldClaims != nil {
		event = event.Time("old_expires_at", time.Unix(oldClaims.Exp, 0))
	}

	event.Msg("license reloaded")

	return nil
}
