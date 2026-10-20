package license

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// sukko_license_expiry_timestamp is the Unix timestamp of the current license's expiry.
// Set at startup and updated on each live reload. Every edition runs on a signed key
// with an expiry (ADR-0038), so this is normally non-zero. A Grafana panel / alert rule
// surfaces "time until expiry" as (sukko_license_expiry_timestamp - time()).
var licenseExpiryTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "sukko_license_expiry_timestamp",
	Help: "Unix timestamp of the current license's expiry.",
})

// editionHolder wraps an Edition for atomic.Pointer storage.
type editionHolder struct{ v Edition }

// limitsHolder wraps Limits for atomic.Pointer storage.
type limitsHolder struct{ v Limits }

// Manager resolves and provides the current edition, limits, and feature gates.
//
// It is created once at startup with NewManager, which validates the signed key. Its
// Edition()/Limits() are set at boot and thereafter only RAISED or renewed by a live
// reload (ReloadNoDowngrade, used by the WatchLicense stream and the admin reload
// API) — they are never lowered mid-flight: a downgrade takes effect at the next
// restart (ADR-0038 Model C). License EXPIRY never changes Edition()/Limits() and
// never shuts the service down; it is surfaced through Claims().IsExpired() (e.g. the
// /edition `expired` flag) and the expiry gauge, not by changing the edition.
// Accessors:
//   - Edition() / Limits() — the current edition and limits
//   - HasFeature()         — uses Edition(), so gates track the current edition
//
// The unconditional Reload (boot-time path used by provisioning's DB-precedence
// resolve) may apply a lower edition — boot is the restart at which a downgrade
// legitimately takes effect; ReloadNoDowngrade is the runtime path that refuses to lower.
type Manager struct {
	// Current edition/limits — set at boot, only raised or renewed at runtime.
	edition atomic.Pointer[editionHolder]
	limits  atomic.Pointer[limitsHolder]

	// Claims from the license key. A Manager built via NewManager/Reload always has
	// non-nil claims (ParseAndVerify succeeded); only NewTestManager leaves them nil.
	// Stored even when expired — useful for debugging, Org() access, and expiry checks.
	claims atomic.Pointer[Claims]

	// reloadMu serializes concurrent Reload() calls on this Manager instance.
	// The three atomic stores must happen together to prevent mixed state.
	reloadMu sync.Mutex

	logger zerolog.Logger
}

// NewManager creates a Manager from a license key string.
//
// Behavior (ADR-0038 — Community requires an account-issued license):
//   - Empty key → ErrNoLicense (fail fast; Community is no longer keyless)
//   - Expired key → error wrapping ErrLicenseExpired (refused at boot; re-key required)
//   - Invalid signature/format → error (corrupt or forged key)
//   - Valid key → edition and limits from claims (with precedence overlay)
//
// Enforcement is at boot (here) and reload only — an expired key is refused exactly
// like a missing one, so there is no mid-flight degrade-to-Community.
func NewManager(licenseKey string, logger zerolog.Logger) (*Manager, error) {
	// No license key → refuse. Services fail fast at startup rather than silently
	// running as Community (ADR-0038). Provisioning resolves its DB-or-env key
	// before calling NewManager, so it never reaches here with an empty key when
	// the DB holds one.
	if licenseKey == "" {
		return nil, ErrNoLicense
	}

	m := &Manager{
		logger: logger.With().Str("component", "license").Logger(),
	}

	// Parse and verify the license key (signature + expiry). Expired and invalid
	// both refuse: enforcement is at boot/reload, so a lapsed key forces a re-key.
	claims, err := ParseAndVerify(licenseKey)
	if err != nil {
		return nil, fmt.Errorf("license validation: %w", err)
	}

	// Valid license — resolve edition and limits
	edition := claims.Edition.normalize()
	limits := resolveLimits(edition, claims.Limits)

	m.edition.Store(&editionHolder{edition})
	m.limits.Store(&limitsHolder{limits})
	m.claims.Store(claims)

	licenseExpiryTimestamp.Set(float64(claims.Exp))
	m.logger.Info().
		Str("edition", edition.String()).
		Str("org", claims.Org).
		Time("expires_at", time.Unix(claims.Exp, 0)).
		Int("nodes", claims.Nodes).
		Msg("License validated")

	return m, nil
}

// Edition returns the booted edition. Fixed at construction; never downgraded
// mid-flight, even after the license expires.
func (m *Manager) Edition() Edition {
	return m.edition.Load().v
}

// Limits returns the booted limits. Fixed at construction; never downgraded
// mid-flight.
func (m *Manager) Limits() Limits {
	return m.limits.Load().v
}

// HasFeature returns true if the booted edition includes the feature. It uses
// Edition(), so feature gates track the booted edition and are not revoked when
// the license expires mid-flight — this Manager never downgrades.
func (m *Manager) HasFeature(feature Feature) bool {
	return EditionHasFeature(m.Edition(), feature)
}

// Claims returns the license claims, or nil if no license key was provided.
// Returns expired claims for degraded licenses (useful for debugging).
func (m *Manager) Claims() *Claims {
	return m.loadClaims()
}

// NewTestManager creates a Manager with a fixed edition for testing.
// It does not require a valid license key — the edition is set directly.
func NewTestManager(edition Edition) *Manager {
	m := &Manager{}
	m.edition.Store(&editionHolder{edition})
	m.limits.Store(&limitsHolder{DefaultLimits(edition)})
	return m
}

// Org returns the licensee organization name, or empty string for Community.
func (m *Manager) Org() string {
	if c := m.loadClaims(); c != nil {
		return c.Org
	}
	return ""
}

// loadClaims safely loads claims from the atomic pointer.
func (m *Manager) loadClaims() *Claims {
	return m.claims.Load()
}

// resolveLimits applies the claims-override-defaults precedence rule.
// For each limit field: if the claims value is > 0, use it; otherwise use the default.
func resolveLimits(edition Edition, claimsLimits Limits) Limits {
	defaults := DefaultLimits(edition)

	if claimsLimits.MaxTenants > 0 {
		defaults.MaxTenants = claimsLimits.MaxTenants
	}
	if claimsLimits.MaxTotalConnections > 0 {
		defaults.MaxTotalConnections = claimsLimits.MaxTotalConnections
	}
	if claimsLimits.MaxShards > 0 {
		defaults.MaxShards = claimsLimits.MaxShards
	}
	if claimsLimits.MaxTopicsPerTenant > 0 {
		defaults.MaxTopicsPerTenant = claimsLimits.MaxTopicsPerTenant
	}
	if claimsLimits.MaxRoutingRulesPerTenant > 0 {
		defaults.MaxRoutingRulesPerTenant = claimsLimits.MaxRoutingRulesPerTenant
	}

	return defaults
}
