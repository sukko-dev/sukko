package license

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// ExpiryWarnWindow is how far before expiry the ticker starts warning.
const ExpiryWarnWindow = 30 * 24 * time.Hour

// expiryCheckInterval is the ticker period for the daily expiry check.
const expiryCheckInterval = 24 * time.Hour

// expiryWatcherComponent is the structured-log component name for the watcher.
const expiryWatcherComponent = "license_expiry_watcher"

// expiryState is the classification of a license's time-to-expiry.
type expiryState int

const (
	expiryOK      expiryState = iota // outside the warning window
	expiryWarn                       // within ExpiryWarnWindow of expiry
	expiryExpired                    // already past expiry
)

// ExpiryWatcher logs a WARN — once at startup, then daily — while the booted license is
// within ExpiryWarnWindow of expiry or already expired. It is detect-to-warn ONLY
// (ADR-0039): it never changes the edition, never shuts the service down, and never
// touches readiness. It exists because a running server survives expiry (enforcement is
// boot/reload only), so a lapse is otherwise silent until the next restart fails — this
// surfaces it in the service's normal log stream (`kubectl logs` / Cloud Run / compose)
// so an operator re-keys ahead of time. One mostly-asleep goroutine; negligible cost.
//
// It mirrors StreamLicenseWatcher's shape (self-contained goroutine + Close) so every
// long-running service wires it the same way (§XVIII).
type ExpiryWatcher struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExpiryWatcher starts the watcher for m and returns it. Call Close() on shutdown.
func NewExpiryWatcher(m *Manager, logger zerolog.Logger) *ExpiryWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &ExpiryWatcher{cancel: cancel}
	lg := logger.With().Str("component", expiryWatcherComponent).Logger()
	w.wg.Go(func() {
		defer logging.RecoverPanic(lg, expiryWatcherComponent, nil)
		runExpiryLoop(ctx, m, lg, expiryCheckInterval)
	})
	return w
}

// Close stops the watcher goroutine and waits for it to exit.
func (w *ExpiryWatcher) Close() {
	w.cancel()
	w.wg.Wait()
}

// runExpiryLoop logs once immediately, then on each tick, until ctx is canceled.
func runExpiryLoop(ctx context.Context, m *Manager, logger zerolog.Logger, interval time.Duration) {
	logExpiry(m, logger, time.Now())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			logExpiry(m, logger, now)
		}
	}
}

// logExpiry reads the manager's live edition and claims (so a renewal applied via reload
// quiets it) and emits the WARN via logExpiryState.
func logExpiry(m *Manager, logger zerolog.Logger, now time.Time) {
	logExpiryState(logger, m.Edition(), m.loadClaims(), now)
}

// logExpiryState emits a WARN when the license is within the warning window or expired,
// and is silent otherwise. Split from logExpiry so the WARN formatting is unit-testable
// without a Manager (NewManager/Reload reject expired keys, so an expired-claims Manager
// cannot be constructed through the public API).
func logExpiryState(logger zerolog.Logger, edition Edition, claims *Claims, now time.Time) {
	state, remaining := classifyExpiry(claims, now, ExpiryWarnWindow)
	switch state {
	case expiryExpired:
		logger.Warn().
			Str("edition", edition.String()).
			Int("days_expired", int((-remaining).Hours()/24)).
			Msg("license EXPIRED — the server keeps running at its booted edition, but it will not restart until re-keyed (ADR-0039)")
	case expiryWarn:
		logger.Warn().
			Str("edition", edition.String()).
			Int("days_left", int(math.Ceil(remaining.Hours()/24))).
			Msg("license expiring soon — re-key before the next restart or the server will not come back up")
	case expiryOK:
		// outside the window — stay silent
	}
}

// classifyExpiry is the pure decision: a nil claims or a zero Exp (no expiry recorded)
// is OK; a non-positive remaining is Expired; within the window is Warn; otherwise OK.
// It returns the signed remaining duration (negative once expired).
func classifyExpiry(claims *Claims, now time.Time, window time.Duration) (expiryState, time.Duration) {
	if claims == nil || claims.Exp == 0 {
		return expiryOK, 0
	}
	remaining := time.Unix(claims.Exp, 0).Sub(now)
	switch {
	case remaining <= 0:
		return expiryExpired, remaining
	case remaining <= window:
		return expiryWarn, remaining
	default:
		return expiryOK, remaining
	}
}
