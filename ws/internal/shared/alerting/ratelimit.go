package alerting

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"sync"
	"time"
)

// RateLimitedAlerter wraps an Alerter with rate limiting to prevent spam.
// It limits the number of identical alerts within a time window.
type RateLimitedAlerter struct {
	alerter Alerter
	window  time.Duration
	max     int

	mu          sync.Mutex
	alerts      map[string]*alertState
	lastCleanup time.Time
}

type alertState struct {
	count      int
	firstSeen  time.Time
	lastSeen   time.Time
	suppressed int
}

// RateLimitConfig holds configuration for rate limiting.
type RateLimitConfig struct {
	// Window is the time window for rate limiting.
	Window time.Duration

	// Max is the maximum number of identical alerts within the window.
	Max int
}

// NewRateLimitedAlerterWithConfig creates a rate-limited alerter with custom config.
// Config values MUST be validated (e.g., via ServerConfig.Validate()) before calling.
func NewRateLimitedAlerterWithConfig(alerter Alerter, cfg RateLimitConfig) *RateLimitedAlerter {
	return &RateLimitedAlerter{
		alerter:     alerter,
		window:      cfg.Window,
		max:         cfg.Max,
		alerts:      make(map[string]*alertState),
		lastCleanup: time.Now(),
	}
}

// Alert sends an alert if it hasn't been rate limited.
func (r *RateLimitedAlerter) Alert(level Level, message string, metadata map[string]any) {
	key := r.alertKey(level, message)
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Periodic cleanup of old entries
	if now.Sub(r.lastCleanup) > r.window {
		r.cleanup(now)
		r.lastCleanup = now
	}

	state, exists := r.alerts[key]
	if !exists {
		// First occurrence
		r.alerts[key] = &alertState{
			count:     1,
			firstSeen: now,
			lastSeen:  now,
		}
		r.mu.Unlock()
		r.alerter.Alert(level, message, metadata)
		r.mu.Lock()
		return
	}

	// Check if window has expired
	if now.Sub(state.firstSeen) > r.window {
		// Reset state
		state.count = 1
		state.firstSeen = now
		state.lastSeen = now
		state.suppressed = 0
		r.mu.Unlock()
		r.alerter.Alert(level, message, metadata)
		r.mu.Lock()
		return
	}

	// Within window
	state.count++
	state.lastSeen = now

	if state.count <= r.max {
		// Under limit, send alert
		r.mu.Unlock()
		r.alerter.Alert(level, message, metadata)
		r.mu.Lock()
		return
	}

	// Over limit, suppress
	state.suppressed++

	// Send a suppression notice periodically (every 10 suppressed alerts)
	if state.suppressed == 10 || state.suppressed == 50 || state.suppressed == 100 {
		suppressedMsg := message + " [RATE LIMITED - see logs for details]"
		suppressedMeta := map[string]any{
			"suppressed_count": state.suppressed,
			"window":           r.window.String(),
			"original_message": message,
		}
		maps.Copy(suppressedMeta, metadata)
		r.mu.Unlock()
		r.alerter.Alert(level, suppressedMsg, suppressedMeta)
		r.mu.Lock()
	}
}

// alertKey generates a unique key for an alert based on level and message.
func (r *RateLimitedAlerter) alertKey(level Level, message string) string {
	h := sha256.New()
	h.Write([]byte(string(level)))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// cleanup removes expired entries.
func (r *RateLimitedAlerter) cleanup(now time.Time) {
	for key, state := range r.alerts {
		if now.Sub(state.firstSeen) > r.window*2 {
			delete(r.alerts, key)
		}
	}
}

// GetSuppressedCount returns the number of suppressed alerts for testing.
func (r *RateLimitedAlerter) GetSuppressedCount(level Level, message string) int {
	key := r.alertKey(level, message)

	r.mu.Lock()
	defer r.mu.Unlock()

	if state, exists := r.alerts[key]; exists {
		return state.suppressed
	}
	return 0
}

// Ensure RateLimitedAlerter implements Alerter.
var _ Alerter = (*RateLimitedAlerter)(nil)
