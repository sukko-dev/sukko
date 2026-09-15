package limits

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// ConnectionRateLimiter provides DoS protection through rate limiting of connection attempts.
//
// Two-level rate limiting:
//   - Per-IP: Prevents single IP from flooding connections
//   - Global: Prevents system-wide overload from distributed attacks
//
// Uses token bucket algorithm (golang.org/x/time/rate) for smooth rate limiting.
//
// Security benefits:
//   - Prevents connection flood DoS attacks
//   - Limits impact of compromised clients
//   - Protects against distributed attacks
//   - Allows legitimate reconnection bursts
type ConnectionRateLimiter struct {
	// Per-IP rate limiting - prevents single client from flooding connections
	ipLimiters map[string]*ipLimiterEntry // IP address → limiter entry
	ipMu       sync.RWMutex               // Protects ipLimiters map
	ipBurst    int                        // Max burst connections per IP (default: 10)
	ipRate     float64                    // Sustained connections/sec per IP (default: 1.0)
	ipTTL      time.Duration              // Cleanup inactive IPs after this duration (default: 5m)

	// Global rate limiting - prevents system-wide overload
	globalLimiter *rate.Limiter // System-wide token bucket limiter
	globalBurst   int           // Max burst connections system-wide (default: 300)
	globalRate    float64       // Sustained connections/sec system-wide (default: 50.0)

	logger zerolog.Logger // Structured logger with "connection_rate_limiter" component tag

	// Background cleanup - removes stale IP entries to prevent memory leak
	cleanupTicker *time.Ticker   // Triggers cleanup every minute
	stopCleanup   chan struct{}  // Signals cleanup goroutine to exit
	wg            sync.WaitGroup // Tracks cleanup goroutine for shutdown
}

// ipLimiterEntry holds a rate limiter and last access time for an IP
type ipLimiterEntry struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

// ConnectionRateLimiterConfig holds configuration for connection rate limiting
type ConnectionRateLimiterConfig struct {
	// Per-IP limits
	IPBurst int           // Max burst connections per IP (default: 10)
	IPRate  float64       // Sustained connections/sec per IP (default: 1.0)
	IPTTL   time.Duration // Cleanup inactive IPs after this duration (default: 5 minutes)

	// Global limits
	GlobalBurst int     // Max burst connections system-wide (default: 300)
	GlobalRate  float64 // Sustained connections/sec system-wide (default: 50.0)

	// CleanupInterval controls how often stale IP entries are removed.
	CleanupInterval time.Duration

	// Logger
	Logger zerolog.Logger
}

// NewConnectionRateLimiter creates a new connection rate limiter.
// Config values MUST be validated (e.g., via ServerConfig.Validate()) before calling;
// zero-value durations will panic.
func NewConnectionRateLimiter(config ConnectionRateLimiterConfig) *ConnectionRateLimiter {
	limiter := &ConnectionRateLimiter{
		ipLimiters:    make(map[string]*ipLimiterEntry),
		ipBurst:       config.IPBurst,
		ipRate:        config.IPRate,
		ipTTL:         config.IPTTL,
		globalLimiter: rate.NewLimiter(rate.Limit(config.GlobalRate), config.GlobalBurst),
		globalBurst:   config.GlobalBurst,
		globalRate:    config.GlobalRate,
		logger:        config.Logger.With().Str("component", "connection_rate_limiter").Logger(),
		stopCleanup:   make(chan struct{}),
	}

	// Start cleanup goroutine with proper lifecycle management
	limiter.cleanupTicker = time.NewTicker(config.CleanupInterval)
	limiter.wg.Go(func() {
		defer logging.RecoverPanic(limiter.logger, "ConnectionRateLimiter.cleanupLoop", nil)
		limiter.cleanupLoop()
	})

	limiter.logger.Info().
		Int("ip_burst", config.IPBurst).
		Float64("ip_rate", config.IPRate).
		Dur("ip_ttl", config.IPTTL).
		Int("global_burst", config.GlobalBurst).
		Float64("global_rate", config.GlobalRate).
		Msg("ConnectionRateLimiter initialized")

	return limiter
}

// RetryAfter reports how long a rejected caller should wait: the per-IP one-token refill
// time. A 429 can also come from the global bucket, which refills faster, so this is the
// conservative of the two. Mirrors gateway.PublishRateLimiter.RetryAfter (§XVIII). A
// non-positive rate (config validates > 0 when enabled) falls back to one second.
func (crl *ConnectionRateLimiter) RetryAfter() time.Duration {
	if crl.ipRate <= 0 {
		return time.Second
	}
	return time.Duration(float64(time.Second) / crl.ipRate)
}

// CheckConnectionAllowed checks if a connection from the given IP is allowed.
//
// Returns:
//   - true: Connection allowed
//   - false: Connection rate limited (should reject with 429 Too Many Requests)
//
// Rate limiting logic:
//  1. Check global rate limit first (system-wide protection)
//  2. If global allows, check per-IP rate limit (per-client protection)
//  3. If both allow, connection proceeds
//
// Example:
//
//	if !limiter.CheckConnectionAllowed(clientIP) {
//	    http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
//	    return
//	}
func (crl *ConnectionRateLimiter) CheckConnectionAllowed(ip string) bool {
	// Check global rate limit first (fast path - no map lookup)
	if !crl.globalLimiter.Allow() {
		crl.logger.Debug().
			Str("ip", ip).
			Float64("global_rate", crl.globalRate).
			Int("global_burst", crl.globalBurst).
			Msg("Connection rejected: global rate limit exceeded")

		// Update Prometheus metrics
		metrics.IncrementConnectionRateLimit("global")
		return false
	}

	// Check per-IP rate limit
	limiter := crl.getIPLimiter(ip)
	if !limiter.Allow() {
		crl.logger.Debug().
			Str("ip", ip).
			Float64("ip_rate", crl.ipRate).
			Int("ip_burst", crl.ipBurst).
			Msg("Connection rejected: per-IP rate limit exceeded")

		// Update Prometheus metrics
		metrics.IncrementConnectionRateLimit("per_ip")
		return false
	}

	// Both checks passed
	return true
}

// getIPLimiter retrieves or creates a rate limiter for the given IP address.
// This is an internal method used by CheckConnectionAllowed.
func (crl *ConnectionRateLimiter) getIPLimiter(ip string) *rate.Limiter {
	crl.ipMu.RLock()
	entry, exists := crl.ipLimiters[ip]
	crl.ipMu.RUnlock()

	if exists {
		// Update last access time (used for cleanup)
		crl.ipMu.Lock()
		entry.lastAccess = time.Now()
		crl.ipMu.Unlock()
		return entry.limiter
	}

	// Create new rate limiter for this IP
	crl.ipMu.Lock()
	defer crl.ipMu.Unlock()

	// Double-check after acquiring write lock (avoid race)
	entry, exists = crl.ipLimiters[ip]
	if exists {
		entry.lastAccess = time.Now()
		return entry.limiter
	}

	// Create new limiter
	limiter := rate.NewLimiter(rate.Limit(crl.ipRate), crl.ipBurst)
	crl.ipLimiters[ip] = &ipLimiterEntry{
		limiter:    limiter,
		lastAccess: time.Now(),
	}

	crl.logger.Debug().
		Str("ip", ip).
		Int("total_tracked_ips", len(crl.ipLimiters)).
		Msg("Created rate limiter for new IP")

	return limiter
}

// cleanupLoop periodically removes stale IP rate limiters to prevent memory leak.
// Runs every minute and removes IPs that haven't been accessed in IPTTL duration.
func (crl *ConnectionRateLimiter) cleanupLoop() {
	for {
		select {
		case <-crl.cleanupTicker.C:
			crl.cleanup()
		case <-crl.stopCleanup:
			crl.cleanupTicker.Stop()
			crl.logger.Info().Msg("Cleanup loop stopped")
			return
		}
	}
}

// cleanup removes stale IP rate limiters that haven't been accessed recently.
func (crl *ConnectionRateLimiter) cleanup() {
	crl.ipMu.Lock()
	defer crl.ipMu.Unlock()

	now := time.Now()
	removed := 0

	for ip, entry := range crl.ipLimiters {
		if now.Sub(entry.lastAccess) > crl.ipTTL {
			delete(crl.ipLimiters, ip)
			removed++
		}
	}

	if removed > 0 {
		crl.logger.Debug().
			Int("removed", removed).
			Int("remaining", len(crl.ipLimiters)).
			Msg("Cleaned up stale IP rate limiters")
	}
}

// Stop gracefully stops the connection rate limiter cleanup goroutine.
// Should be called during application shutdown.
func (crl *ConnectionRateLimiter) Stop() {
	crl.logger.Info().Msg("Stopping ConnectionRateLimiter")
	close(crl.stopCleanup)
	crl.wg.Wait()
}

// GetStats returns current rate limiter statistics for monitoring/debugging.
func (crl *ConnectionRateLimiter) GetStats() map[string]any {
	crl.ipMu.RLock()
	trackedIPs := len(crl.ipLimiters)
	crl.ipMu.RUnlock()

	return map[string]any{
		"tracked_ips":  trackedIPs,
		"ip_burst":     crl.ipBurst,
		"ip_rate":      crl.ipRate,
		"ip_ttl":       crl.ipTTL.String(),
		"global_burst": crl.globalBurst,
		"global_rate":  crl.globalRate,
	}
}
