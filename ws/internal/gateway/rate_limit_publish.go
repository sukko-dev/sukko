package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/shared/logging"

	"github.com/rs/zerolog"
)

// Cleanup configuration for publish rate limiters.
const (
	publishLimiterCleanupInterval = 5 * time.Minute
	publishLimiterMaxAge          = 10 * time.Minute
)

var publishRateLimitersActive = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "gateway_publish_rate_limiters_active",
	Help: "Current number of active publish rate limiter entries (tenant + IP)",
})

// limiterEntry holds a rate limiter with last-used timestamp for cleanup.
type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed atomic.Int64 // Unix seconds
}

// PublishRateLimiter provides per-tenant AND per-IP rate limiting for REST publish.
// Both limits must pass for a request to be allowed (Constitution IX).
//
// Memory management: background cleanup goroutine removes entries not used
// for publishLimiterMaxAge (10 min). Goroutine lifecycle per Constitution VII.
type PublishRateLimiter struct {
	tenantLimiters sync.Map // tenantID → *limiterEntry
	ipLimiters     sync.Map // IP → *limiterEntry
	rateLimit      rate.Limit
	burst          int
	logger         zerolog.Logger
}

// NewPublishRateLimiter creates a rate limiter for REST publish requests.
// Starts a background cleanup goroutine that exits when ctx is canceled.
func NewPublishRateLimiter(ctx context.Context, wg *sync.WaitGroup, ratePerSec float64, burst int, logger zerolog.Logger) *PublishRateLimiter {
	pl := &PublishRateLimiter{
		rateLimit: rate.Limit(ratePerSec),
		burst:     burst,
		logger:    logger.With().Str("component", "publish_rate_limiter").Logger(),
	}

	wg.Go(func() {
		defer logging.RecoverPanic(logger, "publish_rate_limiter_cleanup", nil)
		pl.cleanupLoop(ctx)
	})

	return pl
}

// RetryAfter reports how long a rejected caller should wait: the time this limiter takes
// to refill one token at its configured rate. It is derived from the limiter's OWN rate —
// the one that actually rejected the request — so the advertised wait cannot drift from the
// enforced limit. A non-positive rate (only reachable in tests; the config layer validates
// GATEWAY_PUBLISH_RATE_LIMIT > 0) falls back to one second rather than dividing by zero.
func (pl *PublishRateLimiter) RetryAfter() time.Duration {
	if pl.rateLimit <= 0 {
		return time.Second
	}
	return time.Duration(float64(time.Second) / float64(pl.rateLimit))
}

// Allow checks both per-tenant and per-IP rate limits.
// Returns true if the request is allowed, false if rate limited.
func (pl *PublishRateLimiter) Allow(tenantID, ip string) bool {
	now := time.Now().Unix()

	// Per-tenant check
	if !pl.allowKey(&pl.tenantLimiters, tenantID, now) {
		return false
	}

	// Per-IP check
	if !pl.allowKey(&pl.ipLimiters, ip, now) {
		return false
	}

	return true
}

// allowKey checks the rate limit for a specific key in the given map.
func (pl *PublishRateLimiter) allowKey(m *sync.Map, key string, nowUnix int64) bool {
	val, loaded := m.LoadOrStore(key, &limiterEntry{
		limiter: rate.NewLimiter(pl.rateLimit, pl.burst),
	})
	entry, ok := val.(*limiterEntry)
	if !ok {
		return false
	}
	entry.lastUsed.Store(nowUnix)

	if !loaded {
		// New entry — update gauge
		publishRateLimitersActive.Inc()
	}

	return entry.limiter.Allow()
}

// cleanupLoop periodically removes stale rate limiter entries.
func (pl *PublishRateLimiter) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(publishLimiterCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pl.cleanup()
		}
	}
}

// cleanup removes entries older than publishLimiterMaxAge.
func (pl *PublishRateLimiter) cleanup() {
	cutoff := time.Now().Add(-publishLimiterMaxAge).Unix()
	var removed int

	cleanMap := func(m *sync.Map) {
		m.Range(func(key, value any) bool {
			entry, ok := value.(*limiterEntry)
			if !ok {
				return true
			}
			if entry.lastUsed.Load() < cutoff {
				m.Delete(key)
				removed++
			}
			return true
		})
	}

	cleanMap(&pl.tenantLimiters)
	cleanMap(&pl.ipLimiters)

	if removed > 0 {
		publishRateLimitersActive.Sub(float64(removed))
		pl.logger.Debug().
			Int("removed", removed).
			Msg("Cleaned up stale publish rate limiter entries")
	}
}
