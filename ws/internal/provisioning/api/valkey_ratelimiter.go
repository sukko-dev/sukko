package api

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"
)

// ValkeyRateLimiter implements RateLimiter using a Valkey fixed-window counter.
// Each key gets a counter with INCR; the first INCR sets an EXPIRE for the window.
// When count exceeds limit, Allow returns (false, retryAfter, nil).
// When Valkey is unavailable, Allow returns (false, 0, err) — handler must fail open.
type ValkeyRateLimiter struct {
	client valkey.Client
	limit  int
	window time.Duration
	logger zerolog.Logger
}

// NewValkeyRateLimiter creates a ValkeyRateLimiter with the given limit per window duration.
func NewValkeyRateLimiter(client valkey.Client, limit int, window time.Duration, logger zerolog.Logger) *ValkeyRateLimiter {
	return &ValkeyRateLimiter{
		client: client,
		limit:  limit,
		window: window,
		logger: logger.With().Str("component", "valkey_rate_limiter").Logger(),
	}
}

// Allow checks whether the given key is within its rate limit window.
// Uses a fixed-window INCR pattern: INCR the counter, EXPIRE on first use.
// Returns (true, 0, nil) when allowed, (false, retryAfter, nil) when limited,
// (false, 0, err) when Valkey is unavailable (caller must fail open).
func (r *ValkeyRateLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	const luaScript = `
local current = redis.call('INCR', KEYS[1])
if tonumber(current) == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
`
	windowSecs := strconv.Itoa(int(r.window.Seconds()))
	// Callers pass fully-namespaced keys (e.g. "webhook_test_rl:<webhookID>") —
	// the limiter uses the key as-is without adding its own prefix.
	cmd := r.client.B().Eval().Script(luaScript).Numkeys(1).Key(key).Arg(windowSecs).Build()
	result := r.client.Do(ctx, cmd)
	if err := result.Error(); err != nil {
		return false, 0, fmt.Errorf("rate limiter valkey: %w", err)
	}
	count, err := result.AsInt64()
	if err != nil {
		return false, 0, fmt.Errorf("rate limiter parse: %w", err)
	}
	if int(count) > r.limit {
		// Approximate retry-after: remainder of window.
		return false, r.window, nil
	}
	return true, 0, nil
}
