package api

import (
	"context"
	"time"
)

// RateLimiter enforces per-key rate limits using a backend store (Valkey).
// Used by WebhookTestHandler to limit test deliveries per webhook and per tenant.
// Placed here (not in ws/internal/webhook/) because only provisioning/api uses it
// — the webhook-worker runner does not rate-limit deliveries (§X: shared code only
// when used by multiple packages).
type RateLimiter interface {
	// Allow checks whether the given key is within its rate limit window.
	// Returns (true, 0, nil) when allowed, (false, retryAfter, nil) when the limit is
	// exceeded, or (false, 0, err) when the backing store is unavailable (fail-open:
	// the handler MUST allow the request and log at Warn when err is non-nil).
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}
