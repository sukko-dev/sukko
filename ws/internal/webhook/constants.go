package webhook

import "time"

// WebhookRetrySchedule is the fixed backoff schedule for retriable delivery failures.
// Index 0 = delay before 1st retry, index 4 = delay before 5th retry.
// For max_retries > 5, retries beyond index 4 reuse the last entry (10m).
// §IV domain exception (docs/engineering-principles.md): application-layer webhook
// delivery uses a fixed schedule (not exponential) — matching industry standard
// (Stripe, GitHub, Pusher, Ably). Exported for test injection.
var WebhookRetrySchedule = [5]time.Duration{
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

// RetryDelay returns the backoff duration for the given retry attempt number (1-indexed).
// attempt=1 → WebhookRetrySchedule[0], attempt≥6 → WebhookRetrySchedule[4].
// Bounds-checked to prevent index-out-of-range on any max_retries value.
func RetryDelay(attempt int) time.Duration {
	idx := max(attempt-1, 0)
	if idx >= len(WebhookRetrySchedule) {
		idx = len(WebhookRetrySchedule) - 1
	}
	return WebhookRetrySchedule[idx]
}

const (
	// WebhookDegradedRetryInterval is the delay between delivery attempts
	// once a webhook has exhausted all retries and entered degraded status.
	// Retries continue indefinitely at this interval until operator intervention.
	WebhookDegradedRetryInterval = time.Hour

	// WebhookDegradedInitialRetryDelay is the delay before the first degraded retry
	// for a webhook that has no recorded LastDeliveryAt (e.g. never successfully
	// delivered or restarted before recording). §I: magic number must be named.
	WebhookDegradedInitialRetryDelay = time.Second

	// WebhookDegradedQueueFullFallback is the reschedule delay when the degradedCh
	// buffer is full and a new degraded timer cannot be enqueued immediately.
	// The timer is rescheduled for this duration rather than dropped permanently.
	// §I: magic number must be named.
	WebhookDegradedQueueFullFallback = time.Minute

	// WebhookTestDeliverDeadline is the fixed gRPC call deadline provisioning uses when
	// calling the worker's TestDeliver RPC. Derivation: WEBHOOK_DELIVERY_TIMEOUT max (30s)
	// + 500ms buffer so provisioning does not time out before the worker can respond.
	// §I: magic numbers must be named constants.
	WebhookTestDeliverDeadline = 30*time.Second + 500*time.Millisecond

	// WebhookTestResponsePreviewBytes is the maximum response body bytes
	// returned in the POST /webhooks/{id}/test API response preview.
	WebhookTestResponsePreviewBytes = 512

	// WebhookDowngradeGracePeriod is the time after a license downgrade
	// before all webhooks are suspended. This is a product policy constant —
	// not an operator-configurable env var (Constitution §XV KISS).
	WebhookDowngradeGracePeriod = 24 * time.Hour
)

const (
	// WebhookSignatureHeader is the HTTP header name carrying the HMAC-SHA256 delivery signature.
	// Used by the worker (sender) and tester receiver — single source of truth (§X).
	WebhookSignatureHeader = "X-Sukko-Signature"

	// WebhookSignaturePrefix is the prefix prepended to the hex-encoded HMAC-SHA256 digest
	// in the signature header value (e.g. "sha256=abc123...").
	WebhookSignaturePrefix = "sha256="
)

// Rate-limit constants for POST /webhooks/{id}/test endpoint.
// Exported so the test handler in provisioning/api can import them from this package (§X).
const (
	WebhookTestRateLimitPerWebhook = 10
	WebhookTestRateLimitPerTenant  = 30
	WebhookTestRateLimitWindow     = 60 * time.Second
	WebhookTestRLKeyPrefix         = "webhook_test_rl:"
	WebhookTestRLTenantKeyPrefix   = "webhook_test_rl_tenant:"
)
