// Package provider defines the push notification provider interface and
// concrete implementations for Web Push, FCM, APNs, and dry-run logging.
package provider

import (
	"context"
	"encoding/json"
	"errors"
)

// Provider sends push notifications to a specific platform.
type Provider interface {
	// Send delivers a single push notification.
	Send(ctx context.Context, job PushJob) error

	// SendBatch delivers multiple push notifications. Implementations may
	// use platform-specific batch APIs where available.
	SendBatch(ctx context.Context, jobs []PushJob) error

	// InvalidateClient removes cached client state for a tenant.
	// Called when provider credentials are rotated so the next Send
	// re-creates the client from fresh credentials.
	InvalidateClient(tenantID string)

	// Name returns the provider name (e.g., "webpush", "fcm", "apns", "dryrun").
	Name() string

	// Close releases any resources held by the provider.
	Close() error
}

// CredentialLookup retrieves provider credentials for a tenant.
// The returned JSON structure depends on the provider:
//   - Web Push: {"vapidPublicKey": "...", "vapidPrivateKey": "...", "vapidContact": "..."}
//   - FCM: Firebase service account JSON
//   - APNs: {"authKey": "base64...", "keyID": "...", "teamID": "...", "bundleID": "..."}
type CredentialLookup func(tenantID string) (json.RawMessage, error)

// PushJob contains all data needed to send one push notification.
type PushJob struct {
	TenantID   string
	Principal  string
	Platform   string // "web", "android", "ios"
	Token      string // FCM/APNs token (empty for web)
	Endpoint   string // Web Push endpoint URL (empty for android/ios)
	P256dhKey  string // Web Push ECDH public key
	AuthSecret string // Web Push auth secret
	Title      string
	Body       string
	Icon       string
	URL        string
	TTL        int
	Urgency    string
	Channel    string // source channel (for logging/metrics)
}

// Sentinel errors for push notification delivery failures.
var (
	// ErrSubscriptionExpired indicates the device token or push subscription is
	// no longer valid (HTTP 410 for Web Push, UNREGISTERED for FCM, etc.).
	// Callers should remove the subscription from storage.
	ErrSubscriptionExpired = errors.New("subscription expired or invalid")

	// ErrRateLimited indicates the push provider rejected the request due to
	// rate limiting (HTTP 429). Callers should retry with backoff.
	ErrRateLimited = errors.New("provider rate limited")
)
