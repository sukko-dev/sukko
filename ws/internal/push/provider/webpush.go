package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// vapidCredentials holds VAPID keys parsed from tenant credentials JSON.
type vapidCredentials struct {
	VAPIDPublicKey  string `json:"vapidPublicKey"`
	VAPIDPrivateKey string `json:"vapidPrivateKey"`
	VAPIDContact    string `json:"vapidContact"`
}

// WebPushProvider sends push notifications via the Web Push protocol (RFC 8030).
type WebPushProvider struct {
	logger     zerolog.Logger
	lookupCred CredentialLookup
}

// NewWebPushProvider creates a Web Push provider that resolves VAPID keys per
// tenant via the supplied credential lookup function.
func NewWebPushProvider(logger zerolog.Logger, lookupCred CredentialLookup) (*WebPushProvider, error) {
	if lookupCred == nil {
		return nil, errors.New("creating webpush provider: credential lookup function is required")
	}
	return &WebPushProvider{
		logger:     logger.With().Str("provider", "webpush").Logger(),
		lookupCred: lookupCred,
	}, nil
}

// Name returns "webpush".
func (p *WebPushProvider) Name() string { return "webpush" }

// InvalidateClient is a no-op for Web Push. VAPID credentials are looked up
// per-request via CredentialLookup — no cached clients to invalidate.
func (p *WebPushProvider) InvalidateClient(_ string) {}

// Close is a no-op for Web Push (no persistent connections).
func (p *WebPushProvider) Close() error { return nil }

// Send delivers a single Web Push notification.
func (p *WebPushProvider) Send(ctx context.Context, job PushJob) error {
	creds, err := p.resolveCredentials(job.TenantID)
	if err != nil {
		return err
	}

	// Build subscription from job fields.
	sub := &webpush.Subscription{
		Endpoint: job.Endpoint,
		Keys: webpush.Keys{
			P256dh: job.P256dhKey,
			Auth:   job.AuthSecret,
		},
	}

	// Build JSON payload matching the Push API showNotification format.
	payload, err := json.Marshal(map[string]string{
		"title": job.Title,
		"body":  job.Body,
		"icon":  job.Icon,
		"url":   job.URL,
	})
	if err != nil {
		return fmt.Errorf("webpush: marshaling payload: %w", err)
	}

	opts := &webpush.Options{
		Subscriber:      creds.VAPIDContact,
		VAPIDPublicKey:  creds.VAPIDPublicKey,
		VAPIDPrivateKey: creds.VAPIDPrivateKey,
		TTL:             job.TTL,
		Urgency:         webpush.Urgency(job.Urgency),
	}

	resp, err := webpush.SendNotificationWithContext(ctx, payload, sub, opts)
	if err != nil {
		return fmt.Errorf("webpush: sending notification: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		p.logger.Debug().
			Str(logging.LogKeyTenantSlug, job.TenantID).
			Str("principal", job.Principal).
			Str("endpoint", job.Endpoint).
			Msg("Web Push notification sent")
		return nil
	case http.StatusGone:
		return fmt.Errorf("webpush: endpoint gone for tenant %s principal %s: %w",
			job.TenantID, job.Principal, ErrSubscriptionExpired)
	case http.StatusTooManyRequests:
		return fmt.Errorf("webpush: rate limited for tenant %s: %w",
			job.TenantID, ErrRateLimited)
	default:
		return fmt.Errorf("webpush: unexpected status %d for tenant %s principal %s",
			resp.StatusCode, job.TenantID, job.Principal)
	}
}

// SendBatch sends notifications sequentially. Web Push has no batch API; each
// subscription requires an individual encrypted request.
func (p *WebPushProvider) SendBatch(ctx context.Context, jobs []PushJob) error {
	var errs []error
	for i := range jobs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("webpush: batch canceled: %w", err)
		}
		if err := p.Send(ctx, jobs[i]); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("webpush: batch had %d failures: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// resolveCredentials fetches and parses VAPID credentials for a tenant.
func (p *WebPushProvider) resolveCredentials(tenantID string) (*vapidCredentials, error) {
	raw, err := p.lookupCred(tenantID)
	if err != nil {
		return nil, fmt.Errorf("webpush: looking up credentials for tenant %s: %w", tenantID, err)
	}
	if raw == nil {
		return nil, fmt.Errorf("webpush: no VAPID credentials configured for tenant %s", tenantID)
	}

	var creds vapidCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return nil, fmt.Errorf("webpush: parsing credentials for tenant %s: %w", tenantID, err)
	}

	if creds.VAPIDPublicKey == "" || creds.VAPIDPrivateKey == "" {
		return nil, fmt.Errorf("webpush: missing VAPID keys for tenant %s", tenantID)
	}

	return &creds, nil
}
