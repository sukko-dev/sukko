package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"

	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
)

// apnsCredentials holds APNs authentication key data parsed from tenant credentials JSON.
type apnsCredentials struct {
	AuthKey  string `json:"authKey"`  // Base64-encoded .p8 private key
	KeyID    string `json:"keyID"`    // 10-character key identifier
	TeamID   string `json:"teamID"`   // 10-character Apple team identifier
	BundleID string `json:"bundleID"` // App bundle identifier (e.g., "com.example.app")
}

// apnsClientEntry caches an APNs client alongside the bundle ID for a tenant.
type apnsClientEntry struct {
	client   *apns2.Client
	bundleID string
}

// APNsProvider sends push notifications via Apple Push Notification service.
type APNsProvider struct {
	logger     zerolog.Logger
	lookupCred CredentialLookup

	// mu protects clients map. Read-heavy (one write per new tenant, reads on
	// every send), so RWMutex is appropriate per Constitution VII.
	mu      sync.RWMutex
	clients map[string]*apnsClientEntry
}

// NewAPNsProvider creates an APNs provider that resolves authentication keys
// per tenant via the supplied credential lookup function.
func NewAPNsProvider(logger zerolog.Logger, lookupCred CredentialLookup) (*APNsProvider, error) {
	if lookupCred == nil {
		return nil, errors.New("creating apns provider: credential lookup function is required")
	}
	return &APNsProvider{
		logger:     logger.With().Str("provider", "apns").Logger(),
		lookupCred: lookupCred,
		clients:    make(map[string]*apnsClientEntry),
	}, nil
}

// Name returns "apns".
func (p *APNsProvider) Name() string { return "apns" }

// InvalidateClient removes the cached apns2.Client for a tenant.
// Called when APNs credentials are rotated via the provisioning API.
func (p *APNsProvider) InvalidateClient(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.clients, tenantID)
	p.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("invalidated cached APNs client")
}

// Close is a no-op for APNs. The apns2 client uses HTTP/2 persistent connections
// that are managed by the underlying transport.
func (p *APNsProvider) Close() error { return nil }

// Send delivers a single APNs notification.
func (p *APNsProvider) Send(ctx context.Context, job PushJob) error {
	entry, err := p.clientEntry(job.TenantID)
	if err != nil {
		return err
	}

	notification := &apns2.Notification{
		DeviceToken: job.Token,
		Topic:       entry.bundleID,
		Payload: payload.NewPayload().
			AlertTitle(job.Title).
			AlertBody(job.Body).
			MutableContent().
			Custom("url", job.URL),
	}

	if job.TTL > 0 {
		notification.PushType = apns2.PushTypeAlert
	}

	resp, err := entry.client.PushWithContext(ctx, notification)
	if err != nil {
		return fmt.Errorf("apns: sending notification for tenant %s principal %s: %w",
			job.TenantID, job.Principal, err)
	}

	if resp.StatusCode == http.StatusGone || resp.Reason == apns2.ReasonUnregistered {
		return fmt.Errorf("apns: token unregistered for tenant %s principal %s: %w",
			job.TenantID, job.Principal, ErrSubscriptionExpired)
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return fmt.Errorf("apns: rate limited for tenant %s: %w",
				job.TenantID, ErrRateLimited)
		}
		return fmt.Errorf("apns: unexpected status %d reason %s for tenant %s principal %s",
			resp.StatusCode, resp.Reason, job.TenantID, job.Principal)
	}

	p.logger.Debug().
		Str(logging.LogKeyTenantSlug, job.TenantID).
		Str("principal", job.Principal).
		Str("token", truncateToken(job.Token)).
		Msg("APNs notification sent")
	return nil
}

// SendBatch sends notifications sequentially. APNs multiplexes requests over a
// single HTTP/2 connection per tenant, so sequential sends are efficient.
func (p *APNsProvider) SendBatch(ctx context.Context, jobs []PushJob) error {
	var errs []error
	for i := range jobs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("apns: batch canceled: %w", err)
		}
		if err := p.Send(ctx, jobs[i]); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("apns: batch had %d failures: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// clientEntry returns a cached APNs client for the tenant, creating one from
// .p8 key credentials on first access.
func (p *APNsProvider) clientEntry(tenantID string) (*apnsClientEntry, error) {
	// Fast path: read lock.
	p.mu.RLock()
	entry, ok := p.clients[tenantID]
	p.mu.RUnlock()

	if ok {
		return entry, nil
	}

	// Slow path: create client under write lock.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock.
	if entry, ok = p.clients[tenantID]; ok {
		return entry, nil
	}

	raw, err := p.lookupCred(tenantID)
	if err != nil {
		return nil, fmt.Errorf("apns: looking up credentials for tenant %s: %w", tenantID, err)
	}
	if raw == nil {
		return nil, fmt.Errorf("apns: no APNs credentials configured for tenant %s", tenantID)
	}

	var creds apnsCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return nil, fmt.Errorf("apns: parsing credentials for tenant %s: %w", tenantID, err)
	}

	if creds.AuthKey == "" || creds.KeyID == "" || creds.TeamID == "" || creds.BundleID == "" {
		return nil, fmt.Errorf("apns: incomplete credentials for tenant %s (need authKey, keyID, teamID, bundleID)", tenantID)
	}

	authKey, err := token.AuthKeyFromBytes([]byte(creds.AuthKey))
	if err != nil {
		return nil, fmt.Errorf("apns: parsing auth key for tenant %s: %w", tenantID, err)
	}

	authToken := &token.Token{
		AuthKey: authKey,
		KeyID:   creds.KeyID,
		TeamID:  creds.TeamID,
	}

	client := apns2.NewTokenClient(authToken).Production()

	entry = &apnsClientEntry{
		client:   client,
		bundleID: creds.BundleID,
	}
	p.clients[tenantID] = entry

	return entry, nil
}
