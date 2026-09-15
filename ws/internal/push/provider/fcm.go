package provider

import (
	"context"
	"errors"
	"fmt"
	"sync"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// fcmMaxBatchSize is the maximum number of tokens per FCM SendEachForMulticast call.
const fcmMaxBatchSize = 500

// FCMProvider sends push notifications via Firebase Cloud Messaging.
type FCMProvider struct {
	logger     zerolog.Logger
	lookupCred CredentialLookup

	// mu protects apps map. Read-heavy (one write per new tenant, reads on
	// every send), so RWMutex is appropriate per Constitution VII.
	mu   sync.RWMutex
	apps map[string]*firebase.App
}

// NewFCMProvider creates an FCM provider that resolves service account
// credentials per tenant via the supplied credential lookup function.
func NewFCMProvider(logger zerolog.Logger, lookupCred CredentialLookup) (*FCMProvider, error) {
	if lookupCred == nil {
		return nil, errors.New("creating fcm provider: credential lookup function is required")
	}
	return &FCMProvider{
		logger:     logger.With().Str("provider", "fcm").Logger(),
		lookupCred: lookupCred,
		apps:       make(map[string]*firebase.App),
	}, nil
}

// Name returns "fcm".
func (p *FCMProvider) Name() string { return "fcm" }

// InvalidateClient removes the cached firebase.App for a tenant.
// Called when FCM credentials are rotated via the provisioning API.
func (p *FCMProvider) InvalidateClient(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.apps, tenantID)
	p.logger.Info().Str(logging.LogKeyTenantSlug, tenantID).Msg("invalidated cached FCM client")
}

// Close is a no-op for FCM (firebase.App has no Close method).
func (p *FCMProvider) Close() error { return nil }

// Send delivers a single FCM notification.
func (p *FCMProvider) Send(ctx context.Context, job PushJob) error {
	client, err := p.messagingClient(ctx, job.TenantID)
	if err != nil {
		return err
	}

	msg := &messaging.Message{
		Token: job.Token,
		Notification: &messaging.Notification{
			Title:    job.Title,
			Body:     job.Body,
			ImageURL: job.Icon,
		},
		Webpush: nil, // FCM provider handles native Android/iOS; web uses WebPushProvider
	}

	if job.URL != "" {
		msg.Data = map[string]string{"url": job.URL}
	}

	_, err = client.Send(ctx, msg)
	if err != nil {
		if messaging.IsUnregistered(err) {
			return fmt.Errorf("fcm: token unregistered for tenant %s principal %s: %w",
				job.TenantID, job.Principal, ErrSubscriptionExpired)
		}
		return fmt.Errorf("fcm: sending notification for tenant %s principal %s: %w",
			job.TenantID, job.Principal, err)
	}

	p.logger.Debug().
		Str(logging.LogKeyTenantSlug, job.TenantID).
		Str("principal", job.Principal).
		Str("token", truncateToken(job.Token)).
		Msg("FCM notification sent")
	return nil
}

// SendBatch groups jobs by tenant and uses SendEachForMulticast for efficient
// batch delivery (up to 500 tokens per call).
func (p *FCMProvider) SendBatch(ctx context.Context, jobs []PushJob) error {
	// Group jobs by tenant for client reuse.
	byTenant := make(map[string][]PushJob)
	for i := range jobs {
		byTenant[jobs[i].TenantID] = append(byTenant[jobs[i].TenantID], jobs[i])
	}

	var errs []error
	for tenantID, tenantJobs := range byTenant {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("fcm: batch canceled: %w", err)
		}

		client, err := p.messagingClient(ctx, tenantID)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if err := p.sendMulticast(ctx, client, tenantID, tenantJobs); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("fcm: batch had %d failures: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// sendMulticast sends notifications to a batch of tokens for a single tenant,
// splitting into chunks of fcmMaxBatchSize.
func (p *FCMProvider) sendMulticast(ctx context.Context, client *messaging.Client, tenantID string, jobs []PushJob) error {
	var errs []error

	for i := 0; i < len(jobs); i += fcmMaxBatchSize {
		end := min(i+fcmMaxBatchSize, len(jobs))
		chunk := jobs[i:end]

		tokens := make([]string, len(chunk))
		for j := range chunk {
			tokens[j] = chunk[j].Token
		}

		// Use the first job's content for the multicast (batch assumes uniform content).
		first := chunk[0]
		msg := &messaging.MulticastMessage{
			Tokens: tokens,
			Notification: &messaging.Notification{
				Title:    first.Title,
				Body:     first.Body,
				ImageURL: first.Icon,
			},
		}
		if first.URL != "" {
			msg.Data = map[string]string{"url": first.URL}
		}

		resp, err := client.SendEachForMulticast(ctx, msg)
		if err != nil {
			errs = append(errs, fmt.Errorf("fcm: multicast for tenant %s: %w", tenantID, err))
			continue
		}

		// Check individual send results for unregistered tokens.
		for j, sendResp := range resp.Responses {
			if sendResp.Error != nil {
				if messaging.IsUnregistered(sendResp.Error) {
					errs = append(errs, fmt.Errorf("fcm: token unregistered for tenant %s principal %s: %w",
						tenantID, chunk[j].Principal, ErrSubscriptionExpired))
				} else {
					errs = append(errs, fmt.Errorf("fcm: send failed for tenant %s principal %s: %w",
						tenantID, chunk[j].Principal, sendResp.Error))
				}
			}
		}

		p.logger.Debug().
			Str(logging.LogKeyTenantSlug, tenantID).
			Int("success", resp.SuccessCount).
			Int("failure", resp.FailureCount).
			Msg("FCM multicast sent")
	}

	if len(errs) > 0 {
		return fmt.Errorf("fcm: multicast batch had %d failures: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// messagingClient returns a cached FCM messaging client for the tenant,
// creating one from service account credentials on first access.
func (p *FCMProvider) messagingClient(ctx context.Context, tenantID string) (*messaging.Client, error) {
	// Fast path: read lock.
	p.mu.RLock()
	app, ok := p.apps[tenantID]
	p.mu.RUnlock()

	if !ok {
		// Slow path: create app under write lock.
		p.mu.Lock()
		// Double-check after acquiring write lock.
		app, ok = p.apps[tenantID]
		if !ok {
			creds, err := p.lookupCred(tenantID)
			if err != nil {
				p.mu.Unlock()
				return nil, fmt.Errorf("fcm: looking up credentials for tenant %s: %w", tenantID, err)
			}
			if creds == nil {
				p.mu.Unlock()
				return nil, fmt.Errorf("fcm: no FCM credentials configured for tenant %s", tenantID)
			}

			newApp, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(creds))
			if err != nil {
				p.mu.Unlock()
				return nil, fmt.Errorf("fcm: creating firebase app for tenant %s: %w", tenantID, err)
			}
			p.apps[tenantID] = newApp
			app = newApp
		}
		p.mu.Unlock()
	}

	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("fcm: creating messaging client for tenant %s: %w", tenantID, err)
	}
	return client, nil
}

// truncateToken returns the first 8 characters of a token for safe logging.
func truncateToken(token string) string {
	if len(token) <= 8 {
		return token
	}
	return token[:8] + "..."
}
