package provisioning

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// invalidationPublishTimeout bounds the Valkey PUBLISH call so a TCP-stalled connection
// never blocks the webhook write path (CreateWebhook, UpdateWebhook, DeleteWebhook).
const invalidationPublishTimeout = 500 * time.Millisecond

// WebhookCacheInvalidator publishes a cache invalidation signal for a tenant.
// Called after webhook create/update/delete/status-change so the webhook-worker
// refreshes its in-memory cache within ~1s instead of waiting for the TTL tick.
// Implemented by InvalidationPublisher; nil-safe in ServiceConfig (Community editions).
type WebhookCacheInvalidator interface {
	Publish(tenantID string)
}

// InvalidationPublisher implements WebhookCacheInvalidator using Valkey PUBLISH.
// Must use valkey.Client.Do(PUBLISH) — NOT the in-process eventbus.Bus, which
// does NOT cross pod boundaries (workers on other pods would never receive it).
type InvalidationPublisher struct {
	client valkey.Client
	logger zerolog.Logger
}

// NewInvalidationPublisher creates an InvalidationPublisher backed by the given Valkey client.
func NewInvalidationPublisher(client valkey.Client, logger zerolog.Logger) *InvalidationPublisher {
	return &InvalidationPublisher{
		client: client,
		logger: logger.With().Str("component", "invalidation_publisher").Logger(),
	}
}

// Publish sends a cache invalidation signal for tenantID on the WebhookInvalidationSubjectPrefix channel.
// Best-effort: errors are logged and discarded so a Valkey outage never blocks webhook write operations.
// A 500ms timeout prevents a TCP-stalled Valkey connection from blocking the calling write path indefinitely.
func (p *InvalidationPublisher) Publish(tenantID string) {
	if p == nil || p.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), invalidationPublishTimeout)
	defer cancel()
	subject := WebhookInvalidationSubjectPrefix + tenantID
	cmd := p.client.B().Publish().Channel(subject).Message("1").Build()
	if err := p.client.Do(ctx, cmd).Error(); err != nil {
		p.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).
			Msg("webhook cache invalidation publish failed (worker will rely on TTL refresh)")
	}
}
