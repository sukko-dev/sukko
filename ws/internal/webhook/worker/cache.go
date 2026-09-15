package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// WebhookRecord is the worker-side in-memory representation of a cached webhook.
// Distinct from provisioning.WebhookRecord — same data, local package.
type WebhookRecord struct {
	ID             string
	TenantID       string
	URL            string
	ChannelPattern string
	SecretEnc      []byte // raw AES-256-GCM ciphertext; NOT base64
	Status         string
	MaxRetries     int
	LastDeliveryAt *time.Time // nil = no prior delivery (proto 0 → nil, never time.Unix(0,0))
}

// WebhookCache is an in-memory map of tenant_id → []WebhookRecord.
// All per-tenant refreshes are coalesced via singleflight.Group to prevent
// concurrent gRPC stampedes on the same tenant.
type WebhookCache struct {
	mu      sync.RWMutex
	records map[string][]WebhookRecord
	group   singleflight.Group
	client  ProvisioningClient
	logger  zerolog.Logger
}

// NewWebhookCache creates a WebhookCache backed by the given ProvisioningClient.
func NewWebhookCache(client ProvisioningClient, logger zerolog.Logger) *WebhookCache {
	return &WebhookCache{
		records: make(map[string][]WebhookRecord),
		client:  client,
		logger:  logger.With().Str("component", "webhook_cache").Logger(),
	}
}

// Get returns the cached webhooks for a tenant. Returns nil if the tenant has no entry.
func (c *WebhookCache) Get(tenantID string) []WebhookRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.records[tenantID]
}

// GetByID finds a single webhook by ID within a tenant's cached records. Returns nil if absent.
func (c *WebhookCache) GetByID(tenantID, webhookID string) *WebhookRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i := range c.records[tenantID] {
		if c.records[tenantID][i].ID == webhookID {
			return &c.records[tenantID][i]
		}
	}
	return nil
}

// TenantIDs returns all tenant IDs with a cache entry.
func (c *WebhookCache) TenantIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.records))
	for k := range c.records {
		ids = append(ids, k)
	}
	return ids
}

// Refresh fetches the latest webhook registrations for tenantID from the provisioning
// gRPC API and updates the cache. Concurrent calls for the same tenant are coalesced
// into a single RPC via singleflight.
func (c *WebhookCache) Refresh(ctx context.Context, tenantID string) error {
	_, err, _ := c.group.Do(tenantID, func() (any, error) {
		return nil, c.fetchAndStore(ctx, tenantID)
	})
	if err != nil {
		return fmt.Errorf("refresh tenant %s: %w", tenantID, err)
	}
	return nil
}

// RefreshAll re-hydrates every currently-cached tenant (TTL ticker target).
// Errors per tenant are logged and skipped so one failing tenant doesn't block others.
func (c *WebhookCache) RefreshAll(ctx context.Context) error {
	for _, tid := range c.TenantIDs() {
		if err := c.Refresh(ctx, tid); err != nil {
			c.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tid).Msg("cache TTL refresh failed for tenant")
		}
	}
	return nil
}

// Hydrate performs the initial full hydration on startup.
// Returns a slice of degraded WebhookRecords so the degradedScheduler can schedule timers.
func (c *WebhookCache) Hydrate(ctx context.Context) ([]WebhookRecord, error) {
	tenantIDs, err := c.client.ListWebhookTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("list webhook tenants: %w", err)
	}
	var degraded []WebhookRecord
	for _, tid := range tenantIDs {
		if err := c.fetchAndStore(ctx, tid); err != nil {
			c.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tid).Msg("hydration failed for tenant; skipping")
			continue
		}
		for _, r := range c.Get(tid) {
			if r.Status == types.WebhookStatusDegraded {
				degraded = append(degraded, r)
			}
		}
	}
	return degraded, nil
}

// fetchAndStore calls ListWebhooksForTenant and writes the result under the write lock.
func (c *WebhookCache) fetchAndStore(ctx context.Context, tenantID string) error {
	recs, err := c.client.ListWebhooksForTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("list webhooks for tenant %s: %w", tenantID, err)
	}
	local := make([]WebhookRecord, len(recs))
	for i, r := range recs {
		local[i] = fromProvisioning(r)
	}
	c.mu.Lock()
	c.records[tenantID] = local
	size := len(c.records)
	c.mu.Unlock()
	cacheSizeGauge.Set(float64(size))
	return nil
}

// fromProvisioning converts a provisioning.WebhookRecord to the worker's local type.
// last_delivery_at_ms == 0 maps to nil (never time.Unix(0,0)).
func fromProvisioning(r *provisioning.WebhookRecord) WebhookRecord {
	return WebhookRecord{
		ID:             r.ID,
		TenantID:       r.TenantID,
		URL:            r.URL,
		ChannelPattern: r.ChannelPattern,
		SecretEnc:      r.SecretEnc,
		Status:         r.Status,
		MaxRetries:     r.MaxRetries,
		LastDeliveryAt: r.LastDeliveryAt, // already *time.Time; nil if none
	}
}
