package worker

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
)

// GRPCProvisioningClient adapts the generated WebhookWorkerServiceClient to the
// worker.ProvisioningClient interface. The generated client uses variadic grpc.CallOption
// args which are incompatible with the interface directly.
type GRPCProvisioningClient struct {
	client provisioningv1.WebhookWorkerServiceClient
}

// NewGRPCProvisioningClient wraps a generated WebhookWorkerServiceClient.
func NewGRPCProvisioningClient(conn grpc.ClientConnInterface) *GRPCProvisioningClient {
	return &GRPCProvisioningClient{
		client: provisioningv1.NewWebhookWorkerServiceClient(conn),
	}
}

// ListWebhookTenants implements ProvisioningClient.
func (c *GRPCProvisioningClient) ListWebhookTenants(ctx context.Context) ([]string, error) {
	resp, err := c.client.ListWebhookTenants(ctx, &provisioningv1.ListWebhookTenantsRequest{})
	if err != nil {
		return nil, fmt.Errorf("ListWebhookTenants: %w", err)
	}
	return resp.GetTenantUuids(), nil
}

// ListWebhooksForTenant implements ProvisioningClient.
func (c *GRPCProvisioningClient) ListWebhooksForTenant(ctx context.Context, tenantID string) ([]*provisioning.WebhookRecord, error) {
	resp, err := c.client.ListWebhooksForTenant(ctx, &provisioningv1.ListWebhooksForTenantRequest{TenantUuid: tenantID})
	if err != nil {
		return nil, fmt.Errorf("ListWebhooksForTenant(%s): %w", tenantID, err)
	}
	records := make([]*provisioning.WebhookRecord, len(resp.GetWebhooks()))
	for i, w := range resp.GetWebhooks() {
		rec := &provisioning.WebhookRecord{
			ID:             w.GetId(),
			TenantID:       w.GetTenantUuid(),
			URL:            w.GetUrl(),
			ChannelPattern: w.GetChannelPattern(),
			SecretEnc:      w.GetSecretEnc(),
			Status:         w.GetStatus(),
			MaxRetries:     int(w.GetMaxRetries()),
		}
		// last_delivery_at_ms == 0 → nil; non-zero → *time.Time.
		if ms := w.GetLastDeliveryAtMs(); ms != 0 {
			t := time.UnixMilli(ms).UTC()
			rec.LastDeliveryAt = &t
		}
		records[i] = rec
	}
	return records, nil
}

// UpdateWebhookStatus implements ProvisioningClient.
func (c *GRPCProvisioningClient) UpdateWebhookStatus(ctx context.Context, id, tenantID, status string, retryCount int) error {
	_, err := c.client.UpdateWebhookStatus(ctx, &provisioningv1.UpdateWebhookStatusRequest{
		WebhookId:  id,
		TenantUuid: tenantID,
		Status:     status,
		RetryCount: int32(retryCount), //nolint:gosec // retryCount is always small (0–10)
	})
	if err != nil {
		return fmt.Errorf("UpdateWebhookStatus(%s, %s): %w", id, status, err)
	}
	return nil
}

// RecordDelivery implements ProvisioningClient.
func (c *GRPCProvisioningClient) RecordDelivery(ctx context.Context, d *provisioning.WebhookDelivery) error {
	_, err := c.client.RecordDelivery(ctx, &provisioningv1.RecordDeliveryRequest{
		WebhookId:     d.WebhookID,
		TenantUuid:    d.TenantID,
		DeliveryId:    d.ID,
		Attempt:       int32(d.Attempt),    //nolint:gosec // attempt is always small (1–10)
		StatusCode:    int32(d.StatusCode), //nolint:gosec // HTTP status codes are always in [0,599]
		LatencyMs:     d.LatencyMS,
		Error:         d.Error,
		DeliveredAtMs: d.DeliveredAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("RecordDelivery(%s): %w", d.ID, err)
	}
	return nil
}
