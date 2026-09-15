package grpcserver

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
)

// stubWebhookStore is a minimal mock for testing WebhookWorkerServer.
type stubWebhookStore struct {
	provisioning.WebhookStore
	records []*provisioning.WebhookRecord
}

func (s *stubWebhookStore) ListByTenantForWorker(_ context.Context, _ string) ([]*provisioning.WebhookRecord, error) {
	return s.records, nil
}

func TestWebhookWorkerServer_ListWebhooksForTenant_LastDeliveryAtMapping(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 22, 12, 0, 0, 0, time.UTC)
	wantMs := ts.UnixMilli()

	tests := []struct {
		name           string
		lastDeliveryAt *time.Time
		wantMs         int64
	}{
		{
			name:           "nil LastDeliveryAt maps to 0",
			lastDeliveryAt: nil,
			wantMs:         0,
		},
		{
			name:           "non-nil LastDeliveryAt maps to UnixMilli",
			lastDeliveryAt: &ts,
			wantMs:         wantMs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &stubWebhookStore{
				records: []*provisioning.WebhookRecord{
					{
						ID:             "wh-1",
						TenantID:       "tenant-1",
						URL:            "https://example.com/hook",
						ChannelPattern: "trades.*",
						SecretEnc:      []byte("encryptedSecret"),
						Status:         "enabled",
						MaxRetries:     5,
						LastDeliveryAt: tt.lastDeliveryAt,
					},
				},
			}

			srv, err := NewWebhookWorkerServer(store, nil, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewWebhookWorkerServer() error = %v", err)
			}

			resp, err := srv.ListWebhooksForTenant(context.Background(),
				&provisioningv1.ListWebhooksForTenantRequest{TenantUuid: "tenant-1"})
			if err != nil {
				t.Fatalf("ListWebhooksForTenant() error = %v", err)
			}
			if len(resp.GetWebhooks()) != 1 {
				t.Fatalf("expected 1 webhook, got %d", len(resp.GetWebhooks()))
			}
			if got := resp.GetWebhooks()[0].GetLastDeliveryAtMs(); got != tt.wantMs {
				t.Errorf("LastDeliveryAtMs = %d, want %d", got, tt.wantMs)
			}
		})
	}
}
