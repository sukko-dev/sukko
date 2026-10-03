package grpcserver

import (
	"context"
	"errors"
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

// stubTenantSlugResolver returns a fixed slug (or error) for any UUID.
type stubTenantSlugResolver struct {
	slug string
	err  error
}

func (r stubTenantSlugResolver) GetSlugByUUID(_ context.Context, _ string) (string, error) {
	return r.slug, r.err
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

			srv, err := NewWebhookWorkerServer(store, stubTenantSlugResolver{slug: "tenant-1"}, nil, zerolog.Nop())
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

// TestWebhookWorkerServer_ListWebhooksForTenant_StampsTenantSlug verifies the response carries the
// tenant's data-path slug resolved from its UUID (ADR-0032) — the field the webhook-worker indexes
// its UUID-keyed cache by so it can match slug-stamped broadcast messages. A lookup error leaves
// the slug empty (worker skips the index write) but the call still succeeds.
func TestWebhookWorkerServer_ListWebhooksForTenant_StampsTenantSlug(t *testing.T) {
	t.Parallel()

	store := &stubWebhookStore{records: []*provisioning.WebhookRecord{{ID: "wh-1", TenantID: "uuid-1", Status: "enabled"}}}

	t.Run("slug resolved and stamped", func(t *testing.T) {
		t.Parallel()
		srv, err := NewWebhookWorkerServer(store, stubTenantSlugResolver{slug: "acme-prod"}, nil, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewWebhookWorkerServer() error = %v", err)
		}
		resp, err := srv.ListWebhooksForTenant(context.Background(),
			&provisioningv1.ListWebhooksForTenantRequest{TenantUuid: "uuid-1"})
		if err != nil {
			t.Fatalf("ListWebhooksForTenant() error = %v", err)
		}
		if got := resp.GetTenantSlug(); got != "acme-prod" {
			t.Errorf("TenantSlug = %q, want %q", got, "acme-prod")
		}
	})

	t.Run("tenant-not-found leaves slug empty but call succeeds (deletion race)", func(t *testing.T) {
		t.Parallel()
		srv, err := NewWebhookWorkerServer(store, stubTenantSlugResolver{err: provisioning.ErrTenantNotFound}, nil, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewWebhookWorkerServer() error = %v", err)
		}
		resp, err := srv.ListWebhooksForTenant(context.Background(),
			&provisioningv1.ListWebhooksForTenantRequest{TenantUuid: "uuid-1"})
		if err != nil {
			t.Fatalf("ListWebhooksForTenant() error = %v", err)
		}
		if got := resp.GetTenantSlug(); got != "" {
			t.Errorf("TenantSlug = %q, want empty on tenant-not-found", got)
		}
		if len(resp.GetWebhooks()) != 1 {
			t.Errorf("expected webhooks still returned on tenant-not-found, got %d", len(resp.GetWebhooks()))
		}
	})

	// A transient slug-lookup failure (DB deadline, pool failover) for a LIVE tenant must fail the
	// RPC rather than return success with an empty slug — otherwise the worker evicts a healthy slug
	// mapping and silently drops deliveries for up to a cache-TTL cycle (ADR-0032 / §IV).
	t.Run("transient lookup error fails the RPC (does not evict the worker mapping)", func(t *testing.T) {
		t.Parallel()
		srv, err := NewWebhookWorkerServer(store, stubTenantSlugResolver{err: errors.New("db deadline exceeded")}, nil, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewWebhookWorkerServer() error = %v", err)
		}
		if _, err := srv.ListWebhooksForTenant(context.Background(),
			&provisioningv1.ListWebhooksForTenantRequest{TenantUuid: "uuid-1"}); err == nil {
			t.Fatal("expected error on transient slug-lookup failure, got nil")
		}
	})
}
