package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	provtestutil "github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// TestWebhookHandler_Create_PersistsTenantUUID is the real-DB acceptance test for #166.
// It wires the real WebhookHandler → real Service → real WebhookRepository → Postgres and
// asserts HandleCreate writes the tenant UUID (not the slug) into the webhooks.tenant_id
// UUID FK column. It fails on the pre-fix code, which read claims.TenantID (a slug) →
// INSERT rejected (invalid UUID syntax / FK violation) → 500. Mock-based handler tests
// cannot catch this because they never touch the UUID column.
func TestWebhookHandler_Create_PersistsTenantUUID(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// Insert a tenant to satisfy the webhooks.tenant_id FK; capture its DB-generated UUID.
	tenant := &provisioning.Tenant{
		Slug:         "acme",
		Name:         "Acme",
		Status:       provisioning.StatusActive,
		ConsumerType: provisioning.ConsumerShared,
	}
	if err := repository.NewTenantRepository(pool).Create(ctx, tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if tenant.ID == tenant.Slug {
		t.Fatalf("test invariant broken: tenant UUID %q must differ from slug %q", tenant.ID, tenant.Slug)
	}

	encKey, err := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("decode enc key: %v", err)
	}
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 provtestutil.NewMockTenantStore(),
		KeyStore:                    provtestutil.NewMockKeyStore(),
		APIKeyStore:                 provtestutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           provtestutil.NewMockRoutingRulesStore(),
		QuotaStore:                  provtestutil.NewMockQuotaStore(),
		AuditStore:                  provtestutil.NewMockAuditStore(),
		KafkaAdmin:                  provtestutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
		WebhookStore:                repository.NewWebhookRepository(pool, zerolog.Nop()),
		EncryptionKey:               encKey,
		MaxWebhooksPerTenant:        10,
		WebhookAllowHTTP:            false,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	h := newTestWebhookHandler(t, svc)

	body, _ := json.Marshal(map[string]any{
		"url":             "https://example.com/hook",
		"channel_pattern": "trades.*",
		"secret":          "s3cr3t",
	})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	// Stash a distinct UUID and slug so a slug leak into the UUID column is caught.
	r = reqWithTenant(r, tenant.ID, tenant.Slug)
	rr := httptest.NewRecorder()

	h.HandleCreate(rr, r)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var resp webhookResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	// Response tenant_id is the UUID (matches OpenAPI Webhook.tenant_id: format uuid).
	if resp.TenantID != tenant.ID {
		t.Errorf("response tenant_id = %q, want UUID %q", resp.TenantID, tenant.ID)
	}

	// Authoritative check: the persisted row's tenant_id equals the tenant UUID.
	var gotTenantID string
	if err := pool.QueryRow(ctx,
		`SELECT tenant_id FROM webhooks WHERE id = $1`, resp.ID).Scan(&gotTenantID); err != nil {
		t.Fatalf("query webhook row: %v", err)
	}
	if gotTenantID != tenant.ID {
		t.Errorf("persisted tenant_id = %q, want UUID %q (slug is %q)", gotTenantID, tenant.ID, tenant.Slug)
	}
}
