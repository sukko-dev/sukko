package provisioning_test

import (
	"context"
	"encoding/hex"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
)

// spyPublisher implements WebhookCacheInvalidator and counts Publish calls.
type spyPublisher struct {
	calls   atomic.Int32
	lastTID atomic.Value
}

func (p *spyPublisher) Publish(tenantID string) {
	p.calls.Add(1)
	p.lastTID.Store(tenantID)
}

func newWebhookSvcWithSpy(t *testing.T, spy provisioning.WebhookCacheInvalidator) *provisioning.Service {
	t.Helper()
	encKey, _ := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 testutil.NewMockTenantStore(),
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
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
		WebhookStore:                testutil.NewMockWebhookStore(),
		MaxWebhooksPerTenant:        10,
		EncryptionKey:               encKey,
		WebhookAllowHTTP:            true, // allow http:// in unit tests
		InvalidationPublisher:       spy,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func createInvalidationTestTenant(t *testing.T, svc *provisioning.Service) string {
	t.Helper()
	resp, err := svc.CreateTenant(context.Background(), provisioning.CreateTenantRequest{
		Slug:         "inv-tenant",
		Name:         "Invalidation Test Tenant",
		ConsumerType: provisioning.ConsumerShared,
	})
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	return resp.Tenant.ID
}

func TestService_CreateWebhook_PublishesInvalidation(t *testing.T) {
	t.Parallel()
	spy := &spyPublisher{}
	svc := newWebhookSvcWithSpy(t, spy)
	tenantID := createInvalidationTestTenant(t, svc)

	_, err := svc.CreateWebhook(context.Background(), provisioning.CreateWebhookRequest{
		TenantID:       tenantID,
		URL:            "http://example.com/hook",
		ChannelPattern: "trades.*",
		Secret:         "mysecret",
		MaxRetries:     3,
	})
	if err != nil {
		t.Fatalf("CreateWebhook() error = %v", err)
	}
	if n := spy.calls.Load(); n != 1 {
		t.Errorf("expected 1 Publish call after CreateWebhook, got %d", n)
	}
	if got, _ := spy.lastTID.Load().(string); got != tenantID {
		t.Errorf("Publish tenantID = %q, want %q", got, tenantID)
	}
}

func TestService_UpdateWebhook_PublishesInvalidation(t *testing.T) {
	t.Parallel()
	spy := &spyPublisher{}
	svc := newWebhookSvcWithSpy(t, spy)
	tenantID := createInvalidationTestTenant(t, svc)

	wh, err := svc.CreateWebhook(context.Background(), provisioning.CreateWebhookRequest{
		TenantID:       tenantID,
		URL:            "http://example.com/hook",
		ChannelPattern: "trades.*",
		Secret:         "mysecret",
	})
	if err != nil {
		t.Fatalf("CreateWebhook() error = %v", err)
	}
	spy.calls.Store(0) // reset after create

	newURL := "http://example.com/hook2"
	_, err = svc.UpdateWebhook(context.Background(), provisioning.UpdateWebhookRequest{
		ID:       wh.ID,
		TenantID: tenantID,
		URL:      &newURL,
	})
	if err != nil {
		t.Fatalf("UpdateWebhook() error = %v", err)
	}
	if n := spy.calls.Load(); n != 1 {
		t.Errorf("expected 1 Publish call after UpdateWebhook, got %d", n)
	}
}

func TestService_DeleteWebhook_PublishesInvalidation(t *testing.T) {
	t.Parallel()
	spy := &spyPublisher{}
	svc := newWebhookSvcWithSpy(t, spy)
	tenantID := createInvalidationTestTenant(t, svc)

	wh, err := svc.CreateWebhook(context.Background(), provisioning.CreateWebhookRequest{
		TenantID:       tenantID,
		URL:            "http://example.com/hook",
		ChannelPattern: "trades.*",
		Secret:         "mysecret",
	})
	if err != nil {
		t.Fatalf("CreateWebhook() error = %v", err)
	}
	spy.calls.Store(0)

	if err := svc.DeleteWebhook(context.Background(), wh.ID, tenantID); err != nil {
		t.Fatalf("DeleteWebhook() error = %v", err)
	}
	if n := spy.calls.Load(); n != 1 {
		t.Errorf("expected 1 Publish call after DeleteWebhook, got %d", n)
	}
}

func TestService_NilInvalidationPublisher_NoPanic(t *testing.T) {
	t.Parallel()
	// nil WebhookCacheInvalidator must not panic — Community edition wires nil.
	svc := newWebhookSvcWithSpy(t, nil)
	tenantID := createInvalidationTestTenant(t, svc)

	_, err := svc.CreateWebhook(context.Background(), provisioning.CreateWebhookRequest{
		TenantID:       tenantID,
		URL:            "http://example.com/hook",
		ChannelPattern: "trades.*",
		Secret:         "mysecret",
	})
	if err != nil {
		t.Fatalf("CreateWebhook() with nil publisher panicked or errored: %v", err)
	}
}
