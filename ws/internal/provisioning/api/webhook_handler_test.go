package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// mockWebhookService implements webhookService for tests.
type mockWebhookService struct {
	createFn func(ctx context.Context, req provisioning.CreateWebhookRequest) (*provisioning.Webhook, error)
	getFn    func(ctx context.Context, id, tenantID string) (*provisioning.Webhook, error)
	listFn   func(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.Webhook, int, error)
	updateFn func(ctx context.Context, req provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error)
	deleteFn func(ctx context.Context, id, tenantID string) error
}

func (m *mockWebhookService) CreateWebhook(ctx context.Context, req provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
	return m.createFn(ctx, req)
}
func (m *mockWebhookService) GetWebhook(ctx context.Context, id, tenantID string) (*provisioning.Webhook, error) {
	return m.getFn(ctx, id, tenantID)
}
func (m *mockWebhookService) ListWebhooks(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.Webhook, int, error) {
	return m.listFn(ctx, tenantID, opts)
}
func (m *mockWebhookService) UpdateWebhook(ctx context.Context, req provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error) {
	return m.updateFn(ctx, req)
}
func (m *mockWebhookService) DeleteWebhook(ctx context.Context, id, tenantID string) error {
	return m.deleteFn(ctx, id, tenantID)
}

// stubWebhook returns a minimal Webhook for use in test assertions.
func stubWebhook() *provisioning.Webhook {
	return &provisioning.Webhook{
		ID:             "wh_abc123",
		TenantID:       "tenant-uuid-1",
		URL:            "https://example.com/hook",
		ChannelPattern: "trades.*",
		Status:         "enabled",
		MaxRetries:     5,
		CreatedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// reqWithTenantID injects tenant claims and stashes the tenant identity in context,
// simulating what RequireTenant does. Webhook handlers read the UUID from context.
// uuid == slug here; tests that must distinguish the two use reqWithTenant.
func reqWithTenantID(r *http.Request, tenantID string) *http.Request {
	r = withClaims(r, &auth.Claims{TenantID: tenantID, Roles: []string{"user"}})
	return r.WithContext(stashTenantIdentity(r.Context(), &provisioning.Tenant{ID: tenantID, Slug: tenantID}))
}

// reqWithTenant stashes distinct tenant UUID and slug, simulating RequireTenant for
// tests that assert handlers thread the correct identity (UUID vs slug).
func reqWithTenant(r *http.Request, uuid, slug string) *http.Request {
	r = withClaims(r, &auth.Claims{TenantID: slug, Roles: []string{"user"}})
	return r.WithContext(stashTenantIdentity(r.Context(), &provisioning.Tenant{ID: uuid, Slug: slug}))
}

// reqWithWebhookID injects a chi URL param for webhookID.
func reqWithWebhookID(r *http.Request, id string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("webhookID", id)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func newTestWebhookHandler(t *testing.T, svc webhookService) *WebhookHandler {
	t.Helper()
	h, err := NewWebhookHandler(svc, testLogger(t))
	if err != nil {
		t.Fatalf("NewWebhookHandler: %v", err)
	}
	return h
}

// TestWebhookHandler_Create_Success verifies 201 + body on happy path.
func TestWebhookHandler_Create_Success(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		createFn: func(_ context.Context, _ provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
			return stubWebhook(), nil
		},
	}
	h := newTestWebhookHandler(t, svc)

	body, _ := json.Marshal(map[string]any{
		"url":             "https://example.com/hook",
		"channel_pattern": "trades.*",
		"secret":          "s3cr3t",
	})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()

	h.HandleCreate(rr, r)

	if rr.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rr.Code)
	}
	var resp webhookResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID != "wh_abc123" {
		t.Errorf("id = %q, want wh_abc123", resp.ID)
	}
	// SecretEnc must never appear in the response.
	var raw map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &raw)
	if _, found := raw["secret_enc"]; found {
		t.Error("secret_enc must not appear in API response (§IX)")
	}
}

// TestWebhookHandler_Create_MissingTenant verifies 401 when no tenant in context.
func TestWebhookHandler_Create_MissingTenant(t *testing.T) {
	t.Parallel()
	h := newTestWebhookHandler(t, &mockWebhookService{})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader([]byte(`{}`)))
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

// TestWebhookHandler_Create_MissingSecret verifies 400 when secret is absent.
func TestWebhookHandler_Create_MissingSecret(t *testing.T) {
	t.Parallel()
	h := newTestWebhookHandler(t, &mockWebhookService{})
	body, _ := json.Marshal(map[string]any{"url": "https://x.com", "channel_pattern": "a.*"})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// TestWebhookHandler_Create_QuotaExceeded verifies 422 on quota error.
func TestWebhookHandler_Create_QuotaExceeded(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		createFn: func(_ context.Context, _ provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
			return nil, provisioning.ErrWebhookQuotaExceeded
		},
	}
	h := newTestWebhookHandler(t, svc)
	body, _ := json.Marshal(map[string]any{"url": "https://x.com", "channel_pattern": "a.*", "secret": "s"})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", rr.Code)
	}
}

// TestWebhookHandler_Get_Success verifies 200 + body.
func TestWebhookHandler_Get_Success(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		getFn: func(_ context.Context, id, _ string) (*provisioning.Webhook, error) {
			if id != "wh_abc123" {
				return nil, provisioning.ErrWebhookNotFound
			}
			return stubWebhook(), nil
		},
	}
	h := newTestWebhookHandler(t, svc)
	r := httptest.NewRequest(http.MethodGet, "/webhooks/wh_abc123", http.NoBody)
	r = reqWithTenantID(r, "tenant-uuid-1")
	r = reqWithWebhookID(r, "wh_abc123")
	rr := httptest.NewRecorder()
	h.HandleGet(rr, r)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

// TestWebhookHandler_Get_NotFound verifies 404.
func TestWebhookHandler_Get_NotFound(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		getFn: func(_ context.Context, _, _ string) (*provisioning.Webhook, error) {
			return nil, provisioning.ErrWebhookNotFound
		},
	}
	h := newTestWebhookHandler(t, svc)
	r := httptest.NewRequest(http.MethodGet, "/webhooks/wh_missing", http.NoBody)
	r = reqWithTenantID(r, "tenant-uuid-1")
	r = reqWithWebhookID(r, "wh_missing")
	rr := httptest.NewRecorder()
	h.HandleGet(rr, r)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestWebhookHandler_List_Success verifies 200 + pagination envelope.
func TestWebhookHandler_List_Success(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		listFn: func(_ context.Context, _ string, _ provisioning.ListOptions) ([]*provisioning.Webhook, int, error) {
			return []*provisioning.Webhook{stubWebhook()}, 1, nil
		},
	}
	h := newTestWebhookHandler(t, svc)
	r := httptest.NewRequest(http.MethodGet, "/webhooks", http.NoBody)
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleList(rr, r)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	var resp webhookListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || len(resp.Items) != 1 {
		t.Errorf("expected 1 item, got total=%d items=%d", resp.Total, len(resp.Items))
	}
}

// TestWebhookHandler_Update_NotFound verifies 404 forwarded from service.
func TestWebhookHandler_Update_NotFound(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		updateFn: func(_ context.Context, _ provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error) {
			return nil, provisioning.ErrWebhookNotFound
		},
	}
	h := newTestWebhookHandler(t, svc)
	body, _ := json.Marshal(map[string]any{"status": "suspended"})
	r := httptest.NewRequest(http.MethodPatch, "/webhooks/wh_gone", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	r = reqWithWebhookID(r, "wh_gone")
	rr := httptest.NewRecorder()
	h.HandleUpdate(rr, r)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestWebhookHandler_Delete_Success verifies 200 on successful delete.
func TestWebhookHandler_Delete_Success(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		deleteFn: func(_ context.Context, _, _ string) error { return nil },
	}
	h := newTestWebhookHandler(t, svc)
	r := httptest.NewRequest(http.MethodDelete, "/webhooks/wh_abc123", http.NoBody)
	r = reqWithTenantID(r, "tenant-uuid-1")
	r = reqWithWebhookID(r, "wh_abc123")
	rr := httptest.NewRecorder()
	h.HandleDelete(rr, r)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

// TestWebhookHandler_Delete_NotFound verifies 404 forwarded from service.
func TestWebhookHandler_Delete_NotFound(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		deleteFn: func(_ context.Context, _, _ string) error {
			return provisioning.ErrWebhookNotFound
		},
	}
	h := newTestWebhookHandler(t, svc)
	r := httptest.NewRequest(http.MethodDelete, "/webhooks/wh_gone", http.NoBody)
	r = reqWithTenantID(r, "tenant-uuid-1")
	r = reqWithWebhookID(r, "wh_gone")
	rr := httptest.NewRecorder()
	h.HandleDelete(rr, r)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestNewWebhookHandler_NilService verifies constructor validates nil service.
func TestNewWebhookHandler_NilService(t *testing.T) {
	t.Parallel()
	_, err := NewWebhookHandler(nil, testLogger(t))
	if err == nil {
		t.Error("expected error for nil service")
	}
}

// TestWebhookResponse_NoSecretEnc verifies toWebhookResponse never exposes SecretEnc.
func TestWebhookResponse_NoSecretEnc(t *testing.T) {
	t.Parallel()
	w := stubWebhook()
	w.SecretEnc = "this-should-not-appear"
	resp := toWebhookResponse(w)

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	if _, found := raw["secret_enc"]; found {
		t.Error("secret_enc must not appear in API response (§IX)")
	}
	if _, found := raw["SecretEnc"]; found {
		t.Error("SecretEnc must not appear in API response (§IX)")
	}
}

// TestWebhookHandler_Create_InvalidURL verifies 400 when service returns ErrWebhookInvalidInput for URL.
func TestWebhookHandler_Create_InvalidURL(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		createFn: func(_ context.Context, _ provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
			return nil, fmt.Errorf("%w: webhook url must use https", provisioning.ErrWebhookInvalidInput)
		},
	}
	h := newTestWebhookHandler(t, svc)
	body, _ := json.Marshal(map[string]any{"url": "http://example.com", "channel_pattern": "a.*", "secret": "s"})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if got := resp["code"]; got != errCodeWebhookInvalidInput {
		t.Errorf("code = %q, want %q", got, errCodeWebhookInvalidInput)
	}
}

// TestWebhookHandler_Create_StoreNotConfigured verifies 403 when webhook store is nil.
func TestWebhookHandler_Create_StoreNotConfigured(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		createFn: func(_ context.Context, _ provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
			return nil, provisioning.ErrWebhookStoreNotConfigured
		},
	}
	h := newTestWebhookHandler(t, svc)
	body, _ := json.Marshal(map[string]any{"url": "https://example.com", "channel_pattern": "a.*", "secret": "s"})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rr.Code)
	}
}

// TestWebhookHandler_Create_InternalError verifies 500 for unexpected service errors.
func TestWebhookHandler_Create_InternalError(t *testing.T) {
	t.Parallel()
	svc := &mockWebhookService{
		createFn: func(_ context.Context, _ provisioning.CreateWebhookRequest) (*provisioning.Webhook, error) {
			return nil, errors.New("unexpected db failure")
		},
	}
	h := newTestWebhookHandler(t, svc)
	body, _ := json.Marshal(map[string]any{"url": "https://example.com", "channel_pattern": "a.*", "secret": "s"})
	r := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	r = reqWithTenantID(r, "tenant-uuid-1")
	rr := httptest.NewRecorder()
	h.HandleCreate(rr, r)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func testLogger(_ *testing.T) zerolog.Logger { return zerolog.Nop() }
