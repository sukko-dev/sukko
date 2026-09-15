package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// pushIntegEncKey is a valid 32-byte (hex) credentials encryption key for the real-DB tests.
const pushIntegEncKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const vapidCredData = `{"public_key":"BGxGBnqX_test_public","private_key":"OISg2_test_private"}`

// TestPushCredentialHandler_Upload_PersistsTenantUUID is the real-DB acceptance test (#173):
// a POST with body tenant_id=<slug> must persist the tenant UUID into the UUID column and echo
// the input slug in the response. Fails on pre-fix code (slug → UUID column → rejected).
func TestPushCredentialHandler_Upload_PersistsTenantUUID(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	tenantRepo := repository.NewTenantRepository(pool)
	tenant := &provisioning.Tenant{Slug: "acme", Name: "Acme", Status: provisioning.StatusActive, ConsumerType: provisioning.ConsumerShared}
	if err := tenantRepo.Create(ctx, tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if tenant.ID == tenant.Slug {
		t.Fatalf("test invariant: UUID %q must differ from slug %q", tenant.ID, tenant.Slug)
	}

	credRepo, err := repository.NewCredentialsRepository(pool, pushIntegEncKey)
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	h, err := NewPushCredentialHandler(credRepo, tenantRepo.GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	post := func() *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"tenant_id": tenant.Slug, "provider": "vapid", "credential_data": vapidCredData})
		r := httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		h.HandleUploadCredentials(rr, r)
		return rr
	}

	rr := post()
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var resp uploadCredentialsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TenantID != tenant.Slug {
		t.Errorf("response tenant_id = %q, want input slug %q (not the UUID)", resp.TenantID, tenant.Slug)
	}

	// Authoritative: persisted tenant_id is the UUID.
	var got string
	if err := pool.QueryRow(ctx, `SELECT tenant_id::text FROM push_credentials WHERE provider = 'vapid'`).Scan(&got); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if got != tenant.ID {
		t.Errorf("persisted tenant_id = %q, want UUID %q (slug %q)", got, tenant.ID, tenant.Slug)
	}

	// Idempotent upsert: a second POST for the same tenant+provider succeeds (not 500),
	// updates in place (no duplicate row), and returns the same non-zero id via the re-Get.
	rr2 := post()
	if rr2.Code != http.StatusCreated {
		t.Fatalf("re-upload status = %d, want 201 (idempotent upsert); body: %s", rr2.Code, rr2.Body.String())
	}
	var resp2 uploadCredentialsResponse
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode re-upload: %v", err)
	}
	if resp2.ID == 0 || resp2.ID != resp.ID {
		t.Errorf("re-upload id = %d, want same non-zero id as first upload (%d)", resp2.ID, resp.ID)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_credentials WHERE provider = 'vapid'`).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("row count = %d, want 1 (upsert must not duplicate)", rows)
	}
}

// TestPushCredentialHandler_Delete_BySlug verifies delete resolves the slug and removes the
// UUID-keyed row.
func TestPushCredentialHandler_Delete_BySlug(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	tenantRepo := repository.NewTenantRepository(pool)
	tenant := &provisioning.Tenant{Slug: "acme", Name: "Acme", Status: provisioning.StatusActive, ConsumerType: provisioning.ConsumerShared}
	if err := tenantRepo.Create(ctx, tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	credRepo, err := repository.NewCredentialsRepository(pool, pushIntegEncKey)
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	h, err := NewPushCredentialHandler(credRepo, tenantRepo.GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	// Seed a credential via the handler.
	body, _ := json.Marshal(map[string]any{"tenant_id": tenant.Slug, "provider": "vapid", "credential_data": vapidCredData})
	cr := httptest.NewRecorder()
	h.HandleUploadCredentials(cr, httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewReader(body)))
	if cr.Code != http.StatusCreated {
		t.Fatalf("seed upload status = %d; body: %s", cr.Code, cr.Body.String())
	}

	// Delete by slug (query param).
	dr := httptest.NewRecorder()
	h.HandleDeleteCredentials(dr, httptest.NewRequest(http.MethodDelete, "/api/v1/push/credentials?tenant_id="+tenant.Slug+"&provider=vapid", http.NoBody))
	if dr.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body: %s", dr.Code, dr.Body.String())
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_credentials WHERE tenant_id = $1`, tenant.ID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("row count = %d, want 0 after delete", n)
	}
}

// TestPushCredentialHandler_SlugNotFound verifies an unknown slug → 404 TENANT_NOT_FOUND
// (distinct from the resource NOT_FOUND), proving resolution ran.
func TestPushCredentialHandler_SlugNotFound(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	credRepo, err := repository.NewCredentialsRepository(pool, pushIntegEncKey)
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	h, err := NewPushCredentialHandler(credRepo, repository.NewTenantRepository(pool).GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	body, _ := json.Marshal(map[string]any{"tenant_id": "ghost", "provider": "vapid", "credential_data": vapidCredData})
	rr := httptest.NewRecorder()
	h.HandleUploadCredentials(rr, httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewReader(body)))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeTenantNotFound)
}

// TestPushCredentialHandler_TransientResolverError verifies a non-sentinel resolver error →
// 503 (not a silent 404/500). Uses a stub resolver; no DB rows needed but a real repo is
// required to construct the handler.
func TestPushCredentialHandler_TransientResolverError(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	credRepo, err := repository.NewCredentialsRepository(pool, pushIntegEncKey)
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	boom := func(context.Context, string) (*provisioning.Tenant, error) { return nil, errors.New("db unavailable") }
	h, err := NewPushCredentialHandler(credRepo, boom, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	body, _ := json.Marshal(map[string]any{"tenant_id": "acme", "provider": "vapid", "credential_data": vapidCredData})
	rr := httptest.NewRecorder()
	h.HandleUploadCredentials(rr, httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", bytes.NewReader(body)))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body: %s", rr.Code, rr.Body.String())
	}
}

// TestNewPushCredentialHandler_NilResolver verifies the constructor rejects a nil resolver.
func TestNewPushCredentialHandler_NilResolver(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	credRepo, err := repository.NewCredentialsRepository(pool, pushIntegEncKey)
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	if _, err := NewPushCredentialHandler(credRepo, nil, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil); err == nil {
		t.Error("expected error for nil tenant resolver")
	}
}
