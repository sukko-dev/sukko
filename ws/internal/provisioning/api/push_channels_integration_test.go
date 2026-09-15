package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// newRealPushChannelHandler seeds a tenant and returns a channel handler wired with the real
// channel repo + a real tenant resolver on the pool, plus the created tenant.
func newRealPushChannelHandler(t *testing.T, pool *pgxpool.Pool) (*PushChannelHandler, *provisioning.Tenant) {
	t.Helper()
	tenantRepo := repository.NewTenantRepository(pool)
	tenant := &provisioning.Tenant{Slug: "acme", Name: "Acme", Status: provisioning.StatusActive, ConsumerType: provisioning.ConsumerShared}
	if err := tenantRepo.Create(context.Background(), tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	h, err := NewPushChannelHandler(repository.NewChannelConfigRepository(pool), tenantRepo.GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop())
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	return h, tenant
}

// TestPushChannelHandler_Create_PersistsTenantUUID (#173): create resolves slug→UUID, persists
// the UUID, echoes the input slug, and still validates the channel prefix against the slug.
func TestPushChannelHandler_Create_PersistsTenantUUID(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	h, tenant := newRealPushChannelHandler(t, pool)
	if tenant.ID == tenant.Slug {
		t.Fatalf("test invariant: UUID %q must differ from slug %q", tenant.ID, tenant.Slug)
	}

	body, _ := json.Marshal(map[string]any{
		"tenant_id": tenant.Slug,
		"patterns":  []string{tenant.Slug + ".orders"},
	})
	rr := httptest.NewRecorder()
	h.HandleCreateChannelConfig(rr, httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewReader(body)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var resp channelConfigResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TenantID != tenant.Slug {
		t.Errorf("response tenant_id = %q, want input slug %q", resp.TenantID, tenant.Slug)
	}
	var got string
	if err := pool.QueryRow(context.Background(), `SELECT tenant_id::text FROM push_channel_configs LIMIT 1`).Scan(&got); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if got != tenant.ID {
		t.Errorf("persisted tenant_id = %q, want UUID %q (slug %q)", got, tenant.ID, tenant.Slug)
	}
}

// TestPushChannelHandler_Create_RejectsNonPrefixedPattern verifies the slug prefix check is
// preserved after the fix (a pattern not prefixed with the tenant slug → 400).
func TestPushChannelHandler_Create_RejectsNonPrefixedPattern(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	h, tenant := newRealPushChannelHandler(t, pool)

	body, _ := json.Marshal(map[string]any{
		"tenant_id": tenant.Slug,
		"patterns":  []string{"other-tenant.orders"},
	})
	rr := httptest.NewRecorder()
	h.HandleCreateChannelConfig(rr, httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewReader(body)))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidPattern)
}

// TestPushChannelHandler_Get_EchoesSlug verifies GET resolves slug→UUID, returns the row, and
// echoes the input slug (not the stored UUID).
func TestPushChannelHandler_Get_EchoesSlug(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	h, tenant := newRealPushChannelHandler(t, pool)

	// Seed via the handler.
	body, _ := json.Marshal(map[string]any{"tenant_id": tenant.Slug, "patterns": []string{tenant.Slug + ".orders"}})
	cr := httptest.NewRecorder()
	h.HandleCreateChannelConfig(cr, httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewReader(body)))
	if cr.Code != http.StatusCreated {
		t.Fatalf("seed status = %d; body: %s", cr.Code, cr.Body.String())
	}

	gr := httptest.NewRecorder()
	h.HandleGetChannelConfig(gr, httptest.NewRequest(http.MethodGet, "/api/v1/push/channels?tenant_id="+tenant.Slug, http.NoBody))
	if gr.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200; body: %s", gr.Code, gr.Body.String())
	}
	var resp channelConfigResponse
	if err := json.Unmarshal(gr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TenantID != tenant.Slug {
		t.Errorf("GET response tenant_id = %q, want input slug %q (not the stored UUID)", resp.TenantID, tenant.Slug)
	}
	// Patterns must round-trip through the TEXT[] column (pgx []string, not a JSON blob).
	if len(resp.Patterns) != 1 || resp.Patterns[0] != tenant.Slug+".orders" {
		t.Errorf("GET response patterns = %v, want [%q]", resp.Patterns, tenant.Slug+".orders")
	}
}

// TestPushChannelHandler_Delete_BySlug verifies delete resolves slug and removes the UUID row.
func TestPushChannelHandler_Delete_BySlug(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	h, tenant := newRealPushChannelHandler(t, pool)

	body, _ := json.Marshal(map[string]any{"tenant_id": tenant.Slug, "patterns": []string{tenant.Slug + ".orders"}})
	cr := httptest.NewRecorder()
	h.HandleCreateChannelConfig(cr, httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewReader(body)))
	if cr.Code != http.StatusCreated {
		t.Fatalf("seed status = %d; body: %s", cr.Code, cr.Body.String())
	}

	dr := httptest.NewRecorder()
	h.HandleDeleteChannelConfig(dr, httptest.NewRequest(http.MethodDelete, "/api/v1/push/channels?tenant_id="+tenant.Slug, http.NoBody))
	if dr.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body: %s", dr.Code, dr.Body.String())
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM push_channel_configs WHERE tenant_id = $1`, tenant.ID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("row count = %d, want 0 after delete", n)
	}
}

// TestPushChannelHandler_SlugNotFound verifies an unknown slug → 404 TENANT_NOT_FOUND.
func TestPushChannelHandler_SlugNotFound(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	h := func() *PushChannelHandler {
		hh, err := NewPushChannelHandler(repository.NewChannelConfigRepository(pool), repository.NewTenantRepository(pool).GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop())
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		return hh
	}()

	body, _ := json.Marshal(map[string]any{"tenant_id": "ghost", "patterns": []string{"ghost.orders"}})
	rr := httptest.NewRecorder()
	h.HandleCreateChannelConfig(rr, httptest.NewRequest(http.MethodPost, "/api/v1/push/channels", bytes.NewReader(body)))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeTenantNotFound)
}

// TestNewPushChannelHandler_NilResolver verifies the constructor rejects a nil resolver
// (the credentials side is covered by TestNewPushCredentialHandler_NilResolver).
func TestNewPushChannelHandler_NilResolver(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	if _, err := NewPushChannelHandler(repository.NewChannelConfigRepository(pool), nil, eventbus.New(zerolog.Nop()), zerolog.Nop()); err == nil {
		t.Error("expected error for nil tenant resolver")
	}
}
