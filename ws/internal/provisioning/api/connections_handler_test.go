package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// newTestConnectionsHandler creates a ConnectionsHandler suitable for unit tests that
// do not reach Valkey. The Valkey client is nil because all tested paths return before
// any reader call. BulkDisconnectConcurrency must be > 0 to avoid a nil-channel panic
// in HandleBulkDisconnect (the sem channel is allocated from this value).
func newTestConnectionsHandler() *ConnectionsHandler {
	cfg := platform.ProvisioningConfig{}
	cfg.Environment = "test"
	cfg.BulkDisconnectConcurrency = 10
	return NewConnectionsHandler(ConnectionsHandlerParams{
		Client:     nil,
		Cfg:        cfg,
		Logger:     zerolog.Nop(),
		ServiceCtx: nil,                      // defaults to context.Background() inside NewConnectionsHandler
		Registerer: prometheus.NewRegistry(), // isolated registry prevents duplicate-metric panics
	})
}

// withTenantClaims injects JWT claims (the source of the tenant slug for registry reads,
// via getTenantSlugFromClaims) and stashes the tenant UUID (for audit-log writes),
// simulating what AuthMiddleware + RequireTenant do. uuid == slug here.
func withTenantClaims(r *http.Request, tenantID string) *http.Request {
	r = r.WithContext(auth.WithClaims(r.Context(), &auth.Claims{TenantID: tenantID}))
	//nolint:contextcheck // stashTenantIdentity derives from r.Context() (mirrors RequireTenant); test helper.
	return r.WithContext(stashTenantIdentity(r.Context(), &provisioning.Tenant{ID: tenantID, Slug: tenantID}))
}

// withTenantIdentity injects a distinct tenant slug (in claims, the registry key) and
// UUID (stashed, the audit key), simulating AuthMiddleware + RequireTenant. Used to prove
// a handler threads the correct identity: slug to the registry, UUID to the audit log.
func withTenantIdentity(r *http.Request, uuid, slug string) *http.Request {
	r = r.WithContext(auth.WithClaims(r.Context(), &auth.Claims{TenantID: slug}))
	//nolint:contextcheck // stashTenantIdentity derives from r.Context() (mirrors RequireTenant); test helper.
	return r.WithContext(stashTenantIdentity(r.Context(), &provisioning.Tenant{ID: uuid, Slug: slug}))
}

// TestHandleListConnections_MissingClaims verifies that HandleListConnections returns
// 401 with UNAUTHORIZED when no JWT claims are present in the context (RequireTenant not bypassed).
func TestHandleListConnections_MissingClaims(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleListConnections(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
	assertErrorCode(t, rr, errCodeUnauthorized)
}

// TestHandleListConnections_InvalidLimit verifies that a non-numeric limit query param
// returns 400 with an appropriate error code. Claims must be injected first because the
// handler checks tenant context before parsing filters.
func TestHandleListConnections_InvalidLimit(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections?limit=abc", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	h.HandleListConnections(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleListConnections_NegativeOffset verifies that a negative offset returns 400.
func TestHandleListConnections_NegativeOffset(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections?offset=-1", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	h.HandleListConnections(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleListConnections_InvalidTransport verifies that an unrecognized transport
// value returns 400. Valid values are "ws" and "sse" only.
func TestHandleListConnections_InvalidTransport(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections?transport=grpc", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	h.HandleListConnections(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleBulkDisconnect_MissingClaims verifies that bulk disconnect returns 401 with
// UNAUTHORIZED when no JWT claims are present — the tenant guard fires before filter validation.
func TestHandleBulkDisconnect_MissingClaims(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections?api_key_id=key-1", http.NoBody)
	// No claims injected — tenant guard fires first.
	rr := httptest.NewRecorder()

	h.HandleBulkDisconnect(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusUnauthorized, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeUnauthorized)
}

// TestHandleBulkDisconnect_RequiresFilter verifies that bulk disconnect returns 400 when
// neither api_key_id nor channel filter is provided (tenant claims present so guard passes).
func TestHandleBulkDisconnect_RequiresFilter(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	h.HandleBulkDisconnect(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleBulkDisconnect_OnlyAPIKeyIDFilterAccepted verifies that api_key_id alone
// satisfies the filter requirement. The handler proceeds past the filter check and hits
// the nil Valkey reader; we only assert the filter-required 400 does not fire.
func TestHandleBulkDisconnect_OnlyAPIKeyIDFilterAccepted(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections?api_key_id=key-1", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	func() {
		defer func() { _ = recover() }()
		h.HandleBulkDisconnect(rr, req)
	}()

	if rr.Code == http.StatusBadRequest {
		assertErrorCodeNot(t, rr, errCodeInvalidRequest, "filter-required 400 must not fire when api_key_id is provided")
	}
}

// TestHandleBulkDisconnect_OnlyChannelFilterAccepted mirrors the api_key_id test for
// the channel filter parameter.
func TestHandleBulkDisconnect_OnlyChannelFilterAccepted(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections?channel=acme.prices", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	func() {
		defer func() { _ = recover() }()
		h.HandleBulkDisconnect(rr, req)
	}()

	if rr.Code == http.StatusBadRequest {
		assertErrorCodeNot(t, rr, errCodeInvalidRequest, "filter-required 400 must not fire when channel is provided")
	}
}

// TestParseConnectionFilters is a table-driven unit test for the parseConnectionFilters
// helper covering the full input-validation matrix without HTTP server overhead.
func TestParseConnectionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		query         string
		defaultLim    int
		maxLim        int
		wantErr       bool
		wantLimit     int
		wantOffset    int
		wantTransport string
	}{
		{
			name:       "defaults applied when no params",
			query:      "",
			defaultLim: 50,
			maxLim:     100,
			wantLimit:  50,
			wantOffset: 0,
		},
		{
			name:       "explicit valid limit",
			query:      "limit=10",
			defaultLim: 50,
			maxLim:     100,
			wantLimit:  10,
		},
		{
			name:       "limit capped at maxLimit",
			query:      "limit=200",
			defaultLim: 50,
			maxLim:     100,
			wantLimit:  100,
		},
		{
			name:       "limit=0 is invalid",
			query:      "limit=0",
			defaultLim: 50,
			maxLim:     100,
			wantErr:    true,
		},
		{
			name:       "limit non-numeric",
			query:      "limit=abc",
			defaultLim: 50,
			maxLim:     100,
			wantErr:    true,
		},
		{
			name:       "valid offset",
			query:      "offset=5",
			defaultLim: 50,
			maxLim:     100,
			wantLimit:  50,
			wantOffset: 5,
		},
		{
			name:       "negative offset is invalid",
			query:      "offset=-1",
			defaultLim: 50,
			maxLim:     100,
			wantErr:    true,
		},
		{
			name:          "valid transport ws",
			query:         "transport=ws",
			defaultLim:    50,
			maxLim:        100,
			wantLimit:     50,
			wantTransport: "ws",
		},
		{
			name:          "valid transport sse",
			query:         "transport=sse",
			defaultLim:    50,
			maxLim:        100,
			wantLimit:     50,
			wantTransport: "sse",
		},
		{
			name:       "invalid transport",
			query:      "transport=grpc",
			defaultLim: 50,
			maxLim:     100,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/?"+tt.query, http.NoBody)
			filters, err := parseConnectionFilters(req, tt.defaultLim, tt.maxLim)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for query %q, got nil", tt.query)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for query %q: %v", tt.query, err)
			}
			if filters.Limit != tt.wantLimit {
				t.Errorf("Limit = %d, want %d", filters.Limit, tt.wantLimit)
			}
			if filters.Offset != tt.wantOffset {
				t.Errorf("Offset = %d, want %d", filters.Offset, tt.wantOffset)
			}
			if tt.wantTransport != "" && filters.Transport != tt.wantTransport {
				t.Errorf("Transport = %q, want %q", filters.Transport, tt.wantTransport)
			}
		})
	}
}

// TestHandleGetConnection_MissingClaims verifies that GET /connections/{connId} returns
// 401 with UNAUTHORIZED when no JWT claims are present in the context.
func TestHandleGetConnection_MissingClaims(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections/some-conn-id", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleGetConnection(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusUnauthorized, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeUnauthorized)
}

// TestHandleGetConnection_MissingConnID verifies that GET /connections/{connId} returns
// 400 when the URL parameter is absent. Claims are injected so the tenant guard passes.
func TestHandleGetConnection_MissingConnID(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/tenants/acme/connections/", http.NoBody)
	req = withTenantClaims(req, "acme")
	// connId URL param is absent — chi.URLParam returns "".
	rr := httptest.NewRecorder()

	h.HandleGetConnection(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleDeleteConnection_MissingClaims verifies that DELETE /connections/{connId}
// returns 401 with UNAUTHORIZED when no JWT claims are present.
func TestHandleDeleteConnection_MissingClaims(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections/some-conn-id", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleDeleteConnection(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusUnauthorized, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeUnauthorized)
}

// TestHandleDeleteConnection_MissingConnID verifies that DELETE /connections/{connId}
// returns 400 when connId is absent from the URL. Claims are injected.
func TestHandleDeleteConnection_MissingConnID(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections/", http.NoBody)
	req = withTenantClaims(req, "acme")
	rr := httptest.NewRecorder()

	h.HandleDeleteConnection(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleAdminListConnections_InvalidLimit verifies that the admin list endpoint
// returns 400 for a non-numeric limit parameter.
func TestHandleAdminListConnections_InvalidLimit(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/connections?limit=bad", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleAdminListConnections(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// TestHandleAdminListConnections_InvalidTransport verifies that an unrecognized transport
// value returns 400 from the admin list endpoint.
func TestHandleAdminListConnections_InvalidTransport(t *testing.T) {
	t.Parallel()

	h := newTestConnectionsHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/connections?transport=tcp", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleAdminListConnections(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rr.Code, http.StatusBadRequest, rr.Body.String())
	}
	assertErrorCode(t, rr, errCodeInvalidRequest)
}

// assertErrorCode decodes the response body and checks the "code" field equals wantCode.
func assertErrorCode(t *testing.T, rr *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body["code"] != wantCode {
		t.Errorf("error code = %q, want %q; body: %v", body["code"], wantCode, body)
	}
}

// assertErrorCodeNot fails if the response body has the given error code.
func assertErrorCodeNot(t *testing.T, rr *httptest.ResponseRecorder, badCode, msg string) {
	t.Helper()
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		return // non-JSON response: code check irrelevant
	}
	if body["code"] == badCode {
		t.Errorf("%s: got error code %q; body: %v", msg, badCode, body)
	}
}

// fakeConnReader is an injectable connectionsRegistryReader for handler tests — it never
// touches Valkey. Fields control the paths exercised; unset methods return benign zero values.
type fakeConnReader struct {
	listResult []ConnectionDetail
	getResult  *ConnectionDetail
	listKeys   []string // records the tenant key passed to listTenantConnections (for slug/UUID assertions)
}

func (f *fakeConnReader) listTenantConnections(_ context.Context, tenantID string, _ connectionFilters) ([]ConnectionDetail, int, error) {
	f.listKeys = append(f.listKeys, tenantID)
	return f.listResult, len(f.listResult), nil
}

func (f *fakeConnReader) getConnection(_ context.Context, _, _ string) (*ConnectionDetail, error) {
	return f.getResult, nil
}

// publishDisconnect returns 0 subscribers (dead-pod path) — reaches the audit-log call.
func (f *fakeConnReader) publishDisconnect(_ context.Context, _, _, _, _ string) (int64, error) {
	return 0, nil
}

func (f *fakeConnReader) isAdminHealthy(_ context.Context, _ string) bool { return true }

func (f *fakeConnReader) fetchHealthKeys(_ context.Context, _ map[string]bool) map[string]map[string]string {
	return map[string]map[string]string{}
}

// mockConnService captures the tenant identifier passed to AuditLog and serves a
// configurable tenant list for the cross-tenant admin path.
type mockConnService struct {
	auditCalled   bool
	auditTenantID string
	auditAction   string
	tenants       []*provisioning.Tenant
}

func (m *mockConnService) AuditLog(_ context.Context, tenantID, action string, _ provisioning.Metadata) {
	m.auditCalled = true
	m.auditTenantID = tenantID
	m.auditAction = action
}

func (m *mockConnService) ListTenants(_ context.Context, opts provisioning.ListOptions) ([]*provisioning.Tenant, int, error) {
	if opts.Offset > 0 {
		return nil, len(m.tenants), nil // second page empty — terminates the enumeration loop
	}
	return m.tenants, len(m.tenants), nil
}

// TestHandleBulkDisconnect_AuditLogUsesTenantUUID verifies the connections half of #166:
// the force-disconnect audit entry MUST carry the tenant UUID (identity-of-record), not the
// slug the caller authenticated with. The registry read still uses the slug (data plane).
// Uses distinct uuid != slug so a regression (passing the slug) is caught.
func TestHandleBulkDisconnect_AuditLogUsesTenantUUID(t *testing.T) {
	t.Parallel()

	svc := &mockConnService{}
	h := newTestConnectionsHandler()
	h.reader = &fakeConnReader{} // no matching connections; the audit call still fires
	h.service = svc

	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections?channel=trades.*", http.NoBody)
	req = withTenantIdentity(req, "uuid-xyz", "acme")
	rr := httptest.NewRecorder()

	h.HandleBulkDisconnect(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", rr.Code, rr.Body.String())
	}
	if !svc.auditCalled {
		t.Fatal("AuditLog was not called")
	}
	if svc.auditTenantID != "uuid-xyz" {
		t.Errorf("AuditLog tenant = %q, want UUID %q (slug is %q)", svc.auditTenantID, "uuid-xyz", "acme")
	}
	if svc.auditAction != provisioning.ActionBulkDisconnect {
		t.Errorf("AuditLog action = %q, want %q", svc.auditAction, provisioning.ActionBulkDisconnect)
	}
}

// TestHandleDeleteConnection_AuditLogUsesTenantUUID verifies the single force-disconnect
// path audits with the tenant UUID, not the slug. The dead-pod branch (publishDisconnect
// returns 0 subscribers) reaches the audit call.
func TestHandleDeleteConnection_AuditLogUsesTenantUUID(t *testing.T) {
	t.Parallel()

	svc := &mockConnService{}
	h := newTestConnectionsHandler()
	h.reader = &fakeConnReader{getResult: &ConnectionDetail{PodID: "pod-1"}}
	h.service = svc

	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections/conn-1", http.NoBody)
	req = withTenantIdentity(req, "uuid-xyz", "acme")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("connId", "conn-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()

	h.HandleDeleteConnection(rr, req)

	if !svc.auditCalled {
		t.Fatal("AuditLog was not called")
	}
	if svc.auditTenantID != "uuid-xyz" {
		t.Errorf("AuditLog tenant = %q, want UUID %q (slug is %q)", svc.auditTenantID, "uuid-xyz", "acme")
	}
	if svc.auditAction != provisioning.ActionForceDisconnect {
		t.Errorf("AuditLog action = %q, want %q", svc.auditAction, provisioning.ActionForceDisconnect)
	}
}

// TestHandleAdminListConnections_CrossTenantUsesSlug verifies the cross-tenant admin
// listing queries the connection registry by tenant SLUG, not the tenant UUID. The
// registry is slug-keyed (data plane), so querying by UUID silently returns zero
// connections for every tenant. Uses slug != uuid to catch a regression.
func TestHandleAdminListConnections_CrossTenantUsesSlug(t *testing.T) {
	t.Parallel()

	reader := &fakeConnReader{listResult: []ConnectionDetail{{ConnectionID: "c1", PodID: "pod-1"}}}
	svc := &mockConnService{tenants: []*provisioning.Tenant{{ID: "uuid-1", Slug: "acme"}}}
	h := newTestConnectionsHandler()
	h.reader = reader
	h.service = svc

	// No tenant_id query param → cross-tenant enumeration path. Admin route: no RequireTenant.
	req := httptest.NewRequest(http.MethodGet, "/admin/connections", http.NoBody)
	rr := httptest.NewRecorder()

	h.HandleAdminListConnections(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if len(reader.listKeys) != 1 {
		t.Fatalf("listTenantConnections called %d times, want 1", len(reader.listKeys))
	}
	if reader.listKeys[0] != "acme" {
		t.Errorf("registry queried with key %q, want slug %q (not UUID %q)", reader.listKeys[0], "acme", "uuid-1")
	}
}

// TestHandleDeleteConnection_MissingUUIDFailsClosed verifies the fail-closed guard: when
// the caller has a validated slug (claims) but no stashed tenant UUID (e.g. a path that
// bypassed RequireTenant's stash), the handler returns 401 rather than writing an audit
// entry with an empty tenant_id. Mirrors the webhook handlers, which guard the UUID they consume.
func TestHandleDeleteConnection_MissingUUIDFailsClosed(t *testing.T) {
	t.Parallel()

	svc := &mockConnService{}
	h := newTestConnectionsHandler()
	h.reader = &fakeConnReader{getResult: &ConnectionDetail{PodID: "pod-1"}}
	h.service = svc

	req := httptest.NewRequest(http.MethodDelete, "/tenants/acme/connections/conn-1", http.NoBody)
	// Claims set (slug present) but NO stashed UUID.
	req = req.WithContext(auth.WithClaims(req.Context(), &auth.Claims{TenantID: "acme"}))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("connId", "conn-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()

	h.HandleDeleteConnection(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body: %s", rr.Code, rr.Body.String())
	}
	if svc.auditCalled {
		t.Error("AuditLog must not be called when the tenant UUID is missing (fail closed)")
	}
}
