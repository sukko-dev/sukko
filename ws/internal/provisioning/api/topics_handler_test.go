package api_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/api"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// topicsTestEnv wires a router over mock stores for the topics API, exposing the
// stores so tests can seed state and auth helpers for both an admin and a
// non-admin (member) role — so the RequireRole gate on writes is exercised.
type topicsTestEnv struct {
	router     http.Handler
	adminAuth  func(*http.Request)
	memberAuth func(*http.Request)
	tenant     *provisioning.Tenant
	topicStore *testutil.MockTopicStore
	routing    *testutil.MockRoutingRulesStore
}

func newTopicsTestEnv(t *testing.T, maxTopics int) *topicsTestEnv {
	t.Helper()

	tenant := testutil.NewTestTenant("test-tenant")
	ts := testutil.NewMockTenantStore()
	_ = ts.Create(context.Background(), tenant)
	topicStore := testutil.NewMockTopicStore()
	routing := testutil.NewMockRoutingRulesStore()

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 ts,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           routing,
		TopicStore:                  topicStore,
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          maxTopics,
		MaxRoutingRulesPerTenant:    100,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  604800000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	validator, privateKey := newTestValidator(t)
	router, err := api.NewRouter(api.RouterConfig{
		Service:        svc,
		Logger:         zerolog.Nop(),
		Validator:      validator,
		EditionManager: license.NewTestManager(license.Enterprise),
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	return &topicsTestEnv{
		router:     router,
		adminAuth:  roleAuth(t, privateKey, "admin"),
		memberAuth: roleAuth(t, privateKey, "member"),
		tenant:     tenant,
		topicStore: topicStore,
		routing:    routing,
	}
}

// roleAuth returns a request decorator that signs a tenant JWT with the given role.
func roleAuth(t *testing.T, privateKey *ecdsa.PrivateKey, role string) func(*http.Request) {
	t.Helper()
	return func(req *http.Request) {
		claims := &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   "test-user",
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
			TenantID: "test-tenant",
			Roles:    []string{role},
		}
		req.Header.Set("Authorization", "Bearer "+createTestToken(t, privateKey, claims))
	}
}

func (e *topicsTestEnv) do(t *testing.T, method, path string, body any, withAuth func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	var r *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, r)
	req.Header.Set("Content-Type", "application/json")
	withAuth(req)
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	return rec
}

func TestRouter_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		suffix     string
		wantStatus int
		wantCode   string
	}{
		{name: "valid — 201", suffix: "analytics", wantStatus: http.StatusCreated},
		{name: "reserved default — 400", suffix: "default", wantStatus: http.StatusBadRequest, wantCode: "RESERVED_TOPIC_SUFFIX"},
		{name: "reserved dead-letter — 400", suffix: "dead-letter", wantStatus: http.StatusBadRequest, wantCode: "RESERVED_TOPIC_SUFFIX"},
		{name: "empty — 400", suffix: "", wantStatus: http.StatusBadRequest, wantCode: "INVALID_TOPIC_SUFFIX"},
		{name: "uppercase — 400", suffix: "Analytics", wantStatus: http.StatusBadRequest, wantCode: "INVALID_TOPIC_SUFFIX"},
		{name: "dotted — 400", suffix: "a.b", wantStatus: http.StatusBadRequest, wantCode: "INVALID_TOPIC_SUFFIX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := newTopicsTestEnv(t, 50)
			rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
				map[string]any{"suffix": tt.suffix}, env.adminAuth)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				if got := decodeErrorCode(t, rec.Body.Bytes()); got != tt.wantCode {
					t.Errorf("code = %q, want %q; body: %s", got, tt.wantCode, rec.Body.String())
				}
			}
		})
	}
}

func TestRouter_CreateTopic_Duplicate(t *testing.T) {
	t.Parallel()
	env := newTopicsTestEnv(t, 50)

	if rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
		map[string]any{"suffix": "analytics"}, env.adminAuth); rec.Code != http.StatusCreated {
		t.Fatalf("first create status = %d; body: %s", rec.Code, rec.Body.String())
	}
	rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
		map[string]any{"suffix": "analytics"}, env.adminAuth)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, want 409; body: %s", rec.Code, rec.Body.String())
	}
	if got := decodeErrorCode(t, rec.Body.Bytes()); got != "TOPIC_ALREADY_EXISTS" {
		t.Errorf("code = %q, want TOPIC_ALREADY_EXISTS", got)
	}
}

func TestRouter_CreateTopic_QuotaExceeded(t *testing.T) {
	t.Parallel()
	env := newTopicsTestEnv(t, 1) // quota of 1

	if rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
		map[string]any{"suffix": "first"}, env.adminAuth); rec.Code != http.StatusCreated {
		t.Fatalf("first create status = %d; body: %s", rec.Code, rec.Body.String())
	}
	rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
		map[string]any{"suffix": "second"}, env.adminAuth)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("quota status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
	if got := decodeErrorCode(t, rec.Body.Bytes()); got != "TOO_MANY_TOPICS" {
		t.Errorf("code = %q, want TOO_MANY_TOPICS", got)
	}
}

func TestRouter_CreateTopic_RequiresAdmin(t *testing.T) {
	t.Parallel()
	env := newTopicsTestEnv(t, 50)
	rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
		map[string]any{"suffix": "analytics"}, env.memberAuth)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("member create status = %d, want 403; body: %s", rec.Code, rec.Body.String())
	}
}

func TestRouter_ListTopics(t *testing.T) {
	t.Parallel()
	env := newTopicsTestEnv(t, 50)
	for _, s := range []string{"analytics", "trade"} {
		if rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
			map[string]any{"suffix": s}, env.adminAuth); rec.Code != http.StatusCreated {
			t.Fatalf("seed create %q status = %d", s, rec.Code)
		}
	}

	rec := env.do(t, http.MethodGet, "/api/v1/tenants/test-tenant/topics", nil, env.memberAuth)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []struct {
			Suffix string `json:"suffix"`
		} `json:"items"`
		Total int `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// default + analytics + trade = 3, default first (List orders stored by suffix).
	if resp.Total != 3 || len(resp.Items) != 3 || resp.Items[0].Suffix != "default" {
		t.Fatalf("items = %+v, total = %d, want [default, analytics, trade]", resp.Items, resp.Total)
	}
}

func TestRouter_ListTopics_OffsetPastEnd(t *testing.T) {
	t.Parallel()
	env := newTopicsTestEnv(t, 50)

	rec := env.do(t, http.MethodGet, "/api/v1/tenants/test-tenant/topics?offset=100&limit=10", nil, env.adminAuth)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []json.RawMessage `json:"items"`
		Total int               `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Only the synthetic default exists; offset past end → empty items, total unchanged.
	if len(resp.Items) != 0 || resp.Total != 1 {
		t.Fatalf("items = %d, total = %d, want 0 items, total 1", len(resp.Items), resp.Total)
	}
}

func TestRouter_DeleteTopic(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		env := newTopicsTestEnv(t, 50)
		if rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
			map[string]any{"suffix": "analytics"}, env.adminAuth); rec.Code != http.StatusCreated {
			t.Fatalf("seed status = %d", rec.Code)
		}
		rec := env.do(t, http.MethodDelete, "/api/v1/tenants/test-tenant/topics/analytics", nil, env.adminAuth)
		if rec.Code != http.StatusOK {
			t.Fatalf("delete status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("reserved — 400", func(t *testing.T) {
		t.Parallel()
		env := newTopicsTestEnv(t, 50)
		rec := env.do(t, http.MethodDelete, "/api/v1/tenants/test-tenant/topics/default", nil, env.adminAuth)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
		}
		if got := decodeErrorCode(t, rec.Body.Bytes()); got != "RESERVED_TOPIC_SUFFIX" {
			t.Errorf("code = %q, want RESERVED_TOPIC_SUFFIX", got)
		}
	})

	t.Run("not found — 404", func(t *testing.T) {
		t.Parallel()
		env := newTopicsTestEnv(t, 50)
		rec := env.do(t, http.MethodDelete, "/api/v1/tenants/test-tenant/topics/ghost", nil, env.adminAuth)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body: %s", rec.Code, rec.Body.String())
		}
		if got := decodeErrorCode(t, rec.Body.Bytes()); got != "TOPIC_NOT_FOUND" {
			t.Errorf("code = %q, want TOPIC_NOT_FOUND", got)
		}
	})

	t.Run("referenced by rule — 409", func(t *testing.T) {
		t.Parallel()
		env := newTopicsTestEnv(t, 50)
		if rec := env.do(t, http.MethodPost, "/api/v1/tenants/test-tenant/topics",
			map[string]any{"suffix": "trade"}, env.adminAuth); rec.Code != http.StatusCreated {
			t.Fatalf("seed status = %d", rec.Code)
		}
		_ = env.routing.Add(context.Background(), env.tenant.ID, provisioning.TopicRoutingRule{
			Pattern: "**.trade", IngressTopic: "trade", Priority: 1,
		})
		rec := env.do(t, http.MethodDelete, "/api/v1/tenants/test-tenant/topics/trade", nil, env.adminAuth)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409; body: %s", rec.Code, rec.Body.String())
		}
		if got := decodeErrorCode(t, rec.Body.Bytes()); got != "TOPIC_REFERENCED_BY_RULE" {
			t.Errorf("code = %q, want TOPIC_REFERENCED_BY_RULE", got)
		}
	})

	t.Run("requires admin — 403", func(t *testing.T) {
		t.Parallel()
		env := newTopicsTestEnv(t, 50)
		rec := env.do(t, http.MethodDelete, "/api/v1/tenants/test-tenant/topics/analytics", nil, env.memberAuth)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403; body: %s", rec.Code, rec.Body.String())
		}
	})
}

// TestRouter_Topics_StoreError_500 covers the store-error fallback branches:
// each write/read path maps an unexpected TopicStore error to its 500 wire code
// via classifyServiceError. Exercises the MockTopicStore error-injection fields.
func TestRouter_Topics_StoreError_500(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     any
		inject   func(*testutil.MockTopicStore)
		wantCode string
	}{
		{
			name:     "create — count fails",
			method:   http.MethodPost,
			path:     "/api/v1/tenants/test-tenant/topics",
			body:     map[string]any{"suffix": "analytics"},
			inject:   func(m *testutil.MockTopicStore) { m.CountErr = errBoom },
			wantCode: "CREATE_TOPIC_FAILED",
		},
		{
			name:     "create — insert fails",
			method:   http.MethodPost,
			path:     "/api/v1/tenants/test-tenant/topics",
			body:     map[string]any{"suffix": "analytics"},
			inject:   func(m *testutil.MockTopicStore) { m.CreateErr = errBoom },
			wantCode: "CREATE_TOPIC_FAILED",
		},
		{
			name:     "list — query fails",
			method:   http.MethodGet,
			path:     "/api/v1/tenants/test-tenant/topics",
			inject:   func(m *testutil.MockTopicStore) { m.ListErr = errBoom },
			wantCode: "LIST_TOPICS_FAILED",
		},
		{
			name:     "delete — exec fails",
			method:   http.MethodDelete,
			path:     "/api/v1/tenants/test-tenant/topics/analytics",
			inject:   func(m *testutil.MockTopicStore) { m.DeleteErr = errBoom },
			wantCode: "DELETE_TOPIC_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := newTopicsTestEnv(t, 50)
			tt.inject(env.topicStore)
			rec := env.do(t, tt.method, tt.path, tt.body, env.adminAuth)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body: %s", rec.Code, rec.Body.String())
			}
			if got := decodeErrorCode(t, rec.Body.Bytes()); got != tt.wantCode {
				t.Errorf("code = %q, want %q", got, tt.wantCode)
			}
		})
	}
}

var errBoom = errors.New("boom")
