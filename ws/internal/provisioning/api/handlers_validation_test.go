package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning/api"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
)

// decodeErrorCode extracts the `code` field from an error response body.
func decodeErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var errResp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v; body: %s", err, string(body))
	}
	return errResp.Code
}

// TestRouter_CreateTenant_Validation pins that invalid tenant/key INPUT on
// POST /api/v1/tenants returns 400 with a specific code (never 500 CREATE_FAILED), and that
// valid input (with and without an embedded key) still returns 201. POST /tenants is
// admin-gated, so requests are authenticated via mustNewRouterWithAuth.
func TestRouter_CreateTenant_Validation(t *testing.T) {
	t.Parallel()

	validPEM, _ := generateTestECKey(t)

	tests := []struct {
		name       string
		reqBody    any
		wantStatus int
		wantCode   string
		wantKeyID  string // if set, the 201 response MUST carry a persisted key with this key_id
	}{
		{
			name:       "valid — 201",
			reqBody:    map[string]any{"slug": "acme-corp", "name": "Acme Corp"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "valid with public_key — 201 and key persisted",
			reqBody: map[string]any{"slug": "acme-corp", "name": "Acme Corp", "public_key": map[string]any{
				"key_id": "acme-key-1", "algorithm": "ES256", "public_key": validPEM,
			}},
			wantStatus: http.StatusCreated,
			wantKeyID:  "acme-key-1",
		},
		{name: "reserved slug — 400 SLUG_RESERVED", reqBody: map[string]any{"slug": "rename", "name": "n"}, wantStatus: http.StatusBadRequest, wantCode: "SLUG_RESERVED"},
		{name: "bad slug — 400 SLUG_INVALID", reqBody: map[string]any{"slug": "Invalid_Slug", "name": "n"}, wantStatus: http.StatusBadRequest, wantCode: "SLUG_INVALID"},
		{name: "empty slug — 400 SLUG_INVALID", reqBody: map[string]any{"slug": "", "name": "n"}, wantStatus: http.StatusBadRequest, wantCode: "SLUG_INVALID"},
		{name: "empty name — 400 NAME_INVALID", reqBody: map[string]any{"slug": "acme-corp", "name": ""}, wantStatus: http.StatusBadRequest, wantCode: "NAME_INVALID"},
		{name: "long name — 400 NAME_INVALID", reqBody: map[string]any{"slug": "acme-corp", "name": strings.Repeat("x", 257)}, wantStatus: http.StatusBadRequest, wantCode: "NAME_INVALID"},
		{name: "bad consumer type — 400 CONSUMER_TYPE_INVALID", reqBody: map[string]any{"slug": "acme-corp", "name": "n", "consumer_type": "exclusive"}, wantStatus: http.StatusBadRequest, wantCode: "CONSUMER_TYPE_INVALID"},
		{
			name: "bad key id — 400 KEY_INVALID",
			reqBody: map[string]any{"slug": "acme-corp", "name": "n", "public_key": map[string]any{
				"key_id": "Bad_Key", "algorithm": "ES256", "public_key": validPEM,
			}},
			wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID",
		},
		{
			name: "bad key algorithm — 400 KEY_INVALID",
			reqBody: map[string]any{"slug": "acme-corp", "name": "n", "public_key": map[string]any{
				"key_id": "acme-key-1", "algorithm": "ES512", "public_key": validPEM,
			}},
			wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID",
		},
		{
			name: "short key pem — 400 KEY_INVALID",
			reqBody: map[string]any{"slug": "acme-corp", "name": "n", "public_key": map[string]any{
				"key_id": "acme-key-1", "algorithm": "ES256", "public_key": "too-short",
			}},
			wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc := newTestServiceWithStores(testutil.NewMockTenantStore(), testutil.NewMockRoutingRulesStore())
			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{Service: svc, Logger: zerolog.Nop()})

			body, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
				"/api/v1/tenants", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				if got := decodeErrorCode(t, rec.Body.Bytes()); got != tt.wantCode {
					t.Errorf("code = %q, want %q; body: %s", got, tt.wantCode, rec.Body.String())
				}
			}
			if tt.wantKeyID != "" {
				// Verify the embedded key was actually persisted (not silently skipped).
				var resp struct {
					Key *struct {
						KeyID string `json:"key_id"`
					} `json:"key"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal create response: %v; body: %s", err, rec.Body.String())
				}
				if resp.Key == nil || resp.Key.KeyID != tt.wantKeyID {
					t.Errorf("expected persisted key %q in response, got %+v; body: %s", tt.wantKeyID, resp.Key, rec.Body.String())
				}
			}
		})
	}
}

// findLogLevel scans zerolog JSON output for the line whose message matches msg and returns
// its level field (the actual defect being fixed is log level, so it must be pinned).
func findLogLevel(t *testing.T, logs, msg string) string {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var e struct {
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		if e.Message == msg {
			return e.Level
		}
	}
	t.Fatalf("no log line with message %q found in: %s", msg, logs)
	return ""
}

// TestRouter_CreateTenant_LogLevel pins a client validation failure (400) MUST log at
// Warn, and a genuine internal failure (500) MUST log at Error — driven off the classified
// status. Also asserts the 500 fallback code, exercising the classifyServiceError default branch.
func TestRouter_CreateTenant_LogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*testutil.MockTenantStore)
		reqBody   any
		wantCode  string
		wantLevel string
	}{
		{
			name:      "client 400 logs warn",
			setup:     func(*testutil.MockTenantStore) {},
			reqBody:   map[string]any{"slug": "Invalid_Slug", "name": "n"},
			wantCode:  "SLUG_INVALID",
			wantLevel: "warn",
		},
		{
			name:      "internal 500 logs error",
			setup:     func(ts *testutil.MockTenantStore) { ts.CreateErr = errors.New("boom") },
			reqBody:   map[string]any{"slug": "acme-corp", "name": "n"},
			wantCode:  "CREATE_FAILED",
			wantLevel: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := testutil.NewMockTenantStore()
			tt.setup(ts)
			svc := newTestServiceWithStores(ts, testutil.NewMockRoutingRulesStore())

			var buf bytes.Buffer
			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{Service: svc, Logger: zerolog.New(&buf)})

			body, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
				"/api/v1/tenants", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if got := decodeErrorCode(t, rec.Body.Bytes()); got != tt.wantCode {
				t.Errorf("code = %q, want %q; body: %s", got, tt.wantCode, rec.Body.String())
			}
			if got := findLogLevel(t, buf.String(), "Failed to create tenant"); got != tt.wantLevel {
				t.Errorf("log level = %q, want %q; logs: %s", got, tt.wantLevel, buf.String())
			}
		})
	}
}

// TestRouter_UpdateTenant_Validation pins that invalid PATCH input returns 400 with a specific
// code (currently 500 UPDATE_FAILED). The seeding is a service-layer dependency: UpdateTenant
// calls GetBySlug first, so an active tenant matching the URL slug must exist or the request
// 404s before validation (the admin token bypasses RequireTenant's ownership check).
func TestRouter_UpdateTenant_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reqBody    any
		wantStatus int
		wantCode   string
	}{
		{name: "valid name — 200", reqBody: map[string]any{"name": "New Name"}, wantStatus: http.StatusOK},
		{name: "empty name — 400 NAME_INVALID", reqBody: map[string]any{"name": ""}, wantStatus: http.StatusBadRequest, wantCode: "NAME_INVALID"},
		{name: "long name — 400 NAME_INVALID", reqBody: map[string]any{"name": strings.Repeat("x", 257)}, wantStatus: http.StatusBadRequest, wantCode: "NAME_INVALID"},
		{name: "bad consumer type — 400 CONSUMER_TYPE_INVALID", reqBody: map[string]any{"consumer_type": "exclusive"}, wantStatus: http.StatusBadRequest, wantCode: "CONSUMER_TYPE_INVALID"},
		{name: "slug in body — 400 SLUG_NOT_PATCHABLE", reqBody: map[string]any{"slug": "new-slug"}, wantStatus: http.StatusBadRequest, wantCode: "SLUG_NOT_PATCHABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := testutil.NewMockTenantStore()
			_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			svc := newTestServiceWithStores(ts, testutil.NewMockRoutingRulesStore())
			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{Service: svc, Logger: zerolog.Nop()})

			body, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPatch,
				"/api/v1/tenants/test-tenant", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

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

// TestRouter_CreateKey_Validation pins that invalid key input on POST .../keys returns
// 400 KEY_INVALID (currently 500 CREATE_KEY_FAILED), and valid input returns 201.
func TestRouter_CreateKey_Validation(t *testing.T) {
	t.Parallel()

	validPEM, _ := generateTestECKey(t)

	tests := []struct {
		name       string
		reqBody    any
		wantStatus int
		wantCode   string
	}{
		{name: "valid — 201", reqBody: map[string]any{"key_id": "acme-key-1", "algorithm": "ES256", "public_key": validPEM}, wantStatus: http.StatusCreated},
		{name: "bad key id — 400 KEY_INVALID", reqBody: map[string]any{"key_id": "Bad_Key", "algorithm": "ES256", "public_key": validPEM}, wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID"},
		{name: "bad algorithm — 400 KEY_INVALID", reqBody: map[string]any{"key_id": "acme-key-1", "algorithm": "ES512", "public_key": validPEM}, wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID"},
		{name: "short pem — 400 KEY_INVALID", reqBody: map[string]any{"key_id": "acme-key-1", "algorithm": "ES256", "public_key": "short"}, wantStatus: http.StatusBadRequest, wantCode: "KEY_INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := testutil.NewMockTenantStore()
			_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			svc := newTestServiceWithStores(ts, testutil.NewMockRoutingRulesStore())
			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{Service: svc, Logger: zerolog.Nop()})

			body, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
				"/api/v1/tenants/test-tenant/keys", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

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
