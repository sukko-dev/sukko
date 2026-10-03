package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// testProvClient creates a ProvisioningClient with an ephemeral keypair for testing.
func testProvClient(t *testing.T, baseURL string) *ProvisioningClient {
	t.Helper()
	provider, _, err := NewEphemeralAuthProvider()
	if err != nil {
		t.Fatalf("NewEphemeralAuthProvider: %v", err)
	}
	return NewProvisioningClient(baseURL, provider, zerolog.Nop())
}

// requireAdminJWT validates that the request has a well-formed admin JWT.
func requireAdminJWT(t *testing.T, r *http.Request) {
	t.Helper()
	auth := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok {
		t.Errorf("auth header = %q, want Bearer <jwt>", auth)
		return
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Errorf("JWT should have 3 parts, got %d", len(parts))
	}
}

func TestProvisioningClient_CreateTenant(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants" {
			t.Errorf("path = %q, want /api/v1/tenants", r.URL.Path)
		}
		requireAdminJWT(t, r)
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}

		// Assert request uses "slug" field, not "id"
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if _, hasID := body["id"]; hasID {
			t.Error("request body must not contain 'id' field")
		}
		if body["slug"] != "test-t1" {
			t.Errorf("request slug = %q, want test-t1", body["slug"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"tenant":{"id":"550e8400-e29b-41d4-a716-446655440000","slug":"test-t1","name":"Test Tenant","status":"active"}}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.CreateTenant(context.Background(), "test-t1", "Test Tenant")
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
}

func TestProvisioningClient_DeleteTenant(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %q, want DELETE", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/tenants/test-t1") {
			t.Errorf("path = %q, want prefix /api/v1/tenants/test-t1", r.URL.Path)
		}
		if r.URL.Query().Get("force") != "true" {
			t.Error("expected force=true query parameter")
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.DeleteTenant(context.Background(), "test-t1")
	if err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
}

func TestProvisioningClient_RegisterKey(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/keys" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/keys", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	expires := time.Now().Add(24 * time.Hour)
	err := client.RegisterKey(context.Background(), "test-t1", RegisterKeyRequest{
		KeyID:     "tester-abcd1234",
		Algorithm: "ES256",
		PublicKey: "-----BEGIN PUBLIC KEY-----\ntest\n-----END PUBLIC KEY-----\n",
		ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatalf("RegisterKey: %v", err)
	}
}

func TestProvisioningClient_RevokeKey(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %q, want DELETE", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/keys/tester-abcd1234" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/keys/tester-abcd1234", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.RevokeKey(context.Background(), "test-t1", "tester-abcd1234")
	if err != nil {
		t.Fatalf("RevokeKey: %v", err)
	}
}

func TestProvisioningClient_HTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"INTERNAL","message":"boom"}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)

	err := client.CreateTenant(context.Background(), "t1", "T1")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("error = %q, want HTTP 500", err.Error())
	}
}

func TestProvisioningClient_GetTenant(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer my-jwt" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)

	// With correct JWT
	status, err := client.GetTenant(context.Background(), "t1", "my-jwt")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}

	// With wrong JWT
	status, err = client.GetTenant(context.Background(), "t1", "wrong-jwt")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", status)
	}
}

func TestProvisioningClient_SetChannelRules(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %q, want PUT", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/channel-rules" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/channel-rules", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}
		requireAdminJWT(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.SetChannelRules(context.Background(), "test-t1", map[string]any{
		"public": []string{"general.*"},
	})
	if err != nil {
		t.Fatalf("SetChannelRules: %v", err)
	}
}

func TestProvisioningClient_SetRoutingRules(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %q, want PUT", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/routing-rules" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/routing-rules", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.SetRoutingRules(context.Background(), "test-t1", []map[string]any{
		{"pattern": "**", "topics": []string{"test"}, "priority": 100},
	})
	if err != nil {
		t.Fatalf("SetRoutingRules: %v", err)
	}
}

func TestProvisioningClient_DeleteRoutingRules(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %q, want DELETE", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/routing-rules" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/routing-rules", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.DeleteRoutingRules(context.Background(), "test-t1")
	if err != nil {
		t.Fatalf("DeleteRoutingRules: %v", err)
	}
}

func TestProvisioningClient_DeleteRoutingRules_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	err := client.DeleteRoutingRules(context.Background(), "test-t1")
	if err != nil {
		t.Fatalf("DeleteRoutingRules with 404 should not error, got: %v", err)
	}
}

func TestProvisioningClient_DeleteRoutingRulesRaw(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		respStatus int
		respBody   string
		wantStatus int
		wantErr    bool
	}{
		{
			name:       "200 → status 200, no error (idempotent delete)",
			respStatus: http.StatusOK,
			wantStatus: http.StatusOK,
			wantErr:    false,
		},
		{
			name:       "403 INSUFFICIENT_ROLE → status surfaced with error",
			respStatus: http.StatusForbidden,
			respBody:   `{"code":"INSUFFICIENT_ROLE","message":"Required role: admin or system"}`,
			wantStatus: http.StatusForbidden,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("method = %q, want DELETE", r.Method)
				}
				if r.URL.Path != "/api/v1/tenants/test-t1/routing-rules" {
					t.Errorf("path = %q, want /api/v1/tenants/test-t1/routing-rules", r.URL.Path)
				}
				w.WriteHeader(tc.respStatus)
				if tc.respBody != "" {
					_, _ = w.Write([]byte(tc.respBody))
				}
			}))
			t.Cleanup(srv.Close)

			client := testProvClient(t, srv.URL)
			status, err := client.DeleteRoutingRulesRaw(context.Background(), "test-t1")
			if status != tc.wantStatus {
				t.Errorf("status = %d, want %d", status, tc.wantStatus)
			}
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisioningClient_CreateWebhook(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/webhooks" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/webhooks", r.URL.Path)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body["url"] != "https://receiver.example.com/hook" {
			t.Errorf("url = %v, want https://receiver.example.com/hook", body["url"])
		}
		if body["channel_pattern"] != "orders.*" {
			t.Errorf("channel_pattern = %v, want orders.*", body["channel_pattern"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = strings.NewReader(`{"id":"wh-abc123"}`).WriteTo(w)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	id, err := client.CreateWebhook(context.Background(), "test-t1",
		"https://receiver.example.com/hook", "orders.*", "secret-hex", 3)
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	if id != "wh-abc123" {
		t.Errorf("id = %q, want wh-abc123", id)
	}
}

func TestProvisioningClient_GetWebhookByID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/webhooks/wh-abc123" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/webhooks/wh-abc123", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = strings.NewReader(`{"id":"wh-abc123","status":"degraded"}`).WriteTo(w)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	status, err := client.GetWebhookByID(context.Background(), "test-t1", "wh-abc123")
	if err != nil {
		t.Fatalf("GetWebhookByID: %v", err)
	}
	if status != "degraded" {
		t.Errorf("status = %q, want degraded", status)
	}
}

func TestProvisioningClient_DeleteWebhook(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %q, want DELETE", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/webhooks/wh-abc123" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/webhooks/wh-abc123", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	if err := client.DeleteWebhook(context.Background(), "test-t1", "wh-abc123"); err != nil {
		t.Fatalf("DeleteWebhook: %v", err)
	}
}

func TestProvisioningClient_CreateWebhook_ErrorResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = strings.NewReader(`{"code":"INVALID_URL","message":"url is not reachable"}`).WriteTo(w)
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	_, err := client.CreateWebhook(context.Background(), "test-t1",
		"not-a-url", "orders.*", "secret", 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestProvisioningClient_TokenVariants asserts the token-parameterized raw methods send the
// caller-supplied bearer token (NOT the admin provider) and surface (status, err) — the header
// injection is a security-relevant behavior distinct from the admin/setHeaders path.
func TestProvisioningClient_TokenVariants(t *testing.T) {
	t.Parallel()

	const token = "user-role-token-xyz"
	const wantAuth = "Bearer " + token

	tests := []struct {
		name       string
		wantMethod string
		call       func(c *ProvisioningClient, ctx context.Context) (int, error)
	}{
		{
			name:       "GetRoutingRulesWithToken → GET",
			wantMethod: http.MethodGet,
			call: func(c *ProvisioningClient, ctx context.Context) (int, error) {
				return c.GetRoutingRulesWithToken(ctx, "test-t1", token)
			},
		},
		{
			name:       "SetRoutingRulesRawWithToken → PUT",
			wantMethod: http.MethodPut,
			call: func(c *ProvisioningClient, ctx context.Context) (int, error) {
				return c.SetRoutingRulesRawWithToken(ctx, "test-t1", token, []map[string]any{{"pattern": "**", "topics": []string{"default"}, "priority": 100}})
			},
		},
		{
			name:       "AddRoutingRuleRawWithToken → POST",
			wantMethod: http.MethodPost,
			call: func(c *ProvisioningClient, ctx context.Context) (int, error) {
				return c.AddRoutingRuleRawWithToken(ctx, "test-t1", token, map[string]any{"pattern": "a.**", "topics": []string{"default"}, "priority": 10})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.wantMethod {
					t.Errorf("method = %q, want %q", r.Method, tc.wantMethod)
				}
				if got := r.Header.Get("Authorization"); got != wantAuth {
					t.Errorf("Authorization = %q, want %q (must send caller token, not admin)", got, wantAuth)
				}
				if r.URL.Path != "/api/v1/tenants/test-t1/routing-rules" {
					t.Errorf("path = %q, want /api/v1/tenants/test-t1/routing-rules", r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)

			client := testProvClient(t, srv.URL)
			status, err := tc.call(client, context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if status != http.StatusOK {
				t.Errorf("status = %d, want 200", status)
			}
		})
	}
}

// TestProvisioningClient_RenameTenantRaw asserts the admin rename POSTs to
// /tenants/{slug}/rename with body {"slug":newSlug} and signs as admin (not a bearer token).
func TestProvisioningClient_RenameTenantRaw(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/rename" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/rename", r.URL.Path)
		}
		requireAdminJWT(t, r)
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body["slug"] != "renamed-t1" {
			t.Errorf("body slug = %q, want renamed-t1", body["slug"])
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"slug":"renamed-t1"}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	status, err := client.RenameTenantRaw(context.Background(), "test-t1", "renamed-t1")
	if err != nil {
		t.Fatalf("RenameTenantRaw: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
}

// TestProvisioningClient_RenameTenantRawWithToken asserts the token variant sends the CALLER's
// bearer token (NOT the admin key) — the exact guard the role-gate negative depends on — and
// surfaces (status, err) on a rejection.
func TestProvisioningClient_RenameTenantRawWithToken(t *testing.T) {
	t.Parallel()

	const token = "user-role-token-xyz"
	const wantAuth = "Bearer " + token

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/rename" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/rename", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("Authorization = %q, want %q (must send caller token, not admin)", got, wantAuth)
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"INSUFFICIENT_ROLE","message":"Required role: admin or system"}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	status, err := client.RenameTenantRawWithToken(context.Background(), "test-t1", token, "renamed-t1")
	if status != http.StatusForbidden {
		t.Errorf("status = %d, want 403", status)
	}
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
	if !strings.Contains(err.Error(), "INSUFFICIENT_ROLE") {
		t.Errorf("error = %q, want to contain INSUFFICIENT_ROLE", err.Error())
	}
}

// TestProvisioningClient_TestAccessRaw asserts test-access POSTs to /tenants/{slug}/test-access
// with the caller's bearer token and body {"groups":[...]}, and RETURNS the success body so the
// caller can parse allowed_patterns.
func TestProvisioningClient_TestAccessRaw(t *testing.T) {
	t.Parallel()

	const token = "tenant-token-abc"
	const wantAuth = "Bearer " + token
	const wantBody = `{"allowed_patterns":["room.vip","general.*"]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/tenants/test-t1/test-access" {
			t.Errorf("path = %q, want /api/v1/tenants/test-t1/test-access", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("Authorization = %q, want %q (must send caller token, not admin)", got, wantAuth)
		}
		var body struct {
			Groups []string `json:"groups"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if len(body.Groups) != 1 || body.Groups[0] != "vip" {
			t.Errorf("groups = %v, want [vip]", body.Groups)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(wantBody))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	status, body, err := client.TestAccessRaw(context.Background(), "test-t1", token, []string{"vip"})
	if err != nil {
		t.Fatalf("TestAccessRaw: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
	if string(body) != wantBody {
		t.Errorf("body = %q, want %q", string(body), wantBody)
	}
}

// TestProvisioningClient_TestAccessRaw_BadJSON asserts a 400 rejection surfaces the status and an
// error embedding the body (for extractErrorCode) with a nil body.
func TestProvisioningClient_TestAccessRaw_BadJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"INVALID_REQUEST","message":"Invalid JSON body"}`))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	status, body, err := client.TestAccessRaw(context.Background(), "test-t1", "tok", []string{"vip"})
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if body != nil {
		t.Errorf("body = %q, want nil on error", string(body))
	}
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "INVALID_REQUEST") {
		t.Errorf("error = %q, want to contain INVALID_REQUEST", err.Error())
	}
}

// TestProvisioningClient_GetRoutingRulesPage asserts the admin paged GET sends ?limit=N and
// returns the raw body.
func TestProvisioningClient_GetRoutingRulesPage(t *testing.T) {
	t.Parallel()

	const wantBody = `{"items":[],"total":0,"limit":200,"offset":0}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if got := r.URL.Query().Get("limit"); got != "200" {
			t.Errorf("limit query = %q, want 200", got)
		}
		requireAdminJWT(t, r)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(wantBody))
	}))
	t.Cleanup(srv.Close)

	client := testProvClient(t, srv.URL)
	body, err := client.GetRoutingRulesPage(context.Background(), "test-t1", 200)
	if err != nil {
		t.Fatalf("GetRoutingRulesPage: %v", err)
	}
	if string(body) != wantBody {
		t.Errorf("body = %q, want %q", string(body), wantBody)
	}
}
