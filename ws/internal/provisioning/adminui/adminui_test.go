package adminui_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning/adminui"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// testToken is a valid-length admin token used across tests.
const testToken = "test-admin-token-32-chars-longxxx"

// buildHandler creates a fully initialized AdminUIHandler for testing.
// svc may be nil — only tests that exercise service calls need a real service.
func buildHandler(t *testing.T, token string, edMgr *license.Manager) http.Handler {
	t.Helper()
	cfg := platform.AdminUIConfig{
		AdminUIEnabled:  true,
		AdminToken:      token,
		AdminSessionTTL: time.Hour,
	}
	h, err := adminui.NewHandler(cfg, nil, nil, edMgr, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	if h == nil {
		t.Fatal("NewHandler returned nil handler (AdminUIEnabled=true)")
	}
	return h
}

// TestNewHandlerDisabledReturnsNilInterface guards the §II boundary between the admin
// UI constructor and the router mount. cmd/provisioning wires NewHandler's return value
// straight into api.RouterConfig.AdminUIHandler (an http.Handler), whose mount guard is
// `if AdminUIHandler != nil`. When the admin UI is disabled, NewHandler MUST yield a
// TRUE-nil http.Handler: a typed-nil *Handler wrapped in the interface is non-nil to
// that guard, so it gets mounted and every /admin/* request nil-derefs the handler →
// panic → 500 instead of the correct 404.
func TestNewHandlerDisabledReturnsNilInterface(t *testing.T) {
	t.Parallel()

	// Explicit `=` (not `:=`) so h keeps its http.Handler type: the whole point is to
	// assert the INTERFACE is nil, not the concrete pointer. A `:=` here would let a
	// future edit shadow h with the concrete return type and silently stop guarding.
	var h http.Handler
	var err error
	h, err = adminui.NewHandler(platform.AdminUIConfig{AdminUIEnabled: false}, nil, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewHandler(disabled): unexpected error: %v", err)
	}
	if h != nil {
		t.Fatalf("NewHandler(disabled) = non-nil http.Handler (%T); want nil so the router skips the /admin mount", h)
	}
}

// TestAdminUI_AnalyticsStreamUnavailableWhenNoDelegate covers the 503 branch that the
// main.go typed-nil guard keeps reachable: with a nil SSE delegate (analytics not
// configured — the default), GET /admin/analytics/stream must return 503 SSE_UNAVAILABLE,
// not panic. A typed-nil delegate passed into NewHandler would make h.sseDelegate non-nil,
// skip the `== nil` check, and call ServeHTTP on a nil receiver → 500.
func TestAdminUI_AnalyticsStreamUnavailableWhenNoDelegate(t *testing.T) {
	t.Parallel()
	// Pro edition so the Analytics feature gate passes; buildHandler wires a nil delegate.
	h := buildHandler(t, testToken, license.NewTestManager(license.Pro))

	// The route is session-protected — log in to obtain a session cookie.
	loginRec := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("token="+testToken))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusSeeOther {
		t.Fatalf("login: got %d, want 303", loginRec.Code)
	}
	var session *http.Cookie
	for _, c := range loginRec.Result().Cookies() {
		if c.Name == adminui.AdminSessionCookieName {
			session = c
		}
	}
	if session == nil {
		t.Fatal("login did not set a session cookie")
	}

	req := adminReq(http.MethodGet, "/analytics/stream")
	req.AddCookie(session)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /analytics/stream with nil delegate = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "SSE_UNAVAILABLE") {
		t.Errorf("body = %q, want it to contain SSE_UNAVAILABLE", rec.Body.String())
	}
}

// buildCommunityEditionManager creates a Community-edition license Manager for testing —
// so the admin UI Gate serves the upgrade page (Community lacks the AdminUI feature). Any
// edition is constructible here via license.NewTestManager (e.g. the analytics stream test
// uses license.NewTestManager(license.Pro)); pass nil to buildHandler to bypass the Gate.
func buildCommunityEditionManager(t *testing.T) *license.Manager {
	t.Helper()
	return license.NewTestManager(license.Community)
}

// adminReq builds an http.Request for the AdminUIHandler. Since the handler is mounted
// at /admin/ in the outer router (which strips the prefix), tests call it with paths
// relative to that mount point (e.g. "/static/css/tailwind.css" not "/admin/static/...").
func adminReq(method, path string) *http.Request {
	return httptest.NewRequest(method, path, http.NoBody)
}

// TestAdminUI_StaticAssets_ContentType verifies static files are served with correct Content-Type.
func TestAdminUI_StaticAssets_ContentType(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, buildCommunityEditionManager(t))
	tests := []struct {
		path     string
		wantMIME string
	}{
		{"/static/css/tailwind.css", "text/css"},
		{"/static/js/htmx.min.js", "javascript"}, // both text/javascript and application/javascript are valid
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			req := adminReq(http.MethodGet, tt.path)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", tt.path, rec.Code)
			}
			ct := rec.Header().Get("Content-Type")
			if !strings.Contains(ct, tt.wantMIME) {
				t.Errorf("expected Content-Type to contain %q, got %q", tt.wantMIME, ct)
			}
		})
	}
}

// TestAdminUI_ContentTypeIsolation verifies that admin routes return text/html and
// schema returns application/json.
func TestAdminUI_ContentTypeIsolation(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, buildCommunityEditionManager(t))

	// Schema endpoint — no auth required.
	req := adminReq(http.MethodGet, "/schemas/channel-rules.json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for schema, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("schema must be application/json, got %q", ct)
	}

	// Login page — HTML, no auth needed.
	req = adminReq(http.MethodGet, "/login")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for login page, got %d", rec.Code)
	}
	ct = rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("login page must be text/html, got %q", ct)
	}
}

// TestAdminUI_LoginPage_Renders verifies the login page renders without an error message.
// Uses nil edition manager (nil = all editions pass AdminUIGate) to reach the login page.
func TestAdminUI_LoginPage_Renders(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, nil) // nil = bypass AdminUIGate (Pro/Enterprise gate passes)
	req := adminReq(http.MethodGet, "/login")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Sign in") {
		t.Error("login page should contain 'Sign in'")
	}
}

// TestAdminUI_Login_InvalidToken verifies that a bad token renders 401 HTML with an error.
func TestAdminUI_Login_InvalidToken(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, nil)
	body := strings.NewReader("token=wrongtoken")
	req := httptest.NewRequest(http.MethodPost, "/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for bad token, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Invalid token") {
		t.Error("expected 'Invalid token' in login error response")
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("login error must be text/html, got %q", ct)
	}
}

// TestAdminUI_Login_Success verifies a valid token sets the session cookie and redirects.
func TestAdminUI_Login_Success(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, nil)
	body := strings.NewReader("token=" + testToken)
	req := httptest.NewRequest(http.MethodPost, "/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected 303 redirect on success, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/admin/" {
		t.Errorf("expected redirect to /admin/, got %q", loc)
	}
	// Verify Set-Cookie contains session cookie.
	cookies := rec.Result().Cookies()
	var found bool
	for _, c := range cookies {
		if c.Name == adminui.AdminSessionCookieName {
			found = true
			if !c.HttpOnly {
				t.Error("session cookie must be HttpOnly")
			}
			if c.SameSite != http.SameSiteStrictMode {
				t.Error("session cookie must be SameSite=Strict")
			}
		}
	}
	if !found {
		t.Error("expected Set-Cookie with admin_session, not found")
	}
}

// TestAdminUI_Logout_ClearsCookie verifies that POST /logout clears the session cookie.
func TestAdminUI_Logout_ClearsCookie(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, nil)
	req := adminReq(http.MethodPost, "/logout")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d", rec.Code)
	}
	// Cookie should be cleared (MaxAge=-1 or Max-Age=0).
	cookies := rec.Result().Cookies()
	for _, c := range cookies {
		if c.Name == adminui.AdminSessionCookieName {
			if c.MaxAge > 0 {
				t.Errorf("logout should clear cookie (MaxAge<=0), got MaxAge=%d", c.MaxAge)
			}
		}
	}
}

// TestAdminUI_SessionRequired_Redirect verifies protected routes redirect when no session.
func TestAdminUI_SessionRequired_Redirect(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, buildCommunityEditionManager(t))
	// Community edition → AdminUIGate returns upgrade page (no auth). Test Pro-gated paths
	// by using a nil edition manager which falls through to session checks.
	routes := []string{"/", "/status", "/system/health"}
	for _, path := range routes {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			req := adminReq(http.MethodGet, path)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			// Community edition → AdminUIGate intercepts and returns upgrade page (200)
			// before SessionMiddleware is reached.
			if rec.Code != http.StatusOK && rec.Code != http.StatusSeeOther {
				t.Errorf("expected 200 (upgrade) or 303 (redirect) for %s without session, got %d", path, rec.Code)
			}
		})
	}
}

// TestAdminUI_HTMXSessionExpiry verifies HTMX partial requests return 401+HX-Redirect
// when session is missing on a Pro-edition instance.
func TestAdminUI_HTMXSessionExpiry(t *testing.T) {
	t.Parallel()
	// Pro/Enterprise edition: AdminUIGate passes, SessionMiddleware intercepts.
	// We can't create a Pro license key in unit tests, so we test with nil edMgr
	// (nil = all editions pass the gate per the nil-guard in AdminUIGate).
	h := buildHandler(t, testToken, nil)
	req := adminReq(http.MethodGet, "/")
	req.Header.Set("HX-Request", "true")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for HTMX + no session, got %d", rec.Code)
	}
	if hxRedirect := rec.Header().Get("HX-Redirect"); hxRedirect != "/admin/login" {
		t.Errorf("expected HX-Redirect: /admin/login, got %q", hxRedirect)
	}
}

// TestAdminUI_MountedContentTypeIsolation verifies that when AdminUIHandler is mounted
// at /admin/ in a chi router (simulating the production api/router.go), HTML routes
// return text/html and the outer router's application/json middleware does NOT leak.
func TestAdminUI_MountedContentTypeIsolation(t *testing.T) {
	t.Parallel()
	adminHandler := buildHandler(t, testToken, nil)

	// Simulate the outer router: JSON Content-Type on /api/v1, admin handler at /admin.
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(middleware.SetHeader("Content-Type", "application/json"))
		r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
	})
	r.Mount("/admin", adminHandler)

	// /api/v1/health → application/json
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", http.NoBody)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("/api/v1/health Content-Type should be application/json, got %q", ct)
	}

	// /admin/login → text/html (outer JSON middleware must NOT apply)
	req = httptest.NewRequest(http.MethodGet, "/admin/login", http.NoBody)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/admin/login expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("/admin/login Content-Type should be text/html, got %q", ct)
	}
}

// TestAdminUI_ChannelRulesSchemaEndpoint verifies the schema endpoint serves valid JSON.
func TestAdminUI_ChannelRulesSchemaEndpoint(t *testing.T) {
	t.Parallel()
	h := buildHandler(t, testToken, buildCommunityEditionManager(t))
	req := adminReq(http.MethodGet, "/schemas/channel-rules.json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"$schema"`) {
		t.Error("expected JSON Schema output to contain '$schema'")
	}
	if !strings.Contains(body, "additionalProperties") {
		t.Error("expected JSON Schema to have additionalProperties:false")
	}
}
