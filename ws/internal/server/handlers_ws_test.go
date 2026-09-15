package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/alerting"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// =============================================================================
// getClientIP Tests
// =============================================================================

func TestGetClientIP_XForwardedFor_SingleIP(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Forwarded-For", "192.168.1.100")
	req.RemoteAddr = "10.0.0.1:12345"

	ip := httputil.GetClientIP(req)

	if ip != "192.168.1.100" {
		t.Errorf("getClientIP: got %q, want %q", ip, "192.168.1.100")
	}
}

func TestGetClientIP_XForwardedFor_MultipleIPs(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	// Multiple IPs in chain: client -> proxy1 -> proxy2
	req.Header.Set("X-Forwarded-For", "203.0.113.50, 198.51.100.178, 192.0.2.1")
	req.RemoteAddr = "10.0.0.1:12345"

	ip := httputil.GetClientIP(req)

	// Should return first IP (original client)
	if ip != "203.0.113.50" {
		t.Errorf("getClientIP: got %q, want %q", ip, "203.0.113.50")
	}
}

func TestGetClientIP_XForwardedFor_WithSpaces(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Forwarded-For", "  192.168.1.100  ")
	req.RemoteAddr = "10.0.0.1:12345"

	ip := httputil.GetClientIP(req)

	// Should trim spaces
	if ip != "192.168.1.100" {
		t.Errorf("getClientIP: got %q, want %q", ip, "192.168.1.100")
	}
}

func TestGetClientIP_NoForwardedHeader_WithPort(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "192.168.1.100:54321"

	ip := httputil.GetClientIP(req)

	// Should extract IP from RemoteAddr
	if ip != "192.168.1.100" {
		t.Errorf("getClientIP: got %q, want %q", ip, "192.168.1.100")
	}
}

func TestGetClientIP_NoForwardedHeader_IPv6WithPort(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "[::1]:54321"

	ip := httputil.GetClientIP(req)

	// Should extract IPv6 IP from RemoteAddr
	if ip != "::1" {
		t.Errorf("getClientIP: got %q, want %q", ip, "::1")
	}
}

func TestGetClientIP_NoForwardedHeader_JustIP(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "192.168.1.100" // No port (unusual but possible)

	ip := httputil.GetClientIP(req)

	// Should return as-is when no port
	if ip != "192.168.1.100" {
		t.Errorf("getClientIP: got %q, want %q", ip, "192.168.1.100")
	}
}

func TestGetClientIP_Localhost(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "127.0.0.1:12345"

	ip := httputil.GetClientIP(req)

	if ip != "127.0.0.1" {
		t.Errorf("getClientIP: got %q, want %q", ip, "127.0.0.1")
	}
}

func TestGetClientIP_LocalhostIPv6(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "[::1]:12345"

	ip := httputil.GetClientIP(req)

	if ip != "::1" {
		t.Errorf("getClientIP: got %q, want %q", ip, "::1")
	}
}

func TestGetClientIP_XForwardedFor_Empty(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Forwarded-For", "")
	req.RemoteAddr = "10.0.0.5:9999"

	ip := httputil.GetClientIP(req)

	// Empty X-Forwarded-For should fall back to RemoteAddr
	if ip != "10.0.0.5" {
		t.Errorf("getClientIP: got %q, want %q", ip, "10.0.0.5")
	}
}

// =============================================================================
// Table-Driven Tests
// =============================================================================

func TestGetClientIP_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		xForwarded string
		remoteAddr string
		expectedIP string
	}{
		{
			name:       "X-Forwarded-For single IP",
			xForwarded: "192.168.1.100",
			remoteAddr: "10.0.0.1:12345",
			expectedIP: "192.168.1.100",
		},
		{
			name:       "X-Forwarded-For multiple IPs",
			xForwarded: "203.0.113.50, 198.51.100.178",
			remoteAddr: "10.0.0.1:12345",
			expectedIP: "203.0.113.50",
		},
		{
			name:       "No X-Forwarded-For with port",
			xForwarded: "",
			remoteAddr: "192.168.1.100:54321",
			expectedIP: "192.168.1.100",
		},
		{
			name:       "IPv6 with port",
			xForwarded: "",
			remoteAddr: "[2001:db8::1]:54321",
			expectedIP: "2001:db8::1",
		},
		{
			name:       "Localhost IPv4",
			xForwarded: "",
			remoteAddr: "127.0.0.1:12345",
			expectedIP: "127.0.0.1",
		},
		{
			name:       "Localhost IPv6",
			xForwarded: "",
			remoteAddr: "[::1]:12345",
			expectedIP: "::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody)
			if tt.xForwarded != "" {
				req.Header.Set("X-Forwarded-For", tt.xForwarded)
			}
			req.RemoteAddr = tt.remoteAddr

			ip := httputil.GetClientIP(req)

			if ip != tt.expectedIP {
				t.Errorf("getClientIP: got %q, want %q", ip, tt.expectedIP)
			}
		})
	}
}

// =============================================================================
// Identity Header Validation Tests (handlers_ws.go)
// =============================================================================

// newHandlerWSTestServer creates a minimal Server for testing handleWebSocket.
// ResourceGuard is configured with generous limits so it always accepts in tests.
func newHandlerWSTestServer(t *testing.T, cfg *platform.ServerConfig) *Server {
	t.Helper()
	if cfg == nil {
		cfg = newTestServerConfig()
	}
	params := Params{
		Config:         cfg,
		Addr:           ":0",
		MaxConnections: cfg.MaxConnections,
	}
	srv, err := NewServer(params, &alerting.NoopAlerter{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

// TestHandleWebSocket_MissingTenantID verifies that a missing X-Sukko-Tenant-ID
// header causes an immediate 400 rejection before any WebSocket upgrade attempt.
func TestHandleWebSocket_MissingTenantID(t *testing.T) {
	t.Parallel()
	srv := newHandlerWSTestServer(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	// Deliberately omit protocol.HeaderTenantID
	w := httptest.NewRecorder()

	srv.handleWebSocket(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if got := w.Body.String(); got == "" {
		t.Error("expected non-empty response body for missing tenant ID")
	}
}

// TestHandleWebSocket_InternalSecret_WrongSecret verifies that a mismatched
// X-Sukko-Internal-Secret is rejected with 401 when InternalSecretEnabled=true.
func TestHandleWebSocket_InternalSecret_WrongSecret(t *testing.T) {
	t.Parallel()
	cfg := newTestServerConfig()
	cfg.InternalSecretEnabled = true
	cfg.InternalSecret = "correct-secret"
	srv := newHandlerWSTestServer(t, cfg)

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set(protocol.HeaderTenantID, "acme")
	req.Header.Set(protocol.HeaderInternalSecret, "wrong-secret")
	w := httptest.NewRecorder()

	srv.handleWebSocket(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (wrong secret should be rejected)", w.Code, http.StatusUnauthorized)
	}
}

// TestHandleWebSocket_InternalSecret_EmptyWhenEnabled verifies that an absent
// X-Sukko-Internal-Secret is rejected with 401 when InternalSecretEnabled=true.
func TestHandleWebSocket_InternalSecret_EmptyWhenEnabled(t *testing.T) {
	t.Parallel()
	cfg := newTestServerConfig()
	cfg.InternalSecretEnabled = true
	cfg.InternalSecret = "correct-secret"
	srv := newHandlerWSTestServer(t, cfg)

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set(protocol.HeaderTenantID, "acme")
	// No internal secret header at all
	w := httptest.NewRecorder()

	srv.handleWebSocket(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (missing secret should be rejected)", w.Code, http.StatusUnauthorized)
	}
}

// TestHandleWebSocket_InternalSecret_DisabledIgnoresHeader verifies that the
// internal secret header is not checked when InternalSecretEnabled=false.
// A correct tenant header with any secret (or no secret) passes the identity checks.
func TestHandleWebSocket_InternalSecret_DisabledIgnoresHeader(t *testing.T) {
	t.Parallel()
	cfg := newTestServerConfig()
	cfg.InternalSecretEnabled = false
	srv := newHandlerWSTestServer(t, cfg)

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set(protocol.HeaderTenantID, "acme")
	req.Header.Set(protocol.HeaderInternalSecret, "any-value-ignored")
	w := httptest.NewRecorder()

	srv.handleWebSocket(w, req)

	// Identity checks pass; upgrade will fail (httptest.ResponseRecorder is not hijackable)
	// but we must NOT get 400 from missing tenant or 401 from secret mismatch.
	if w.Code == http.StatusUnauthorized {
		t.Errorf("status = 401: secret header was checked when InternalSecretEnabled=false")
	}
	if w.Body.String() == "missing tenant ID\n" {
		t.Error("got 'missing tenant ID' response — tenant header was not read")
	}
}

// TestHandleWebSocket_InternalSecret_CorrectSecret verifies that a correct
// X-Sukko-Internal-Secret passes the identity checks when InternalSecretEnabled=true.
func TestHandleWebSocket_InternalSecret_CorrectSecret(t *testing.T) {
	t.Parallel()
	cfg := newTestServerConfig()
	cfg.InternalSecretEnabled = true
	cfg.InternalSecret = "correct-secret"
	srv := newHandlerWSTestServer(t, cfg)

	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set(protocol.HeaderTenantID, "acme")
	req.Header.Set(protocol.HeaderInternalSecret, "correct-secret")
	w := httptest.NewRecorder()

	srv.handleWebSocket(w, req)

	// Must NOT get 400 "missing tenant ID" or 401 "unauthorized".
	// The upgrade will fail (not a real conn), but identity checks passed.
	if w.Code == http.StatusUnauthorized {
		t.Errorf("status = 401: correct secret was incorrectly rejected")
	}
	if w.Body.String() == "missing tenant ID\n" {
		t.Error("got 'missing tenant ID' response — tenant header was not read")
	}
}
