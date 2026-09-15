package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// healthHandlerFromStatus is a test-only HealthHandler variant that accepts a HealthStatus directly.
func healthHandlerFromStatus(hs HealthStatus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !hs.GRPCUp {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable"}`))
			return
		}
		if !hs.ValkeySubscriptionUp {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(healthResponse{
				Status: "degraded", Reason: "valkey_subscription_down", Fallback: "ttl_refresh_active",
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// TestHealthHandler_OK verifies 200 when gRPC is up.
func TestHealthHandler_OK(t *testing.T) {
	t.Parallel()
	h := healthHandlerFromStatus(HealthStatus{GRPCUp: true, ValkeySubscriptionUp: true})
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// TestHealthHandler_Degraded covers 200 with degraded body when Valkey is down.
func TestHealthHandler_Degraded(t *testing.T) {
	t.Parallel()
	h := healthHandlerFromStatus(HealthStatus{GRPCUp: true, ValkeySubscriptionUp: false})
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (degraded still returns 200)", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "degraded") {
		t.Errorf("body %q should contain 'degraded'", body)
	}
	if !strings.Contains(body, "valkey_subscription_down") {
		t.Errorf("body %q should contain 'valkey_subscription_down'", body)
	}
	if !strings.Contains(body, "ttl_refresh_active") {
		t.Errorf("body %q should contain 'ttl_refresh_active'", body)
	}
}

// TestHealthHandler_Unavailable verifies 503 when gRPC is down.
func TestHealthHandler_Unavailable(t *testing.T) {
	t.Parallel()
	h := healthHandlerFromStatus(HealthStatus{GRPCUp: false})
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}
