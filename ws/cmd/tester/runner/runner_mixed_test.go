package runner

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestRunnerMixed_StaticAPIKey verifies that when TESTER_API_KEY (cfg.APIKey) is non-empty,
// the runner uses the static key for mixed-mode and does NOT call CreateAPIKey.
func TestRunnerMixed_StaticAPIKey(t *testing.T) { //nolint:paralleltest // uses shared mock server
	var createCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api-keys") {
			createCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"key_id":"dynamic-key-123"}`))
			return
		}
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api-keys/") {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Handle provisioning setup calls (tenant, keys, etc.)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tenants":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/keys"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	r := New(Config{
		GatewayURL:         srv.URL, // WS dial will fail — that's OK; we only test the API key lifecycle
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthMode:           AuthModeMixed,
		APIKey:             "static-pk-test123", // non-empty → no CreateAPIKey call
		AuthUpgradeTimeout: 5 * time.Second,
	}, zerolog.Nop())

	_, startErr := r.Start("test-mixed-static", TestConfig{
		Type:     TestLoad,
		AuthMode: AuthModeMixed,
		Duration: "50ms", // very short so test completes quickly
	})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	if n := createCount.Load(); n != 0 {
		t.Errorf("CreateAPIKey called %d time(s), want 0 when static key is configured", n)
	}
}

// TestRunnerMixed_DynamicAPIKey verifies that when cfg.APIKey is empty in mixed mode,
// CreateAPIKey is called once and RevokeAPIKey is called once on teardown.
func TestRunnerMixed_DynamicAPIKey(t *testing.T) { //nolint:paralleltest // uses shared mock server
	var createCount, revokeCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api-keys") {
			createCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"key_id":"dynamic-key-456"}`))
			return
		}
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api-keys/") {
			revokeCount.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tenants":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/keys"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	r := New(Config{
		GatewayURL:         srv.URL,
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthMode:           AuthModeMixed,
		APIKey:             "", // empty → must create dynamically
		AuthUpgradeTimeout: 5 * time.Second,
	}, zerolog.Nop())

	_, startErr := r.Start("test-mixed-dynamic", TestConfig{
		Type:     TestLoad,
		AuthMode: AuthModeMixed,
		Duration: "50ms", // very short so test completes quickly
	})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	if n := createCount.Load(); n != 1 {
		t.Errorf("CreateAPIKey called %d time(s), want 1 in dynamic mixed mode", n)
	}
	if n := revokeCount.Load(); n != 1 {
		t.Errorf("RevokeAPIKey called %d time(s), want 1 on teardown", n)
	}
}

// TestRunnerMixed_CreateAPIKeyFailure verifies that when CreateAPIKey returns an error
// in mixed mode with no static key, the run is marked StatusFailed.
func TestRunnerMixed_CreateAPIKeyFailure(t *testing.T) { //nolint:paralleltest // uses shared mock server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api-keys") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tenants":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/keys"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	r := New(Config{
		GatewayURL:         srv.URL,
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthMode:           AuthModeMixed,
		APIKey:             "",
		AuthUpgradeTimeout: 5 * time.Second,
	}, zerolog.Nop())

	run, startErr := r.Start("test-mixed-fail", TestConfig{
		Type:     TestLoad,
		AuthMode: AuthModeMixed,
		Duration: "50ms", // very short so test completes quickly if it runs
	})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	status, report := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("run status = %q, want %q", status, StatusFailed)
	}
	if report == nil {
		t.Fatal("expected non-nil report on failure")
	}
}
