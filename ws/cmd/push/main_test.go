package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

// fakeWatcher implements licenseStateGetter for testing.
type fakeWatcher struct{ state int32 }

func (f *fakeWatcher) State() int32 { return f.state }

// ---------------------------------------------------------------------------
// Tests: /health
// ---------------------------------------------------------------------------

func TestHealthHandler_Connected(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(&fakeWatcher{state: provapi.StreamStateConnected}, license.NewTestManager(license.Enterprise))
	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q, want %q", body["status"], "ok")
	}
	if body["service"] != serviceName {
		t.Errorf("service = %q, want %q", body["service"], serviceName)
	}
	if body["license_stream"] != provapi.StreamLabelConnected {
		t.Errorf("license_stream = %q, want %q", body["license_stream"], provapi.StreamLabelConnected)
	}
}

func TestHealthHandler_Degraded(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(&fakeWatcher{state: provapi.StreamStateDisconnected}, license.NewTestManager(license.Enterprise))
	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (liveness always returns 200)", w.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["license_stream"] != provapi.StreamLabelDegraded {
		t.Errorf("license_stream = %q, want %q", body["license_stream"], provapi.StreamLabelDegraded)
	}
}

// TestHealthHandler_Always200 verifies the liveness invariant:
// /health MUST always return HTTP 200 regardless of stream state or edition.
func TestHealthHandler_Always200(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		state   int32
		edition license.Edition
	}{
		{"connected-enterprise", provapi.StreamStateConnected, license.Enterprise},
		{"degraded-enterprise", provapi.StreamStateDisconnected, license.Enterprise},
		{"connected-community", provapi.StreamStateConnected, license.Community},
		{"degraded-community", provapi.StreamStateDisconnected, license.Community},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mux := newHTTPMux(&fakeWatcher{state: tc.state}, license.NewTestManager(tc.edition))
			req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("/health must always return 200, got %d", w.Code)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: /ready
// ---------------------------------------------------------------------------

func TestReadyHandler_Enterprise(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(&fakeWatcher{state: provapi.StreamStateConnected}, license.NewTestManager(license.Enterprise))
	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (Enterprise active)", w.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["ready"] != true {
		t.Errorf("ready = %v, want true", body["ready"])
	}
	if body["edition"] != "enterprise" {
		t.Errorf("edition = %v, want %q", body["edition"], "enterprise")
	}
}

func TestReadyHandler_Community(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(&fakeWatcher{state: provapi.StreamStateConnected}, license.NewTestManager(license.Community))
	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (Community edition — not ready)", w.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["ready"] != false {
		t.Errorf("ready = %v, want false", body["ready"])
	}
	if body["edition"] != "community" {
		t.Errorf("edition = %v, want %q", body["edition"], "community")
	}
}

// TestReadyHandler_StreamDegraded verifies a stream disconnect does NOT
// change readiness as long as the cached edition is Enterprise.
func TestReadyHandler_StreamDegraded_EnterpriseStaysReady(t *testing.T) {
	t.Parallel()

	mux := newHTTPMux(&fakeWatcher{state: provapi.StreamStateDisconnected}, license.NewTestManager(license.Enterprise))
	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — Enterprise pod must stay ready during stream disconnect", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["ready"] != true {
		t.Errorf("ready = %v, want true", body["ready"])
	}
}
