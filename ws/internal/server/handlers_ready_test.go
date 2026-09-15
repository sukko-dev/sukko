package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

// newReadyTestServer builds a minimal Server for handleReady tests — the handler only touches
// s.backend and s.logger.
func newReadyTestServer(mb *mockBackend) *Server {
	return &Server{
		backend: mb,
		logger:  zerolog.Nop(),
	}
}

func TestHandleReady_BackendReady_200(t *testing.T) {
	t.Parallel()
	s := newReadyTestServer(&mockBackend{notReady: false})

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body struct {
		Status string `json:"status"`
		Ready  bool   `json:"ready"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ready" || !body.Ready {
		t.Errorf("body = %+v, want status=ready ready=true", body)
	}
}

func TestHandleReady_BackendNotReady_503(t *testing.T) {
	t.Parallel()
	s := newReadyTestServer(&mockBackend{notReady: true})

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d (backend not ready → readiness gate closed)", rec.Code, http.StatusServiceUnavailable)
	}
	var body struct {
		Status string `json:"status"`
		Ready  bool   `json:"ready"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "not_ready" || body.Ready {
		t.Errorf("body = %+v, want status=not_ready ready=false", body)
	}
}

// A nil backend is direct mode with nothing to wait for → always ready. The backend field is left as
// a true nil interface (not a typed-nil *mockBackend, which would be a non-nil interface).
func TestHandleReady_NilBackend_200(t *testing.T) {
	t.Parallel()
	s := &Server{logger: zerolog.Nop()}

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (nil backend = direct mode, always ready)", rec.Code, http.StatusOK)
	}
}
