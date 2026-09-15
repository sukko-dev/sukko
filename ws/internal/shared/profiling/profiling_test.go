package profiling

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

func TestInitPprof_Enabled(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	logger := zerolog.Nop()

	InitPprof(mux.HandleFunc, true, logger)

	// Verify pprof index handler is registered
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("pprof index returned status %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestInitPprof_Disabled(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	logger := zerolog.Nop()

	InitPprof(mux.HandleFunc, false, logger)

	// Verify pprof handlers are NOT registered
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", http.NoBody)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Error("pprof index should not be registered when disabled")
	}
}

func TestInitPyroscope_Disabled(t *testing.T) {
	t.Parallel()
	cfg := PyroscopeConfig{Enabled: false}
	logger := zerolog.Nop()

	stop, err := InitPyroscope(cfg, logger)
	if err != nil {
		t.Fatalf("InitPyroscope(disabled) error: %v", err)
	}

	// No-op stop should not panic
	stop()
}
