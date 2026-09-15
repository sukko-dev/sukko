package api

import (
	"net/http"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/runner"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/version"
)

// NewRouter creates the HTTP handler for the tester API.
func NewRouter(r *runner.Runner, authToken, adminKeyID string, logger zerolog.Logger) http.Handler {
	mux := http.NewServeMux()

	h := &handlers{
		runner:     r,
		authToken:  authToken,
		adminKeyID: adminKeyID,
		logger:     logger,
	}

	// Public endpoints (no auth)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		httputil.WriteHealthOK(w, "sukko-tester")
	})
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, r *http.Request) {
		info := version.Get("sukko-tester")
		writeJSON(w, http.StatusOK, info)
	})
	mux.HandleFunc("GET /api/v1/capabilities", h.getCapabilities)

	// Protected endpoints
	mux.HandleFunc("POST /api/v1/tests", h.authMiddleware(h.startTest))
	mux.HandleFunc("POST /api/v1/tests/{id}/stop", h.authMiddleware(h.stopTest))
	mux.HandleFunc("GET /api/v1/tests/{id}", h.authMiddleware(h.getTest))
	mux.HandleFunc("GET /api/v1/tests/{id}/metrics", h.authMiddleware(h.streamMetrics))

	// Webhook delivery receiver — called by the webhook-worker, not the CLI.
	// No auth: the HMAC signature on each delivery is the integrity check.
	mux.HandleFunc("POST /webhook-receive/{runID}", r.WebhookReceiveHandler())

	return mux
}
