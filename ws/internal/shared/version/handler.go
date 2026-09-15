package version

import (
	"net/http"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

// Handler returns an http.HandlerFunc for the /version endpoint.
// serviceName is embedded in the response (e.g., "ws-server", "ws-gateway").
// Edition info is served by the separate GET /edition endpoint.
func Handler(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		_ = httputil.WriteJSON(w, http.StatusOK, Get(serviceName))
	}
}
