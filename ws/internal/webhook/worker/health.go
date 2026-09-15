package worker

import (
	"net/http"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

type healthResponse struct {
	Status   string `json:"status"`
	Reason   string `json:"reason,omitempty"`
	Fallback string `json:"fallback,omitempty"`
}

// HealthHandler returns an http.HandlerFunc for GET /health.
// returns 200 with {"status":"ok"} when gRPC is up;
// returns 200 with degraded body when only Valkey subscription is down;
// returns 503 when gRPC is unavailable.
func HealthHandler(runner *Runner, logger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hs := runner.Health()

		if !hs.GRPCUp {
			if err := httputil.WriteJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "unavailable"}); err != nil {
				logger.Warn().Err(err).Msg("health: failed to write unavailable response")
			}
			return
		}

		if !hs.ValkeySubscriptionUp {
			// degraded body with TTL-fallback note.
			if err := httputil.WriteJSON(w, http.StatusOK, healthResponse{
				Status:   "degraded",
				Reason:   "valkey_subscription_down",
				Fallback: "ttl_refresh_active",
			}); err != nil {
				logger.Warn().Err(err).Msg("health: failed to write degraded response")
			}
			return
		}

		if err := httputil.WriteJSON(w, http.StatusOK, healthResponse{Status: "ok"}); err != nil {
			logger.Warn().Err(err).Msg("health: failed to write ok response")
		}
	}
}
