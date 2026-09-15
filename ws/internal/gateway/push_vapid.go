package gateway

import (
	"net/http"

	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// HandlePushVAPIDKey returns the tenant's VAPID public key for Web Push subscriptions.
//
// Flow:
//  1. Authenticate via shared authenticateRequest()
//  2. Forward to push service via gRPC GetVAPIDKey
//  3. Return {"public_key": "..."}
func (gw *Gateway) HandlePushVAPIDKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. Authenticate
	authRes, authErr := gw.authenticateRequest(ctx, r)
	if authErr != nil {
		status, code := authErrorResponse(authErr)
		httputil.WriteError(w, status, code, authErr.Error())
		return
	}

	// 2. Forward to push service
	if gw.pushClient == nil {
		// LOG-012: Push service unavailable
		gw.logger.Warn().Str("handler", "vapid-key").Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
			Msg("push service unavailable")
		httputil.WriteError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE",
			"push service connection not available")
		return
	}

	resp, err := gw.pushClient.GetVAPIDKey(ctx, &pushv1.GetVAPIDKeyRequest{
		TenantSlug: authRes.TenantSlug,
	})
	if err != nil {
		gw.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
			Msg("Push GetVAPIDKey failed")
		writePushServiceError(w, err, "failed to retrieve VAPID key")
		return
	}

	// 3. Return public key
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"public_key": resp.GetPublicKey(),
	})
}
