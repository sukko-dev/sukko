package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// REST publish error codes and metric outcome labels (§I — named constants, not literals).
const (
	errCodePublishNotRoutable = "PUBLISH_NOT_ROUTABLE"
	errCodeInvalidChannel     = "INVALID_CHANNEL"
	errCodeServiceUnavailable = "SERVICE_UNAVAILABLE"
	errCodeInternal           = "INTERNAL_ERROR"

	publishOutcomeNotRoutable = "not_routable"
	publishOutcomeInvalidChan = "invalid_channel"
	publishOutcomeUnavailable = "unavailable"
	publishOutcomeError       = "error"
)

// mapPublishError maps a gRPC status code from ws-server's Publish RPC to the REST
// response shape (§XII). Only Internal is a genuine 500; the rest are expected conditions
// (client misconfiguration or retryable degradation).
func mapPublishError(code codes.Code) (httpStatus int, errCode, message, metricLabel string) {
	switch code {
	case codes.FailedPrecondition:
		return http.StatusConflict, errCodePublishNotRoutable,
			"no routing rule applies to this channel for the tenant", publishOutcomeNotRoutable
	case codes.InvalidArgument:
		return http.StatusBadRequest, errCodeInvalidChannel,
			"invalid channel", publishOutcomeInvalidChan
	case codes.Unavailable:
		return http.StatusServiceUnavailable, errCodeServiceUnavailable,
			"publish temporarily unavailable, retry", publishOutcomeUnavailable
	default:
		return http.StatusInternalServerError, errCodeInternal,
			"failed to publish message", publishOutcomeError
	}
}

// publishRequest is the JSON body format for POST /api/v1/publish.
type publishRequest struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// HandlePublish handles REST publish requests — stateless message publishing.
//
// Flow:
//  1. Validate Content-Type and body size
//  2. Parse JSON body
//  3. Authenticate via shared authenticateRequest()
//  4. Check channel publish permissions
//  5. Rate limit via PublishRateLimiter
//  6. Call gRPC Publish() on ws-server
//  7. Return 200 {"status":"accepted","channel":"..."}
func (gw *Gateway) HandlePublish(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx := r.Context()

	// 1. Validate Content-Type
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		RecordRestPublish("invalid_content_type", time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "Content-Type must be application/json")
		return
	}

	// Limit body size to prevent abuse (same as GATEWAY_MAX_PUBLISH_SIZE)
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(gw.config.MaxPublishSize)+1))
	if err != nil {
		RecordRestPublish("read_error", time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "failed to read request body")
		return
	}
	if len(body) > gw.config.MaxPublishSize {
		RecordRestPublish("body_too_large", time.Since(startTime))
		httputil.WriteError(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE",
			"request body exceeds maximum publish size")
		return
	}

	// 2. Parse JSON body
	var req publishRequest
	if err := json.Unmarshal(body, &req); err != nil {
		RecordRestPublish("invalid_json", time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}
	if req.Channel == "" {
		RecordRestPublish("missing_channel", time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "channel is required")
		return
	}
	if len(req.Data) == 0 {
		RecordRestPublish("missing_data", time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "data is required")
		return
	}

	// 3. Authenticate
	authRes, authErr := gw.authenticateRequest(ctx, r)
	if authErr != nil {
		authStatus, code := authErrorResponse(authErr)
		RecordRestPublish("auth_failed", time.Since(startTime))
		httputil.WriteError(w, authStatus, code, authErr.Error())
		return
	}

	// 3a–4. Permission checks
	// Block API-key-only — JWT required for publish
	if authRes.APIKeyOnly {
		RecordRestPublish("forbidden", time.Since(startTime))
		httputil.WriteError(w, http.StatusForbidden, "FORBIDDEN",
			"publish requires JWT authentication — API key provides read-only access")
		return
	}

	// Validate channel format
	if strings.Count(req.Channel, ".")+1 < protocol.MinInternalChannelParts {
		RecordRestPublish(publishOutcomeInvalidChan, time.Since(startTime))
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST",
			fmt.Sprintf("channel must have at least %d dot-separated parts", protocol.MinInternalChannelParts))
		return
	}

	// Validate tenant prefix
	if !auth.ValidateChannelTenant(req.Channel, authRes.TenantSlug) {
		RecordRestPublish("forbidden", time.Since(startTime))
		httputil.WriteError(w, http.StatusForbidden, "FORBIDDEN",
			"channel tenant prefix does not match authenticated tenant")
		return
	}

	// Enforce per-tenant publish rules — the same shared check the WS proxy
	// uses (checkPublishAllowed → tenant checker, §XVIII). Full channel is
	// passed; the helper strips the tenant prefix internally.
	if !gw.checkPublishAllowed(ctx, authRes.TenantSlug, authRes.Claims, req.Channel) {
		RecordRestPublish("forbidden", time.Since(startTime))
		httputil.WriteError(w, http.StatusForbidden, "FORBIDDEN",
			"channel rules deny publish to this channel")
		return
	}

	// 5. Rate limit
	if gw.publishRateLimiter != nil {
		clientIP := httputil.GetClientIP(r)
		if !gw.publishRateLimiter.Allow(authRes.TenantSlug, clientIP) {
			RecordRestPublish("rate_limited", time.Since(startTime))
			// Retry-After = this limiter's one-token refill time (§IX), read from the
			// limiter that rejected the request so the hint cannot drift from the
			// enforced rate. Mirrors the revocation handler's advertisement.
			httputil.WriteRateLimited(w, gw.publishRateLimiter.RetryAfter(),
				"RATE_LIMITED", "publish rate limit exceeded")
			return
		}
	}

	// 6. Call gRPC Publish() on ws-server
	if gw.serverClient == nil {
		RecordRestPublish(publishOutcomeUnavailable, time.Since(startTime))
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable,
			"ws-server connection not available")
		return
	}

	resp, err := gw.serverClient.Client().Publish(ctx, &serverv1.PublishRequest{
		TenantSlug: authRes.TenantSlug,
		Channel:    req.Channel,
		Data:       req.Data,
		Principal:  authRes.Principal,
	})
	if err != nil {
		code := status.Code(err)
		httpStatus, errCode, message, label := mapPublishError(code)
		// Reject-class / unavailable are expected conditions — Warn, not Error (§V).
		evt := gw.logger.Error()
		if httpStatus != http.StatusInternalServerError {
			evt = gw.logger.Warn()
		}
		evt.Err(err).
			Str("channel", req.Channel).
			Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
			Str(logging.LogKeyGRPCCode, code.String()).
			Msg("gRPC Publish failed")
		RecordRestPublish(label, time.Since(startTime))
		httputil.WriteError(w, httpStatus, errCode, message)
		return
	}

	// 7. Return success
	RecordRestPublish("success", time.Since(startTime))
	respBody := map[string]string{
		"status":  resp.GetStatus(),
		"channel": resp.GetChannel(),
	}
	// Stable message identity: the ingress record's mid (ADR-0018), always
	// carried in kafka mode. Egress copies never carry delivery identity.
	if resp.GetMid() != "" {
		respBody["mid"] = resp.GetMid()
	}
	_ = httputil.WriteJSON(w, http.StatusOK, respBody)
}
