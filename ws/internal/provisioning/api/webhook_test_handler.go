package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/webhook"
)

const (
	errCodeWebhookWorkerUnavailable = "WEBHOOK_WORKER_UNAVAILABLE"
	errCodeRateLimitExceeded        = "RATE_LIMIT_EXCEEDED"
)

// testDeliveryClient is the gRPC client interface for calling the worker's TestDeliver RPC.
// Matches the generated grpc.CallOption variadic signature.
// Defined as an interface for testability.
type testDeliveryClient interface {
	TestDeliver(ctx context.Context, req *provisioningv1.TestDeliverRequest, opts ...grpc.CallOption) (*provisioningv1.TestDeliverResponse, error)
}

// TestDeliveryResult is the JSON response body for a successful test delivery call.
// The HTTP status is always 200 — the destination's status (success or failure) is in the body.
type TestDeliveryResult struct {
	StatusCode          int    `json:"status_code"`
	LatencyMS           int64  `json:"latency_ms"`
	ResponseBodyPreview string `json:"response_body_preview"`
	Error               string `json:"error,omitempty"`
}

// WebhookTestHandler handles POST /api/v1/tenants/{slug}/webhooks/{webhookID}/test.
// Enforces per-webhook (10/min) and per-tenant (30/min) rate limits via Valkey before
// forwarding to the worker's internal gRPC TestDeliver RPC.
type WebhookTestHandler struct {
	client             testDeliveryClient
	webhookRateLimiter RateLimiter // per-webhook: 10/min
	tenantRateLimiter  RateLimiter // per-tenant: 30/min
	logger             zerolog.Logger
}

// NewWebhookTestHandler creates a WebhookTestHandler.
// webhookRL enforces the per-webhook limit (10/min); tenantRL enforces the per-tenant limit (30/min).
// Both are required; unavailability is handled fail-open.
func NewWebhookTestHandler(client testDeliveryClient, webhookRL, tenantRL RateLimiter, logger zerolog.Logger) (*WebhookTestHandler, error) {
	if client == nil {
		return nil, errors.New("webhook test handler: gRPC client is required")
	}
	if webhookRL == nil {
		return nil, errors.New("webhook test handler: webhook rate limiter is required")
	}
	if tenantRL == nil {
		return nil, errors.New("webhook test handler: tenant rate limiter is required")
	}
	return &WebhookTestHandler{
		client:             client,
		webhookRateLimiter: webhookRL,
		tenantRateLimiter:  tenantRL,
		logger:             logger.With().Str("component", "webhook_test_handler").Logger(),
	}, nil
}

// HandleTestDeliver handles POST /api/v1/tenants/{slug}/webhooks/{webhookID}/test.
// Rate-limited to 10/webhook/min and 30/tenant/min.
// Maps gRPC status → HTTP per NOT_FOUND→404, UNAVAILABLE/deadline→503, OK→200.
func (h *WebhookTestHandler) HandleTestDeliver(w http.ResponseWriter, r *http.Request) {
	webhookID := chi.URLParam(r, "webhookID")
	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		return
	}

	// Rate limit: per-webhook (10/min) — fail-open when Valkey unavailable.
	webhookKey := webhook.WebhookTestRLKeyPrefix + webhookID
	if allowed, retryAfter, err := h.webhookRateLimiter.Allow(r.Context(), webhookKey); err != nil {
		h.logger.Warn().Err(err).Str("webhook_id", webhookID).
			Msg("rate limiter unavailable for webhook key — failing open")
	} else if !allowed {
		httputil.WriteRateLimited(w, retryAfter, errCodeRateLimitExceeded, "rate limit exceeded for this webhook")
		return
	}

	// Rate limit: per-tenant (30/min) — fail-open when Valkey unavailable.
	tenantKey := webhook.WebhookTestRLTenantKeyPrefix + tenantID
	if allowed, retryAfter, err := h.tenantRateLimiter.Allow(r.Context(), tenantKey); err != nil {
		h.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenantID).
			Msg("rate limiter unavailable for tenant key — failing open")
	} else if !allowed {
		httputil.WriteRateLimited(w, retryAfter, errCodeRateLimitExceeded, "rate limit exceeded for this tenant")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), webhook.WebhookTestDeliverDeadline)
	defer cancel()

	resp, err := h.client.TestDeliver(ctx, &provisioningv1.TestDeliverRequest{
		WebhookId:  webhookID,
		TenantUuid: tenantID,
	})
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			httputil.WriteError(w, http.StatusNotFound, errCodeWebhookNotFound, "webhook not found")
		case codes.Unavailable, codes.DeadlineExceeded:
			httputil.WriteError(w, http.StatusServiceUnavailable, errCodeWebhookWorkerUnavailable, "webhook worker is not reachable")
		default:
			h.logger.Error().Err(err).Str("webhook_id", webhookID).Msg("TestDeliver RPC error")
			httputil.WriteError(w, http.StatusInternalServerError, errCodeInternal, "test delivery failed")
		}
		return
	}

	if err := httputil.WriteJSON(w, http.StatusOK, TestDeliveryResult{
		StatusCode:          int(resp.GetStatusCode()),
		LatencyMS:           resp.GetLatencyMs(),
		ResponseBodyPreview: resp.GetResponseBodyPreview(),
		Error:               resp.GetError(),
	}); err != nil {
		h.logger.Warn().Err(err).Str("webhook_id", webhookID).Msg("test deliver: failed to write response")
	}
}
