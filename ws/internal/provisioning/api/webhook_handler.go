package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

var (
	webhookRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_webhook_requests_total",
		Help: "Total webhook API requests by operation and status.",
	}, []string{"op", "status"})

	webhookRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "provisioning_webhook_request_duration_seconds",
		Help:    "Webhook API request duration by operation.",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"op"})
)

const (
	errCodeWebhookNotFound      = "WEBHOOK_NOT_FOUND"
	errCodeWebhookQuotaExceeded = "WEBHOOK_QUOTA_EXCEEDED"
	errCodeWebhookInvalidInput  = "WEBHOOK_INVALID_INPUT"
)

// webhookService is the subset of provisioning.Service used by WebhookHandler.
// Defined as an interface so handler tests can inject mocks.
type webhookService interface {
	CreateWebhook(ctx context.Context, req provisioning.CreateWebhookRequest) (*provisioning.Webhook, error)
	GetWebhook(ctx context.Context, id, tenantID string) (*provisioning.Webhook, error)
	ListWebhooks(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.Webhook, int, error)
	UpdateWebhook(ctx context.Context, req provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error)
	DeleteWebhook(ctx context.Context, id, tenantID string) error
}

// WebhookHandler handles CRUD for webhook registrations.
type WebhookHandler struct {
	service webhookService
	logger  zerolog.Logger
}

// NewWebhookHandler creates a WebhookHandler. Returns an error if service is nil.
func NewWebhookHandler(svc webhookService, logger zerolog.Logger) (*WebhookHandler, error) {
	if svc == nil {
		return nil, errors.New("webhook handler: service is required")
	}
	return &WebhookHandler{
		service: svc,
		logger:  logger.With().Str("component", "webhook_handler").Logger(),
	}, nil
}

// webhookResponse is the JSON shape returned for a single webhook.
// SecretEnc is intentionally omitted — never expose encrypted secrets in API responses (§IX).
type webhookResponse struct {
	ID             string  `json:"id"`
	TenantID       string  `json:"tenant_id"`
	URL            string  `json:"url"`
	ChannelPattern string  `json:"channel_pattern"`
	Status         string  `json:"status"`
	MaxRetries     int     `json:"max_retries"`
	RetryCount     int     `json:"retry_count"`
	LastStatus     *string `json:"last_status,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

// webhookListResponse is the paginated list response.
type webhookListResponse struct {
	Items  []*webhookResponse `json:"items"`
	Total  int                `json:"total"`
	Limit  int                `json:"limit"`
	Offset int                `json:"offset"`
}

// createWebhookRequest is the JSON body for POST /webhooks.
type createWebhookRequest struct {
	URL            string `json:"url"`
	ChannelPattern string `json:"channel_pattern"`
	Secret         string `json:"secret"`
	MaxRetries     int    `json:"max_retries,omitempty"`
}

// updateWebhookRequest is the JSON body for PATCH /webhooks/{id}.
type updateWebhookRequest struct {
	URL            *string `json:"url,omitempty"`
	ChannelPattern *string `json:"channel_pattern,omitempty"`
	MaxRetries     *int    `json:"max_retries,omitempty"`
	Status         *string `json:"status,omitempty"`
}

// toResponse converts a domain Webhook to the API response shape.
func toWebhookResponse(w *provisioning.Webhook) *webhookResponse {
	return &webhookResponse{
		ID:             w.ID,
		TenantID:       w.TenantID,
		URL:            w.URL,
		ChannelPattern: w.ChannelPattern,
		Status:         w.Status,
		MaxRetries:     w.MaxRetries,
		RetryCount:     w.RetryCount,
		LastStatus:     w.LastStatus,
		CreatedAt:      w.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:      w.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// HandleCreate handles POST /api/v1/tenants/{tenantSlug}/webhooks.
func (h *WebhookHandler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(webhookRequestDuration.WithLabelValues("create"))
	defer timer.ObserveDuration()

	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		webhookRequestsTotal.WithLabelValues("create", "error").Inc()
		return
	}

	var body createWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid JSON body")
		webhookRequestsTotal.WithLabelValues("create", "error").Inc()
		return
	}
	if body.Secret == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "secret is required")
		webhookRequestsTotal.WithLabelValues("create", "error").Inc()
		return
	}

	webhook, err := h.service.CreateWebhook(r.Context(), provisioning.CreateWebhookRequest{
		TenantID:       tenantID,
		URL:            body.URL,
		ChannelPattern: body.ChannelPattern,
		Secret:         body.Secret,
		MaxRetries:     body.MaxRetries,
	})
	if err != nil {
		h.mapError(w, err, "create webhook")
		webhookRequestsTotal.WithLabelValues("create", "error").Inc()
		return
	}

	_ = httputil.WriteJSON(w, http.StatusCreated, toWebhookResponse(webhook))
	webhookRequestsTotal.WithLabelValues("create", "success").Inc()
}

// HandleList handles GET /api/v1/tenants/{tenantSlug}/webhooks.
func (h *WebhookHandler) HandleList(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(webhookRequestDuration.WithLabelValues("list"))
	defer timer.ObserveDuration()

	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		webhookRequestsTotal.WithLabelValues("list", "error").Inc()
		return
	}

	limit, offset := parsePagination(r, defaultPageLimit, maxPageLimit)
	webhooks, total, err := h.service.ListWebhooks(r.Context(), tenantID, provisioning.ListOptions{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		h.mapError(w, err, "list webhooks")
		webhookRequestsTotal.WithLabelValues("list", "error").Inc()
		return
	}

	items := make([]*webhookResponse, 0, len(webhooks))
	for _, wh := range webhooks {
		items = append(items, toWebhookResponse(wh))
	}
	_ = httputil.WriteJSON(w, http.StatusOK, webhookListResponse{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	})
	webhookRequestsTotal.WithLabelValues("list", "success").Inc()
}

// HandleGet handles GET /api/v1/tenants/{tenantSlug}/webhooks/{webhookID}.
func (h *WebhookHandler) HandleGet(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(webhookRequestDuration.WithLabelValues("get"))
	defer timer.ObserveDuration()

	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		webhookRequestsTotal.WithLabelValues("get", "error").Inc()
		return
	}
	webhookID := chi.URLParam(r, "webhookID")

	webhook, err := h.service.GetWebhook(r.Context(), webhookID, tenantID)
	if err != nil {
		h.mapError(w, err, "get webhook")
		webhookRequestsTotal.WithLabelValues("get", "error").Inc()
		return
	}
	_ = httputil.WriteJSON(w, http.StatusOK, toWebhookResponse(webhook))
	webhookRequestsTotal.WithLabelValues("get", "success").Inc()
}

// HandleUpdate handles PATCH /api/v1/tenants/{tenantSlug}/webhooks/{webhookID}.
func (h *WebhookHandler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(webhookRequestDuration.WithLabelValues("update"))
	defer timer.ObserveDuration()

	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		webhookRequestsTotal.WithLabelValues("update", "error").Inc()
		return
	}
	webhookID := chi.URLParam(r, "webhookID")

	var body updateWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid JSON body")
		webhookRequestsTotal.WithLabelValues("update", "error").Inc()
		return
	}

	webhook, err := h.service.UpdateWebhook(r.Context(), provisioning.UpdateWebhookRequest{
		ID:             webhookID,
		TenantID:       tenantID,
		URL:            body.URL,
		ChannelPattern: body.ChannelPattern,
		MaxRetries:     body.MaxRetries,
		Status:         body.Status,
	})
	if err != nil {
		h.mapError(w, err, "update webhook")
		webhookRequestsTotal.WithLabelValues("update", "error").Inc()
		return
	}
	_ = httputil.WriteJSON(w, http.StatusOK, toWebhookResponse(webhook))
	webhookRequestsTotal.WithLabelValues("update", "success").Inc()
}

// HandleDelete handles DELETE /api/v1/tenants/{tenantSlug}/webhooks/{webhookID}.
func (h *WebhookHandler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(webhookRequestDuration.WithLabelValues("delete"))
	defer timer.ObserveDuration()

	tenantID := getTenantUUIDFromContext(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "missing tenant context")
		webhookRequestsTotal.WithLabelValues("delete", "error").Inc()
		return
	}
	webhookID := chi.URLParam(r, "webhookID")

	if err := h.service.DeleteWebhook(r.Context(), webhookID, tenantID); err != nil {
		h.mapError(w, err, "delete webhook")
		webhookRequestsTotal.WithLabelValues("delete", "error").Inc()
		return
	}
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
	webhookRequestsTotal.WithLabelValues("delete", "success").Inc()
}

// mapError translates domain errors to HTTP responses.
func (h *WebhookHandler) mapError(w http.ResponseWriter, err error, op string) {
	switch {
	case errors.Is(err, provisioning.ErrWebhookNotFound):
		httputil.WriteError(w, http.StatusNotFound, errCodeWebhookNotFound, "webhook not found")
	case errors.Is(err, provisioning.ErrWebhookQuotaExceeded):
		httputil.WriteError(w, http.StatusUnprocessableEntity, errCodeWebhookQuotaExceeded, "webhook quota exceeded")
	case errors.Is(err, provisioning.ErrWebhookInvalidInput):
		httputil.WriteError(w, http.StatusBadRequest, errCodeWebhookInvalidInput, err.Error())
	case errors.Is(err, provisioning.ErrWebhookStoreNotConfigured):
		httputil.WriteError(w, http.StatusForbidden, errCodeEditionLimit, "webhooks require Pro edition or higher")
	default:
		h.logger.Error().Err(err).Str("op", op).Msg("webhook operation failed")
		httputil.WriteError(w, http.StatusInternalServerError, errCodeInternal, "internal error")
	}
}
