package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// topicsMaxPageLimit is the per-page cap for topic list responses. Topics are
// small structs bounded by the MaxTopics quota, so the routing-rules cap applies.
const topicsMaxPageLimit = routingRulesMaxPageLimit

// ListTopics returns a tenant's provisioned topics (the deterministic default
// topic is always included first). Paginated per §XII over the assembled list.
func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	topics, err := h.service.ListTopics(r.Context(), tenantSlug)
	if err != nil {
		status, code, msg := classifyServiceError(err, errCodeListTopicsFailed, "Failed to list topics")
		h.logServiceError(status, err, "Failed to list topics", tenantSlug)
		httputil.WriteError(w, status, code, msg)
		return
	}

	limit, offset := parsePagination(r, defaultPageLimit, topicsMaxPageLimit)
	total := len(topics)
	start := min(offset, total)
	end := min(start+limit, total)
	page := topics[start:end]

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  page,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// CreateTopic provisions a non-default topic for a tenant (ADR-0006 Phase 2).
func (h *Handler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "CreateTopic").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed")
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	topic, err := h.service.CreateTopic(r.Context(), tenantSlug, req.Suffix)
	if err != nil {
		switch {
		case errors.Is(err, provisioning.ErrReservedTopicSuffix):
			httputil.WriteError(w, http.StatusBadRequest, errCodeReservedTopicSuffix, err.Error())
		case errors.Is(err, provisioning.ErrInvalidTopicSuffix):
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidTopicSuffix, err.Error())
		case errors.Is(err, provisioning.ErrTopicQuotaExceeded):
			// Mirrors ErrTooManyRoutingRules: the configured quota is a client
			// outcome (400), distinct from the edition cap (403, via classify).
			httputil.WriteError(w, http.StatusBadRequest, errCodeTopicQuotaExceeded, err.Error())
		case errors.Is(err, provisioning.ErrTopicAlreadyExists):
			httputil.WriteError(w, http.StatusConflict, errCodeTopicAlreadyExists, err.Error())
		default:
			// Classify once so the log level follows the status: tenant-not-active,
			// tenant-not-found, and the edition cap are expected client outcomes.
			status, code, msg := classifyServiceError(err, errCodeCreateTopicFailed, "Failed to create topic")
			h.logServiceError(status, err, "Failed to create topic", tenantSlug)
			httputil.WriteError(w, status, code, msg)
		}
		return
	}

	RecordTopicCreated()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("suffix", topic.Suffix).Msg("topic created")

	_ = httputil.WriteJSON(w, http.StatusCreated, map[string]any{
		"topic": topic,
	})
}

// DeleteTopic removes a provisioned topic. Reserved suffixes cannot be deleted,
// and a topic still referenced by a routing rule is rejected (409).
func (h *Handler) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	suffix := chi.URLParam(r, "topicSuffix")

	if err := h.service.DeleteTopic(r.Context(), tenantSlug, suffix); err != nil {
		switch {
		case errors.Is(err, provisioning.ErrReservedTopicSuffix):
			httputil.WriteError(w, http.StatusBadRequest, errCodeReservedTopicSuffix, err.Error())
		case errors.Is(err, provisioning.ErrTopicReferencedByRule):
			httputil.WriteError(w, http.StatusConflict, errCodeTopicReferencedByRule, err.Error())
		case errors.Is(err, provisioning.ErrTopicNotFound):
			httputil.WriteError(w, http.StatusNotFound, errCodeTopicNotFound, err.Error())
		default:
			status, code, msg := classifyServiceError(err, errCodeDeleteTopicFailed, "Failed to delete topic")
			h.logServiceError(status, err, "Failed to delete topic", tenantSlug)
			httputil.WriteError(w, status, code, msg)
		}
		return
	}

	RecordTopicDeleted()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("suffix", suffix).Msg("topic deleted")

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": statusDeleted})
}
