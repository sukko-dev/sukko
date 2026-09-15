package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// NOTE: httputil.WriteJSON errors are assigned to _ throughout this file.
// If WriteJSON fails, the client has disconnected and the HTTP response is
// already committed — there is no way to communicate a secondary error.

// Sentinel errors for push channel handler construction.
var (
	errNilChannelConfigRepo = errors.New("push channel handler: channel config repository is required")
	errNilChannelEventBus   = errors.New("push channel handler: event bus is required")
)

// validUrgencies is the set of allowed urgency values per Web Push spec.
var validUrgencies = map[string]bool{
	"very-low": true,
	"low":      true,
	"normal":   true,
	"high":     true,
}

// PushChannelHandler handles push channel configuration CRUD.
type PushChannelHandler struct {
	channelConfigRepo *repository.ChannelConfigRepository
	resolveTenant     provisioning.TenantLookupFunc
	eventBus          *eventbus.Bus
	logger            zerolog.Logger
}

// NewPushChannelHandler creates a PushChannelHandler. resolveTenant maps the client-supplied
// tenant slug to the tenant UUID written to the (UUID-typed) DB column.
func NewPushChannelHandler(
	channelConfigRepo *repository.ChannelConfigRepository,
	resolveTenant provisioning.TenantLookupFunc,
	eventBus *eventbus.Bus,
	logger zerolog.Logger,
) (*PushChannelHandler, error) {
	if channelConfigRepo == nil {
		return nil, errNilChannelConfigRepo
	}
	if resolveTenant == nil {
		return nil, errNilTenantResolver
	}
	if eventBus == nil {
		return nil, errNilChannelEventBus
	}
	return &PushChannelHandler{
		channelConfigRepo: channelConfigRepo,
		resolveTenant:     resolveTenant,
		eventBus:          eventBus,
		logger:            logger.With().Str("component", "push_channel_handler").Logger(),
	}, nil
}

// createChannelConfigRequest is the JSON body for POST /api/v1/push/channels.
// tenant_id is the tenant SLUG (also the channel-pattern prefix); the server resolves it to
// the UUID stored at rest.
type createChannelConfigRequest struct {
	TenantID       string   `json:"tenant_id"`
	Patterns       []string `json:"patterns"`
	DefaultTTL     int      `json:"default_ttl"`
	DefaultUrgency string   `json:"default_urgency"`
}

// channelConfigResponse is the JSON response for channel config operations.
// tenant_id echoes the input slug (never the internal UUID).
type channelConfigResponse struct {
	TenantID       string   `json:"tenant_id"`
	Patterns       []string `json:"patterns"`
	DefaultTTL     int      `json:"default_ttl"`
	DefaultUrgency string   `json:"default_urgency"`
}

// deleteChannelConfigRequest is the JSON body for DELETE /api/v1/push/channels.
type deleteChannelConfigRequest struct {
	TenantID string `json:"tenant_id"`
}

// maxChannelConfigBodySize is the maximum request body size for channel config operations (1MB).
const maxChannelConfigBodySize = 1 << 20

// HandleCreateChannelConfig handles POST /api/v1/push/channels.
func (h *PushChannelHandler) HandleCreateChannelConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxChannelConfigBodySize)
	var req createChannelConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	if req.TenantID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingTenantID, "tenant_id is required")
		return
	}
	if len(req.Patterns) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingPatterns, "patterns must be non-empty")
		return
	}

	// Channel patterns are SLUG-prefixed (data plane) — validate against the slug BEFORE
	// resolving, so a malformed pattern is rejected without a DB round-trip.
	for _, pattern := range req.Patterns {
		if !auth.ValidateChannelTenant(pattern, req.TenantID) {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidPattern,
				fmt.Sprintf("pattern %q must start with tenant prefix %q", pattern, req.TenantID+"."))
			return
		}
	}

	if req.DefaultUrgency != "" && !validUrgencies[req.DefaultUrgency] {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidUrgency,
			"default_urgency must be one of: very-low, low, normal, high")
		return
	}

	// Apply defaults.
	if req.DefaultUrgency == "" {
		req.DefaultUrgency = "normal"
	}
	if req.DefaultTTL == 0 {
		req.DefaultTTL = defaultPushTTL
	}

	// Resolve slug → UUID for the (UUID-typed) FK column.
	tenantUUID, ok := resolveTenantUUID(r.Context(), h.resolveTenant, req.TenantID, w, h.logger)
	if !ok {
		return
	}

	config := &repository.PushChannelConfig{
		TenantID:       tenantUUID,
		Patterns:       req.Patterns,
		DefaultTTL:     req.DefaultTTL,
		DefaultUrgency: req.DefaultUrgency,
	}

	if err := h.channelConfigRepo.Upsert(r.Context(), config); err != nil {
		h.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.TenantID).
			Str(logging.LogKeyTenantUUID, tenantUUID).
			Msg("Failed to upsert push channel config")
		httputil.WriteError(w, http.StatusInternalServerError, errCodeUpsertFailed, "Failed to save push channel config")
		return
	}

	h.eventBus.Publish(eventbus.Event{Type: eventbus.PushConfigChanged})

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, req.TenantID).
		Str(logging.LogKeyTenantUUID, tenantUUID).
		Int("patterns", len(req.Patterns)).
		Int("default_ttl", req.DefaultTTL).
		Str("default_urgency", req.DefaultUrgency).
		Msg("Push channel config created/updated")

	_ = httputil.WriteJSON(w, http.StatusCreated, channelConfigResponse{
		TenantID:       req.TenantID, // echo the input slug, never the internal UUID
		Patterns:       config.Patterns,
		DefaultTTL:     config.DefaultTTL,
		DefaultUrgency: config.DefaultUrgency,
	})
}

// HandleGetChannelConfig handles GET /api/v1/push/channels.
func (h *PushChannelHandler) HandleGetChannelConfig(w http.ResponseWriter, r *http.Request) {
	tenantSlug := r.URL.Query().Get("tenant_id")
	if tenantSlug == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingTenantID, "tenant_id query parameter is required")
		return
	}

	tenantUUID, ok := resolveTenantUUID(r.Context(), h.resolveTenant, tenantSlug, w, h.logger)
	if !ok {
		return
	}

	config, err := h.channelConfigRepo.Get(r.Context(), tenantUUID)
	if err != nil {
		if errors.Is(err, repository.ErrChannelConfigNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, "Push channel config not found for tenant")
		} else {
			h.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, tenantSlug).
				Str(logging.LogKeyTenantUUID, tenantUUID).
				Msg("Failed to get push channel config")
			httputil.WriteError(w, http.StatusInternalServerError, errCodeInternal, "Failed to retrieve channel config")
		}
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, channelConfigResponse{
		TenantID:       tenantSlug, // echo the input slug, not the stored UUID
		Patterns:       config.Patterns,
		DefaultTTL:     config.DefaultTTL,
		DefaultUrgency: config.DefaultUrgency,
	})
}

// HandleDeleteChannelConfig handles DELETE /api/v1/push/channels.
func (h *PushChannelHandler) HandleDeleteChannelConfig(w http.ResponseWriter, r *http.Request) {
	var req deleteChannelConfigRequest

	// Support both query params and JSON body.
	if r.Body != nil && r.ContentLength > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxChannelConfigBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
			return
		}
	}

	if v := r.URL.Query().Get("tenant_id"); v != "" {
		req.TenantID = v
	}

	if req.TenantID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingTenantID, "tenant_id is required")
		return
	}

	tenantUUID, ok := resolveTenantUUID(r.Context(), h.resolveTenant, req.TenantID, w, h.logger)
	if !ok {
		return
	}

	if err := h.channelConfigRepo.Delete(r.Context(), tenantUUID); err != nil {
		if errors.Is(err, repository.ErrChannelConfigNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, "Push channel config not found for tenant")
		} else {
			h.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, req.TenantID).
				Str(logging.LogKeyTenantUUID, tenantUUID).
				Msg("Failed to delete push channel config")
			httputil.WriteError(w, http.StatusInternalServerError, errCodeInternal, "Failed to delete channel config")
		}
		return
	}

	h.eventBus.Publish(eventbus.Event{Type: eventbus.PushConfigChanged})

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, req.TenantID).
		Str(logging.LogKeyTenantUUID, tenantUUID).
		Msg("Push channel config deleted")

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// defaultPushTTL is the default TTL for push messages (28 days in seconds).
const defaultPushTTL = 2419200
