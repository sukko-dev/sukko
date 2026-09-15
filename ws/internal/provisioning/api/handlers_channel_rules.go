package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// GetChannelRules retrieves channel rules for a tenant.
// GET /tenants/{tenantSlug}/channel-rules
func (h *Handler) GetChannelRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	rules, err := h.service.GetChannelRules(r.Context(), tenantSlug)
	if err != nil {
		if errors.Is(err, types.ErrChannelRulesNotFound) {
			httputil.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Channel rules not configured for this tenant")
			return
		}
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to get channel rules")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_FAILED", "Failed to get channel rules")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, rules)
}

// SetChannelRules creates or updates channel rules for a tenant.
// PUT /tenants/{tenantSlug}/channel-rules
func (h *Handler) SetChannelRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.SetChannelRulesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON body")
		return
	}

	// Build rules from request
	rules := &types.ChannelRules{
		Public:               req.Public,
		GroupMappings:        req.GroupMappings,
		Default:              req.Default,
		PublishPublic:        req.PublishPublic,
		PublishGroupMappings: req.PublishGroupMappings,
		PublishDefault:       req.PublishDefault,
	}

	// Initialize nil slices/maps to empty values
	if rules.Public == nil {
		rules.Public = []string{}
	}
	if rules.GroupMappings == nil {
		rules.GroupMappings = make(map[string][]string)
	}
	if rules.Default == nil {
		rules.Default = []string{}
	}
	if rules.PublishPublic == nil {
		rules.PublishPublic = []string{}
	}
	if rules.PublishGroupMappings == nil {
		rules.PublishGroupMappings = make(map[string][]string)
	}
	if rules.PublishDefault == nil {
		rules.PublishDefault = []string{}
	}

	// Validate rules
	if err := rules.Validate(); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "VALIDATION_FAILED", err.Error())
		return
	}

	// Set via service (upsert)
	if err := h.service.SetChannelRules(r.Context(), tenantSlug, rules); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to set channel rules")
		h.writeServiceError(w, err, "SET_FAILED", "Failed to set channel rules")
		return
	}

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, tenantSlug).
		Int("public_patterns", len(rules.Public)).
		Int("group_mappings", len(rules.GroupMappings)).
		Int("publish_public_patterns", len(rules.PublishPublic)).
		Int("publish_group_mappings", len(rules.PublishGroupMappings)).
		Msg("Channel rules set")

	_ = httputil.WriteJSON(w, http.StatusOK, rules)
}

// DeleteChannelRules deletes channel rules for a tenant.
// DELETE /tenants/{tenantSlug}/channel-rules
func (h *Handler) DeleteChannelRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	if err := h.service.DeleteChannelRules(r.Context(), tenantSlug); err != nil {
		if errors.Is(err, types.ErrChannelRulesNotFound) {
			httputil.WriteError(w, http.StatusNotFound, "NOT_FOUND", "Channel rules not configured for this tenant")
			return
		}
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to delete channel rules")
		h.writeServiceError(w, err, "DELETE_FAILED", "Failed to delete channel rules")
		return
	}

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, tenantSlug).
		Msg("Channel rules deleted")

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// TestAccess tests what channel patterns a set of groups would have access to.
// POST /tenants/{tenantSlug}/test-access
func (h *Handler) TestAccess(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.TestAccessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON body")
		return
	}

	// Get channel rules
	rules, err := h.service.GetChannelRules(r.Context(), tenantSlug)
	if err != nil {
		if errors.Is(err, types.ErrChannelRulesNotFound) {
			// No rules configured - return empty patterns
			_ = httputil.WriteJSON(w, http.StatusOK, provisioning.TestAccessResponse{
				AllowedPatterns: []string{},
			})
			return
		}
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to get channel rules for access test")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_FAILED", "Failed to get channel rules")
		return
	}

	// Compute allowed patterns for the given groups
	allowedPatterns := rules.Rules.ComputeAllowedPatterns(req.Groups)

	_ = httputil.WriteJSON(w, http.StatusOK, provisioning.TestAccessResponse{
		AllowedPatterns: allowedPatterns,
	})
}
