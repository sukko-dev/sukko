package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// NOTE: httputil.WriteJSON errors are assigned to _ throughout this file.
// If WriteJSON fails, the client has disconnected and the HTTP response is
// already committed — there is no way to communicate a secondary error.

// Pagination defaults for list endpoints.
const (
	defaultPageLimit = 50  // Default number of items per page
	maxPageLimit     = 100 // Maximum allowed items per page

	// routingRulesMaxPageLimit is the per-page cap for routing rules list responses.
	// Higher than the general maxPageLimit since routing rules are small structs.
	routingRulesMaxPageLimit = 200
)

// API error code constants (Constitution §I — no magic strings).
// All codes that appear more than once, or that callers must match exactly, are named here.
const (
	errCodeInvalidRequest        = "INVALID_REQUEST"
	errCodeNotFound              = "NOT_FOUND"
	errCodeDuplicatePriority     = "ROUTING_RULE_DUPLICATE_PRIORITY"
	errCodeDuplicatePattern      = "ROUTING_RULE_DUPLICATE_PATTERN"
	errCodeTooManyRoutingRules   = "TOO_MANY_ROUTING_RULES"
	errCodeRoutingRuleValidation = "ROUTING_RULE_VALIDATION_ERROR"
	errCodeTopicNotProvisioned   = "TOPIC_NOT_PROVISIONED"
	errCodeInsufficientRole      = "INSUFFICIENT_ROLE"
	errCodeEditionLimit          = "EDITION_LIMIT"

	// Middleware / auth codes.
	errCodeUnauthorized      = "UNAUTHORIZED"
	errCodeMissingToken      = "MISSING_TOKEN"
	errCodeInvalidAuthHeader = "INVALID_AUTH_HEADER"
	errCodeTokenExpired      = "TOKEN_EXPIRED"
	errCodeKeyRevoked        = "KEY_REVOKED"
	errCodeInvalidToken      = "INVALID_TOKEN"
	errCodeNotAuthenticated  = "NOT_AUTHENTICATED"
	errCodeTenantMismatch    = "TENANT_MISMATCH"
	errCodeRateLimited       = "RATE_LIMITED"

	// writeServiceError sentinel-mapped codes.
	errCodeSlugAlreadyTaken     = "SLUG_ALREADY_TAKEN"
	errCodeSlugReserved         = "SLUG_RESERVED"
	errCodeSlugInvalid          = "SLUG_INVALID"
	errCodeSlugUnchanged        = "SLUG_UNCHANGED"
	errCodeSlugRenameInProgress = "SLUG_RENAME_IN_PROGRESS"
	errCodeSlugNotPatchable     = "SLUG_NOT_PATCHABLE"
	errCodeServiceUnavailable   = "SERVICE_UNAVAILABLE"
	errCodeTenantNotFound       = "TENANT_NOT_FOUND"
	errCodeTenantDeleted        = "TENANT_DELETED"
	errCodeTenantNotActive      = "TENANT_NOT_ACTIVE"
	errCodeKeyNotOwned          = "KEY_NOT_OWNED"
	errCodeAPIKeyNotFound       = "API_KEY_NOT_FOUND" //nolint:gosec // G101 false positive: "KEY" in name triggers credential heuristic; this is an API error code string, not a secret
	errCodeAPIKeyNotOwned       = "API_KEY_NOT_OWNED" //nolint:gosec // G101 false positive: same as above
	errCodeQuotaNotFound        = "QUOTA_NOT_FOUND"
	errCodeKeyNotFound          = "KEY_NOT_FOUND"
	errCodeInvalidQuota         = "INVALID_QUOTA"
	errCodeFeatureNotConfigured = "FEATURE_NOT_CONFIGURED"
	errCodeAddRoutingRuleFailed = "ADD_ROUTING_RULE_FAILED"
	errCodeGetTenantFailed      = "GET_TENANT_FAILED"
	errCodeInternal             = "INTERNAL_ERROR"
	errCodeNameInvalid          = "NAME_INVALID"
	errCodeConsumerTypeInvalid  = "CONSUMER_TYPE_INVALID"
	errCodeKeyInvalid           = "KEY_INVALID"

	// Push credential / channel error codes (§I — named; referenced by the push handlers).
	errCodeMissingTenantID       = "MISSING_TENANT_ID"
	errCodeMissingProvider       = "MISSING_PROVIDER"
	errCodeInvalidProvider       = "INVALID_PROVIDER"
	errCodeMissingCredentialData = "MISSING_CREDENTIAL_DATA"
	errCodeInvalidCredentialData = "INVALID_CREDENTIAL_DATA" //nolint:gosec // G101 false positive: API error-code string, not a credential
	errCodeFCMConnectivity       = "FCM_CONNECTIVITY_FAILED"
	errCodeCreateFailed          = "CREATE_FAILED"
	errCodeUpdateFailed          = "UPDATE_FAILED"
	errCodeCreateKeyFailed       = "CREATE_KEY_FAILED"
	errCodeKeyAlreadyExists      = "KEY_ALREADY_EXISTS"
	errCodeRenameFailed          = "RENAME_FAILED"
	errCodeMissingPatterns       = "MISSING_PATTERNS"
	errCodeInvalidPattern        = "INVALID_PATTERN"
	errCodeInvalidUrgency        = "INVALID_URGENCY"
	errCodeUpsertFailed          = "UPSERT_FAILED"
)

// Response status string constants (Constitution §I — JSON response type codes appearing in
// multiple handlers must be named constants to prevent silent divergence on rename).
const (
	statusDeleted = "deleted"
	statusRevoked = "revoked"
)

// Handler provides HTTP handlers for provisioning operations.
type Handler struct {
	service *provisioning.Service
	cfg     platform.ProvisioningConfig
	logger  zerolog.Logger
}

// NewHandler creates a new Handler.
func NewHandler(svc *provisioning.Service, cfg platform.ProvisioningConfig, logger zerolog.Logger) (*Handler, error) {
	if svc == nil {
		return nil, errors.New("handler: service is required")
	}
	return &Handler{
		service: svc,
		cfg:     cfg,
		logger:  logger,
	}, nil
}

// classifyServiceError maps a service error to its HTTP status, wire code, and message.
// Known sentinels map to 4xx; anything unrecognized falls back to 500 with the caller's
// fallbackCode/fallbackMsg. It is the single source of truth for both the response body and
// the log level (see logServiceError) so the two cannot diverge.
func classifyServiceError(err error, fallbackCode, fallbackMsg string) (status int, code, msg string) {
	switch {
	case errors.Is(err, provisioning.ErrSlugAlreadyTaken):
		return http.StatusConflict, errCodeSlugAlreadyTaken, "Slug already taken"
	case errors.Is(err, provisioning.ErrKeyAlreadyExists):
		return http.StatusConflict, errCodeKeyAlreadyExists, "A key with this key_id already exists"
	case errors.Is(err, provisioning.ErrSlugReserved):
		return http.StatusBadRequest, errCodeSlugReserved, "Slug is reserved"
	case errors.Is(err, provisioning.ErrSlugInvalid):
		return http.StatusBadRequest, errCodeSlugInvalid, err.Error()
	case errors.Is(err, provisioning.ErrNameInvalid):
		return http.StatusBadRequest, errCodeNameInvalid, err.Error()
	case errors.Is(err, provisioning.ErrConsumerTypeInvalid):
		return http.StatusBadRequest, errCodeConsumerTypeInvalid, err.Error()
	case errors.Is(err, provisioning.ErrKeyInvalid):
		return http.StatusBadRequest, errCodeKeyInvalid, err.Error()
	case errors.Is(err, provisioning.ErrSlugUnchanged):
		return http.StatusBadRequest, errCodeSlugUnchanged, "New slug is identical to current slug"
	case errors.Is(err, provisioning.ErrSlugRenameInProgress):
		return http.StatusConflict, errCodeSlugRenameInProgress, "Slug rename already in progress or within hold period"
	case errors.Is(err, provisioning.ErrSlugImmutableViaPatch):
		return http.StatusBadRequest, errCodeSlugNotPatchable, "Slug cannot be changed via PATCH; use the /rename endpoint"
	case errors.Is(err, provisioning.ErrTenantNotFound):
		return http.StatusNotFound, errCodeTenantNotFound, "Tenant not found"
	case errors.Is(err, provisioning.ErrTenantDeleted):
		return http.StatusConflict, errCodeTenantDeleted, "Cannot modify deleted tenant"
	case errors.Is(err, provisioning.ErrTenantNotActive):
		return http.StatusConflict, errCodeTenantNotActive, "Tenant is not active"
	case errors.Is(err, provisioning.ErrKeyNotOwnedByTenant):
		return http.StatusForbidden, errCodeKeyNotOwned, "Key does not belong to tenant"
	case errors.Is(err, provisioning.ErrAPIKeyNotFound):
		return http.StatusNotFound, errCodeAPIKeyNotFound, "API key not found"
	case errors.Is(err, provisioning.ErrAPIKeyNotOwnedByTenant):
		return http.StatusForbidden, errCodeAPIKeyNotOwned, "API key does not belong to tenant"
	case errors.Is(err, provisioning.ErrQuotaNotFound):
		return http.StatusNotFound, errCodeQuotaNotFound, "Quota not found"
	case errors.Is(err, provisioning.ErrKeyNotFound):
		return http.StatusNotFound, errCodeKeyNotFound, "Key not found"
	case errors.Is(err, provisioning.ErrInvalidQuota):
		return http.StatusBadRequest, errCodeInvalidQuota, err.Error()
	case errors.Is(err, provisioning.ErrChannelRulesNotConfigured),
		errors.Is(err, provisioning.ErrRoutingRulesNotConfigured):
		return http.StatusNotImplemented, errCodeFeatureNotConfigured, "Feature store not configured"
	case errors.Is(err, provisioning.ErrTooManyRoutingRules):
		return http.StatusBadRequest, errCodeTooManyRoutingRules, "Too many routing rules"
	case isEditionError(err):
		return editionErrorResponse(err)
	default:
		return http.StatusInternalServerError, fallbackCode, fallbackMsg
	}
}

// writeServiceError classifies err and writes the response. Retained for the ~11 handlers
// that do not yet log off the classified status; in-scope handlers (create/update/key-create/
// rename) call classifyServiceError directly so they classify exactly once (log + response).
func (h *Handler) writeServiceError(w http.ResponseWriter, err error, code, msg string) {
	status, wireCode, wireMsg := classifyServiceError(err, code, msg)
	httputil.WriteError(w, status, wireCode, wireMsg)
}

// logServiceError logs a service error at a level driven by its classified HTTP status:
// client errors (4xx) are Warn, internal failures (5xx) are Error.
func (h *Handler) logServiceError(status int, err error, op, slug string) {
	evt := h.logger.Warn()
	if status >= http.StatusInternalServerError {
		evt = h.logger.Error()
	}
	evt.Err(err).Str(logging.LogKeyTenantSlug, slug).Msg(op)
}

// isEditionError returns true if the error is a license edition limit or feature error.
func isEditionError(err error) bool {
	_, isLimit := errors.AsType[*license.EditionLimitError](err)
	_, isFeature := errors.AsType[*license.EditionFeatureError](err)
	return isLimit || isFeature
}

// editionErrorResponse maps a license edition limit/feature error to a 403 response triple.
// Both edition error types normalise to the "EDITION_LIMIT" wire code, matching the
// RequireFeature middleware.
func editionErrorResponse(err error) (status int, code, msg string) {
	if limitErr, ok := errors.AsType[*license.EditionLimitError](err); ok {
		return http.StatusForbidden, limitErr.Code(), limitErr.Error()
	}
	if featureErr, ok := errors.AsType[*license.EditionFeatureError](err); ok {
		return http.StatusForbidden, errCodeEditionLimit, featureErr.Error()
	}
	return http.StatusInternalServerError, errCodeInternal, "edition error"
}

// Health returns basic health status.
func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready returns readiness status (database connectivity).
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.service.Ready(r.Context()); err != nil {
		h.logger.Error().Err(err).Msg("Readiness check failed")
		httputil.WriteError(w, http.StatusServiceUnavailable, "NOT_READY", "Database not available")
		return
	}
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// Metrics returns Prometheus metrics.
func (h *Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

// CreateTenant creates a new tenant.
func (h *Handler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req provisioning.CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "CreateTenant").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	resp, err := h.service.CreateTenant(r.Context(), req)
	if err != nil {
		status, code, msg := classifyServiceError(err, errCodeCreateFailed, "Failed to create tenant")
		h.logServiceError(status, err, "Failed to create tenant", req.Slug)
		RecordTenantOperation(tenantOpCreate, pkgmetrics.ResultError)
		httputil.WriteError(w, status, code, msg)
		return
	}

	RecordTenantCreated()
	RecordTenantOperation(tenantOpCreate, pkgmetrics.ResultSuccess)
	h.logger.Info().Str(logging.LogKeyTenantSlug, req.Slug).Str("operation", tenantOpCreate).Msg("tenant create succeeded") // LOG-017
	_ = httputil.WriteJSON(w, http.StatusCreated, resp)
}

// GetTenant retrieves a tenant by slug.
func (h *Handler) GetTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	tenant, err := h.service.GetTenantBySlug(r.Context(), tenantSlug)
	if err != nil {
		if errors.Is(err, provisioning.ErrTenantNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeTenantNotFound, "Tenant not found")
			return
		}
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to get tenant")
		httputil.WriteError(w, http.StatusInternalServerError, errCodeGetTenantFailed, "Failed to get tenant")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, tenant)
}

// ListTenants returns a paginated list of tenants.
func (h *Handler) ListTenants(w http.ResponseWriter, r *http.Request) {
	opts := parseListOptions(r)

	tenants, total, err := h.service.ListTenants(r.Context(), opts)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list tenants")
		httputil.WriteError(w, http.StatusInternalServerError, "LIST_FAILED", "Failed to list tenants")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  tenants,
		"total":  total,
		"limit":  opts.Limit,
		"offset": opts.Offset,
	})
}

// UpdateTenant updates tenant metadata.
func (h *Handler) UpdateTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.UpdateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "UpdateTenant").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	if req.Slug != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeSlugNotPatchable, "Slug cannot be changed via PATCH; use POST /rename")
		return
	}

	tenant, err := h.service.UpdateTenant(r.Context(), tenantSlug, req)
	if err != nil {
		status, code, msg := classifyServiceError(err, errCodeUpdateFailed, "Failed to update tenant")
		h.logServiceError(status, err, "Failed to update tenant", tenantSlug)
		httputil.WriteError(w, status, code, msg)
		return
	}

	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("operation", "update").Msg("tenant update succeeded") // LOG-017
	_ = httputil.WriteJSON(w, http.StatusOK, tenant)
}

// SuspendTenant suspends a tenant.
func (h *Handler) SuspendTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	if err := h.service.SuspendTenant(r.Context(), tenantSlug); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to suspend tenant")
		RecordTenantOperation(tenantOpSuspend, pkgmetrics.ResultError)
		h.writeServiceError(w, err, "SUSPEND_FAILED", "Failed to suspend tenant")
		return
	}

	RecordTenantOperation(tenantOpSuspend, pkgmetrics.ResultSuccess)
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("operation", tenantOpSuspend).Msg("tenant suspend succeeded") // LOG-017
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": string(provisioning.StatusSuspended)})
}

// ReactivateTenant reactivates a suspended tenant.
func (h *Handler) ReactivateTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	if err := h.service.ReactivateTenant(r.Context(), tenantSlug); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to reactivate tenant")
		RecordTenantOperation(tenantOpReactivate, pkgmetrics.ResultError)
		h.writeServiceError(w, err, "REACTIVATE_FAILED", "Failed to reactivate tenant")
		return
	}

	RecordTenantOperation(tenantOpReactivate, pkgmetrics.ResultSuccess)
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("operation", tenantOpReactivate).Msg("tenant reactivate succeeded") // LOG-017
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": string(provisioning.StatusActive)})
}

// DeprovisionTenant initiates tenant deletion. With ?force=true, deletes immediately
// (no grace period, no reactivation). Without force, uses the standard grace period.
func (h *Handler) DeprovisionTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	force := r.URL.Query().Get("force") == "true"

	if err := h.service.DeprovisionTenant(r.Context(), tenantSlug, force); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Bool("force", force).Msg("Failed to deprovision tenant")
		RecordTenantOperation(tenantOpDeprovision, pkgmetrics.ResultError)
		h.writeServiceError(w, err, "DEPROVISION_FAILED", "Failed to deprovision tenant")
		return
	}

	status := string(provisioning.StatusDeprovisioning)
	if force {
		status = statusDeleted
	}
	RecordTenantOperation(tenantOpDeprovision, pkgmetrics.ResultSuccess)
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("operation", tenantOpDeprovision).Msg("tenant deprovision succeeded") // LOG-017
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": status})                                                       // WriteJSON error = broken client connection; nothing actionable
}

// CreateKey registers a new public key.
func (h *Handler) CreateKey(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.CreateKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "CreateKey").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	key, err := h.service.CreateKey(r.Context(), tenantSlug, req)
	if err != nil {
		status, code, msg := classifyServiceError(err, errCodeCreateKeyFailed, "Failed to create key")
		h.logServiceError(status, err, "Failed to create key", tenantSlug)
		httputil.WriteError(w, status, code, msg)
		return
	}

	RecordKeyCreated()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", req.KeyID).Str("operation", "create").Msg("key create succeeded") // LOG-018
	_ = httputil.WriteJSON(w, http.StatusCreated, key)
}

// ListKeys returns keys for a tenant with pagination.
func (h *Handler) ListKeys(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	opts := parseListOptions(r)

	keys, total, err := h.service.ListKeys(r.Context(), tenantSlug, opts)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to list keys")
		httputil.WriteError(w, http.StatusInternalServerError, "LIST_KEYS_FAILED", "Failed to list keys")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  keys,
		"total":  total,
		"limit":  opts.Limit,
		"offset": opts.Offset,
	})
}

// RevokeKey revokes a key.
func (h *Handler) RevokeKey(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	keyID := chi.URLParam(r, "keyID")

	if err := h.service.RevokeKey(r.Context(), tenantSlug, keyID); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", keyID).Msg("Failed to revoke key")
		h.writeServiceError(w, err, "REVOKE_KEY_FAILED", "Failed to revoke key")
		return
	}

	RecordKeyRevoked()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", keyID).Str("operation", "revoke").Msg("key revoke succeeded") // LOG-018
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": statusRevoked})
}

// GetActiveKeys returns all active keys (for WS Gateway).
func (h *Handler) GetActiveKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.service.GetActiveKeys(r.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get active keys")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_KEYS_FAILED", "Failed to get active keys")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"keys": keys,
	})
}

// GetRoutingRules returns paginated routing rules for a tenant.
func (h *Handler) GetRoutingRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	limit, offset := parsePagination(r, defaultPageLimit, routingRulesMaxPageLimit)

	rules, total, err := h.service.ListRoutingRules(r.Context(), tenantSlug, limit, offset)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to list routing rules")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_ROUTING_RULES_FAILED", "Failed to retrieve routing rules")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  rules,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// AddRoutingRule adds a single routing rule for a tenant.
func (h *Handler) AddRoutingRule(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.AddRoutingRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "AddRoutingRule").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed")
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	if err := h.service.AddRoutingRule(r.Context(), tenantSlug, req.Rule); err != nil {
		switch {
		case errors.Is(err, provisioning.ErrDuplicatePriority):
			httputil.WriteError(w, http.StatusConflict, errCodeDuplicatePriority, "A routing rule with this priority already exists")
		case errors.Is(err, provisioning.ErrDuplicateRoutingPattern):
			httputil.WriteError(w, http.StatusConflict, errCodeDuplicatePattern, "A routing rule with this pattern already exists")
		case errors.Is(err, provisioning.ErrTopicNotProvisioned):
			httputil.WriteError(w, http.StatusBadRequest, errCodeTopicNotProvisioned, err.Error())
		case errors.Is(err, provisioning.ErrInvalidRoutingPattern),
			errors.Is(err, provisioning.ErrMissingIngressTopic),
			errors.Is(err, provisioning.ErrTooManyTopics),
			errors.Is(err, provisioning.ErrReservedTopicSuffix),
			errors.Is(err, provisioning.ErrEgressIngressOverlap),
			errors.Is(err, provisioning.ErrDuplicateEgressTopic):
			httputil.WriteError(w, http.StatusBadRequest, errCodeRoutingRuleValidation, err.Error())
		default:
			// Classify once so the log level follows the status: the Community rule-count
			// 403 is an expected client outcome (the cap is the tier wall since ADR-0014),
			// not an internal failure, and it now fires on every grid run.
			status, code, msg := classifyServiceError(err, errCodeAddRoutingRuleFailed, "Failed to add routing rule")
			h.logServiceError(status, err, "Failed to add routing rule", tenantSlug)
			httputil.WriteError(w, status, code, msg)
		}
		return
	}

	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("pattern", req.Rule.Pattern).Int("priority", req.Rule.Priority).Msg("routing rule added")

	_ = httputil.WriteJSON(w, http.StatusCreated, map[string]any{
		"rule": req.Rule,
	})
}

// ReplaceRoutingRules atomically replaces all routing rules for a tenant.
func (h *Handler) ReplaceRoutingRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.ReplaceRoutingRulesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "ReplaceRoutingRules").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	// Boundary validation (defense in depth — Constitution II).
	if err := provisioning.ValidateRoutingRules(req.Rules, h.cfg.MaxRoutingRulesPerTenant, h.cfg.MaxTopicsPerRule); err != nil {
		switch {
		case errors.Is(err, provisioning.ErrTooManyRoutingRules):
			httputil.WriteError(w, http.StatusBadRequest, errCodeTooManyRoutingRules, err.Error())
		default:
			httputil.WriteError(w, http.StatusBadRequest, errCodeRoutingRuleValidation, err.Error())
		}
		return
	}

	if err := h.service.ReplaceRoutingRules(r.Context(), tenantSlug, req.Rules); err != nil {
		switch {
		case errors.Is(err, provisioning.ErrTopicNotProvisioned):
			httputil.WriteError(w, http.StatusBadRequest, errCodeTopicNotProvisioned, err.Error())
		case errors.Is(err, provisioning.ErrDuplicateRoutingPattern):
			httputil.WriteError(w, http.StatusBadRequest, errCodeDuplicatePattern, "Routing rules contain duplicate patterns")
		default:
			// Classify once — see AddRoutingRule: the rule-count 403 is a client outcome.
			status, code, msg := classifyServiceError(err, "SET_ROUTING_RULES_FAILED", "Failed to replace routing rules")
			h.logServiceError(status, err, "Failed to replace routing rules", tenantSlug)
			httputil.WriteError(w, status, code, msg)
		}
		return
	}

	RecordRoutingRulesSet()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Int("rule_count", len(req.Rules)).Msg("routing rules replaced") // LOG-019

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  req.Rules,
		"total":  len(req.Rules),
		"limit":  len(req.Rules),
		"offset": 0,
	})
}

// DeleteRoutingRules deletes all routing rules for a tenant (idempotent).
func (h *Handler) DeleteRoutingRules(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	if err := h.service.DeleteRoutingRules(r.Context(), tenantSlug); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to delete routing rules")
		httputil.WriteError(w, http.StatusInternalServerError, "DELETE_ROUTING_RULES_FAILED", "Failed to delete routing rules")
		return
	}

	RecordRoutingRulesDeleted()
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": statusDeleted})
}

// GetQuota returns quotas for a tenant.
func (h *Handler) GetQuota(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	quota, err := h.service.GetQuota(r.Context(), tenantSlug)
	if err != nil {
		if errors.Is(err, provisioning.ErrQuotaNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeQuotaNotFound, "Quota not found")
			return
		}
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to get quota")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_QUOTA_FAILED", "Failed to get quota")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, quota)
}

// UpdateQuota updates quotas for a tenant.
func (h *Handler) UpdateQuota(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.UpdateQuotaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "UpdateQuota").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	quota, err := h.service.UpdateQuota(r.Context(), tenantSlug, req)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to update quota")
		h.writeServiceError(w, err, "UPDATE_QUOTA_FAILED", "Failed to update quota")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, quota)
}

// GetAuditLog returns audit entries for a tenant.
func (h *Handler) GetAuditLog(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	opts := parseListOptions(r)

	entries, total, err := h.service.GetAuditLog(r.Context(), tenantSlug, opts)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to get audit log")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_AUDIT_FAILED", "Failed to get audit log")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  entries,
		"total":  total,
		"limit":  opts.Limit,
		"offset": opts.Offset,
	})
}

// CreateAPIKey creates a new API key for a tenant.
func (h *Handler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "CreateAPIKey").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed") // LOG-023
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	key, err := h.service.CreateAPIKey(r.Context(), tenantSlug, req)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to create API key")
		h.writeServiceError(w, err, "CREATE_API_KEY_FAILED", "Failed to create API key")
		return
	}

	RecordAPIKeyCreated()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", key.KeyID).Str("operation", "create").Msg("key create succeeded") // LOG-018
	_ = httputil.WriteJSON(w, http.StatusCreated, key)
}

// ListAPIKeys returns API keys for a tenant with pagination.
func (h *Handler) ListAPIKeys(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	opts := parseListOptions(r)

	keys, total, err := h.service.ListAPIKeys(r.Context(), tenantSlug, opts)
	if err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Msg("Failed to list API keys")
		httputil.WriteError(w, http.StatusInternalServerError, "LIST_API_KEYS_FAILED", "Failed to list API keys")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  keys,
		"total":  total,
		"limit":  opts.Limit,
		"offset": opts.Offset,
	})
}

// RevokeAPIKey revokes an API key.
func (h *Handler) RevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")
	keyID := chi.URLParam(r, "keyID")

	if err := h.service.RevokeAPIKey(r.Context(), tenantSlug, keyID); err != nil {
		h.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", keyID).Msg("Failed to revoke API key")
		h.writeServiceError(w, err, "REVOKE_API_KEY_FAILED", "Failed to revoke API key")
		return
	}

	RecordAPIKeyRevoked()
	h.logger.Info().Str(logging.LogKeyTenantSlug, tenantSlug).Str("key_id", keyID).Str("operation", "revoke").Msg("key revoke succeeded") // LOG-018
	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": statusRevoked})
}

// RenameTenant renames a tenant's slug (Kafka namespace and URL identifier).
// POST /tenants/{tenantSlug}/rename
func (h *Handler) RenameTenant(w http.ResponseWriter, r *http.Request) {
	tenantSlug := chi.URLParam(r, "tenantSlug")

	var req provisioning.RenameTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn().Err(err).Str("handler", "RenameTenant").Str("remote_addr", r.RemoteAddr).Msg("request body parse failed")
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	tenant, err := h.service.RenameTenant(r.Context(), tenantSlug, req.Slug)
	if err != nil {
		status, code, msg := classifyServiceError(err, errCodeRenameFailed, "Failed to rename tenant")
		// Level-aware log (client 4xx → Warn, 5xx → Error), preserving the new_slug field.
		evt := h.logger.Warn()
		if status >= http.StatusInternalServerError {
			evt = h.logger.Error()
		}
		evt.Err(err).Str(logging.LogKeyTenantSlug, tenantSlug).Str("new_slug", req.Slug).Msg("Failed to rename tenant")
		httputil.WriteError(w, status, code, msg)
		return
	}

	h.logger.Info().
		Str("old_slug", tenantSlug).
		Str("new_slug", req.Slug).
		Str("operation", "rename").
		Msg("tenant rename succeeded")
	_ = httputil.WriteJSON(w, http.StatusOK, tenant)
}

// GetActiveAPIKeys returns all active API keys (for gateway).
func (h *Handler) GetActiveAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.service.GetActiveAPIKeys(r.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get active API keys")
		httputil.WriteError(w, http.StatusInternalServerError, "GET_API_KEYS_FAILED", "Failed to get active API keys")
		return
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"keys": keys,
	})
}

// parseListOptions extracts pagination options from query params.
// parsePagination extracts limit and offset from query parameters,
// clamping limit to [1, maxLimit] and defaulting to defaultLimit.
func parsePagination(r *http.Request, defaultLimit, maxLimit int) (limit, offset int) {
	limit = defaultLimit
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = min(n, maxLimit)
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}

func parseListOptions(r *http.Request) provisioning.ListOptions {
	opts := provisioning.ListOptions{
		Limit:  defaultPageLimit,
		Offset: 0,
	}

	if limit := r.URL.Query().Get("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 && l <= maxPageLimit {
			opts.Limit = l
		}
	}

	if offset := r.URL.Query().Get("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o >= 0 {
			opts.Offset = o
		}
	}

	if status := r.URL.Query().Get("status"); status != "" {
		s := provisioning.TenantStatus(status)
		if s.IsValid() {
			opts.Status = &s
		}
	}

	return opts
}
