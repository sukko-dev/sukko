package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// NOTE: httputil.WriteJSON errors are assigned to _ throughout this file.
// If WriteJSON fails, the client has disconnected and the HTTP response is
// already committed — there is no way to communicate a secondary error.

// validProviders is the set of allowed push credential providers.
var validProviders = map[string]bool{
	"fcm":   true,
	"apns":  true,
	"vapid": true,
}

// PushCredentialHandler handles push credential upload and deletion.
type PushCredentialHandler struct {
	credentialsRepo *repository.CredentialsRepository
	resolveTenant   provisioning.TenantLookupFunc
	eventBus        *eventbus.Bus
	logger          zerolog.Logger
	// editionManager gates fcm/apns credentials behind MobilePush (Enterprise,
	// ADR-0009); vapid is covered by the route-level WebPush gate. Nil skips
	// the per-provider check (same graceful pattern as the route middleware).
	editionManager *license.Manager
}

// NewPushCredentialHandler creates a PushCredentialHandler. resolveTenant maps the
// client-supplied tenant slug to the tenant UUID written to the (UUID-typed) DB columns.
func NewPushCredentialHandler(
	credentialsRepo *repository.CredentialsRepository,
	resolveTenant provisioning.TenantLookupFunc,
	eventBus *eventbus.Bus,
	logger zerolog.Logger,
	editionManager *license.Manager,
) (*PushCredentialHandler, error) {
	if credentialsRepo == nil {
		return nil, errNilCredentialsRepo
	}
	if resolveTenant == nil {
		return nil, errNilTenantResolver
	}
	if eventBus == nil {
		return nil, errNilEventBus
	}
	return &PushCredentialHandler{
		credentialsRepo: credentialsRepo,
		resolveTenant:   resolveTenant,
		eventBus:        eventBus,
		logger:          logger.With().Str("component", "push_credential_handler").Logger(),
		editionManager:  editionManager,
	}, nil
}

// resolveTenantUUID resolves a tenant slug to its UUID via lookup, writing the mapped error
// response and returning ok=false on failure. Mirrors RequireTenant's error mapping
// (middleware.go): an unknown slug → 404 TENANT_NOT_FOUND; any other (transient) error → 503
// — a transient dependency failure MUST NOT be reported as a client 404 (§III).
func resolveTenantUUID(
	ctx context.Context,
	lookup provisioning.TenantLookupFunc,
	slug string,
	w http.ResponseWriter,
	logger zerolog.Logger,
) (string, bool) {
	tenant, err := lookup(ctx, slug)
	if err != nil {
		if errors.Is(err, provisioning.ErrTenantNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeTenantNotFound, "Tenant not found")
			return "", false
		}
		logger.Error().Err(err).Str(logging.LogKeyTenantSlug, slug).Msg("resolve tenant slug failed")
		httputil.WriteError(w, http.StatusServiceUnavailable, errCodeServiceUnavailable, "Service temporarily unavailable")
		return "", false
	}
	return tenant.ID, true
}

// uploadCredentialsRequest is the JSON body for POST /api/v1/push/credentials.
// tenant_id is the tenant SLUG; the server resolves it to the UUID stored at rest.
type uploadCredentialsRequest struct {
	TenantID       string `json:"tenant_id"`
	Provider       string `json:"provider"`
	CredentialData string `json:"credential_data"`
}

// uploadCredentialsResponse is the JSON body returned on successful upload.
// tenant_id echoes the input slug (never the internal UUID).
type uploadCredentialsResponse struct {
	ID       int64  `json:"id"`
	Provider string `json:"provider"`
	TenantID string `json:"tenant_id"`
}

// deleteCredentialsRequest is the JSON body for DELETE /api/v1/push/credentials.
type deleteCredentialsRequest struct {
	TenantID string `json:"tenant_id"`
	Provider string `json:"provider"`
}

// maxCredentialBodySize is the maximum request body size for credential operations (1MB).
const maxCredentialBodySize = 1 << 20

// HandleUploadCredentials handles POST /api/v1/push/credentials.
func (h *PushCredentialHandler) HandleUploadCredentials(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxCredentialBodySize)
	var req uploadCredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
		return
	}

	if req.TenantID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingTenantID, "tenant_id is required")
		return
	}
	if req.Provider == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingProvider, "provider is required")
		return
	}
	if !validProviders[req.Provider] {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidProvider, "provider must be one of: fcm, apns, vapid")
		return
	}

	// fcm/apns credentials are Enterprise (MobilePush, ADR-0009). The route-level
	// gate covers WebPush (Pro); this per-provider check is the finer boundary.
	if (req.Provider == "fcm" || req.Provider == "apns") &&
		h.editionManager != nil && !h.editionManager.HasFeature(license.MobilePush) {
		httputil.WriteError(w, http.StatusForbidden, errCodeEditionLimit,
			"fcm/apns push credentials require Enterprise edition")
		return
	}
	if req.CredentialData == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingCredentialData, "credential_data is required")
		return
	}
	if err := validateCredentialData(req.Provider, req.CredentialData); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidCredentialData, err.Error())
		return
	}

	// Connectivity test: FCM OAuth2 token exchange. Runs during credential validation,
	// before tenant resolution — so its log line carries the slug only.
	if req.Provider == "fcm" {
		if err := testFCMConnectivity(req.CredentialData); err != nil {
			h.logger.Warn().Err(err).
				Str(logging.LogKeyTenantSlug, req.TenantID).
				Msg("FCM connectivity test failed")
			httputil.WriteError(w, http.StatusBadRequest, errCodeFCMConnectivity, "FCM connectivity test failed: "+err.Error())
			return
		}
	}
	// APNs: skip in v1 (complex to test without real device token). VAPID: no external service.
	if req.Provider == "apns" {
		h.logger.Warn().
			Str(logging.LogKeyTenantSlug, req.TenantID).
			Msg("APNs connectivity test skipped in v1")
	}

	// Resolve the tenant slug → UUID just before the (UUID-typed) write.
	tenantUUID, ok := resolveTenantUUID(r.Context(), h.resolveTenant, req.TenantID, w, h.logger)
	if !ok {
		return
	}

	cred := &repository.PushCredential{
		TenantID:       tenantUUID,
		Provider:       req.Provider,
		CredentialData: req.CredentialData,
	}

	// Idempotent upsert (mirror gRPC StorePushCredentials): Create, and on a duplicate for the
	// same tenant+provider, Update in place. Update does not return the row ID, so re-Get it for
	// the response.
	if err := h.credentialsRepo.Create(r.Context(), cred); err != nil {
		switch {
		case errors.Is(err, repository.ErrCredentialAlreadyExists):
			if uerr := h.credentialsRepo.Update(r.Context(), cred); uerr != nil {
				h.logger.Error().Err(uerr).
					Str(logging.LogKeyTenantSlug, req.TenantID).
					Str(logging.LogKeyTenantUUID, tenantUUID).
					Str("provider", req.Provider).
					Msg("Failed to update push credential")
				httputil.WriteError(w, http.StatusInternalServerError, errCodeUpsertFailed, "Failed to save push credential")
				return
			}
			// Update succeeded; re-Get only repopulates cred.ID for the response (Update
			// returns no ID). A failed re-Get is non-fatal — the write is durable — so log
			// it and return id:0 rather than failing an already-successful upsert.
			if existing, gerr := h.credentialsRepo.Get(r.Context(), tenantUUID, req.Provider); gerr == nil {
				cred.ID = existing.ID
			} else {
				h.logger.Warn().Err(gerr).
					Str(logging.LogKeyTenantSlug, req.TenantID).
					Str(logging.LogKeyTenantUUID, tenantUUID).
					Str("provider", req.Provider).
					Msg("upsert: re-fetch after update failed; response id will be 0")
			}
		default:
			h.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, req.TenantID).
				Str(logging.LogKeyTenantUUID, tenantUUID).
				Str("provider", req.Provider).
				Msg("Failed to create push credential")
			httputil.WriteError(w, http.StatusInternalServerError, errCodeCreateFailed, "Failed to create push credential")
			return
		}
	}

	h.eventBus.Publish(eventbus.Event{Type: eventbus.PushConfigChanged})

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, req.TenantID).
		Str(logging.LogKeyTenantUUID, tenantUUID).
		Str("provider", req.Provider).
		Int64("id", cred.ID).
		Msg("Push credential created")

	_ = httputil.WriteJSON(w, http.StatusCreated, uploadCredentialsResponse{
		ID:       cred.ID,
		Provider: cred.Provider,
		TenantID: req.TenantID, // echo the input slug, never the internal UUID
	})
}

// HandleDeleteCredentials handles DELETE /api/v1/push/credentials.
func (h *PushCredentialHandler) HandleDeleteCredentials(w http.ResponseWriter, r *http.Request) {
	var req deleteCredentialsRequest

	// Support both query params and JSON body.
	if r.Body != nil && r.ContentLength > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxCredentialBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "Invalid JSON body")
			return
		}
	}

	// Query params override body (if present).
	if v := r.URL.Query().Get("tenant_id"); v != "" {
		req.TenantID = v
	}
	if v := r.URL.Query().Get("provider"); v != "" {
		req.Provider = v
	}

	if req.TenantID == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingTenantID, "tenant_id is required")
		return
	}
	if req.Provider == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeMissingProvider, "provider is required")
		return
	}
	if !validProviders[req.Provider] {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidProvider, "provider must be one of: fcm, apns, vapid")
		return
	}

	tenantUUID, ok := resolveTenantUUID(r.Context(), h.resolveTenant, req.TenantID, w, h.logger)
	if !ok {
		return
	}

	if err := h.credentialsRepo.Delete(r.Context(), tenantUUID, req.Provider); err != nil {
		if errors.Is(err, repository.ErrCredentialNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, "Push credential not found")
		} else {
			h.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, req.TenantID).
				Str(logging.LogKeyTenantUUID, tenantUUID).
				Str("provider", req.Provider).
				Msg("Failed to delete push credential")
			httputil.WriteError(w, http.StatusInternalServerError, errCodeInternal, "Failed to delete credential")
		}
		return
	}

	h.eventBus.Publish(eventbus.Event{Type: eventbus.PushConfigChanged})

	h.logger.Info().
		Str(logging.LogKeyTenantSlug, req.TenantID).
		Str(logging.LogKeyTenantUUID, tenantUUID).
		Str("provider", req.Provider).
		Msg("Push credential deleted")

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
