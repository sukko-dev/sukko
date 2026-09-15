package api

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

// NOTE: httputil.WriteJSON errors are assigned to _ throughout this file.
// If WriteJSON fails, the client has disconnected and the HTTP response is
// already committed — there is no way to communicate a secondary error.

// AdminKeysHandler handles admin key registration, revocation, and listing.
type AdminKeysHandler struct {
	repo     *repository.AdminKeyRepository
	registry *provauth.AdminKeyRegistry
	eventBus *eventbus.Bus
	logger   zerolog.Logger
}

// NewAdminKeysHandler creates an AdminKeysHandler.
func NewAdminKeysHandler(
	repo *repository.AdminKeyRepository,
	registry *provauth.AdminKeyRegistry,
	eventBus *eventbus.Bus,
	logger zerolog.Logger,
) *AdminKeysHandler {
	return &AdminKeysHandler{
		repo:     repo,
		registry: registry,
		eventBus: eventBus,
		logger:   logger.With().Str("component", "admin_keys_handler").Logger(),
	}
}

// registerRequest is the JSON body for POST /api/v1/admin/keys.
type registerRequest struct {
	Name      string `json:"name"`
	Algorithm string `json:"algorithm"`
	PublicKey string `json:"public_key"` // PEM-encoded
}

// adminKeyResponse is the JSON response for admin key operations.
type adminKeyResponse struct {
	KeyID        string  `json:"key_id"`
	Name         string  `json:"name"`
	Algorithm    string  `json:"algorithm"`
	PublicKey    string  `json:"public_key"`
	RegisteredBy string  `json:"registered_by"`
	CreatedAt    string  `json:"created_at"`
	RevokedAt    *string `json:"revoked_at,omitempty"`
}

// Register handles POST /api/v1/admin/keys — register a new admin public key.
func (h *AdminKeysHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_JSON", "invalid request body")
		return
	}

	// Default algorithm
	if req.Algorithm == "" {
		req.Algorithm = "Ed25519"
	}

	// Validate key material — fail fast at registration, not at auth time (Constitution II)
	if err := validatePublicKeyMaterial(req.Algorithm, req.PublicKey); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_KEY", err.Error())
		return
	}

	// Default name
	if req.Name == "" {
		req.Name = "unnamed"
	}

	// Determine who is registering (from auth context)
	registeredBy := auth.GetActor(r.Context())

	key := &repository.AdminKey{
		Name:         req.Name,
		Algorithm:    req.Algorithm,
		PublicKey:    req.PublicKey,
		RegisteredBy: registeredBy,
	}

	if err := h.repo.Create(r.Context(), key); err != nil {
		h.logger.Error().Err(err).Str("name", req.Name).Msg("failed to register admin key")
		httputil.WriteError(w, http.StatusInternalServerError, "INTERNAL", "failed to register admin key")
		return
	}

	// Refresh cache + publish event
	h.refreshCache(r.Context())

	h.logger.Info().
		Str("key_id", key.KeyID).
		Str("name", key.Name).
		Str("algorithm", key.Algorithm).
		Str("registered_by", registeredBy).
		Msg("admin key registered")

	_ = httputil.WriteJSON(w, http.StatusCreated, adminKeyResponse{
		KeyID:        key.KeyID,
		Name:         key.Name,
		Algorithm:    key.Algorithm,
		PublicKey:    key.PublicKey,
		RegisteredBy: key.RegisteredBy,
		CreatedAt:    key.CreatedAt.Format("2006-01-02T15:04:05Z"),
	})
}

// Revoke handles DELETE /api/v1/admin/keys/{id} — revoke an admin key.
func (h *AdminKeysHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "id")
	if keyID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "MISSING_KEY_ID", "key ID is required")
		return
	}

	// Guard: don't revoke the last active key
	count, err := h.repo.CountActive(r.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to count active admin keys")
		httputil.WriteError(w, http.StatusInternalServerError, "INTERNAL", "failed to check active key count")
		return
	}
	if count <= 1 {
		httputil.WriteError(w, http.StatusConflict, "LAST_KEY", "cannot revoke the last active admin key")
		return
	}

	if err := h.repo.Revoke(r.Context(), keyID); err != nil {
		h.logger.Error().Err(err).Str("key_id", keyID).Msg("failed to revoke admin key")
		httputil.WriteError(w, http.StatusNotFound, errCodeKeyNotFound, "admin key not found or already revoked")
		return
	}

	// Refresh cache + publish event
	h.refreshCache(r.Context())

	actor := auth.GetActor(r.Context())
	h.logger.Info().
		Str("key_id", keyID).
		Str("revoked_by", actor).
		Msg("admin key revoked")

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"key_id": keyID,
		"status": "revoked",
	})
}

// List handles GET /api/v1/admin/keys — list active admin keys.
func (h *AdminKeysHandler) List(w http.ResponseWriter, r *http.Request) {
	keys, err := h.repo.ListActive(r.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to list admin keys")
		httputil.WriteError(w, http.StatusInternalServerError, "INTERNAL", "failed to list admin keys")
		return
	}

	items := make([]adminKeyResponse, 0, len(keys))
	for _, k := range keys {
		items = append(items, adminKeyResponse{
			KeyID:        k.KeyID,
			Name:         k.Name,
			Algorithm:    k.Algorithm,
			PublicKey:    k.PublicKey,
			RegisteredBy: k.RegisteredBy,
			CreatedAt:    k.CreatedAt.Format("2006-01-02T15:04:05Z"),
		})
	}

	_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": len(items),
	})
}

// refreshCache reloads admin keys from DB into the in-memory registry and publishes an event.
func (h *AdminKeysHandler) refreshCache(ctx context.Context) {
	keys, err := h.repo.ListActive(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to refresh admin key cache")
		return
	}

	keyInfos := make([]*auth.KeyInfo, 0, len(keys))
	for _, k := range keys {
		pubKey, err := provauth.ParsePublicKeyPEM(k.PublicKey)
		if err != nil {
			h.logger.Warn().Err(err).Str("key_id", k.KeyID).Msg("skipping admin key with invalid public key material")
			continue
		}
		keyInfos = append(keyInfos, provauth.AdminKeyToKeyInfo(k.KeyID, k.Name, k.Algorithm, pubKey))
	}

	h.registry.Refresh(keyInfos)
	h.eventBus.Publish(eventbus.Event{Type: eventbus.AdminKeysChanged})
}

// validatePublicKeyMaterial validates that the PEM-encoded key is a parseable public key
// of the expected algorithm. Constitution II: fail fast at registration.
func validatePublicKeyMaterial(algorithm, pemEncoded string) error {
	if pemEncoded == "" {
		return errors.New("public_key is required")
	}

	pubKey, err := provauth.ParsePublicKeyPEM(pemEncoded)
	if err != nil {
		return fmt.Errorf("validate key material: %w", err)
	}

	switch algorithm {
	case "Ed25519":
		if _, ok := pubKey.(ed25519.PublicKey); !ok {
			return errors.New("key material is not a valid Ed25519 public key")
		}
	case "RS256":
		// x509.ParsePKIXPublicKey already validates RSA key structure
	default:
		return fmt.Errorf("unsupported algorithm: %s (supported: Ed25519, RS256)", algorithm)
	}

	return nil
}
