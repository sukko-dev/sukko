package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// Prometheus metrics for license reload.
var (
	licenseReloadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_license_reload_total",
		Help: "Total license reload attempts by result",
	}, []string{"result"})

	licenseEditionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "provisioning_license_edition",
		Help: "Current license edition (1 = active for the labeled edition)",
	}, []string{"edition"})
)

// License reload result labels (Prometheus metric label values).
const (
	reloadSuccess          = "success"
	reloadInvalidSignature = "invalid_signature"
	reloadExpired          = "expired"
	reloadReplay           = "replay_detected"
	reloadInvalidFormat    = "invalid_format"
	reloadInternalError    = "internal_error"
	reloadEditionChange    = "edition_change_rejected"
	reloadRateLimited      = "rate_limited"
)

// editionChangeCode is the API error code returned when a reload is rejected due to an edition change.
const editionChangeCode = "EDITION_CHANGE_REQUIRES_RESTART"

// editionChangeRejectedResponse is the JSON body for HTTP 409 edition-change rejections.
type editionChangeRejectedResponse struct {
	Code             string `json:"code"`
	Message          string `json:"message"`
	CurrentEdition   string `json:"current_edition"`
	RequestedEdition string `json:"requested_edition"`
}

const (
	licenseReloadTokenInterval = 6 * time.Second
	licenseReloadBurst         = 3
)

// LicenseHandler serves the license reload endpoint: admin-authenticated, ECDSA
// signature-verified, per-IP rate limited, and replay-protected via the token's iat.
type LicenseHandler struct {
	manager      *license.Manager
	licenseRepo  provisioning.LicenseStateStore
	eventBus     *eventbus.Bus
	logger       zerolog.Logger
	currentKeyMu sync.Mutex // guards currentKey read/write
	currentKey   string     // raw key string for WatchLicense snapshot
	rateLimiter  *ipRateLimiter
}

// ipRateLimiterMaxEntries caps the per-IP rate limiter map to prevent unbounded growth.
// License reload is very low-traffic; 1000 unique IPs is more than enough.
const ipRateLimiterMaxEntries = 1000

// ipRateLimiter provides per-IP rate limiting for the license endpoint.
type ipRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	rate     rate.Limit
	burst    int
}

func newIPRateLimiter(r rate.Limit, burst int) *ipRateLimiter {
	return &ipRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rate:     r,
		burst:    burst,
	}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	limiter, ok := l.limiters[ip]
	if !ok {
		// Evict all entries when map exceeds cap to prevent unbounded growth.
		// License reload is very low-traffic so this is effectively never hit
		// under normal operation, but prevents a slow leak from port scans or bots.
		if len(l.limiters) >= ipRateLimiterMaxEntries {
			clear(l.limiters)
		}
		limiter = rate.NewLimiter(l.rate, l.burst)
		l.limiters[ip] = limiter
	}
	l.mu.Unlock()
	return limiter.Allow()
}

// NewLicenseHandler creates a LicenseHandler.
func NewLicenseHandler(
	manager *license.Manager,
	licenseRepo provisioning.LicenseStateStore,
	eventBus *eventbus.Bus,
	logger zerolog.Logger,
) *LicenseHandler {
	return &LicenseHandler{
		manager:     manager,
		licenseRepo: licenseRepo,
		eventBus:    eventBus,
		logger:      logger.With().Str("component", "license_handler").Logger(),
		rateLimiter: newIPRateLimiter(rate.Every(licenseReloadTokenInterval), licenseReloadBurst),
	}
}

// SetCurrentKey sets the raw license key string for WatchLicense snapshot access.
// Called on startup (after loading from DB/env) and after each successful reload.
func (h *LicenseHandler) SetCurrentKey(key string) {
	h.currentKeyMu.Lock()
	h.currentKey = key
	h.currentKeyMu.Unlock()
}

// CurrentKey returns the raw license key string for WatchLicense snapshot.
func (h *LicenseHandler) CurrentKey() string {
	h.currentKeyMu.Lock()
	defer h.currentKeyMu.Unlock()
	return h.currentKey
}

// licenseReloadRequest is the JSON body for POST /api/v1/license.
type licenseReloadRequest struct {
	Key string `json:"key"`
}

// licenseReloadResponse is the JSON response on successful reload.
type licenseReloadResponse struct {
	Edition   string `json:"edition"`
	Org       string `json:"org"`
	ExpiresAt string `json:"expires_at"`
	Status    string `json:"status"`
}

// HandleReload handles POST /api/v1/license.
// Admin auth + ECDSA P-256 license signature — defense in depth.
// Rate-limited per IP. Replay-protected via iat.
func (h *LicenseHandler) HandleReload(w http.ResponseWriter, r *http.Request) {
	// Rate limiting
	clientIP := httputil.GetClientIP(r)
	if !h.rateLimiter.allow(clientIP) {
		licenseReloadTotal.WithLabelValues(reloadRateLimited).Inc()
		// Retry-After = the limiter's token interval (rate.Every(licenseReloadTokenInterval)).
		httputil.WriteRateLimited(w, licenseReloadTokenInterval, errCodeRateLimited, "too many license reload requests")
		return
	}

	// Extract admin actor from middleware context for audit logging.
	// GetSubject cannot fail on Claims populated by AdminJWTMiddleware.
	adminSub := ""
	if claims := auth.GetClaims(r.Context()); claims != nil {
		adminSub, _ = claims.GetSubject()
	}
	adminKID := auth.GetActor(r.Context())

	// Parse request body (limit to 64KB — a license key is ~200 bytes)
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	var req licenseReloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		licenseReloadTotal.WithLabelValues(reloadInvalidFormat).Inc()
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_JSON", "invalid request body")
		return
	}
	if req.Key == "" {
		licenseReloadTotal.WithLabelValues(reloadInvalidFormat).Inc()
		httputil.WriteError(w, http.StatusBadRequest, "MISSING_KEY", "key field is required")
		return
	}

	// Pre-check: signature + expiry gate before edition/replay comparison.
	// Errors here use handleReloadError for consistent error classification.
	incomingClaims, parseErr := license.ParseAndVerify(req.Key)
	if parseErr != nil {
		h.handleReloadError(w, fmt.Errorf("reload: %w", parseErr), clientIP, adminSub, adminKID)
		return
	}

	// Explicit replay pre-check: ensures ordering sig → replay → edition → Reload().
	if current := h.manager.Claims(); current != nil && current.Iat > 0 && incomingClaims.Iat <= current.Iat {
		h.handleReloadError(w, fmt.Errorf("reload: %w", license.ErrReplayDetected), clientIP, adminSub, adminKID)
		return
	}

	// Edition check: reject if incoming edition differs from startup-resolved edition.
	currentEdition := h.manager.Edition()
	requestedEdition := incomingClaims.Edition
	if requestedEdition != currentEdition {
		licenseReloadTotal.WithLabelValues(reloadEditionChange).Inc()
		h.logger.Warn().
			Bool("audit", true).
			Str("admin_sub", adminSub).
			Str("admin_kid", adminKID).
			Str("client_ip", clientIP).
			Str("current_edition", currentEdition.String()).
			Str("requested_edition", requestedEdition.String()).
			Msg("edition change rejected — restart or rollout required to change editions")
		_ = httputil.WriteJSON(w, http.StatusConflict, editionChangeRejectedResponse{
			Code:             editionChangeCode,
			Message:          fmt.Sprintf("edition change from %s to %s requires a service restart or rollout", currentEdition, requestedEdition),
			CurrentEdition:   currentEdition.String(),
			RequestedEdition: requestedEdition.String(),
		})
		return
	}

	// Reload Manager (validates ECDSA P-256 signature, checks iat replay, atomic swap)
	if err := h.manager.Reload(req.Key); err != nil {
		h.handleReloadError(w, err, clientIP, adminSub, adminKID)
		return
	}

	// After successful Reload(), claims are guaranteed non-nil and non-expired
	// (ParseAndVerify validated signature + expiry).
	claims := h.manager.Claims()
	expiresAt := time.Unix(claims.Exp, 0)
	edition := claims.Edition.String()
	org := claims.Org

	// Persist to DB (encrypted)
	if err := h.licenseRepo.Upsert(r.Context(), req.Key, edition, org, &expiresAt); err != nil {
		// Reload succeeded but persistence failed — log error, still return success
		// The key is active in memory; persistence will be retried on next reload
		h.logger.Error().Err(err).
			Bool("audit", true).
			Str("admin_sub", adminSub).
			Str("admin_kid", adminKID).
			Msg("license reload succeeded but DB persistence failed")
	}

	// Update current key for WatchLicense snapshot
	h.SetCurrentKey(req.Key)

	// Publish event for WatchLicense stream
	h.eventBus.Publish(eventbus.Event{Type: eventbus.LicenseChanged})

	// Update Prometheus gauge
	licenseEditionGauge.Reset()
	licenseEditionGauge.WithLabelValues(edition).Set(1)

	licenseReloadTotal.WithLabelValues(reloadSuccess).Inc()

	h.logger.Info().
		Bool("audit", true).
		Str("admin_sub", adminSub).
		Str("admin_kid", adminKID).
		Str("edition", edition).
		Str("org", org).
		Str("client_ip", clientIP).
		Str("expires_at", expiresAt.Format(time.RFC3339)).
		Msg("license reloaded via API")

	_ = httputil.WriteJSON(w, http.StatusOK, licenseReloadResponse{
		Edition:   edition,
		Org:       org,
		ExpiresAt: expiresAt.Format(time.RFC3339),
		Status:    "reloaded",
	})
}

// handleReloadError classifies the reload error and returns the appropriate HTTP response.
func (h *LicenseHandler) handleReloadError(w http.ResponseWriter, err error, clientIP, adminSub, adminKID string) {
	switch {
	case errors.Is(err, license.ErrReplayDetected):
		licenseReloadTotal.WithLabelValues(reloadReplay).Inc()
		h.logger.Warn().Err(err).
			Bool("audit", true).Str("admin_sub", adminSub).Str("admin_kid", adminKID).
			Str("client_ip", clientIP).Msg("license replay detected")
		httputil.WriteError(w, http.StatusConflict, "REPLAY_DETECTED", "key iat is not newer than current — replay rejected")

	case errors.Is(err, license.ErrLicenseExpired):
		licenseReloadTotal.WithLabelValues(reloadExpired).Inc()
		h.logger.Warn().Err(err).
			Bool("audit", true).Str("admin_sub", adminSub).Str("admin_kid", adminKID).
			Str("client_ip", clientIP).Msg("expired license key pushed")
		httputil.WriteError(w, http.StatusBadRequest, "KEY_EXPIRED", "pushed key is already expired")

	case errors.Is(err, license.ErrLicenseInvalidSignature):
		licenseReloadTotal.WithLabelValues(reloadInvalidSignature).Inc()
		h.logger.Warn().Err(err).
			Bool("audit", true).Str("admin_sub", adminSub).Str("admin_kid", adminKID).
			Str("client_ip", clientIP).Msg("invalid license signature")
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_SIGNATURE", "license signature verification failed")

	case errors.Is(err, license.ErrLicenseInvalidFormat):
		licenseReloadTotal.WithLabelValues(reloadInvalidFormat).Inc()
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_FORMAT", "license key format invalid")

	default:
		licenseReloadTotal.WithLabelValues(reloadInternalError).Inc()
		h.logger.Error().Err(err).
			Bool("audit", true).Str("admin_sub", adminSub).Str("admin_kid", adminKID).
			Str("client_ip", clientIP).Msg("license reload failed")
		httputil.WriteError(w, http.StatusInternalServerError, "INTERNAL", "license reload failed")
	}
}
