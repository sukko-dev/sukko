package api

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	testerauth "github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/runner"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

const testIDLength = 8

const maxRequestBodySize = 1 << 20 // 1MB

// API error codes returned in error responses.
const (
	errCodeInvalidRequest    = "INVALID_REQUEST"
	errCodeInvalidType       = "INVALID_TYPE"
	errCodeIncompleteCtx     = "INCOMPLETE_CONTEXT"
	errCodeInvalidSignKey    = "INVALID_SIGNING_KEY"
	errCodeInvalidAdminKey   = "INVALID_ADMIN_KEY"
	errCodeInvalidAdminKID   = "INVALID_ADMIN_KEY_ID"
	errCodeConflict          = "CONFLICT"
	errCodeNotFound          = "NOT_FOUND"
	errCodeInternalError     = "INTERNAL_ERROR"
	errCodeStreamUnsupported = "STREAMING_UNSUPPORTED"
	errCodeUnauthorized      = "UNAUTHORIZED"
)

// adminKeyIDPattern and adminKeyIDRegex validate admin key ID format:
// 3–63 chars, starts with a lowercase letter, contains lowercase letters, digits, and hyphens only.
const adminKeyIDPattern = `^[a-z][a-z0-9-]{2,62}$`

var adminKeyIDRegex = regexp.MustCompile(adminKeyIDPattern)

// ValidateAdminKeyID returns an error if id is not a valid admin key ID.
// An empty string is accepted (treated as "not provided" — callers must enforce presence separately).
// A non-empty ID must be 3–63 chars: starts with a lowercase letter; contains lowercase letters, digits, and hyphens only.
func ValidateAdminKeyID(id string) error {
	if id == "" || adminKeyIDRegex.MatchString(id) {
		return nil
	}
	return fmt.Errorf("admin_key_id %q must be 3–63 characters: lowercase letters, digits, and hyphens only, starting with a letter", id)
}

type handlers struct {
	runner     *runner.Runner
	authToken  string
	adminKeyID string // effective kid used when per-request admin_key omits admin_key_id
	logger     zerolog.Logger
}

func (h *handlers) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.authToken == "" {
			next(w, r)
			return
		}

		auth := r.Header.Get("Authorization")
		expected := "Bearer " + h.authToken
		if subtle.ConstantTimeCompare([]byte(auth), []byte(expected)) != 1 {
			httputil.WriteError(w, http.StatusUnauthorized, errCodeUnauthorized, "invalid or missing authorization token")
			return
		}
		next(w, r)
	}
}

// TestContext holds deployment context passed from the CLI.
// All-or-nothing: when present, core fields are required.
// SECURITY: This struct is decode-only (request deserialization). It is NEVER serialized
// in responses — the handler converts to runner.TestContext before any response is written.
type TestContext struct {
	GatewayURL          string `json:"gateway_url"`
	ProvisioningURL     string `json:"provisioning_url"`
	Environment         string `json:"environment"`
	KafkaBrokers        string `json:"kafka_brokers,omitempty"`         // Per-run override of the tester's KAFKA_BROKERS for the kafka-ingest suite.
	KafkaTopicNamespace string `json:"kafka_topic_namespace,omitempty"` // Per-run override of the tester's KAFKA_TOPIC_NAMESPACE; MUST match the server-under-test.
	// AdminKeyPath is accepted for deprecation detection only — never forwarded to runner.
	// Use admin_key (base64 Ed25519 private key) in the top-level request body instead.
	AdminKeyPath string `json:"admin_key_path,omitempty"`
}

type startTestRequest struct {
	Type        string       `json:"type"`
	Connections int          `json:"connections,omitempty"`
	Duration    string       `json:"duration,omitempty"`
	PublishRate int          `json:"publish_rate,omitempty"`
	RampRate    int          `json:"ramp_rate,omitempty"`
	Suite       string       `json:"suite,omitempty"`
	TenantID    string       `json:"tenant_id,omitempty"`
	SigningKey  string       `json:"signing_key,omitempty"`  // PEM PKCS#8 ECDSA P-256 private key (raw text, NOT base64) for license-reload suite
	AdminKey    string       `json:"admin_key,omitempty"`    // base64.StdEncoding Ed25519 private key for per-request admin auth
	AdminKeyID  string       `json:"admin_key_id,omitempty"` // kid for admin_key JWT; falls back to server AdminKeyID when omitted
	Context     *TestContext `json:"context,omitempty"`
	// Auth mode fields
	AuthMode     string  `json:"auth_mode,omitempty"`
	APIKey       string  `json:"api_key,omitempty"`        // accepted in POST body; never echoed (TestConfig.APIKey is json:"-")
	AuthMixRatio float64 `json:"auth_mix_ratio,omitempty"` // 0.0 treated as "omitted" (float64 zero value) → uses default 0.5
}

func (h *handlers) startTest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	var req startTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Debug().Err(err).Msg("invalid request body")
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest, "invalid request body")
		return
	}

	if req.Type == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidType, "test type is required")
		return
	}

	switch runner.TestType(req.Type) {
	case runner.TestSmoke, runner.TestLoad, runner.TestStress, runner.TestSoak, runner.TestValidate:
		// valid
	default:
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidType, fmt.Sprintf("unknown test type: %q", req.Type))
		return
	}

	// Auth mode validation. When auth_mode is absent, infer the mode for a validate run of the
	// api-key or upgrade suite (§XV: each has exactly one legal mode, so this completes a
	// mandatory 1:1 mapping — not multi-meaning mode detection); every other absent-mode
	// request defaults to jwt. Kept in lockstep with the runner boundary (§II).
	authMode := runner.AuthModeJWT
	switch {
	case req.AuthMode == "" && req.Type == string(runner.TestValidate) && req.Suite == runner.SuiteAPIKey:
		authMode = runner.AuthModeAPIKey
	case req.AuthMode == "" && req.Type == string(runner.TestValidate) && req.Suite == runner.SuiteUpgrade:
		authMode = runner.AuthModeUpgrade
	case req.AuthMode != "":
		switch runner.AuthMode(req.AuthMode) {
		case runner.AuthModeJWT, runner.AuthModeAPIKey, runner.AuthModeUpgrade, runner.AuthModeMixed:
			authMode = runner.AuthMode(req.AuthMode)
		default:
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("unknown auth_mode %q (valid: jwt, api-key, upgrade, mixed)", req.AuthMode))
			return
		}
	}
	if req.AuthMixRatio != 0 && (req.AuthMixRatio < runner.AuthMixRatioMin || req.AuthMixRatio > runner.AuthMixRatioMax) {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("auth_mix_ratio must be in [%.1f, %.1f], got %v", runner.AuthMixRatioMin, runner.AuthMixRatioMax, req.AuthMixRatio))
		return
	}
	testType := runner.TestType(req.Type)
	switch authMode {
	case runner.AuthModeJWT:
		// no restrictions
	case runner.AuthModeMixed:
		switch testType {
		case runner.TestLoad, runner.TestSoak:
			// valid for mixed mode
		case runner.TestValidate, runner.TestSmoke, runner.TestStress:
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("auth_mode=mixed is only valid for load/soak tests, not %q", req.Type))
			return
		}
	case runner.AuthModeAPIKey:
		if testType != runner.TestValidate {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("auth_mode=api-key is only valid for type=validate, not %q", req.Type))
			return
		}
		if req.Suite != "" && req.Suite != runner.SuiteAPIKey && req.Suite != runner.SuiteRestPublish {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("auth_mode=api-key only supports suite=%s or suite=%s, not %q", runner.SuiteAPIKey, runner.SuiteRestPublish, req.Suite))
			return
		}
		// tenant_id is optional in api-key mode: empty/absent/null → the runner
		// self-provisions a throwaway tenant; a supplied tenant_id is used as-is. Guard removed
		// in lockstep with the runner boundary (§II).
	case runner.AuthModeUpgrade:
		if testType != runner.TestValidate {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("auth_mode=upgrade is only valid for type=validate, not %q", req.Type))
			return
		}
		if req.Suite != "" && req.Suite != runner.SuiteUpgrade {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
				fmt.Sprintf("auth_mode=upgrade only supports suite=%s, not %q", runner.SuiteUpgrade, req.Suite))
			return
		}
		// tenant_id is optional in upgrade mode: empty/absent/null → auth.Setup
		// self-provisions a throwaway tenant; a supplied tenant_id is used as-is. Guard removed
		// in lockstep with the runner boundary (§II).
	}

	// suite=revocation is only valid for stress and soak test types.
	if req.Suite == runner.SuiteRevocation && testType != runner.TestStress && testType != runner.TestSoak {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidRequest,
			fmt.Sprintf("suite=%s is only valid for type=stress or type=soak, not %q", runner.SuiteRevocation, req.Type))
		return
	}

	// Validate context block (all-or-nothing)
	if req.Context != nil {
		if req.Context.GatewayURL == "" || req.Context.ProvisioningURL == "" ||
			req.Context.Environment == "" {
			httputil.WriteError(w, http.StatusBadRequest, errCodeIncompleteCtx, "all core context fields required: gateway_url, provisioning_url, environment")
			return
		}
	}

	id, err := uuid.NewRandom()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, errCodeInternalError, "failed to generate test ID")
		return
	}
	testID := id.String()[:testIDLength]

	// Signing key for the license-reload suite (§XVIII deliberate divergence from admin_key):
	// signing_key carries the PEM (PKCS#8 ECDSA P-256) text DIRECTLY — it is NOT base64-encoded.
	// admin_key below stays base64.StdEncoding Ed25519. The two credentials use different
	// algorithms and wire encodings (license signing is ECDSA P-256; admin-JWT signing is
	// Ed25519) and MUST NOT be conflated. The raw PEM is passed through unchanged; the
	// authoritative parse happens downstream in newLicenseKeyGeneratorFromBytes. We parse
	// here as a boundary check (§II) so a malformed key fails fast with a clear error.
	var signingKeyBytes []byte
	if req.SigningKey != "" {
		if _, err := testerauth.ParseECDSAP256PrivateKeyPEM([]byte(req.SigningKey)); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidSignKey,
				"signing_key must be a PEM PKCS#8 ECDSA P-256 private key")
			return
		}
		h.logger.Info().Msg("signing key provided via API")
		signingKeyBytes = []byte(req.SigningKey)
	}

	// admin_key_id without admin_key is a caller error — reject immediately.
	if req.AdminKeyID != "" && req.AdminKey == "" {
		httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidAdminKID,
			"admin_key_id requires admin_key")
		return
	}

	// Decode admin key if provided (per-request admin auth override)
	var adminKeyBytes []byte
	var adminKeyID string
	if req.AdminKey != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.AdminKey)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidAdminKey, "admin_key must be valid base64 (base64.StdEncoding)")
			return
		}
		if len(decoded) != ed25519.PrivateKeySize {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidAdminKey,
				fmt.Sprintf("admin key must be %d bytes (Ed25519 private key), got %d", ed25519.PrivateKeySize, len(decoded)))
			return
		}
		if req.AdminKeyID != "" && !adminKeyIDRegex.MatchString(req.AdminKeyID) {
			httputil.WriteError(w, http.StatusBadRequest, errCodeInvalidAdminKID,
				"admin_key_id must be 3–63 characters: lowercase letters, digits, and hyphens only, starting with a letter")
			return
		}
		adminKeyBytes = decoded
		adminKeyID = req.AdminKeyID
		if adminKeyID == "" {
			adminKeyID = h.adminKeyID
		}
		h.logger.Info().Msg("admin key provided via API")
	}

	cfg := runner.TestConfig{
		Type:            runner.TestType(req.Type),
		Connections:     req.Connections,
		Duration:        req.Duration,
		PublishRate:     req.PublishRate,
		RampRate:        req.RampRate,
		Suite:           req.Suite,
		TenantID:        req.TenantID,
		SigningKeyBytes: signingKeyBytes,
		AdminKeyBytes:   adminKeyBytes,
		AdminKeyID:      adminKeyID,
		AuthMode:        authMode,
		APIKey:          req.APIKey,
		AuthMixRatio:    req.AuthMixRatio,
	}
	if req.Context != nil {
		if req.Context.AdminKeyPath != "" {
			// admin_key_path is deprecated — use top-level admin_key instead.
			h.logger.Warn().Str("field", "admin_key_path").Msg("admin_key_path is deprecated; use admin_key in the request body instead")
		}
		cfg.Context = &runner.TestContext{
			GatewayURL:      req.Context.GatewayURL,
			ProvisioningURL: req.Context.ProvisioningURL,
			Environment:     req.Context.Environment,
		}
		// Per-run broker override for the kafka-ingest suite (credentials stay env-only).
		if req.Context.KafkaBrokers != "" {
			cfg.KafkaBrokers = req.Context.KafkaBrokers
		}
		// Per-run topic-namespace override — MUST match the server-under-test (runner normalizes it).
		if req.Context.KafkaTopicNamespace != "" {
			cfg.KafkaTopicNamespace = req.Context.KafkaTopicNamespace
		}
	}

	run, err := h.runner.Start(testID, cfg)
	if err != nil {
		status := http.StatusInternalServerError
		code := errCodeInternalError
		msg := "failed to start test"
		if errors.Is(err, runner.ErrTestAlreadyExists) {
			status = http.StatusConflict
			code = errCodeConflict
			msg = "a test with this ID already exists"
		}
		httputil.WriteError(w, status, code, msg)
		return
	}

	// Use the lock-protected accessors — execute() may concurrently write run.Status.
	status, _ := run.StatusSnapshot()
	config := run.ConfigSnapshot()
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":     run.ID,
		"status": status,
		"config": config,
	})
}

func (h *handlers) stopTest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.runner.Stop(id); err != nil {
		if errors.Is(err, runner.ErrTestNotFound) {
			httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, fmt.Sprintf("test %q not found", id))
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, errCodeInternalError, "failed to stop test")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopping"})
}

func (h *handlers) getTest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := h.runner.Get(id)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, fmt.Sprintf("test %q not found", id))
		return
	}

	status, report := run.StatusSnapshot()
	resp := map[string]any{
		"id":     run.ID,
		"status": status,
		"config": run.ConfigSnapshot(),
	}
	if report != nil {
		resp["report"] = report
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *handlers) streamMetrics(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := h.runner.Get(id)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, errCodeNotFound, fmt.Sprintf("test %q not found", id))
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		httputil.WriteError(w, http.StatusInternalServerError, errCodeStreamUnsupported, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			snapshot := run.Collector.Snapshot()
			data, _ := json.Marshal(snapshot) // Snapshot is all primitives — marshal cannot fail
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()

			// Stop streaming when test completes
			status, report := run.StatusSnapshot()
			if status == runner.StatusComplete || status == runner.StatusFailed || status == runner.StatusStopped {
				if report != nil {
					finalData, _ := json.Marshal(report) // Report is all primitives — marshal cannot fail
					if _, err := fmt.Fprintf(w, "event: complete\ndata: %s\n\n", finalData); err != nil {
						return
					}
					flusher.Flush()
				}
				return
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	_ = httputil.WriteJSON(w, status, data) // best-effort: client may have disconnected
}
