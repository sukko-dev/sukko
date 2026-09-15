package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// newTestGatewayConfig returns a gateway config for testing.
// Tests that need a Gateway instance should use newGatewayWithMockValidator
// to bypass the gRPC validator setup.
func newTestGatewayConfig() *platform.GatewayConfig {
	return &platform.GatewayConfig{
		BaseConfig: platform.BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "test",
		},
		AuthConfig: platform.AuthConfig{
			AuthMode: "required",
		},
		Port:                         3000,
		ReadTimeout:                  15 * time.Second,
		WriteTimeout:                 15 * time.Second,
		IdleTimeout:                  60 * time.Second,
		BackendURL:                   "ws://localhost:3005/ws",
		DialTimeout:                  10 * time.Second,
		MessageTimeout:               60 * time.Second,
		PublishRateLimit:             10.0,
		PublishBurst:                 100,
		MaxPublishSize:               65536,
		MaxFrameSize:                 1048576,
		TenantConnectionLimitEnabled: true,
		DefaultTenantConnectionLimit: 1000,
		AuthRefreshRateInterval:      30 * time.Second,
		AuthValidationTimeout:        5 * time.Second,
		ShutdownTimeout:              30 * time.Second,
		ChannelRulesCacheTTL:         1 * time.Minute,
		RegistryQueryTimeout:         5 * time.Second,
		RequireTenantID:              true,
		ProvisioningClientConfig: platform.ProvisioningClientConfig{
			ProvisioningGRPCAddr: "localhost:9090",
			GRPCReconnectConfig:  platform.GRPCReconnectConfig{GRPCReconnectDelay: 1 * time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
		},
	}
}

// newTestLogger returns a zerolog logger that discards output.
func newTestLogger() zerolog.Logger {
	return zerolog.Nop()
}

// newGatewayWithMockValidator creates a gateway with a mock validator and a
// permissive rules-backed tenant checker (provisioning-only authorization).
// This is useful for testing auth behavior without a database connection.
func newGatewayWithMockValidator(cfg *platform.GatewayConfig, logger zerolog.Logger, validator *auth.MultiTenantValidator) *Gateway {
	registry := newMockChannelRulesProvider()
	registry.setRules("test-tenant", &types.ChannelRules{Public: []string{"*"}})
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		panic(err) // test helper: provider is never nil
	}
	return &Gateway{
		config:             cfg,
		tenantPermChecker:  checker,
		validator:          validator,
		connectionRegistry: NewConnectionRegistry(),
		logger:             logger.With().Str("component", "gateway").Logger(),
	}
}

func TestNew_AuthEnabled_ConfigValidation(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	cfg.ProvisioningGRPCAddr = "" // No gRPC address

	// Config validation should catch missing gRPC address
	err := cfg.Validate()
	if err == nil {
		t.Error("Validate() should fail when auth is enabled without gRPC address")
	}
}

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryToken string
		authHeader string
		expected   string
	}{
		{
			name:       "query param only",
			queryToken: "token-from-query",
			authHeader: "",
			expected:   "token-from-query",
		},
		{
			name:       "bearer header only",
			queryToken: "",
			authHeader: "Bearer token-from-header",
			expected:   "token-from-header",
		},
		{
			name:       "query param takes precedence",
			queryToken: "query-token",
			authHeader: "Bearer header-token",
			expected:   "query-token",
		},
		{
			name:       "no token",
			queryToken: "",
			authHeader: "",
			expected:   "",
		},
		{
			name:       "malformed bearer header - no space",
			queryToken: "",
			authHeader: "Bearertoken",
			expected:   "",
		},
		{
			name:       "malformed bearer header - short",
			queryToken: "",
			authHeader: "Bear",
			expected:   "",
		},
		{
			name:       "non-bearer auth header",
			queryToken: "",
			authHeader: "Basic dXNlcjpwYXNz",
			expected:   "",
		},
		{
			name:       "bearer header with empty token",
			queryToken: "",
			authHeader: "Bearer ",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			url := "/ws"
			if tt.queryToken != "" {
				url += "?token=" + tt.queryToken
			}

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			got := httputil.ExtractBearerToken(req)
			if got != tt.expected {
				t.Errorf("ExtractBearerToken() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGateway_HandleHealth(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	logger := newTestLogger()
	gw := newGatewayWithMockValidator(cfg, logger, nil)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	gw.HandleHealth(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		t.Errorf("HandleHealth() status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	// Check content type
	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("HandleHealth() Content-Type = %q, want %q", contentType, "application/json")
	}

	// Check body - parse as JSON to avoid ordering issues
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("Failed to parse response body as JSON: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("HandleHealth() status = %q, want %q", result["status"], "ok")
	}
	if result["service"] != ServiceName {
		t.Errorf("HandleHealth() service = %q, want %q", result["service"], ServiceName)
	}
	if result["provisioning_license_stream"] != provapi.StreamLabelDisabled {
		t.Errorf("HandleHealth() provisioning_license_stream = %q, want %q", result["provisioning_license_stream"], provapi.StreamLabelDisabled)
	}
}

func TestGateway_NewServer(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.Port = 3456
	cfg.ReadTimeout = 20 * time.Second
	cfg.WriteTimeout = 25 * time.Second
	cfg.IdleTimeout = 120 * time.Second
	logger := newTestLogger()
	gw := newGatewayWithMockValidator(cfg, logger, nil)

	server := gw.NewServer()

	if server == nil {
		t.Fatal("NewServer() returned nil")
	}

	// Check address
	expectedAddr := ":3456"
	if server.Addr != expectedAddr {
		t.Errorf("Server.Addr = %q, want %q", server.Addr, expectedAddr)
	}

	// Check timeouts
	if server.ReadTimeout != cfg.ReadTimeout {
		t.Errorf("Server.ReadTimeout = %v, want %v", server.ReadTimeout, cfg.ReadTimeout)
	}
	if server.WriteTimeout != cfg.WriteTimeout {
		t.Errorf("Server.WriteTimeout = %v, want %v", server.WriteTimeout, cfg.WriteTimeout)
	}
	if server.IdleTimeout != cfg.IdleTimeout {
		t.Errorf("Server.IdleTimeout = %v, want %v", server.IdleTimeout, cfg.IdleTimeout)
	}

	// Check that handler is set
	if server.Handler == nil {
		t.Error("Server.Handler should not be nil")
	}
}

func TestGateway_HandleWebSocket_NoToken_WithMockValidator(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required" // Enable auth for this test
	logger := newTestLogger()

	// Create a mock validator using StaticKeyRegistry
	registry := auth.NewStaticKeyRegistry()
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}

	gw := newGatewayWithMockValidator(cfg, logger, validator)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Should return 401 Unauthorized when no token provided
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() without token status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestGateway_HandleWebSocket_InvalidToken_WithMockValidator(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required" // Enable auth for this test
	logger := newTestLogger()

	// Create a mock validator using StaticKeyRegistry (empty, so all tokens fail)
	registry := auth.NewStaticKeyRegistry()
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}

	gw := newGatewayWithMockValidator(cfg, logger, validator)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?token=invalid-token", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Should return 401 Unauthorized for invalid token
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() with invalid token status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestGateway_Close_NilFields(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	logger := newTestLogger()
	gw := newGatewayWithMockValidator(cfg, logger, nil)

	// Should not panic when closing gateway with nil streamKeyRegistry and streamTenantRegistry
	if err := gw.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

// mockAPIKeyLookup implements APIKeyLookup for testing API key auth flows.
type mockAPIKeyLookup struct {
	keys map[string]*provapi.APIKeyInfo
}

func (m *mockAPIKeyLookup) Lookup(apiKey string) (*provapi.APIKeyInfo, bool) {
	info, ok := m.keys[apiKey]
	if !ok || !info.IsActive {
		return nil, false
	}
	return info, true
}

func (m *mockAPIKeyLookup) Close() error { return nil }

// mockTenantSlugResolver is a test double for TenantSlugResolver (UUID -> slug).
type mockTenantSlugResolver struct {
	slugs   map[string]string // tenant UUID -> current slug
	present bool
}

func (m *mockTenantSlugResolver) ResolveTenantSlug(_ context.Context, tenantUUID string) (string, error) {
	if slug, ok := m.slugs[tenantUUID]; ok {
		return slug, nil
	}
	return "", auth.ErrTenantNotResolvable
}

func (m *mockTenantSlugResolver) TenantUUIDsPresent() bool { return m.present }

// mockLicenseWatcher implements licenseWatcher for testing.
type mockLicenseWatcher struct {
	state       int32
	closeCalled bool
}

func (m *mockLicenseWatcher) State() int32 { return m.state }
func (m *mockLicenseWatcher) Close() error { m.closeCalled = true; return nil }

// newGatewayWithAPIKeyMock creates a gateway with auth enabled and injects both
// a mock API key registry and an optional JWT validator. When validator is nil,
// only API-key auth paths are exercisable.
func newGatewayWithAPIKeyMock(cfg *platform.GatewayConfig, logger zerolog.Logger, validator *auth.MultiTenantValidator, apiKeys *mockAPIKeyLookup) *Gateway {
	registry := newMockChannelRulesProvider()
	registry.setRules("test-tenant", &types.ChannelRules{Public: []string{"*"}})
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		panic(err) // test helper: provider is never nil
	}
	return &Gateway{
		config:             cfg,
		tenantPermChecker:  checker,
		validator:          validator,
		connectionRegistry: NewConnectionRegistry(),
		apiKeyRegistry:     apiKeys,
		logger:             logger.With().Str("component", "gateway").Logger(),
	}
}

// generateTestECKeyForGateway generates an ECDSA P-256 key pair and returns
// the PEM-encoded public key and private key for signing test JWTs.
func generateTestECKeyForGateway(t *testing.T) (string, *ecdsa.PrivateKey) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate EC key: %v", err)
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("Failed to marshal public key: %v", err)
	}

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	return string(pem.EncodeToMemory(pemBlock)), privateKey
}

// createTestTokenForGateway creates a signed JWT for testing.
// Auto-generates jti if not set (the gateway mandates one).
func createTestTokenForGateway(t *testing.T, key *auth.KeyInfo, privateKey any, claims *auth.Claims) string {
	t.Helper()

	// Auto-generate jti if not set (mandatory
	if claims.ID == "" {
		claims.ID = uuid.NewString()
	}

	method, err := auth.GetSigningMethod(key.Algorithm)
	if err != nil {
		t.Fatalf("GetSigningMethod failed: %v", err)
	}

	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = key.KeyID

	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}
	return tokenString
}

func TestHandleWebSocket_NoCredentials(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() no credentials status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}
	if errResp["code"] != "UNAUTHORIZED" {
		t.Errorf("error code = %q, want %q", errResp["code"], "UNAUTHORIZED")
	}
	if errResp["message"] != "token or api_key required" {
		t.Errorf("error message = %q, want %q", errResp["message"], "token or api_key required")
	}
}

func TestHandleWebSocket_InvalidAPIKey(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"valid-key": {
			KeyID:    "pk_live_abc",
			TenantID: "acme",
			Name:     "test key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?api_key=wrong-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() invalid API key status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}
	if errResp["code"] != "UNAUTHORIZED" {
		t.Errorf("error code = %q, want %q", errResp["code"], "UNAUTHORIZED")
	}
	if errResp["message"] != "invalid api key" {
		t.Errorf("error message = %q, want %q", errResp["message"], "invalid api key")
	}
}

func TestHandleWebSocket_InactiveAPIKey(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"inactive-key": {
			KeyID:    "pk_live_inactive",
			TenantID: "acme",
			Name:     "inactive key",
			IsActive: false, // Inactive keys should be rejected
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?api_key=inactive-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() inactive API key status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHandleWebSocket_APIKeyOnly_Valid(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	cfg.TenantConnectionLimitEnabled = false // Disable to isolate auth testing
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"valid-key": {
			KeyID:    "pk_live_abc",
			TenantID: "acme",
			Name:     "test key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?api_key=valid-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// A valid API key should pass auth. The request will fail at the WebSocket
	// upgrade step (httptest.ResponseRecorder doesn't support upgrades), but
	// critically it must NOT fail with 401. The gobwas/ws upgrader writes
	// directly to the ResponseWriter and does not set a standard HTTP status on
	// failure, so the recorder keeps its default 200.
	if resp.StatusCode == http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() valid API key should not return 401, got %d", resp.StatusCode)
	}
}

func TestHandleWebSocket_APIKeyViaHeader(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"header-key": {
			KeyID:    "pk_live_header",
			TenantID: "acme",
			Name:     "header key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)

	// Send API key via X-API-Key header instead of query parameter
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Invalid key via header should return 401
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() invalid API key via header status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHandleWebSocket_APIKeyAndJWT_TenantMismatch(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	cfg.TenantConnectionLimitEnabled = false
	logger := newTestLogger()

	// Set up JWT validator with a test key for tenant "acme"
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// API key belongs to tenant "globex" (different from JWT tenant "acme")
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"globex-key": {
			KeyID:    "pk_live_globex",
			TenantID: "globex",
			Name:     "globex key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, validator, mock)

	// Create a valid JWT for tenant "acme"
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "acme",
	}
	tokenString := createTestTokenForGateway(t, key, privateKey, claims)

	// Send both API key (globex) and JWT (acme) — tenant mismatch
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?token="+tokenString+"&api_key=globex-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() tenant mismatch status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}
	if errResp["message"] != "api key and token tenant mismatch" {
		t.Errorf("error message = %q, want %q", errResp["message"], "api key and token tenant mismatch")
	}
}

func TestHandleWebSocket_APIKeyAndJWT_TenantMatch(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	cfg.TenantConnectionLimitEnabled = false
	logger := newTestLogger()

	// Set up JWT validator with a test key for tenant "acme"
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// API key also belongs to tenant "acme" (matching JWT)
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"acme-key": {
			KeyID:    "pk_live_acme",
			TenantID: "acme",
			Name:     "acme key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, validator, mock)

	// Create a valid JWT for tenant "acme"
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-456",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "acme",
	}
	tokenString := createTestTokenForGateway(t, key, privateKey, claims)

	// Send both API key and JWT for same tenant — should pass auth
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?token="+tokenString+"&api_key=acme-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	// Auth should pass; request fails at WebSocket upgrade (not 401)
	if resp.StatusCode == http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() matching tenants should not return 401, got %d", resp.StatusCode)
	}
}

func TestHandleWebSocket_BothCredentials_InvalidAPIKey(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	// Set up JWT validator
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// Empty API key registry — all keys invalid
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, validator, mock)

	// Create a valid JWT
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-789",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "acme",
	}
	tokenString := createTestTokenForGateway(t, key, privateKey, claims)

	// Both credentials present but API key is invalid — should reject before JWT validation
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?token="+tokenString+"&api_key=bad-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() invalid API key with valid JWT status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}
	if errResp["message"] != "invalid api key" {
		t.Errorf("error message = %q, want %q", errResp["message"], "invalid api key")
	}
}

func TestHandleWebSocket_BothCredentials_InvalidJWT(t *testing.T) {
	t.Parallel()
	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	logger := newTestLogger()

	// Set up JWT validator (empty registry — no keys registered, so all tokens fail)
	registry := auth.NewStaticKeyRegistry()

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	// Valid API key
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"acme-key": {
			KeyID:    "pk_live_acme",
			TenantID: "acme",
			Name:     "acme key",
			IsActive: true,
		},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, validator, mock)

	// Both credentials: valid API key but garbled JWT
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ws?token=not-a-valid-jwt&api_key=acme-key", nil)
	w := httptest.NewRecorder()

	gw.HandleWebSocket(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("HandleWebSocket() valid API key + invalid JWT status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}
	if errResp["message"] != "invalid token" {
		t.Errorf("error message = %q, want %q", errResp["message"], "invalid token")
	}
}

func TestGateway_LicenseStreamState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		watcher licenseWatcher
		want    string
	}{
		{name: "nil watcher", watcher: nil, want: provapi.StreamLabelDisabled},
		{name: "connected", watcher: &mockLicenseWatcher{state: provapi.StreamStateConnected}, want: provapi.StreamLabelConnected},
		{name: "disconnected", watcher: &mockLicenseWatcher{state: provapi.StreamStateDisconnected}, want: provapi.StreamLabelDisconnected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := newGatewayWithMockValidator(newTestGatewayConfig(), newTestLogger(), nil)
			if tt.watcher != nil {
				gw.SetLicenseWatcher(tt.watcher)
			}
			if got := gw.licenseStreamState(); got != tt.want {
				t.Errorf("licenseStreamState() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGateway_HandleHealth_LicenseStreamDoesNotAffectStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		watcher       *mockLicenseWatcher
		wantStatus    string
		wantLicStream string
	}{
		{
			name:          "disconnected watcher, all other streams nil",
			watcher:       &mockLicenseWatcher{state: provapi.StreamStateDisconnected},
			wantStatus:    "ok",
			wantLicStream: provapi.StreamLabelDisconnected,
		},
		{
			name:          "connected watcher",
			watcher:       &mockLicenseWatcher{state: provapi.StreamStateConnected},
			wantStatus:    "ok",
			wantLicStream: provapi.StreamLabelConnected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := newGatewayWithMockValidator(newTestGatewayConfig(), newTestLogger(), nil)
			gw.SetLicenseWatcher(tt.watcher)

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
			w := httptest.NewRecorder()
			gw.HandleHealth(w, req)

			resp := w.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("HandleHealth() status code = %d, want %d", resp.StatusCode, http.StatusOK)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}
			var result map[string]string
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatalf("Failed to parse response body: %v", err)
			}
			if result["status"] != tt.wantStatus {
				t.Errorf("status = %q, want %q", result["status"], tt.wantStatus)
			}
			if result["provisioning_license_stream"] != tt.wantLicStream {
				t.Errorf("provisioning_license_stream = %q, want %q", result["provisioning_license_stream"], tt.wantLicStream)
			}
		})
	}
}

func TestGateway_HandleReady_LicenseStreamDoesNotBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		watcher        *mockLicenseWatcher
		wantHTTPStatus int
		wantLicStream  string
	}{
		{
			name:           "disconnected watcher",
			watcher:        &mockLicenseWatcher{state: provapi.StreamStateDisconnected},
			wantHTTPStatus: http.StatusOK,
			wantLicStream:  provapi.StreamLabelDisconnected,
		},
		{
			name:           "connected watcher",
			watcher:        &mockLicenseWatcher{state: provapi.StreamStateConnected},
			wantHTTPStatus: http.StatusOK,
			wantLicStream:  provapi.StreamLabelConnected,
		},
		{
			name:           "nil watcher",
			watcher:        nil,
			wantHTTPStatus: http.StatusOK,
			wantLicStream:  provapi.StreamLabelDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := newGatewayWithMockValidator(newTestGatewayConfig(), newTestLogger(), nil)
			if tt.watcher != nil {
				gw.SetLicenseWatcher(tt.watcher)
			}

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/ready", nil)
			w := httptest.NewRecorder()
			gw.HandleReady(w, req)

			resp := w.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tt.wantHTTPStatus {
				t.Errorf("HandleReady() status code = %d, want %d (license stream must not cause 503)", resp.StatusCode, tt.wantHTTPStatus)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}
			var result map[string]string
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatalf("Failed to parse response body: %v", err)
			}
			if result["provisioning_license_stream"] != tt.wantLicStream {
				t.Errorf("provisioning_license_stream = %q, want %q", result["provisioning_license_stream"], tt.wantLicStream)
			}
		})
	}
}

func TestGateway_Close_WithLicenseWatcher(t *testing.T) {
	t.Parallel()
	gw := newGatewayWithMockValidator(newTestGatewayConfig(), newTestLogger(), nil)
	mock := &mockLicenseWatcher{}
	gw.SetLicenseWatcher(mock)

	if err := gw.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if !mock.closeCalled {
		t.Error("Close() did not call licenseWatcher.Close()")
	}
}

// TestHandleWebSocket_IdentityHeaderStripping verifies that inbound X-Sukko-* headers
// from the client request are stripped before the gateway dials the backend (§II, §IX).
// A malicious client must not be able to inject forged identity headers that reach ws-server.
func TestHandleWebSocket_IdentityHeaderStripping(t *testing.T) {
	t.Parallel()
	// Fake backend: captures headers then completes WebSocket upgrade.
	var capturedHeaders http.Header
	var capturedMu sync.Mutex
	var backendOnce sync.Once
	backendReady := make(chan struct{})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendOnce.Do(func() {
			capturedMu.Lock()
			capturedHeaders = r.Header.Clone()
			capturedMu.Unlock()
			_, _, _, _ = ws.UpgradeHTTP(r, w)
			close(backendReady)
		})
	}))
	defer backend.Close()

	cfg := newTestGatewayConfig()
	cfg.AuthMode = "required"
	cfg.BackendURL = "ws" + strings.TrimPrefix(backend.URL, "http") + "/ws"
	cfg.DialTimeout = 3 * time.Second
	cfg.TenantConnectionLimitEnabled = false
	logger := newTestLogger()

	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"valid-key": {KeyID: "pk_live_abc", TenantID: "acme", Name: "test key", IsActive: true},
	}}
	gw := newGatewayWithAPIKeyMock(cfg, logger, nil, mock)
	gw.tenantSlugResolver = &mockTenantSlugResolver{slugs: map[string]string{"acme": "acme"}, present: true}

	gatewayServer := httptest.NewServer(http.HandlerFunc(gw.HandleWebSocket))
	defer gatewayServer.Close()

	// Connect to the gateway with spoofed X-Sukko-* headers in the upgrade request.
	const (
		spoofedTenantID = "evil-tenant"
		spoofedAPIKeyID = "evil-api-key-id"
		spoofedUserID   = "evil-user-id"
		spoofedSecret   = "evil-secret"
	)
	dialer := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(http.Header{
			protocol.HeaderTenantID:       {spoofedTenantID},
			protocol.HeaderAPIKeyID:       {spoofedAPIKeyID},
			protocol.HeaderUserID:         {spoofedUserID},
			protocol.HeaderInternalSecret: {spoofedSecret},
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/ws?api_key=valid-key"
	conn, _, _, _ := dialer.Dial(ctx, wsURL)
	if conn != nil {
		defer conn.Close()
	}

	// Wait for the backend to receive the gateway's dial request.
	select {
	case <-backendReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for backend to receive the gateway dial")
	}

	capturedMu.Lock()
	hdrs := capturedHeaders
	capturedMu.Unlock()

	// Assert that spoofed headers were stripped by the gateway before dialing.
	if got := hdrs.Get(protocol.HeaderTenantID); got == spoofedTenantID {
		t.Errorf("backend received spoofed %s = %q; gateway must strip client-supplied identity headers", protocol.HeaderTenantID, got)
	}
	if got := hdrs.Get(protocol.HeaderAPIKeyID); got == spoofedAPIKeyID {
		t.Errorf("backend received spoofed %s = %q; gateway must strip client-supplied identity headers", protocol.HeaderAPIKeyID, got)
	}
	if got := hdrs.Get(protocol.HeaderUserID); got == spoofedUserID {
		t.Errorf("backend received spoofed %s = %q; gateway must strip client-supplied identity headers", protocol.HeaderUserID, got)
	}
	if got := hdrs.Get(protocol.HeaderInternalSecret); got == spoofedSecret {
		t.Errorf("backend received spoofed %s = %q; gateway must strip client-supplied identity headers", protocol.HeaderInternalSecret, got)
	}

	// Assert the gateway-built X-Sukko-Tenant-ID is forwarded correctly.
	if got := hdrs.Get(protocol.HeaderTenantID); got != "acme" {
		t.Errorf("backend %s = %q, want %q (gateway-built from auth result)", protocol.HeaderTenantID, got, "acme")
	}
}
