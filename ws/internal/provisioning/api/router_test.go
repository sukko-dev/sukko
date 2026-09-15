package api_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/api"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/provisioning/revocation"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
	sharedtestutil "github.com/sukko-dev/sukko/internal/shared/testutil"
)

// generateTestECKey generates an ECDSA P-256 key pair for testing.
func generateTestECKey(t *testing.T) (string, *ecdsa.PrivateKey) {
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

// createTestToken creates a signed JWT token for testing.
func createTestToken(t *testing.T, privateKey *ecdsa.PrivateKey, claims *auth.Claims) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = "test-key-1"

	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("Failed to sign token: %v", err)
	}

	return tokenString
}

// newTestService creates a provisioning service with mock stores.
func newTestService() *provisioning.Service {
	return newTestServiceWithStores(
		testutil.NewMockTenantStore(),
		testutil.NewMockRoutingRulesStore(),
		testutil.NewMockChannelRulesStore(),
	)
}

// newTestServiceWithStores creates a provisioning service with specific mock stores.
func newTestServiceWithStores(tenantStore *testutil.MockTenantStore, routingStore *testutil.MockRoutingRulesStore, channelRulesStore ...*testutil.MockChannelRulesStore) *provisioning.Service {
	svc, _ := newTestServiceWithKafka(tenantStore, routingStore, channelRulesStore...)
	return svc
}

// newTestServiceWithKafka creates a provisioning service and returns the kafka admin for test assertions.
func newTestServiceWithKafka(tenantStore *testutil.MockTenantStore, routingStore *testutil.MockRoutingRulesStore, channelRulesStore ...*testutil.MockChannelRulesStore) (*provisioning.Service, *testutil.MockKafkaAdmin) {
	kafkaAdmin := testutil.NewMockKafkaAdmin()
	cfg := provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           routingStore,
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    100,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  604800000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	}
	if len(channelRulesStore) > 0 {
		cfg.ChannelRulesStore = channelRulesStore[0]
	}
	svc, err := provisioning.NewService(cfg)
	if err != nil {
		panic("newTestServiceWithKafka: " + err.Error())
	}
	return svc, kafkaAdmin
}

// mustNewRouter creates a test router, failing the test on error.
// Automatically sets EditionManager to Enterprise if not provided,
// so all feature gates pass through in existing tests.
// newTestValidator creates a MultiTenantValidator with a static key registry for tests.
// Returns the validator and the private key for signing test tokens.
func newTestValidator(t *testing.T) (*auth.MultiTenantValidator, *ecdsa.PrivateKey) {
	t.Helper()
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)
	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}
	return validator, privateKey
}

func mustNewRouter(t *testing.T, cfg api.RouterConfig) http.Handler {
	t.Helper()
	r, _ := mustNewRouterWithAuth(t, cfg)
	return r
}

// mustNewRouterWithAuth creates a test router and returns a function that adds
// an admin-level Authorization header to HTTP requests. This is the preferred
// helper for tests that need to send authenticated requests.
func mustNewRouterWithAuth(t *testing.T, cfg api.RouterConfig) (handler http.Handler, setAuth func(*http.Request)) {
	t.Helper()
	if cfg.EditionManager == nil {
		cfg.EditionManager = license.NewTestManager(license.Enterprise)
	}
	var privateKey *ecdsa.PrivateKey
	if cfg.Validator == nil {
		cfg.Validator, privateKey = newTestValidator(t)
	}
	router, err := api.NewRouter(cfg)
	if err != nil {
		t.Fatalf("mustNewRouterWithAuth: %v", err)
	}

	addAuth := func(req *http.Request) {
		if privateKey == nil {
			t.Fatal("mustNewRouterWithAuth: no private key available — pass Validator via cfg to use custom auth")
		}
		claims := &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   "test-admin",
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
			TenantID: "test-tenant",
			Roles:    []string{"admin"},
		}
		token := createTestToken(t, privateKey, claims)
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return router, addAuth
}

func TestRouter_CORSPreflight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		origin         string
		allowedOrigins []string
		wantAllowed    bool
	}{
		{
			name:           "allowed origin",
			origin:         "http://localhost:3000",
			allowedOrigins: []string{"http://localhost:3000"},
			wantAllowed:    true,
		},
		{
			name:           "multiple allowed origins",
			origin:         "https://app.example.com",
			allowedOrigins: []string{"http://localhost:3000", "https://app.example.com"},
			wantAllowed:    true,
		},
		{
			name:           "disallowed origin",
			origin:         "https://evil.com",
			allowedOrigins: []string{"http://localhost:3000"},
			wantAllowed:    false,
		},
		{
			name:           "wildcard origin",
			origin:         "https://any.domain.com",
			allowedOrigins: []string{"*"},
			wantAllowed:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			router := mustNewRouter(t, api.RouterConfig{
				Service:            newTestService(),
				Logger:             zerolog.Nop(),
				CORSAllowedOrigins: tt.allowedOrigins,
				CORSMaxAge:         3600,
			})

			// Create preflight OPTIONS request
			req := httptest.NewRequestWithContext(context.Background(), http.MethodOptions, "/api/v1/tenants", nil)
			req.Header.Set("Origin", tt.origin)
			req.Header.Set("Access-Control-Request-Method", "POST")
			req.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type")

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			// Check response
			allowOrigin := rec.Header().Get("Access-Control-Allow-Origin")
			allowMethods := rec.Header().Get("Access-Control-Allow-Methods")
			allowHeaders := rec.Header().Get("Access-Control-Allow-Headers")
			maxAge := rec.Header().Get("Access-Control-Max-Age")

			if tt.wantAllowed {
				if allowOrigin == "" {
					t.Error("expected Access-Control-Allow-Origin header, got empty")
				}
				if allowMethods == "" {
					t.Error("expected Access-Control-Allow-Methods header, got empty")
				}
				if allowHeaders == "" {
					t.Error("expected Access-Control-Allow-Headers header, got empty")
				}
				if maxAge == "" {
					t.Error("expected Access-Control-Max-Age header, got empty")
				}
				// Preflight should return 200 or 204
				if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
					t.Errorf("expected status 200 or 204, got %d", rec.Code)
				}
			} else if allowOrigin != "" && allowOrigin != tt.origin {
				// For disallowed origins, the CORS middleware typically doesn't set headers
				// If there's an Allow-Origin header, it shouldn't match the disallowed origin
				t.Logf("Allow-Origin header present: %s", allowOrigin)
			}
		})
	}
}

func TestRouter_CORSHeaders_OnActualRequest(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, api.RouterConfig{
		Service:            newTestService(),
		Logger:             zerolog.Nop(),
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		CORSMaxAge:         3600,
	})

	// Make an actual GET request with Origin header
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Should have CORS headers on actual request too
	allowOrigin := rec.Header().Get("Access-Control-Allow-Origin")
	if allowOrigin != "http://localhost:3000" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", allowOrigin, "http://localhost:3000")
	}

	// Credentials MUST NOT be allowed: the provisioning API is bearer-authed (no
	// cookies), so Access-Control-Allow-Credentials is intentionally absent — this
	// avoids the CORS origin-echo + credentials footgun.
	allowCreds := rec.Header().Get("Access-Control-Allow-Credentials")
	if allowCreds != "" {
		t.Errorf("Access-Control-Allow-Credentials = %q, want it absent (bearer auth, no cookies)", allowCreds)
	}
}

func TestRouter_AuthRequired_RequiresToken(t *testing.T) {
	t.Parallel()

	// Create a key registry with a test key
	registry := auth.NewStaticKeyRegistry()
	ecPEM, _ := generateTestECKey(t)

	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	// Request without token should fail
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestRouter_AuthRequired_ValidToken(t *testing.T) {
	t.Parallel()

	// Create a key registry with a test key
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	// Create a valid token with admin role
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "test-tenant",
		Roles:    []string{"admin"},
	}

	token := createTestToken(t, privateKey, claims)

	// Request with valid token should succeed
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestRouter_AuthRequired_ExpiredToken(t *testing.T) {
	t.Parallel()

	// Create a key registry with a test key
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	// Create an expired token
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)), // Expired
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		},
		TenantID: "test-tenant",
		Roles:    []string{"admin"},
	}

	token := createTestToken(t, privateKey, claims)

	// Request with expired token should fail
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestRouter_AuthRequired_InvalidToken(t *testing.T) {
	t.Parallel()

	// Create a key registry with a test key
	registry := auth.NewStaticKeyRegistry()
	ecPEM, _ := generateTestECKey(t)

	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	tests := []struct {
		name  string
		token string
	}{
		{
			name:  "malformed token",
			token: "not-a-valid-jwt",
		},
		{
			name:  "empty token",
			token: "",
		},
		{
			name:  "only header",
			token: "Bearer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants", nil)
			if tt.token != "" {
				req.Header.Set("Authorization", tt.token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
			}
		})
	}
}

func TestRouter_AuthRequired_RoleRequirement(t *testing.T) {
	t.Parallel()

	// Create a key registry with a test key
	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)

	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	tests := []struct {
		name       string
		roles      []string
		path       string
		method     string
		wantStatus int
	}{
		{
			name:       "admin can list tenants",
			roles:      []string{"admin"},
			path:       "/api/v1/tenants",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "system can list tenants",
			roles:      []string{"system"},
			path:       "/api/v1/tenants",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "user without admin role cannot list tenants",
			roles:      []string{"user"},
			path:       "/api/v1/tenants",
			method:     http.MethodGet,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "system can get active keys",
			roles:      []string{"system"},
			path:       "/api/v1/keys/active",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "admin can get active keys",
			roles:      []string{"admin"},
			path:       "/api/v1/keys/active",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "user cannot get active keys",
			roles:      []string{"user"},
			path:       "/api/v1/keys/active",
			method:     http.MethodGet,
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			claims := &auth.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject:   "user-123",
					ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
					IssuedAt:  jwt.NewNumericDate(time.Now()),
				},
				TenantID: "test-tenant",
				Roles:    tt.roles,
			}

			token := createTestToken(t, privateKey, claims)

			req := httptest.NewRequestWithContext(context.Background(), tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestRouter_HealthEndpoints_NoAuth(t *testing.T) {
	t.Parallel()

	// Even with auth enabled, health endpoints should work without auth
	registry := auth.NewStaticKeyRegistry()
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service: newTestService(),
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	tests := []struct {
		path       string
		wantStatus int
	}{
		{"/health", http.StatusOK},
		{"/ready", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

// =============================================================================
// Routing Rules Endpoint Tests (W8)
// =============================================================================

func TestRouter_RoutingRules_CRUD(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc, kafkaAdmin := newTestServiceWithKafka(tenantStore, routingStore)

	// Pre-create a tenant so service calls succeed
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	// Pre-seed Kafka topics referenced by routing rules in this test.
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.test-tenant.trade", 1, 1, nil)
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.test-tenant.orderbook", 1, 1, nil)

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	// 1. GET routing rules — empty initially (200 with empty array)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants/test-tenant/routing-rules", nil)
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET (empty) status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// 2. PUT routing rules
	rules := provisioning.ReplaceRoutingRulesRequest{
		Rules: []provisioning.TopicRoutingRule{
			{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
			{Pattern: "**.orderbook", Topics: []string{"orderbook"}, Priority: 2},
		},
	}
	body, _ := json.Marshal(rules)
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/api/v1/tenants/test-tenant/routing-rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("PUT status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// 3. GET routing rules — should find them now
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants/test-tenant/routing-rules", nil)
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET (after set) status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var getResp struct {
		Items  []provisioning.TopicRoutingRule `json:"items"`
		Total  int                             `json:"total"`
		Limit  int                             `json:"limit"`
		Offset int                             `json:"offset"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("failed to parse GET response: %v", err)
	}
	if len(getResp.Items) != 2 {
		t.Errorf("expected 2 rules, got %d", len(getResp.Items))
	}

	// 4. DELETE routing rules
	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/api/v1/tenants/test-tenant/routing-rules", nil)
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("DELETE status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	// 5. GET after delete — empty (200 with empty array)
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants/test-tenant/routing-rules", nil)
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET (after delete) status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestRouter_AddRoutingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testutil.MockTenantStore, *testutil.MockKafkaAdmin, *testutil.MockRoutingRulesStore)
		reqBody    any
		wantStatus int
		wantCode   string
	}{
		{
			name: "201 happy path",
			setup: func(ts *testutil.MockTenantStore, k *testutil.MockKafkaAdmin, _ *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
				_ = k.CreateTopic(context.Background(), "test.t1.trade", 1, 1, nil)
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 5},
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "409 duplicate priority",
			setup: func(ts *testutil.MockTenantStore, k *testutil.MockKafkaAdmin, rs *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
				_ = k.CreateTopic(context.Background(), "test.t1.trade", 1, 1, nil)
				rs.AddErr = provisioning.ErrDuplicatePriority
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 5},
			},
			wantStatus: http.StatusConflict,
			wantCode:   "ROUTING_RULE_DUPLICATE_PRIORITY",
		},
		{
			name: "409 duplicate pattern",
			setup: func(ts *testutil.MockTenantStore, k *testutil.MockKafkaAdmin, rs *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
				_ = k.CreateTopic(context.Background(), "test.t1.trade", 1, 1, nil)
				rs.AddErr = provisioning.ErrDuplicateRoutingPattern
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 10},
			},
			wantStatus: http.StatusConflict,
			wantCode:   "ROUTING_RULE_DUPLICATE_PATTERN",
		},
		{
			name: "400 invalid pattern",
			setup: func(ts *testutil.MockTenantStore, _ *testutil.MockKafkaAdmin, _ *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.**.bad", Topics: []string{"trade"}, Priority: 1},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ROUTING_RULE_VALIDATION_ERROR",
		},
		{
			name: "400 empty topics",
			setup: func(ts *testutil.MockTenantStore, _ *testutil.MockKafkaAdmin, _ *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{}, Priority: 1},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ROUTING_RULE_VALIDATION_ERROR",
		},
		{
			name: "400 topic not provisioned",
			setup: func(ts *testutil.MockTenantStore, _ *testutil.MockKafkaAdmin, _ *testutil.MockRoutingRulesStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("t1"))
				// topic "trade" not created in kafka — TopicExists returns false
			},
			reqBody: provisioning.AddRoutingRuleRequest{
				Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 5},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "TOPIC_NOT_PROVISIONED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tenantStore := testutil.NewMockTenantStore()
			routingStore := testutil.NewMockRoutingRulesStore()
			svc, kafkaAdmin := newTestServiceWithKafka(tenantStore, routingStore)
			if tt.setup != nil {
				tt.setup(tenantStore, kafkaAdmin, routingStore)
			}

			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
				Service: svc,
				Logger:  zerolog.Nop(),
			})

			b, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/tenants/t1/routing-rules", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			if tt.wantCode != "" {
				var resp struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("parse response: %v", err)
				}
				if resp.Code != tt.wantCode {
					t.Errorf("code = %q, want %q", resp.Code, tt.wantCode)
				}
			}
		})
	}
}

func TestRouter_ReplaceRoutingRules_DuplicatePattern(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("t1"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	// "trades.*" and "trades.**" are different raw patterns but NormalizePattern converts both to
	// "trades.**" before service-level ValidateRoutingRules — this makes them duplicate post-normalization.
	// The handler's boundary validation passes (raw strings differ); the service catches the duplicate.
	rules := provisioning.ReplaceRoutingRulesRequest{
		Rules: []provisioning.TopicRoutingRule{
			{Pattern: "trades.*", Topics: []string{"trades"}, Priority: 1},
			{Pattern: "trades.**", Topics: []string{"trades"}, Priority: 2},
		},
	}
	b, _ := json.Marshal(rules)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/api/v1/tenants/t1/routing-rules", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Code != "ROUTING_RULE_DUPLICATE_PATTERN" {
		t.Errorf("code = %q, want ROUTING_RULE_DUPLICATE_PATTERN", resp.Code)
	}
}

func TestRouter_RoutingRules_InvalidJSON(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/api/v1/tenants/test-tenant/routing-rules",
		bytes.NewReader([]byte(`{invalid json}`)))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestRouter_RoutingRules_InvalidRules(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	// Empty rules should fail validation
	rules := provisioning.ReplaceRoutingRulesRequest{
		Rules: []provisioning.TopicRoutingRule{},
	}
	body, _ := json.Marshal(rules)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/api/v1/tenants/test-tenant/routing-rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestRouter_RoutingRules_DeleteNonexistent(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/api/v1/tenants/test-tenant/routing-rules", nil)
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// =============================================================================
// Channel Rules Endpoint Tests
// =============================================================================

func TestRouter_ChannelRules_PublishFields(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	channelRulesStore := testutil.NewMockChannelRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore, channelRulesStore)

	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("tenant-sub-only"))
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("tenant-sub-pub"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	tests := []struct {
		name               string
		tenantID           string
		body               string
		wantPublic         []string
		wantPublishPublic  []string
		wantPublishDefault []string
		wantPublishGroups  map[string][]string
		wantStatus         int
	}{
		{
			name:               "subscribe-only rules",
			tenantID:           "tenant-sub-only",
			body:               `{"public":["general.*"],"group_mappings":{"vip":["room.vip"]},"default":["general.*"]}`,
			wantPublic:         []string{"general.*"},
			wantPublishPublic:  []string{},
			wantPublishDefault: []string{},
			wantPublishGroups:  map[string][]string{},
			wantStatus:         http.StatusOK,
		},
		{
			name:               "subscribe and publish rules",
			tenantID:           "tenant-sub-pub",
			body:               `{"public":["general.*","dm.{principal}"],"publish_public":["general.*","dm.{principal}"],"publish_group_mappings":{"vip":["room.vip"]},"publish_default":["general.*"]}`,
			wantPublic:         []string{"general.*", "dm.{principal}"},
			wantPublishPublic:  []string{"general.*", "dm.{principal}"},
			wantPublishDefault: []string{"general.*"},
			wantPublishGroups:  map[string][]string{"vip": {"room.vip"}},
			wantStatus:         http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// PUT channel rules
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/api/v1/tenants/"+tt.tenantID+"/channel-rules",
				bytes.NewReader([]byte(tt.body)))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("PUT status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			// GET channel rules back and verify publish fields roundtrip
			req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants/"+tt.tenantID+"/channel-rules", nil)
			addAuth(req)
			rec = httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("GET status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
			}

			var got struct {
				Rules struct {
					Public               []string            `json:"public"`
					PublishPublic        []string            `json:"publish_public"`
					PublishDefault       []string            `json:"publish_default"`
					PublishGroupMappings map[string][]string `json:"publish_group_mappings"`
				} `json:"rules"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("failed to parse GET response: %v", err)
			}

			if len(got.Rules.Public) != len(tt.wantPublic) {
				t.Errorf("public = %v, want %v", got.Rules.Public, tt.wantPublic)
			}
			if len(got.Rules.PublishPublic) != len(tt.wantPublishPublic) {
				t.Errorf("publish_public = %v, want %v", got.Rules.PublishPublic, tt.wantPublishPublic)
			}
			if len(got.Rules.PublishDefault) != len(tt.wantPublishDefault) {
				t.Errorf("publish_default = %v, want %v", got.Rules.PublishDefault, tt.wantPublishDefault)
			}
			if len(got.Rules.PublishGroupMappings) != len(tt.wantPublishGroups) {
				t.Errorf("publish_group_mappings = %v, want %v", got.Rules.PublishGroupMappings, tt.wantPublishGroups)
			}
		})
	}
}

func TestRouter_RoutingRules_RequiresAdminRole(t *testing.T) {
	t.Parallel()

	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKey(t)
	keyInfo := &auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(keyInfo); err != nil {
		t.Fatalf("AddKey failed: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator failed: %v", err)
	}

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router := mustNewRouter(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),

		Validator: validator,
	})

	tests := []struct {
		name          string
		roles         []string
		method        string
		wantForbidden bool // true = expect 403, false = expect auth to pass (any non-401/403)
	}{
		{
			name:          "user cannot PUT routing rules",
			roles:         []string{"user"},
			method:        http.MethodPut,
			wantForbidden: true,
		},
		{
			name:          "admin can PUT routing rules",
			roles:         []string{"admin"},
			method:        http.MethodPut,
			wantForbidden: false,
		},
		{
			name:          "user cannot POST routing rule",
			roles:         []string{"user"},
			method:        http.MethodPost,
			wantForbidden: true,
		},
		{
			name:          "admin can POST routing rule",
			roles:         []string{"admin"},
			method:        http.MethodPost,
			wantForbidden: false,
		},
		{
			name:          "user cannot DELETE routing rules",
			roles:         []string{"user"},
			method:        http.MethodDelete,
			wantForbidden: true,
		},
		{
			name:          "user can GET routing rules",
			roles:         []string{"user"},
			method:        http.MethodGet,
			wantForbidden: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			claims := &auth.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject:   "user-123",
					ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
					IssuedAt:  jwt.NewNumericDate(time.Now()),
				},
				TenantID: "test-tenant",
				Roles:    tt.roles,
			}
			token := createTestToken(t, privateKey, claims)

			var body *bytes.Reader
			switch tt.method {
			case http.MethodPut:
				rules := provisioning.ReplaceRoutingRulesRequest{
					Rules: []provisioning.TopicRoutingRule{
						{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
					},
				}
				b, _ := json.Marshal(rules)
				body = bytes.NewReader(b)
			case http.MethodPost:
				req := provisioning.AddRoutingRuleRequest{
					Rule: provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
				}
				b, _ := json.Marshal(req)
				body = bytes.NewReader(b)
			default:
				body = bytes.NewReader(nil)
			}

			req := httptest.NewRequestWithContext(context.Background(), tt.method, "/api/v1/tenants/test-tenant/routing-rules", body)
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if tt.wantForbidden {
				if rec.Code != http.StatusForbidden {
					t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
				}
			} else {
				// Verify auth passes (not 401/403)
				if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
					t.Errorf("expected auth to pass, got status %d; body: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// =============================================================================
// API Key Endpoint Tests
// =============================================================================

func TestRouter_APIKeys(t *testing.T) {
	t.Parallel()

	t.Run("create api key - 201", func(t *testing.T) {
		t.Parallel()

		tenantStore := testutil.NewMockTenantStore()
		routingStore := testutil.NewMockRoutingRulesStore()
		svc := newTestServiceWithStores(tenantStore, routingStore)

		_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

		router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
			Service: svc,
			Logger:  zerolog.Nop(),
		})

		body, _ := json.Marshal(map[string]string{"name": "test-key"})
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/tenants/test-tenant/api-keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if _, ok := resp["key_id"]; !ok {
			t.Error("response missing key_id field")
		}
	})

	t.Run("list api keys - 200", func(t *testing.T) {
		t.Parallel()

		tenantStore := testutil.NewMockTenantStore()
		routingStore := testutil.NewMockRoutingRulesStore()
		svc := newTestServiceWithStores(tenantStore, routingStore)

		_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

		router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
			Service: svc,
			Logger:  zerolog.Nop(),
		})

		// Create a key first
		body, _ := json.Marshal(map[string]string{"name": "list-test-key"})
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/tenants/test-tenant/api-keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("seed create status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
		}

		// List keys
		req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants/test-tenant/api-keys", nil)
		addAuth(req)
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var resp struct {
			Items []map[string]any `json:"items"`
			Total int              `json:"total"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if len(resp.Items) == 0 {
			t.Error("expected at least 1 item in list, got 0")
		}
	})

	t.Run("revoke api key - 200", func(t *testing.T) {
		t.Parallel()

		tenantStore := testutil.NewMockTenantStore()
		routingStore := testutil.NewMockRoutingRulesStore()
		svc := newTestServiceWithStores(tenantStore, routingStore)

		_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

		router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
			Service: svc,
			Logger:  zerolog.Nop(),
		})

		// Create a key first
		body, _ := json.Marshal(map[string]string{"name": "revoke-test-key"})
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/tenants/test-tenant/api-keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("seed create status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
		}

		var createResp struct {
			KeyID string `json:"key_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &createResp); err != nil {
			t.Fatalf("failed to parse create response: %v", err)
		}

		// Revoke the key
		req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/api/v1/tenants/test-tenant/api-keys/"+createResp.KeyID, nil)
		addAuth(req)
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}
	})

	t.Run("get active api keys - 200", func(t *testing.T) {
		t.Parallel()

		router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
			Service: newTestService(),
			Logger:  zerolog.Nop(),
		})

		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/api-keys/active", nil)
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var resp struct {
			Keys []map[string]any `json:"keys"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
	})

	t.Run("create api key - tenant not found - 404", func(t *testing.T) {
		t.Parallel()

		router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
			Service: newTestService(),
			Logger:  zerolog.Nop(),
		})

		body, _ := json.Marshal(map[string]string{"name": "orphan-key"})
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/tenants/nonexistent-tenant/api-keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		// The mock tenant store returns ErrTenantNotFound for nonexistent tenants,
		// which writeServiceError maps to 404.
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusNotFound, rec.Body.String())
		}
	})
}

// emptyKeyRegistry implements auth.KeyRegistry with no keys (all lookups fail).
// Used only for the license auth test where tenant JWT validation is in the
// middleware chain but never exercised (admin JWT takes precedence).
type emptyKeyRegistry struct{}

func (emptyKeyRegistry) GetKey(context.Context, string) (*auth.KeyInfo, error) {
	return nil, auth.ErrKeyNotFound
}

func (emptyKeyRegistry) GetKeysByTenantUUID(context.Context, string) ([]*auth.KeyInfo, error) {
	return nil, nil
}

func (emptyKeyRegistry) Close() error { return nil }

// noopLicenseStore implements provisioning.LicenseStateStore as a no-op.
// Used in the license auth integration test for the happy-path (200) case
// where Upsert is called after a successful license reload.
type noopLicenseStore struct{}

func (noopLicenseStore) Upsert(context.Context, string, string, string, *time.Time) error {
	return nil
}

func (noopLicenseStore) Load(context.Context) (string, error) { return "", nil }

func TestRouter_RenameTenant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testutil.MockTenantStore)
		reqBody    any
		wantStatus int
		wantCode   string
	}{
		{
			name: "happy path — 200 with updated slug",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			},
			reqBody:    map[string]string{"slug": "new-corp"},
			wantStatus: http.StatusOK,
		},
		{
			name: "slug unchanged — 400",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			},
			reqBody:    map[string]string{"slug": "test-tenant"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "SLUG_UNCHANGED",
		},
		{
			name: "slug invalid — 400",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			},
			reqBody:    map[string]string{"slug": "Invalid_Slug"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "SLUG_INVALID",
		},
		{
			name: "slug reserved — 400",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			},
			reqBody:    map[string]string{"slug": "rename"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "SLUG_RESERVED",
		},
		{
			name: "slug already taken — 409",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
				_ = ts.Create(context.Background(), testutil.NewTestTenant("taken-corp"))
			},
			reqBody:    map[string]string{"slug": "taken-corp"},
			wantStatus: http.StatusConflict,
			wantCode:   "SLUG_ALREADY_TAKEN",
		},
		{
			name:       "tenant not found — 404",
			setup:      func(_ *testutil.MockTenantStore) {},
			reqBody:    map[string]string{"slug": "new-corp"},
			wantStatus: http.StatusNotFound,
			wantCode:   "TENANT_NOT_FOUND",
		},
		{
			name: "invalid JSON — 400",
			setup: func(ts *testutil.MockTenantStore) {
				_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
			},
			reqBody:    "not-json{{{",
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := testutil.NewMockTenantStore()
			tt.setup(ts)
			svc := newTestServiceWithStores(ts, testutil.NewMockRoutingRulesStore())

			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
				Service: svc,
				Logger:  zerolog.Nop(),
			})

			var body []byte
			switch v := tt.reqBody.(type) {
			case string:
				body = []byte(v)
			default:
				body, _ = json.Marshal(v)
			}

			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
				"/api/v1/tenants/test-tenant/rename", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				var errResp struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
					t.Fatalf("unmarshal error response: %v; body: %s", err, rec.Body.String())
				}
				if errResp.Code != tt.wantCode {
					t.Errorf("error code = %q, want %q; body: %s", errResp.Code, tt.wantCode, rec.Body.String())
				}
			}
		})
	}
}

func TestRouter_RenameTenant_RequiresAdminRole(t *testing.T) {
	t.Parallel()

	ts := testutil.NewMockTenantStore()
	_ = ts.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	// Build a validator with a known key, then sign the user-role token with that same key.
	ecPEM, privateKey := generateTestECKey(t)
	registry := auth.NewStaticKeyRegistry()
	if err := registry.AddKey(&auth.KeyInfo{
		KeyID:        "test-key-1",
		TenantID:     "test-tenant",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator: %v", err)
	}

	router := mustNewRouter(t, api.RouterConfig{
		Service:   newTestServiceWithStores(ts, testutil.NewMockRoutingRulesStore()),
		Logger:    zerolog.Nop(),
		Validator: validator,
	})

	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-only",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "test-tenant",
		Roles:    []string{"user"},
	}
	token := createTestToken(t, privateKey, claims)

	body, _ := json.Marshal(map[string]string{"slug": "new-corp"})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/api/v1/tenants/test-tenant/rename", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d (non-admin must be denied); body: %s",
			rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

// TestIsReservedSlug_CoversAllRouteSegments verifies that all action-endpoint path
// segments used in the router are in the reserved slug list, so no tenant can shadow them.
func TestIsReservedSlug_CoversAllRouteSegments(t *testing.T) {
	t.Parallel()

	// These are the exact route segment strings used in api/router.go as action endpoints
	// or resource collections nested under /tenants/{tenantSlug}/. If any of these were
	// not reserved, a tenant with that slug could shadow the route.
	routeSegments := []string{
		"rename", "suspend", "reactivate",
		"keys", "api-keys", "routing-rules", "quotas", "audit",
		"channel-rules", "tokens", "test-access",
	}

	svc, _ := newTestServiceWithKafka(
		testutil.NewMockTenantStore(),
		testutil.NewMockRoutingRulesStore(),
	)
	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	for _, seg := range routeSegments {
		t.Run(seg, func(t *testing.T) {
			t.Parallel()
			body, _ := json.Marshal(map[string]string{"slug": seg, "name": seg + " corp"})
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
				"/api/v1/tenants", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("slug %q: status = %d, want 400 (must be rejected as reserved); body: %s",
					seg, rec.Code, rec.Body.String())
				return
			}
			var errResp struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("slug %q: unmarshal error response: %v; body: %s", seg, err, rec.Body.String())
			}
			if errResp.Code != "SLUG_RESERVED" {
				t.Errorf("slug %q: code = %q, want SLUG_RESERVED; body: %s", seg, errResp.Code, rec.Body.String())
			}
		})
	}
}

//nolint:paralleltest // shares package-level publicKey via license.SetPublicKeyForTesting
func TestRouter_LicenseEndpoint_AdminAuth(t *testing.T) {
	// --- License signing keypair (for the happy-path 200 test case) ---
	licensePriv, licensePub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(licensePub)

	// --- Admin auth keypair (for AdminJWTMiddleware) ---
	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate admin keypair: %v", err)
	}
	const adminKID = "test-admin-key-1"

	// Register admin key in the AdminKeyRegistry.
	adminRegistry := provauth.NewAdminKeyRegistry()
	adminRegistry.Refresh([]*auth.KeyInfo{{
		KeyID:     adminKID,
		Algorithm: "EdDSA",
		PublicKey: adminPub,
		IsActive:  true,
	}})
	adminValidator := provauth.NewAdminValidator(adminRegistry)

	// Generate a second unregistered Ed25519 keypair (for UNAUTHORIZED test case).
	_, unregisteredPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate unregistered keypair: %v", err)
	}

	// Create MultiTenantValidator with an empty key registry.
	tenantValidator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    emptyKeyRegistry{},
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("create tenant validator: %v", err)
	}

	// Create a real LicenseManager via NewManager (empty key = Community, but Reload
	// uses the package-level publicKey overridden above).
	licenseManager, err := license.NewManager("", zerolog.Nop())
	if err != nil {
		t.Fatalf("create license manager: %v", err)
	}

	licenseHandler := api.NewLicenseHandler(
		licenseManager,
		noopLicenseStore{},
		eventbus.New(zerolog.Nop()),
		zerolog.Nop(),
	)

	router := mustNewRouter(t, api.RouterConfig{
		Service:        newTestService(),
		Logger:         zerolog.Nop(),
		Validator:      tenantValidator,
		AdminValidator: adminValidator,
		LicenseHandler: licenseHandler,
	})

	// Helper: mint an admin JWT with the given private key, kid, and roles.
	mintAdminJWT := func(privKey ed25519.PrivateKey, kid string, roles []string) string {
		t.Helper()
		now := time.Now()
		claims := jwt.MapClaims{
			"iss":   "sukko-admin",
			"sub":   "test-admin",
			"roles": roles,
			"exp":   jwt.NewNumericDate(now.Add(5 * time.Minute)),
			"iat":   jwt.NewNumericDate(now),
			"jti":   uuid.NewString(),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		token.Header["kid"] = kid
		signed, err := token.SignedString(privKey)
		if err != nil {
			t.Fatalf("sign admin JWT: %v", err)
		}
		return signed
	}

	// Sign a valid test license for the happy-path case.
	// Manager is Community (no key), so key must use Community edition to pass the edition gate.
	validLicenseKey := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "Test Org",
		Iat:     time.Now().Unix(),
		Exp:     time.Now().Add(24 * time.Hour).Unix(),
	}, licensePriv)

	tests := []struct {
		name       string
		setAuth    func(*http.Request)
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "no auth header → MISSING_TOKEN",
			setAuth:    nil,
			body:       `{"key":"test"}`,
			wantStatus: http.StatusUnauthorized,
			wantCode:   "MISSING_TOKEN",
		},
		{
			name: "admin JWT signed by unregistered key → UNAUTHORIZED",
			setAuth: func(req *http.Request) {
				token := mintAdminJWT(unregisteredPriv, "unregistered-kid", []string{"admin"})
				req.Header.Set("Authorization", "Bearer "+token)
			},
			body:       `{"key":"test"}`,
			wantStatus: http.StatusUnauthorized,
			wantCode:   "UNAUTHORIZED",
		},
		{
			name: "valid admin JWT with wrong role → INSUFFICIENT_ROLE",
			setAuth: func(req *http.Request) {
				token := mintAdminJWT(adminPriv, adminKID, []string{"viewer"})
				req.Header.Set("Authorization", "Bearer "+token)
			},
			body:       `{"key":"test"}`,
			wantStatus: http.StatusForbidden,
			wantCode:   "INSUFFICIENT_ROLE",
		},
		{
			name: "valid admin JWT + valid license → 200 (end-to-end)",
			setAuth: func(req *http.Request) {
				token := mintAdminJWT(adminPriv, adminKID, []string{"admin"})
				req.Header.Set("Authorization", "Bearer "+token)
			},
			body:       `{"key":"` + validLicenseKey + `"}`,
			wantStatus: http.StatusOK,
			wantCode:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel — shares package-level license publicKey.

			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/license", bytes.NewReader([]byte(tt.body)))
			req.Header.Set("Content-Type", "application/json")
			if tt.setAuth != nil {
				tt.setAuth(req)
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			if tt.wantCode != "" {
				var errResp struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
					t.Fatalf("unmarshal error response: %v; body: %s", err, rec.Body.String())
				}
				if errResp.Code != tt.wantCode {
					t.Errorf("error code = %q, want %q; body: %s", errResp.Code, tt.wantCode, rec.Body.String())
				}
			}
		})
	}
}

func TestRouter_TokenRevocation_EditionGate(t *testing.T) {
	t.Parallel()

	// Pre-create the tenant so RequireTenant middleware can resolve the slug.
	tenantStore := testutil.NewMockTenantStore()
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
	svc := newTestServiceWithStores(tenantStore, testutil.NewMockRoutingRulesStore())

	// RevocationHandler setup — revocation.New starts a pruneLoop goroutine; Close is required.
	// t.Cleanup fires after all parallel subtests complete (unlike defer, which fires when the
	// parent function returns — before parallel subtests resume).
	store := revocation.New(zerolog.Nop())
	t.Cleanup(store.Close)
	bus := eventbus.New(zerolog.Nop())
	revHandler := api.NewRevocationHandler(store, bus, time.Hour, 30, 5, zerolog.Nop())

	body := `{"sub":"user-123"}`

	tests := []struct {
		name       string
		handler    *api.RevocationHandler
		edition    license.Edition
		wantStatus int
		wantCode   string
	}{
		{
			name:       "community blocked — EDITION_LIMIT",
			handler:    revHandler,
			edition:    license.Community,
			wantStatus: http.StatusForbidden,
			wantCode:   api.ErrCodeEditionLimit,
		},
		{
			name:       "pro allowed",
			handler:    revHandler,
			edition:    license.Pro,
			wantStatus: http.StatusOK,
		},
		{
			name:       "nil handler — route unregistered",
			handler:    nil,
			edition:    license.Pro,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
				Service:           svc,
				Logger:            zerolog.Nop(),
				EditionManager:    license.NewTestManager(tt.edition),
				RevocationHandler: tt.handler,
			})

			req := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/api/v1/tenants/test-tenant/tokens/revoke",
				bytes.NewBufferString(body),
			)
			req.Header.Set("Content-Type", "application/json")
			addAuth(req)

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				var errResp struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
					t.Fatalf("unmarshal error response: %v; body: %s", err, rec.Body.String())
				}
				if errResp.Code != tt.wantCode {
					t.Errorf("error code = %q, want %q; body: %s", errResp.Code, tt.wantCode, rec.Body.String())
				}
			}
		})
	}
}

// TestChannelRules_CommunityAccess verifies channel-rules routes are ungated:
// channel rules are the sole channel-authorization mechanism (provisioning-
// only), so Community deployments MUST be able to manage them. Previously the
// write routes were Pro-gated via RequireFeature.
func TestChannelRules_CommunityAccess(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	channelRulesStore := testutil.NewMockChannelRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore, channelRulesStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service:        svc,
		Logger:         zerolog.Nop(),
		EditionManager: license.NewTestManager(license.Community),
	})

	// PUT channel rules under Community MUST succeed (no FEATURE/EDITION 403).
	rulesBody := `{"public":["general.*"],"publish_public":["general.*"]}`
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPut,
		"/api/v1/tenants/test-tenant/channel-rules", bytes.NewBufferString(rulesBody))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT channel-rules under Community = %d, want %d; body: %s",
			rec.Code, http.StatusOK, rec.Body.String())
	}

	// GET and DELETE also reachable.
	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet,
		"/api/v1/tenants/test-tenant/channel-rules", nil)
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET channel-rules under Community = %d; body: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/api/v1/tenants/test-tenant/channel-rules", nil)
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE channel-rules under Community = %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestRouter_PushRoutes_RegisteredWhenHandlerSet verifies the #173 wiring: when the push
// handlers are set on RouterConfig, POST /api/v1/push/credentials is registered (an
// unauthenticated request reaches auth middleware → 401, not 404); when unset, the route is
// absent (404). This is the regression guard for the "handlers never wired → routes 404" bug.
func TestRouter_PushRoutes_RegisteredWhenHandlerSet(t *testing.T) {
	t.Parallel()
	pool := sharedtestutil.NewTestPool(t)
	credRepo, err := repository.NewCredentialsRepository(pool, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("new credentials repo: %v", err)
	}
	h, err := api.NewPushCredentialHandler(credRepo, repository.NewTenantRepository(pool).GetBySlug, eventbus.New(zerolog.Nop()), zerolog.Nop(), nil)
	if err != nil {
		t.Fatalf("new push credential handler: %v", err)
	}

	// Authenticate the request so it passes AuthMiddleware/RequireRole/RequireFeature (default
	// EditionManager is Enterprise) — otherwise an unauthenticated request short-circuits at
	// 401 before route matching, making "registered vs not" indistinguishable. With the handler
	// wired, the route is registered → the request reaches the handler (400 empty body), NOT 404.
	wired, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service:               newTestService(),
		Logger:                zerolog.Nop(),
		PushCredentialHandler: h,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", http.NoBody)
	addAuth(req)
	rec := httptest.NewRecorder()
	wired.ServeHTTP(rec, req)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("push credentials route not registered (404) despite handler set; body: %s", rec.Body.String())
	}

	// Without the handler, the route is not registered → an authenticated request 404s.
	bare, addAuth2 := mustNewRouterWithAuth(t, api.RouterConfig{Service: newTestService(), Logger: zerolog.Nop()})
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/push/credentials", http.NoBody)
	addAuth2(req2)
	rec2 := httptest.NewRecorder()
	bare.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Errorf("without handler set, route should be 404, got %d; body: %s", rec2.Code, rec2.Body.String())
	}
}

// newCommunityServiceWithKafka builds a provisioning service whose SERVICE-level edition
// manager is Community, so the per-edition routing-rule count (10) is the binding limit
// rather than the config limit (100). The router's EditionManager is separate — it drives
// RequireFeature middleware; this one drives the service's quota checks.
func newCommunityServiceWithKafka(
	tenantStore *testutil.MockTenantStore,
	routingStore *testutil.MockRoutingRulesStore,
) (*provisioning.Service, *testutil.MockKafkaAdmin) {
	kafkaAdmin := testutil.NewMockKafkaAdmin()
	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantStore,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           routingStore,
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    eventbus.New(zerolog.Nop()),
		EditionManager:              license.NewTestManager(license.Community),
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    100,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  604800000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newCommunityServiceWithKafka: " + err.Error())
	}
	return svc, kafkaAdmin
}

// routingRulesBody builds a PUT body with n distinct rules, all pointing at one
// pre-provisioned topic so only the rule COUNT varies.
func routingRulesBody(n int) []byte {
	rules := make([]map[string]any, 0, n)
	for i := range n {
		rules = append(rules, map[string]any{
			"pattern":  fmt.Sprintf("test.%d.**", i),
			"topics":   []string{"trade"},
			"priority": i + 1,
		})
	}
	b, _ := json.Marshal(map[string]any{"rules": rules})
	return b
}

// TestRoutingRules_CommunityAccessAndQuota is the routing-rules sibling of
// TestChannelRules_CommunityAccess. Routing rules are ungated on every edition (ADR-0014),
// so Community reaches the API — and the per-edition COUNT (Community 10) is the wall.
// The count MUST hold on BOTH write paths: PUT (full replace) and POST (incremental add).
// A POST path that only checks the config limit (100) would let Community walk past its
// 10-rule cap one rule at a time, silently voiding the tier wall.
func TestRoutingRules_CommunityAccessAndQuota(t *testing.T) {
	t.Parallel()

	const communityMaxRules = 10 // license.DefaultLimits(Community).MaxRoutingRulesPerTenant

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc, kafkaAdmin := newCommunityServiceWithKafka(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
	_ = kafkaAdmin.CreateTopic(context.Background(), "test.test-tenant.trade", 1, 1, nil)

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service:        svc,
		Logger:         zerolog.Nop(),
		EditionManager: license.NewTestManager(license.Community),
	})

	put := func(n int) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPut,
			"/api/v1/tenants/test-tenant/routing-rules", bytes.NewReader(routingRulesBody(n)))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	// 1. Ungated (ADR-0014): a within-cap PUT under Community must succeed — no feature 403.
	if rec := put(communityMaxRules); rec.Code != http.StatusOK {
		t.Fatalf("PUT %d rules under Community = %d, want %d; body: %s",
			communityMaxRules, rec.Code, http.StatusOK, rec.Body.String())
	}

	// 2. The COUNT is the wall: one rule past the edition cap is rejected by the service's
	// edition check (the config validator's 100 passes first), so the code is the
	// dimensioned EDITION_LIMIT_ROUTING_RULES_PER_TENANT — never the bare EDITION_LIMIT
	// feature code, which would mean the gate came back.
	rec := put(communityMaxRules + 1)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("PUT %d rules under Community = %d, want %d; body: %s",
			communityMaxRules+1, rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if code := errorCodeOf(t, rec); code != "EDITION_LIMIT_ROUTING_RULES_PER_TENANT" {
		t.Errorf("PUT over-cap code = %q, want EDITION_LIMIT_ROUTING_RULES_PER_TENANT; body: %s",
			code, rec.Body.String())
	}

	// 3. Same wall on the POST path. Reset to exactly the cap, then add one more.
	if rec := put(communityMaxRules); rec.Code != http.StatusOK {
		t.Fatalf("reset to %d rules = %d; body: %s", communityMaxRules, rec.Code, rec.Body.String())
	}
	addBody, _ := json.Marshal(map[string]any{"rule": map[string]any{
		"pattern": "test.overflow.**", "topics": []string{"trade"}, "priority": 999,
	}})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/api/v1/tenants/test-tenant/routing-rules", bytes.NewReader(addBody))
	req.Header.Set("Content-Type", "application/json")
	addAuth(req)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("POST rule %d under Community = %d, want %d (the edition cap must bind on the "+
			"incremental path too, else Community walks past it one rule at a time); body: %s",
			communityMaxRules+1, rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if code := errorCodeOf(t, rec); code != "EDITION_LIMIT_ROUTING_RULES_PER_TENANT" {
		t.Errorf("POST over-cap code = %q, want EDITION_LIMIT_ROUTING_RULES_PER_TENANT; body: %s",
			code, rec.Body.String())
	}
}

// errorCodeOf extracts the `code` field from an ErrorResponse body.
func errorCodeOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return body.Code
}

// TestCreateKey_DuplicateIsConflict pins the status for re-registering an existing key id.
// A duplicate is a caller-correctable collision, not a server fault: §XII maps 409 to
// conflict, and returning 500 both misreports blame and defeats idempotent bootstrap
// scripts, which cannot distinguish "already done" from "the server is broken".
func TestCreateKey_DuplicateIsConflict(t *testing.T) {
	t.Parallel()

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc := newTestServiceWithStores(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))

	router, addAuth := mustNewRouterWithAuth(t, api.RouterConfig{
		Service: svc,
		Logger:  zerolog.Nop(),
	})

	// A valid ES256 public key in PEM, so the request passes structural validation and
	// reaches the store on both attempts.
	pubPEM, _ := generateTestECKey(t)
	body, err := json.Marshal(map[string]any{
		"key_id":     "dup-key",
		"algorithm":  "ES256",
		"public_key": pubPEM,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
			"/api/v1/tenants/test-tenant/keys", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuth(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	if rec := post(); rec.Code != http.StatusCreated {
		t.Fatalf("first POST = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	rec := post()
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d, want %d (409); body: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if code := errorCodeOf(t, rec); code != "KEY_ALREADY_EXISTS" {
		t.Errorf("duplicate POST code = %q, want KEY_ALREADY_EXISTS; body: %s", code, rec.Body.String())
	}
}
