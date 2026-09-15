package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

func TestHandlePublish_NoServerClient(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)
	gw.serverClient = nil // no gRPC client

	body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlePublish_InvalidJSON(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePublish_MissingChannel(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)

	body := `{"data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorCode(t, rec, "INVALID_REQUEST")
}

func TestHandlePublish_MissingData(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)

	body := `{"channel":"test.ch"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlePublish_BodyTooLarge(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)
	gw.config.MaxPublishSize = 10 // 10 bytes max

	body := `{"channel":"test.ch","data":{"msg":"this is way too long for 10 bytes"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestHandlePublish_InvalidContentType(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader("data"))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlePublish_RateLimited(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)
	// Create a rate limiter with burst=1
	gw.publishRateLimiter = &PublishRateLimiter{
		rateLimit: 0.001, // extremely low rate
		burst:     1,
	}

	makeReq := func() *httptest.ResponseRecorder {
		body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		gw.HandlePublish(rec, req)
		return rec
	}

	// First request passes (burst=1)
	rec1 := makeReq()
	// With no server client, it should get to ServiceUnavailable (past rate limit)
	if rec1.Code != http.StatusServiceUnavailable {
		t.Errorf("first request: status = %d, want %d (no server client)", rec1.Code, http.StatusServiceUnavailable)
	}

	// Second request should be rate limited
	rec2 := makeReq()
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
	// §IX: the 429 MUST advertise Retry-After. The value is derived from the injected
	// limiter's own rate (0.001/s → one token per 1000s), so it is exact and this also
	// pins that the hint comes from the ENFORCING limiter rather than from config.
	if got := rec2.Header().Get("Retry-After"); got != "1000" {
		t.Errorf("second request: Retry-After = %q, want %q (1/0.001 s) (§IX)", got, "1000")
	}
}

// assertErrorCode checks the error response has the expected code field.
func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	var resp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}
	if resp.Code != wantCode {
		t.Errorf("error code = %q, want %q", resp.Code, wantCode)
	}
}

// testLogger returns a no-op logger for tests.
func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

func TestHandlePublish_APIKeyOnly_Forbidden(t *testing.T) {
	t.Parallel()

	gw, _ := publishTestGatewayWithJWT(t)
	// Simulate API-key-only by setting up an API key registry
	gw.apiKeyRegistry = &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"test-key": {KeyID: "k1", TenantID: "acme", IsActive: true},
	}}
	gw.tenantSlugResolver = &mockTenantSlugResolver{slugs: map[string]string{"acme": "acme"}, present: true}

	body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "test-key")
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

// publishTestGatewayWithJWT creates a Gateway with JWT auth enabled and returns
// the gateway, a valid JWT token string, and the tenant ID.
func publishTestGatewayWithJWT(t *testing.T) (_ *Gateway, _ string) {
	t.Helper()

	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "pub-test-key",
		TenantID:     "acme",
		Algorithm:    "ES256",
		PublicKeyPEM: ecPEM,
		IsActive:     true,
	}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey: %v", err)
	}

	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator: %v", err)
	}

	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "acme",
	}
	tokenString := createTestTokenForGateway(t, key, privateKey, claims)

	gw := &Gateway{
		config: &platform.GatewayConfig{
			AuthConfig:     platform.AuthConfig{AuthMode: "required"},
			MaxPublishSize: 65536,
		},
		validator: validator,
		logger:    testLogger(),
		// Permissive rules for tenant "acme" — provisioning-only authorization
		// requires a checker; tests override for rules-denied scenarios.
		tenantPermChecker: testTenantChecker("acme", &types.ChannelRules{
			Public:        []string{"*"},
			PublishPublic: []string{"*"},
		}),
	}

	return gw, tokenString
}

func TestHandlePublish_WrongTenantPrefix(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)

	// JWT tenant is "acme" but channel uses "wrong" prefix
	body := `{"channel":"wrong.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestHandlePublish_InvalidChannelFormat(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)

	// Single-part channel — less than MinInternalChannelParts
	body := `{"channel":"acme","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlePublish_ValidJWT_Passes(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)
	gw.serverClient = nil // will hit 503 after passing all checks

	// Valid tenant prefix + format
	body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	// Should pass all checks → reach server client → 503 (nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d (valid JWT should pass checks)", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlePublish_RulesDenied(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)
	// Tenant has subscribe-side rules but NO publish-side rules → all
	// publishes denied (side-specific deny-all).
	gw.tenantPermChecker = testTenantChecker("acme", &types.ChannelRules{
		Public: []string{"*"},
	})

	body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d (rules-denied publish)", rec.Code, http.StatusForbidden)
	}
	assertErrorCode(t, rec, "FORBIDDEN")
}

func TestHandlePublish_RulesAllowed_PassesAuthorization(t *testing.T) {
	t.Parallel()

	gw, token := publishTestGatewayWithJWT(t)
	// Publish-side rules allow the channel; with no server client the request
	// proceeds past authorization to the 503 backend guard — proving the
	// rules check allowed it.
	gw.tenantPermChecker = testTenantChecker("acme", &types.ChannelRules{
		PublishPublic: []string{"general.*"},
	})
	gw.serverClient = nil

	body := `{"channel":"acme.general.messages","data":{"msg":"hello"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandlePublish(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d (past authz, no backend)", rec.Code, http.StatusServiceUnavailable)
	}
}
