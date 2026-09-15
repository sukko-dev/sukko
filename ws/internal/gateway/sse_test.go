package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// sseTestGatewayWithJWT creates a Gateway with JWT auth for SSE handler testing.
// Returns the gateway and a valid Bearer token for tenant "acme".
func sseTestGatewayWithJWT(t *testing.T) (gw *Gateway, token string) {
	t.Helper()

	registry := auth.NewStaticKeyRegistry()
	ecPEM, privateKey := generateTestECKeyForGateway(t)

	key := &auth.KeyInfo{
		KeyID:        "sse-test-key",
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

	gw = &Gateway{
		config: &platform.GatewayConfig{
			AuthConfig:           platform.AuthConfig{AuthMode: "required"},
			SSEKeepAliveInterval: 45_000_000_000, // 45s
		},
		validator: validator,
		logger:    testLogger(),
		// Permissive rules for tenant "acme" — provisioning-only authorization
		// requires a checker; individual tests override for deny scenarios.
		tenantPermChecker: testTenantChecker("acme", &types.ChannelRules{
			Public:        []string{"*"},
			PublishPublic: []string{"*"},
		}),
	}

	return gw, tokenString
}

func TestHandleSSE_MissingChannels(t *testing.T) {
	t.Parallel()

	gw, _ := sseTestGatewayWithJWT(t)

	req := httptest.NewRequest(http.MethodGet, "/sse", http.NoBody)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleSSE_EmptyChannels(t *testing.T) {
	t.Parallel()

	gw, _ := sseTestGatewayWithJWT(t)

	req := httptest.NewRequest(http.MethodGet, "/sse?channels=", http.NoBody)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleSSE_EmptyChannelsWithCommas(t *testing.T) {
	t.Parallel()

	gw, _ := sseTestGatewayWithJWT(t)

	req := httptest.NewRequest(http.MethodGet, "/sse?channels=,,", http.NoBody)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleSSE_NoServerClient(t *testing.T) {
	t.Parallel()

	gw, token := sseTestGatewayWithJWT(t)
	gw.serverClient = nil

	req := httptest.NewRequest(http.MethodGet, "/sse?channels=acme.general.messages", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleSSE_TenantLimitExceeded(t *testing.T) {
	t.Parallel()

	gw, token := sseTestGatewayWithJWT(t)
	// Create a tracker with limit=0 (always rejects)
	gw.connTracker = NewTenantConnectionTracker(0)

	req := httptest.NewRequest(http.MethodGet, "/sse?channels=acme.general.messages", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	// §IX: a 429 MUST carry Retry-After — without it a polite client retries
	// immediately into a limit that has not moved.
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive integer (§IX)", got)
	}
}

func TestHandleSSE_AllChannelsFiltered(t *testing.T) {
	t.Parallel()

	gw, token := sseTestGatewayWithJWT(t)
	// Rules allow *.trade for acme — but the channels below use a wrong
	// tenant prefix, so all are filtered before rules even apply.
	gw.tenantPermChecker = testTenantChecker("acme", &types.ChannelRules{Public: []string{"*.trade"}})

	// All channels have wrong tenant → all filtered by filterSubscribeChannels
	// (JWT tenant is "acme" but channels use "wrong")
	req := httptest.NewRequest(http.MethodGet, "/sse?channels=wrong.BTC.trade,wrong.ETH.trade", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	gw.HandleSSE(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
