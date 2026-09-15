package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
)

// TestAuthenticateRequest_CrossTenantJWT_Rejected is the gateway-level regression
// for #158: a JWT signed by tenant A's key but claiming tenant B is rejected with
// ErrTenantMismatch; a matching-tenant token is accepted.
func TestAuthenticateRequest_CrossTenantJWT_Rejected(t *testing.T) {
	t.Parallel()

	ecPEM, privateKey := generateTestECKeyForGateway(t)
	registry := auth.NewStaticKeyRegistry()
	key := &auth.KeyInfo{KeyID: "k1", TenantID: "tenant-a", Algorithm: "ES256", PublicKeyPEM: ecPEM, IsActive: true}
	if err := registry.AddKey(key); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:     registry,
		RequireTenantID: true,
		TenantResolver:  identityTenantResolver{}, // resolves slug->slug; key TenantID is a slug here
	})
	if err != nil {
		t.Fatalf("NewMultiTenantValidator: %v", err)
	}
	gw := &Gateway{
		config:    &platform.GatewayConfig{AuthConfig: platform.AuthConfig{AuthMode: "required"}},
		validator: validator,
		logger:    zerolog.Nop(),
	}

	mint := func(tenantClaim string) string {
		return createTestTokenForGateway(t, key, privateKey, &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   "user-1",
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
			TenantID: tenantClaim,
		})
	}

	// Cross-tenant: key owns tenant-a, token claims tenant-b -> rejected.
	req := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+mint("tenant-b"))
	if _, err := gw.authenticateRequest(context.Background(), req); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant token: err = %v, want ErrTenantMismatch", err)
	}

	// Matching tenant -> accepted.
	req2 := httptest.NewRequest(http.MethodGet, "/ws", http.NoBody)
	req2.Header.Set("Authorization", "Bearer "+mint("tenant-a"))
	if _, err := gw.authenticateRequest(context.Background(), req2); err != nil {
		t.Fatalf("matching-tenant token: err = %v, want nil", err)
	}
}

func TestAuthenticateRequest_NoCredentials(t *testing.T) {
	t.Parallel()

	gw := &Gateway{
		config: &platform.GatewayConfig{
			AuthConfig: platform.AuthConfig{AuthMode: "required"},
		},
		logger: testLogger(),
	}

	req := httptest.NewRequest(http.MethodGet, "/sse", http.NoBody)
	// No Authorization header, no X-API-Key, no ?token=
	_, err := gw.authenticateRequest(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for no credentials")
	}
	if !errors.Is(err, ErrNoCredentials) {
		t.Errorf("error = %v, want ErrNoCredentials", err)
	}
}

func TestAuthenticateRequest_SentinelErrors(t *testing.T) {
	t.Parallel()

	// Verify sentinel errors are distinct
	if errors.Is(ErrNoCredentials, ErrInvalidToken) {
		t.Error("ErrNoCredentials should not equal ErrInvalidToken")
	}
	if errors.Is(ErrInvalidAPIKey, ErrTenantMismatch) {
		t.Error("ErrInvalidAPIKey should not equal ErrTenantMismatch")
	}
}

// TestAuthenticateRequest_APIKeyOnly_PopulatesAPIKeyID verifies that the api_key-only
// auth path correctly populates APIKeyID from the resolved KeyID and leaves UserID empty.
func TestAuthenticateRequest_APIKeyOnly_PopulatesAPIKeyID(t *testing.T) {
	t.Parallel()

	cfg := &platform.GatewayConfig{
		AuthConfig: platform.AuthConfig{AuthMode: "required"},
	}
	// API keys carry only the tenant UUID; the gateway resolves the slug from
	// the reverse projection for data-plane channel scoping (#161 B0).
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"sk_live_secret": {
			KeyID:    "db-key-uuid-001",
			TenantID: "tenant-uuid-acme",
			Name:     "test key",
			IsActive: true,
		},
	}}
	gw := &Gateway{
		config:         cfg,
		apiKeyRegistry: mock,
		tenantSlugResolver: &mockTenantSlugResolver{
			slugs:   map[string]string{"tenant-uuid-acme": "acme"},
			present: true,
		},
		logger: zerolog.Nop(),
	}

	req := httptest.NewRequest(http.MethodGet, "/ws?api_key=sk_live_secret", http.NoBody)
	result, err := gw.authenticateRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("authenticateRequest() error = %v, want nil", err)
	}

	if result.APIKeyID != "db-key-uuid-001" {
		t.Errorf("APIKeyID = %q, want %q", result.APIKeyID, "db-key-uuid-001")
	}
	if result.UserID != "" {
		t.Errorf("UserID = %q, want empty for API-key-only auth", result.UserID)
	}
	if result.AuthMethod != "api_key" {
		t.Errorf("AuthMethod = %q, want %q", result.AuthMethod, "api_key")
	}
	// TenantUUID is the API key's owning tenant UUID; TenantSlug is resolved from it.
	if result.TenantUUID != "tenant-uuid-acme" {
		t.Errorf("TenantUUID = %q, want %q", result.TenantUUID, "tenant-uuid-acme")
	}
	if result.TenantSlug != "acme" {
		t.Errorf("TenantSlug = %q, want %q", result.TenantSlug, "acme")
	}
}

// TestAuthenticateRequest_APIKey_FailClosed asserts API-key auth never proceeds
// with an empty tenant slug (§II/§IX): a cold projection or missing resolver is
// a retryable ErrTenantUnavailable, and an unknown tenant against a warm
// projection is a hard ErrTenantMismatch.
func TestAuthenticateRequest_APIKey_FailClosed(t *testing.T) {
	t.Parallel()

	cfg := &platform.GatewayConfig{
		AuthConfig: platform.AuthConfig{AuthMode: "required"},
	}
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{
		"sk_live_secret": {KeyID: "k1", TenantID: "tenant-uuid-acme", Name: "k", IsActive: true},
	}}

	tests := []struct {
		name     string
		resolver TenantSlugResolver
		wantErr  error
	}{
		{"nil resolver fails closed", nil, ErrTenantUnavailable},
		{"cold projection is retryable", &mockTenantSlugResolver{present: false}, ErrTenantUnavailable},
		{"unknown tenant on warm projection is rejected", &mockTenantSlugResolver{slugs: map[string]string{}, present: true}, ErrTenantMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gw := &Gateway{
				config:             cfg,
				apiKeyRegistry:     mock,
				tenantSlugResolver: tt.resolver,
				logger:             zerolog.Nop(),
			}
			req := httptest.NewRequest(http.MethodGet, "/ws?api_key=sk_live_secret", http.NoBody)
			if _, err := gw.authenticateRequest(context.Background(), req); !errors.Is(err, tt.wantErr) {
				t.Errorf("authenticateRequest() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestAuthenticateRequest_APIKeyOnly_InvalidKey verifies that APIKeyID is NOT populated
// when the API key is invalid (lookup fails).
func TestAuthenticateRequest_APIKeyOnly_InvalidKey(t *testing.T) {
	t.Parallel()

	cfg := &platform.GatewayConfig{
		AuthConfig: platform.AuthConfig{AuthMode: "required"},
	}
	mock := &mockAPIKeyLookup{keys: map[string]*provapi.APIKeyInfo{}} // empty registry
	gw := &Gateway{
		config:         cfg,
		apiKeyRegistry: mock,
		logger:         zerolog.Nop(),
	}

	req := httptest.NewRequest(http.MethodGet, "/ws?api_key=nonexistent", http.NoBody)
	_, err := gw.authenticateRequest(context.Background(), req)
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey", err)
	}
}
