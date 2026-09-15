package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// withTenantSlug injects a chi URL param "tenantSlug" into r's context.
func withTenantSlug(r *http.Request, slug string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("tenantSlug", slug)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// withClaims injects auth claims into r's context.
func withClaims(r *http.Request, claims *auth.Claims) *http.Request {
	return r.WithContext(auth.WithClaims(r.Context(), claims))
}

// requireTenantHandler wraps a "got it" handler with RequireTenant using the provided lookup func.
// holdPeriod controls how long old-slug JWTs are accepted after a rename.
func requireTenantHandler(lookup provisioning.TenantLookupFunc, holdPeriod time.Duration) http.Handler {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return RequireTenant(lookup, holdPeriod)(ok)
}

func TestAuthMiddleware_SkipsWhenPreAuthenticated(t *testing.T) {
	t.Parallel()

	// Create a validator that should never be called — if it is, the test will
	// panic because StaticKeyRegistry has no keys, but more importantly the
	// handler assertion below would fail.
	registry := &auth.StaticKeyRegistry{}
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("unexpected error creating validator: %v", err)
	}

	logger := zerolog.Nop()

	// Pre-set claims in context (as admin token middleware would)
	preClaims := &auth.Claims{
		Roles: []string{"admin", "system"},
	}

	var gotClaims *auth.Claims
	handler := AuthMiddleware(validator, logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotClaims = auth.GetClaims(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	ctx := auth.WithClaims(context.Background(), preClaims)
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if gotClaims == nil {
		t.Fatal("expected claims in context")
	}
	if !gotClaims.HasRole("admin") {
		t.Error("expected admin role preserved from pre-auth")
	}
	if !gotClaims.HasRole("system") {
		t.Error("expected system role preserved from pre-auth")
	}
}

func TestAuthMiddleware_RejectsWhenNoClaims(t *testing.T) {
	t.Parallel()

	registry := &auth.StaticKeyRegistry{}
	validator, err := auth.NewMultiTenantValidator(auth.MultiTenantValidatorConfig{
		KeyRegistry:    registry,
		TenantResolver: identityTenantResolver{},
	})
	if err != nil {
		t.Fatalf("unexpected error creating validator: %v", err)
	}

	logger := zerolog.Nop()

	handlerCalled := false
	handler := AuthMiddleware(validator, logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/tenants", nil)
	// No Authorization header, no claims in context
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if handlerCalled {
		t.Error("handler should not have been called without auth")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestRequireTenant(t *testing.T) {
	t.Parallel()

	activeTenant := &provisioning.Tenant{
		ID:     "uuid-1234",
		Slug:   "acme-corp",
		Status: provisioning.StatusActive,
	}
	recentRename := time.Now().Add(-time.Minute)    // within any reasonable hold period
	expiredRename := time.Now().Add(-2 * time.Hour) // older than holdPeriod (1h) — grace window closed
	renamedTenant := &provisioning.Tenant{
		ID:              "uuid-1234",
		Slug:            "new-corp",
		Status:          provisioning.StatusActive,
		SlugRenameState: provisioning.SlugRenameStateComplete,
		PreviousSlug:    "acme-corp",
		SlugRenamedAt:   &recentRename,
	}
	deprovisioningTenant := &provisioning.Tenant{
		ID:              "uuid-1234",
		Slug:            "new-corp",
		Status:          provisioning.StatusDeprovisioning,
		SlugRenameState: provisioning.SlugRenameStateComplete,
		PreviousSlug:    "acme-corp",
	}
	deletedTenant := &provisioning.Tenant{
		ID:              "uuid-1234",
		Slug:            "new-corp",
		Status:          provisioning.StatusDeleted,
		SlugRenameState: provisioning.SlugRenameStateComplete,
		PreviousSlug:    "acme-corp",
	}

	tests := []struct {
		name       string
		claims     *auth.Claims
		slugParam  string
		tenant     *provisioning.Tenant
		lookupErr  error
		wantStatus int
	}{
		{
			name:       "admin role bypasses ownership check",
			claims:     &auth.Claims{TenantID: "other-tenant", Roles: []string{"admin"}},
			slugParam:  "acme-corp",
			tenant:     activeTenant,
			wantStatus: http.StatusOK,
		},
		{
			name:       "system role bypasses ownership check",
			claims:     &auth.Claims{TenantID: "other-tenant", Roles: []string{"system"}},
			slugParam:  "acme-corp",
			tenant:     activeTenant,
			wantStatus: http.StatusOK,
		},
		{
			name:       "direct slug match passes",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "acme-corp",
			tenant:     activeTenant,
			wantStatus: http.StatusOK,
		},
		{
			name:       "grace period — previous slug accepted for active renamed tenant",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "new-corp",
			tenant:     renamedTenant,
			wantStatus: http.StatusOK,
		},
		{
			name:       "grace period — rejected when tenant is deprovisioning (not active)",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "new-corp",
			tenant:     deprovisioningTenant,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "grace period — rejected when tenant is deleted (not active)",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "new-corp",
			tenant:     deletedTenant,
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "empty PreviousSlug guard — complete state without previous slug is not grace period",
			claims:    &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam: "new-corp",
			tenant: &provisioning.Tenant{
				ID: "uuid-1234", Slug: "new-corp", Status: provisioning.StatusActive,
				SlugRenameState: provisioning.SlugRenameStateComplete, PreviousSlug: "",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "mismatch — different tenant denied",
			claims:     &auth.Claims{TenantID: "other-tenant", Roles: []string{"user"}},
			slugParam:  "acme-corp",
			tenant:     activeTenant,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "tenant not found — 404",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "acme-corp",
			lookupErr:  provisioning.ErrTenantNotFound,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "store error — 503",
			claims:     &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam:  "acme-corp",
			lookupErr:  errors.New("db connection lost"),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "no claims in context — 401",
			claims:     nil,
			slugParam:  "acme-corp",
			tenant:     activeTenant,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:      "grace period — rejected when hold period has expired",
			claims:    &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam: "new-corp",
			tenant: &provisioning.Tenant{
				ID:              "uuid-1234",
				Slug:            "new-corp",
				Status:          provisioning.StatusActive,
				SlugRenameState: provisioning.SlugRenameStateComplete,
				PreviousSlug:    "acme-corp",
				SlugRenamedAt:   &expiredRename,
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "grace period — rejected when SlugRenameState is pending (rename in progress)",
			claims:    &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam: "new-corp",
			tenant: &provisioning.Tenant{
				ID:              "uuid-1234",
				Slug:            "new-corp",
				Status:          provisioning.StatusActive,
				SlugRenameState: provisioning.SlugRenameStatePending,
				PreviousSlug:    "acme-corp",
				SlugRenamedAt:   &recentRename,
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "grace period — rejected when SlugRenamedAt is nil despite complete state",
			claims:    &auth.Claims{TenantID: "acme-corp", Roles: []string{"user"}},
			slugParam: "new-corp",
			tenant: &provisioning.Tenant{
				ID:              "uuid-1234",
				Slug:            "new-corp",
				Status:          provisioning.StatusActive,
				SlugRenameState: provisioning.SlugRenameStateComplete,
				PreviousSlug:    "acme-corp",
				SlugRenamedAt:   nil,
			},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lookup := func(_ context.Context, _ string) (*provisioning.Tenant, error) {
				if tt.lookupErr != nil {
					return nil, tt.lookupErr
				}
				return tt.tenant, nil
			}

			handler := requireTenantHandler(lookup, time.Hour)

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			req = withTenantSlug(req, tt.slugParam)
			if tt.claims != nil {
				req = withClaims(req, tt.claims)
			}
			// Inject zerolog logger (required by middleware for warning logs).
			logger := zerolog.Nop()
			req = req.WithContext(logger.WithContext(req.Context()))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestRequireTenant_StashesTenantUUID verifies that RequireTenant stashes the validated
// tenant's UUID (identity-of-record) in context on both the direct-match and grace-period
// branches, and that the admin/system bypass stashes nothing (the tenant is never looked
// up). This is the load-bearing guarantee behind #166: handlers that write UUID-keyed
// records read the UUID from context instead of re-deriving it from claims.TenantID (a slug).
func TestRequireTenant_StashesTenantUUID(t *testing.T) {
	t.Parallel()

	recentRename := time.Now().Add(-time.Minute) // within the 1h hold period
	active := &provisioning.Tenant{ID: "uuid-active", Slug: "acme", Status: provisioning.StatusActive}
	renamed := &provisioning.Tenant{
		ID:              "uuid-renamed",
		Slug:            "new-corp", // current slug
		Status:          provisioning.StatusActive,
		SlugRenameState: provisioning.SlugRenameStateComplete,
		PreviousSlug:    "old-corp",
		SlugRenamedAt:   &recentRename,
	}

	lookup := func(_ context.Context, slug string) (*provisioning.Tenant, error) {
		switch slug {
		case "acme":
			return active, nil
		case "new-corp":
			return renamed, nil
		}
		return nil, provisioning.ErrTenantNotFound
	}

	tests := []struct {
		name      string
		claims    *auth.Claims
		slugParam string
		wantUUID  string
	}{
		{
			name:      "direct slug match stashes uuid",
			claims:    &auth.Claims{TenantID: "acme", Roles: []string{"user"}},
			slugParam: "acme",
			wantUUID:  "uuid-active",
		},
		{
			// Grace window: claims carry the OLD slug, but the stashed UUID is the
			// validated tenant's stable UUID (rename-independent).
			name:      "grace-period match stashes uuid",
			claims:    &auth.Claims{TenantID: "old-corp", Roles: []string{"user"}},
			slugParam: "new-corp",
			wantUUID:  "uuid-renamed",
		},
		{
			// Admin bypass returns before the lookup — nothing to stash.
			name:      "admin bypass stashes nothing",
			claims:    &auth.Claims{TenantID: "whatever", Roles: []string{"admin"}},
			slugParam: "acme",
			wantUUID:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotUUID string
			capture := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUUID = getTenantUUIDFromContext(r)
				w.WriteHeader(http.StatusOK)
			})
			handler := RequireTenant(lookup, time.Hour)(capture)

			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			req = withTenantSlug(req, tt.slugParam)
			req = withClaims(req, tt.claims)
			req = req.WithContext(zerolog.Nop().WithContext(req.Context()))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
			}
			if gotUUID != tt.wantUUID {
				t.Errorf("stashed UUID = %q, want %q", gotUUID, tt.wantUUID)
			}
		})
	}
}

// TestAdminJWTMiddleware_ActorClientIPHasNoPort pins the audit-trail IP fix (§IX):
// the actor context must carry a bare IP, never Go's host:port RemoteAddr — the
// provisioning_audit.ip_address column is INET, and a host:port value made every
// admin-action audit INSERT fail with SQLSTATE 22P02 (observed on each action in
// the compose e2e logs), silently losing the audit trail.
func TestAdminJWTMiddleware_ActorClientIPHasNoPort(t *testing.T) {
	t.Parallel()

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate admin keypair: %v", err)
	}
	const kid = "audit-ip-test-key"
	registry := provauth.NewAdminKeyRegistry()
	registry.Refresh([]*auth.KeyInfo{{KeyID: kid, Algorithm: "EdDSA", PublicKey: adminPub, IsActive: true}})
	validator := provauth.NewAdminValidator(registry)

	now := time.Now()
	claims := jwt.MapClaims{
		"iss": "sukko-admin", "sub": "test-admin", "roles": []string{"admin"},
		"exp": jwt.NewNumericDate(now.Add(5 * time.Minute)), "iat": jwt.NewNumericDate(now),
		"jti": uuid.NewString(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(adminPriv)
	if err != nil {
		t.Fatalf("sign admin JWT: %v", err)
	}

	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		wantIP     string
	}{
		{"ipv4 host:port stripped", "172.18.0.7:60328", "", "172.18.0.7"},
		{"ipv6 bracket host:port stripped", "[2001:db8::1]:443", "", "2001:db8::1"},
		{"x-forwarded-for wins behind a proxy", "10.0.0.2:1234", "203.0.113.9", "203.0.113.9"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotIP string
			next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				gotIP = auth.GetClientIPFromContext(r.Context())
			})
			handler := AdminJWTMiddleware(validator, zerolog.Nop())(next)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", http.NoBody)
			req.Header.Set("Authorization", "Bearer "+signed)
			req.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
			}
			if gotIP != tc.wantIP {
				t.Errorf("actor client IP = %q, want %q (a host:port here breaks the INET audit column)", gotIP, tc.wantIP)
			}
		})
	}
}

// TestRateLimitMiddleware_RetryAfterHeader pins §IX on the API middleware: a 429 from
// either limiter (global or per-IP) MUST carry a positive Retry-After, or every polite
// client degenerates into an immediate-retry loop against an empty bucket.
func TestRateLimitMiddleware_RetryAfterHeader(t *testing.T) {
	t.Parallel()

	// requestsPerMinute=60 → global bucket of 60, refilling 1/s. Drain the global
	// bucket; the 61st request 429s from the global limiter.
	handler := RateLimitMiddleware(60)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var last *httptest.ResponseRecorder
	for range 61 {
		last = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", http.NoBody)
		req.RemoteAddr = "203.0.113.9:1234"
		handler.ServeHTTP(last, req)
	}

	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("61st request: status = %d, want %d", last.Code, http.StatusTooManyRequests)
	}
	if got := last.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive integer (§IX)", got)
	}
}
