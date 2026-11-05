package api_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// signAdminJWT mints a real operator admin JWT (iss:sukko-admin) signed by priv —
// the only credential that confers operator authority on the provisioning control
// plane. Mirrors the CLI/tester admin signer (cmd/tester/auth/auth_provider.go).
func signAdminJWT(t *testing.T, priv ed25519.PrivateKey, kid string, roles ...string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   provauth.AdminJWTIssuer,
		"sub":   "test-operator",
		"roles": roles,
		"exp":   jwt.NewNumericDate(now.Add(5 * time.Minute)),
		"iat":   jwt.NewNumericDate(now),
		"jti":   uuid.NewString(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign admin JWT: %v", err)
	}
	return signed
}

// TestRouter_SelfAssertedTenantRole_CannotEscalate is the regression guard for the
// provisioning control-plane authorization hole (§IX): admin/system authority derives
// from the sukko-admin issuer — i.e. an operator holding a registered admin key — NOT
// from the self-asserted `roles` JWT claim. A tenant signs its own JWTs, so before this
// fix a tenant minting roles:["admin"] passed every RequireRole gate AND RequireTenant's
// ownership-skip, yielding cross-tenant platform admin. ADR-0033: the provisioning API is
// an operator control plane; tenants self-manage nothing through it.
func TestRouter_SelfAssertedTenantRole_CannotEscalate(t *testing.T) {
	t.Parallel()

	// Tenant validator + signing key for "test-tenant" (the attacker's own tenant).
	tenantValidator, tenantKey := newTestValidator(t)

	// Operator (admin-JWT) validator + registered key — the legitimate control-plane credential.
	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate admin keypair: %v", err)
	}
	const adminKID = "operator-key-1"
	registry := provauth.NewAdminKeyRegistry()
	registry.Refresh([]*auth.KeyInfo{{KeyID: adminKID, Algorithm: "EdDSA", PublicKey: adminPub, IsActive: true}})

	tenantStore := testutil.NewMockTenantStore()
	routingStore := testutil.NewMockRoutingRulesStore()
	svc, _, _ := newTestServiceWithKafka(tenantStore, routingStore)
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("test-tenant"))
	_ = tenantStore.Create(context.Background(), testutil.NewTestTenant("other-tenant"))

	router := mustNewRouter(t, api.RouterConfig{
		Service:          svc,
		Logger:           zerolog.Nop(),
		Validator:        tenantValidator,
		AdminValidator:   provauth.NewAdminValidator(registry),
		AdminKeyRegistry: registry,
		EditionManager:   license.NewTestManager(license.Enterprise),
	})

	// The attack: a tenant-signed token self-asserting admin+system roles.
	tenantAdmin := createTestToken(t, tenantKey, &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "attacker",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "test-tenant",
		Roles:    []string{"admin", "system"},
	})
	// A legitimate operator token (positive control — must keep working).
	operator := signAdminJWT(t, adminPriv, adminKID, "admin")

	// Each call yields a distinct rule (unique pattern + priority) so the tenant-rejected and
	// operator-allowed subtests never collide on the shared store — the positive control stays
	// independent of the negative even under a regressed gate.
	var ruleSeq int
	routingRuleBody := func() io.Reader {
		ruleSeq++
		b, _ := json.Marshal(provisioning.AddRoutingRuleRequest{
			Rule: provisioning.TopicRoutingRule{Pattern: fmt.Sprintf("**.trade%d", ruleSeq), IngressTopic: "trade", Priority: ruleSeq},
		})
		return bytes.NewReader(b)
	}

	cases := []struct {
		name         string
		method       string
		path         string
		body         func() io.Reader
		wantTenant   int // tenant-admin token: must be rejected after the fix
		wantOperator int // operator token: must still succeed
	}{
		{
			name:         "global RequireRole: list all tenants",
			method:       http.MethodGet,
			path:         "/api/v1/tenants",
			wantTenant:   http.StatusForbidden,
			wantOperator: http.StatusOK,
		},
		{
			name:         "tenant-scoped RequireRole: write own routing rules",
			method:       http.MethodPost,
			path:         "/api/v1/tenants/test-tenant/routing-rules",
			body:         routingRuleBody,
			wantTenant:   http.StatusForbidden,
			wantOperator: http.StatusCreated,
		},
		{
			name:         "RequireTenant ownership-skip: read another tenant",
			method:       http.MethodGet,
			path:         "/api/v1/tenants/other-tenant",
			wantTenant:   http.StatusForbidden,
			wantOperator: http.StatusOK,
		},
	}

	// Assertions run sequentially (not as parallel subtests): every case shares one router and one
	// mutable routing store, so a parallel fan-out would race on it (§VIII).
	send := func(method, path string, bodyFn func() io.Reader, bearer string) (int, string) {
		var body io.Reader
		if bodyFn != nil {
			body = bodyFn()
		}
		req := httptest.NewRequestWithContext(context.Background(), method, path, body)
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	for _, tc := range cases {
		if code, body := send(tc.method, tc.path, tc.body, tenantAdmin); code != tc.wantTenant {
			t.Errorf("%s — tenant self-asserted admin: status = %d, want %d (a self-asserted role must not confer operator authority); body: %s",
				tc.name, code, tc.wantTenant, body)
		}
		if code, body := send(tc.method, tc.path, tc.body, operator); code != tc.wantOperator {
			t.Errorf("%s — operator: status = %d, want %d (the fix must not break legitimate operators); body: %s",
				tc.name, code, tc.wantOperator, body)
		}
	}
}

// TestAuthMiddleware_SetsTenantActorForAudit pins the audit-actor truthfulness fix (§IX):
// AuthMiddleware must record the authenticated tenant user as the audit actor. Before the
// fix it never called auth.WithActor, so GetActorType fell through to the "system" default
// and GetActor to "system" — any audited action under a tenant JWT was attributed to the
// platform, not the tenant user who performed it. (The route-level RequireRole gates from
// roadmap #23 now keep tenant tokens out of the audited management handlers; this is the
// defense-in-depth backstop so attribution is truthful wherever a tenant token is audited.)
func TestAuthMiddleware_SetsTenantActorForAudit(t *testing.T) {
	t.Parallel()

	validator, tenantKey := newTestValidator(t)
	token := createTestToken(t, tenantKey, &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-123",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "test-tenant",
		Roles:    []string{"user"},
	})

	var gotActor, gotType, gotIP string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotActor = auth.GetActor(r.Context())
		gotType = auth.GetActorType(r.Context())
		gotIP = auth.GetClientIPFromContext(r.Context())
	})
	handler := api.AuthMiddleware(validator, zerolog.Nop())(next)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/tenants/test-tenant", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	req.RemoteAddr = "172.18.0.9:5555"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if gotType != provisioning.ActorTypeUser {
		t.Errorf("actor type = %q, want %q (a tenant JWT action must audit as the tenant user, not the \"system\" default — §IX)", gotType, provisioning.ActorTypeUser)
	}
	if want := "test-tenant:user-123"; gotActor != want {
		t.Errorf("actor = %q, want %q", gotActor, want)
	}
	if want := "172.18.0.9"; gotIP != want {
		t.Errorf("actor client IP = %q, want %q (bare IP for the INET audit column)", gotIP, want)
	}
}
