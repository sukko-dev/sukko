package auth

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
)

// TestSignRequest_AdminRolesClaim verifies the admin JWT carries roles:["admin"].
// Without it, provisioning's RequireRole("admin","system") returns 403 INSUFFICIENT_ROLE
// and the tester cannot perform admin operations such as creating its throwaway tenant.
func TestSignRequest_AdminRolesClaim(t *testing.T) {
	t.Parallel()

	provider, _, err := NewEphemeralAuthProvider()
	if err != nil {
		t.Fatalf("NewEphemeralAuthProvider: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://provisioning/api/v1/tenants", http.NoBody)
	provider.SignRequest(req)

	authz := req.Header.Get("Authorization")
	tokenStr, ok := strings.CutPrefix(authz, "Bearer ")
	if !ok {
		t.Fatalf("Authorization header = %q, want Bearer <jwt>", authz)
	}

	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(tokenStr, claims); err != nil {
		t.Fatalf("parse token: %v", err)
	}

	if iss, _ := claims["iss"].(string); iss != provauth.AdminJWTIssuer {
		t.Errorf("iss = %q, want %q", iss, provauth.AdminJWTIssuer)
	}

	rawRoles, ok := claims["roles"].([]any)
	if !ok {
		t.Fatalf("roles claim missing or wrong type: %#v", claims["roles"])
	}
	roles := make([]string, 0, len(rawRoles))
	for _, r := range rawRoles {
		if s, ok := r.(string); ok {
			roles = append(roles, s)
		}
	}
	if !slices.Contains(roles, "admin") {
		t.Errorf("roles = %v, want to contain \"admin\"", roles)
	}
}
