package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// fakeTenantResolver maps tenant slugs to UUIDs for binding tests. An unknown
// slug returns ErrTenantNotResolvable (fail closed).
type fakeTenantResolver map[string]string

func (f fakeTenantResolver) ResolveTenantUUID(_ context.Context, slug string) (string, error) {
	if uuid, ok := f[slug]; ok {
		return uuid, nil
	}
	return "", ErrTenantNotResolvable
}

// identityTenantResolver resolves a slug to itself. Used by legacy tests whose
// key TenantID equals the token tenant_id slug, so the binding is a pass-through.
type identityTenantResolver struct{}

func (identityTenantResolver) ResolveTenantUUID(_ context.Context, slug string) (string, error) {
	if slug == "" {
		return "", ErrTenantNotResolvable
	}
	return slug, nil
}

// TestMultiTenantValidator_ValidateToken_KeyTenantBinding is the regression test
// for the cross-tenant JWT vulnerability (#158): a token signed by tenant A's key
// but claiming tenant B must be rejected. The binding compares the tenant UUID the
// claim resolves to against the signing key's owning tenant UUID.
//
// It fails without the binding block in ValidateJWT (the mismatch row would be
// accepted). Fixtures use the production scheme: claim = slug, key.TenantID = UUID,
// resolver maps slug -> UUID.
func TestMultiTenantValidator_ValidateToken_KeyTenantBinding(t *testing.T) {
	t.Parallel()

	const (
		uuidA = "11111111-1111-1111-1111-111111111111"
		uuidB = "22222222-2222-2222-2222-222222222222"
	)
	// Resolver: slug -> stable UUID (rename-agnostic identity for the test).
	resolver := fakeTenantResolver{"tenant-a": uuidA, "tenant-b": uuidB}

	ecPEM, privateKey := generateTestECKey(t)

	// keyA is owned by tenant A (UUID). keyNoTenant has no owning tenant.
	newRegistry := func(t *testing.T, keyTenantUUID string) (*StaticKeyRegistry, *KeyInfo) {
		t.Helper()
		reg := NewStaticKeyRegistry()
		key := &KeyInfo{
			KeyID:        "key-A",
			TenantID:     keyTenantUUID,
			Algorithm:    "ES256",
			PublicKeyPEM: ecPEM,
			IsActive:     true,
		}
		if err := reg.AddKey(key); err != nil {
			t.Fatalf("AddKey: %v", err)
		}
		return reg, key
	}

	mintToken := func(t *testing.T, key *KeyInfo, tenantClaim string) string {
		t.Helper()
		return createTestToken(t, key, privateKey, &Claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   "user-1",
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
			TenantID: tenantClaim,
		})
	}

	tests := []struct {
		name             string
		keyTenantUUID    string // owning tenant UUID of the signing key
		tenantClaim      string // tenant_id (slug) in the token
		wantErr          bool
		wantMismatch     bool // expect errors.Is(err, ErrTenantMismatch)
		wantUnresolvable bool // expect errors.Is(err, ErrTenantNotResolvable)
	}{
		{name: "match: claim resolves to key's tenant", keyTenantUUID: uuidA, tenantClaim: "tenant-a", wantErr: false},
		{name: "mismatch: claim resolves to a different tenant", keyTenantUUID: uuidA, tenantClaim: "tenant-b", wantErr: true, wantMismatch: true},
		{name: "unresolvable claim", keyTenantUUID: uuidA, tenantClaim: "ghost", wantErr: true, wantUnresolvable: true},
		{name: "empty owning tenant on key", keyTenantUUID: "", tenantClaim: "tenant-a", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg, key := newRegistry(t, tc.keyTenantUUID)
			validator, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
				KeyRegistry:     reg,
				RequireTenantID: true,
				TenantResolver:  resolver,
			})
			if err != nil {
				t.Fatalf("NewMultiTenantValidator: %v", err)
			}

			claims, err := validator.ValidateToken(context.Background(), mintToken(t, key, tc.tenantClaim))
			if tc.wantErr && err == nil {
				t.Fatalf("expected rejection, got nil error (cross-tenant token accepted)")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected acceptance, got error: %v", err)
			}
			if tc.wantMismatch && !errors.Is(err, ErrTenantMismatch) {
				t.Fatalf("expected ErrTenantMismatch, got: %v", err)
			}
			if tc.wantUnresolvable && !errors.Is(err, ErrTenantNotResolvable) {
				t.Fatalf("expected ErrTenantNotResolvable, got: %v", err)
			}
			// On the accepted (match) path, the resolved UUID must be recorded.
			if !tc.wantErr && claims.ResolvedTenantUUID != tc.keyTenantUUID {
				t.Fatalf("ResolvedTenantUUID = %q, want %q", claims.ResolvedTenantUUID, tc.keyTenantUUID)
			}
		})
	}
}

// emptyStringResolver returns ("", nil) — a resolver that neither errors nor
// resolves. The binding must still reject (fail closed), not treat "" as a match.
type emptyStringResolver struct{}

func (emptyStringResolver) ResolveTenantUUID(_ context.Context, _ string) (string, error) {
	return "", nil
}

// TestMultiTenantValidator_NilResolver_ConstructionGuard asserts the constructor
// rejects a nil resolver (fail loud at startup rather than fail open at runtime).
func TestMultiTenantValidator_NilResolver_ConstructionGuard(t *testing.T) {
	t.Parallel()
	_, err := NewMultiTenantValidator(MultiTenantValidatorConfig{
		KeyRegistry:    NewStaticKeyRegistry(),
		TenantResolver: nil,
	})
	if err == nil {
		t.Fatal("expected NewMultiTenantValidator to reject a nil TenantResolver")
	}
}

// TestValidateJWT_EmptyResolution_Rejected covers the ("", nil) branch: a resolver
// that returns an empty UUID with no error must still be rejected.
func TestValidateJWT_EmptyResolution_Rejected(t *testing.T) {
	t.Parallel()
	ecPEM, privateKey := generateTestECKey(t)
	reg := NewStaticKeyRegistry()
	key := &KeyInfo{KeyID: "key-A", TenantID: "uuid-A", Algorithm: "ES256", PublicKeyPEM: ecPEM, IsActive: true}
	if err := reg.AddKey(key); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	token := createTestToken(t, key, privateKey, &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "tenant-a",
	})
	if _, err := ValidateJWT(context.Background(), token, ValidateOpts{KeyResolver: reg, TenantResolver: emptyStringResolver{}}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch for empty resolution, got: %v", err)
	}
}

// TestValidateJWT_TenantBinding_FailsClosedWithoutResolver verifies that binding
// enabled with a nil resolver rejects (never fails open), exercised via ValidateJWT
// directly since NewMultiTenantValidator refuses a nil resolver at construction.
func TestValidateJWT_TenantBinding_FailsClosedWithoutResolver(t *testing.T) {
	t.Parallel()
	ecPEM, privateKey := generateTestECKey(t)
	reg := NewStaticKeyRegistry()
	key := &KeyInfo{KeyID: "key-A", TenantID: "uuid-A", Algorithm: "ES256", PublicKeyPEM: ecPEM, IsActive: true}
	if err := reg.AddKey(key); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	token := createTestToken(t, key, privateKey, &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		TenantID: "tenant-a",
	})

	// Binding enabled (DisableTenantBinding false), resolver nil -> must reject.
	if _, err := ValidateJWT(context.Background(), token, ValidateOpts{KeyResolver: reg}); err == nil {
		t.Fatal("expected rejection with binding enabled and nil resolver, got nil")
	}

	// Binding disabled (admin path) -> accepted despite no resolver.
	if _, err := ValidateJWT(context.Background(), token, ValidateOpts{KeyResolver: reg, DisableTenantBinding: true}); err != nil {
		t.Fatalf("binding disabled should accept, got: %v", err)
	}
}
