package auth

import (
	"context"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestWithClaims_GetClaims(t *testing.T) {
	t.Parallel()
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user123",
		},
		TenantID: "acme",
		Roles:    []string{"admin"},
	}

	ctx := context.Background()

	// Initially no claims
	if got := GetClaims(ctx); got != nil {
		t.Errorf("GetClaims(empty context) = %v, want nil", got)
	}

	// Add claims
	ctx = WithClaims(ctx, claims)

	// Retrieve claims
	got := GetClaims(ctx)
	if got == nil {
		t.Fatal("GetClaims() returned nil, want claims")
	}
	if got.Subject != claims.Subject {
		t.Errorf("GetClaims().Subject = %q, want %q", got.Subject, claims.Subject)
	}
	if got.TenantID != claims.TenantID {
		t.Errorf("GetClaims().TenantID = %q, want %q", got.TenantID, claims.TenantID)
	}
}

func TestWithActor_GetActor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Default actor is DefaultActor
	if got := GetActor(ctx); got != DefaultActor {
		t.Errorf("GetActor(empty context) = %q, want %q", got, DefaultActor)
	}

	// Add actor
	ctx = WithActor(ctx, "acme:user123", "user", "192.168.1.100")

	// Retrieve actor
	if got := GetActor(ctx); got != "acme:user123" {
		t.Errorf("GetActor() = %q, want %q", got, "acme:user123")
	}
}

func TestGetActorType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Default actor type is DefaultActorType
	if got := GetActorType(ctx); got != DefaultActorType {
		t.Errorf("GetActorType(empty context) = %q, want %q", got, DefaultActorType)
	}

	// Add actor with type
	ctx = WithActor(ctx, "acme:user123", "user", "192.168.1.100")

	// Retrieve actor type
	if got := GetActorType(ctx); got != "user" {
		t.Errorf("GetActorType() = %q, want %q", got, "user")
	}
}

func TestGetClientIPFromContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Default IP is empty
	if got := GetClientIPFromContext(ctx); got != "" {
		t.Errorf("GetClientIPFromContext(empty context) = %q, want %q", got, "")
	}

	// Add actor with IP
	ctx = WithActor(ctx, "acme:user123", "user", "192.168.1.100")

	// Retrieve IP
	if got := GetClientIPFromContext(ctx); got != "192.168.1.100" {
		t.Errorf("GetClientIPFromContext() = %q, want %q", got, "192.168.1.100")
	}
}

func TestContextChaining(t *testing.T) {
	t.Parallel()
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user123",
		},
		TenantID: "acme",
	}

	ctx := context.Background()
	ctx = WithClaims(ctx, claims)
	ctx = WithActor(ctx, "acme:user123", "api_key", "10.0.0.1")

	// Both should work
	if got := GetClaims(ctx); got == nil || got.Subject != "user123" {
		t.Error("Claims not preserved after WithActor")
	}
	if got := GetActor(ctx); got != "acme:user123" {
		t.Error("Actor not set correctly after WithClaims")
	}
	if got := GetActorType(ctx); got != "api_key" {
		t.Error("ActorType not set correctly")
	}
	if got := GetClientIPFromContext(ctx); got != "10.0.0.1" {
		t.Error("ClientIP not set correctly")
	}
}

func TestGetClaims_NilValue(t *testing.T) {
	t.Parallel()
	// Test that context with nil claims value returns nil
	ctx := WithClaims(context.Background(), nil)
	if got := GetClaims(ctx); got != nil {
		t.Errorf("GetClaims(nil claims) = %v, want nil", got)
	}
}
