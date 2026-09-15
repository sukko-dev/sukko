package license

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestEditionHandler_NilManager(t *testing.T) {
	t.Parallel()
	handler := EditionHandler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Edition != "community" {
		t.Errorf("Edition = %q, want community", resp.Edition)
	}
	if resp.Limits.MaxTenants != 3 {
		t.Errorf("MaxTenants = %d, want 3", resp.Limits.MaxTenants)
	}
	if resp.Usage != nil {
		t.Error("Usage should be nil for nil manager")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestEditionHandler_ValidPro(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "Acme", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	handler := EditionHandler(mgr, nil)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Edition != "pro" {
		t.Errorf("Edition = %q, want pro", resp.Edition)
	}
	if resp.Org != "Acme" {
		t.Errorf("Org = %q, want Acme", resp.Org)
	}
	if resp.ExpiresAt == nil {
		t.Fatal("ExpiresAt should be set")
	}
	if resp.Expired {
		t.Error("Expired should be false")
	}
	if resp.Limits.MaxTenants != 50 {
		t.Errorf("MaxTenants = %d, want 50 (Pro)", resp.Limits.MaxTenants)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestEditionHandler_WithUsage(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "Test", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	tenants := 5
	usageFn := func(_ context.Context) *EditionUsage {
		return &EditionUsage{Tenants: &tenants}
	}

	handler := EditionHandler(mgr, usageFn)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Usage == nil {
		t.Fatal("Usage should be set")
	}
	if resp.Usage.Tenants == nil || *resp.Usage.Tenants != 5 {
		t.Errorf("Usage.Tenants = %v, want 5", resp.Usage.Tenants)
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestEditionHandler_ExpiredLicense(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "ExpiredCo", Exp: time.Now().Add(-time.Hour).Unix()}
	key := SignTestLicense(claims, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	handler := EditionHandler(mgr, nil)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Edition != "community" {
		t.Errorf("Edition = %q, want community (degraded)", resp.Edition)
	}
	if !resp.Expired {
		t.Error("Expired should be true")
	}
	if resp.Org != "ExpiredCo" {
		t.Errorf("Org = %q, want ExpiredCo", resp.Org)
	}
	if resp.Limits.MaxTenants != 3 {
		t.Errorf("MaxTenants = %d, want 3 (Community degraded)", resp.Limits.MaxTenants)
	}
}

func TestEditionHandler_NilUsageFunc(t *testing.T) {
	t.Parallel()
	handler := EditionHandler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Usage != nil {
		t.Error("Usage should be nil when usageFn is nil")
	}
}

//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting
func TestEditionHandler_UsageFuncReturnsNil(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	claims := Claims{Edition: Pro, Org: "Test", Exp: time.Now().Add(time.Hour).Unix()}
	key := SignTestLicense(claims, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}

	usageFn := func(_ context.Context) *EditionUsage {
		return nil // usage unavailable
	}

	handler := EditionHandler(mgr, usageFn)

	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Edition != "pro" {
		t.Errorf("Edition = %q, want pro", resp.Edition)
	}
	if resp.Usage != nil {
		t.Error("Usage should be nil when usageFn returns nil")
	}
}
