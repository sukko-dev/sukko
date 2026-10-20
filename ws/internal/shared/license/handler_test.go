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

// (An expired-at-boot license no longer produces a running server under ADR-0038
// increment B — NewManager refuses it, covered by TestNewManager_ExpiredAtStartup —
// so there is no /edition-with-expired-at-boot-manager case to test. The running +
// mid-flight-expired case is covered below.)

// TestEditionHandler_MidFlightExpiry_ReportsBootedEdition (ADR-0038): a server
// that booted with a valid Pro license and whose key expires WHILE RUNNING keeps
// serving Pro until restart (no mid-flight downgrade), so /edition must report the
// booted edition/limits — with expired=true so the operator knows to re-key — not
// a Community downgrade that lies about what the server is actually enforcing.
//
//nolint:paralleltest // shares package-level publicKey via SetPublicKeyForTesting; also sleeps across a second boundary
func TestEditionHandler_MidFlightExpiry_ReportsBootedEdition(t *testing.T) {
	priv, pub := GenerateTestKeyPair()
	SetPublicKeyForTesting(pub)

	// Valid at construction (exp one second out — unambiguously not yet expired when
	// NewManager verifies), then expired after the wait below.
	claims := Claims{Edition: Pro, Org: "MidFlight", Exp: time.Now().Unix() + 1}
	key := SignTestLicense(claims, priv)
	mgr, err := NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	// Wait safely past the expiry second so the license is expired mid-flight.
	time.Sleep(2100 * time.Millisecond)
	if c := mgr.Claims(); c == nil || !c.IsExpired() {
		t.Fatal("precondition: license should be expired mid-flight")
	}

	handler := EditionHandler(mgr, nil)
	req := httptest.NewRequest(http.MethodGet, "/edition", http.NoBody)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var resp EditionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Booted edition + limits, NOT a Community downgrade.
	if resp.Edition != "pro" {
		t.Errorf("Edition = %q, want pro (booted edition held mid-flight, no downgrade)", resp.Edition)
	}
	if resp.Limits.MaxTenants != 50 {
		t.Errorf("MaxTenants = %d, want 50 (booted Pro limits)", resp.Limits.MaxTenants)
	}
	// ...but the response still tells the operator the license has lapsed.
	if !resp.Expired {
		t.Error("Expired should be true (honest expiry flag)")
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
