package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	provtestutil "github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// testLicensePriv and testLicensePub are Ed25519 keypair for test license signing.
// Tests in this file MUST NOT use t.Parallel() since they call SetPublicKeyForTesting.
var testLicensePriv, testLicensePub = license.GenerateTestKeyPair()

func setupLicenseHandlerTest(t *testing.T) *LicenseHandler {
	t.Helper()
	license.SetPublicKeyForTesting(testLicensePub)

	logger := zerolog.Nop()
	mgr, err := license.NewManager("", logger)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	bus := eventbus.New(logger)
	repo := provtestutil.NewMockLicenseStateStore()

	return NewLicenseHandler(mgr, repo, bus, logger)
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_Success(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// setupLicenseHandlerTest creates a Community manager — use a Community key so editions match.
	key := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "TestOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: key})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleReload(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp licenseReloadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Edition != license.Community.String() {
		t.Errorf("edition = %q, want %q", resp.Edition, license.Community.String())
	}
	if resp.Org != "TestOrg" {
		t.Errorf("org = %q, want %q", resp.Org, "TestOrg")
	}
	if resp.Status != "reloaded" {
		t.Errorf("status = %q, want %q", resp.Status, "reloaded")
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_MissingKey(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	body, _ := json.Marshal(licenseReloadRequest{Key: ""})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_InvalidJSON(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader([]byte("not json")))
	rec := httptest.NewRecorder()

	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_InvalidSignature(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	body, _ := json.Marshal(licenseReloadRequest{Key: "not-a-valid-license"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_ReplayDetected(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	iat := time.Now().Unix()
	// Both keys use Community edition to match the Community manager — edition check passes.
	key1 := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "Acme",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     iat,
	}, testLicensePriv)

	// First reload — success
	body1, _ := json.Marshal(licenseReloadRequest{Key: key1})
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body1))
	rec1 := httptest.NewRecorder()
	h.HandleReload(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first reload status = %d, want %d", rec1.Code, http.StatusOK)
	}

	// Second reload with same iat — replay (edition still matches, replay fires first)
	key2 := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "Acme",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     iat, // same iat — triggers ErrReplayDetected
	}, testLicensePriv)

	body2, _ := json.Marshal(licenseReloadRequest{Key: key2})
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body2))
	rec2 := httptest.NewRecorder()
	h.HandleReload(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Errorf("replay status = %d, want %d", rec2.Code, http.StatusConflict)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_ExpiredKey(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	key := license.SignTestLicense(license.Claims{
		Edition: license.Pro,
		Org:     "Acme",
		Exp:     time.Now().Add(-24 * time.Hour).Unix(), // expired yesterday
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: key})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_RateLimited(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	body, _ := json.Marshal(licenseReloadRequest{Key: "any"})

	// Exhaust the burst (3 requests allowed)
	for range 4 {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.HandleReload(rec, req)
	}

	// Next request should be rate-limited
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	// §IX: a 429 MUST carry Retry-After — without it a polite client retries
	// immediately into a limit that has not moved.
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive integer (§IX)", got)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_SameEditionRenewal(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// Community manager + Community key — same edition, no pre-seeding needed.
	key := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "RenewOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: key})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_UpgradeRejected(t *testing.T) {
	h := setupLicenseHandlerTest(t) // Community manager

	proKey := license.SignTestLicense(license.Claims{
		Edition: license.Pro,
		Org:     "UpgradeOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: proKey})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}

	var resp editionChangeRejectedResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Code != editionChangeCode {
		t.Errorf("code = %q, want %q", resp.Code, editionChangeCode)
	}
	if resp.CurrentEdition != license.Community.String() {
		t.Errorf("current_edition = %q, want %q", resp.CurrentEdition, license.Community.String())
	}
	if resp.RequestedEdition != license.Pro.String() {
		t.Errorf("requested_edition = %q, want %q", resp.RequestedEdition, license.Pro.String())
	}

	// Manager state must be unchanged.
	if h.manager.Edition() != license.Community {
		t.Errorf("manager Edition() = %q after rejected upgrade, want Community", h.manager.Edition())
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_DowngradeRejected(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// Pre-seed manager with Enterprise so we can test downgrade to Community.
	enterpriseKey := license.SignTestLicense(license.Claims{
		Edition: license.Enterprise,
		Org:     "DowngradeOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix() - 2,
	}, testLicensePriv)
	if err := h.manager.Reload(enterpriseKey); err != nil {
		t.Fatalf("pre-seed Reload() error = %v", err)
	}

	communityKey := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "DowngradeOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: communityKey})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}

	var resp editionChangeRejectedResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Code != editionChangeCode {
		t.Errorf("code = %q, want %q", resp.Code, editionChangeCode)
	}
	if resp.CurrentEdition != license.Enterprise.String() {
		t.Errorf("current_edition = %q, want %q", resp.CurrentEdition, license.Enterprise.String())
	}
	if resp.RequestedEdition != license.Community.String() {
		t.Errorf("requested_edition = %q, want %q", resp.RequestedEdition, license.Community.String())
	}

	// Manager state must be unchanged.
	if h.manager.Edition() != license.Enterprise {
		t.Errorf("manager Edition() = %q after rejected downgrade, want Enterprise", h.manager.Edition())
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_CommunityNoKeyRejected(t *testing.T) {
	h := setupLicenseHandlerTest(t) // Community manager with no startup license key

	enterpriseKey := license.SignTestLicense(license.Claims{
		Edition: license.Enterprise,
		Org:     "EnterpriseOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: enterpriseKey})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d; body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_EditionCheckAfterSignatureCheck(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// Bad signature — must return 400, not 409 (confirming ordering: sig check before edition check).
	body, _ := json.Marshal(licenseReloadRequest{Key: "not-a-valid-license"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d (sig check must precede edition check)", rec.Code, http.StatusBadRequest)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_EditionChangeMetricIncrement(t *testing.T) {
	h := setupLicenseHandlerTest(t) // Community manager

	before := testutil.ToFloat64(licenseReloadTotal.WithLabelValues(reloadEditionChange))

	proKey := license.SignTestLicense(license.Claims{
		Edition: license.Pro,
		Org:     "MetricOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: proKey})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}

	after := testutil.ToFloat64(licenseReloadTotal.WithLabelValues(reloadEditionChange))
	if after != before+1 {
		t.Errorf("licenseReloadTotal{result=%q} = %v, want %v", reloadEditionChange, after, before+1)
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_EditionChangeAuditLog(t *testing.T) {
	license.SetPublicKeyForTesting(testLicensePub)

	var logBuf bytes.Buffer
	auditLogger := zerolog.New(&logBuf)

	mgr, err := license.NewManager("", auditLogger)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	bus := eventbus.New(auditLogger)
	repo := provtestutil.NewMockLicenseStateStore()
	h := NewLicenseHandler(mgr, repo, bus, auditLogger)

	proKey := license.SignTestLicense(license.Claims{
		Edition: license.Pro,
		Org:     "AuditOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix(),
	}, testLicensePriv)

	body, _ := json.Marshal(licenseReloadRequest{Key: proKey})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}

	logOutput := logBuf.String()
	for _, want := range []string{`"audit":true`, `"current_edition"`, `"requested_edition"`} {
		if !bytes.Contains(logBuf.Bytes(), []byte(want)) {
			t.Errorf("audit log missing field %q; log = %s", want, logOutput)
		}
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestCurrentKey_SetAndGet(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	if got := h.CurrentKey(); got != "" {
		t.Errorf("initial CurrentKey() = %q, want empty", got)
	}

	h.SetCurrentKey("test-key-123")
	if got := h.CurrentKey(); got != "test-key-123" {
		t.Errorf("CurrentKey() = %q, want %q", got, "test-key-123")
	}
}
