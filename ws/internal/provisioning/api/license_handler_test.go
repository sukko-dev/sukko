package api

import (
	"bytes"
	"context"
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

// testLicensePriv and testLicensePub are an ECDSA P-256 keypair for test license signing.
// Tests in this file MUST NOT use t.Parallel() since they call SetPublicKeyForTesting.
var testLicensePriv, testLicensePub = license.GenerateTestKeyPair()

func setupLicenseHandlerTest(t *testing.T) *LicenseHandler {
	t.Helper()
	license.SetPublicKeyForTesting(testLicensePub)

	logger := zerolog.Nop()
	mgr := license.NewTestManager(license.Community)

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
	// Same-edition renewal applies live → status "active" (ADR-0038).
	if resp.Status != reloadStatusActive {
		t.Errorf("status = %q, want %q", resp.Status, reloadStatusActive)
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

	// Second reload — same iat but DIFFERENT claims (a forged/stale key). This is a
	// genuine replay: the same iat does not make it the current key, so it is rejected
	// with 409. (A re-push of the IDENTICAL key is a no-op — see the Unchanged test.)
	key2 := license.SignTestLicense(license.Claims{
		Edition: license.Community,
		Org:     "Impostor",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     iat, // same iat, different org — triggers ErrReplayDetected
	}, testLicensePriv)

	body2, _ := json.Marshal(licenseReloadRequest{Key: key2})
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body2))
	rec2 := httptest.NewRecorder()
	h.HandleReload(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Errorf("replay status = %d, want %d", rec2.Code, http.StatusConflict)
	}
}

// reloadKey POSTs key to HandleReload and asserts the status code, returning the
// recorder so the caller can inspect the body.
func reloadKey(t *testing.T, h *LicenseHandler, key string, wantCode int) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(licenseReloadRequest{Key: key})
	rec := httptest.NewRecorder()
	h.HandleReload(rec, httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body)))
	if rec.Code != wantCode {
		t.Fatalf("reload status = %d, want %d; body=%s", rec.Code, wantCode, rec.Body.String())
	}
	return rec
}

// TestHandleReload_IdenticalKeyUnchanged pins the idempotent no-op at the handler: a
// re-push of the current key (same iat AND same claims — what `sukko up`'s push-service
// reconcile does) returns 200 status "unchanged", not a 409 replay, AND does NOT persist
// a second time or broadcast (the load-bearing ADR-0041 claim).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_IdenticalKeyUnchanged(t *testing.T) {
	license.SetPublicKeyForTesting(testLicensePub)
	repo := provtestutil.NewMockLicenseStateStore()
	h := NewLicenseHandler(license.NewTestManager(license.Community), repo, eventbus.New(zerolog.Nop()), zerolog.Nop())

	key := license.SignTestLicense(license.Claims{
		Edition: license.Community, Org: "Acme",
		Exp: time.Now().Add(365 * 24 * time.Hour).Unix(), Iat: time.Now().Unix(),
	}, testLicensePriv)

	reloadKey(t, h, key, http.StatusOK) // first — applied + persisted once
	if n := repo.UpsertCallCount(); n != 1 {
		t.Fatalf("after first reload Upsert calls = %d, want 1", n)
	}

	rec := reloadKey(t, h, key, http.StatusOK) // re-push the identical key
	var resp licenseReloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != reloadStatusUnchanged {
		t.Errorf("status = %q, want %q", resp.Status, reloadStatusUnchanged)
	}
	if n := repo.UpsertCallCount(); n != 1 {
		t.Errorf("after unchanged re-push Upsert calls = %d, want 1 (a no-op must not persist)", n)
	}
}

// TestHandleReload_DeferredDowngradeRepush pins the deferral-window case: re-pushing a
// still-DEFERRED downgrade key (its iat equals the persisted anchor but its claims differ
// from the live key) is a 200 "unchanged" no-op, not a 409 replay. Without the
// CurrentKey() byte-equal check it would wrongly 409 "iat not newer than current".
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_DeferredDowngradeRepush(t *testing.T) {
	license.SetPublicKeyForTesting(testLicensePub)
	h := NewLicenseHandler(license.NewTestManager(license.Community), provtestutil.NewMockLicenseStateStore(), eventbus.New(zerolog.Nop()), zerolog.Nop())
	exp := time.Now().Add(365 * 24 * time.Hour).Unix()

	// Boot the live edition up to Pro.
	pro := license.SignTestLicense(license.Claims{Edition: license.Pro, Org: "Acme", Exp: exp, Iat: 10}, testLicensePriv)
	reloadKey(t, h, pro, http.StatusOK)

	// A valid downgrade (newer iat) — accepted but deferred to restart (pending_restart).
	downgrade := license.SignTestLicense(license.Claims{Edition: license.Community, Org: "Acme", Exp: exp, Iat: 20}, testLicensePriv)
	rec1 := reloadKey(t, h, downgrade, http.StatusOK)
	var r1 licenseReloadResponse
	_ = json.Unmarshal(rec1.Body.Bytes(), &r1)
	if r1.Status != reloadStatusPendingRestart {
		t.Fatalf("downgrade status = %q, want %q", r1.Status, reloadStatusPendingRestart)
	}

	// Re-push the SAME deferred key — must be 200 unchanged, not a 409 replay.
	rec2 := reloadKey(t, h, downgrade, http.StatusOK)
	var r2 licenseReloadResponse
	_ = json.Unmarshal(rec2.Body.Bytes(), &r2)
	if r2.Status != reloadStatusUnchanged {
		t.Errorf("deferred re-push status = %q, want %q (not a replay)", r2.Status, reloadStatusUnchanged)
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

// TestHandleReload_UpgradeAppliedLive: a reload that RAISES the edition is applied
// live (ADR-0038 Model C) — 200 active, manager upgrades, key persisted + broadcast.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_UpgradeAppliedLive(t *testing.T) {
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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp licenseReloadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != reloadStatusActive {
		t.Errorf("status = %q, want %q", resp.Status, reloadStatusActive)
	}
	if resp.Edition != license.Pro.String() {
		t.Errorf("edition = %q, want %q", resp.Edition, license.Pro.String())
	}

	// Upgrade takes effect live, and the key is persisted + broadcast.
	if h.manager.Edition() != license.Pro {
		t.Errorf("manager Edition() = %q after upgrade, want Pro", h.manager.Edition())
	}
	if h.CurrentKey() != proKey {
		t.Error("CurrentKey not updated — upgrade must persist + broadcast the new key")
	}
}

// TestHandleReload_DowngradeDeferred: a reload that LOWERS the edition is accepted
// (200 pending_restart), persisted + broadcast, but does NOT change the running
// edition — it takes effect at the next restart (ADR-0038 Model C).
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_DowngradeDeferred(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// Pre-seed manager with Enterprise so we can push a Community downgrade.
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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp licenseReloadResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != reloadStatusPendingRestart {
		t.Errorf("status = %q, want %q", resp.Status, reloadStatusPendingRestart)
	}
	if resp.Edition != license.Community.String() {
		t.Errorf("edition = %q, want %q (the accepted/pending key)", resp.Edition, license.Community.String())
	}

	// The running edition is unchanged — downgrade deferred to restart...
	if h.manager.Edition() != license.Enterprise {
		t.Errorf("manager Edition() = %q after downgrade, want Enterprise (deferred to restart)", h.manager.Edition())
	}
	// ...but the key is still persisted + broadcast so the next restart boots it.
	if h.CurrentKey() != communityKey {
		t.Error("CurrentKey not updated — a deferred downgrade must still persist + broadcast the key")
	}
}

// TestHandleReload_DeferredDowngrade_ThenOlderKeyRejected is the replay regression guard
// for ADR-0038's deferral: once a downgrade is accepted+deferred (advancing the persisted
// key without advancing the live manager), a subsequently pushed key whose iat is older
// than the deferred one — but newer than the live manager's — must still be rejected as
// replay, or it would overwrite the newer persisted key. Anchoring replay on the live
// manager alone would wrongly accept it.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_DeferredDowngrade_ThenOlderKeyRejected(t *testing.T) {
	license.SetPublicKeyForTesting(testLicensePub)
	mgr := license.NewTestManager(license.Community)
	repo := provtestutil.NewMockLicenseStateStore()
	h := NewLicenseHandler(mgr, repo, eventbus.New(zerolog.Nop()), zerolog.Nop())

	future := time.Now().Add(365 * 24 * time.Hour).Unix()
	// Boot the live manager to Enterprise@10.
	ent := license.SignTestLicense(license.Claims{Edition: license.Enterprise, Org: "Org", Exp: future, Iat: 10}, testLicensePriv)
	if err := h.manager.Reload(ent); err != nil {
		t.Fatalf("seed Reload: %v", err)
	}

	post := func(key string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(licenseReloadRequest{Key: key})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.HandleReload(rec, req)
		return rec
	}

	// Push Pro@20 — a downgrade vs live Enterprise → accepted + deferred (200), persisted.
	proDown := license.SignTestLicense(license.Claims{Edition: license.Pro, Org: "Org", Exp: future, Iat: 20}, testLicensePriv)
	if rec := post(proDown); rec.Code != http.StatusOK {
		t.Fatalf("deferred downgrade status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// Push Community@15 — OLDER than the deferred Pro@20, newer than live Enterprise@10.
	// Must be rejected as replay (409).
	commOld := license.SignTestLicense(license.Claims{Edition: license.Community, Org: "Org", Exp: future, Iat: 15}, testLicensePriv)
	if rec := post(commOld); rec.Code != http.StatusConflict {
		t.Fatalf("older key after deferral status = %d, want 409 (replay); body=%s", rec.Code, rec.Body.String())
	}

	// The persisted key must still be the newer Pro@20, not overwritten by the older key.
	got, _ := repo.Load(context.Background())
	if got != proDown {
		t.Error("persisted key was overwritten by an older key after a deferred downgrade")
	}
}

//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_InvalidSignatureBeforeApply(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	// Bad signature — must return 400: signature verification precedes any apply.
	body, _ := json.Marshal(licenseReloadRequest{Key: "not-a-valid-license"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/license", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d (sig check must precede apply)", rec.Code, http.StatusBadRequest)
	}
}

// TestHandleReload_DowngradeDeferredMetricIncrement: a deferred downgrade increments
// the reloadDowngradeDeferred result counter (not reloadSuccess), at 200 OK.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_DowngradeDeferredMetricIncrement(t *testing.T) {
	h := setupLicenseHandlerTest(t)

	entKey := license.SignTestLicense(license.Claims{
		Edition: license.Enterprise,
		Org:     "MetricOrg",
		Exp:     time.Now().Add(365 * 24 * time.Hour).Unix(),
		Iat:     time.Now().Unix() - 2,
	}, testLicensePriv)
	if err := h.manager.Reload(entKey); err != nil {
		t.Fatalf("pre-seed Reload() error = %v", err)
	}

	before := testutil.ToFloat64(licenseReloadTotal.WithLabelValues(reloadDowngradeDeferred))

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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	after := testutil.ToFloat64(licenseReloadTotal.WithLabelValues(reloadDowngradeDeferred))
	if after != before+1 {
		t.Errorf("licenseReloadTotal{result=%q} = %v, want %v", reloadDowngradeDeferred, after, before+1)
	}
}

// TestHandleReload_AcceptedAuditLog: an accepted reload emits an audit log carrying
// the admin actor and the accepted/live edition + status fields.
//
//nolint:paralleltest // SetPublicKeyForTesting mutates package-level state
func TestHandleReload_AcceptedAuditLog(t *testing.T) {
	license.SetPublicKeyForTesting(testLicensePub)

	var logBuf bytes.Buffer
	auditLogger := zerolog.New(&logBuf)

	mgr := license.NewTestManager(license.Community)
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

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	logOutput := logBuf.String()
	for _, want := range []string{`"audit":true`, `"accepted_edition"`, `"live_edition"`, `"status"`} {
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
