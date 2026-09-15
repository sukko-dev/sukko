package runner

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// upgradeMockOptions injects provisioning failures for the upgrade self-provision tests.
type upgradeMockOptions struct {
	failCreateTenant bool // 500 on POST /tenants (auth setup failure)
	failCreateAPIKey bool // 500 on POST .../api-keys
	badKeyBody       bool // 201 but unparseable api-key body
	failTeardown     bool // 500 on every DELETE — best-effort
}

// upgradeMockProv records the provisioning calls of an upgrade validate run. Upgrade mode
// provisions THREE resources: the throwaway tenant + JWT keypair (both via auth.Setup) and
// the run-scoped API key (the new branch). Teardown reverses in LIFO order.
type upgradeMockProv struct {
	createTenant  atomic.Int32
	registerJWT   atomic.Int32 // POST .../keys  (auth.Setup keypair)
	createAPIKey  atomic.Int32 // POST .../api-keys (self-provision)
	revokeAPIKey  atomic.Int32 // DELETE .../api-keys/{id}
	revokeJWT     atomic.Int32 // DELETE .../keys/{id}
	deleteTenant  atomic.Int32 // DELETE .../tenants/{id}
	mu            sync.Mutex
	teardownOrder []string // observed delete order (three-resource LIFO)
}

func (m *upgradeMockProv) record(kind string) {
	m.mu.Lock()
	m.teardownOrder = append(m.teardownOrder, kind)
	m.mu.Unlock()
}

func (m *upgradeMockProv) deletes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.teardownOrder...)
}

// newUpgradeMock builds a mock provisioning server. The DELETE matchers MUST be ordered
// `/api-keys/` → `/keys/` → `/tenants/` (the api-key and keypair paths both sit under
// `/tenants/`). GatewayURL points here too, so the suite's WS connect fails — but these tests
// assert the provisioning LIFECYCLE, which is unaffected.
func newUpgradeMock(t *testing.T, opts upgradeMockOptions) (*httptest.Server, *upgradeMockProv) {
	t.Helper()
	m := &upgradeMockProv{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/api-keys"):
			m.createAPIKey.Add(1)
			if opts.failCreateAPIKey {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			if opts.badKeyBody {
				_, _ = w.Write([]byte(`{"unexpected":true}`))
				return
			}
			_, _ = w.Write([]byte(`{"key_id":"upgrade-run-key-1"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/keys"):
			m.registerJWT.Add(1)
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && path == "/api/v1/tenants":
			m.createTenant.Add(1)
			if opts.failCreateTenant {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && strings.Contains(path, "/api-keys/"):
			m.revokeAPIKey.Add(1)
			m.record("revoke-apikey")
			w.WriteHeader(teardownStatus(opts.failTeardown))
		case r.Method == http.MethodDelete && strings.Contains(path, "/keys/"):
			m.revokeJWT.Add(1)
			m.record("revoke-jwt")
			w.WriteHeader(teardownStatus(opts.failTeardown))
		case r.Method == http.MethodDelete && strings.Contains(path, "/tenants/"):
			m.deleteTenant.Add(1)
			m.record("delete-tenant")
			w.WriteHeader(teardownStatus(opts.failTeardown))
		default:
			w.WriteHeader(http.StatusOK) // channel-rules seeding, etc.
		}
	}))
	t.Cleanup(srv.Close)
	return srv, m
}

func teardownStatus(fail bool) int {
	if fail {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

func newUpgradeRunner(srv *httptest.Server, staticKey string) *Runner {
	return New(Config{
		GatewayURL:         srv.URL,
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthUpgradeTimeout: 2 * time.Second,
		APIKey:             staticKey,
	}, zerolog.Nop())
}

// (a) Happy path: nothing supplied → tenant + JWT keypair (auth.Setup) + run-scoped API key
// (new branch); teardown reverses LIFO: api-key revoke → JWT keypair revoke → tenant delete.
func TestRunnerUpgrade_SelfProvision_HappyPath(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{})
	r := newUpgradeRunner(srv, "")

	run, err := r.Start("upgrade-happy", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createTenant.Load(); got != 1 {
		t.Errorf("CreateTenant=%d, want 1", got)
	}
	if got := m.registerJWT.Load(); got != 1 {
		t.Errorf("RegisterKey(JWT)=%d, want 1", got)
	}
	if got := m.createAPIKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1", got)
	}
	if d := m.deletes(); len(d) != 3 || d[0] != "revoke-apikey" || d[1] != "revoke-jwt" || d[2] != "delete-tenant" {
		t.Errorf("teardown order = %v, want [revoke-apikey revoke-jwt delete-tenant] (three-resource LIFO)", d)
	}
	status, _ := run.StatusSnapshot()
	if status != StatusComplete {
		t.Errorf("status=%q, want %q (run completes even though the mock is not a real gateway)", status, StatusComplete)
	}
}

// (b) API-key provisioning failure → run fails; the tenant + keypair (registered earlier by
// auth.Setup) are still cleaned up, and no api-key revoke is attempted.
func TestRunnerUpgrade_CreateKeyFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{failCreateAPIKey: true})
	r := newUpgradeRunner(srv, "")

	run, err := r.Start("upgrade-keyfail", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.revokeJWT.Load(); got != 1 {
		t.Errorf("RevokeKey(JWT)=%d, want 1 (auth.Setup cleanup still runs)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1", got)
	}
	if got := m.revokeAPIKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (api key was never created)", got)
	}
}

// (b) Tenant provisioning failure → run fails at auth setup; no api-key create attempted.
func TestRunnerUpgrade_CreateTenantFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{failCreateTenant: true})
	r := newUpgradeRunner(srv, "")

	run, err := r.Start("upgrade-tenantfail", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.createAPIKey.Load(); got != 0 {
		t.Errorf("CreateAPIKey=%d, want 0 (setup aborted before key provisioning)", got)
	}
}

// (c) Teardown failures are best-effort: all three deletes 500 → run result unaffected.
func TestRunnerUpgrade_TeardownFailuresDoNotFailRun(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{failTeardown: true})
	r := newUpgradeRunner(srv, "")

	run, err := r.Start("upgrade-teardownfail", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusComplete {
		t.Errorf("status=%q, want %q (teardown failures must not alter the run result)", status, StatusComplete)
	}
	if d := m.deletes(); len(d) != 3 {
		t.Errorf("teardown attempts = %v, want all three attempted", d)
	}
}

// (d) Static key precedence: cfg.APIKey set → no api-key provisioned/revoked; tenant + keypair
// still created by auth.Setup.
func TestRunnerUpgrade_StaticKey_NoKeyProvision(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{})
	r := newUpgradeRunner(srv, "static-upgrade-key")

	_, err := r.Start("upgrade-static", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createAPIKey.Load(); got != 0 {
		t.Errorf("CreateAPIKey=%d, want 0 with a static key supplied", got)
	}
	if got := m.revokeAPIKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 — supplied keys are never revoked", got)
	}
	if got := m.createTenant.Load(); got != 1 {
		t.Errorf("CreateTenant=%d, want 1 (auth.Setup still runs)", got)
	}
}

// (d) Supplied tenant precedence: request tenant_id set → tenant used as-is, never
// created/deleted; api key still self-provisioned.
func TestRunnerUpgrade_SuppliedTenant_NoTenantProvision(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{})
	r := newUpgradeRunner(srv, "")

	_, err := r.Start("upgrade-suppliedtenant", TestConfig{
		Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade, TenantID: "preexisting-tenant",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createTenant.Load(); got != 0 {
		t.Errorf("CreateTenant=%d, want 0 with a supplied tenant_id", got)
	}
	if got := m.deleteTenant.Load(); got != 0 {
		t.Errorf("DeleteTenant=%d, want 0 — supplied tenants are never deleted", got)
	}
	if got := m.createAPIKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1", got)
	}
}

// (e) Runner-boundary inference: no auth_mode + validate + suite=upgrade → upgrade path runs
// (api-key POST observed); type=load + suite=upgrade → jwt (no re-moding, no api-key POST).
func TestRunnerUpgrade_InferAuthModeFromSuite(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{})
	r := newUpgradeRunner(srv, "")

	_, err := r.Start("upgrade-infer", TestConfig{Type: TestValidate, Suite: SuiteUpgrade}) // AuthMode omitted
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()
	if got := m.createAPIKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1 — auth_mode must be inferred as upgrade from suite", got)
	}

	srv2, m2 := newUpgradeMock(t, upgradeMockOptions{})
	r2 := newUpgradeRunner(srv2, "")
	_, err = r2.Start("upgrade-infer-load", TestConfig{Type: TestLoad, Suite: SuiteUpgrade, Duration: "50ms"}) // load → jwt, not upgrade
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r2.Wait()
	if got := m2.createAPIKey.Load(); got != 0 {
		t.Errorf("CreateAPIKey=%d, want 0 — inference is validate-only, load must stay jwt", got)
	}
}

// (analog) API key created but key_id unparseable → run fails, cannot revoke it, but
// the tenant + keypair are still cleaned up.
func TestRunnerUpgrade_ParseKeyFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newUpgradeMock(t, upgradeMockOptions{badKeyBody: true})
	r := newUpgradeRunner(srv, "")

	run, err := r.Start("upgrade-parsefail", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.revokeAPIKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (key_id unknown — manual cleanup)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1", got)
	}
}

// (e) Cancellation between auth.Setup and api-key creation → tenant + keypair cleanup still
// run (registered by auth.Setup); no api-key revoke (key create was canceled).
func TestRunnerUpgrade_CancelBetweenSetupAndKey(t *testing.T) { //nolint:paralleltest // shared mock server
	reached := make(chan struct{})
	release := make(chan struct{})
	var m upgradeMockProv

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/api-keys"):
			m.createAPIKey.Add(1)
			close(reached)
			<-release
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/keys"):
			m.registerJWT.Add(1)
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && path == "/api/v1/tenants":
			m.createTenant.Add(1)
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && strings.Contains(path, "/api-keys/"):
			m.revokeAPIKey.Add(1)
			m.record("revoke-apikey")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && strings.Contains(path, "/keys/"):
			m.revokeJWT.Add(1)
			m.record("revoke-jwt")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && strings.Contains(path, "/tenants/"):
			m.deleteTenant.Add(1)
			m.record("delete-tenant")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	r := New(Config{
		GatewayURL:         srv.URL,
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthUpgradeTimeout: 2 * time.Second,
	}, zerolog.Nop())

	_, err := r.Start("upgrade-cancel", TestConfig{Type: TestValidate, Suite: SuiteUpgrade, AuthMode: AuthModeUpgrade})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	<-reached
	_ = r.Stop("upgrade-cancel")
	close(release)

	r.Wait()

	if got := m.revokeAPIKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (api-key create canceled → nothing to revoke)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1 (auth.Setup cleanup runs despite cancel)", got)
	}
}
