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

// apiKeyMockOptions injects provisioning failures for the api-key self-provision tests.
type apiKeyMockOptions struct {
	failCreateTenant bool // 500 on POST /tenants (auth setup failure)
	failCreateKey    bool // 500 on POST .../api-keys
	badKeyBody       bool // 201 but unparseable body
	failTeardown     bool // 500 on DELETE (revoke + tenant delete) — best-effort
}

// apiKeyMockProv records the provisioning calls made by an api-key validate run.
type apiKeyMockProv struct {
	createTenant atomic.Int32
	createKey    atomic.Int32
	revokeKey    atomic.Int32
	deleteTenant atomic.Int32

	mu          sync.Mutex
	deleteOrder []string // "revoke" / "deleteTenant" in observed order
}

func (m *apiKeyMockProv) recordDelete(kind string) {
	m.mu.Lock()
	m.deleteOrder = append(m.deleteOrder, kind)
	m.mu.Unlock()
}

func (m *apiKeyMockProv) deletes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.deleteOrder...)
}

// newAPIKeyMock builds a mock provisioning server. The GatewayURL points here too — the WS
// handshake fails (this is a plain HTTP server), so the suite's checks fail, but the tests
// assert the provisioning LIFECYCLE (tenant + key create/teardown), which is unaffected.
// The api-keys DELETE case MUST be matched before the tenants DELETE case (the key path
// also contains "/tenants/").
func newAPIKeyMock(t *testing.T, opts apiKeyMockOptions) (*httptest.Server, *apiKeyMockProv) {
	t.Helper()
	m := &apiKeyMockProv{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/api-keys"):
			m.createKey.Add(1)
			if opts.failCreateKey {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			if opts.badKeyBody {
				_, _ = w.Write([]byte(`{"unexpected":true}`))
				return
			}
			_, _ = w.Write([]byte(`{"key_id":"apikey-run-1"}`))
		case r.Method == http.MethodDelete && strings.Contains(path, "/api-keys/"):
			m.revokeKey.Add(1)
			m.recordDelete("revoke")
			if opts.failTeardown {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && path == "/api/v1/tenants":
			m.createTenant.Add(1)
			if opts.failCreateTenant {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && strings.Contains(path, "/tenants/"):
			m.deleteTenant.Add(1)
			m.recordDelete("deleteTenant")
			if opts.failTeardown {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			// channel-rules seeding and anything else.
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, m
}

func newAPIKeyRunner(srv *httptest.Server, staticKey string) *Runner {
	return New(Config{
		GatewayURL:         srv.URL,
		ProvisioningURL:    srv.URL,
		JWTLifetime:        5 * time.Minute,
		KeyExpiry:          24 * time.Hour,
		AuthUpgradeTimeout: 2 * time.Second,
		APIKey:             staticKey,
	}, zerolog.Nop())
}

// (a) Happy path: nothing supplied → tenant + key self-provisioned, then torn down in
// reverse order (key revoke before tenant delete).
func TestRunnerAPIKey_SelfProvision_HappyPath(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{})
	r := newAPIKeyRunner(srv, "")

	run, err := r.Start("apikey-happy", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createTenant.Load(); got != 1 {
		t.Errorf("CreateTenant=%d, want 1", got)
	}
	if got := m.createKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1", got)
	}
	if got := m.revokeKey.Load(); got != 1 {
		t.Errorf("RevokeAPIKey=%d, want 1", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1", got)
	}
	if d := m.deletes(); len(d) != 2 || d[0] != "revoke" || d[1] != "deleteTenant" {
		t.Errorf("teardown order = %v, want [revoke deleteTenant]", d)
	}
	status, _ := run.StatusSnapshot()
	if status != StatusComplete {
		t.Errorf("status=%q, want %q (run completes even though the mock is not a real gateway)", status, StatusComplete)
	}
}

// (d) Static key precedence: cfg.APIKey set → no key provisioned/revoked; throwaway tenant
// still created (none supplied).
func TestRunnerAPIKey_StaticKey_NoKeyProvision(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{})
	r := newAPIKeyRunner(srv, "static-apikey-xyz")

	_, err := r.Start("apikey-static", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createKey.Load(); got != 0 {
		t.Errorf("CreateAPIKey=%d, want 0 with a static key supplied", got)
	}
	if got := m.revokeKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 — supplied keys are never revoked", got)
	}
	if got := m.createTenant.Load(); got != 1 {
		t.Errorf("CreateTenant=%d, want 1 (no tenant supplied)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1", got)
	}
}

// (d) Supplied tenant precedence: request tenant_id set → tenant used as-is, never
// created/deleted; key still self-provisioned (none supplied).
func TestRunnerAPIKey_SuppliedTenant_NoTenantProvision(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{})
	r := newAPIKeyRunner(srv, "")

	_, err := r.Start("apikey-suppliedtenant", TestConfig{
		Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey, TenantID: "preexisting-tenant",
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
	if got := m.createKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1", got)
	}
	if got := m.revokeKey.Load(); got != 1 {
		t.Errorf("RevokeAPIKey=%d, want 1", got)
	}
}

// (b) Key provisioning failure at setup → run fails; the throwaway tenant is still cleaned
// up (its cleanup was registered before key provisioning) and nothing is revoked.
func TestRunnerAPIKey_CreateKeyFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{failCreateKey: true})
	r := newAPIKeyRunner(srv, "")

	run, err := r.Start("apikey-keyfail", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.createTenant.Load(); got != 1 {
		t.Errorf("CreateTenant=%d, want 1", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1 (tenant cleaned despite key failure)", got)
	}
	if got := m.revokeKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (key was never created)", got)
	}
}

// (b) Tenant provisioning failure → run fails at auth setup; no key create is attempted and
// no tenant delete (it was never created).
func TestRunnerAPIKey_CreateTenantFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{failCreateTenant: true})
	r := newAPIKeyRunner(srv, "")

	run, err := r.Start("apikey-tenantfail", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.createKey.Load(); got != 0 {
		t.Errorf("CreateAPIKey=%d, want 0 (setup aborted before key provisioning)", got)
	}
	if got := m.deleteTenant.Load(); got != 0 {
		t.Errorf("DeleteTenant=%d, want 0 (tenant was never created)", got)
	}
}

// (c) Teardown failures are best-effort: revoke + delete both 500 → run result unaffected.
func TestRunnerAPIKey_TeardownFailuresDoNotFailRun(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{failTeardown: true})
	r := newAPIKeyRunner(srv, "")

	run, err := r.Start("apikey-teardownfail", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusComplete {
		t.Errorf("status=%q, want %q (teardown failures must not alter the run result)", status, StatusComplete)
	}
	if got := m.revokeKey.Load(); got != 1 {
		t.Errorf("RevokeAPIKey attempts=%d, want 1", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant attempts=%d, want 1", got)
	}
}

// Key created but key_id unparseable → run fails, cannot revoke (id unknown), but
// the tenant is still deleted.
func TestRunnerAPIKey_ParseKeyFailure(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{badKeyBody: true})
	r := newAPIKeyRunner(srv, "")

	run, err := r.Start("apikey-parsefail", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	status, _ := run.StatusSnapshot()
	if status != StatusFailed {
		t.Errorf("status=%q, want %q", status, StatusFailed)
	}
	if got := m.revokeKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (key_id unknown — manual cleanup)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1", got)
	}
}

// (f) Auth-mode inference at the runner boundary: no auth_mode + validate + suite=api-key
// resolves to api-key mode, so the self-provision path runs.
func TestRunnerAPIKey_InferAuthModeFromSuite(t *testing.T) { //nolint:paralleltest // shared mock server
	srv, m := newAPIKeyMock(t, apiKeyMockOptions{})
	r := newAPIKeyRunner(srv, "")

	_, err := r.Start("apikey-infer", TestConfig{Type: TestValidate, Suite: SuiteAPIKey}) // AuthMode omitted
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	if got := m.createKey.Load(); got != 1 {
		t.Errorf("CreateAPIKey=%d, want 1 — auth_mode must be inferred as api-key from suite", got)
	}
}

// (e) Cancellation between tenant and key creation: the run is stopped while the key-create
// request is in flight → the tenant cleanup still runs and nothing is revoked.
func TestRunnerAPIKey_CancelBetweenTenantAndKey(t *testing.T) { //nolint:paralleltest // shared mock server
	reached := make(chan struct{})
	release := make(chan struct{})
	var m apiKeyMockProv

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/api-keys"):
			m.createKey.Add(1)
			close(reached) // signal the key-create POST arrived
			<-release      // hold the request open until the test cancels the run
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodDelete && strings.Contains(path, "/api-keys/"):
			m.revokeKey.Add(1)
			m.recordDelete("revoke")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && path == "/api/v1/tenants":
			m.createTenant.Add(1)
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && strings.Contains(path, "/tenants/"):
			m.deleteTenant.Add(1)
			m.recordDelete("deleteTenant")
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

	_, err := r.Start("apikey-cancel", TestConfig{Type: TestValidate, Suite: SuiteAPIKey, AuthMode: AuthModeAPIKey})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	<-reached                   // key-create POST is in flight
	_ = r.Stop("apikey-cancel") // cancel the run's context → CreateAPIKey returns canceled
	close(release)              // let the mock goroutine finish

	r.Wait()

	if got := m.revokeKey.Load(); got != 0 {
		t.Errorf("RevokeAPIKey=%d, want 0 (key create canceled → nothing to revoke)", got)
	}
	if got := m.deleteTenant.Load(); got != 1 {
		t.Errorf("DeleteTenant=%d, want 1 (tenant cleanup runs despite cancel)", got)
	}
}
