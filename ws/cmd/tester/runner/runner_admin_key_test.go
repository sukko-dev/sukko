package runner

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// provisioningMock creates an httptest.Server that handles all provisioning API calls.
// It records the JWT kid from every Authorization header it sees.
type provisioningMock struct {
	mu       sync.Mutex
	kidsSeen []string
	srv      *httptest.Server
}

func newProvisioningMock(t *testing.T) *provisioningMock {
	t.Helper()
	m := &provisioningMock{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture kid from Authorization JWT
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			tokenStr := strings.TrimPrefix(auth, "Bearer ")
			p := jwt.NewParser()
			if tok, _, err := p.ParseUnverified(tokenStr, jwt.MapClaims{}); err == nil {
				if kid, ok := tok.Header["kid"].(string); ok {
					m.mu.Lock()
					m.kidsSeen = append(m.kidsSeen, kid)
					m.mu.Unlock()
				}
			}
		}

		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && path == "/api/v1/tenants":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/keys"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/api-keys"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"key_id":"test-api-key-123"}`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && strings.Contains(path, "/tenants/"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			slug := path[strings.LastIndex(path, "/")+1:]
			_, _ = w.Write([]byte(`{"id":"00000000-0000-0000-0000-000000000001","slug":"` + slug + `"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/tenants"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"items":[],"total":0}`))
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/keys"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"items":[]}`))
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/quota"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/audit-log"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"action":"test"}]`))
		case r.Method == http.MethodPut, r.Method == http.MethodPatch:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/suspend"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/reactivate"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(func() { m.srv.Close() })
	return m
}

func (m *provisioningMock) kidsSeenSnapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.kidsSeen))
	copy(out, m.kidsSeen)
	return out
}

// writeKeyFile writes an Ed25519 private key to a temp file and returns the path.
func writeKeyFile(t *testing.T, key ed25519.PrivateKey) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.key")
	if err := os.WriteFile(path, []byte(key), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return path
}

// TestRunnerExecute_AdminProviderWired verifies that when cfg.AdminKeyFile is set,
// run.authResult.AdminProvider is non-nil and its KeyID does NOT start with "ephemeral-".
func TestRunnerExecute_AdminProviderWired(t *testing.T) { //nolint:paralleltest // starts a live test run against a mock server; cannot be parallelized
	// Cannot use t.Parallel() — this test starts a test run that contacts a mock server.

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	keyPath := writeKeyFile(t, priv)

	m := newProvisioningMock(t)

	r := New(Config{
		GatewayURL:      m.srv.URL, // smoke test will fail to connect (not a real gateway)
		ProvisioningURL: m.srv.URL,
		AdminKeyFile:    keyPath,
		AdminKeyID:      "test-admin-kid",
		JWTLifetime:     5 * time.Minute,
		KeyExpiry:       24 * time.Hour,
	}, zerolog.Nop())

	run, startErr := r.Start("test-h", TestConfig{Type: TestSmoke})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	// authResult is set after auth.Setup() succeeds, regardless of whether the test itself passed.
	if run.authResult == nil {
		t.Fatal("authResult is nil — auth.Setup() must have failed; check mock server handlers")
	}
	if run.authResult.AdminProvider == nil {
		t.Fatal("AdminProvider is nil — admin provider was not wired from cfg.AdminKeyFile")
	}
	if run.authResult.AdminProvider.KeyID() != "test-admin-kid" {
		t.Errorf("AdminProvider.KeyID() = %q, want %q",
			run.authResult.AdminProvider.KeyID(), "test-admin-kid")
	}
}

// TestSecondarySetupCallers_PropagateProvider verifies that validate:provisioning secondary
// auth.Setup() uses the same AdminProvider kid as the primary setup.
func TestSecondarySetupCallers_PropagateProvider(t *testing.T) { //nolint:paralleltest // shares a mock HTTP server across the test body; cannot be parallelized
	// Cannot use t.Parallel() — uses a shared mock server.

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	keyPath := writeKeyFile(t, priv)

	m := newProvisioningMock(t)

	r := New(Config{
		GatewayURL:      m.srv.URL,
		ProvisioningURL: m.srv.URL,
		AdminKeyFile:    keyPath,
		AdminKeyID:      "expected-kid",
		JWTLifetime:     5 * time.Minute,
		KeyExpiry:       24 * time.Hour,
	}, zerolog.Nop())

	run, startErr := r.Start("test-m", TestConfig{Type: TestValidate, Suite: "provisioning"})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	if run.authResult == nil {
		t.Fatal("authResult is nil — auth.Setup() failed")
	}

	// Assert that admin-signed requests use the propagated admin kid. The suite now also emits
	// intentional NON-admin tokens (validateProvisioningRouting mints user-role and cross-tenant
	// tokens via setupA/setupB.Minter, kid "tester-<testID>...") for the role and
	// tenant-isolation checks — those are expected and are NOT admin requests. Every captured kid
	// must therefore be either the propagated admin kid OR a tester-minter kid; crucially, NONE
	// may be an "ephemeral-" kid — an ephemeral kid means a secondary auth.Setup silently
	// generated its own keypair instead of propagating the admin provider (the failure guarded).
	kids := m.kidsSeenSnapshot()
	if len(kids) == 0 {
		t.Fatal("no JWT kids captured — mock server never received admin requests")
	}
	adminKidCount := 0
	for i, kid := range kids {
		switch {
		case kid == "expected-kid":
			adminKidCount++
		case strings.HasPrefix(kid, "tester-"):
			// Intentional user-role / cross-tenant token minted by the suite — not an admin request.
		default:
			t.Errorf("request[%d]: kid = %q, want %q or a tester-minter kid (secondary setup used wrong provider)", i, kid, "expected-kid")
		}
		if strings.HasPrefix(kid, "ephemeral-") {
			t.Errorf("request[%d]: kid = %q is ephemeral — secondary auth.Setup did not propagate the admin provider", i, kid)
		}
	}

	// Verify the secondary setup calls (validateProvisioning + the routing block's tenant B) used
	// the propagated provider: expect ≥2 admin-signed requests carrying "expected-kid" (primary
	// setup + secondary setup CreateTenant, at minimum).
	if adminKidCount < 2 {
		t.Errorf("expected ≥2 admin requests with kid %q (primary + secondary setup), got %d", "expected-kid", adminKidCount)
	}
}

// TestValidateAuth_Check9_RetryExhaustion verifies that when all API-key connection attempts
// fail, check 9 is emitted as fail with the last error.
// MUST NOT call t.Parallel() — overrides package-level connectRetryBackoffs.
func TestValidateAuth_Check9_RetryExhaustion(t *testing.T) { //nolint:paralleltest // mutates package-level connectRetryBackoffs; cannot be parallelized
	orig := connectRetryBackoffs
	connectRetryBackoffs = []time.Duration{0, 0} // 2 fast attempts
	t.Cleanup(func() { connectRetryBackoffs = orig })

	m := newProvisioningMock(t)

	// Use a dead gateway URL — all WebSocket connections will fail.
	r := New(Config{
		GatewayURL:      "ws://127.0.0.1:1", // port 1 is never open
		ProvisioningURL: m.srv.URL,
		JWTLifetime:     5 * time.Minute,
		KeyExpiry:       24 * time.Hour,
	}, zerolog.Nop())

	run, startErr := r.Start("test-p", TestConfig{Type: TestValidate, Suite: "auth"})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	_, report := run.StatusSnapshot()
	if report == nil {
		t.Fatal("report is nil")
	}

	// Find check 9 (api key accepted) — should be fail.
	for _, c := range report.Checks {
		if c.Name == "api key accepted" {
			if c.Status != metrics.CheckStatusFail {
				t.Errorf("check 'api key accepted': status = %q, want %s", c.Status, metrics.CheckStatusFail)
			}
			return
		}
	}
	t.Errorf("check 'api key accepted' not found in report (got %d checks)", len(report.Checks))
}

// TestValidateAuth_Check9_CtxCancelled verifies that when the context is canceled,
// check 9 exits promptly and is emitted as fail.
// MUST NOT call t.Parallel() — overrides package-level connectRetryBackoffs.
func TestValidateAuth_Check9_CtxCancelled(t *testing.T) { //nolint:paralleltest // mutates package-level connectRetryBackoffs; cannot be parallelized
	orig := connectRetryBackoffs
	connectRetryBackoffs = []time.Duration{0, 500 * time.Millisecond} // 2nd attempt would block
	t.Cleanup(func() { connectRetryBackoffs = orig })

	m := newProvisioningMock(t)

	r := New(Config{
		GatewayURL:      "ws://127.0.0.1:1",
		ProvisioningURL: m.srv.URL,
		JWTLifetime:     5 * time.Minute,
		KeyExpiry:       24 * time.Hour,
	}, zerolog.Nop())

	run, startErr := r.Start("test-q", TestConfig{Type: TestValidate, Suite: "auth"})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	// Cancel the run immediately — ctx will be canceled during retry sleep.
	_ = r.Stop("test-q")

	r.Wait()

	_, report := run.StatusSnapshot()
	if report == nil {
		return // canceled before completing any checks — acceptable
	}

	for _, c := range report.Checks {
		if c.Name == "api key accepted" && c.Status == metrics.CheckStatusPass {
			t.Error("check 'api key accepted' reported pass after ctx cancel")
		}
	}
}

// TestValidateAuth_Check9_CreateAPIKeyFailed_SkipsRevoke verifies that when CreateAPIKey fails,
// revocation is never called (no nil dereference / panic).
// MUST NOT call t.Parallel() — overrides package-level connectRetryBackoffs.
func TestValidateAuth_Check9_CreateAPIKeyFailed_SkipsRevoke(t *testing.T) { //nolint:paralleltest // mutates package-level connectRetryBackoffs; cannot be parallelized
	orig := connectRetryBackoffs
	connectRetryBackoffs = []time.Duration{0} // single fast attempt
	t.Cleanup(func() { connectRetryBackoffs = orig })

	// Mock server that returns 500 for CreateAPIKey.
	var revokeCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/api-keys") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/api-keys/") {
			revokeCount++
			w.WriteHeader(http.StatusOK)
			return
		}
		// Handle all other provisioning calls normally.
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tenants":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/keys"):
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	r := New(Config{
		GatewayURL:      srv.URL,
		ProvisioningURL: srv.URL,
		JWTLifetime:     5 * time.Minute,
		KeyExpiry:       24 * time.Hour,
	}, zerolog.Nop())

	run, startErr := r.Start("test-r", TestConfig{Type: TestValidate, Suite: "auth"})
	if startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}

	r.Wait()

	if revokeCount != 0 {
		t.Errorf("RevokeAPIKey called %d time(s), want 0 when CreateAPIKey failed", revokeCount)
	}

	// Check 9 should be fail (create api key failed)
	_, report := run.StatusSnapshot()
	if report == nil {
		t.Fatal("report is nil")
	}
	for _, c := range report.Checks {
		if c.Name == "api key accepted" && c.Status == metrics.CheckStatusPass {
			t.Error("check 'api key accepted' reported pass despite CreateAPIKey failure")
		}
	}
}
