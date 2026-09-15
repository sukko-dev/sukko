package runner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// newValidateUpgradeRun constructs a minimal TestRun for validateUpgrade unit tests.
func newValidateUpgradeRun(gatewayURL, apiKey string, minter *auth.Minter) *TestRun {
	return &TestRun{
		ID: "test-val-upgrade",
		Config: TestConfig{
			Type:       TestValidate,
			Suite:      "upgrade",
			GatewayURL: gatewayURL,
			TenantID:   "test-tenant",
			AuthMode:   AuthModeUpgrade,
		},
		Status:    StatusRunning,
		Collector: metrics.NewCollector(),
		authResult: &auth.SetupResult{
			TenantID: "test-tenant",
			Minter:   minter,
		},
		apiKey:             apiKey,
		authUpgradeTimeout: 100 * time.Millisecond, // short timeout so tests complete quickly
	}
}

// TestValidateUpgrade_NoAPIKey verifies that validateUpgrade returns a non-nil
// error (not check results) when run.apiKey is empty.
func TestValidateUpgrade_NoAPIKey(t *testing.T) {
	t.Parallel()

	minter := testMinter(t)
	run := newValidateUpgradeRun("ws://invalid:9999", "", minter)
	checks, err := validateUpgrade(context.Background(), run, zerolog.Nop())
	if err == nil {
		t.Fatal("expected error when apiKey is empty, got nil")
	}
	if !strings.Contains(err.Error(), "no api key configured") {
		t.Errorf("error = %q, want it to contain 'no api key configured'", err.Error())
	}
	if len(checks) != 0 {
		t.Errorf("expected no checks on early error, got %d", len(checks))
	}
}

// TestValidateUpgrade_NilMinter verifies that validateUpgrade returns a non-nil
// error when run.authResult.Minter is nil (api-key-only setup, which skips
// keypair registration).
func TestValidateUpgrade_NilMinter(t *testing.T) {
	t.Parallel()

	run := newValidateUpgradeRun("ws://invalid:9999", "pk_live_test123", nil)
	checks, err := validateUpgrade(context.Background(), run, zerolog.Nop())
	if err == nil {
		t.Fatal("expected error when Minter is nil, got nil")
	}
	if !strings.Contains(err.Error(), "Minter is nil") {
		t.Errorf("error = %q, want it to contain 'Minter is nil'", err.Error())
	}
	if len(checks) != 0 {
		t.Errorf("expected no checks on early error, got %d", len(checks))
	}
}

// TestValidateUpgrade_BasicHappyPath verifies that when run.apiKey is set and
// run.authResult.Minter is non-nil, validateUpgrade returns (checks, nil) even
// if the WS connection fails (unreachable gateway). The function returns check
// results with fail status, but the function-level error is nil.
func TestValidateUpgrade_BasicHappyPath(t *testing.T) {
	t.Parallel()

	minter := testMinter(t)
	// Use an unreachable gateway URL — the WS connect will fail but the function
	// should return check results (not a function-level error).
	run := newValidateUpgradeRun("ws://127.0.0.1:1", "pk_live_test123", minter)
	checks, err := validateUpgrade(context.Background(), run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateUpgrade returned function-level error: %v (expected nil — failures should be in check results)", err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check result (fail check for WS connect)")
	}
	// The first check should be "api key initial connect" with fail status.
	if checks[0].Name != "api key initial connect" {
		t.Errorf("checks[0].Name = %q, want %q", checks[0].Name, "api key initial connect")
	}
	if checks[0].Status != metrics.CheckStatusFail {
		t.Errorf("checks[0].Status = %q, want fail (WS connection to :1 should fail)", checks[0].Status)
	}
}

// upgradeMockMode selects how the mock gateway replies to an `auth` refresh frame.
type upgradeMockMode int

const (
	modeDiscriminate upgradeMockMode = iota // auth_ack for a live JWT, auth_error for an expired one (correct gateway)
	modeAlwaysAck                           // auth_ack even for an expired JWT (breaks the negative check)
	modeAlwaysError                         // auth_error even for a live JWT (breaks the happy check)
)

// jwtExpired decodes the (unverified) JWT payload and reports whether its `exp` is in the
// past. The mock only needs to read the claim to decide its reply — it never verifies the
// signature.
func jwtExpired(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return false
	}
	return claims.Exp > 0 && time.Now().Unix() >= claims.Exp
}

// newUpgradeMockGateway returns a ws:// URL for a mock gateway that upgrades API-key
// connections and, on an `auth` frame, replies with an auth_ack/auth_error frame per `mode`
// (mirrors validate_api_key_test.go's gobwas mock). It discriminates on cause by parsing the
// submitted JWT's `exp` — so a correct (discriminate) mock passes both the happy and negative
// checks, while the always-ack / always-error modes each break exactly one side.
func newUpgradeMockGateway(t *testing.T, mode upgradeMockMode) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		conn, _, _, err := ws.HTTPUpgrader{}.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			data, err := wsutil.ReadClientText(conn)
			if err != nil {
				return
			}
			var frame struct {
				Type string `json:"type"`
				Data struct {
					Token string `json:"token"`
				} `json:"data"`
			}
			if json.Unmarshal(data, &frame) != nil || frame.Type != msgTypeAuth {
				continue // ignore subscribe and anything else — check 2 is fire-and-forget
			}
			reply := respTypeAuthAck
			switch mode {
			case modeAlwaysAck:
				reply = respTypeAuthAck
			case modeAlwaysError:
				reply = respTypeAuthError
			default: // modeDiscriminate
				if jwtExpired(frame.Data.Token) {
					reply = respTypeAuthError
				}
			}
			resp, _ := json.Marshal(map[string]any{"type": reply})
			_ = wsutil.WriteServerText(conn, resp)
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// checkStatus returns the status of the named check, or "" if absent.
func checkStatus(checks []metrics.CheckResult, name string) string {
	for _, c := range checks {
		if c.Name == name {
			return c.Status
		}
	}
	return ""
}

// TestValidateUpgrade_DiscriminatingMock_AllChecksPass verifies the full happy path against a
// gateway that correctly acks live JWTs and rejects expired ones: all four checks pass.
func TestValidateUpgrade_DiscriminatingMock_AllChecksPass(t *testing.T) {
	t.Parallel()

	run := newValidateUpgradeRun(newUpgradeMockGateway(t, modeDiscriminate), "pk_live_test123", testMinter(t))
	run.authUpgradeTimeout = 2 * time.Second // generous for CI; localhost reply is sub-ms
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checks, err := validateUpgrade(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateUpgrade: %v", err)
	}
	for _, name := range []string{"api key initial connect", "public channel subscribe", "auth upgrade", "expired JWT rejected"} {
		if got := checkStatus(checks, name); got != metrics.CheckStatusPass {
			t.Errorf("check %q: status = %q, want pass", name, got)
		}
	}
}

// TestValidateUpgrade_AlwaysAck_NegativeCheckFails proves the negative check genuinely
// discriminates: a gateway that acks even the expired JWT must make "expired JWT rejected"
// FAIL (unexpected auth_ack) while "auth upgrade" still passes. Guards against one-sided green.
func TestValidateUpgrade_AlwaysAck_NegativeCheckFails(t *testing.T) {
	t.Parallel()

	run := newValidateUpgradeRun(newUpgradeMockGateway(t, modeAlwaysAck), "pk_live_test123", testMinter(t))
	run.authUpgradeTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checks, err := validateUpgrade(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateUpgrade: %v", err)
	}
	if got := checkStatus(checks, "auth upgrade"); got != metrics.CheckStatusPass {
		t.Errorf("check 'auth upgrade': status = %q, want pass (valid JWT still acked)", got)
	}
	if got := checkStatus(checks, "expired JWT rejected"); got != metrics.CheckStatusFail {
		t.Errorf("check 'expired JWT rejected': status = %q, want fail (gateway wrongly acked an expired JWT)", got)
	}
}

// TestValidateUpgrade_AlwaysError_HappyCheckFails is the mirror image: a gateway that errors
// even the live JWT must make "auth upgrade" FAIL while "expired JWT rejected" still passes.
func TestValidateUpgrade_AlwaysError_HappyCheckFails(t *testing.T) {
	t.Parallel()

	run := newValidateUpgradeRun(newUpgradeMockGateway(t, modeAlwaysError), "pk_live_test123", testMinter(t))
	run.authUpgradeTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checks, err := validateUpgrade(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateUpgrade: %v", err)
	}
	if got := checkStatus(checks, "auth upgrade"); got != metrics.CheckStatusFail {
		t.Errorf("check 'auth upgrade': status = %q, want fail (gateway wrongly errored a live JWT)", got)
	}
	if got := checkStatus(checks, "expired JWT rejected"); got != metrics.CheckStatusPass {
		t.Errorf("check 'expired JWT rejected': status = %q, want pass (expired JWT still errored)", got)
	}
}

// testMinter creates a real Minter backed by a freshly-generated keypair.
// Used by upgrade tests that need a non-nil Minter without a provisioning server.
func testMinter(t *testing.T) *auth.Minter {
	t.Helper()
	kp, err := auth.GenerateKeypair("test-upgrade-kp")
	if err != nil {
		t.Fatalf("GenerateKeypair: %v", err)
	}
	return auth.NewMinter(auth.MinterConfig{
		Keypair:  kp,
		TenantID: "test-tenant",
		Lifetime: 15 * time.Minute,
	})
}
