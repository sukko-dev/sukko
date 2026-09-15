package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// newValidateAPIKeyRun constructs a minimal TestRun for validateAPIKey unit tests.
// It bypasses execute(), so BOTH deny-wait fields must be set here (production seeds them
// unconditionally from the defaultDenyWait* consts). 3s/1s keeps the 3-attempt production shape
// while staying fast; individual tests overwrite with smaller values as needed.
func newValidateAPIKeyRun(gatewayURL, apiKey string) *TestRun {
	return &TestRun{
		ID: "test-val-apikey",
		Config: TestConfig{
			Type:       TestValidate,
			Suite:      "api-key",
			GatewayURL: gatewayURL,
			TenantID:   "test-tenant",
			AuthMode:   AuthModeAPIKey,
		},
		Status:                StatusRunning,
		Collector:             metrics.NewCollector(),
		apiKey:                apiKey,
		denyWaitDeadline:      3 * time.Second, // short deadline for tests; drives Check 3 deny-wait
		denyWaitRetryInterval: 1 * time.Second,
	}
}

// wsRestServerOpts parameterizes the mock's private-channel deny behavior for the #216
// retry-path tests. The zero value preserves the original deny-on-first-subscribe behavior,
// so existing call sites are unaffected (a blanket global withhold would break/slow the
// deny-on-first tests — forbidden by the spec).
type wsRestServerOpts struct {
	withholdDenies    int           // N: per-connection; withhold the deny for the first N private subscribes, deny on attempt N+1. 0 = deny immediately.
	privateSubscribed chan struct{} // non-nil: non-blocking signal per private subscribe received (deterministic test synchronization)
	subscribeCount    *atomic.Int64 // non-nil: incremented per private subscribe received
	closeAfterDeny    bool          // close the conn immediately after writing the deny frame (drives the transport-error-after-deny path)
}

// newWsRestServer returns an httptest.Server that:
//   - Upgrades /ws requests to WebSocket using gobwas.
//   - Parses incoming subscribe frames; sends back an error frame for channels
//     whose name contains "private" (simulating gateway access-control denial).
//   - Responds to POST /api/v1/publish with the provided REST publish status code.
//
// The server accepts connections on http://host:port. For WS connections,
// gobwas ws.Dialer dials TCP directly to host:port and performs the HTTP
// upgrade, so the server works regardless of whether the caller uses
// "ws://" or "http://" scheme in the GatewayURL.
//
// Note: production code's restpublish.NewClient uses run.Config.GatewayURL
// directly. If GatewayURL is "ws://...", Go's http.Client rejects the
// scheme before even connecting. This is a known limitation in
// validate_api_key.go (should use httpURL() for the REST call).
// Tests that exercise the REST path must use an "http://" GatewayURL.
func newWsRestServer(t *testing.T, restPublishStatus int, opts ...wsRestServerOpts) *httptest.Server {
	t.Helper()
	var o wsRestServerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			upgrader := ws.HTTPUpgrader{}
			conn, _, _, err := upgrader.Upgrade(r, w)
			if err != nil {
				return
			}
			defer conn.Close()
			privateSubs := 0 // per-connection withhold counter (per-connection state)
			for {
				msg, err := wsutil.ReadClientText(conn)
				if err != nil {
					return
				}
				// Parse the frame and deny unauthorized subscribes / publishes.
				var frame struct {
					Type    string `json:"type"`
					Channel string `json:"channel"`
					Data    struct {
						Channels []string `json:"channels"`
					} `json:"data"`
				}
				if jsonErr := json.Unmarshal(msg, &frame); jsonErr != nil {
					continue
				}
				switch frame.Type {
				case "subscribe":
					for _, ch := range frame.Data.Channels {
						// Deny the private/group/user channels the deny checks isolate; the public
						// channel (validate.public) matches none of these and stays allowed.
						isPrivate := strings.Contains(ch, "private")
						if !isPrivate && !strings.Contains(ch, "room.") && !strings.Contains(ch, "dm.") {
							continue
						}
						// The #216 retry-path machinery (withhold / count / signal / close) applies to
						// the private-channel deny only — the group/user channels always deny at once,
						// so they never perturb the retry-path assertions.
						if isPrivate {
							privateSubs++
							if o.subscribeCount != nil {
								o.subscribeCount.Add(1)
							}
							if o.privateSubscribed != nil {
								select {
								case o.privateSubscribed <- struct{}{}:
								default:
								}
							}
							if privateSubs <= o.withholdDenies {
								continue // withhold: simulate the slow/lost deny that caused #216
							}
						}
						// Real server deny frame: top-level type + code (the tester reads Message.Code),
						// NOT a nested data.code. subscribe_error / invalid_request per the wire contract.
						errMsg, _ := json.Marshal(map[string]any{
							"type":    respTypeSubscribeError,
							"channel": ch,
							"code":    wsErrCodeInvalidRequest,
						})
						_ = wsutil.WriteServerText(conn, errMsg)
						if isPrivate && o.closeAfterDeny {
							_ = conn.Close()
							return
						}
					}
				case "publish":
					// API keys cannot publish over WS — the gateway rejects with publish_error/forbidden
					// (top-level code). Answer every publish so the "ws publish denied" check resolves in
					// one round-trip (else it burns ~3× the deadline retrying).
					errMsg, _ := json.Marshal(map[string]any{
						"type":    respTypePublishError,
						"channel": frame.Channel,
						"code":    wsErrCodeForbidden,
					})
					_ = wsutil.WriteServerText(conn, errMsg)
				}
			}
		}
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/publish" {
			w.WriteHeader(restPublishStatus)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestValidateAPIKey_NoAPIKey verifies that validateAPIKey returns a non-nil
// error (not a check result) when run.apiKey is empty.
func TestValidateAPIKey_NoAPIKey(t *testing.T) {
	t.Parallel()

	run := newValidateAPIKeyRun("ws://invalid:9999", "")
	checks, err := validateAPIKey(context.Background(), run, zerolog.Nop())
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

// TestValidateAPIKey_HappyPath verifies all seven checks pass: the mock accepts
// API-key WS connections (Check 1), allows public-channel subscribe (Check 2),
// sends a subscribe_error frame for the private channel (Check 3 — gateway denies it),
// returns 403 for REST publish (Check 4 — gateway blocks API-key REST publish), and
// denies the group/user channel subscribes and the WS publish (Checks 5-7 — the new
// scoping deny checks: subscribe_error/invalid_request and publish_error/forbidden).
func TestValidateAPIKey_HappyPath(t *testing.T) {
	t.Parallel()

	// Use ws:// so gobwas accepts the scheme for the WS upgrade. validateAPIKey
	// applies httpURL() before passing to restpublish.NewClient, converting
	// ws:// → http://, so the REST call also reaches the mock (which returns 403).
	srv := newWsRestServer(t, http.StatusForbidden)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check result")
	}

	wantPass := []string{
		"api key accepted", "public channel subscribe", "private channel denied", "api key REST publish blocked",
		"group channel denied", "user channel denied", "ws publish denied",
	}
	for _, name := range wantPass {
		var found bool
		for _, c := range checks {
			if c.Name == name {
				found = true
				if c.Status != metrics.CheckStatusPass {
					t.Errorf("check %q: status = %q, want %q; error: %s", name, c.Status, metrics.CheckStatusPass, c.Error)
				}
				break
			}
		}
		if !found {
			t.Errorf("check %q not found in results (got %d checks)", name, len(checks))
		}
	}

	if got := run.Collector.ConnectionsAPIKey.Load(); got != 1 {
		t.Errorf("ConnectionsAPIKey = %d, want 1", got)
	}
}

// TestValidateAPIKey_RESTPublishReturns200_IncreasesErrorCounter verifies that
// when the REST publish endpoint returns 200 (unexpected — gateway should return
// 403 for API-key clients), Check 4 fails and RESTPublishErrors is incremented.
//
// This test uses a ws:// GatewayURL. gobwas dials TCP directly so the WS upgrade
// succeeds. validateAPIKey calls httpURL() before passing the URL to
// restpublish.NewClient, converting ws:// → http://, so Go's http.Client can
// also reach the mock. Checks 1–3 pass (WS + mock sends deny for private channel),
// Check 4 fails because the mock returns 200 instead of 403, and RESTPublishErrors
// is incremented to 1.
func TestValidateAPIKey_RESTPublishReturns200_IncreasesErrorCounter(t *testing.T) {
	t.Parallel()

	srv := newWsRestServer(t, http.StatusOK)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}
	if len(checks) == 0 {
		t.Fatal("expected at least one check result")
	}

	var foundRESTCheck bool
	for _, c := range checks {
		if c.Name == "api key REST publish blocked" {
			foundRESTCheck = true
			if c.Status != metrics.CheckStatusFail {
				t.Errorf("check %q: status = %q, want %q", c.Name, c.Status, metrics.CheckStatusFail)
			}
			if got := run.Collector.RESTPublishErrors.Load(); got != 1 {
				t.Errorf("RESTPublishErrors = %d, want 1 (mock returns 200, default branch increments counter)", got)
			}
			break
		}
	}
	if !foundRESTCheck {
		t.Fatal("check 'api key REST publish blocked' not found in results")
	}
}

// TestValidateAPIKey_SubscribeErrorFrameDenies verifies Check 3 passes when the gateway
// denies the private channel with a subscribe_error frame (the real server frame type) —
// not a generic "error" frame. This exercises the widened errCh matcher.
func TestValidateAPIKey_SubscribeErrorFrameDenies(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			conn, _, _, err := ws.HTTPUpgrader{}.Upgrade(r, w)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				msg, err := wsutil.ReadClientText(conn)
				if err != nil {
					return
				}
				var frame struct {
					Type string `json:"type"`
					Data struct {
						Channels []string `json:"channels"`
					} `json:"data"`
				}
				if json.Unmarshal(msg, &frame) != nil || frame.Type != "subscribe" {
					continue
				}
				for _, ch := range frame.Data.Channels {
					if strings.Contains(ch, "private") {
						errMsg, _ := json.Marshal(map[string]any{
							"type":    "subscribe_error", // NOT "error" — the real denial frame type
							"channel": ch,
							"code":    "invalid_request", // top-level code (Message.Code), NOT nested data.code
						})
						_ = wsutil.WriteServerText(conn, errMsg)
					}
				}
			}
		}
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/publish" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}

	var found bool
	for _, c := range checks {
		if c.Name == "private channel denied" {
			found = true
			if c.Status != metrics.CheckStatusPass {
				t.Errorf("check 'private channel denied': status = %q, want %q; error: %s", c.Status, metrics.CheckStatusPass, c.Error)
			}
		}
	}
	if !found {
		t.Error("check 'private channel denied' not found — the subscribe_error frame was not captured")
	}
}

// TestValidateAPIKey_SeedsNarrowPublicRule verifies the suite seeds channel rules that
// narrow public to just the suite's public channel — so an API-key client (nil claims →
// rules.Public only) is denied the private channel. The WS handshake fails against this
// mock (no upgrade), but the seed runs first and its payload is recorded.
func TestValidateAPIKey_SeedsNarrowPublicRule(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var gotRules map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/channel-rules") {
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&gotRules)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK) // WS GET gets 200 (not 101) → handshake fails, connect check fails
	}))
	t.Cleanup(srv.Close)

	provider, _, err := auth.NewEphemeralAuthProvider()
	if err != nil {
		t.Fatalf("NewEphemeralAuthProvider: %v", err)
	}
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	run.authResult = &auth.SetupResult{
		TenantID:   "test-tenant",
		ProvClient: auth.NewProvisioningClient(srv.URL, provider, zerolog.Nop()),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := validateAPIKey(ctx, run, zerolog.Nop()); err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotRules == nil {
		t.Fatal("SetChannelRules was never called — the suite must seed narrow rules")
	}
	pub, _ := gotRules["public"].([]any)
	if len(pub) != 1 || pub[0] != validatePublicChannel {
		t.Errorf("seeded public rule = %v, want [%q]", gotRules["public"], validatePublicChannel)
	}
}

// findCheck returns the first check with the given name, or nil.
func findCheck(checks []metrics.CheckResult, name string) *metrics.CheckResult {
	for i := range checks {
		if checks[i].Name == name {
			return &checks[i]
		}
	}
	return nil
}

// TestValidateAPIKey_DenyOnRetry drives the #216 flake path deterministically: the mock
// withholds the first deny (simulating a slow/lost deny round-trip), so only a re-subscribe
// retry elicits it. Asserts the check passes AND the retry actually fired (≥2 private
// subscribes observed) — without the count assertion the test would pass vacuously against
// the old one-shot code whenever timing got lucky.
func TestValidateAPIKey_DenyOnRetry(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	srv := newWsRestServer(t, http.StatusForbidden, wsRestServerOpts{
		withholdDenies: 1,
		subscribeCount: &count,
	})
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	run.denyWaitRetryInterval = 50 * time.Millisecond
	run.denyWaitDeadline = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}

	c := findCheck(checks, "private channel denied")
	if c == nil {
		t.Fatalf("check %q not found (got %d checks)", "private channel denied", len(checks))
	}
	if c.Status != metrics.CheckStatusPass {
		t.Errorf("private channel denied: status = %q, want pass; error: %s", c.Status, c.Error)
	}
	if got := count.Load(); got < 2 {
		t.Errorf("private subscribes observed = %d, want >= 2 (retry must actually fire)", got)
	}
}

// TestValidateAPIKey_NeverDenied_HardFail proves the retry did not introduce a vacuous pass:
// a platform that never denies produces a hard FAIL with the timeout message. Also asserts
// the windowed-send bound: the loop stops sending one interval before the deadline,
// so the observed subscribe count stays strictly below deadline/interval.
func TestValidateAPIKey_NeverDenied_HardFail(t *testing.T) {
	t.Parallel()

	const (
		interval = 200 * time.Millisecond
		deadline = 500 * time.Millisecond
	)
	var count atomic.Int64
	srv := newWsRestServer(t, http.StatusForbidden, wsRestServerOpts{
		withholdDenies: 1 << 30, // never deny
		subscribeCount: &count,
	})
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	run.denyWaitRetryInterval = interval
	run.denyWaitDeadline = deadline

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}

	c := findCheck(checks, "private channel denied")
	if c == nil {
		t.Fatalf("check %q not found (got %d checks)", "private channel denied", len(checks))
	}
	if c.Status != metrics.CheckStatusFail {
		t.Errorf("private channel denied: status = %q, want fail (never-denied must red the check)", c.Status)
	}
	if !strings.Contains(c.Error, "timed out waiting for deny signal") {
		t.Errorf("error = %q, want the timeout message", c.Error)
	}
	// Windowed-send bound: strictly fewer sends than deadline/interval proves the
	// final-interval send was suppressed (no send at deadline−ε). MUST be float division —
	// integer Duration division (500ms/200ms == 2) would collapse the bound to 2 < 2.
	bound := float64(deadline) / float64(interval) // 2.5
	if got := float64(count.Load()); got >= bound {
		t.Errorf("private subscribes = %v, want < %v (loop must stop sending one interval before the deadline)", got, bound)
	}
}

// TestValidateAPIKey_ParentCancelDuringWait verifies parent-context cancellation
// mid-wait short-circuits cleanly with NO "private channel denied" result recorded. The
// cancel is synchronized on the mock's subscribe-received signal — never a wall-clock sleep
// (a sleep-race would reintroduce the timing flakiness this fix removes).
func TestValidateAPIKey_ParentCancelDuringWait(t *testing.T) {
	t.Parallel()

	subscribed := make(chan struct{}, 1)
	srv := newWsRestServer(t, http.StatusForbidden, wsRestServerOpts{
		withholdDenies:    1 << 30, // never deny — the wait only ends via the cancel
		privateSubscribed: subscribed,
	})
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	run.denyWaitRetryInterval = 50 * time.Millisecond
	run.denyWaitDeadline = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Cancel only after the mock has observed the first private subscribe — the wait
		// loop is then provably past its send and blocked in the select.
		select {
		case <-subscribed:
			cancel()
		case <-time.After(5 * time.Second):
			// Safety valve so a broken suite cannot hang the test; the deadline assert below fails it.
			cancel()
		}
	}()

	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}
	if c := findCheck(checks, "private channel denied"); c != nil {
		t.Errorf("parent-cancel must record NO private-channel result, got status %q (error %q)", c.Status, c.Error)
	}
}

// TestValidateAPIKey_DenyWinsOverTransportError is the end-to-end deny-wins integration case:
// the mock withholds the first deny, then answers the retry with a deny AND immediately
// closes the connection. Whichever branch the race takes (deny consumed in the select, or a
// later write error probing the buffered deny), a correct implementation passes. (The
// deterministic branch-level assertion is TestWaitForDeny_DenyWinsProbe_Deterministic (deny_wait_test.go) —
// this end-to-end shape cannot force the transport-error branch reliably.)
func TestValidateAPIKey_DenyWinsOverTransportError(t *testing.T) {
	t.Parallel()

	srv := newWsRestServer(t, http.StatusForbidden, wsRestServerOpts{
		withholdDenies: 1,
		closeAfterDeny: true,
	})
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	run := newValidateAPIKeyRun(wsURL, "pk_live_test123")
	run.denyWaitRetryInterval = 50 * time.Millisecond
	run.denyWaitDeadline = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	checks, err := validateAPIKey(ctx, run, zerolog.Nop())
	if err != nil {
		t.Fatalf("validateAPIKey: %v", err)
	}

	c := findCheck(checks, "private channel denied")
	if c == nil {
		t.Fatalf("check %q not found (got %d checks)", "private channel denied", len(checks))
	}
	if c.Status != metrics.CheckStatusPass {
		t.Errorf("private channel denied: status = %q, want pass (deny must win); error: %s", c.Status, c.Error)
	}
}
