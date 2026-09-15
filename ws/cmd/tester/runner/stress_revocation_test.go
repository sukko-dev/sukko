package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// newEditionServer returns an httptest.Server whose /edition endpoint returns the given edition JSON.
// It also returns 200 for any POST to /api/v1/tenants/*/tokens/revoke.
func newEditionServer(t *testing.T, edition string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/edition":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"edition": edition})
		case r.Method == http.MethodPost && r.URL.Path != "":
			// Accept revoke calls with 200.
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newStressRevocationRun(gatewayURL string, ar *auth.SetupResult) *TestRun {
	return &TestRun{
		ID: "test-stress-revocation",
		Config: TestConfig{
			Type:        TestStress,
			Suite:       SuiteRevocation,
			GatewayURL:  gatewayURL,
			Connections: 3,
			RampRate:    100,
		},
		Status:     StatusRunning,
		Collector:  metrics.NewCollector(),
		authResult: ar,
	}
}

// TestRunStressRevocationEditionGate verifies community edition → single CheckStatusSkip.
func TestRunStressRevocationEditionGate(t *testing.T) {
	t.Parallel()
	srv := newEditionServer(t, "community")
	defer srv.Close()

	ar := testAuthResult(t)
	run := newStressRevocationRun(srv.URL, ar)

	report, err := runStressRevocationWithDeadline(context.Background(), run, zerolog.Nop(), time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if report.Checks[0].Name != "edition-gate" {
		t.Errorf("expected first check to be edition-gate, got %q", report.Checks[0].Name)
	}
	if report.Checks[0].Status != metrics.CheckStatusSkip {
		t.Errorf("expected CheckStatusSkip for community edition, got %s", report.Checks[0].Status)
	}
}

// TestRunStressRevocationNilTokenFunc verifies that nil TokenFunc returns an error.
func TestRunStressRevocationNilTokenFunc(t *testing.T) {
	t.Parallel()
	srv := newEditionServer(t, "pro")
	defer srv.Close()

	ar := testAuthResult(t)
	ar.TokenFunc = nil // explicitly nil

	run := newStressRevocationRun(srv.URL, ar)
	_, err := runStressRevocationWithDeadline(context.Background(), run, zerolog.Nop(), time.Millisecond)
	if err == nil {
		t.Fatal("expected error for nil TokenFunc")
	}
}

// TestRunStressRevocationRevokeFailure verifies that a revoke HTTP 500 is reflected as CheckStatusFail.
// Pool connections fail (no WS server), actualConnected=0, so close-event collection completes
// immediately. The revoke call itself is made; if it fails, s1-revoke check fails.
func TestRunStressRevocationRevokeFailure(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/edition" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"edition": "enterprise"})
			return
		}
		// All revoke calls → 500 (exhaust retries).
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ar := testAuthResult(t)
	run := newStressRevocationRun(srv.URL, ar)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	report, err := runStressRevocationWithDeadline(ctx, run, zerolog.Nop(), time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var foundRevokeFail bool
	for _, c := range report.Checks {
		if c.Name == "s1-revoke" && c.Status == metrics.CheckStatusFail {
			foundRevokeFail = true
		}
	}
	if !foundRevokeFail {
		t.Errorf("expected s1-revoke check to fail; checks: %v", report.Checks)
	}
}

// TestRunStressRevocation_ZeroConnections_NoHang verifies that when no WS connections
// can be established (unreachable server), the runner returns a non-nil report without
// hanging. actualConnected=0, so the close-event loop is a no-op.
func TestRunStressRevocation_ZeroConnections_NoHang(t *testing.T) {
	t.Parallel()
	srv := newEditionServer(t, "pro")
	defer srv.Close()

	ar := testAuthResult(t)
	run := newStressRevocationRun(srv.URL, ar)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1ms deadline: close-event loop exits immediately.
	report, err := runStressRevocationWithDeadline(ctx, run, zerolog.Nop(), time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	// Report should complete within the 5s context; hanging would fail the test.
}
