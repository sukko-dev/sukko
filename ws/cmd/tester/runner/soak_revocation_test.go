package runner

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

func newSoakRevocationRun(gatewayURL string, ar *auth.SetupResult) *TestRun {
	return &TestRun{
		ID: "test-soak-revocation",
		Config: TestConfig{
			Type:        TestSoak,
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

// TestRunSoakRevocationEditionGate verifies community edition → single CheckStatusSkip.
func TestRunSoakRevocationEditionGate(t *testing.T) {
	t.Parallel()
	srv := newEditionServer(t, "community")
	defer srv.Close()

	ar := testAuthResult(t)
	run := newSoakRevocationRun(srv.URL, ar)

	report, err := runSoakRevocationWithTimeout(context.Background(), run, zerolog.Nop(), time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if report.Checks[0].Name != "edition-gate" {
		t.Errorf("expected first check edition-gate, got %q", report.Checks[0].Name)
	}
	if report.Checks[0].Status != metrics.CheckStatusSkip {
		t.Errorf("expected CheckStatusSkip for community edition, got %s", report.Checks[0].Status)
	}
}

// TestRunSoakRevocationNilTokenFunc verifies that nil TokenFunc returns an error.
func TestRunSoakRevocationNilTokenFunc(t *testing.T) {
	t.Parallel()
	srv := newEditionServer(t, "pro")
	defer srv.Close()

	ar := testAuthResult(t)
	ar.TokenFunc = nil

	run := newSoakRevocationRun(srv.URL, ar)
	_, err := runSoakRevocationWithTimeout(context.Background(), run, zerolog.Nop(), time.Millisecond)
	if err == nil {
		t.Fatal("expected error for nil TokenFunc")
	}
}

// TestBuildSoakRevocationReport verifies pass/fail/skip aggregation logic.
func TestBuildSoakRevocationReport(t *testing.T) {
	t.Parallel()

	run := newSoakRevocationRun("ws://localhost:1", testAuthResult(t))

	t.Run("all pass → report passes", func(t *testing.T) {
		t.Parallel()
		checks := []metrics.CheckResult{
			{Name: "a", Status: metrics.CheckStatusPass},
			{Name: "b", Status: metrics.CheckStatusPass},
		}
		r := buildSoakRevocationReport(checks, run, false)
		if r.Status != metrics.ReportStatusPass {
			t.Errorf("status = %s, want PASS", r.Status)
		}
		if r.SkippedChecks != 0 {
			t.Errorf("skippedChecks = %d, want 0", r.SkippedChecks)
		}
	})

	t.Run("any fail → report fails", func(t *testing.T) {
		t.Parallel()
		checks := []metrics.CheckResult{
			{Name: "a", Status: metrics.CheckStatusPass},
			{Name: "b", Status: metrics.CheckStatusFail},
		}
		r := buildSoakRevocationReport(checks, run, false)
		if r.Status != metrics.ReportStatusFail {
			t.Errorf("status = %s, want FAIL", r.Status)
		}
	})

	t.Run("failed=true with no fail checks still fails", func(t *testing.T) {
		t.Parallel()
		checks := []metrics.CheckResult{
			{Name: "a", Status: metrics.CheckStatusPass},
		}
		r := buildSoakRevocationReport(checks, run, true)
		if r.Status != metrics.ReportStatusFail {
			t.Errorf("status = %s, want FAIL when failed=true", r.Status)
		}
	})

	t.Run("skip checks counted", func(t *testing.T) {
		t.Parallel()
		checks := []metrics.CheckResult{
			{Name: "a", Status: metrics.CheckStatusSkip},
			{Name: "b", Status: metrics.CheckStatusSkip},
			{Name: "c", Status: metrics.CheckStatusPass},
		}
		r := buildSoakRevocationReport(checks, run, false)
		if r.SkippedChecks != 2 {
			t.Errorf("skippedChecks = %d, want 2", r.SkippedChecks)
		}
		if r.Status != metrics.ReportStatusPass {
			t.Errorf("status = %s, want PASS (skipped != failed)", r.Status)
		}
	})

	t.Run("empty checks no abort → passes", func(t *testing.T) {
		t.Parallel()
		r := buildSoakRevocationReport(nil, run, false)
		if r.Status != metrics.ReportStatusPass {
			t.Errorf("status = %s, want PASS for empty checks", r.Status)
		}
	})
}
