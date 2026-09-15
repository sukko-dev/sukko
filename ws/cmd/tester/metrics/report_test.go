package metrics

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sukko-dev/sukko/cmd/tester/stats"
)

func TestReport_WriteJSON(t *testing.T) {
	t.Parallel()

	r := &Report{
		TestType: "smoke",
		Status:   "pass",
		Checks: []CheckResult{
			{Name: "check1", Status: "pass", Latency: "10ms"},
		},
	}

	var buf bytes.Buffer
	if err := r.WriteJSON(&buf); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	var decoded Report
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.TestType != "smoke" {
		t.Errorf("TestType = %q, want %q", decoded.TestType, "smoke")
	}
	if decoded.Status != "pass" {
		t.Errorf("Status = %q, want %q", decoded.Status, "pass")
	}
	if len(decoded.Checks) != 1 || decoded.Checks[0].Name != "check1" {
		t.Errorf("unexpected checks: %+v", decoded.Checks)
	}
}

func TestReport_WriteTable(t *testing.T) {
	t.Parallel()

	r := &Report{
		TestType: "load",
		Status:   "pass",
		Metrics: Snapshot{
			Elapsed:           "10s",
			ConnectionsActive: 50,
			ConnectionsTotal:  100,
			ConnectionsFailed: 5,
			MessagesSent:      1000,
			MessagesReceived:  950,
			MessagesDropped:   10,
			Latency: stats.Snapshot{
				Count: 100,
				P50:   5.0,
				P95:   15.0,
				P99:   25.0,
				P999:  40.0,
			},
		},
		Checks: []CheckResult{
			{Name: "check1", Status: "pass", Latency: "5ms"},
			{Name: "check2", Status: "fail", Error: "timeout"},
		},
		Errors: []string{"connection reset"},
	}

	var buf bytes.Buffer
	r.WriteTable(&buf)
	out := buf.String()

	// Verify key sections present
	mustContain := []string{
		"Test Report: load",
		"Status: pass",
		"Duration: 10s",
		"[pass] check1 (5ms)",
		"[fail] check2",
		"timeout",
		"Connections: 50 active",
		"Messages: 1000 sent",
		"p50=5.0ms",
		"connection reset",
	}

	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("output missing %q:\n%s", s, out)
		}
	}
}

func TestReport_WriteTable_NoChecks(t *testing.T) {
	t.Parallel()

	r := &Report{
		TestType: "soak",
		Status:   "pass",
		Metrics:  Snapshot{Elapsed: "60s"},
	}

	var buf bytes.Buffer
	r.WriteTable(&buf)
	out := buf.String()
	if strings.Contains(out, "Checks:") {
		t.Error("expected no Checks section for empty checks")
	}
}

func TestReportStatusConstants(t *testing.T) {
	t.Parallel()
	if ReportStatusPass != "pass" {
		t.Errorf("ReportStatusPass = %q, want %q", ReportStatusPass, "pass")
	}
	if ReportStatusFail != "fail" {
		t.Errorf("ReportStatusFail = %q, want %q", ReportStatusFail, "fail")
	}
	if ReportStatusError != "error" {
		t.Errorf("ReportStatusError = %q, want %q", ReportStatusError, "error")
	}
	if CheckStatusPass != "pass" {
		t.Errorf("CheckStatusPass = %q, want %q", CheckStatusPass, "pass")
	}
	if CheckStatusFail != "fail" {
		t.Errorf("CheckStatusFail = %q, want %q", CheckStatusFail, "fail")
	}
	if CheckStatusSkip != "skip" {
		t.Errorf("CheckStatusSkip = %q, want %q", CheckStatusSkip, "skip")
	}
}

func TestReport_WriteTable_NoLatency(t *testing.T) {
	t.Parallel()

	r := &Report{
		TestType: "smoke",
		Status:   "pass",
		Metrics:  Snapshot{Elapsed: "1s"},
	}

	var buf bytes.Buffer
	r.WriteTable(&buf)
	out := buf.String()
	if strings.Contains(out, "Latency:") {
		t.Error("expected no Latency line when count=0")
	}
}
