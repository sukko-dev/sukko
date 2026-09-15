package runner

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func newTestRunner() *Runner {
	return New(Config{
		GatewayURL:      "ws://localhost:3000",
		ProvisioningURL: "http://localhost:8080",
	}, zerolog.Nop())
}

func TestNew(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	if r == nil || r.tests == nil {
		t.Fatal("expected non-nil runner with initialized tests map")
	}
}

func TestRunner_Get_NotFound(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	_, err := r.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent test")
	}
	if !errors.Is(err, ErrTestNotFound) {
		t.Errorf("expected ErrTestNotFound, got %v", err)
	}
}

func TestRunner_Stop_NotFound(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	err := r.Stop("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent test")
	}
	if !errors.Is(err, ErrTestNotFound) {
		t.Errorf("expected ErrTestNotFound, got %v", err)
	}
}

func TestRunner_Start_DuplicateID(t *testing.T) {
	t.Parallel()

	r := newTestRunner()

	// Start a test — it will fail to connect but that's OK for this test
	cfg := TestConfig{
		Type:     TestSmoke,
		Duration: "1s",
	}
	_, err := r.Start("test-1", cfg)
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}

	// Try to start with same ID
	_, err = r.Start("test-1", cfg)
	if err == nil {
		t.Fatal("expected error for duplicate test ID")
	}
	if !errors.Is(err, ErrTestAlreadyExists) {
		t.Errorf("expected ErrTestAlreadyExists, got %v", err)
	}

	// Cleanup: stop and wait
	_ = r.Stop("test-1")
	r.Wait()
}

func TestRunner_Start_DefaultsFilled(t *testing.T) {
	t.Parallel()

	r := newTestRunner()

	cfg := TestConfig{
		Type: TestSmoke,
	}
	run, err := r.Start("test-defaults", cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify defaults were filled from runner config
	if run.Config.GatewayURL != "ws://localhost:3000" {
		t.Errorf("GatewayURL = %q, want %q", run.Config.GatewayURL, "ws://localhost:3000")
	}
	if run.Config.ProvisioningURL != "http://localhost:8080" {
		t.Errorf("ProvisioningURL = %q, want %q", run.Config.ProvisioningURL, "http://localhost:8080")
	}

	_ = r.Stop("test-defaults")
	r.Wait()
}

func TestRunner_Start_StatusRunning(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	run, err := r.Start("test-status", TestConfig{Type: TestSmoke})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if run.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", run.Status, StatusRunning)
	}
	if run.Collector == nil {
		t.Error("expected non-nil Collector")
	}

	_ = r.Stop("test-status")
	r.Wait()
}

func TestRunner_Get_AfterStart(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	_, err := r.Start("test-get", TestConfig{Type: TestSmoke})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	run, err := r.Get("test-get")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.ID != "test-get" {
		t.Errorf("ID = %q, want %q", run.ID, "test-get")
	}

	_ = r.Stop("test-get")
	r.Wait()
}

func TestTestRun_StatusSnapshot(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	run, err := r.Start("test-snap", TestConfig{Type: TestSmoke})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	status, report := run.StatusSnapshot()
	if status != StatusRunning {
		t.Errorf("Status = %q, want %q", status, StatusRunning)
	}
	if report != nil {
		t.Error("expected nil report while running")
	}

	_ = r.Stop("test-snap")
	r.Wait()
}

func TestTestType_Constants(t *testing.T) {
	t.Parallel()

	// Verify test type constants are defined
	types := []TestType{TestSmoke, TestLoad, TestStress, TestSoak, TestValidate}
	for _, tt := range types {
		if tt == "" {
			t.Error("found empty test type constant")
		}
	}
}

func TestTestStatus_Constants(t *testing.T) {
	t.Parallel()

	statuses := []TestStatus{StatusPending, StatusRunning, StatusComplete, StatusFailed, StatusStopped}
	for _, s := range statuses {
		if s == "" {
			t.Error("found empty status constant")
		}
	}
}

func TestSuiteRegistry_ContainsRevocation(t *testing.T) {
	t.Parallel()

	info, ok := SuiteRegistry[SuiteRevocation]
	if !ok {
		t.Fatalf("SuiteRegistry missing %q", SuiteRevocation)
	}
	if info.Name != SuiteRevocation {
		t.Errorf("Name = %q, want %q", info.Name, SuiteRevocation)
	}
	if info.Description == "" {
		t.Error("Description must not be empty")
	}
}

// TestRunner_Start_SuiteRevocationValidType verifies that stress and soak are accepted.
func TestRunner_Start_SuiteRevocationValidType(t *testing.T) {
	t.Parallel()

	for _, tt := range []TestType{TestStress, TestSoak} {
		t.Run(string(tt), func(t *testing.T) {
			t.Parallel()
			r := newTestRunner()
			run, err := r.Start("test-rev-"+string(tt), TestConfig{
				Type:  tt,
				Suite: SuiteRevocation,
			})
			if err != nil {
				t.Fatalf("Start(%s, revocation): unexpected error: %v", tt, err)
			}
			_ = r.Stop(run.ID)
			r.Wait()
		})
	}
}

// TestRunner_Start_SuiteRevocationInvalidType verifies validate + SuiteRevocation returns ErrInvalidConfig.
func TestRunner_Start_SuiteRevocationInvalidType(t *testing.T) {
	t.Parallel()

	r := newTestRunner()
	_, err := r.Start("test-rev-bad", TestConfig{
		Type:  TestValidate,
		Suite: SuiteRevocation,
	})
	if err == nil {
		t.Fatal("expected error for suite=revocation with type=validate")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestRunner_JSONMarshal_NoNATSJetStreamURLs(t *testing.T) {
	t.Parallel()
	data, err := json.Marshal(TestConfig{})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if strings.Contains(string(data), "nats_jetstream_urls") {
		t.Errorf("TestConfig JSON must not contain 'nats_jetstream_urls', got: %s", data)
	}
}

// TestTenantChannel asserts the qualification helper produces the exact
// "{tenant}.{suffix}" wire format the gateway's ValidateChannelTenant expects.
func TestTenantChannel(t *testing.T) {
	t.Parallel()
	if got := tenantChannel("tester-abc", "general.test"); got != "tester-abc.general.test" {
		t.Errorf("tenantChannel = %q, want %q", got, "tester-abc.general.test")
	}
}
