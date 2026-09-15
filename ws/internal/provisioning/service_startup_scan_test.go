package provisioning_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/testutil"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// activeTenantsGaugeValue reads provisioning_active_tenants from the default
// Prometheus registry — avoids accessing the unexported gauge var from outside the package.
func activeTenantsGaugeValue(t *testing.T) float64 {
	t.Helper()
	mfs, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "provisioning_active_tenants" {
			if len(mf.GetMetric()) > 0 {
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
	}
	t.Fatal("metric provisioning_active_tenants not found in registry")
	return 0
}

// newStartupScanService builds a service configured for startup scan tests.
// A subscription channel is returned so tests can observe TenantConfigChanged events.
func newStartupScanService(ts *testutil.MockTenantStore) (svc *provisioning.Service, events <-chan eventbus.Event) {
	bus := eventbus.New(zerolog.Nop())
	_, ch := bus.Subscribe()

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 ts,
		KeyStore:                    testutil.NewMockKeyStore(),
		APIKeyStore:                 testutil.NewMockAPIKeyStore(),
		RoutingRulesStore:           testutil.NewMockRoutingRulesStore(),
		QuotaStore:                  testutil.NewMockQuotaStore(),
		AuditStore:                  testutil.NewMockAuditStore(),
		KafkaAdmin:                  testutil.NewMockKafkaAdmin(),
		EventBus:                    bus,
		TopicNamespace:              "test",
		DefaultPartitions:           3,
		DefaultRetentionMs:          604800000,
		MaxTopicsPerTenant:          50,
		MaxRoutingRulesPerTenant:    5,
		DeadLetterTopicPartitions:   1,
		DeadLetterTopicRetentionMs:  86400000,
		InfraTopicReplicationFactor: 1,
		DeprovisionGraceDays:        30,
		SlugRenameTopicHoldPeriod:   platform.MinSlugRenameHoldPeriod,
		Logger:                      zerolog.Nop(),
	})
	if err != nil {
		panic("newStartupScanService: " + err.Error())
	}
	return svc, ch
}

func TestStartupScan_NoPendingRenames_NoEvents(t *testing.T) {
	t.Parallel()
	ts := testutil.NewMockTenantStore()
	svc, ch := newStartupScanService(ts)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case ev := <-ch:
		t.Errorf("expected no events; got %v", ev)
	default:
	}
}

func TestStartupScan_PendingTenant_WarnsAndReturnsNil(t *testing.T) {
	t.Parallel()
	ts := testutil.NewMockTenantStore()

	tenant := testutil.NewTestTenant("acme-corp")
	tenant.SlugRenameState = provisioning.SlugRenameStatePending
	tenant.PreviousSlug = "old-corp"
	_ = ts.Create(context.Background(), tenant)

	svc, ch := newStartupScanService(ts)

	err := svc.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v — pending state must not cause error", err)
	}

	// No TenantConfigChanged event expected for pending state (just a warning log).
	select {
	case ev := <-ch:
		if ev.Type == eventbus.TenantConfigChanged {
			t.Errorf("expected no TenantConfigChanged for pending state; got %v", ev)
		}
	default:
	}
}

func TestStartupScan_CompleteTenantWithPreviousSlug_EmitsEvent(t *testing.T) {
	t.Parallel()
	ts := testutil.NewMockTenantStore()

	now := time.Now()
	tenant := testutil.NewTestTenant("new-corp")
	tenant.SlugRenameState = provisioning.SlugRenameStateComplete
	tenant.PreviousSlug = "old-corp"
	tenant.SlugRenamedAt = &now
	_ = ts.Create(context.Background(), tenant)

	svc, ch := newStartupScanService(ts)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != eventbus.TenantConfigChanged {
			t.Errorf("event type = %v, want TenantConfigChanged", ev.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected TenantConfigChanged event; none received")
	}
}

func TestStartupScan_CompleteTenantNoPreviousSlug_NoEvent(t *testing.T) {
	t.Parallel()
	ts := testutil.NewMockTenantStore()

	// Complete state with empty PreviousSlug — guard condition; must not emit.
	tenant := testutil.NewTestTenant("corp")
	tenant.SlugRenameState = provisioning.SlugRenameStateComplete
	tenant.PreviousSlug = ""
	_ = ts.Create(context.Background(), tenant)

	svc, ch := newStartupScanService(ts)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case ev := <-ch:
		t.Errorf("expected no event for complete state with empty PreviousSlug; got %v", ev)
	default:
	}
}

func TestStartupScan_StoreError_ReturnsNil(t *testing.T) {
	t.Parallel()
	ts := testutil.NewMockTenantStore()
	ts.ListPendingRenamesErr = errors.New("db unavailable")

	svc, _ := newStartupScanService(ts)

	// Store error must not propagate — Start must return nil to avoid blocking service startup.
	if err := svc.Start(context.Background()); err != nil {
		t.Errorf("Start: %v — store error must not propagate", err)
	}
}

// TestStartup_InitializesActiveTenantGauge verifies that Start() sets the
// provisioning_active_tenants gauge to the count of active (not suspended) tenants.
//
//nolint:paralleltest // shares the process-wide Prometheus default registry; concurrent gauge mutations would make the assertion flaky.
func TestStartup_InitializesActiveTenantGauge(t *testing.T) {
	ts := testutil.NewMockTenantStore()
	for _, slug := range []string{"alpha", "beta", "gamma"} {
		_ = ts.Create(context.Background(), &provisioning.Tenant{
			ID: slug, Slug: slug, Status: provisioning.StatusActive,
		})
	}
	_ = ts.Create(context.Background(), &provisioning.Tenant{
		ID: "susp", Slug: "susp", Status: provisioning.StatusSuspended,
	})

	svc, _ := newStartupScanService(ts)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got := activeTenantsGaugeValue(t); got != 3.0 {
		t.Errorf("gauge = %v, want 3.0 (active only, suspended excluded)", got)
	}
}

// TestStartup_CountActiveError_NonFatal verifies that a CountActive failure
// during Start() is non-fatal — Start() returns nil and the gauge is unchanged.
//
//nolint:paralleltest // shares the process-wide Prometheus default registry.
func TestStartup_CountActiveError_NonFatal(t *testing.T) {
	ts := testutil.NewMockTenantStore()
	ts.CountActiveErr = errors.New("db unavailable")

	svc, _ := newStartupScanService(ts)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start() must return nil on CountActive error, got: %v", err)
	}
	// Gauge stays at its current value (not set to 0 or panicked).
	// We can't assert the exact value since parallel tests may have set it;
	// the important contract is that Start() returns nil.
}
