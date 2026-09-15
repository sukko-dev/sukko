package provapi

import (
	"context"
	"errors"
	"testing"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// TestStreamChannelRulesProvider_ResolveTenantUUID covers the gateway tenant
// resolver: current-slug resolution, previous-slug alias during a rename hold,
// unknown-slug rejection, and the DS-001 readiness flag.
func TestStreamChannelRulesProvider_ResolveTenantUUID(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	const uuidA = "11111111-1111-1111-1111-111111111111"

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "tenant-a", TenantUuid: uuidA, PreviousSlug: "tenant-a-old"},
		},
	})

	// Current slug resolves.
	if got, err := r.ResolveTenantUUID(context.Background(), "tenant-a"); err != nil || got != uuidA {
		t.Fatalf("resolve current slug = (%q, %v), want (%q, nil)", got, err, uuidA)
	}
	// Previous slug (rename hold alias) resolves to the same UUID.
	if got, err := r.ResolveTenantUUID(context.Background(), "tenant-a-old"); err != nil || got != uuidA {
		t.Fatalf("resolve previous slug = (%q, %v), want (%q, nil)", got, err, uuidA)
	}
	// Unknown slug is rejected.
	if _, err := r.ResolveTenantUUID(context.Background(), "ghost"); !errors.Is(err, auth.ErrTenantNotResolvable) {
		t.Fatalf("resolve unknown slug err = %v, want ErrTenantNotResolvable", err)
	}
	if !r.TenantUUIDsPresent() {
		t.Fatal("TenantUUIDsPresent should be true after a snapshot carrying UUIDs")
	}

	// Snapshot rebuild drops the previous-slug alias once the hold window ends
	// (provisioning stops emitting previous_slug).
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "tenant-a", TenantUuid: uuidA},
		},
	})
	if _, err := r.ResolveTenantUUID(context.Background(), "tenant-a-old"); !errors.Is(err, auth.ErrTenantNotResolvable) {
		t.Fatalf("previous-slug alias should be gone after rebuild, err = %v", err)
	}
}

// TestStreamChannelRulesProvider_DS001Readiness covers the rolling-deploy skew
// guard: a non-empty snapshot with no tenant UUIDs keeps readiness degraded; a
// zero-tenant snapshot is ready; and a later delta carrying a UUID recovers.
func TestStreamChannelRulesProvider_DS001Readiness(t *testing.T) {
	t.Parallel()
	// Old provisioning peer: tenants present but no tenant_uuid -> degraded.
	r := newTestChannelRulesProvider()
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "tenant-a"}},
	})
	if r.TenantUUIDsPresent() {
		t.Fatal("TenantUUIDsPresent should be false when snapshot carries no UUIDs")
	}

	// Recovery via a later delta carrying a UUID.
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: false,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "tenant-a", TenantUuid: "u"}},
	})
	if !r.TenantUUIDsPresent() {
		t.Fatal("TenantUUIDsPresent should recover to true on a delta carrying a UUID")
	}

	// Zero-tenant snapshot is ready (never a permanent 503).
	r2 := newTestChannelRulesProvider()
	r2.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{IsSnapshot: true})
	if !r2.TenantUUIDsPresent() {
		t.Fatal("zero-tenant snapshot should be ready, not degraded")
	}
}
