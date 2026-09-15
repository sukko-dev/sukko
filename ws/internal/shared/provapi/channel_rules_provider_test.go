package provapi

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// newTestChannelRulesProvider creates a minimal StreamChannelRulesProvider for unit testing
// without gRPC connections or Prometheus metrics.
func newTestChannelRulesProvider() *StreamChannelRulesProvider {
	r := &StreamChannelRulesProvider{
		channelRules:     make(map[string]*types.ChannelRules),
		tenantUUIDBySlug: make(map[string]string),
		slugByUUID:       make(map[string]string),
		logger:           zerolog.Nop(),
	}
	r.routingSnapshots.Store(make(map[string]TenantRoutingSnapshot))
	return r
}

func TestNewStreamChannelRulesProvider_EmptyGRPCAddr(t *testing.T) {
	t.Parallel()
	_, err := NewStreamChannelRulesProvider(StreamChannelRulesProviderConfig{
		GRPCAddr: "",
	})
	if err == nil {
		t.Fatal("expected error for empty GRPCAddr, got nil")
	}
}

func TestChannelRulesProvider_Snapshot(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{
				TenantSlug: "tenant-a",
				ChannelRules: &provisioningv1.ChannelRules{
					PublicChannels:  []string{"*.trade"},
					DefaultChannels: []string{"news"},
					GroupMappings: map[string]*provisioningv1.GroupChannels{
						"admin": {Channels: []string{"admin.*"}},
					},
				},
			},
		},
	})

	ctx := context.Background()

	t.Run("GetChannelRules", func(t *testing.T) {
		t.Parallel()
		rules, err := r.GetChannelRules(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("GetChannelRules() error = %v", err)
		}
		if len(rules.Public) != 1 || rules.Public[0] != "*.trade" {
			t.Errorf("Public = %v, want [*.trade]", rules.Public)
		}
		if len(rules.Default) != 1 || rules.Default[0] != "news" {
			t.Errorf("Default = %v, want [news]", rules.Default)
		}
		if len(rules.GroupMappings) != 1 {
			t.Errorf("GroupMappings len = %d, want 1", len(rules.GroupMappings))
		}
	})

	t.Run("GetChannelRules not found", func(t *testing.T) {
		t.Parallel()
		_, err := r.GetChannelRules(ctx, "nonexistent")
		if !errors.Is(err, types.ErrChannelRulesNotFound) {
			t.Errorf("expected ErrChannelRulesNotFound, got %v", err)
		}
	})
}

func TestChannelRulesProvider_SnapshotReplacesAll(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	// First snapshot
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{
				TenantSlug: "tenant-a",
				ChannelRules: &provisioningv1.ChannelRules{
					PublicChannels: []string{"*.trade"},
				},
			},
		},
	})

	// Second snapshot replaces all
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{
				TenantSlug: "tenant-b",
				ChannelRules: &provisioningv1.ChannelRules{
					PublicChannels: []string{"*.market"},
				},
			},
		},
	})

	ctx := context.Background()

	// tenant-a should be gone
	_, err := r.GetChannelRules(ctx, "tenant-a")
	if !errors.Is(err, types.ErrChannelRulesNotFound) {
		t.Error("tenant-a should not exist after new snapshot")
	}

	// tenant-b should exist
	rules, err := r.GetChannelRules(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("GetChannelRules(tenant-b) error = %v", err)
	}
	if len(rules.Public) != 1 || rules.Public[0] != "*.market" {
		t.Errorf("Public = %v, want [*.market]", rules.Public)
	}
}

func TestChannelRulesProvider_DeltaRemove(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	// Load snapshot
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{
				TenantSlug: "tenant-a",
				ChannelRules: &provisioningv1.ChannelRules{
					PublicChannels: []string{"*.trade"},
				},
			},
		},
	})

	// Delta: remove tenant-a
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot:         false,
		RemovedTenantSlugs: []string{"tenant-a"},
	})

	ctx := context.Background()

	_, err := r.GetChannelRules(ctx, "tenant-a")
	if !errors.Is(err, types.ErrChannelRulesNotFound) {
		t.Error("tenant-a channel rules should be removed")
	}
}

func TestProtoToChannelRules(t *testing.T) {
	t.Parallel()

	t.Run("full rules", func(t *testing.T) {
		t.Parallel()
		cr := &provisioningv1.ChannelRules{
			PublicChannels:  []string{"*.trade", "*.market"},
			DefaultChannels: []string{"news", "alerts"},
			GroupMappings: map[string]*provisioningv1.GroupChannels{
				"admin":  {Channels: []string{"admin.*"}},
				"trader": {Channels: []string{"trade.*", "order.*"}},
			},
		}

		rules := protoToChannelRules(cr)

		if len(rules.Public) != 2 {
			t.Errorf("Public len = %d, want 2", len(rules.Public))
		}
		if len(rules.Default) != 2 {
			t.Errorf("Default len = %d, want 2", len(rules.Default))
		}
		if len(rules.GroupMappings) != 2 {
			t.Errorf("GroupMappings len = %d, want 2", len(rules.GroupMappings))
		}
	})

	t.Run("empty group mappings", func(t *testing.T) {
		t.Parallel()
		cr := &provisioningv1.ChannelRules{
			PublicChannels: []string{"*.trade"},
		}

		rules := protoToChannelRules(cr)

		if rules.GroupMappings != nil {
			t.Error("GroupMappings should be nil when empty")
		}
	})
}

// =============================================================================
// Routing snapshot tests
// =============================================================================

func TestGetRoutingSnapshot_NotFound(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	_, ok := r.GetRoutingSnapshot("nonexistent")
	if ok {
		t.Error("GetRoutingSnapshot should return ok=false for unknown tenant")
	}
}

func TestGetRoutingSnapshot_FoundAfterUpdate(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{
				TenantSlug: "acme",
				RoutingRules: []*provisioningv1.TopicRoutingRule{
					{Pattern: "acme.*.trade", Topics: []string{"trades"}, Priority: 1},
				},
			},
		},
	})

	snap, ok := r.GetRoutingSnapshot("acme")
	if !ok {
		t.Fatal("GetRoutingSnapshot should return ok=true after updateTenantConfigs")
	}
	if len(snap.Rules) != 1 {
		t.Errorf("Rules len = %d, want 1", len(snap.Rules))
	}
	// NormalizePattern converts bare * to ** before storage.
	if snap.Rules[0].Pattern != "acme.**.trade" {
		t.Errorf("Rules[0].Pattern = %q, want %q", snap.Rules[0].Pattern, "acme.**.trade")
	}
}

func TestSnapshotReplace_COWDoesNotMutatePrior(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{{
			TenantSlug: "acme",
			RoutingRules: []*provisioningv1.TopicRoutingRule{
				{Pattern: "acme.*.trade", Topics: []string{"trades"}, Priority: 1},
			},
		}},
	})

	// Capture the snapshot before the next replace.
	before, _ := r.GetRoutingSnapshot("acme")

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{{
			TenantSlug: "acme",
			RoutingRules: []*provisioningv1.TopicRoutingRule{
				{Pattern: "acme.*.orders", Topics: []string{"orders"}, Priority: 1},
			},
		}},
	})

	after, _ := r.GetRoutingSnapshot("acme")

	// The previously handed-out snapshot must not have been mutated in place.
	if len(before.Rules) != 1 || before.Rules[0].Topics[0] != "trades" {
		t.Errorf("before.Rules = %+v, want the original trades rule (COW must not mutate the old map)", before.Rules)
	}
	if len(after.Rules) != 1 || after.Rules[0].Topics[0] != "orders" {
		t.Errorf("after.Rules = %+v, want the replaced orders rule", after.Rules)
	}
}

func TestGetRoutingSnapshot_ConcurrentReadsDontRace(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "acme"}},
	})

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			r.GetRoutingSnapshot("acme")
			r.GetRoutingSnapshot("missing")
		}()
	}
	wg.Wait()
}

func TestUpdateTenantConfigs_ConcurrentWithReads_NoRace(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "acme"}},
	})

	var wg sync.WaitGroup

	// Writer — the COW path that snapshotMu serializes.
	wg.Go(func() {
		for i := range 50 {
			topic := "trades"
			if i%2 == 1 {
				topic = "orders"
			}
			r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
				IsSnapshot: true,
				Tenants: []*provisioningv1.TenantConfig{{
					TenantSlug: "acme",
					RoutingRules: []*provisioningv1.TopicRoutingRule{
						{Pattern: "acme.*.trade", Topics: []string{topic}, Priority: 1},
					},
				}},
			})
		}
	})

	// Readers.
	for range 10 {
		wg.Go(func() {
			for range 50 {
				r.GetRoutingSnapshot("acme")
			}
		})
	}

	wg.Wait()
}

func TestSnapshotReplace_RoutingSnapshotsTrimmedForRemovedTenants(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()

	// Initial snapshot with two tenants.
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "acme"},
			{TenantSlug: "globex"},
		},
	})

	// New snapshot with only one tenant.
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "globex"}},
	})

	_, acmeOk := r.GetRoutingSnapshot("acme")
	if acmeOk {
		t.Error("acme snapshot should be removed after new full snapshot")
	}

	_, globexOk := r.GetRoutingSnapshot("globex")
	if !globexOk {
		t.Error("globex snapshot should remain")
	}
}

// =============================================================================
// SnapshotReceived (readiness gating)
// =============================================================================

func TestChannelRulesProvider_SnapshotReceived(t *testing.T) {
	t.Parallel()

	t.Run("false at construction", func(t *testing.T) {
		t.Parallel()
		r := newTestChannelRulesProvider()
		if r.SnapshotReceived() {
			t.Error("SnapshotReceived must be false before any snapshot is applied")
		}
	})

	t.Run("delta update does not flip it", func(t *testing.T) {
		t.Parallel()
		r := newTestChannelRulesProvider()
		r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
			IsSnapshot: false,
			Tenants: []*provisioningv1.TenantConfig{
				{TenantSlug: "t1", ChannelRules: &provisioningv1.ChannelRules{PublicChannels: []string{"*"}}},
			},
		})
		if r.SnapshotReceived() {
			t.Error("a delta update must NOT mark the snapshot as received")
		}
	})

	t.Run("true only after snapshot applied", func(t *testing.T) {
		t.Parallel()
		r := newTestChannelRulesProvider()
		r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
			IsSnapshot: true,
			Tenants: []*provisioningv1.TenantConfig{
				{TenantSlug: "t1", ChannelRules: &provisioningv1.ChannelRules{PublicChannels: []string{"*"}}},
			},
		})
		if !r.SnapshotReceived() {
			t.Error("SnapshotReceived must be true after the snapshot is applied")
		}
	})

	t.Run("empty snapshot still counts as applied", func(t *testing.T) {
		t.Parallel()
		// A deployment with zero tenants is a valid, fully-applied snapshot —
		// tenants are simply "none configured" (deny-all, healthy), not unknown.
		r := newTestChannelRulesProvider()
		r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{IsSnapshot: true})
		if !r.SnapshotReceived() {
			t.Error("an empty snapshot must still mark the provider ready")
		}
	})
}

// TestResolveTenantSlug_ReverseResolution covers the UUID->slug reverse map (B0):
// a known UUID resolves to its slug, an unknown UUID fails closed.
func TestResolveTenantSlug_ReverseResolution(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "acme", TenantUuid: "uuid-acme"},
			{TenantSlug: "beta", TenantUuid: "uuid-beta"},
		},
	})

	got, err := r.ResolveTenantSlug(context.Background(), "uuid-acme")
	if err != nil {
		t.Fatalf("ResolveTenantSlug(uuid-acme) error = %v", err)
	}
	if got != "acme" {
		t.Errorf("ResolveTenantSlug(uuid-acme) = %q, want %q", got, "acme")
	}
	if _, err := r.ResolveTenantSlug(context.Background(), "uuid-unknown"); !errors.Is(err, auth.ErrTenantNotResolvable) {
		t.Errorf("ResolveTenantSlug(unknown) error = %v, want ErrTenantNotResolvable", err)
	}
}

// TestResolveTenantSlug_RenameHoldReturnsCurrentNotPrevious asserts the reverse
// map returns the CURRENT slug during a rename hold window, never the previous
// alias — while the forward map still resolves both slugs (JWT binding).
func TestResolveTenantSlug_RenameHoldReturnsCurrentNotPrevious(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "acme-v2", TenantUuid: "uuid-acme", PreviousSlug: "acme"},
		},
	})

	// Forward: both current and previous slug resolve to the UUID.
	if uuid, _ := r.ResolveTenantUUID(context.Background(), "acme"); uuid != "uuid-acme" {
		t.Errorf("ResolveTenantUUID(previous slug) = %q, want uuid-acme", uuid)
	}
	if uuid, _ := r.ResolveTenantUUID(context.Background(), "acme-v2"); uuid != "uuid-acme" {
		t.Errorf("ResolveTenantUUID(current slug) = %q, want uuid-acme", uuid)
	}

	// Reverse: the UUID resolves to the current slug only.
	slug, err := r.ResolveTenantSlug(context.Background(), "uuid-acme")
	if err != nil {
		t.Fatalf("ResolveTenantSlug error = %v", err)
	}
	if slug != "acme-v2" {
		t.Errorf("ResolveTenantSlug during rename hold = %q, want current slug %q (never previous)", slug, "acme-v2")
	}
}

// TestResolveTenantSlug_DeltaRemovalPrunesReverseEntry asserts a delta removal
// (keyed by slug) prunes the UUID-keyed reverse entry — no stale uuid->slug that
// would mis-scope a later API-key auth.
func TestResolveTenantSlug_DeltaRemovalPrunesReverseEntry(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants: []*provisioningv1.TenantConfig{
			{TenantSlug: "doomed", TenantUuid: "uuid-doomed"},
		},
	})
	if _, err := r.ResolveTenantSlug(context.Background(), "uuid-doomed"); err != nil {
		t.Fatalf("precondition: ResolveTenantSlug(uuid-doomed) error = %v", err)
	}

	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot:         false,
		RemovedTenantSlugs: []string{"doomed"},
	})
	if _, err := r.ResolveTenantSlug(context.Background(), "uuid-doomed"); !errors.Is(err, auth.ErrTenantNotResolvable) {
		t.Errorf("after removal: ResolveTenantSlug(uuid-doomed) error = %v, want ErrTenantNotResolvable (no stale slug)", err)
	}
}

// TestResolveTenantSlug_ConcurrentWithUpdates_NoRace exercises the reverse map
// under concurrent stream updates and reads (Constitution §VII / -race).
func TestResolveTenantSlug_ConcurrentWithUpdates_NoRace(t *testing.T) {
	t.Parallel()
	r := newTestChannelRulesProvider()
	r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "acme", TenantUuid: "uuid-acme"}},
	})

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			r.updateTenantConfigs(&provisioningv1.WatchTenantConfigResponse{
				IsSnapshot: true,
				Tenants:    []*provisioningv1.TenantConfig{{TenantSlug: "acme", TenantUuid: "uuid-acme"}},
			})
		}
	})
	wg.Go(func() {
		for range 100 {
			_, _ = r.ResolveTenantSlug(context.Background(), "uuid-acme")
		}
	})
	wg.Wait()
}
