package provapi

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
)

// newTestTopicRegistry creates a minimal StreamTopicRegistry for unit testing
// without gRPC connections or Prometheus metrics.
func newTestTopicRegistry() *StreamTopicRegistry {
	return &StreamTopicRegistry{
		namespace: "test",
		logger:    zerolog.Nop(),
	}
}

func TestTopicRegistry_Snapshot(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.trade", "test.tenant-a.order"},
		DedicatedTenants: []*provisioningv1.DedicatedTenant{
			{
				TenantSlug: "tenant-b",
				Topics:     []string{"test.tenant-b.trade"},
			},
		},
	})

	ctx := context.Background()

	t.Run("GetSharedTenantTopics", func(t *testing.T) {
		t.Parallel()
		topics, err := r.GetSharedTenantTopics(ctx, "test")
		if err != nil {
			t.Fatalf("GetSharedTenantTopics() error = %v", err)
		}
		if len(topics) != 2 {
			t.Fatalf("len(topics) = %d, want 2", len(topics))
		}
		if topics[0] != "test.tenant-a.trade" {
			t.Errorf("topics[0] = %q, want %q", topics[0], "test.tenant-a.trade")
		}
	})

	t.Run("GetSharedTenantTopics returns copy", func(t *testing.T) {
		t.Parallel()
		topics1, _ := r.GetSharedTenantTopics(ctx, "test")
		topics2, _ := r.GetSharedTenantTopics(ctx, "test")

		// Modify the first copy
		if len(topics1) > 0 {
			topics1[0] = "modified"
		}

		// Second copy should be unaffected
		if len(topics2) > 0 && topics2[0] == "modified" {
			t.Error("GetSharedTenantTopics should return a copy, not a reference")
		}
	})

	t.Run("GetDedicatedTenants", func(t *testing.T) {
		t.Parallel()
		tenants, err := r.GetDedicatedTenants(ctx, "test")
		if err != nil {
			t.Fatalf("GetDedicatedTenants() error = %v", err)
		}
		if len(tenants) != 1 {
			t.Fatalf("len(tenants) = %d, want 1", len(tenants))
		}
		if tenants[0].TenantID != "tenant-b" {
			t.Errorf("TenantID = %q, want %q", tenants[0].TenantID, "tenant-b")
		}
		if len(tenants[0].Topics) != 1 {
			t.Errorf("len(topics) = %d, want 1", len(tenants[0].Topics))
		}
	})

	t.Run("GetDedicatedTenants returns copy", func(t *testing.T) {
		t.Parallel()
		tenants1, _ := r.GetDedicatedTenants(ctx, "test")
		tenants2, _ := r.GetDedicatedTenants(ctx, "test")

		if len(tenants1) > 0 {
			tenants1[0].TenantID = "modified"
		}

		if len(tenants2) > 0 && tenants2[0].TenantID == "modified" {
			t.Error("GetDedicatedTenants should return a copy, not a reference")
		}
	})
}

func TestTopicRegistry_SnapshotReplacesAll(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	// First snapshot
	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.trade"},
	})

	// Second snapshot replaces
	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-b.order"},
	})

	ctx := context.Background()

	topics, err := r.GetSharedTenantTopics(ctx, "test")
	if err != nil {
		t.Fatalf("GetSharedTenantTopics() error = %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("len(topics) = %d, want 1", len(topics))
	}
	if topics[0] != "test.tenant-b.order" {
		t.Errorf("topics[0] = %q, want %q", topics[0], "test.tenant-b.order")
	}
}

func TestTopicRegistry_OnUpdateCallback(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	var callCount atomic.Int32
	r.SetOnUpdate(func() {
		callCount.Add(1)
	})

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.trade"},
	})

	if count := callCount.Load(); count != 1 {
		t.Errorf("onUpdate called %d times, want 1", count)
	}

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.order"},
	})

	if count := callCount.Load(); count != 2 {
		t.Errorf("onUpdate called %d times, want 2", count)
	}
}

func TestTopicRegistry_EmptySnapshot(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot: true,
	})

	ctx := context.Background()

	topics, err := r.GetSharedTenantTopics(ctx, "test")
	if err != nil {
		t.Fatalf("GetSharedTenantTopics() error = %v", err)
	}
	if len(topics) != 0 {
		t.Errorf("expected empty topics, got %d", len(topics))
	}

	tenants, err := r.GetDedicatedTenants(ctx, "test")
	if err != nil {
		t.Fatalf("GetDedicatedTenants() error = %v", err)
	}
	if len(tenants) != 0 {
		t.Errorf("expected empty tenants, got %d", len(tenants))
	}
}

func TestTopicRegistry_State(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	if state := r.State(); state != StreamStateDisconnected {
		t.Errorf("initial state = %d, want %d", state, StreamStateDisconnected)
	}

	r.streamState.Store(StreamStateConnected)
	if state := r.State(); state != StreamStateConnected {
		t.Errorf("state after Store = %d, want %d", state, StreamStateConnected)
	}
}

// TestTopicRegistry_SnapshotReceived guards the readiness gate (#179 P3): false until the first
// snapshot is applied, true afterwards. A non-snapshot (incremental) update MUST NOT flip it.
func TestTopicRegistry_SnapshotReceived(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	if r.SnapshotReceived() {
		t.Fatal("SnapshotReceived must be false before any snapshot is applied")
	}

	// An incremental (non-snapshot) update alone MUST NOT mark the registry ready.
	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   false,
		SharedTopics: []string{"test.tenant-a.trade"},
	})
	if r.SnapshotReceived() {
		t.Error("SnapshotReceived must stay false after a non-snapshot update")
	}

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.trade"},
	})
	if !r.SnapshotReceived() {
		t.Error("SnapshotReceived must be true after the snapshot is applied")
	}
}

// TestTopicRegistry_TopicTenants guards the authoritative topic→tenant map (#179 P3): shared topics
// carry their tenant via shared_topic_tenants, dedicated topics via dedicated_tenants. TopicTenants
// returns a copy.
func TestTopicRegistry_TopicTenants(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.trade"},
		SharedTopicTenants: []*provisioningv1.SharedTopic{
			{TenantSlug: "tenant-a", Topic: "test.tenant-a.trade"},
		},
		DedicatedTenants: []*provisioningv1.DedicatedTenant{
			{TenantSlug: "tenant-b", Topics: []string{"test.tenant-b.trade"}},
		},
	})

	got, err := r.TopicTenants(context.Background(), "test")
	if err != nil {
		t.Fatalf("TopicTenants() error = %v", err)
	}
	want := map[string]string{
		"test.tenant-a.trade": "tenant-a",
		"test.tenant-b.trade": "tenant-b",
	}
	if len(got) != len(want) {
		t.Fatalf("TopicTenants len = %d, want %d (%v)", len(got), len(want), got)
	}
	for topic, tenant := range want {
		if got[topic] != tenant {
			t.Errorf("TopicTenants[%q] = %q, want %q", topic, got[topic], tenant)
		}
	}

	// Mutating the returned map MUST NOT affect the registry's internal state.
	got["test.tenant-a.trade"] = "mutated"
	again, _ := r.TopicTenants(context.Background(), "test")
	if again["test.tenant-a.trade"] != "tenant-a" {
		t.Error("TopicTenants must return a copy, not the internal map")
	}
}

// TestTopicRegistry_CreateOnlyTopics pins the ADR-0018 split at the registry
// boundary: create_only_topics (routing-rule egress topics) are exposed for
// topic CREATION only and never leak into the shared or dedicated consume sets.
func TestTopicRegistry_CreateOnlyTopics(t *testing.T) {
	t.Parallel()
	r := newTestTopicRegistry()

	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:       true,
		SharedTopics:     []string{"test.tenant-a.default", "test.tenant-a.trades"},
		CreateOnlyTopics: []string{"test.tenant-a.audit"},
	})

	got := r.CreateOnlyTopics()
	if len(got) != 1 || got[0] != "test.tenant-a.audit" {
		t.Fatalf("CreateOnlyTopics() = %v, want [test.tenant-a.audit]", got)
	}

	shared, err := r.GetSharedTenantTopics(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetSharedTenantTopics: %v", err)
	}
	for _, topic := range shared {
		if topic == "test.tenant-a.audit" {
			t.Error("egress topic leaked into the shared consume set")
		}
	}

	// Returned slice is a copy — mutating it must not corrupt the cache.
	got[0] = "mutated"
	if again := r.CreateOnlyTopics(); again[0] != "test.tenant-a.audit" {
		t.Error("CreateOnlyTopics() must return a defensive copy")
	}

	// A subsequent snapshot replaces the set.
	r.updateTopics(&provisioningv1.WatchTopicsResponse{
		IsSnapshot:   true,
		SharedTopics: []string{"test.tenant-a.default"},
	})
	if again := r.CreateOnlyTopics(); len(again) != 0 {
		t.Errorf("CreateOnlyTopics() after replacing snapshot = %v, want empty", again)
	}
}
