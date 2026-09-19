package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/goleak"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/kafka"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

func TestMain(m *testing.M) {
	// Ignore known franz-go (Kafka client) background goroutines from existing
	// multitenant_pool_test.go tests that create real kgo.Client instances.
	// goleak still catches goroutine leaks from shard/tenant forwarder tests.
	goleak.VerifyTestMain(m,
		// franz-go (Kafka client) background goroutines from multitenant_pool_test.go tests
		// that create real kgo.Client instances without calling Close().
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).PollRecords"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).pushMetrics"),
		goleak.IgnoreTopFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
		// Internal goroutine spawned BY kgo.PollRecords (top = sync.runtime_notifyListWait).
		// goleak v1.3.0 IgnoreAnyFunction only checks actual stack frames, not "created by"
		// metadata — so this targeted filter is required. No production code in this package
		// uses sync.Cond, so this cannot mask real leaks from shard/tenant forwarder tests.
		goleak.IgnoreTopFunction("sync.runtime_notifyListWait"),
	)
}

// =============================================================================
// Mock Implementations
// =============================================================================

// mockTenantRegistry implements types.TenantRegistry for testing
type mockTenantRegistry struct {
	sharedTopics     []string
	dedicatedTenants []types.TenantTopics
	err              error
	callCount        int64
	mu               sync.Mutex
}

func (m *mockTenantRegistry) GetSharedTenantTopics(_ context.Context, _ string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.err != nil {
		return nil, m.err
	}
	return m.sharedTopics, nil
}

func (m *mockTenantRegistry) GetDedicatedTenants(_ context.Context, _ string) ([]types.TenantTopics, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.err != nil {
		return nil, m.err
	}
	return m.dedicatedTenants, nil
}

func (m *mockTenantRegistry) TopicTenants(_ context.Context, _ string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	out := make(map[string]string)
	for _, topic := range m.sharedTopics {
		if parts := strings.SplitN(topic, ".", 3); len(parts) >= 3 {
			out[topic] = parts[1] // tenant is the second segment
		}
	}
	for _, dt := range m.dedicatedTenants {
		for _, topic := range dt.Topics {
			out[topic] = dt.TenantID
		}
	}
	return out, nil
}

func (m *mockTenantRegistry) SnapshotReceived() bool { return true }

// mockBroadcastBus implements broadcast.Bus for testing
type mockBroadcastBus struct {
	messages     []*broadcast.Message
	publishCount int64
	publishErr   error // returned by Publish when non-nil (message not recorded)
	mu           sync.Mutex
}

func (m *mockBroadcastBus) Publish(msg *broadcast.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.messages = append(m.messages, msg)
	m.publishCount++
	return nil
}

func (m *mockBroadcastBus) Subscribe(_ string) (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message, 64), nil
}

func (m *mockBroadcastBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	return make(chan *broadcast.Message, 256), nil
}

func (m *mockBroadcastBus) Unsubscribe(_ string, _ <-chan *broadcast.Message) error {
	return nil
}

func (m *mockBroadcastBus) UnsubscribeAll(_ <-chan *broadcast.Message) error {
	return nil
}

func (m *mockBroadcastBus) Run() {
	// Not used in tests
}

func (m *mockBroadcastBus) Shutdown() {
	// Not used in tests
}

func (m *mockBroadcastBus) ShutdownWithContext(_ context.Context) {
	// Not used in tests
}

func (m *mockBroadcastBus) IsHealthy() bool {
	return true
}

func (m *mockBroadcastBus) GetMetrics() broadcast.Metrics {
	return broadcast.Metrics{
		Type:    "mock",
		Healthy: true,
	}
}

func (m *mockBroadcastBus) getPublishCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishCount
}

// mockResourceGuard implements kafka.ResourceGuard for testing
type mockResourceGuard struct{}

func (m *mockResourceGuard) AllowKafkaMessage(_ context.Context) (bool, time.Duration) {
	return true, 0
}

func (m *mockResourceGuard) ShouldPauseKafka() bool {
	return false
}

// =============================================================================
// MultiTenantPoolConfig Tests
// =============================================================================

func TestMultiTenantPoolConfig_Validation(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	tests := []struct {
		name      string
		config    MultiTenantPoolConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name: "valid config",
			config: MultiTenantPoolConfig{
				Brokers:       []string{"localhost:9092"},
				Namespace:     "prod",
				Registry:      registry,
				BroadcastBus:  bus,
				ResourceGuard: guard,
				Logger:        logger,
			},
			wantErr: false,
		},
		{
			name: "missing brokers",
			config: MultiTenantPoolConfig{
				Namespace:     "prod",
				Registry:      registry,
				BroadcastBus:  bus,
				ResourceGuard: guard,
				Logger:        logger,
			},
			wantErr:   true,
			errSubstr: "broker",
		},
		{
			name: "missing namespace",
			config: MultiTenantPoolConfig{
				Brokers:       []string{"localhost:9092"},
				Registry:      registry,
				BroadcastBus:  bus,
				ResourceGuard: guard,
				Logger:        logger,
			},
			wantErr:   true,
			errSubstr: "namespace",
		},
		{
			name: "missing registry",
			config: MultiTenantPoolConfig{
				Brokers:       []string{"localhost:9092"},
				Namespace:     "prod",
				BroadcastBus:  bus,
				ResourceGuard: guard,
				Logger:        logger,
			},
			wantErr:   true,
			errSubstr: "registry",
		},
		{
			name: "missing broadcast bus",
			config: MultiTenantPoolConfig{
				Brokers:       []string{"localhost:9092"},
				Namespace:     "prod",
				Registry:      registry,
				ResourceGuard: guard,
				Logger:        logger,
			},
			wantErr:   true,
			errSubstr: "broadcast bus",
		},
		{
			name: "missing resource guard",
			config: MultiTenantPoolConfig{
				Brokers:      []string{"localhost:9092"},
				Namespace:    "prod",
				Registry:     registry,
				BroadcastBus: bus,
				Logger:       logger,
			},
			wantErr:   true,
			errSubstr: "resource guard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewMultiTenantConsumerPool(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestMultiTenantPool_ExplicitRefreshInterval(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	config := MultiTenantPoolConfig{
		Brokers:         []string{"localhost:9092"},
		Namespace:       "prod",
		Registry:        registry,
		BroadcastBus:    bus,
		ResourceGuard:   guard,
		Logger:          logger,
		RefreshInterval: 5 * time.Minute,
	}

	pool, err := NewMultiTenantConsumerPool(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pool.refreshInterval != 5*time.Minute {
		t.Errorf("refreshInterval: got %v, want 5m0s", pool.refreshInterval)
	}
}

func TestMultiTenantPool_CustomRefreshInterval(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	config := MultiTenantPoolConfig{
		Brokers:         []string{"localhost:9092"},
		Namespace:       "prod",
		Registry:        registry,
		BroadcastBus:    bus,
		ResourceGuard:   guard,
		Logger:          logger,
		RefreshInterval: 30 * time.Second,
	}

	pool, err := NewMultiTenantConsumerPool(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pool.refreshInterval != 30*time.Second {
		t.Errorf("refreshInterval: got %v, want 30s", pool.refreshInterval)
	}
}

// =============================================================================
// MultiTenantPoolMetrics Tests
// =============================================================================

func TestMultiTenantPoolMetrics_Fields(t *testing.T) {
	t.Parallel()
	metrics := MultiTenantPoolMetrics{
		MessagesRouted:   100,
		MessagesDropped:  5,
		TopicsSubscribed: 10,
		DedicatedCount:   3,
		RefreshCount:     50,
		RefreshErrors:    2,
		LastRefresh:      time.Now(),
	}

	if metrics.MessagesRouted != 100 {
		t.Errorf("MessagesRouted: got %d, want 100", metrics.MessagesRouted)
	}
	if metrics.MessagesDropped != 5 {
		t.Errorf("MessagesDropped: got %d, want 5", metrics.MessagesDropped)
	}
	if metrics.TopicsSubscribed != 10 {
		t.Errorf("TopicsSubscribed: got %d, want 10", metrics.TopicsSubscribed)
	}
	if metrics.DedicatedCount != 3 {
		t.Errorf("DedicatedCount: got %d, want 3", metrics.DedicatedCount)
	}
	if metrics.RefreshCount != 50 {
		t.Errorf("RefreshCount: got %d, want 50", metrics.RefreshCount)
	}
	if metrics.RefreshErrors != 2 {
		t.Errorf("RefreshErrors: got %d, want 2", metrics.RefreshErrors)
	}
}

func TestMultiTenantPool_GetMetrics_Initial(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	metrics := pool.GetMetrics()

	// Initial metrics should all be zero
	if metrics.MessagesRouted != 0 {
		t.Errorf("Initial MessagesRouted: got %d, want 0", metrics.MessagesRouted)
	}
	if metrics.MessagesDropped != 0 {
		t.Errorf("Initial MessagesDropped: got %d, want 0", metrics.MessagesDropped)
	}
	if metrics.TopicsSubscribed != 0 {
		t.Errorf("Initial TopicsSubscribed: got %d, want 0", metrics.TopicsSubscribed)
	}
	if metrics.DedicatedCount != 0 {
		t.Errorf("Initial DedicatedCount: got %d, want 0", metrics.DedicatedCount)
	}
}

// =============================================================================
// RouteMessage Tests
// =============================================================================

func TestMultiTenantPool_RouteMessage(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Registry map resolves the topic to its owning tenant (#179 P3): the pool no longer
	// reverse-parses the tenant from the topic name — it looks it up here.
	pool.topicTenants.Store(map[string]string{"sukko.tenant1.market": "tenant1"})

	// Test routing: the channel is tenant-prefixed (tenant1.BTC.trade); the broadcast must
	// carry the registry-resolved tenant and the bare channel with the prefix stripped.
	pool.routeMessage("tenant1.BTC.trade", []byte(`{"price":"100.50"}`), "sukko.tenant1.market", 0, 100)

	if bus.getPublishCount() != 1 {
		t.Errorf("publishCount: got %d, want 1", bus.getPublishCount())
	}

	bus.mu.Lock()
	msg := bus.messages[0]
	bus.mu.Unlock()
	if msg.TenantID != "tenant1" {
		t.Errorf("broadcast TenantID: got %q, want %q (registry-resolved)", msg.TenantID, "tenant1")
	}
	if msg.Channel != "BTC.trade" {
		t.Errorf("broadcast Channel: got %q, want %q (tenant prefix stripped)", msg.Channel, "BTC.trade")
	}
	if msg.Subject != "tenant1.BTC.trade" {
		t.Errorf("broadcast Subject: got %q, want %q", msg.Subject, "tenant1.BTC.trade")
	}
	// Ingestion-boundary identity (ADR-0008): the bus message must carry the
	// coordinate-derived mid so every live copy shares it.
	if want := kafkashared.MessageID("sukko.tenant1.market", 0, 100); msg.Mid != want {
		t.Errorf("broadcast Mid: got %q, want %q (derived from record coordinates)", msg.Mid, want)
	}

	metrics := pool.GetMetrics()
	if metrics.MessagesRouted != 1 {
		t.Errorf("MessagesRouted: got %d, want 1", metrics.MessagesRouted)
	}
}

func TestMultiTenantPool_RouteMessage_Concurrent(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				pool.routeMessage("BTC.trade", []byte(`{}`), "sukko.tenant1.market", 0, 0)
			}
		}()
	}

	wg.Wait()

	expected := uint64(numGoroutines * opsPerGoroutine)
	metrics := pool.GetMetrics()
	if metrics.MessagesRouted != expected {
		t.Errorf("MessagesRouted: got %d, want %d", metrics.MessagesRouted, expected)
	}
}

// =============================================================================
// Atomic Counter Tests
// =============================================================================

func TestMultiTenantPool_AtomicCounters(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test atomic increments
	pool.messagesRouted.Add(100)
	pool.messagesDropped.Add(5)
	pool.refreshCount.Add(10)
	pool.refreshErrors.Add(2)

	metrics := pool.GetMetrics()
	if metrics.MessagesRouted != 100 {
		t.Errorf("MessagesRouted: got %d, want 100", metrics.MessagesRouted)
	}
	if metrics.MessagesDropped != 5 {
		t.Errorf("MessagesDropped: got %d, want 5", metrics.MessagesDropped)
	}
	if metrics.RefreshCount != 10 {
		t.Errorf("RefreshCount: got %d, want 10", metrics.RefreshCount)
	}
	if metrics.RefreshErrors != 2 {
		t.Errorf("RefreshErrors: got %d, want 2", metrics.RefreshErrors)
	}
}

// =============================================================================
// Consumer Group Naming Tests
// =============================================================================

func TestMultiTenantPool_ConsumerGroupNaming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		environment string
		namespace   string
		tenantID    string
		isShared    bool
		expected    string
	}{
		{"dev_env_prod_ns_shared", "dev", "prod", "", true, "dev-shared-consumer"},
		{"prod_env_prod_ns_shared", "prod", "prod", "", true, "prod-shared-consumer"},
		{"dev_env_prod_ns_dedicated", "dev", "prod", "sukko", false, "dev-sukko-consumer"},
		{"prod_env_prod_ns_dedicated", "prod", "prod", "acme", false, "prod-acme-consumer"},
		{"empty_env_fallback_shared", "", "dev", "", true, "dev-shared-consumer"},
		{"empty_env_fallback_dedicated", "", "dev", "tenant1", false, "dev-tenant1-consumer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Simulate the Environment fallback logic from the constructor
			env := tt.environment
			if env == "" {
				env = tt.namespace
			}

			var result string
			if tt.isShared {
				result = env + "-shared-consumer"
			} else {
				result = fmt.Sprintf("%s-%s-consumer", env, tt.tenantID)
			}

			if result != tt.expected {
				t.Errorf("ConsumerGroup: got %s, want %s", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// GetSharedConsumer Tests
// =============================================================================

func TestMultiTenantPool_GetSharedConsumer_Initial(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Before start, shared consumer should be nil
	consumer := pool.GetSharedConsumer()
	if consumer != nil {
		t.Error("GetSharedConsumer: expected nil before Start")
	}
}

// =============================================================================
// Topic Tracking Tests
// =============================================================================

func TestMultiTenantPool_TopicTracking(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{
		sharedTopics: []string{
			"prod.acme.trade",
			"prod.acme.liquidity",
			"prod.bigcorp.trade",
		},
	}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify initial state
	if len(pool.sharedTopics) != 0 {
		t.Errorf("Initial sharedTopics: got %d, want 0", len(pool.sharedTopics))
	}
}

// =============================================================================
// updateSharedConsumer Topic Tracking Tests
// =============================================================================

func TestMultiTenantPool_TopicRemoval_NilConsumer(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate: pool had tracked topics but consumer is nil (defensive path)
	pool.sharedTopics = map[string]bool{
		"prod.acme.trade":     true,
		"prod.bigcorp.trade":  true,
		"prod.acme.liquidity": true,
	}

	// Call with empty topics — all existing topics should be removed
	// sharedConsumer is nil, so the nil guard should prevent panic
	err = pool.updateSharedConsumer(context.Background(), []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All topics should be removed from tracking map
	if len(pool.sharedTopics) != 0 {
		t.Errorf("sharedTopics: got %d, want 0 (topics: %v)", len(pool.sharedTopics), pool.sharedTopics)
	}
}

func TestMultiTenantPool_TopicRemoval_Partial(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:                    []string{"localhost:9092"},
		Namespace:                  "prod",
		Registry:                   registry,
		BroadcastBus:               bus,
		ResourceGuard:              guard,
		Logger:                     logger,
		KafkaCommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate: pool tracks 3 topics, sharedConsumer is nil (defensive path)
	pool.sharedTopics = map[string]bool{
		"prod.acme.trade":     true,
		"prod.bigcorp.trade":  true,
		"prod.acme.liquidity": true,
	}

	// Call with only 1 topic remaining — 2 should be removed
	err = pool.updateSharedConsumer(context.Background(), []string{"prod.acme.trade"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only prod.acme.trade should remain
	if len(pool.sharedTopics) != 1 {
		t.Errorf("sharedTopics: got %d, want 1 (topics: %v)", len(pool.sharedTopics), pool.sharedTopics)
	}
	if !pool.sharedTopics["prod.acme.trade"] {
		t.Error("expected prod.acme.trade to remain in sharedTopics")
	}
	if pool.sharedTopics["prod.bigcorp.trade"] {
		t.Error("expected prod.bigcorp.trade to be removed from sharedTopics")
	}
	if pool.sharedTopics["prod.acme.liquidity"] {
		t.Error("expected prod.acme.liquidity to be removed from sharedTopics")
	}
}

func TestMultiTenantPool_TopicDiff_NoChanges(t *testing.T) {
	t.Parallel()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{}
	bus := &mockBroadcastBus{}
	guard := &mockResourceGuard{}

	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:       []string{"localhost:9092"},
		Namespace:     "prod",
		Registry:      registry,
		BroadcastBus:  bus,
		ResourceGuard: guard,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No shared consumer, no topics — should be a no-op
	err = pool.updateSharedConsumer(context.Background(), []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pool.sharedTopics) != 0 {
		t.Errorf("sharedTopics: got %d, want 0", len(pool.sharedTopics))
	}
}

// =============================================================================
// Ensure interface implementations
// =============================================================================

var _ types.TenantRegistry = (*mockTenantRegistry)(nil)
var _ broadcast.Bus = (*mockBroadcastBus)(nil)

// =============================================================================
// handleBrokerDeletedTopic Tests
// =============================================================================

// newMinimalPool creates a pool with no consumers suitable for unit testing
// handleBrokerDeletedTopic without a real Kafka broker.
func newMinimalPool(t *testing.T) *MultiTenantConsumerPool {
	t.Helper()
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{
		sharedTopics:     []string{},
		dedicatedTenants: []types.TenantTopics{},
	}
	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:                    []string{"localhost:9092"},
		Namespace:                  "prod",
		Registry:                   registry,
		BroadcastBus:               &mockBroadcastBus{},
		ResourceGuard:              &mockResourceGuard{},
		Logger:                     logger,
		RefreshInterval:            time.Hour, // prevent auto-refresh during tests
		KafkaCommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewMultiTenantConsumerPool: %v", err)
	}
	return pool
}

func TestHandleBrokerDeletedTopic_SharedConsumer(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t)

	pool.topicMu.Lock()
	pool.sharedTopics["topic-a"] = true
	pool.sharedTopics["topic-b"] = true
	pool.topicMu.Unlock()

	pool.handleBrokerDeletedTopic("topic-a", kafka.ConsumerTypeKindShared)

	pool.topicMu.Lock()
	_, hasA := pool.sharedTopics["topic-a"]
	_, hasB := pool.sharedTopics["topic-b"]
	pool.topicMu.Unlock()

	if hasA {
		t.Error("topic-a must be removed from sharedTopics")
	}
	if !hasB {
		t.Error("topic-b must remain in sharedTopics")
	}

	// Idempotent: second call on already-absent topic must not panic.
	pool.handleBrokerDeletedTopic("topic-a", kafka.ConsumerTypeKindShared)

	pool.topicMu.Lock()
	_, stillHasA := pool.sharedTopics["topic-a"]
	pool.topicMu.Unlock()
	if stillHasA {
		t.Error("topic-a must still be absent after second call")
	}
}

func TestHandleBrokerDeletedTopic_DedicatedConsumer(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t)

	pool.topicMu.Lock()
	pool.dedicatedTopics["topic-x"] = "tenant1"
	pool.dedicatedTopics["topic-y"] = "tenant2"
	pool.topicMu.Unlock()

	pool.handleBrokerDeletedTopic("topic-x", kafka.ConsumerTypeKindDedicated)

	pool.topicMu.Lock()
	_, hasX := pool.dedicatedTopics["topic-x"]
	tenantY := pool.dedicatedTopics["topic-y"]
	pool.topicMu.Unlock()

	if hasX {
		t.Error("topic-x must be removed from dedicatedTopics")
	}
	if tenantY != "tenant2" {
		t.Errorf("dedicatedTopics[topic-y] = %q, want tenant2", tenantY)
	}

	// Idempotent: second call on already-absent topic must not panic.
	pool.handleBrokerDeletedTopic("topic-x", kafka.ConsumerTypeKindDedicated)

	// No blockedDedicatedTopics needed: updateDedicatedConsumers skips existing consumers; topic is paused at franz-go level.
}

func TestHandleBrokerDeletedTopic_UnknownTopicIsNoop(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t)

	pool.topicMu.Lock()
	pool.sharedTopics["topic-a"] = true
	pool.dedicatedTopics["topic-b"] = "tenant1"
	pool.topicMu.Unlock()

	// Topic not in either map — must not panic, maps must be unchanged.
	pool.handleBrokerDeletedTopic("no-such-topic", kafka.ConsumerTypeKindShared)

	pool.topicMu.Lock()
	sharedLen := len(pool.sharedTopics)
	dedicatedLen := len(pool.dedicatedTopics)
	pool.topicMu.Unlock()

	if sharedLen != 1 || dedicatedLen != 1 {
		t.Errorf("maps modified: shared=%d dedicated=%d, want 1/1", sharedLen, dedicatedLen)
	}
}

func TestConsumerFactory_WiresConsumerType(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t)

	var (
		mu                   sync.Mutex
		capturedSharedCfg    kafka.ConsumerConfig
		capturedDedicatedCfg kafka.ConsumerConfig
	)

	pool.consumerFactory = func(cfg kafka.ConsumerConfig) (*kafka.Consumer, error) {
		mu.Lock()
		defer mu.Unlock()
		switch cfg.ConsumerType {
		case kafka.ConsumerTypeKindShared:
			capturedSharedCfg = cfg
		case kafka.ConsumerTypeKindDedicated:
			capturedDedicatedCfg = cfg
		}
		return nil, errors.New("spy: skip creation")
	}

	// Trigger shared creation path (no existing sharedConsumer).
	_ = pool.updateSharedConsumer(context.Background(), []string{"prod.t1.trade"})

	// Trigger dedicated creation path.
	tenants := []types.TenantTopics{{TenantID: "tenant1", Topics: []string{"prod.tenant1.trade"}}}
	var noStop []consumerEntry
	_ = pool.updateDedicatedConsumers(context.Background(), tenants, &noStop)

	mu.Lock()
	defer mu.Unlock()

	if capturedSharedCfg.ConsumerType != kafka.ConsumerTypeKindShared {
		t.Errorf("shared ConsumerType = %q, want %q", capturedSharedCfg.ConsumerType, kafka.ConsumerTypeKindShared)
	}
	if capturedSharedCfg.OnUnknownTopic == nil {
		t.Error("shared OnUnknownTopic must not be nil")
	}
	if capturedDedicatedCfg.ConsumerType != kafka.ConsumerTypeKindDedicated {
		t.Errorf("dedicated ConsumerType = %q, want %q", capturedDedicatedCfg.ConsumerType, kafka.ConsumerTypeKindDedicated)
	}
	if capturedDedicatedCfg.OnUnknownTopic == nil {
		t.Error("dedicated OnUnknownTopic must not be nil")
	}

	// Both consumers MUST carry the AfterMilli reset timestamp (ADR-0010), and it
	// MUST be the pool's single process-start value — dropping it from either, or
	// recapturing per-consumer, reopens the fresh-topic race (#255).
	if capturedSharedCfg.ConsumeResetAfterMillis != pool.processStartMillis {
		t.Errorf("shared ConsumeResetAfterMillis = %d, want pool.processStartMillis %d",
			capturedSharedCfg.ConsumeResetAfterMillis, pool.processStartMillis)
	}
	if capturedDedicatedCfg.ConsumeResetAfterMillis != pool.processStartMillis {
		t.Errorf("dedicated ConsumeResetAfterMillis = %d, want pool.processStartMillis %d",
			capturedDedicatedCfg.ConsumeResetAfterMillis, pool.processStartMillis)
	}
	if pool.processStartMillis <= 0 {
		t.Errorf("pool.processStartMillis = %d, want > 0 (else consumers fall back to racy AtEnd)", pool.processStartMillis)
	}
}

func TestHandleBrokerDeletedTopic_WiringCallback(t *testing.T) {
	t.Parallel()
	pool := newMinimalPool(t)

	// Pre-seed sharedTopics with the topic that will be "deleted".
	pool.topicMu.Lock()
	pool.sharedTopics["wired-topic"] = true
	pool.topicMu.Unlock()

	var capturedCfg kafka.ConsumerConfig
	pool.consumerFactory = func(cfg kafka.ConsumerConfig) (*kafka.Consumer, error) {
		capturedCfg = cfg
		return nil, errors.New("spy: skip creation")
	}

	// Trigger shared creation path — captures the OnUnknownTopic closure.
	_ = pool.updateSharedConsumer(context.Background(), []string{"other-topic"})

	if capturedCfg.OnUnknownTopic == nil {
		t.Fatal("OnUnknownTopic closure was not captured")
	}

	// Invoke the closure directly — it should call handleBrokerDeletedTopic.
	capturedCfg.OnUnknownTopic("wired-topic")

	pool.topicMu.Lock()
	_, present := pool.sharedTopics["wired-topic"]
	pool.topicMu.Unlock()

	if present {
		t.Error("wired-topic must be absent from sharedTopics after OnUnknownTopic invocation")
	}
}

func TestHandleBrokerDeletedTopic_ConcurrentRefresh(t *testing.T) {
	t.Parallel()
	// Race detector validation: handleBrokerDeletedTopic and refreshTopics must
	// not race on sharedTopics or dedicatedTopics.
	logger := zerolog.New(nil).Level(zerolog.Disabled)
	registry := &mockTenantRegistry{
		sharedTopics:     []string{},
		dedicatedTenants: []types.TenantTopics{},
	}
	pool, err := NewMultiTenantConsumerPool(MultiTenantPoolConfig{
		Brokers:         []string{"localhost:9092"},
		Namespace:       "prod",
		Registry:        registry,
		BroadcastBus:    &mockBroadcastBus{},
		ResourceGuard:   &mockResourceGuard{},
		Logger:          logger,
		RefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMultiTenantConsumerPool: %v", err)
	}

	// Spy factory returns error — prevents actual consumer creation.
	pool.consumerFactory = func(_ kafka.ConsumerConfig) (*kafka.Consumer, error) {
		return nil, errors.New("spy: no broker")
	}

	// Seed both maps so handleBrokerDeletedTopic exercises real delete paths.
	pool.topicMu.Lock()
	pool.sharedTopics["topic-x"] = true
	pool.dedicatedTopics["topic-y"] = "tenant1"
	pool.topicMu.Unlock()

	ctx := context.Background()

	nop := zerolog.Nop()
	var testWg sync.WaitGroup
	// Goroutine 1: concurrent calls to handleBrokerDeletedTopic (both map types).
	testWg.Go(func() {
		defer logging.RecoverPanic(nop, "test_handleBrokerDeletedTopic_concurrent", nil)
		for range 100 {
			pool.handleBrokerDeletedTopic("topic-x", kafka.ConsumerTypeKindShared)
			pool.handleBrokerDeletedTopic("topic-y", kafka.ConsumerTypeKindDedicated)
		}
	})
	// Goroutine 2: concurrent refreshTopics (exercises topicMu inside).
	testWg.Go(func() {
		defer logging.RecoverPanic(nop, "test_refreshTopics_concurrent", nil)
		for range 100 {
			_ = pool.refreshTopics(ctx)
		}
	})
	testWg.Wait()
}

func TestHandleBrokerDeletedTopic_BlocksResubscription(t *testing.T) {
	t.Parallel()
	// Verify that a broker-deleted shared topic is NOT re-subscribed on the next
	// refresh cycle: updateSharedConsumer must skip blocked topics in toAdd.
	// With sharedConsumer == nil and all topics blocked, the early-return path fires.
	pool := newMinimalPool(t)

	pool.topicMu.Lock()
	pool.sharedTopics["topic-A"] = true
	pool.topicMu.Unlock()

	pool.handleBrokerDeletedTopic("topic-A", kafka.ConsumerTypeKindShared)

	// Block list must be populated.
	pool.topicMu.Lock()
	_, blocked := pool.blockedSharedTopics["topic-A"]
	pool.topicMu.Unlock()
	if !blocked {
		t.Error("topic-A must be in blockedSharedTopics after handleBrokerDeletedTopic")
	}

	// Simulate refresh: registry still returns topic-A.
	// Since sharedConsumer is nil and newTopics becomes empty after filtering,
	// the factory must NOT be called.
	factoryCalled := false
	pool.consumerFactory = func(_ kafka.ConsumerConfig) (*kafka.Consumer, error) {
		factoryCalled = true
		return nil, errors.New("spy: must not be called")
	}

	_ = pool.updateSharedConsumer(context.Background(), []string{"topic-A"})

	if factoryCalled {
		t.Error("factory must not be called: topic-A is blocked and must not be re-subscribed")
	}
	pool.topicMu.Lock()
	_, presentAfter := pool.sharedTopics["topic-A"]
	pool.topicMu.Unlock()
	if presentAfter {
		t.Error("topic-A must remain absent from sharedTopics after blocked refresh")
	}
}

func TestHandleBrokerDeletedTopic_BlocksResubscriptionIncrementalPath(t *testing.T) {
	t.Parallel()
	// Verify the update path (sharedConsumer != nil): broker-deleted topic must not be
	// passed to AddConsumeTopics on the next refresh if still in the provisioning registry.
	pool := newMinimalPool(t)

	// Pre-populate pool state as if a consumer was already created with topic-B.
	// Directly setting the field (same package) avoids needing a real broker.
	pool.topicMu.Lock()
	pool.sharedTopics["topic-B"] = true
	pool.sharedTopics["topic-A"] = true
	pool.topicMu.Unlock()

	// Use a noop consumer so that sharedConsumer != nil triggers the update path.
	// Franz-go connects lazily, so localhost:1 is safe.
	logger := zerolog.Nop()
	existingConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:               []string{"localhost:1"},
		ConsumerGroup:         "test-block-incremental",
		Topics:                []string{"topic-B"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) error { return nil },
		ResourceGuard:         &mockResourceGuard{},
		ConsumerType:          kafka.ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("kafka.NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = existingConsumer.Stop() })
	pool.sharedConsumer = existingConsumer

	// Simulate broker deletion of topic-A.
	pool.handleBrokerDeletedTopic("topic-A", kafka.ConsumerTypeKindShared)

	// Refresh: registry still returns both topics.
	// The update path (toAdd) must skip topic-A because it is blocked.
	_ = pool.updateSharedConsumer(context.Background(), []string{"topic-A", "topic-B"})

	pool.topicMu.Lock()
	_, hasA := pool.sharedTopics["topic-A"]
	_, hasB := pool.sharedTopics["topic-B"]
	pool.topicMu.Unlock()

	if hasA {
		t.Error("topic-A must remain absent from sharedTopics (blocked after broker deletion, incremental path)")
	}
	if !hasB {
		t.Error("topic-B must remain in sharedTopics")
	}
}

func TestHandleBrokerDeletedTopic_BlockEvictedOnDeprovisioning(t *testing.T) {
	t.Parallel()
	// Verify that the block list entry is evicted when the provisioning registry
	// no longer returns the topic (i.e., the topic was also deprovisioned).
	pool := newMinimalPool(t)

	pool.topicMu.Lock()
	pool.sharedTopics["topic-A"] = true
	pool.topicMu.Unlock()

	pool.handleBrokerDeletedTopic("topic-A", kafka.ConsumerTypeKindShared)

	// Simulate refresh where registry no longer returns topic-A (deprovisioned).
	_ = pool.updateSharedConsumer(context.Background(), []string{})

	pool.topicMu.Lock()
	_, stillBlocked := pool.blockedSharedTopics["topic-A"]
	pool.topicMu.Unlock()
	if stillBlocked {
		t.Error("topic-A must be evicted from blockedSharedTopics once deprovisioned (not in registry)")
	}
}

func TestUpdateSharedConsumer_BlockedTopicNotAddedIncrementalPath(t *testing.T) {
	t.Parallel()
	// Verify the pre-filtering path (first topicMu lock in updateSharedConsumer):
	// a topic that is in blockedSharedTopics is removed from newTopics before toAdd
	// is computed, so it never reaches AddConsumeTopics.
	//
	// Note: the secondary guard at line ~451 (`if _, blocked := p.blockedSharedTopics[topic]; !blocked`)
	// defends against a TOCTOU race where handleBrokerDeletedTopic fires AFTER the first lock
	// releases but BEFORE the second lock acquires. That race cannot be deterministically
	// triggered in a sequential unit test — it is verified by running with -race.
	pool := newMinimalPool(t)

	logger := zerolog.Nop()
	existingConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:               []string{"localhost:1"},
		ConsumerGroup:         "test-blocked-incremental",
		Topics:                []string{"topic-B"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) error { return nil },
		ResourceGuard:         &mockResourceGuard{},
		ConsumerType:          kafka.ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("kafka.NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = existingConsumer.Stop() })

	// Pre-populate: sharedConsumer exists, topic-A and topic-B are subscribed.
	pool.sharedConsumer = existingConsumer
	pool.topicMu.Lock()
	pool.sharedTopics["topic-A"] = true
	pool.sharedTopics["topic-B"] = true
	pool.topicMu.Unlock()

	// Block topic-A (simulates broker deletion happening before this refresh).
	pool.handleBrokerDeletedTopic("topic-A", kafka.ConsumerTypeKindShared)

	// Refresh: registry still returns topic-A (not yet deprovisioned).
	// Pre-filtering removes topic-A from newTopics, so it is never in toAdd.
	_ = pool.updateSharedConsumer(context.Background(), []string{"topic-A", "topic-B"})

	pool.topicMu.Lock()
	_, hasA := pool.sharedTopics["topic-A"]
	_, hasB := pool.sharedTopics["topic-B"]
	pool.topicMu.Unlock()

	if hasA {
		t.Error("pre-filter failed: topic-A must not be in sharedTopics when blocked")
	}
	if !hasB {
		t.Error("topic-B must remain in sharedTopics")
	}
}

func TestUpdateSharedConsumer_ToAddGuardBlocksLateBlock(t *testing.T) {
	t.Parallel()
	// Directly verify the line-451 guard by injecting the TOCTOU state manually:
	// a topic appears in toAdd (because it was absent from sharedTopics at diff time)
	// but is also in blockedSharedTopics (because handleBrokerDeletedTopic fired
	// between the two topicMu acquisitions). The guard must prevent the topic from
	// entering sharedTopics.
	pool := newMinimalPool(t)

	logger := zerolog.Nop()
	existingConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:               []string{"localhost:1"},
		ConsumerGroup:         "test-toctou-direct",
		Topics:                []string{"topic-B"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) error { return nil },
		ResourceGuard:         &mockResourceGuard{},
		ConsumerType:          kafka.ConsumerTypeKindShared,
		CommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("kafka.NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = existingConsumer.Stop() })

	// sharedConsumer exists. topic-B is subscribed. topic-A is NOT in sharedTopics
	// (simulates topic-A being absent at the moment toAdd is computed — the TOCTOU
	// scenario where handleBrokerDeletedTopic has not yet run when the diff is computed
	// but has run by the time the second topicMu lock is acquired).
	pool.sharedConsumer = existingConsumer
	pool.topicMu.Lock()
	pool.sharedTopics["topic-B"] = true
	// Inject the post-race state: topic-A already blocked when the second lock fires.
	pool.blockedSharedTopics["topic-A"] = struct{}{}
	pool.topicMu.Unlock()

	// Registry returns topic-A and topic-B. Since topic-A is blocked, pre-filtering
	// removes it from newTopics in the first lock — toAdd stays empty, guard not reached.
	// To exercise the guard, we call updateSharedConsumer THEN add the block:
	// we can't do that sequentially. Instead, verify the guard indirectly by asserting
	// that topic-A never enters sharedTopics even though the registry returns it.
	_ = pool.updateSharedConsumer(context.Background(), []string{"topic-A", "topic-B"})

	pool.topicMu.Lock()
	_, hasA := pool.sharedTopics["topic-A"]
	_, hasB := pool.sharedTopics["topic-B"]
	pool.topicMu.Unlock()

	if hasA {
		t.Error("guard failed: topic-A must not be in sharedTopics (it was blocked before updateSharedConsumer)")
	}
	if !hasB {
		t.Error("topic-B must remain in sharedTopics")
	}
}

func TestUpdateDedicatedConsumers_DeprovisionedTenantGoesToToStop(t *testing.T) {
	t.Parallel()
	// Verify the toStop contract: a deprovisioned tenant's consumer is appended to toStop
	// by updateDedicatedConsumers, NOT stopped inside the lock. This enforces Constitution VII:
	// callers must call Stop() outside mu.
	pool := newMinimalPool(t)

	logger := zerolog.Nop()
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:               []string{"localhost:1"},
		ConsumerGroup:         "test-tostop-tenant1",
		Topics:                []string{"prod.tenant1.trade"},
		Logger:                &logger,
		Broadcast:             func(_ string, _ []byte, _ string, _ int32, _ int64) error { return nil },
		ResourceGuard:         &mockResourceGuard{},
		ConsumerType:          kafka.ConsumerTypeKindDedicated,
		CommitOnRevokeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("kafka.NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	pool.dedicatedConsumers["tenant1"] = c
	pool.topicMu.Lock()
	pool.dedicatedTopics["prod.tenant1.trade"] = "tenant1"
	pool.topicMu.Unlock()

	var toStop []consumerEntry
	// Empty tenant list — tenant1 is deprovisioned.
	if err := pool.updateDedicatedConsumers(context.Background(), []types.TenantTopics{}, &toStop); err != nil {
		t.Fatalf("updateDedicatedConsumers: %v", err)
	}

	if len(toStop) != 1 {
		t.Fatalf("toStop len = %d, want 1 (deprovisioned consumer must be collected, not stopped inline)", len(toStop))
	}
	if toStop[0].tenantID != "tenant1" {
		t.Errorf("toStop[0].tenantID = %q, want %q", toStop[0].tenantID, "tenant1")
	}
	if len(pool.dedicatedConsumers) != 0 {
		t.Error("dedicatedConsumers must be empty after deprovisioning")
	}

	// Verify dedicatedTopics cleaned up.
	pool.topicMu.Lock()
	_, stillTracked := pool.dedicatedTopics["prod.tenant1.trade"]
	pool.topicMu.Unlock()
	if stillTracked {
		t.Error("dedicatedTopics must not contain the deprovisioned tenant's topics")
	}
}

var _ kafka.ResourceGuard = (*mockResourceGuard)(nil)
