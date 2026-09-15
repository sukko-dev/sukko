package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

func TestNoopChannelRulesProvider_GetChannelRules(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	provider := NewNoopChannelRulesProvider(logger)

	rules, err := provider.GetChannelRules(context.Background(), "test-tenant")

	if !errors.Is(err, types.ErrChannelRulesNotFound) {
		t.Errorf("expected ErrChannelRulesNotFound, got %v", err)
	}
	if rules != nil {
		t.Errorf("expected nil rules, got %+v", rules)
	}
}

func TestNoopChannelRulesProvider_Close(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	provider := NewNoopChannelRulesProvider(logger)

	err := provider.Close()

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestNoopChannelRulesProvider_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ChannelRulesProvider = (*NoopChannelRulesProvider)(nil)
}

// mockChannelRulesProvider is a test helper that implements ChannelRulesProvider.
// mu guards channelRules so concurrency tests can mutate rules while readers
// run under -race.
type mockChannelRulesProvider struct {
	mu           sync.RWMutex
	channelRules map[string]*types.ChannelRules
	// snapshotDone defaults true: a missing tenant reads as "none configured"
	// (deny-all, healthy). Set false to simulate the cold-start window.
	snapshotDone atomic.Bool
	// disconnected simulates a downed stream (rules unknown for uncached tenants).
	disconnected atomic.Bool
}

func newMockChannelRulesProvider() *mockChannelRulesProvider {
	m := &mockChannelRulesProvider{
		channelRules: make(map[string]*types.ChannelRules),
	}
	m.snapshotDone.Store(true)
	return m
}

func (m *mockChannelRulesProvider) setRules(tenantID string, rules *types.ChannelRules) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channelRules[tenantID] = rules
}

func (m *mockChannelRulesProvider) GetChannelRules(_ context.Context, tenantID string) (*types.ChannelRules, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if rules, ok := m.channelRules[tenantID]; ok {
		return rules, nil
	}
	return nil, types.ErrChannelRulesNotFound
}

func (m *mockChannelRulesProvider) SnapshotReceived() bool {
	return m.snapshotDone.Load()
}

func (m *mockChannelRulesProvider) State() int32 {
	if m.disconnected.Load() {
		return provapi.StreamStateDisconnected
	}
	return provapi.StreamStateConnected
}

func (m *mockChannelRulesProvider) Close() error {
	return nil
}

var _ ChannelRulesProvider = (*mockChannelRulesProvider)(nil)

func TestMockChannelRulesProvider_GetChannelRules(t *testing.T) {
	t.Parallel()

	provider := newMockChannelRulesProvider()
	provider.channelRules["acme"] = &types.ChannelRules{
		Public: []string{"*.public"},
		GroupMappings: map[string][]string{
			"traders": {"*.trade", "*.liquidity"},
		},
	}

	tests := []struct {
		name     string
		tenantID string
		wantErr  error
	}{
		{
			name:     "found",
			tenantID: "acme",
			wantErr:  nil,
		},
		{
			name:     "not found",
			tenantID: "unknown",
			wantErr:  types.ErrChannelRulesNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules, err := provider.GetChannelRules(context.Background(), tt.tenantID)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}
			if tt.wantErr == nil && rules == nil {
				t.Error("expected rules, got nil")
			}
		})
	}
}
