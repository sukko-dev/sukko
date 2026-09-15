package orchestration

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// =============================================================================
// Mock ShardMetrics Implementation for Testing
// =============================================================================

// mockShard implements ShardMetrics interface for testing
type mockShard struct {
	currentConns int64
	maxConns     int
	addr         string
}

func (m *mockShard) GetCurrentConnections() int64 { return m.currentConns }
func (m *mockShard) GetMaxConnections() int       { return m.maxConns }
func (m *mockShard) GetAddr() string              { return m.addr }

// newMockShards creates mock shards from connection counts
func newMockShards(configs []struct{ current, max int64 }) []ShardMetrics {
	shards := make([]ShardMetrics, len(configs))
	for i, cfg := range configs {
		shards[i] = &mockShard{
			currentConns: cfg.current,
			maxConns:     int(cfg.max),
			addr:         "localhost:300" + string(rune('0'+i)),
		}
	}
	return shards
}

// =============================================================================
// LoadBalancerConfig Tests
// =============================================================================

func TestLoadBalancerConfig_Fields(t *testing.T) {
	t.Parallel()
	cfg := LoadBalancerConfig{
		Addr:             ":3000",
		Shards:           []*Shard{},
		Logger:           zerolog.Nop(),
		HTTPReadTimeout:  15 * time.Second,
		HTTPWriteTimeout: 15 * time.Second,
		HTTPIdleTimeout:  60 * time.Second,
	}

	if cfg.Addr != ":3000" {
		t.Errorf("Addr: got %s, want :3000", cfg.Addr)
	}
	if cfg.HTTPReadTimeout != 15*time.Second {
		t.Errorf("HTTPReadTimeout: got %v, want 15s", cfg.HTTPReadTimeout)
	}
	if cfg.HTTPWriteTimeout != 15*time.Second {
		t.Errorf("HTTPWriteTimeout: got %v, want 15s", cfg.HTTPWriteTimeout)
	}
	if cfg.HTTPIdleTimeout != 60*time.Second {
		t.Errorf("HTTPIdleTimeout: got %v, want 60s", cfg.HTTPIdleTimeout)
	}
}

func TestLoadBalancerConfig_NoShards(t *testing.T) {
	t.Parallel()
	cfg := LoadBalancerConfig{
		Addr:   ":3000",
		Shards: []*Shard{}, // Empty shards
		Logger: zerolog.Nop(),
	}

	_, err := NewLoadBalancer(cfg)
	if err == nil {
		t.Error("NewLoadBalancer should return error with no shards")
	}
}

func TestLoadBalancerConfig_NilShards(t *testing.T) {
	t.Parallel()
	cfg := LoadBalancerConfig{
		Addr:   ":3000",
		Shards: nil, // Nil shards
		Logger: zerolog.Nop(),
	}

	_, err := NewLoadBalancer(cfg)
	if err == nil {
		t.Error("NewLoadBalancer should return error with nil shards")
	}
}

// =============================================================================
// SelectShardByMetrics Tests (Using ShardMetrics Interface)
// =============================================================================

func TestSelectShardByMetrics_LeastConnections(t *testing.T) {
	t.Parallel()
	shards := newMockShards([]struct{ current, max int64 }{
		{50, 100},
		{30, 100}, // Least connections
		{70, 100},
	})

	idx := SelectShardByMetrics(shards)

	if idx != 1 {
		t.Errorf("Should select shard 1 (least connections), got %d", idx)
	}
}

func TestSelectShardByMetrics_AllFull(t *testing.T) {
	t.Parallel()
	shards := newMockShards([]struct{ current, max int64 }{
		{100, 100}, // Full
		{100, 100}, // Full
		{100, 100}, // Full
	})

	idx := SelectShardByMetrics(shards)

	if idx != -1 {
		t.Errorf("Should return -1 when all shards full, got %d", idx)
	}
}

func TestSelectShardByMetrics_SingleShard(t *testing.T) {
	t.Parallel()
	shards := newMockShards([]struct{ current, max int64 }{
		{25, 100},
	})

	idx := SelectShardByMetrics(shards)

	if idx != 0 {
		t.Errorf("Should select shard 0, got %d", idx)
	}
}

func TestSelectShardByMetrics_EvenDistribution(t *testing.T) {
	t.Parallel()
	// All shards have same connections
	shards := newMockShards([]struct{ current, max int64 }{
		{50, 100},
		{50, 100},
		{50, 100},
	})

	idx := SelectShardByMetrics(shards)

	// Should pick first shard when tied
	if idx != 0 {
		t.Errorf("Should select first shard when tied, got %d", idx)
	}
}

func TestSelectShardByMetrics_RespectsCapacity(t *testing.T) {
	t.Parallel()
	shards := newMockShards([]struct{ current, max int64 }{
		{10, 100}, // Available
		{50, 50},  // Full (50/50)
		{20, 100}, // Available
	})

	idx := SelectShardByMetrics(shards)

	// Should skip shard 1 (full) and pick shard 0 (fewest)
	if idx != 0 {
		t.Errorf("Should select shard 0 (skip full shard), got %d", idx)
	}
}

func TestSelectShardByMetrics_EmptyShards(t *testing.T) {
	t.Parallel()
	idx := SelectShardByMetrics([]ShardMetrics{})

	if idx != -1 {
		t.Errorf("Should return -1 for empty shard list, got %d", idx)
	}
}

func TestSelectShardByMetrics_NilShards(t *testing.T) {
	t.Parallel()
	idx := SelectShardByMetrics(nil)

	if idx != -1 {
		t.Errorf("Should return -1 for nil shard list, got %d", idx)
	}
}

func TestSelectShardByMetrics_PartialCapacity(t *testing.T) {
	t.Parallel()
	// First two shards full, third has room
	shards := newMockShards([]struct{ current, max int64 }{
		{100, 100}, // Full
		{100, 100}, // Full
		{50, 100},  // Available
	})

	idx := SelectShardByMetrics(shards)

	if idx != 2 {
		t.Errorf("Should select shard 2 (only available), got %d", idx)
	}
}

// =============================================================================
// Health Status Calculation Tests
// =============================================================================

func TestHealthStatus_Calculation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		totalConns      int64
		totalMaxConns   int64
		allShardsOK     bool
		expectedStatus  string
		expectedHealthy bool
	}{
		{"healthy_low_usage", 10, 100, true, "healthy", true},
		{"healthy_medium_usage", 50, 100, true, "healthy", true},
		{"degraded_high_usage", 95, 100, true, "degraded", true},
		{"unhealthy_over_capacity", 110, 100, true, "unhealthy", false},
		{"unhealthy_shard_error", 50, 100, false, "unhealthy", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Simulate health calculation from handleHealth
			var capacityPercent float64
			if tt.totalMaxConns > 0 {
				capacityPercent = float64(tt.totalConns) / float64(tt.totalMaxConns) * 100
			}

			isHealthy := tt.allShardsOK && tt.totalConns <= tt.totalMaxConns
			status := "healthy"

			if !isHealthy {
				status = "unhealthy"
			} else if capacityPercent > 90 {
				status = "degraded"
			}

			if status != tt.expectedStatus {
				t.Errorf("status: got %s, want %s", status, tt.expectedStatus)
			}
			if isHealthy != tt.expectedHealthy {
				t.Errorf("healthy: got %v, want %v", isHealthy, tt.expectedHealthy)
			}
		})
	}
}

func TestCapacityPercent_Calculation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		current    int64
		max        int64
		expectedPC float64
	}{
		{"zero_of_hundred", 0, 100, 0.0},
		{"fifty_of_hundred", 50, 100, 50.0},
		{"hundred_of_hundred", 100, 100, 100.0},
		{"zero_max", 50, 0, 0.0}, // Edge case: avoid div by zero
		{"partial", 25, 200, 12.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var capacityPercent float64
			if tt.max > 0 {
				capacityPercent = float64(tt.current) / float64(tt.max) * 100
			}

			if capacityPercent != tt.expectedPC {
				t.Errorf("capacity percent: got %.2f, want %.2f", capacityPercent, tt.expectedPC)
			}
		})
	}
}

// =============================================================================
// Shard Status Calculation Tests
// =============================================================================

func TestShardStatus_Calculation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		current        int64
		max            int64
		expectedStatus string
	}{
		{"available", 50, 100, "available"},
		{"medium", 80, 100, "medium"},
		{"high", 95, 100, "high"},
		{"full", 100, 100, "full"},
		{"over_capacity", 110, 100, "full"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Simulate shard status calculation
			utilization := 0.0
			if tt.max > 0 {
				utilization = (float64(tt.current) / float64(tt.max)) * 100
			}

			var shardStatus string
			switch {
			case tt.current >= tt.max:
				shardStatus = "full"
			case utilization > 90:
				shardStatus = "high"
			case utilization > 75:
				shardStatus = "medium"
			default:
				shardStatus = "available"
			}

			if shardStatus != tt.expectedStatus {
				t.Errorf("shard status: got %s, want %s", shardStatus, tt.expectedStatus)
			}
		})
	}
}
