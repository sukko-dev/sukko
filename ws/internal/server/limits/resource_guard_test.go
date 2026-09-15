package limits

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// mockSystemMonitor allows controlled CPU values for testing
type mockSystemMonitor struct {
	cpuPercent         float64
	smoothedCPUPercent float64
}

func (m *mockSystemMonitor) GetCPUPercent() float64 {
	return m.cpuPercent
}

func (m *mockSystemMonitor) GetSmoothedCPUPercent() float64 {
	if m.smoothedCPUPercent != 0 {
		return m.smoothedCPUPercent
	}
	return m.cpuPercent
}

func (m *mockSystemMonitor) GetMemoryBytes() int64 {
	return 100 * 1024 * 1024 // 100MB - always under limit
}

func (m *mockSystemMonitor) GetGoroutines() int {
	return 100 // Always under limit
}

func (m *mockSystemMonitor) GetCPUAllocation() float64 {
	return 1.0
}

func (m *mockSystemMonitor) GetMetrics() metrics.SystemMetrics {
	return metrics.SystemMetrics{
		CPUPercent:  m.cpuPercent,
		MemoryBytes: 100 * 1024 * 1024,
		MemoryMB:    100,
		Goroutines:  100,
	}
}

func TestCPURejectHysteresis(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialState   bool    // isRejectingCPU initial value
		currentCPU     float64 // CPU percentage to test
		upperThreshold float64 // CPURejectThreshold
		lowerThreshold float64 // CPURejectThresholdLower
		expectedAccept bool    // whether connection should be accepted
		expectedState  bool    // expected isRejectingCPU after call
	}{
		{
			name:           "accepting, CPU below upper - stay accepting",
			initialState:   false,
			currentCPU:     70.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: true,
			expectedState:  false,
		},
		{
			name:           "accepting, CPU at upper - stay accepting (not exceeded)",
			initialState:   false,
			currentCPU:     75.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: true,
			expectedState:  false,
		},
		{
			name:           "accepting, CPU above upper - start rejecting",
			initialState:   false,
			currentCPU:     80.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: false,
			expectedState:  true,
		},
		{
			name:           "rejecting, CPU in deadband - stay rejecting",
			initialState:   true,
			currentCPU:     70.0, // Between 65 and 75
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: false,
			expectedState:  true,
		},
		{
			name:           "rejecting, CPU at lower - stay rejecting (not below)",
			initialState:   true,
			currentCPU:     65.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: false,
			expectedState:  true,
		},
		{
			name:           "rejecting, CPU below lower - stop rejecting",
			initialState:   true,
			currentCPU:     60.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: true,
			expectedState:  false,
		},
		{
			name:           "rejecting, CPU still high above upper - stay rejecting",
			initialState:   true,
			currentCPU:     90.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: false,
			expectedState:  true,
		},
		{
			name:           "accepting, CPU very low - stay accepting",
			initialState:   false,
			currentCPU:     10.0,
			upperThreshold: 75.0,
			lowerThreshold: 65.0,
			expectedAccept: true,
			expectedState:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create mock system monitor with controlled CPU value
			mock := &mockSystemMonitor{cpuPercent: tt.currentCPU}

			var connCount atomic.Int64
			logger := logging.NewLogger(logging.LoggerConfig{
				Level:       logging.LogLevelError, // Quiet for tests
				Format:      logging.LogFormatJSON,
				ServiceName: "ws-server",
			})

			config := ResourceGuardConfig{
				MaxConnections:           1000, // High limit, won't trigger
				MemoryLimit:              1024 * 1024 * 1024,
				MaxGoroutines:            10000,
				CPURejectThreshold:       tt.upperThreshold,
				CPURejectThresholdLower:  tt.lowerThreshold,
				CPUPauseThreshold:        90.0, // Won't affect this test
				CPUPauseThresholdLower:   80.0,
				MaxKafkaMessagesPerSec:   1000,
				MaxBroadcastsPerSec:      100,
				RateLimitBurstMultiplier: 2,
			}

			// Use the new constructor with mock monitor
			rg := NewResourceGuardWithMonitor(config, logger, &connCount, mock)

			// Set initial hysteresis state
			rg.isRejectingCPU.Store(tt.initialState)

			// Call ShouldAcceptConnection and verify behavior
			accept, _ := rg.ShouldAcceptConnection()

			if accept != tt.expectedAccept {
				t.Errorf("ShouldAcceptConnection() = %v, want %v", accept, tt.expectedAccept)
			}

			if rg.isRejectingCPU.Load() != tt.expectedState {
				t.Errorf("Final isRejectingCPU state = %v, want %v",
					rg.isRejectingCPU.Load(), tt.expectedState)
			}
		})
	}
}

func TestCPUPauseKafkaHysteresis(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		initialState   bool    // isPausingKafka initial value
		currentCPU     float64 // CPU percentage to test
		upperThreshold float64 // CPUPauseThreshold
		lowerThreshold float64 // CPUPauseThresholdLower
		expectedPause  bool    // whether Kafka should be paused
		expectedState  bool    // expected isPausingKafka after call
	}{
		{
			name:           "running, CPU below upper - stay running",
			initialState:   false,
			currentCPU:     70.0,
			upperThreshold: 80.0,
			lowerThreshold: 70.0,
			expectedPause:  false,
			expectedState:  false,
		},
		{
			name:           "running, CPU above upper - start pausing",
			initialState:   false,
			currentCPU:     85.0,
			upperThreshold: 80.0,
			lowerThreshold: 70.0,
			expectedPause:  true,
			expectedState:  true,
		},
		{
			name:           "pausing, CPU in deadband - stay pausing",
			initialState:   true,
			currentCPU:     75.0, // Between 70 and 80
			upperThreshold: 80.0,
			lowerThreshold: 70.0,
			expectedPause:  true,
			expectedState:  true,
		},
		{
			name:           "pausing, CPU below lower - resume",
			initialState:   true,
			currentCPU:     65.0,
			upperThreshold: 80.0,
			lowerThreshold: 70.0,
			expectedPause:  false,
			expectedState:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create mock system monitor with controlled CPU value
			mock := &mockSystemMonitor{cpuPercent: tt.currentCPU}

			var connCount atomic.Int64
			logger := logging.NewLogger(logging.LoggerConfig{
				Level:       logging.LogLevelError, // Quiet for tests
				Format:      logging.LogFormatJSON,
				ServiceName: "ws-server",
			})

			config := ResourceGuardConfig{
				MaxConnections:           1000,
				MemoryLimit:              1024 * 1024 * 1024,
				MaxGoroutines:            10000,
				CPURejectThreshold:       90.0,
				CPURejectThresholdLower:  80.0,
				CPUPauseThreshold:        tt.upperThreshold,
				CPUPauseThresholdLower:   tt.lowerThreshold,
				MaxKafkaMessagesPerSec:   1000,
				MaxBroadcastsPerSec:      100,
				RateLimitBurstMultiplier: 2,
			}

			// Use the new constructor with mock monitor
			rg := NewResourceGuardWithMonitor(config, logger, &connCount, mock)

			// Set initial hysteresis state
			rg.isPausingKafka.Store(tt.initialState)

			// Call ShouldPauseKafka and verify behavior
			shouldPause := rg.ShouldPauseKafka()

			if shouldPause != tt.expectedPause {
				t.Errorf("ShouldPauseKafka() = %v, want %v", shouldPause, tt.expectedPause)
			}

			if rg.isPausingKafka.Load() != tt.expectedState {
				t.Errorf("Final isPausingKafka state = %v, want %v",
					rg.isPausingKafka.Load(), tt.expectedState)
			}
		})
	}
}

func TestHysteresisStateVisibility(t *testing.T) {
	t.Parallel()
	// Test that GetStats() exposes hysteresis state
	var connCount atomic.Int64
	connCount.Store(5)
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelInfo,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       75.0,
		CPURejectThresholdLower:  65.0,
		CPUPauseThreshold:        80.0,
		CPUPauseThresholdLower:   70.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)

	// Initial state should be false (accepting, not pausing)
	stats := rg.GetStats()

	// Check that hysteresis state fields are present
	if _, ok := stats["cpu_rejecting"]; !ok {
		t.Error("GetStats() missing cpu_rejecting field")
	}
	if _, ok := stats["cpu_pausing_kafka"]; !ok {
		t.Error("GetStats() missing cpu_pausing_kafka field")
	}
	if _, ok := stats["cpu_reject_threshold_lower"]; !ok {
		t.Error("GetStats() missing cpu_reject_threshold_lower field")
	}
	if _, ok := stats["cpu_pause_threshold_lower"]; !ok {
		t.Error("GetStats() missing cpu_pause_threshold_lower field")
	}

	// Verify initial states are false
	if stats["cpu_rejecting"].(bool) != false {
		t.Error("Initial cpu_rejecting should be false")
	}
	if stats["cpu_pausing_kafka"].(bool) != false {
		t.Error("Initial cpu_pausing_kafka should be false")
	}

	// Set states to true and verify they're reflected
	rg.isRejectingCPU.Store(true)
	rg.isPausingKafka.Store(true)

	stats = rg.GetStats()
	if stats["cpu_rejecting"].(bool) != true {
		t.Error("cpu_rejecting should be true after Store(true)")
	}
	if stats["cpu_pausing_kafka"].(bool) != true {
		t.Error("cpu_pausing_kafka should be true after Store(true)")
	}
}

func TestHysteresisInitialState(t *testing.T) {
	t.Parallel()
	// Test that hysteresis starts in accepting/running state (not rejecting/pausing)
	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelInfo,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       75.0,
		CPURejectThresholdLower:  65.0,
		CPUPauseThreshold:        80.0,
		CPUPauseThresholdLower:   70.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)

	// atomic.Bool zero value is false, which means:
	// - isRejectingCPU = false → accepting connections
	// - isPausingKafka = false → consuming Kafka
	if rg.isRejectingCPU.Load() != false {
		t.Error("Initial isRejectingCPU should be false (accepting)")
	}
	if rg.isPausingKafka.Load() != false {
		t.Error("Initial isPausingKafka should be false (consuming)")
	}
}

func TestHysteresisConfigInStats(t *testing.T) {
	t.Parallel()
	// Test that threshold configs are exposed in GetStats()
	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelInfo,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       75.0,
		CPURejectThresholdLower:  65.0,
		CPUPauseThreshold:        80.0,
		CPUPauseThresholdLower:   70.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)
	stats := rg.GetStats()

	// Verify threshold values are correct
	if stats["cpu_reject_threshold"].(float64) != 75.0 {
		t.Errorf("cpu_reject_threshold: got %v, want 75.0", stats["cpu_reject_threshold"])
	}
	if stats["cpu_reject_threshold_lower"].(float64) != 65.0 {
		t.Errorf("cpu_reject_threshold_lower: got %v, want 65.0", stats["cpu_reject_threshold_lower"])
	}
	if stats["cpu_pause_threshold"].(float64) != 80.0 {
		t.Errorf("cpu_pause_threshold: got %v, want 80.0", stats["cpu_pause_threshold"])
	}
	if stats["cpu_pause_threshold_lower"].(float64) != 70.0 {
		t.Errorf("cpu_pause_threshold_lower: got %v, want 70.0", stats["cpu_pause_threshold_lower"])
	}
}

// TestSmoothedCPUUsedOverRaw verifies that ShouldAcceptConnection and
// ShouldPauseKafka use GetSmoothedCPUPercent (not GetCPUPercent) for decisions.
// This is critical: raw CPU spikes to 100% but smoothed stays at 30%.
func TestSmoothedCPUUsedOverRaw(t *testing.T) {
	t.Parallel()

	mock := &mockSystemMonitor{
		cpuPercent:         100.0, // Raw: spike to 100%
		smoothedCPUPercent: 30.0,  // Smoothed: only 30%
	}

	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       75.0,
		CPURejectThresholdLower:  65.0,
		CPUPauseThreshold:        80.0,
		CPUPauseThresholdLower:   70.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuardWithMonitor(config, logger, &connCount, mock)

	// With raw CPU=100% but smoothed=30%, connection should be ACCEPTED
	// (smoothed 30% < reject threshold 75%)
	accept, reason := rg.ShouldAcceptConnection()
	if !accept {
		t.Errorf("Should accept (smoothed 30%% < 75%% threshold), rejected with: %s", reason)
	}

	// Kafka should NOT be paused (smoothed 30% < pause threshold 80%)
	if rg.ShouldPauseKafka() {
		t.Error("Should not pause Kafka (smoothed 30% < 80% threshold)")
	}
}

// =============================================================================
// GoroutineLimiter Tests
// =============================================================================

func TestGoroutineLimiter_AcquireRelease(t *testing.T) {
	t.Parallel()
	limiter := NewGoroutineLimiter(3)

	// Initial state
	if limiter.Current() != 0 {
		t.Errorf("Initial Current() should be 0, got %d", limiter.Current())
	}
	if limiter.Max() != 3 {
		t.Errorf("Max() should be 3, got %d", limiter.Max())
	}

	// Acquire 3 slots
	for i := range 3 {
		if !limiter.Acquire() {
			t.Errorf("Acquire() %d should succeed", i+1)
		}
	}

	if limiter.Current() != 3 {
		t.Errorf("Current() should be 3, got %d", limiter.Current())
	}

	// 4th acquire should fail
	if limiter.Acquire() {
		t.Error("4th Acquire() should fail when at limit")
	}

	// Release one
	limiter.Release()
	if limiter.Current() != 2 {
		t.Errorf("Current() should be 2 after release, got %d", limiter.Current())
	}

	// Now acquire should succeed again
	if !limiter.Acquire() {
		t.Error("Acquire() should succeed after release")
	}
}

func TestGoroutineLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	limiter := NewGoroutineLimiter(100)
	const numGoroutines = 200
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	acquired := make(chan bool, numGoroutines*iterations)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				if limiter.Acquire() {
					acquired <- true
					// Small delay to increase contention
					limiter.Release()
				} else {
					acquired <- false
				}
			}
		}()
	}

	wg.Wait()
	close(acquired)

	// Count successful acquires
	successCount := 0
	for success := range acquired {
		if success {
			successCount++
		}
	}

	// Should have many successful acquires (exact count depends on timing)
	if successCount == 0 {
		t.Error("Should have at least some successful acquires")
	}

	// Final state should be 0 (all released)
	if limiter.Current() != 0 {
		t.Errorf("Final Current() should be 0, got %d", limiter.Current())
	}
}

func TestGoroutineLimiter_ZeroMax(t *testing.T) {
	t.Parallel()
	limiter := NewGoroutineLimiter(0)

	if limiter.Max() != 0 {
		t.Errorf("Max() should be 0, got %d", limiter.Max())
	}

	// Acquire should always fail
	if limiter.Acquire() {
		t.Error("Acquire() should fail when max is 0")
	}
}

// =============================================================================
// Rate Limiter Tests
// =============================================================================

func TestResourceGuard_AllowBroadcast(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError, // Quiet
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       90.0,
		CPURejectThresholdLower:  80.0,
		CPUPauseThreshold:        95.0,
		CPUPauseThresholdLower:   85.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      10, // Low limit for testing
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)

	// Burst capacity is 2x rate = 20
	// First 20 should succeed (burst)
	successCount := 0
	for range 25 {
		if rg.AllowBroadcast() {
			successCount++
		}
	}

	// Should have ~20 successes (burst capacity)
	if successCount < 15 || successCount > 25 {
		t.Errorf("Expected ~20 successes from burst capacity, got %d", successCount)
	}
}

func TestResourceGuard_AllowKafkaMessage(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       90.0,
		CPURejectThresholdLower:  80.0,
		CPUPauseThreshold:        95.0,
		CPUPauseThresholdLower:   85.0,
		MaxKafkaMessagesPerSec:   10, // Low limit for testing
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)
	ctx := context.Background()

	// Burst capacity is 2x rate = 20
	successCount := 0
	for range 25 {
		allow, _ := rg.AllowKafkaMessage(ctx)
		if allow {
			successCount++
		}
	}

	// Should have ~20 successes (burst capacity)
	if successCount < 15 || successCount > 25 {
		t.Errorf("Expected ~20 successes from burst capacity, got %d", successCount)
	}
}

func TestResourceGuard_AllowKafkaMessage_WaitDuration(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       90.0,
		CPURejectThresholdLower:  80.0,
		CPUPauseThreshold:        95.0,
		CPUPauseThresholdLower:   85.0,
		MaxKafkaMessagesPerSec:   10,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)
	ctx := context.Background()

	// Exhaust burst capacity
	for range 25 {
		rg.AllowKafkaMessage(ctx)
	}

	// Next call should return wait duration
	allow, waitDuration := rg.AllowKafkaMessage(ctx)
	if allow {
		t.Error("Should not allow after burst exhausted")
	}
	if waitDuration <= 0 {
		t.Error("Should return positive wait duration")
	}
}

// =============================================================================
// Connection Acceptance Tests with ResourceGuard
// =============================================================================

func TestResourceGuard_ShouldAcceptConnection_MaxConnections(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	connCount.Store(100) // At limit
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           100, // Set limit to current count
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       90.0,
		CPURejectThresholdLower:  80.0,
		CPUPauseThreshold:        95.0,
		CPUPauseThresholdLower:   85.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)

	accept, reason := rg.ShouldAcceptConnection()
	if accept {
		t.Error("Should reject when at max connections")
	}
	if reason == "" {
		t.Error("Should provide rejection reason")
	}
}

func TestResourceGuard_ShouldAcceptConnection_BelowLimit(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	connCount.Store(50) // Below limit
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           100,
		MemoryLimit:              1024 * 1024 * 1024,
		MaxGoroutines:            10000,
		CPURejectThreshold:       99.0, // High to avoid CPU rejection
		CPURejectThresholdLower:  98.0,
		CPUPauseThreshold:        99.5,
		CPUPauseThresholdLower:   99.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)

	accept, reason := rg.ShouldAcceptConnection()
	if !accept {
		t.Errorf("Should accept when below limits, rejected with: %s", reason)
	}
}

// =============================================================================
// GetStats Tests
// =============================================================================

func TestResourceGuard_GetStats_AllFields(t *testing.T) {
	t.Parallel()
	var connCount atomic.Int64
	connCount.Store(42)
	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevelError,
		Format:      logging.LogFormatJSON,
		ServiceName: "ws-server",
	})

	config := ResourceGuardConfig{
		MaxConnections:           1000,
		MemoryLimit:              512 * 1024 * 1024,
		MaxGoroutines:            5000,
		CPURejectThreshold:       75.0,
		CPURejectThresholdLower:  65.0,
		CPUPauseThreshold:        80.0,
		CPUPauseThresholdLower:   70.0,
		MaxKafkaMessagesPerSec:   1000,
		MaxBroadcastsPerSec:      100,
		RateLimitBurstMultiplier: 2,
	}

	rg := NewResourceGuard(config, logger, &connCount)
	stats := rg.GetStats()

	// Verify all expected fields exist
	expectedFields := []string{
		"max_connections",
		"current_connections",
		"cpu_percent",
		"cpu_reject_threshold",
		"cpu_reject_threshold_lower",
		"cpu_rejecting",
		"cpu_pause_threshold",
		"cpu_pause_threshold_lower",
		"cpu_pausing_kafka",
		"memory_bytes",
		"memory_limit_bytes",
		"goroutines_current",
		"goroutines_limit",
		"kafka_rate_limit",
		"broadcast_rate_limit",
	}

	for _, field := range expectedFields {
		if _, ok := stats[field]; !ok {
			t.Errorf("GetStats() missing field: %s", field)
		}
	}

	// Verify specific values
	if stats["max_connections"].(int) != 1000 {
		t.Errorf("max_connections: got %v, want 1000", stats["max_connections"])
	}
	if stats["current_connections"].(int64) != 42 {
		t.Errorf("current_connections: got %v, want 42", stats["current_connections"])
	}
	if stats["memory_limit_bytes"].(int64) != 512*1024*1024 {
		t.Errorf("memory_limit_bytes: got %v, want %d", stats["memory_limit_bytes"], 512*1024*1024)
	}
	if stats["goroutines_limit"].(int) != 5000 {
		t.Errorf("goroutines_limit: got %v, want 5000", stats["goroutines_limit"])
	}
	if stats["kafka_rate_limit"].(int) != 1000 {
		t.Errorf("kafka_rate_limit: got %v, want 1000", stats["kafka_rate_limit"])
	}
	if stats["broadcast_rate_limit"].(int) != 100 {
		t.Errorf("broadcast_rate_limit: got %v, want 100", stats["broadcast_rate_limit"])
	}
}
