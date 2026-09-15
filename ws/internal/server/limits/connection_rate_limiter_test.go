package limits

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// testBaseConfig returns a ConnectionRateLimiterConfig with safe defaults for testing.
// Tests override individual fields as needed.
func testBaseConfig() ConnectionRateLimiterConfig {
	return ConnectionRateLimiterConfig{
		IPBurst:         10,
		IPRate:          1.0,
		IPTTL:           5 * time.Minute,
		GlobalBurst:     300,
		GlobalRate:      50.0,
		CleanupInterval: 1 * time.Minute,
		Logger:          zerolog.Nop(),
	}
}

// =============================================================================
// NewConnectionRateLimiter Tests
// =============================================================================

func TestNewConnectionRateLimiter_ExplicitConfig(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	stats := limiter.GetStats()

	if stats["ip_burst"].(int) != 10 {
		t.Errorf("ip_burst: got %v, want 10", stats["ip_burst"])
	}
	if stats["ip_rate"].(float64) != 1.0 {
		t.Errorf("ip_rate: got %v, want 1.0", stats["ip_rate"])
	}
	if stats["global_burst"].(int) != 300 {
		t.Errorf("global_burst: got %v, want 300", stats["global_burst"])
	}
	if stats["global_rate"].(float64) != 50.0 {
		t.Errorf("global_rate: got %v, want 50.0", stats["global_rate"])
	}
}

func TestNewConnectionRateLimiter_CustomConfig(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPBurst = 20
	cfg.IPRate = 2.0
	cfg.IPTTL = 10 * time.Minute
	cfg.GlobalBurst = 500
	cfg.GlobalRate = 100.0
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	stats := limiter.GetStats()

	if stats["ip_burst"].(int) != 20 {
		t.Errorf("ip_burst: got %v, want 20", stats["ip_burst"])
	}
	if stats["ip_rate"].(float64) != 2.0 {
		t.Errorf("ip_rate: got %v, want 2.0", stats["ip_rate"])
	}
	if stats["global_burst"].(int) != 500 {
		t.Errorf("global_burst: got %v, want 500", stats["global_burst"])
	}
	if stats["global_rate"].(float64) != 100.0 {
		t.Errorf("global_rate: got %v, want 100.0", stats["global_rate"])
	}
}

// =============================================================================
// CheckConnectionAllowed Tests
// =============================================================================

func TestCheckConnectionAllowed_NewIP(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.GlobalBurst = 100
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// First connection from new IP should succeed
	if !limiter.CheckConnectionAllowed("192.168.1.1") {
		t.Error("First connection from new IP should be allowed")
	}
}

func TestCheckConnectionAllowed_IPBurstLimit(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPBurst = 5
	cfg.IPRate = 0.1
	cfg.GlobalBurst = 100
	cfg.GlobalRate = 100.0
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	ip := "192.168.1.1"

	// Should allow 5 connections (burst capacity)
	for i := range 5 {
		if !limiter.CheckConnectionAllowed(ip) {
			t.Errorf("Connection %d should be allowed (within burst)", i+1)
		}
	}

	// 6th connection should be rate limited
	if limiter.CheckConnectionAllowed(ip) {
		t.Error("6th connection should be rate limited (exceeded burst)")
	}
}

func TestCheckConnectionAllowed_GlobalBurstLimit(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPBurst = 100
	cfg.IPRate = 10.0
	cfg.GlobalBurst = 5
	cfg.GlobalRate = 0.1
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// Use different IPs to not hit per-IP limit
	successCount := 0
	for i := range 10 {
		ip := "192.168.1." + string(rune('1'+i))
		if limiter.CheckConnectionAllowed(ip) {
			successCount++
		}
	}

	// Should only allow ~5 connections (global burst)
	if successCount < 4 || successCount > 6 {
		t.Errorf("Expected ~5 allowed connections (global burst), got %d", successCount)
	}
}

func TestCheckConnectionAllowed_SeparateIPBuckets(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPBurst = 3
	cfg.IPRate = 0.1
	cfg.GlobalBurst = 100
	cfg.GlobalRate = 100.0
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// Exhaust IP1's burst
	for range 3 {
		limiter.CheckConnectionAllowed("192.168.1.1")
	}

	// IP1 should be rate limited
	if limiter.CheckConnectionAllowed("192.168.1.1") {
		t.Error("IP1 should be rate limited")
	}

	// IP2 should still be able to connect
	if !limiter.CheckConnectionAllowed("192.168.1.2") {
		t.Error("IP2 should not be rate limited")
	}
}

func TestCheckConnectionAllowed_Concurrent(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPBurst = 100
	cfg.IPRate = 10.0
	cfg.GlobalBurst = 1000
	cfg.GlobalRate = 100.0
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	var wg sync.WaitGroup

	// Multiple goroutines making concurrent connection attempts
	for i := range 50 {
		wg.Go(func() {
			ip := "192.168.1." + string(rune('0'+(i%10)))
			for range 10 {
				limiter.CheckConnectionAllowed(ip)
			}
		})
	}

	wg.Wait()
	// Test passes if no race conditions or panics occur
}

// =============================================================================
// GetStats Tests
// =============================================================================

func TestGetStats_TrackedIPs(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.GlobalBurst = 100
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// Initially no tracked IPs
	stats := limiter.GetStats()
	if stats["tracked_ips"].(int) != 0 {
		t.Errorf("Initial tracked_ips: got %v, want 0", stats["tracked_ips"])
	}

	// Make connections from 3 different IPs
	limiter.CheckConnectionAllowed("192.168.1.1")
	limiter.CheckConnectionAllowed("192.168.1.2")
	limiter.CheckConnectionAllowed("192.168.1.3")

	stats = limiter.GetStats()
	if stats["tracked_ips"].(int) != 3 {
		t.Errorf("After 3 IPs tracked_ips: got %v, want 3", stats["tracked_ips"])
	}
}

func TestGetStats_ReturnsAllFields(t *testing.T) {
	t.Parallel()
	limiter := NewConnectionRateLimiter(testBaseConfig())
	defer limiter.Stop()

	stats := limiter.GetStats()

	expectedFields := []string{"tracked_ips", "ip_burst", "ip_rate", "ip_ttl", "global_burst", "global_rate"}
	for _, field := range expectedFields {
		if _, ok := stats[field]; !ok {
			t.Errorf("GetStats() missing field: %s", field)
		}
	}
}

// =============================================================================
// Cleanup Tests
// =============================================================================

func TestCleanup_RemovesStaleIPs(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPTTL = 100 * time.Millisecond // Very short TTL for testing
	cfg.GlobalBurst = 100
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// Create IP entries
	limiter.CheckConnectionAllowed("192.168.1.1")
	limiter.CheckConnectionAllowed("192.168.1.2")

	// Verify they exist
	stats := limiter.GetStats()
	if stats["tracked_ips"].(int) != 2 {
		t.Fatalf("Expected 2 tracked IPs, got %d", stats["tracked_ips"].(int))
	}

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// Trigger cleanup manually
	limiter.cleanup()

	// Verify IPs were removed
	stats = limiter.GetStats()
	if stats["tracked_ips"].(int) != 0 {
		t.Errorf("After cleanup tracked_ips: got %v, want 0", stats["tracked_ips"])
	}
}

func TestCleanup_KeepsRecentIPs(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.IPTTL = 1 * time.Second
	cfg.GlobalBurst = 100
	limiter := NewConnectionRateLimiter(cfg)
	defer limiter.Stop()

	// Create IP entries
	limiter.CheckConnectionAllowed("192.168.1.1")

	// Trigger cleanup immediately (IP should be kept)
	limiter.cleanup()

	stats := limiter.GetStats()
	if stats["tracked_ips"].(int) != 1 {
		t.Errorf("After cleanup tracked_ips: got %v, want 1 (IP should be kept)", stats["tracked_ips"])
	}
}

// =============================================================================
// Stop Tests
// =============================================================================

func TestStop_StopsCleanupGoroutine(t *testing.T) {
	t.Parallel()
	cfg := testBaseConfig()
	cfg.GlobalBurst = 100
	limiter := NewConnectionRateLimiter(cfg)

	// Stop should not panic and should return quickly
	done := make(chan struct{})
	go func() {
		limiter.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success - Stop returned
	case <-time.After(1 * time.Second):
		t.Error("Stop() did not return within 1 second")
	}
}

// TestConnectionRateLimiter_RetryAfter pins the Retry-After hint for the WS-upgrade
// rejection path. That path is the one 429 in the platform that does NOT go through
// httputil.WriteRateLimited (a WebSocket upgrade keeps a plain-text body), so it is the
// likeliest place for the header to silently regress — this plus the handler's own use of
// httputil.RetryAfterSeconds is what keeps it honest.
func TestConnectionRateLimiter_RetryAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ipRate float64
		want   time.Duration
	}{
		{"100/s refills a token every 10ms", 100, 10 * time.Millisecond},
		{"1/s refills every second", 1, time.Second},
		{"0.5/s is a two-second wait", 0.5, 2 * time.Second},
		{"non-positive rate falls back to 1s rather than dividing by zero", 0, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			crl := &ConnectionRateLimiter{ipRate: tt.ipRate}
			if got := crl.RetryAfter(); got != tt.want {
				t.Errorf("RetryAfter() = %v, want %v", got, tt.want)
			}
		})
	}
}
