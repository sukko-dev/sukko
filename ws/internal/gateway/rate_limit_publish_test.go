package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

func newTestPublishRateLimiter(t *testing.T, ratePerSec float64, burst int) (*PublishRateLimiter, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	logger := zerolog.Nop()
	pl := NewPublishRateLimiter(ctx, &wg, ratePerSec, burst, logger)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return pl, cancel
}

func TestPublishRateLimiter_AllowTenant(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 2) // 2 burst

	// First 2 requests should pass (burst capacity)
	if !pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("first request should be allowed")
	}
	if !pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("second request should be allowed (within burst)")
	}

	// Third request should be rate limited (burst exhausted, no time for refill)
	if pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("third request should be rate limited")
	}
}

func TestPublishRateLimiter_AllowIP(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 2)

	// Same IP, different tenants — IP limit should kick in
	if !pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("first request should be allowed")
	}
	if !pl.Allow("tenant-2", "10.0.0.1") {
		t.Error("second request from different tenant, same IP should be allowed")
	}
	if pl.Allow("tenant-3", "10.0.0.1") {
		t.Error("third request should be rate limited by IP (burst=2)")
	}
}

func TestPublishRateLimiter_BothMustPass(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 1) // 1 burst

	// First request passes both
	if !pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("first request should pass both limits")
	}

	// Second request: different tenant (passes tenant limit) but same IP (fails IP limit)
	if pl.Allow("tenant-2", "10.0.0.1") {
		t.Error("should fail because IP limit exhausted even though tenant-2 has fresh limit")
	}
}

func TestPublishRateLimiter_DifferentKeysIndependent(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 1)

	// tenant-1 + IP-1
	if !pl.Allow("tenant-1", "10.0.0.1") {
		t.Error("first request should pass")
	}

	// tenant-2 + IP-2 — completely different keys, should pass
	if !pl.Allow("tenant-2", "10.0.0.2") {
		t.Error("different tenant + different IP should pass independently")
	}
}

func TestPublishRateLimiter_Concurrent(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 1000, 100)

	var wg sync.WaitGroup
	var allowed, denied int64
	var mu sync.Mutex

	for range 50 {
		wg.Go(func() {
			result := pl.Allow("tenant-1", "10.0.0.1")
			mu.Lock()
			if result {
				allowed++
			} else {
				denied++
			}
			mu.Unlock()
		})
	}

	wg.Wait()

	// With burst=100 and 50 concurrent requests, all should pass
	if denied > 0 {
		t.Errorf("expected all 50 to pass with burst=100, got %d allowed, %d denied", allowed, denied)
	}
}

func TestPublishRateLimiter_Cleanup(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 10)

	// Create some entries
	pl.Allow("tenant-1", "10.0.0.1")
	pl.Allow("tenant-2", "10.0.0.2")

	// Manually set lastUsed to past (simulating stale entries)
	staleTime := time.Now().Add(-20 * time.Minute).Unix()
	pl.tenantLimiters.Range(func(_, value any) bool {
		value.(*limiterEntry).lastUsed.Store(staleTime)
		return true
	})
	pl.ipLimiters.Range(func(_, value any) bool {
		value.(*limiterEntry).lastUsed.Store(staleTime)
		return true
	})

	// Run cleanup
	pl.cleanup()

	// Verify entries were removed
	var tenantCount, ipCount int
	pl.tenantLimiters.Range(func(_, _ any) bool { tenantCount++; return true })
	pl.ipLimiters.Range(func(_, _ any) bool { ipCount++; return true })

	if tenantCount != 0 {
		t.Errorf("expected 0 tenant entries after cleanup, got %d", tenantCount)
	}
	if ipCount != 0 {
		t.Errorf("expected 0 IP entries after cleanup, got %d", ipCount)
	}
}

func TestPublishRateLimiter_CleanupPreservesRecent(t *testing.T) {
	t.Parallel()

	pl, _ := newTestPublishRateLimiter(t, 10, 10)

	// Create entries — these are recent (lastUsed = now)
	pl.Allow("tenant-1", "10.0.0.1")

	// Run cleanup
	pl.cleanup()

	// Verify recent entries preserved
	var tenantCount int
	pl.tenantLimiters.Range(func(_, _ any) bool { tenantCount++; return true })

	if tenantCount != 1 {
		t.Errorf("expected 1 tenant entry (recent), got %d", tenantCount)
	}
}

// TestPublishRateLimiter_RetryAfter pins the Retry-After hint against the limiter's OWN
// rate. The advertised wait must come from the limiter that actually rejected the request,
// not from a config value read separately — production wires both from the same setting, so
// a drift between them would be invisible until the header started lying.
func TestPublishRateLimiter_RetryAfter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rate float64
		want time.Duration
	}{
		{"10/s refills a token every 100ms", 10, 100 * time.Millisecond},
		{"1/s refills every second", 1, time.Second},
		{"0.001/s is a ~1000s wait", 0.001, 1000 * time.Second},
		{"non-positive rate falls back to 1s rather than dividing by zero", 0, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pl := &PublishRateLimiter{rateLimit: rate.Limit(tt.rate), burst: 1}
			if got := pl.RetryAfter(); got != tt.want {
				t.Errorf("RetryAfter() = %v, want %v", got, tt.want)
			}
		})
	}
}
