package limits

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// TokenBucket Tests
// =============================================================================

func TestNewTokenBucket(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(100, 10)

	if tb.maxTokens != 100 {
		t.Errorf("maxTokens: got %v, want 100", tb.maxTokens)
	}
	if tb.refillRate != 10 {
		t.Errorf("refillRate: got %v, want 10", tb.refillRate)
	}
	if tb.tokens != 100 {
		t.Errorf("tokens should start at maxTokens: got %v, want 100", tb.tokens)
	}
}

func TestTokenBucket_TryConsume_Success(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(10, 1)

	// Should allow first 10 requests (burst capacity)
	for i := range 10 {
		if !tb.TryConsume(1) {
			t.Errorf("TryConsume(%d) should succeed", i+1)
		}
	}
}

func TestTokenBucket_TryConsume_Exhausted(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(5, 1)

	// Exhaust all tokens
	for range 5 {
		tb.TryConsume(1)
	}

	// 6th request should fail
	if tb.TryConsume(1) {
		t.Error("TryConsume should fail when tokens exhausted")
	}
}

func TestTokenBucket_TryConsume_MultipleTokens(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(10, 1)

	// Consume 5 tokens at once
	if !tb.TryConsume(5) {
		t.Error("TryConsume(5) should succeed")
	}

	// Consume 5 more
	if !tb.TryConsume(5) {
		t.Error("TryConsume(5) should succeed (5 tokens remaining)")
	}

	// No tokens left
	if tb.TryConsume(1) {
		t.Error("TryConsume(1) should fail (0 tokens remaining)")
	}
}

func TestTokenBucket_TryConsume_InsufficientTokens(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(5, 1)

	// Try to consume more than available
	if tb.TryConsume(10) {
		t.Error("TryConsume(10) should fail with only 5 tokens")
	}

	// Bucket should still have all tokens (no partial consume)
	if !tb.TryConsume(5) {
		t.Error("Bucket should still have 5 tokens after failed consume")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(10, 100) // 100 tokens/second refill rate

	// Exhaust all tokens
	for range 10 {
		tb.TryConsume(1)
	}

	// Wait for refill (100 tokens/sec = 10 tokens in 100ms)
	time.Sleep(110 * time.Millisecond)

	// Should have refilled enough tokens
	if !tb.TryConsume(1) {
		t.Error("TryConsume should succeed after refill")
	}
}

func TestTokenBucket_RefillCapAtMax(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(10, 100) // 100 tokens/second

	// Wait longer than needed for full refill
	time.Sleep(200 * time.Millisecond)

	// Should still only have maxTokens (10), not 20
	// Consume 10 should work
	for i := range 10 {
		if !tb.TryConsume(1) {
			t.Errorf("TryConsume(%d) should succeed (max tokens)", i+1)
		}
	}

	// 11th should fail
	if tb.TryConsume(1) {
		t.Error("TryConsume(11) should fail - bucket capped at maxTokens")
	}
}

func TestTokenBucket_Concurrent(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(1000, 100)

	var wg sync.WaitGroup
	successCount := int64(0)
	var mu sync.Mutex

	// 100 goroutines each trying to consume 20 tokens
	for range 100 {
		wg.Go(func() {
			for range 20 {
				if tb.TryConsume(1) {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}
		})
	}

	wg.Wait()

	// Should have at most 1000 successes (initial bucket size)
	// May have slightly more due to refill during test
	if successCount > 1100 {
		t.Errorf("Too many successes: got %d, max should be ~1000 + refill", successCount)
	}
	if successCount < 900 {
		t.Errorf("Too few successes: got %d, expected ~1000", successCount)
	}
}

func TestTokenBucket_ZeroTokens(t *testing.T) {
	t.Parallel()
	tb := NewTokenBucket(0, 0)

	// Should fail immediately with 0 capacity
	if tb.TryConsume(1) {
		t.Error("TryConsume should fail with 0 token bucket")
	}
}

// =============================================================================
// RateLimiter Tests
// =============================================================================

func TestNewRateLimiter(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	if rl == nil {
		t.Fatal("NewRateLimiter should return non-nil")
	}
}

func TestRateLimiter_CheckLimit_NewClient(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	// First check for a new client should succeed
	if !rl.CheckLimit(123) {
		t.Error("CheckLimit should succeed for new client")
	}
}

func TestRateLimiter_CheckLimit_SeparateBuckets(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	// Exhaust client 1's bucket (default: 100 tokens)
	for range 100 {
		rl.CheckLimit(1)
	}

	// Client 1 should be rate limited
	if rl.CheckLimit(1) {
		t.Error("Client 1 should be rate limited after 100 requests")
	}

	// Client 2 should still be able to make requests
	if !rl.CheckLimit(2) {
		t.Error("Client 2 should not be rate limited")
	}
}

func TestRateLimiter_RemoveClient(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	// Create bucket for client
	rl.CheckLimit(123)

	// Remove client
	rl.RemoveClient(123)

	// Client should get a fresh bucket (full 100 tokens)
	// This verifies RemoveClient actually removed the old bucket
	for i := range 100 {
		if !rl.CheckLimit(123) {
			t.Errorf("After RemoveClient, client should have fresh bucket (failed at %d)", i+1)
		}
	}
}

func TestRateLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	var wg sync.WaitGroup

	// Multiple clients making concurrent requests
	for clientID := int64(1); clientID <= 10; clientID++ {
		wg.Go(func() {
			for range 50 {
				rl.CheckLimit(clientID)
			}
		})
	}

	wg.Wait()

	// Concurrent cleanup
	for clientID := int64(1); clientID <= 10; clientID++ {
		wg.Go(func() {
			rl.RemoveClient(clientID)
		})
	}

	wg.Wait()
}

func TestRateLimiter_DefaultLimits(t *testing.T) {
	t.Parallel()
	rl := NewRateLimiter(100, 10)

	// Default should be 100 burst, 10/sec sustained
	// Verify by consuming 100 tokens
	successCount := 0
	for range 110 {
		if rl.CheckLimit(1) {
			successCount++
		}
	}

	// Should succeed ~100 times (burst capacity)
	if successCount < 95 || successCount > 105 {
		t.Errorf("Default limit should allow ~100 burst, got %d successes", successCount)
	}
}
