package protocol

import (
	"testing"
)

// =============================================================================
// Size Limits Tests
// =============================================================================

func TestDefaultMaxPublishSize_Value(t *testing.T) {
	t.Parallel()
	expectedSize := 64 * 1024 // 64KB

	if DefaultMaxPublishSize != expectedSize {
		t.Errorf("DefaultMaxPublishSize = %d, want %d", DefaultMaxPublishSize, expectedSize)
	}
}

func TestDefaultMaxPublishSize_Is64KB(t *testing.T) {
	t.Parallel()
	// 64KB = 65536 bytes
	if DefaultMaxPublishSize != 65536 {
		t.Errorf("DefaultMaxPublishSize = %d, want 65536 (64KB)", DefaultMaxPublishSize)
	}
}

func TestDefaultSendBufferSize_Value(t *testing.T) {
	t.Parallel()
	if DefaultSendBufferSize != 256 {
		t.Errorf("DefaultSendBufferSize = %d, want 256", DefaultSendBufferSize)
	}
}

func TestDefaultSendBufferSize_Positive(t *testing.T) {
	t.Parallel()
	if DefaultSendBufferSize <= 0 {
		t.Errorf("DefaultSendBufferSize = %d, must be positive", DefaultSendBufferSize)
	}
}

// =============================================================================
// Rate Limits Tests
// =============================================================================

func TestDefaultPublishRateLimit_Value(t *testing.T) {
	t.Parallel()
	if DefaultPublishRateLimit != 10.0 {
		t.Errorf("DefaultPublishRateLimit = %f, want 10.0", DefaultPublishRateLimit)
	}
}

func TestDefaultPublishRateLimit_Positive(t *testing.T) {
	t.Parallel()
	if DefaultPublishRateLimit <= 0 {
		t.Errorf("DefaultPublishRateLimit = %f, must be positive", DefaultPublishRateLimit)
	}
}

func TestDefaultPublishBurst_Value(t *testing.T) {
	t.Parallel()
	if DefaultPublishBurst != 100 {
		t.Errorf("DefaultPublishBurst = %d, want 100", DefaultPublishBurst)
	}
}

func TestDefaultPublishBurst_Positive(t *testing.T) {
	t.Parallel()
	if DefaultPublishBurst <= 0 {
		t.Errorf("DefaultPublishBurst = %d, must be positive", DefaultPublishBurst)
	}
}

func TestDefaultPublishBurst_GreaterThanRateLimit(t *testing.T) {
	t.Parallel()
	// Burst should be greater than rate limit for smooth traffic handling
	if float64(DefaultPublishBurst) <= DefaultPublishRateLimit {
		t.Errorf("DefaultPublishBurst (%d) should be greater than DefaultPublishRateLimit (%f)",
			DefaultPublishBurst, DefaultPublishRateLimit)
	}
}

// =============================================================================
// Channel Parts Tests
// =============================================================================

func TestMinInternalChannelParts_Value(t *testing.T) {
	t.Parallel()
	if MinInternalChannelParts != 2 {
		t.Errorf("MinInternalChannelParts = %d, want 2", MinInternalChannelParts)
	}
}

func TestMinInternalChannelParts_Positive(t *testing.T) {
	t.Parallel()
	if MinInternalChannelParts <= 0 {
		t.Errorf("MinInternalChannelParts = %d, must be positive", MinInternalChannelParts)
	}
}

// =============================================================================
// Constants Relationship Tests
// =============================================================================

func TestLimits_ReasonableValues(t *testing.T) {
	t.Parallel()
	// Sanity checks for reasonable default values

	// Max publish size should be at least 1KB
	if DefaultMaxPublishSize < 1024 {
		t.Errorf("DefaultMaxPublishSize = %d, should be at least 1KB", DefaultMaxPublishSize)
	}

	// Max publish size should be at most 1MB for WebSocket efficiency
	if DefaultMaxPublishSize > 1024*1024 {
		t.Errorf("DefaultMaxPublishSize = %d, should be at most 1MB", DefaultMaxPublishSize)
	}

	// Rate limit should be at least 1 msg/sec
	if DefaultPublishRateLimit < 1.0 {
		t.Errorf("DefaultPublishRateLimit = %f, should be at least 1.0", DefaultPublishRateLimit)
	}

	// Burst should be at least 10 for reasonable traffic handling
	if DefaultPublishBurst < 10 {
		t.Errorf("DefaultPublishBurst = %d, should be at least 10", DefaultPublishBurst)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkConstantAccess_Size(b *testing.B) {
	for b.Loop() {
		_ = DefaultMaxPublishSize
	}
}

func BenchmarkConstantAccess_RateLimit(b *testing.B) {
	for b.Loop() {
		_ = DefaultPublishRateLimit
	}
}

func BenchmarkConstantAccess_ChannelParts(b *testing.B) {
	for b.Loop() {
		_ = MinInternalChannelParts
	}
}
