package api

import (
	"context"
	"testing"
	"time"
)

// mockRateLimiter is a minimal implementation of RateLimiter for tests.
type mockRateLimiter struct {
	allowed    bool
	retryAfter time.Duration
	err        error
}

func (m *mockRateLimiter) Allow(_ context.Context, _ string) (bool, time.Duration, error) {
	return m.allowed, m.retryAfter, m.err
}

func TestRateLimiter_InterfaceSatisfied(t *testing.T) {
	t.Parallel()
	// Verify mockRateLimiter satisfies the interface at compile time.
	var _ RateLimiter = &mockRateLimiter{}
}
