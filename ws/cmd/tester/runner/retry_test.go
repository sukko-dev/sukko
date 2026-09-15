package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestConnectWithRetry_FailsAllAttempts(t *testing.T) {
	t.Parallel()

	// Use an unreachable address — all attempts should fail
	_, err := connectWithRetry(context.Background(), "ws://localhost:59999", "token", zerolog.Nop())
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

func TestConnectWithRetry_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := connectWithRetry(ctx, "ws://localhost:59999", "token", zerolog.Nop())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for canceled context")
	}
	// Should return quickly, not wait for all backoff delays
	if elapsed > 2*time.Second {
		t.Errorf("took %v, expected fast cancellation", elapsed)
	}
}

func TestWithRetry(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("transient")
	backoffs := []time.Duration{time.Millisecond}

	t.Run("success_first_attempt", func(t *testing.T) {
		t.Parallel()
		calls := 0
		got, err := withRetry(context.Background(), func() (int, error) {
			calls++
			return 42, nil
		}, 3, backoffs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 42 || calls != 1 {
			t.Errorf("got=%d calls=%d, want got=42 calls=1", got, calls)
		}
	})

	t.Run("success_after_retry", func(t *testing.T) {
		t.Parallel()
		calls := 0
		got, err := withRetry(context.Background(), func() (int, error) {
			calls++
			if calls < 3 {
				return 0, sentinel
			}
			return 7, nil
		}, 5, backoffs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 7 || calls != 3 {
			t.Errorf("got=%d calls=%d, want got=7 calls=3", got, calls)
		}
	})

	t.Run("exhausted_returns_last_error", func(t *testing.T) {
		t.Parallel()
		_, err := withRetry(context.Background(), func() (int, error) {
			return 0, sentinel
		}, 3, backoffs)
		if !errors.Is(err, sentinel) {
			t.Errorf("err = %v, want sentinel", err)
		}
	})

	t.Run("zero_attempts_returns_error", func(t *testing.T) {
		t.Parallel()
		_, err := withRetry(context.Background(), func() (int, error) {
			return 0, nil
		}, 0, backoffs)
		if err == nil {
			t.Fatal("expected error for attempts=0")
		}
	})

	t.Run("context_cancel_during_backoff", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		start := time.Now()
		_, err := withRetry(ctx, func() (int, error) {
			calls++
			if calls == 1 {
				cancel()
			}
			return 0, sentinel
		}, 5, []time.Duration{5 * time.Second})
		if err == nil {
			t.Fatal("expected error on ctx cancel")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("took %v, want fast cancellation", elapsed)
		}
	})
}
