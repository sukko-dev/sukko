package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// withRetry calls fn up to attempts times (must be > 0), sleeping between failures using
// the backoffs slice. It is context-aware: a canceled context aborts the
// sleep and returns immediately.
func withRetry[T any](ctx context.Context, fn func() (T, error), attempts int, backoffs []time.Duration) (T, error) {
	var zero T
	if attempts <= 0 {
		return zero, fmt.Errorf("withRetry: attempts must be > 0, got %d", attempts)
	}
	var lastErr error
	for attempt := range attempts {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt < attempts-1 {
			idx := min(attempt, len(backoffs)-1)
			select {
			case <-time.After(backoffs[idx]):
			case <-ctx.Done():
				return zero, fmt.Errorf("withRetry: %w", ctx.Err())
			}
		}
	}
	return zero, lastErr
}

// connectRetryBackoffs is the exponential backoff schedule for initial connection
// attempts after key registration (handles key registry cache race).
var connectRetryBackoffs = []time.Duration{0, 1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

// connectWithRetry attempts a WebSocket connection with exponential backoff.
// Used at test startup to handle the key registry cache race — the gateway may
// not have the newly registered key in its cache yet.
func connectWithRetry(ctx context.Context, gatewayURL, token string, logger zerolog.Logger, onMessage ...func(testerws.Message)) (*testerws.Client, error) {
	var msgCallback func(testerws.Message)
	if len(onMessage) > 0 {
		msgCallback = onMessage[0]
	}

	var lastErr error
	for i, delay := range connectRetryBackoffs {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("connect with retry: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		client, err := testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: gatewayURL,
			Token:      token,
			Logger:     logger,
			OnMessage:  msgCallback,
		})
		if err == nil {
			if i > 0 {
				logger.Info().Int("attempt", i+1).Msg("connected after retry")
			}
			return client, nil
		}

		lastErr = err
		logger.Debug().Err(err).Int("attempt", i+1).Msg("connection attempt failed")
	}

	return nil, fmt.Errorf("connect with retry: all %d attempts failed: %w", len(connectRetryBackoffs), lastErr)
}
