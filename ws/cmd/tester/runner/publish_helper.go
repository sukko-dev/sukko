package runner

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
)

// publishLoop publishes generated messages at the given rate for the specified duration.
// Uses the Publisher interface — works with any backend. Increments MessagesSent on success.
func publishLoop(ctx context.Context, pub publisher.Publisher, gen *publisher.Generator, channel string, rate int, duration time.Duration, collector *metrics.Collector, logger zerolog.Logger) {
	if rate <= 0 {
		return
	}

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.After(duration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-ticker.C:
			data, err := gen.Next(channel)
			if err != nil {
				logger.Warn().Err(err).Msg("generate message failed")
				continue
			}
			if err := pub.Publish(ctx, channel, data); err != nil {
				logger.Warn().Err(err).Msg("publish failed")
			} else {
				collector.MessagesSent.Add(1)
			}
		}
	}
}
