package runner

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// runSoakMixed implements mixed-mode soak testing with two separate pools.
// The JWT pool maintains token refresh; the API-key pool subscribes without refresh.
func runSoakMixed(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	connections := run.Config.Connections
	if connections <= 0 {
		connections = defaultSoakConnections
	}
	publishRate := run.Config.PublishRate
	if publishRate <= 0 {
		publishRate = defaultSoakPublishRate
	}
	rampRate := run.Config.RampRate
	if rampRate <= 0 {
		rampRate = defaultRampRate
	}
	duration, err := time.ParseDuration(run.Config.Duration)
	if err != nil || duration <= 0 {
		duration = defaultSoakDuration
	}

	ratio := run.Config.AuthMixRatio
	if ratio == 0 {
		ratio = defaultAuthMixRatio
	}

	apiKeyConns := int(math.Floor(float64(connections) * ratio))
	jwtConns := connections - apiKeyConns

	if ratio == 1.0 {
		logger.Warn().Msg("auth_mix_ratio=1.0: no JWT pool — publish rate is zero for this run")
	}

	apiKeyRamp := 0
	if apiKeyConns > 0 {
		apiKeyRamp = max(1, int(math.Floor(float64(rampRate)*ratio)))
	}
	jwtRamp := rampRate - apiKeyRamp
	if jwtConns > 0 && jwtRamp < 1 {
		jwtRamp = 1
	}

	logger.Info().
		Int("jwt_conns", jwtConns).
		Int("api_key_conns", apiKeyConns).
		Float64("ratio", ratio).
		Str("duration", duration.String()).
		Msg("mixed soak: starting two-pool ramp")

	var poolWg sync.WaitGroup

	// JWT pool with token refresh.
	if jwtConns > 0 {
		jwtPool := testerws.NewPool(logger.With().Str("pool", "jwt").Logger())
		defer jwtPool.Drain()

		poolWg.Go(func() {
			defer logging.RecoverPanic(logger, "soak_mixed_jwt_pool", nil)

			if rampErr := jwtPool.RampUp(ctx, testerws.PoolConfig{
				GatewayURL: run.Config.GatewayURL,
				TokenFunc:  run.authResult.TokenFunc,
				Channels:   []string{soakTestChannel},
				OnMessage: func(msg testerws.Message) {
					run.Collector.MessagesReceived.Add(1)
				},
			}, jwtConns, jwtRamp); rampErr != nil {
				logger.Warn().Err(rampErr).Msg("jwt pool ramp interrupted")
			}

			run.Collector.ConnectionsJWT.Add(int64(jwtConns))
			run.Collector.ConnectionsTotal.Add(int64(jwtConns))
			run.Collector.ConnectionsActive.Add(jwtPool.Active())

			// Soak: publish + token refresh loop (same as standard soak).
			if run.authResult != nil && run.authResult.TokenFunc != nil {
				pub, pubErr := publisher.NewDirectPublisher(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0))
				if pubErr == nil {
					defer pub.Close() //nolint:errcheck // best-effort
					gen := publisher.NewGenerator()
					publishLoop(ctx, pub, gen, soakTestChannel, publishRate, duration, run.Collector, logger)
				}
			} else {
				select {
				case <-ctx.Done():
				case <-time.After(duration):
				}
			}
		})
	}

	// API-key pool: subscribe only, no token refresh.
	if apiKeyConns > 0 {
		apiKeyPool := testerws.NewPool(logger.With().Str("pool", "api-key").Logger())
		defer apiKeyPool.Drain()

		poolWg.Go(func() {
			defer logging.RecoverPanic(logger, "soak_mixed_apikey_pool", nil)

			if rampErr := apiKeyPool.RampUp(ctx, testerws.PoolConfig{
				GatewayURL: run.Config.GatewayURL,
				APIKey:     run.apiKey,
				Channels:   []string{soakTestChannel},
				OnMessage: func(msg testerws.Message) {
					run.Collector.MessagesReceived.Add(1)
				},
			}, apiKeyConns, apiKeyRamp); rampErr != nil {
				logger.Warn().Err(rampErr).Msg("api-key pool ramp interrupted")
			}

			run.Collector.ConnectionsAPIKey.Add(int64(apiKeyConns))
			run.Collector.ConnectionsTotal.Add(int64(apiKeyConns))
			run.Collector.ConnectionsActive.Add(apiKeyPool.Active())

			// Hold — API-key connections don't refresh tokens.
			select {
			case <-ctx.Done():
			case <-time.After(duration):
			}
		})
	}

	poolWg.Wait()
	run.Collector.ConnectionsActive.Store(0)

	return buildMixedReport(run, "soak"), nil
}
