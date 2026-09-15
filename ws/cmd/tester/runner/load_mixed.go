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

// runLoadMixed implements mixed-mode load testing with two separate pools:
// one JWT pool and one API-key pool. The JWT pool publishes; the API-key pool
// subscribes only (API keys cannot REST-publish: gateway returns 403).
//
// Note: auth_mix_ratio=0.0 in the JSON body is treated as "omitted" (float64 zero value)
// and defaults to 0.5. To get all-JWT, omit the field or use auth_mode=jwt.
func runLoadMixed(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	connections := run.Config.Connections
	if connections <= 0 {
		connections = defaultLoadConnections
	}
	publishRate := run.Config.PublishRate
	if publishRate <= 0 {
		publishRate = defaultLoadPublishRate
	}
	rampRate := run.Config.RampRate
	if rampRate <= 0 {
		rampRate = defaultRampRate
	}
	duration, err := time.ParseDuration(run.Config.Duration)
	if err != nil || duration <= 0 {
		duration = defaultLoadDuration
	}

	ratio := run.Config.AuthMixRatio
	if ratio == 0 {
		ratio = defaultAuthMixRatio
	}

	// Split connections proportionally.
	apiKeyConns := int(math.Floor(float64(connections) * ratio))
	jwtConns := connections - apiKeyConns

	// Split ramp rates proportionally, clamped to minimum 1 for non-empty pools.
	// NOTE: at very low rampRate (≤2), total may slightly exceed configured value due to max(1,...) clamp.
	apiKeyRamp := 0
	if apiKeyConns > 0 {
		apiKeyRamp = max(1, int(math.Floor(float64(rampRate)*ratio)))
	}
	jwtRamp := rampRate - apiKeyRamp
	if jwtConns > 0 && jwtRamp < 1 {
		jwtRamp = 1
	}

	if ratio == 1.0 {
		logger.Warn().Msg("auth_mix_ratio=1.0: no JWT pool — publish rate is zero for this run")
	}

	logger.Info().
		Int("jwt_conns", jwtConns).
		Int("api_key_conns", apiKeyConns).
		Float64("ratio", ratio).
		Msg("mixed load: starting two-pool ramp")

	var poolWg sync.WaitGroup

	// JWT pool: ramps JWT connections and publishes messages.
	if jwtConns > 0 {
		jwtPool := testerws.NewPool(logger.With().Str("pool", "jwt").Logger())
		defer jwtPool.Drain()

		poolWg.Go(func() {
			defer logging.RecoverPanic(logger, "load_mixed_jwt_pool", nil)

			if rampErr := jwtPool.RampUp(ctx, testerws.PoolConfig{
				GatewayURL: run.Config.GatewayURL,
				TokenFunc:  run.authResult.TokenFunc,
				Channels:   []string{loadTestChannel},
				OnMessage: func(msg testerws.Message) {
					run.Collector.MessagesReceived.Add(1)
				},
			}, jwtConns, jwtRamp); rampErr != nil {
				logger.Warn().Err(rampErr).Msg("jwt pool ramp interrupted")
			}

			// Count connections after ramp (approximation: failed ones are in ConnectionsFailed).
			run.Collector.ConnectionsJWT.Add(int64(jwtConns))
			run.Collector.ConnectionsTotal.Add(int64(jwtConns))
			run.Collector.ConnectionsActive.Add(jwtPool.Active())

			// Only the JWT pool publishes (API keys cannot REST-publish).
			mixedPublish(ctx, run, publishRate, duration, logger)
		})
	}

	// API-key pool: ramps API-key connections and subscribes. No publishing.
	if apiKeyConns > 0 {
		apiKeyPool := testerws.NewPool(logger.With().Str("pool", "api-key").Logger())
		defer apiKeyPool.Drain()

		poolWg.Go(func() {
			defer logging.RecoverPanic(logger, "load_mixed_apikey_pool", nil)

			if rampErr := apiKeyPool.RampUp(ctx, testerws.PoolConfig{
				GatewayURL: run.Config.GatewayURL,
				APIKey:     run.apiKey,
				Channels:   []string{loadTestChannel},
				OnMessage: func(msg testerws.Message) {
					run.Collector.MessagesReceived.Add(1)
				},
			}, apiKeyConns, apiKeyRamp); rampErr != nil {
				logger.Warn().Err(rampErr).Msg("api-key pool ramp interrupted")
			}

			run.Collector.ConnectionsAPIKey.Add(int64(apiKeyConns))
			run.Collector.ConnectionsTotal.Add(int64(apiKeyConns))
			run.Collector.ConnectionsActive.Add(apiKeyPool.Active())

			// Hold until test ends (API-key pool does not publish).
			select {
			case <-ctx.Done():
			case <-time.After(duration):
			}
		})
	}

	poolWg.Wait()
	run.Collector.ConnectionsActive.Store(0)

	return buildMixedReport(run, "load"), nil
}

// mixedPublish creates a JWT publisher and runs publishLoop for the given duration.
// Used by the JWT pool in mixed mode.
func mixedPublish(ctx context.Context, run *TestRun, publishRate int, duration time.Duration, logger zerolog.Logger) {
	if publishRate <= 0 || run.authResult == nil || run.authResult.TokenFunc == nil {
		return
	}
	pub, err := publisher.NewDirectPublisher(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0))
	if err != nil {
		logger.Warn().Err(err).Msg("mixed: failed to create publisher")
		return
	}
	defer pub.Close() //nolint:errcheck // best-effort cleanup
	gen := publisher.NewGenerator()
	publishLoop(ctx, pub, gen, loadTestChannel, publishRate, duration, run.Collector, logger)
}

// buildMixedReport constructs a standard load/soak report for mixed mode.
func buildMixedReport(run *TestRun, testType string) *metrics.Report {
	overallStatus := metrics.ReportStatusPass
	if run.Collector.ConnectionsFailed.Load() > 0 {
		overallStatus = metrics.ReportStatusFail
	}
	return &metrics.Report{
		TestType: testType,
		Status:   overallStatus,
		Metrics:  run.Collector.Snapshot(),
	}
}
