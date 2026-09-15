package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

const (
	defaultSoakConnections = 100
	defaultSoakPublishRate = 10
	defaultSoakDuration    = 2 * time.Hour
)

func runSoak(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	// §II: nil-guard — mixed mode is dispatched separately from execute(); non-mixed soak requires JWT.
	if run.authResult.TokenFunc == nil {
		return nil, errors.New("runSoak: TokenFunc is nil — use auth_mode=mixed for api-key soak tests")
	}

	// Provisioning-only channel authorization: seed permissive rules so soak
	// subscribes/publishes aren't denied by the deny-all default.
	if err := seedDefaultChannelRules(ctx, run); err != nil {
		return nil, fmt.Errorf("seed channel rules: %w", err)
	}

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

	testChannel := tenantChannel(run.authResult.TenantID, soakTestChannel)

	// Canary connection: tracks sequences for message loss/duplication detection
	ct := newChannelTrackers()
	canary, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		Token:      run.authResult.TokenFunc(0),
		Logger:     logger.With().Str("role", "canary").Logger(),
		OnMessage: func(msg testerws.Message) {
			switch msg.Type {
			case "auth_ack":
				run.Collector.AuthRefreshTotal.Add(1)
			case "auth_error":
				run.Collector.AuthRefreshFailed.Add(1)
				logger.Warn().Str("type", msg.Type).Msg("canary auth refresh rejected by gateway")
			default:
				run.Collector.MessagesReceived.Add(1)
				ct.track(msg)
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create canary connection: %w", err)
	}
	if err := canary.Subscribe([]string{testChannel}); err != nil {
		_ = canary.Close()
		return nil, fmt.Errorf("canary subscribe: %w", err)
	}
	var canaryWg sync.WaitGroup
	canaryWg.Go(func() {
		defer logging.RecoverPanic(logger, "canary_readloop", nil)
		_, _ = canary.ReadLoop(ctx)
	})

	pool := testerws.NewPool(logger)
	defer pool.Drain()

	logger.Info().Int("connections", connections).Str("duration", duration.String()).Msg("soak test starting")

	poolCount := connections - 1
	if poolCount > 0 {
		if err := pool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: run.Config.GatewayURL,
			TokenFunc:  func(i int) string { return run.authResult.TokenFunc(i + 1) },
			Channels:   []string{testChannel},
			OnMessage: func(msg testerws.Message) {
				switch msg.Type {
				case "auth_ack":
					run.Collector.AuthRefreshTotal.Add(1)
				case "auth_error":
					run.Collector.AuthRefreshFailed.Add(1)
					logger.Warn().Str("type", msg.Type).Msg("auth refresh rejected by gateway")
				default:
					run.Collector.MessagesReceived.Add(1)
				}
			},
		}, poolCount, rampRate); err != nil {
			return nil, fmt.Errorf("ramp up: %w", err)
		}
	}

	totalActive := pool.Active() + 1 // pool + canary
	run.Collector.ConnectionsActive.Store(totalActive)
	run.Collector.ConnectionsTotal.Store(totalActive)

	pub, pubErr := publisher.NewDirectPublisher(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0))
	if pubErr != nil {
		return nil, fmt.Errorf("create publisher: %w", pubErr)
	}
	gen := publisher.NewGenerator()
	defer pub.Close() //nolint:errcheck // best-effort cleanup on teardown

	// Sustain phase: publish at rate with periodic token refresh
	publishInterval := time.Second / time.Duration(publishRate)
	publishTicker := time.NewTicker(publishInterval)
	defer publishTicker.Stop()

	refreshInterval := run.jwtLifetime - run.jwtRefreshBefore
	if refreshInterval <= 0 {
		refreshInterval = 13 * time.Minute // safe fallback: default 15m lifetime - 2m buffer
	}
	refreshTicker := time.NewTicker(refreshInterval)
	defer refreshTicker.Stop()

	deadline := time.After(duration)

	var status string
	for {
		select {
		case <-ctx.Done():
			status = "stopped"
			goto done
		case <-deadline:
			status = "pass"
			goto done
		case <-publishTicker.C:
			data, _ := gen.Next(testChannel) // json.Marshal on literal map of primitives cannot fail
			if err := pub.Publish(ctx, testChannel, data); err != nil {
				run.Collector.ErrorsTotal.Add(1)
				logger.Warn().Err(err).Msg("publish failed")
			} else {
				run.Collector.MessagesSent.Add(1)
			}
		case <-refreshTicker.C:
			refreshed, failed := pool.RefreshAll(run.authResult.Minter.TokenFunc())
			if err := canary.RefreshToken(run.authResult.TokenFunc(0)); err != nil {
				logger.Warn().Err(err).Msg("canary token refresh failed")
			}
			logger.Debug().Int("refreshed", refreshed).Int("failed", failed).Msg("auth refresh cycle")
		}
	}

done:
	run.Collector.ConnectionsActive.Store(0)
	_ = canary.Close()
	canaryWg.Wait()
	stats := ct.aggregateStats()
	run.Collector.MessagesLost.Store(stats.Gaps)
	run.Collector.MessagesDuplicated.Store(stats.Duplicates)

	return &metrics.Report{
		TestType: "soak",
		Status:   status,
		Metrics:  run.Collector.Snapshot(),
	}, nil
}
