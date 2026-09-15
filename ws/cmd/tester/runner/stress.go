package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

const (
	defaultStressConnections = 1000
	defaultStressRampRate    = 100
	defaultStressDuration    = 10 * time.Minute
)

func runStress(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	// §II: nil-guard — api-key and upgrade modes do not produce a TokenFunc.
	if run.authResult.TokenFunc == nil {
		return nil, errors.New("runStress: TokenFunc is nil — api-key and upgrade modes are not supported for stress tests")
	}

	maxConnections := run.Config.Connections
	if maxConnections <= 0 {
		maxConnections = defaultStressConnections
	}
	rampRate := run.Config.RampRate
	if rampRate <= 0 {
		rampRate = defaultStressRampRate
	}
	duration, err := time.ParseDuration(run.Config.Duration)
	if err != nil || duration <= 0 {
		duration = defaultStressDuration
	}

	testChannel := tenantChannel(run.authResult.TenantID, stressTestChannel)
	pool := testerws.NewPool(logger)
	defer pool.Drain()

	logger.Info().Int("max_connections", maxConnections).Int("ramp_rate", rampRate).Msg("stress test starting")

	if err := pool.RampUp(ctx, testerws.PoolConfig{
		GatewayURL: run.Config.GatewayURL,
		TokenFunc:  run.authResult.TokenFunc,
		Channels:   []string{testChannel},
		OnMessage: func(msg testerws.Message) {
			run.Collector.MessagesReceived.Add(1)
		},
	}, maxConnections, rampRate); err != nil {
		logger.Warn().Err(err).Msg("ramp up interrupted")
	}

	run.Collector.ConnectionsActive.Store(pool.Active())
	run.Collector.ConnectionsTotal.Store(pool.Active())

	// Hold for duration
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}

	status := "pass"
	if pool.Active() < int64(maxConnections) {
		status = "degraded"
	}

	return &metrics.Report{
		TestType: "stress",
		Status:   status,
		Metrics:  run.Collector.Snapshot(),
		Checks: []metrics.CheckResult{
			{
				Name: "max connections reached",
				Status: func() string {
					if pool.Active() >= int64(maxConnections) {
						return "pass"
					}
					return "fail"
				}(),
				Error: fmt.Sprintf("reached %d/%d", pool.Active(), maxConnections),
			},
		},
	}, nil
}
