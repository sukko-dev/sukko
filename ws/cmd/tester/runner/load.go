package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

const (
	defaultLoadConnections = 100
	defaultLoadPublishRate = 10
	defaultLoadDuration    = 5 * time.Minute
)

func runLoad(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	// §II: nil-guard — mixed mode is dispatched separately from execute(); non-mixed load requires JWT.
	if run.authResult.TokenFunc == nil {
		return nil, errors.New("runLoad: TokenFunc is nil — use auth_mode=mixed for api-key load tests")
	}

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

	// Channel mode: distribute across public/user/group channels
	if run.Config.ChannelMode {
		return runLoadChannelMode(ctx, run, logger, connections, publishRate, rampRate, duration)
	}

	testChannel := tenantChannel(run.authResult.TenantID, loadTestChannel)

	// Phase 1: Ramp up connections (canary + pool)
	logger.Info().Int("connections", connections).Int("ramp_rate", rampRate).Msg("ramping up")

	// Canary connection: tracks sequences for message loss/duplication detection
	ct := newChannelTrackers()
	canary, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		Token:      run.authResult.TokenFunc(0),
		Logger:     logger.With().Str("role", "canary").Logger(),
		OnMessage: func(msg testerws.Message) {
			run.Collector.MessagesReceived.Add(1)
			ct.track(msg)
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

	// Pool: remaining connections (just counting, no tracking)
	pool := testerws.NewPool(logger)
	defer pool.Drain()

	poolCount := connections - 1
	if poolCount > 0 {
		if err := pool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: run.Config.GatewayURL,
			TokenFunc:  func(i int) string { return run.authResult.TokenFunc(i + 1) },
			Channels:   []string{testChannel},
			OnMessage: func(msg testerws.Message) {
				run.Collector.MessagesReceived.Add(1)
			},
		}, poolCount, rampRate); err != nil {
			return nil, fmt.Errorf("ramp up: %w", err)
		}
	}

	totalActive := pool.Active() + 1 // pool + canary
	run.Collector.ConnectionsActive.Store(totalActive)
	run.Collector.ConnectionsTotal.Store(totalActive)

	// Phase 2: Sustain — publish at rate
	logger.Info().Int("rate", publishRate).Str("duration", duration.String()).Msg("sustaining load")

	pub, err := publisher.NewDirectPublisher(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0))
	if err != nil {
		return nil, fmt.Errorf("create publisher: %w", err)
	}
	defer pub.Close() //nolint:errcheck // best-effort cleanup on teardown

	gen := publisher.NewGenerator()
	publishLoop(ctx, pub, gen, testChannel, publishRate, duration, run.Collector, logger)

	// Phase 3: Drain + populate tracker stats
	run.Collector.ConnectionsActive.Store(0)
	_ = canary.Close() // triggers ReadLoop exit
	canaryWg.Wait()    // ensure ReadLoop goroutine finished before reading stats
	stats := ct.aggregateStats()
	run.Collector.MessagesLost.Store(stats.Gaps)
	run.Collector.MessagesDuplicated.Store(stats.Duplicates)

	return &metrics.Report{
		TestType: "load",
		Status:   "pass",
		Metrics:  run.Collector.Snapshot(),
	}, nil
}

// runLoadChannelMode distributes connections across public, user-scoped, and group-scoped channels.
// Split: 40% public, 30% user-scoped, 30% group-scoped.
func runLoadChannelMode(ctx context.Context, run *TestRun, logger zerolog.Logger, connections, publishRate, rampRate int, duration time.Duration) (*metrics.Report, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID

	// Tenant-qualified channel names — the gateway requires the tenant prefix
	// on every subscribe/publish channel; rule patterns stay bare (matched
	// after the prefix is stripped).
	publicChannel := tenantChannel(tenantID, "general.test")
	groupChannel := tenantChannel(tenantID, "room.vip")

	// Setup: channel rules + routing rules
	if err := provClient.SetChannelRules(ctx, tenantID, testChannelRules); err != nil {
		return nil, fmt.Errorf("set channel rules: %w", err)
	}
	if err := provClient.SetRoutingRules(ctx, tenantID, testRoutingRules); err != nil {
		return nil, fmt.Errorf("set routing rules: %w", err)
	}

	// Split connections: 40% public, 30% user-scoped, 30% group
	publicCount := connections * 40 / 100
	userCount := connections * 30 / 100
	groupCount := connections - publicCount - userCount

	logger.Info().
		Int("total", connections).
		Int("public", publicCount).
		Int("user_scoped", userCount).
		Int("group_scoped", groupCount).
		Msg("channel mode: distributing connections")

	minter := run.authResult.Minter

	// Create 3 pools with different token functions
	publicPool := testerws.NewPool(logger)
	defer publicPool.Drain()

	userPool := testerws.NewPool(logger)
	defer userPool.Drain()

	groupPool := testerws.NewPool(logger)
	defer groupPool.Drain()

	// Canary connections: one per channel type for sequence tracking (only if that type has allocation)
	ct := newChannelTrackers()
	var canaryCount int64
	var canaryWg sync.WaitGroup
	var canaryCleanups []func() // collected for shutdown

	// Public canary (only if publicCount > 0)
	if publicCount > 0 {
		publicCanary, err := testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      run.authResult.TokenFunc(0),
			Logger:     logger.With().Str("role", "canary-public").Logger(),
			OnMessage: func(msg testerws.Message) {
				run.Collector.PublicReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
				ct.track(msg)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create public canary: %w", err)
		}
		if err := publicCanary.Subscribe([]string{publicChannel}); err != nil {
			_ = publicCanary.Close()
			return nil, fmt.Errorf("public canary subscribe: %w", err)
		}
		canaryWg.Go(func() {
			defer logging.RecoverPanic(logger, "canary_public_readloop", nil)
			_, _ = publicCanary.ReadLoop(ctx)
		})
		canaryCleanups = append(canaryCleanups, func() { _ = publicCanary.Close() })
		canaryCount++
	}

	// User-scoped canary (only if userCount > 0)
	if userCount > 0 {
		userCanaryToken, err := minter.MintWithClaims(auth.MintOptions{
			ConnIndex: 999,
			Subject:   "canary-user",
		})
		if err != nil {
			return nil, fmt.Errorf("mint user canary token: %w", err)
		}
		userCanary, err := testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      userCanaryToken,
			Logger:     logger.With().Str("role", "canary-user").Logger(),
			OnMessage: func(msg testerws.Message) {
				run.Collector.UserScopedReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
				ct.track(msg)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create user canary: %w", err)
		}
		// User-scoped channels are concrete per-subject names — the gateway
		// resolves placeholders in rule PATTERNS, never in requested channel
		// names, so the canary subscribes to its own resolved channel.
		if err := userCanary.Subscribe([]string{tenantChannel(tenantID, "dm.canary-user")}); err != nil {
			_ = userCanary.Close()
			return nil, fmt.Errorf("user canary subscribe: %w", err)
		}
		canaryWg.Go(func() {
			defer logging.RecoverPanic(logger, "canary_user_readloop", nil)
			_, _ = userCanary.ReadLoop(ctx)
		})
		canaryCleanups = append(canaryCleanups, func() { _ = userCanary.Close() })
		canaryCount++
	}

	// Group-scoped canary (only if groupCount > 0)
	if groupCount > 0 {
		groupCanaryToken, err := minter.MintWithClaims(auth.MintOptions{
			ConnIndex: 998,
			Subject:   "canary-group",
			Groups:    []string{"vip"},
		})
		if err != nil {
			return nil, fmt.Errorf("mint group canary token: %w", err)
		}
		groupCanary, err := testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      groupCanaryToken,
			Logger:     logger.With().Str("role", "canary-group").Logger(),
			OnMessage: func(msg testerws.Message) {
				run.Collector.GroupScopedReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
				ct.track(msg)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create group canary: %w", err)
		}
		if err := groupCanary.Subscribe([]string{groupChannel}); err != nil {
			_ = groupCanary.Close()
			return nil, fmt.Errorf("group canary subscribe: %w", err)
		}
		canaryWg.Go(func() {
			defer logging.RecoverPanic(logger, "canary_group_readloop", nil)
			_, _ = groupCanary.ReadLoop(ctx)
		})
		canaryCleanups = append(canaryCleanups, func() { _ = groupCanary.Close() })
		canaryCount++
	}

	// Public pool: default tokens, subscribe to general.test
	publicPoolCount := max(publicCount-1, 0) // subtract canary, floor at 0
	if publicPoolCount > 0 {
		if err := publicPool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: run.Config.GatewayURL,
			TokenFunc:  func(i int) string { return run.authResult.TokenFunc(i + 1) },
			Channels:   []string{publicChannel},
			OnMessage: func(_ testerws.Message) {
				run.Collector.PublicReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
			},
		}, publicPoolCount, rampRate); err != nil {
			return nil, fmt.Errorf("ramp up public pool: %w", err)
		}
	}

	// User-scoped pool: each connection subscribes to dm.<subject>
	userPoolCount := max(userCount-1, 0) // subtract canary, floor at 0
	if userPoolCount > 0 {
		if err := userPool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: run.Config.GatewayURL,
			TokenFunc: func(i int) string {
				token, err := minter.MintWithClaims(auth.MintOptions{
					ConnIndex: 1000 + i,
					Subject:   fmt.Sprintf("load-user-%04d", i),
				})
				if err != nil {
					logger.Error().Err(err).Int("conn", i).Msg("mint user-scoped token failed")
					return "" // empty token → connection will fail, tracked by pool
				}
				return token
			},
			ChannelsFunc: func(i int) []string { // per-connection resolved user channel
				return []string{tenantChannel(tenantID, fmt.Sprintf("dm.load-user-%04d", i))}
			},
			OnMessage: func(_ testerws.Message) {
				run.Collector.UserScopedReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
			},
		}, userPoolCount, rampRate); err != nil {
			return nil, fmt.Errorf("ramp up user pool: %w", err)
		}
	}

	// Group pool: vip group, subscribe to room.vip
	groupPoolCount := max(groupCount-1, 0) // subtract canary, floor at 0
	if groupPoolCount > 0 {
		if err := groupPool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: run.Config.GatewayURL,
			TokenFunc: func(i int) string {
				token, err := minter.MintWithClaims(auth.MintOptions{
					ConnIndex: 2000 + i,
					Subject:   fmt.Sprintf("load-group-%04d", i),
					Groups:    []string{"vip"},
				})
				if err != nil {
					logger.Error().Err(err).Int("conn", i).Msg("mint group-scoped token failed")
					return "" // empty token → connection will fail, tracked by pool
				}
				return token
			},
			Channels: []string{groupChannel},
			OnMessage: func(_ testerws.Message) {
				run.Collector.GroupScopedReceived.Add(1)
				run.Collector.MessagesReceived.Add(1)
			},
		}, groupPoolCount, rampRate); err != nil {
			return nil, fmt.Errorf("ramp up group pool: %w", err)
		}
	}

	totalActive := publicPool.Active() + userPool.Active() + groupPool.Active() + canaryCount
	run.Collector.ConnectionsActive.Store(totalActive)
	run.Collector.ConnectionsTotal.Store(totalActive)

	// Phase 2: Sustain — publish to all channel types at rate
	logger.Info().Int("rate", publishRate).Str("duration", duration.String()).Msg("sustaining channel-mode load")

	pub, err := publisher.NewDirectPublisher(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0))
	if err != nil {
		return nil, fmt.Errorf("create publisher: %w", err)
	}
	defer pub.Close() //nolint:errcheck // best-effort cleanup

	gen := publisher.NewGenerator()

	// Distribute publish rate: 50% public, 50% group.
	// User-scoped channels are receive-only (each user has a unique dm.<subject> channel —
	// publishing to individual channels at scale is impractical for load testing).
	publicRate := max(publishRate/2, 1)
	groupRate := max(publishRate-publicRate, 1)

	endTime := time.Now().Add(duration)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for time.Now().Before(endTime) {
		select {
		case <-ctx.Done():
			goto done
		case <-ticker.C:
			// Publish to public channel
			for range publicRate {
				data, _ := gen.Next(publicChannel) // json.Marshal on literal map of primitives cannot fail
				if err := pub.Publish(ctx, publicChannel, data); err == nil {
					run.Collector.PublicSent.Add(1)
					run.Collector.MessagesSent.Add(1)
				} else {
					run.Collector.ErrorsTotal.Add(1)
				}
			}
			// Publish to group channel
			for range groupRate {
				data, _ := gen.Next(groupChannel) // json.Marshal on literal map of primitives cannot fail
				if err := pub.Publish(ctx, groupChannel, data); err == nil {
					run.Collector.GroupScopedSent.Add(1)
					run.Collector.MessagesSent.Add(1)
				} else {
					run.Collector.ErrorsTotal.Add(1)
				}
			}
		}
	}

done:
	run.Collector.ConnectionsActive.Store(0)
	// Close canaries and wait for ReadLoop goroutines before reading stats
	for _, cleanup := range canaryCleanups {
		cleanup()
	}
	canaryWg.Wait()
	stats := ct.aggregateStats()
	run.Collector.MessagesLost.Store(stats.Gaps)
	run.Collector.MessagesDuplicated.Store(stats.Duplicates)

	return &metrics.Report{
		TestType: "load:channels",
		Status:   "pass",
		Metrics:  run.Collector.Snapshot(),
	}, nil
}
