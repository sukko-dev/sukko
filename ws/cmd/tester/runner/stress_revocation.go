package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gobwas/ws"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	"github.com/sukko-dev/sukko/cmd/tester/stats"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Stress:revocation constants (all named per spec §I — no magic numbers).
const (
	stressRevocationSub     = "stress-revocation-user"
	stressKeepSub           = "stress-keep-user"
	stressRevokeSub         = "stress-revoke-user"
	stressRevocationChannel = "stress.revocation.test"

	stressForceDisconnectTimeout = 10 * time.Second // S1-AC1: p99 MUST be ≤ this
	stressP100SoftGuard          = 30 * time.Second // injectable close-event collection deadline
	stressPublishRate            = 10               // messages per second for S2 publisher
	stressS2Connections          = 1000             // total for S2 (500 keep + 500 revoke)
	stressS3VerifyCount          = 100              // reconnect connections verified in S3
	stressPassThreshold          = 0.95             // 95% pass threshold used by S1-AC5 and S3
)

type closeEvent struct {
	index int
	t     time.Time
}

// runStressRevocation is the production entry point — uses stressP100SoftGuard.
func runStressRevocation(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	return runStressRevocationWithDeadline(ctx, run, logger, stressP100SoftGuard)
}

// runStressRevocationWithDeadline is the testable entry point — accepts injectable deadline.
func runStressRevocationWithDeadline(ctx context.Context, run *TestRun, logger zerolog.Logger, closeEventDeadline time.Duration) (*metrics.Report, error) {
	gwURL := run.Config.GatewayURL
	minter := run.authResult.Minter
	tenantID := run.authResult.TenantID

	// Edition gate (§XIII).
	edition, err := fetchCurrentEdition(ctx, gwURL)
	if err != nil {
		return nil, fmt.Errorf("fetch edition: %w", err)
	}
	if !license.EditionHasFeature(edition, license.TokenRevocation) {
		return &metrics.Report{
			TestType: "stress",
			Status:   metrics.ReportStatusPass,
			Checks: []metrics.CheckResult{{
				Name:   "edition-gate",
				Status: metrics.CheckStatusSkip,
				Error:  "stress:revocation requires Pro+ edition (current: " + string(edition) + ")",
			}},
		}, nil
	}

	if run.authResult.TokenFunc == nil {
		return nil, errors.New("runStressRevocation: TokenFunc required (auth_mode must produce tokens)")
	}

	maxConnections := run.Config.Connections
	if maxConnections <= 0 {
		maxConnections = defaultStressConnections
	}
	rampRate := run.Config.RampRate
	if rampRate <= 0 {
		rampRate = defaultStressRampRate
	}

	var allChecks []metrics.CheckResult
	latencyHist := stats.NewHistogram()

	// ── S1: mass force-disconnect ─────────────────────────────────────────────
	logger.Info().Int("connections", maxConnections).Msg("stress:revocation S1 starting")

	s1CloseEvents := make(chan closeEvent, maxConnections)
	s1Once := make([]sync.Once, maxConnections)

	s1Pool := testerws.NewPool(logger)

	if err := s1Pool.RampUp(ctx, testerws.PoolConfig{
		GatewayURL: gwURL,
		TokenFunc: func(i int) string {
			tok, mintErr := minter.MintWithClaims(auth.MintOptions{
				Subject:  stressRevocationSub,
				JTI:      fmt.Sprintf("%s-s1-%d", run.ID, i),
				IssuedAt: time.Now(),
			})
			if mintErr != nil {
				logger.Warn().Err(mintErr).Int("i", i).Msg("S1 mint failed")
				return ""
			}
			return tok
		},
		OnClose: func(connIndex int, code ws.StatusCode, closeTime time.Time) {
			s1Once[connIndex].Do(func() {
				select {
				case s1CloseEvents <- closeEvent{index: connIndex, t: closeTime}:
				default:
				}
			})
		},
	}, maxConnections, rampRate); err != nil {
		logger.Warn().Err(err).Msg("S1 ramp up interrupted")
	}

	actualConnected := int(s1Pool.Active())
	run.Collector.ConnectionsActive.Store(int64(actualConnected))
	run.Collector.ConnectionsTotal.Store(int64(actualConnected))

	// Baseline gateway counter before revocation.
	var s1Baseline float64
	if run.Config.GatewayMetricsURL != "" {
		if gauges, scrapeErr := scrapeGatewayMetrics(ctx, run.Config.GatewayMetricsURL+"/metrics"); scrapeErr == nil {
			s1Baseline = gauges["gateway_token_force_disconnects_total"]
		}
	}

	// Revoke by sub (with retry for 5xx/429).
	callerTok, callerErr := minter.MintWithClaims(auth.MintOptions{
		Subject: stressRevocationSub,
		JTI:     run.ID + "-s1-caller",
	})
	if callerErr != nil {
		s1Pool.Drain()
		return nil, fmt.Errorf("S1 mint caller token: %w", callerErr)
	}
	httpClient := &http.Client{Timeout: editionHTTPTimeout}
	revStatus, revErr := withRetry(ctx, func() (int, error) {
		code, err := revokeTokenWithClient(ctx, httpClient, gwURL, callerTok, tenantID, revokeRequest{Sub: stressRevocationSub})
		if err != nil {
			return 0, err
		}
		if code == http.StatusTooManyRequests || code >= 500 {
			return code, fmt.Errorf("HTTP %d", code)
		}
		return code, nil
	}, len(revocationReconnectBackoffs)+1, revocationReconnectBackoffs)
	if revErr != nil || revStatus != http.StatusOK {
		s1Pool.Drain()
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s1-revoke", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", revStatus, revErr),
		})
		return buildStressRevocationReport(allChecks, run), nil
	}

	t0 := time.Now() // latency clock starts after confirmed revocation

	// Collect close events with injectable deadline.
	latencies := make([]time.Duration, maxConnections)
	var received int
	dl := time.NewTimer(closeEventDeadline)
	defer dl.Stop()
S1Drain:
	for received < actualConnected {
		select {
		case ev := <-s1CloseEvents:
			if ev.t.After(t0) {
				latencies[ev.index] = ev.t.Sub(t0)
			}
			received++
		case <-dl.C:
			logger.Warn().Int("received", received).Int("expected", actualConnected).
				Dur("deadline", closeEventDeadline).Msg("S1 close-event deadline exceeded")
			break S1Drain
		case <-ctx.Done():
			break S1Drain
		}
	}
	s1Pool.Drain() // safe: any subsequent OnClose sends to buffered channel are non-blocking

	for i := range latencies {
		if latencies[i] > 0 {
			latencyHist.Record(latencies[i])
		}
	}
	snap := latencyHist.Snapshot()

	// S1-AC1: p99 latency.
	p99 := time.Duration(snap.P99 * float64(time.Millisecond))
	if snap.Count > 0 && p99 <= stressForceDisconnectTimeout {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s1-force-disconnect-p99", Status: metrics.CheckStatusPass,
			Latency: p99.Round(time.Millisecond).String(),
		})
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s1-force-disconnect-p99", Status: metrics.CheckStatusFail,
			Error:   fmt.Sprintf("p99 %s exceeds %s (count=%d)", p99.Round(time.Millisecond), stressForceDisconnectTimeout, snap.Count),
			Latency: p99.Round(time.Millisecond).String(),
		})
	}

	// S1-AC2: gateway counter delta.
	if run.Config.GatewayMetricsURL != "" {
		select {
		case <-time.After(revocationSettlementWindow):
		case <-ctx.Done():
		}
		if gauges, scrapeErr := scrapeGatewayMetrics(ctx, run.Config.GatewayMetricsURL+"/metrics"); scrapeErr == nil {
			delta := gauges["gateway_token_force_disconnects_total"] - s1Baseline
			if delta >= float64(actualConnected) {
				allChecks = append(allChecks, metrics.CheckResult{Name: "s1-force-disconnect-counter", Status: metrics.CheckStatusPass})
			} else {
				allChecks = append(allChecks, metrics.CheckResult{
					Name: "s1-force-disconnect-counter", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("expected delta ≥ %d, got %.0f", actualConnected, delta),
				})
			}
		} else {
			allChecks = append(allChecks, metrics.CheckResult{
				Name: "s1-force-disconnect-counter", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("metrics scrape: %v", scrapeErr),
			})
		}
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name:   "s1-force-disconnect-counter",
			Status: metrics.CheckStatusSkip,
			Error:  "TESTER_GATEWAY_METRICS_URL not set",
		})
	}

	// S1-AC5: close-event count (≥95% received).
	threshold := int(float64(actualConnected) * stressPassThreshold)
	if received >= threshold {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s1-close-event-count", Status: metrics.CheckStatusPass,
			Latency: fmt.Sprintf("%d/%d", received, actualConnected),
		})
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s1-close-event-count", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("received %d close events, want ≥ %d (95%% of %d)", received, threshold, actualConnected),
		})
	}

	// ── S2: selective disconnect with active delivery ─────────────────────────
	logger.Info().Msg("stress:revocation S2 starting")

	// Provision channel + routing rules for message delivery.
	provClient := run.authResult.ProvClient
	if provClient != nil {
		if setErr := provClient.SetChannelRules(ctx, tenantID, map[string]any{
			"public": []string{stressRevocationChannel},
		}); setErr != nil {
			allChecks = append(allChecks, metrics.CheckResult{
				Name: "s2-channel-provisioned", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("set channel rules: %v", setErr),
			})
			return buildStressRevocationReport(allChecks, run), nil
		}
		if setErr := provClient.SetRoutingRules(ctx, tenantID, []map[string]any{
			{"pattern": stressRevocationChannel, "ingress_topic": stressRevocationChannel, "priority": 1},
		}); setErr != nil {
			allChecks = append(allChecks, metrics.CheckResult{
				Name: "s2-channel-provisioned", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("set routing rules: %v", setErr),
			})
			return buildStressRevocationReport(allChecks, run), nil
		}
		defer func() { //nolint:contextcheck // cleanup must survive parent cancellation
			cleanupCtx := context.Background()
			_ = provClient.DeleteRoutingRules(cleanupCtx, tenantID)
		}()
	}

	fullChannel := tenantID + "." + stressRevocationChannel

	// Canary connection with SequenceTracker.
	canaryTok, canaryErr := minter.MintWithClaims(auth.MintOptions{
		Subject: "stress-revocation-canary",
		JTI:     run.ID + "-s2-canary",
	})
	if canaryErr != nil {
		return nil, fmt.Errorf("S2 mint canary token: %w", canaryErr)
	}
	canaryTracker := newChannelTrackers()
	canaryClient, canaryConnErr := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: gwURL,
		Token:      canaryTok,
		Logger:     logger,
		OnMessage: func(msg testerws.Message) {
			canaryTracker.track(msg)
		},
	})
	if canaryConnErr != nil {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s2-canary-connect", Status: metrics.CheckStatusFail,
			Error: canaryConnErr.Error(),
		})
		return buildStressRevocationReport(allChecks, run), nil
	}
	canaryReadCtx, canaryReadCancel := context.WithCancel(ctx)
	var canaryWg sync.WaitGroup
	// Cleanup order: cancel context → close socket (unblocks ReadLoop) → wait.
	defer func() {
		canaryReadCancel()
		_ = canaryClient.Close()
		canaryWg.Wait()
	}()
	if subErr := canaryClient.Subscribe([]string{fullChannel}); subErr != nil {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s2-canary-connect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("canary subscribe: %v", subErr),
		})
		return buildStressRevocationReport(allChecks, run), nil
	}
	canaryWg.Go(func() {
		defer logging.RecoverPanic(logger, "stress-revocation-canary-read", nil)
		_, _ = canaryClient.ReadLoop(canaryReadCtx)
	})

	// S2 pools: 500 keep + 500 revoke.
	const s2Half = stressS2Connections / 2
	s2CloseEvents := make(chan closeEvent, s2Half)
	s2Once := make([]sync.Once, stressS2Connections)

	s2Pool := testerws.NewPool(logger)
	if err := s2Pool.RampUp(ctx, testerws.PoolConfig{
		GatewayURL: gwURL,
		Channels:   []string{fullChannel},
		TokenFunc: func(i int) string {
			sub := stressKeepSub
			if i >= s2Half {
				sub = stressRevokeSub
			}
			tok, mintErr := minter.MintWithClaims(auth.MintOptions{
				Subject: sub,
				JTI:     fmt.Sprintf("%s-s2-%d", run.ID, i),
			})
			if mintErr != nil {
				return ""
			}
			return tok
		},
		OnClose: func(connIndex int, code ws.StatusCode, closeTime time.Time) {
			if connIndex >= s2Half {
				s2Once[connIndex].Do(func() {
					select {
					case s2CloseEvents <- closeEvent{index: connIndex, t: closeTime}:
					default:
					}
				})
			}
		},
	}, stressS2Connections, rampRate); err != nil {
		logger.Warn().Err(err).Msg("S2 ramp up interrupted")
	}
	defer s2Pool.Drain()

	// Publisher goroutine.
	pub, pubErr := publisher.NewDirectPublisher(ctx, gwURL, run.authResult.TokenFunc(0))
	if pubErr != nil {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s2-publisher", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("create publisher: %v", pubErr),
		})
		return buildStressRevocationReport(allChecks, run), nil
	}
	defer pub.Close() //nolint:errcheck // best-effort cleanup

	gen := publisher.NewGenerator()
	publishInterval := time.Second / time.Duration(stressPublishRate)
	pubTicker := time.NewTicker(publishInterval)
	defer pubTicker.Stop()
	pubStop := make(chan struct{})

	var pubWg sync.WaitGroup
	pubWg.Go(func() {
		defer logging.RecoverPanic(logger, "stress-revocation-publish", nil)
		for {
			select {
			case <-pubStop:
				return
			case <-ctx.Done():
				return
			case <-pubTicker.C:
				data, _ := gen.Next(fullChannel)
				if err := pub.Publish(ctx, fullChannel, data); err != nil {
					run.Collector.ErrorsTotal.Add(1)
				} else {
					run.Collector.MessagesSent.Add(1)
				}
			}
		}
	})

	// Capture S2 baseline.
	var s2Baseline float64
	if run.Config.GatewayMetricsURL != "" {
		if gauges, scrapeErr := scrapeGatewayMetrics(ctx, run.Config.GatewayMetricsURL+"/metrics"); scrapeErr == nil {
			s2Baseline = gauges["gateway_token_force_disconnects_total"]
		}
	}

	// Revoke the revoke sub.
	revokeTok, revokeErr := minter.MintWithClaims(auth.MintOptions{
		Subject: stressRevokeSub,
		JTI:     run.ID + "-s2-revoke-caller",
	})
	if revokeErr != nil {
		close(pubStop)
		pubWg.Wait()
		return nil, fmt.Errorf("S2 mint revoke caller token: %w", revokeErr)
	}
	s2RevStatus, s2RevErr := withRetry(ctx, func() (int, error) {
		code, err := revokeTokenWithClient(ctx, httpClient, gwURL, revokeTok, tenantID, revokeRequest{Sub: stressRevokeSub})
		if err != nil {
			return 0, err
		}
		if code == http.StatusTooManyRequests || code >= 500 {
			return code, fmt.Errorf("HTTP %d", code)
		}
		return code, nil
	}, len(revocationReconnectBackoffs)+1, revocationReconnectBackoffs)
	if s2RevErr != nil || s2RevStatus != http.StatusOK {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s2-revoke", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", s2RevStatus, s2RevErr),
		})
		close(pubStop)
		pubWg.Wait()
		return buildStressRevocationReport(allChecks, run), nil
	}

	// Wait for 500 close events.
	var s2Received int
	s2Dl := time.NewTimer(stressP100SoftGuard)
	defer s2Dl.Stop()
S2Drain:
	for s2Received < s2Half {
		select {
		case <-s2CloseEvents:
			s2Received++
		case <-s2Dl.C:
			logger.Warn().Int("received", s2Received).Int("expected", s2Half).Msg("S2 close-event deadline exceeded")
			break S2Drain
		case <-ctx.Done():
			break S2Drain
		}
	}

	select {
	case <-time.After(revocationSettlementWindow):
	case <-ctx.Done():
	}
	close(pubStop)
	pubWg.Wait()

	// S2-AC1: canary gaps == 0.
	canaryStats := canaryTracker.aggregateStats()
	if canaryStats.Gaps == 0 {
		allChecks = append(allChecks, metrics.CheckResult{Name: "s2-canary-no-gaps", Status: metrics.CheckStatusPass})
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s2-canary-no-gaps", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("canary detected %d sequence gaps during revocation", canaryStats.Gaps),
		})
	}

	// S2-AC2: gateway counter delta.
	if run.Config.GatewayMetricsURL != "" {
		if gauges, scrapeErr := scrapeGatewayMetrics(ctx, run.Config.GatewayMetricsURL+"/metrics"); scrapeErr == nil {
			delta := gauges["gateway_token_force_disconnects_total"] - s2Baseline
			if delta >= float64(s2Half) {
				allChecks = append(allChecks, metrics.CheckResult{Name: "s2-force-disconnect-counter", Status: metrics.CheckStatusPass})
			} else {
				allChecks = append(allChecks, metrics.CheckResult{
					Name: "s2-force-disconnect-counter", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("expected delta ≥ %d, got %.0f", s2Half, delta),
				})
			}
		}
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name:   "s2-force-disconnect-counter",
			Status: metrics.CheckStatusSkip,
			Error:  "TESTER_GATEWAY_METRICS_URL not set",
		})
	}

	// ── S3: reconnect verification ────────────────────────────────────────────
	logger.Info().Msg("stress:revocation S3 starting")

	// Mint tokens with IssuedAt=now+1s to ensure iat > revoked_at.
	connectTime := time.Now().Add(time.Second)
	var s3Success int
	s3Clients := make([]*testerws.Client, 0, stressS3VerifyCount)
	defer func() {
		for _, c := range s3Clients {
			_ = c.Close()
		}
	}()
	for i := range stressS3VerifyCount {
		tok, mintErr := minter.MintWithClaims(auth.MintOptions{
			Subject:  stressRevocationSub,
			JTI:      fmt.Sprintf("%s-s3-%d", run.ID, i),
			IssuedAt: connectTime,
		})
		if mintErr != nil {
			continue
		}
		c, connErr := testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: gwURL,
			Token:      tok,
			Logger:     logger,
		})
		if connErr != nil {
			continue
		}
		s3Clients = append(s3Clients, c)
		s3Success++
	}

	s3Want := int(float64(stressS3VerifyCount) * stressPassThreshold)
	if s3Success >= s3Want {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s3-reconnect", Status: metrics.CheckStatusPass,
			Latency: fmt.Sprintf("%d/%d connected", s3Success, stressS3VerifyCount),
		})
	} else {
		allChecks = append(allChecks, metrics.CheckResult{
			Name: "s3-reconnect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("only %d/%d reconnected (want ≥ %d)", s3Success, stressS3VerifyCount, s3Want),
		})
	}

	return buildStressRevocationReport(allChecks, run), nil
}

func buildStressRevocationReport(checks []metrics.CheckResult, run *TestRun) *metrics.Report {
	status := metrics.ReportStatusPass
	for _, c := range checks {
		if c.Status == metrics.CheckStatusFail {
			status = metrics.ReportStatusFail
			break
		}
	}
	return &metrics.Report{
		TestType: "stress",
		Status:   status,
		Metrics:  run.Collector.Snapshot(),
		Checks:   checks,
	}
}
