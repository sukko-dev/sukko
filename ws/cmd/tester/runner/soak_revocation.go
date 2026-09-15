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
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Soak:revocation constants (all named per spec §I — no magic numbers).
const (
	soakRevocationSub   = "soak-revocation-user"
	soakRevocationS4Sub = "soak-revocation-s4-user" // S4 uses a distinct sub

	soakRevocationCycleInterval = 30 * time.Second
	soakRevocationExpiry        = 60 * time.Second // JTI exp for S4 pruner validation

	soakPrunePollStart    = 55 * time.Second  // initial wait before polling for pruning
	soakPrunePollInterval = 5 * time.Second   // between polls
	soakPruneFailTimeout  = 120 * time.Second // S4 overall fail deadline

	soakMemoryGrowthThreshold        = 0.20 // S3-AC1: gateway memory MUST NOT grow > 20% above post-ramp baseline
	soakRevocationErrorRateThreshold = 0.05 // cumulative error rate (>5% = fail; at exactly 5% = pass)
	soakMapPlateauWindowFraction     = 0.25 // S3-AC2: measure growth in last 25% of soak duration
	soakMapPlateauThreshold          = 0.05 // S3-AC2: plateau growth MUST be ≤ 5% of peak
	soakGoroutineDriftThreshold      = 0.10 // S3-AC3: goroutines MUST stay within 10% of post-ramp baseline

	soakS4Count = 100 // S4: number of JTIs revoked to validate gateway pruner
)

// soakRevocationCloseEvent represents a connection close notification from the pool.
type soakRevocationCloseEvent struct {
	index int
	code  ws.StatusCode
}

// soakGaugeSample is a point-in-time metric sample.
type soakGaugeSample struct {
	mapEntries float64
	goroutines float64
	memBytes   float64
}

// runSoakRevocation is the production entry point.
func runSoakRevocation(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	return runSoakRevocationWithTimeout(ctx, run, logger, soakPerConnectionCloseTimeout)
}

// soakPerConnectionCloseTimeout is the per-slot close-wait before marking a slot dead.
const soakPerConnectionCloseTimeout = 5 * time.Second

// runSoakRevocationWithTimeout is testable — accepts injectable per-connection close timeout.
func runSoakRevocationWithTimeout(ctx context.Context, run *TestRun, logger zerolog.Logger, perConnCloseTimeout time.Duration) (*metrics.Report, error) {
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
			TestType: "soak",
			Status:   metrics.ReportStatusPass,
			Checks: []metrics.CheckResult{{
				Name:   "edition-gate",
				Status: metrics.CheckStatusSkip,
				Error:  "soak:revocation requires Pro+ edition (current: " + string(edition) + ")",
			}},
		}, nil
	}

	if run.authResult.TokenFunc == nil {
		return nil, errors.New("runSoakRevocation: TokenFunc required")
	}

	connections := run.Config.Connections
	if connections <= 0 {
		connections = defaultSoakConnections
	}
	rampRate := run.Config.RampRate
	if rampRate <= 0 {
		rampRate = defaultRampRate
	}
	duration, err := time.ParseDuration(run.Config.Duration)
	if err != nil || duration <= 0 {
		duration = defaultSoakDuration
	}
	revocationsPerCycle := run.Config.RevocationsPerCycle
	if revocationsPerCycle <= 0 {
		revocationsPerCycle = defaultSoakRevocationsPerCycle
	}

	var checks []metrics.CheckResult
	httpClient := &http.Client{Timeout: editionHTTPTimeout}

	// ── preflight check (only when metrics URL is set) ────────────────
	metricsURL := run.Config.GatewayMetricsURL
	metricsEnabled := metricsURL != ""
	if metricsEnabled {
		preflightOK, preflightChecks := runSoakRevocationPreflight(ctx, minter, tenantID, gwURL, metricsURL+"/metrics", run.ID, httpClient, logger)
		checks = append(checks, preflightChecks...)
		if !preflightOK {
			return buildSoakRevocationReport(checks, run, false), nil
		}
	}

	// ── Pool ramp-up ─────────────────────────────────────────────────────────
	currentJTI := make([]string, connections)
	for i := range connections {
		currentJTI[i] = fmt.Sprintf("%s-soak-%d-init", run.ID, i)
	}
	deadSlots := make([]bool, connections)

	// Per-slot close-event channels (capacity 1, non-blocking send ensures at-most-one event).
	closeChans := make([]chan soakRevocationCloseEvent, connections)
	for i := range connections {
		closeChans[i] = make(chan soakRevocationCloseEvent, 1)
	}

	pool := testerws.NewPool(logger)
	if err := pool.RampUp(ctx, testerws.PoolConfig{
		GatewayURL: gwURL,
		TokenFunc: func(i int) string {
			tok, mintErr := minter.MintWithClaims(auth.MintOptions{
				Subject: soakRevocationSub,
				JTI:     currentJTI[i],
			})
			if mintErr != nil {
				return ""
			}
			return tok
		},
		OnClose: func(connIndex int, code ws.StatusCode, _ time.Time) {
			select {
			case closeChans[connIndex] <- soakRevocationCloseEvent{index: connIndex, code: code}:
			default:
			}
		},
	}, connections, rampRate); err != nil {
		logger.Warn().Err(err).Msg("soak:revocation ramp up interrupted")
	}
	defer pool.Drain()

	run.Collector.ConnectionsActive.Store(pool.Active())
	run.Collector.ConnectionsTotal.Store(pool.Active())

	// ── S3: metrics monitoring goroutine ─────────────────────────────────────
	// Collects gateway gauge samples during soak for AC1/AC2/AC3.
	var (
		samplesMu      sync.Mutex
		samples        []soakGaugeSample
		baselineSample soakGaugeSample
		baselineSet    bool
	)

	var monitorWg sync.WaitGroup
	if metricsEnabled {
		monitorWg.Go(func() {
			defer logging.RecoverPanic(logger, "soak-revocation-monitor", nil)
			ticker := time.NewTicker(run.Config.GatewayMetricsInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					gauges, scrapeErr := scrapeGatewayMetrics(ctx, metricsURL+"/metrics")
					if scrapeErr != nil {
						logger.Warn().Err(scrapeErr).Msg("soak metrics scrape failed")
						continue
					}
					s := soakGaugeSample{
						mapEntries: gauges["gateway_token_revocation_map_entries"],
						goroutines: gauges["go_goroutines"],
						memBytes:   gauges["go_memstats_sys_bytes"],
					}
					samplesMu.Lock()
					if !baselineSet {
						baselineSample = s
						baselineSet = true
					}
					samples = append(samples, s)
					samplesMu.Unlock()
				}
			}
		})
	}

	// ── Soak cycle loop ───────────────────────────────────────────────────────
	cycleTicker := time.NewTicker(soakRevocationCycleInterval)
	defer cycleTicker.Stop()
	deadline := time.After(duration)

	var totalRevocations, revocationErrors int

	logger.Info().Dur("duration", duration).Int("revocations_per_cycle", revocationsPerCycle).Msg("soak:revocation loop starting")

SoakLoop:
	for {
		select {
		case <-ctx.Done():
			break SoakLoop
		case <-deadline:
			break SoakLoop
		case <-cycleTicker.C:
			// Revoke + reconnect up to revocationsPerCycle non-dead slots.
			revoked := 0
			for i := range connections {
				if revoked >= revocationsPerCycle {
					break
				}
				if deadSlots[i] {
					continue
				}

				// Close old connection.
				pool.CloseAt(i)

				// Wait for close event with per-connection timeout.
				select {
				case <-closeChans[i]:
				case <-time.After(perConnCloseTimeout):
					logger.Warn().Int("slot", i).Msg("soak: close timeout; marking slot dead")
					deadSlots[i] = true
					run.Collector.ConnectionsFailed.Add(1)
					revoked++
					continue
				}
				// Reset per-slot close infrastructure for FillSlot's new connection.
				closeChans[i] = make(chan soakRevocationCloseEvent, 1)

				// Revoke old JTI.
				callerTok, mintErr := minter.MintWithClaims(auth.MintOptions{
					Subject: soakRevocationSub,
					JTI:     run.ID + fmt.Sprintf("-cycle-caller-%d", i),
				})
				if mintErr == nil {
					revStatus, revErr := withRetry(ctx, func() (int, error) {
						code, err := revokeTokenWithClient(ctx, httpClient, gwURL, callerTok, tenantID,
							revokeRequest{JTI: currentJTI[i]})
						if err != nil {
							return 0, err
						}
						if code == http.StatusTooManyRequests || code >= 500 {
							return code, fmt.Errorf("HTTP %d", code)
						}
						return code, nil
					}, len(revocationReconnectBackoffs)+1, revocationReconnectBackoffs)
					totalRevocations++
					if revErr != nil || revStatus != http.StatusOK {
						revocationErrors++
					}
				}

				// FillSlot with new JTI.
				newJTI := fmt.Sprintf("%s-soak-%d-c%d", run.ID, i, totalRevocations)
				freshCfg := testerws.PoolConfig{
					GatewayURL: gwURL,
					TokenFunc: func(_ int) string {
						tok, mintErr := minter.MintWithClaims(auth.MintOptions{
							Subject: soakRevocationSub,
							JTI:     newJTI,
						})
						if mintErr != nil {
							return ""
						}
						return tok
					},
					OnClose: func(connIndex int, code ws.StatusCode, _ time.Time) {
						select {
						case closeChans[i] <- soakRevocationCloseEvent{index: connIndex, code: code}:
						default:
						}
					},
				}
				if fillErr := pool.FillSlot(i, freshCfg); fillErr != nil { //nolint:contextcheck // FillSlot intentionally uses context.Background() — soak connections must outlive per-cycle cancellations (see ws/pool.go comment)
					logger.Warn().Err(fillErr).Int("slot", i).Msg("soak: FillSlot failed; marking dead")
					deadSlots[i] = true
					run.Collector.ConnectionsFailed.Add(1)
				} else {
					currentJTI[i] = newJTI
				}

				run.Collector.ConnectionsActive.Store(pool.Active())
				revoked++
			}

			// Detect all-slots-dead: zero revocations with live slots expected means every
			// slot has failed. A vacuous pass (0 revocations → 0 errors → 0% error rate)
			// would silently report success while performing no testing.
			if revoked == 0 && revocationsPerCycle > 0 {
				allDead := true
				for _, dead := range deadSlots {
					if !dead {
						allDead = false
						break
					}
				}
				if allDead {
					logger.Error().Msg("soak:revocation all connection slots dead — aborting")
					checks = append(checks, metrics.CheckResult{
						Name: "s3-revocation-error-rate", Status: metrics.CheckStatusFail,
						Error: "all connection slots dead — zero revocations performed",
					})
					monitorWg.Wait()
					return buildSoakRevocationReport(checks, run, true), nil
				}
			}

			// check cumulative error rate.
			if isErrorRateExceeded(revocationErrors, totalRevocations, soakRevocationErrorRateThreshold) {
				logger.Error().Int("errors", revocationErrors).Int("total", totalRevocations).
					Float64("rate", float64(revocationErrors)/float64(totalRevocations)).
					Msg("soak:revocation error rate exceeded threshold")
				monitorWg.Wait() // drain monitor goroutine
				checks = append(checks, metrics.CheckResult{
					Name: "s3-revocation-error-rate", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("cumulative error rate %.1f%% exceeds %.0f%% threshold (%d/%d)",
						100*float64(revocationErrors)/float64(totalRevocations),
						100*soakRevocationErrorRateThreshold, revocationErrors, totalRevocations),
				})
				return buildSoakRevocationReport(checks, run, true), nil
			}
		}
	}

	// Drain monitoring goroutine.
	monitorWg.Wait()

	// ── S3 ACs: evaluate gateway gauge samples ────────────────────────────────
	samplesMu.Lock()
	allSamples := make([]soakGaugeSample, len(samples))
	copy(allSamples, samples)
	bl := baselineSample
	blSet := baselineSet
	samplesMu.Unlock()

	switch {
	case !metricsEnabled:
		for _, name := range []string{"s3-memory-growth", "s3-map-plateau", "s3-goroutine-drift"} {
			checks = append(checks, metrics.CheckResult{
				Name: name, Status: metrics.CheckStatusSkip, Error: "TESTER_GATEWAY_METRICS_URL not set",
			})
		}
	case !blSet || len(allSamples) < 2:
		for _, name := range []string{"s3-memory-growth", "s3-map-plateau", "s3-goroutine-drift"} {
			checks = append(checks, metrics.CheckResult{
				Name: name, Status: metrics.CheckStatusSkip, Error: "insufficient metric samples",
			})
		}
	default:
		// S3-AC1: memory growth ≤ 20% above post-ramp baseline.
		finalMem := allSamples[len(allSamples)-1].memBytes
		if bl.memBytes > 0 && finalMem/bl.memBytes > (1+soakMemoryGrowthThreshold) {
			checks = append(checks, metrics.CheckResult{
				Name: "s3-memory-growth", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("memory grew %.1f%% (baseline=%.0fMB, final=%.0fMB, limit=%.0f%%)",
					100*(finalMem/bl.memBytes-1), bl.memBytes/1e6, finalMem/1e6, 100*soakMemoryGrowthThreshold),
			})
		} else {
			checks = append(checks, metrics.CheckResult{Name: "s3-memory-growth", Status: metrics.CheckStatusPass})
		}

		// S3-AC2: revocation map plateau — growth in last 25% of samples ≤ 5% of peak.
		windowStart := int(float64(len(allSamples)) * (1 - soakMapPlateauWindowFraction))
		var peakMap, windowMax, windowMin float64
		for _, s := range allSamples {
			if s.mapEntries > peakMap {
				peakMap = s.mapEntries
			}
		}
		if windowStart < len(allSamples) {
			windowMin = allSamples[windowStart].mapEntries
			windowMax = allSamples[windowStart].mapEntries
			for _, s := range allSamples[windowStart:] {
				if s.mapEntries > windowMax {
					windowMax = s.mapEntries
				}
				if s.mapEntries < windowMin {
					windowMin = s.mapEntries
				}
			}
		}
		plateauGrowth := windowMax - windowMin
		if peakMap > 0 && plateauGrowth/peakMap <= soakMapPlateauThreshold {
			checks = append(checks, metrics.CheckResult{Name: "s3-map-plateau", Status: metrics.CheckStatusPass})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "s3-map-plateau", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("map growth %.0f entries in last %.0f%% of soak (peak=%.0f, threshold=%.0f%%)",
					plateauGrowth, 100*soakMapPlateauWindowFraction, peakMap, 100*soakMapPlateauThreshold),
			})
		}

		// S3-AC3: goroutine drift within 10% of baseline.
		finalGoroutines := allSamples[len(allSamples)-1].goroutines
		if bl.goroutines > 0 {
			drift := (finalGoroutines - bl.goroutines) / bl.goroutines
			if drift < 0 {
				drift = -drift
			}
			if drift <= soakGoroutineDriftThreshold {
				checks = append(checks, metrics.CheckResult{Name: "s3-goroutine-drift", Status: metrics.CheckStatusPass})
			} else {
				checks = append(checks, metrics.CheckResult{
					Name: "s3-goroutine-drift", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("goroutines drifted %.1f%% (baseline=%.0f, final=%.0f, limit=%.0f%%)",
						100*drift, bl.goroutines, finalGoroutines, 100*soakGoroutineDriftThreshold),
				})
			}
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "s3-goroutine-drift", Status: metrics.CheckStatusSkip, Error: "zero baseline goroutines",
			})
		}
	}

	// ── S4: JTI revocation with Exp → pruner validation ──────────────────────
	if !metricsEnabled {
		checks = append(checks, metrics.CheckResult{
			Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusSkip,
			Error: "TESTER_GATEWAY_METRICS_URL not set",
		})
		return buildSoakRevocationReport(checks, run, false), nil
	}

	logger.Info().Msg("soak:revocation S4 starting")

	// Scrape baseline map entries before S4 revocations.
	s4Gauges, s4ScrapeErr := scrapeGatewayMetrics(ctx, metricsURL+"/metrics")
	if s4ScrapeErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("pre-S4 scrape: %v", s4ScrapeErr),
		})
		return buildSoakRevocationReport(checks, run, false), nil
	}
	s4Baseline := s4Gauges["gateway_token_revocation_map_entries"]

	// Revoke 100 JTIs with Exp set — S4 sub is distinct from main soak sub.
	expAt := time.Now().Add(soakRevocationExpiry)
	expUnix := expAt.Unix()

	var s4RevErrors int
	for i := range soakS4Count {
		callerTok, mintErr := minter.MintWithClaims(auth.MintOptions{
			Subject: soakRevocationS4Sub,
			JTI:     fmt.Sprintf("%s-s4-%d", run.ID, i),
		})
		if mintErr != nil {
			s4RevErrors++
			continue
		}
		revStatus, revErr := withRetry(ctx, func() (int, error) {
			code, err := revokeTokenWithClient(ctx, httpClient, gwURL, callerTok, tenantID,
				revokeRequest{
					JTI: fmt.Sprintf("%s-s4-%d", run.ID, i),
					Exp: &expUnix,
				})
			if err != nil {
				return 0, err
			}
			if code == http.StatusTooManyRequests || code >= 500 {
				return code, fmt.Errorf("HTTP %d", code)
			}
			return code, nil
		}, len(revocationReconnectBackoffs)+1, revocationReconnectBackoffs)
		if revErr != nil || revStatus != http.StatusOK {
			s4RevErrors++
		}
	}
	if s4RevErrors == soakS4Count {
		checks = append(checks, metrics.CheckResult{
			Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusFail,
			Error: "all S4 revocations failed",
		})
		return buildSoakRevocationReport(checks, run, false), nil
	}

	s4RevTime := time.Now() // reference point for poll timers

	// Wait soakPrunePollStart before beginning to poll.
	select {
	case <-time.After(soakPrunePollStart - time.Since(s4RevTime)):
	case <-ctx.Done():
		checks = append(checks, metrics.CheckResult{
			Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusSkip, Error: "context canceled",
		})
		return buildSoakRevocationReport(checks, run, false), nil
	}

	// Poll until map entries drop back to baseline (entries pruned after expiry).
	// Add headroom so the internal pollGaugesUntil deadline fires first with a
	// descriptive per-metric error, rather than the opaque ctx deadline exceeded.
	pruneCtx, pruneCancel := context.WithTimeout(ctx, soakPruneFailTimeout+5*time.Second)
	defer pruneCancel()

	finalEntries, pollErr := pollGaugesUntil(pruneCtx,
		func(pCtx context.Context) (map[string]float64, error) {
			return scrapeGatewayMetrics(pCtx, metricsURL+"/metrics")
		},
		"gateway_token_revocation_map_entries",
		func(v float64) bool { return v <= s4Baseline },
		soakPrunePollInterval, soakPruneFailTimeout,
	)
	if pollErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("map not pruned within deadline: %v (last=%.0f, baseline=%.0f)", pollErr, finalEntries, s4Baseline),
		})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "s4-revocation-map-pruned", Status: metrics.CheckStatusPass})
	}

	return buildSoakRevocationReport(checks, run, false), nil
}

// runSoakRevocationPreflight runs the revocation preflight check.
// Returns (passed, checks) — if not passed, callers should skip S3/S4.
func runSoakRevocationPreflight(
	ctx context.Context,
	minter *auth.Minter,
	tenantID, gwURL, metricsEndpoint, runID string,
	httpClient *http.Client,
	logger zerolog.Logger,
) (bool, []metrics.CheckResult) {
	var checks []metrics.CheckResult

	// Scrape baseline.
	before, err := scrapeGatewayMetrics(ctx, metricsEndpoint)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "revocation-map-instrumented", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("pre-preflight scrape: %v", err),
		})
		return false, checks
	}

	// Throw-away revocation to trigger map entry.
	tok, mintErr := minter.MintWithClaims(auth.MintOptions{
		Subject: soakRevocationSub,
		JTI:     runID + "-preflight",
	})
	if mintErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "revocation-map-instrumented", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("mint preflight token: %v", mintErr),
		})
		return false, checks
	}
	revStatus, revErr := withRetry(ctx, func() (int, error) {
		code, err := revokeTokenWithClient(ctx, httpClient, gwURL, tok, tenantID,
			revokeRequest{JTI: runID + "-preflight"})
		if err != nil {
			return 0, err
		}
		if code == http.StatusTooManyRequests || code >= 500 {
			return code, fmt.Errorf("HTTP %d", code)
		}
		return code, nil
	}, len(revocationReconnectBackoffs)+1, revocationReconnectBackoffs)
	if revErr != nil || revStatus != http.StatusOK {
		checks = append(checks, metrics.CheckResult{
			Name: "revocation-map-instrumented", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("preflight revoke: HTTP %d, err: %v", revStatus, revErr),
		})
		return false, checks
	}

	// Allow a short settlement, then scrape again.
	select {
	case <-time.After(revocationSettlementWindow):
	case <-ctx.Done():
	}
	after, err := scrapeGatewayMetrics(ctx, metricsEndpoint)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "revocation-map-instrumented", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("post-preflight scrape: %v", err),
		})
		return false, checks
	}

	if after["gateway_token_revocation_map_entries"] <= before["gateway_token_revocation_map_entries"] {
		checks = append(checks, metrics.CheckResult{
			Name: "revocation-map-instrumented", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("map entries did not increase (before=%.0f, after=%.0f); metric not instrumented",
				before["gateway_token_revocation_map_entries"], after["gateway_token_revocation_map_entries"]),
		})
		logger.Warn().Msg("preflight failed: skipping S3/S4 metric-dependent ACs")
		return false, checks
	}

	checks = append(checks, metrics.CheckResult{Name: "revocation-map-instrumented", Status: metrics.CheckStatusPass})
	return true, checks
}

func buildSoakRevocationReport(checks []metrics.CheckResult, run *TestRun, failed bool) *metrics.Report {
	var skippedChecks int
	hasFail := failed
	for _, c := range checks {
		if c.Status == metrics.CheckStatusFail {
			hasFail = true
		}
		if c.Status == metrics.CheckStatusSkip {
			skippedChecks++
		}
	}
	status := metrics.ReportStatusPass
	if hasFail {
		status = metrics.ReportStatusFail
	}
	return &metrics.Report{
		TestType:      "soak",
		Status:        status,
		Metrics:       run.Collector.Snapshot(),
		Checks:        checks,
		SkippedChecks: skippedChecks,
	}
}
