package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// validateLicenseReload runs the license-reload validation suite.
// Execution order (rate limiter awareness):
//  1. Capture starting edition
//  2. Invalid key rejection (uses burst budget — run first)
//  3. Transition chain CE→Pro→Enterprise→Pro→CE (gating + connection survival)
//  4. Restore starting edition (defer cleanup)
func validateLicenseReload(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provURL := run.Config.ProvisioningURL
	gwURL := run.Config.GatewayURL
	signer := run.authResult.ProvClient.AuthProvider()

	var (
		keygen *licenseKeyGenerator
		err    error
	)
	switch {
	case len(run.Config.SigningKeyBytes) > 0:
		keygen, err = newLicenseKeyGeneratorFromBytes(run.Config.SigningKeyBytes)
	case run.Config.SigningKeyFile != "":
		keygen, err = newLicenseKeyGenerator(run.Config.SigningKeyFile)
	default:
		return nil, errors.New("signing key required for license-reload suite: " +
			"pass signing_key in request body or set TESTER_SIGNING_KEY_FILE env var")
	}
	if err != nil {
		return nil, fmt.Errorf("license key generator: %w", err)
	}

	// Capture starting edition for restore
	startEdition, err := fetchCurrentEdition(ctx, provURL)
	if err != nil {
		return nil, fmt.Errorf("fetch starting edition: %w", err)
	}
	logger.Info().Str("starting_edition", string(startEdition)).Msg("license-reload suite starting")

	// Defer: restore original edition
	defer func() { //nolint:contextcheck // intentional: use background context so cleanup survives parent cancellation
		restoreCtx, cancel := context.WithTimeout(context.Background(), licenseHTTPTimeout)
		defer cancel()
		restoreKey := keygen.sign(startEdition, "Restore", 365*24*time.Hour)
		status, restoreErr := postLicense(restoreCtx, provURL, restoreKey, signer)
		if restoreErr != nil || status != http.StatusOK {
			logger.Warn().Err(restoreErr).Int("status", status).
				Str("edition", string(startEdition)).Msg("license-reload cleanup: failed to restore edition")
		} else {
			logger.Info().Str("edition", string(startEdition)).Msg("license-reload cleanup: edition restored")
		}
	}()

	// Phase 1: Invalid key rejection (Scenario 3 — uses burst budget, run first)
	invalidChecks := checkInvalidKeys(ctx, provURL, keygen, signer)
	// Phase 2: Transition chain (Scenarios 1 + 2 combined)
	chainChecks := checkTransitionChain(ctx, provURL, gwURL, keygen, run, logger, signer)

	checks := make([]metrics.CheckResult, 0, len(invalidChecks)+len(chainChecks))
	checks = append(checks, invalidChecks...)
	checks = append(checks, chainChecks...)

	return checks, nil
}

// checkInvalidKeys tests bad signature, expired, and replay rejection (Scenario 3).
func checkInvalidKeys(ctx context.Context, provURL string, keygen *licenseKeyGenerator, signer auth.Provider) []metrics.CheckResult {
	var checks []metrics.CheckResult

	// Ensure we're on Pro for the invalid key tests
	proIat := keygen.nextIat // capture before sign() increments
	proKey := keygen.sign(license.Pro, "Test Pro Setup", 24*time.Hour)
	status, err := postLicense(ctx, provURL, proKey, signer)
	if err != nil || status != http.StatusOK {
		errMsg := fmt.Sprintf("failed to push Pro setup key: status=%d err=%v", status, err)
		return []metrics.CheckResult{{Name: "invalid keys setup", Status: "fail", Error: errMsg}}
	}
	if ok, _ := pollEdition(ctx, provURL, license.Pro); !ok {
		return []metrics.CheckResult{{Name: "invalid keys setup", Status: "fail", Error: "Pro edition not reflected after setup push"}}
	}

	// 1. Bad signature
	status, _ = postLicense(ctx, provURL, "not-a-valid-license-key", signer)
	if status == http.StatusBadRequest {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: bad signature", Status: "pass"})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: bad signature", Status: "fail",
			Error: fmt.Sprintf("expected 400, got %d", status)})
	}

	// 2. Expired key
	expiredKey := keygen.signExpired(license.Pro)
	status, _ = postLicense(ctx, provURL, expiredKey, signer)
	if status == http.StatusBadRequest {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: expired", Status: "pass"})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: expired", Status: "fail",
			Error: fmt.Sprintf("expected 400, got %d", status)})
	}

	// 3. Replay (same iat as the Pro key we pushed above)
	replayKey := keygen.signWithIat(license.Pro, proIat)
	status, _ = postLicense(ctx, provURL, replayKey, signer)
	if status == http.StatusConflict {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: replay", Status: "pass"})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "invalid key: replay", Status: "fail",
			Error: fmt.Sprintf("expected 409, got %d", status)})
	}

	// Verify edition unchanged after all invalid pushes
	edition, _ := fetchCurrentEdition(ctx, provURL)
	if edition == license.Pro {
		checks = append(checks, metrics.CheckResult{Name: "edition stable after invalid keys", Status: "pass"})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "edition stable after invalid keys", Status: "fail",
			Error: fmt.Sprintf("expected pro, got %s", edition)})
	}

	return checks
}

// checkTransitionChain runs CE→Pro→Enterprise→Pro→CE with gating + WS connection survival.
func checkTransitionChain(ctx context.Context, provURL, gwURL string, keygen *licenseKeyGenerator, run *TestRun, logger zerolog.Logger, signer auth.Provider) []metrics.CheckResult {
	var checks []metrics.CheckResult
	token := run.authResult.TokenFunc(0)
	httpGW := httpURL(gwURL)

	// Message delivery channel — receives channel names when messages arrive on the WS connection.
	msgCh := make(chan string, 10) //nolint:mnd // buffer for message delivery checks

	// Step 0: Push CE key (starting point for chain)
	ceKey := keygen.sign(license.Community, "Test CE", 24*time.Hour)
	if status, err := postLicense(ctx, provURL, ceKey, signer); err != nil || status != http.StatusOK {
		return []metrics.CheckResult{{Name: "transition chain setup", Status: "fail",
			Error: fmt.Sprintf("failed to push CE key: status=%d err=%v", status, err)}}
	}
	if ok, _ := pollEdition(ctx, httpGW, license.Community); !ok {
		return []metrics.CheckResult{{Name: "transition chain setup", Status: "fail",
			Error: "CE edition not reflected on gateway"}}
	}

	// Step 1: CE → Pro
	proKey := keygen.sign(license.Pro, "Test Pro", 24*time.Hour)
	postLicenseAndCheck(ctx, provURL, proKey, signer) // error handled by propagation check
	ok, _ := pollEdition(ctx, httpGW, license.Pro)
	checks = append(checks, propagationCheck("CE→Pro", ok))
	checks = append(checks, checkSSEGate(ctx, httpGW, token, true, logger)...)
	checks = append(checks, checkRESTGate(ctx, httpGW, token, true)...)

	// Open WS connection for survival test (stays open for all remaining transitions)
	wsClient, wsErr := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: gwURL,
		Token:      token,
		Logger:     logger,
		OnMessage: func(msg testerws.Message) {
			if msg.Channel != "" {
				select {
				case msgCh <- msg.Channel:
				default: // non-blocking — drop if buffer full
				}
			}
		},
	})
	if wsErr != nil {
		checks = append(checks, metrics.CheckResult{Name: "ws connect for survival", Status: "fail",
			Error: fmt.Sprintf("failed to connect: %v", wsErr)})
		return checks
	}
	// Start read loop via wg.Go (§VII — no bare goroutines).
	// Cleanup order: cancel context → close socket (unblocks ReadLoop) → wait.
	readCtx, readCancel := context.WithCancel(ctx)
	var readWg sync.WaitGroup
	readWg.Go(func() {
		defer logging.RecoverPanic(logger, "license-reload-read-loop", nil)
		_, _ = wsClient.ReadLoop(readCtx)
	})
	defer func() {
		readCancel()
		_ = wsClient.Close()
		readWg.Wait()
	}()

	// Subscribe to delivery channel
	if err := wsClient.Subscribe([]string{tenantChannel(run.authResult.TenantID, "test.license-delivery")}); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "ws subscribe", Status: "fail",
			Error: fmt.Sprintf("subscribe failed: %v", err)})
		return checks
	}
	checks = append(checks, checkMessageDelivery(ctx, httpGW, token, run.authResult.TenantID, msgCh, "CE→Pro")...)

	// Step 2: Pro → Enterprise
	entKey := keygen.sign(license.Enterprise, "Test Enterprise", 24*time.Hour)
	postLicenseAndCheck(ctx, provURL, entKey, signer)
	ok, _ = pollEdition(ctx, httpGW, license.Enterprise)
	checks = append(checks, propagationCheck("Pro→Enterprise", ok))
	checks = append(checks, checkPushGate(ctx, httpGW, token, true)...)
	checks = append(checks, checkWSSurvival(wsClient, run.authResult.TenantID, "Pro→Enterprise"))
	checks = append(checks, checkMessageDelivery(ctx, httpGW, token, run.authResult.TenantID, msgCh, "Pro→Enterprise")...)

	// Step 3: Enterprise → Pro
	proKey2 := keygen.sign(license.Pro, "Test Pro 2", 24*time.Hour)
	postLicenseAndCheck(ctx, provURL, proKey2, signer)
	ok, _ = pollEdition(ctx, httpGW, license.Pro)
	checks = append(checks, propagationCheck("Enterprise→Pro", ok))
	checks = append(checks, checkPushGate(ctx, httpGW, token, false)...)
	checks = append(checks, checkSSEGate(ctx, httpGW, token, true, logger)...)
	checks = append(checks, checkWSSurvival(wsClient, run.authResult.TenantID, "Enterprise→Pro"))
	checks = append(checks, checkMessageDelivery(ctx, httpGW, token, run.authResult.TenantID, msgCh, "Enterprise→Pro")...)

	// Step 4: Pro → CE
	ceKey2 := keygen.sign(license.Community, "Test CE 2", 24*time.Hour)
	postLicenseAndCheck(ctx, provURL, ceKey2, signer)
	ok, _ = pollEdition(ctx, httpGW, license.Community)
	checks = append(checks, propagationCheck("Pro→CE", ok))
	checks = append(checks, checkSSEGate(ctx, httpGW, token, false, logger)...)
	checks = append(checks, checkRESTGate(ctx, httpGW, token, false)...)
	checks = append(checks, checkWSSurvival(wsClient, run.authResult.TenantID, "Pro→CE"))
	// No message delivery check — REST publish is gated on CE

	return checks
}

// postLicenseAndCheck posts a license key and logs errors (non-fatal — propagation check catches failures).
func postLicenseAndCheck(ctx context.Context, provURL, key string, signer auth.Provider) {
	if _, err := postLicense(ctx, provURL, key, signer); err != nil {
		// Non-fatal: propagation check will catch the failure
		_ = err
	}
}
