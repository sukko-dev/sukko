package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Auth-upgrade frame types. Tester-local constants (§I): the tester matches wire frame
// types as bare strings and imports no internal/gateway code (§X decoupling), mirroring
// respTypeSubscribeError in the api-key suite.
const (
	msgTypeAuth       = "auth"       // client→server: the upgrade request carrying a JWT
	respTypeAuthAck   = "auth_ack"   // server→client: upgrade accepted
	respTypeAuthError = "auth_error" // server→client: upgrade rejected (e.g. expired JWT)
)

// validateUpgrade runs the upgrade validation suite.
// Validates the auth upgrade flow: connect with API key, send auth message,
// receive auth_ack, then verify upgrade success via counter.
// Also validates the negative path: expired JWT → auth_error.
func validateUpgrade(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	var checks []metrics.CheckResult

	if run.apiKey == "" {
		return nil, errors.New("validateUpgrade: no api key configured (set TESTER_API_KEY or pass api_key in request)")
	}
	if run.authResult.Minter == nil {
		return nil, errors.New("validateUpgrade: Minter is nil — upgrade mode requires auth.Setup (keypair registration)")
	}

	// Check 1: Connect with API key
	client1, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		APIKey:     run.apiKey,
		Logger:     logger.With().Str("suite", "upgrade").Logger(),
		OnMessage: func(msg testerws.Message) {
			run.Collector.MessagesReceived.Add(1)
		},
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name:   "api key initial connect",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("connect: %v", err),
		})
		return checks, nil
	}
	run.Collector.ConnectionsAPIKey.Add(1)
	run.Collector.ConnectionsTotal.Add(1)
	var wg1 sync.WaitGroup
	wg1.Go(func() {
		defer logging.RecoverPanic(logger, "validate_upgrade_client1_readloop", nil)
		_, _ = client1.ReadLoop(ctx)
	})
	defer func() {
		_ = client1.Close()
		wg1.Wait()
	}()

	checks = append(checks, metrics.CheckResult{
		Name:   "api key initial connect",
		Status: metrics.CheckStatusPass,
	})

	// Check 2: Subscribe to public channel
	if err := client1.Subscribe([]string{tenantChannel(run.Config.TenantID, validatePublicChannel)}); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name:   "public channel subscribe",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("subscribe: %v", err),
		})
		return checks, nil
	}
	checks = append(checks, metrics.CheckResult{
		Name:   "public channel subscribe",
		Status: metrics.CheckStatusPass,
	})

	// Check 3 (happy path): Auth upgrade.
	// ackCh is buffered(1) so a late auth_ack never blocks the ReadLoop goroutine (§VII).
	checks = append(checks, upgradeCheck(ctx, run, logger, false)...)

	// Check 4 (negative): Expired JWT → expect auth_error.
	checks = append(checks, upgradeCheck(ctx, run, logger, true)...)

	return checks, nil
}

// upgradeCheck performs one upgrade attempt (happy or negative).
// negative=false: mint valid JWT, expect auth_ack.
// negative=true:  mint expired JWT, expect auth_error.
func upgradeCheck(ctx context.Context, run *TestRun, logger zerolog.Logger, negative bool) []metrics.CheckResult {
	checkName := "auth upgrade"
	if negative {
		checkName = "expired JWT rejected"
	}

	ackCh := make(chan testerws.Message, 1)
	client, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		APIKey:     run.apiKey,
		Logger:     logger.With().Str("phase", checkName).Logger(),
		OnMessage: func(msg testerws.Message) {
			if msg.Type == respTypeAuthAck || msg.Type == respTypeAuthError {
				select {
				case ackCh <- msg:
				default:
				}
			}
		},
	})
	if err != nil {
		return []metrics.CheckResult{{
			Name:   checkName,
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("connect: %v", err),
		}}
	}

	upgradeCtx, upgradeCancel := context.WithTimeout(ctx, run.authUpgradeTimeout)
	defer upgradeCancel()

	var upgradeWg sync.WaitGroup
	upgradeWg.Go(func() {
		defer logging.RecoverPanic(logger, "validate_upgrade_readloop", nil)
		_, _ = client.ReadLoop(upgradeCtx)
	})
	defer func() {
		upgradeWg.Wait()
		_ = client.Close()
	}()

	// Mint JWT
	var token string
	var mintErr error
	if negative {
		token, mintErr = run.authResult.Minter.MintExpired(1)
	} else {
		token, mintErr = run.authResult.Minter.Mint(0)
	}
	if mintErr != nil {
		return []metrics.CheckResult{{
			Name:   checkName,
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("mint JWT: %v", mintErr),
		}}
	}

	run.Collector.AuthUpgradeTotal.Add(1)
	if err := client.RefreshToken(token); err != nil {
		run.Collector.AuthUpgradeFailed.Add(1)
		return []metrics.CheckResult{{
			Name:   checkName,
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("send RefreshToken: %v", err),
		}}
	}

	select {
	case msg := <-ackCh:
		upgradeCancel() // stop ReadLoop promptly
		if !negative && msg.Type == respTypeAuthAck {
			run.Collector.ConnectionsUpgrade.Add(1)
			return []metrics.CheckResult{{
				Name:   checkName,
				Status: metrics.CheckStatusPass,
			}}
		} else if negative && msg.Type == respTypeAuthError {
			return []metrics.CheckResult{{
				Name:   checkName,
				Status: metrics.CheckStatusPass,
			}}
		}
		run.Collector.AuthUpgradeFailed.Add(1)
		return []metrics.CheckResult{{
			Name:   checkName,
			Status: metrics.CheckStatusFail,
			Error:  "unexpected response type: " + msg.Type,
		}}
	case <-upgradeCtx.Done():
		run.Collector.AuthUpgradeFailed.Add(1)
		return []metrics.CheckResult{{
			Name:   checkName,
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("timed out waiting for response after %v", run.authUpgradeTimeout),
		}}
	}
}
