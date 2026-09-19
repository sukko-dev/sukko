package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	testersse "github.com/sukko-dev/sukko/cmd/tester/sse"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// revocationTimeout is the maximum wait for revocation propagation (gRPC stream delivery).
const revocationTimeout = 5 * time.Second

// pushRetryAttempts is the number of re-subscribe attempts when verifying push registration deletion.
const pushRetryAttempts = 5

// pushRetryInterval is the interval between push re-subscribe attempts.
const pushRetryInterval = 1 * time.Second

// connectAndReadLoop connects a WS client. Callers MUST launch ReadLoop via wg.Go() (§VII).
func connectAndReadLoop(ctx context.Context, gwURL, token string, logger zerolog.Logger) (*testerws.Client, error) {
	client, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: gwURL,
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return client, nil
}

// waitForCloseCode waits for a close code on the channel with timeout.
func waitForCloseCode(checkName string, codeCh <-chan ws.StatusCode, timeout time.Duration, expected ws.StatusCode) metrics.CheckResult {
	start := time.Now()
	select {
	case code := <-codeCh:
		latency := time.Since(start).Round(time.Millisecond).String()
		if code == expected {
			return metrics.CheckResult{Name: checkName, Status: metrics.CheckStatusPass, Latency: latency}
		}
		return metrics.CheckResult{Name: checkName, Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected close code %d, got %d", expected, code), Latency: latency}
	case <-time.After(timeout):
		return metrics.CheckResult{Name: checkName, Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("timeout waiting for close code %d after %s", expected, timeout)}
	}
}

// tryConnect attempts a WebSocket connection and returns (client, httpStatus, error).
// On success, caller must close the client. On auth failure (401/403), returns nil client
// with the HTTP status code extracted from the dial error.
func tryConnect(ctx context.Context, gwURL, token string, logger zerolog.Logger) (*testerws.Client, int, error) {
	client, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: gwURL,
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		if statusErr, ok := errors.AsType[ws.StatusError](err); ok {
			return nil, int(statusErr), nil
		}
		return nil, 0, fmt.Errorf("try connect: %w", err)
	}
	return client, http.StatusSwitchingProtocols, nil
}

// validateTokenRevocation runs the token-revocation validation suite.
func validateTokenRevocation(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	gwURL := run.Config.GatewayURL
	minter := run.authResult.Minter
	tenantID := run.authResult.TenantID
	provClient := run.authResult.ProvClient

	var checks []metrics.CheckResult

	// --- Edition gate ---
	edition, err := fetchCurrentEdition(ctx, httpURL(gwURL))
	if err != nil {
		return nil, fmt.Errorf("fetch edition: %w", err)
	}
	if !license.EditionHasFeature(edition, license.TokenRevocation) {
		return []metrics.CheckResult{{
			Name: "edition-gate", Status: metrics.CheckStatusSkip,
			Error: "token revocation requires Pro+ edition (current: " + string(edition) + ")",
		}}, nil
	}
	logger.Info().Str("edition", string(edition)).Msg("token-revocation suite starting")

	// --- Readiness gate: don't revoke until the gateway's revocation stream is connected,
	// else a revoke published before the gateway has its snapshot is missed (cold-start flake). ---
	if run.Config.GatewayMetricsURL == "" {
		return append(checks, metrics.CheckResult{
			Name: "revocation-stream-ready", Status: metrics.CheckStatusFail,
			Error: "TESTER_GATEWAY_METRICS_URL not set — cannot verify revocation-stream readiness",
		}), nil
	}
	if readyErr := waitForRevocationStreamReady(ctx, run.Config.GatewayMetricsURL); readyErr != nil {
		return append(checks, metrics.CheckResult{
			Name: "revocation-stream-ready", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("gateway revocation stream not ready: %v", readyErr),
		}), nil
	}
	checks = append(checks, metrics.CheckResult{Name: "revocation-stream-ready", Status: metrics.CheckStatusPass})

	// The /revoke endpoint lives on the PROVISIONING admin API (not proxied by the gateway),
	// so revocation POSTs target provURL; WS/SSE/push connects still use the gateway (gwURL).
	provURL := run.Config.ProvisioningURL

	// --- Scenario 1: jti revocation (WS) ---
	checks = append(checks, checkJTIRevocation(ctx, gwURL, provURL, minter, tenantID, logger)...)

	// --- Scenario 2: sub revocation (WS) ---
	checks = append(checks, checkSubRevocation(ctx, gwURL, provURL, minter, tenantID, logger)...)

	// --- Scenario 3: SSE force-disconnect ---
	checks = append(checks, checkSSERevocation(ctx, gwURL, provURL, minter, tenantID, provClient, logger)...)

	// --- Scenario 4: push registration cleanup (Pro+ — WebPush, ADR-0009) ---
	switch {
	case !license.EditionHasFeature(edition, license.WebPush):
		checks = append(checks, metrics.CheckResult{
			Name: "push-registration-deleted", Status: metrics.CheckStatusSkip,
			Error: "push requires Pro edition or higher (current: " + string(edition) + ")",
		})
	case !pushServiceAvailable(ctx, httpURL(gwURL), run.authResult.TokenFunc(0)):
		// Edition includes WebPush but the stack runs without the push service
		// (deploy-optional) — skip with the deployment cause, not the edition.
		checks = append(checks, metrics.CheckResult{
			Name: "push-registration-deleted", Status: metrics.CheckStatusSkip,
			Error: "push service not deployed (unreachable)",
		})
	default:
		checks = append(checks, checkPushRevocation(ctx, gwURL, provURL, minter, tenantID, provClient, logger)...)
	}

	// --- Scenario 5: invalid requests + cross-tenant ---
	invalidChecks, invalidErr := checkInvalidRevocations(ctx, run, provURL, minter, tenantID, logger)
	if invalidErr != nil {
		return nil, fmt.Errorf("invalid revocations: %w", invalidErr)
	}
	checks = append(checks, invalidChecks...)

	// --- Edge cases ---
	checks = append(checks, checkEdgeCases(ctx, provURL, minter, tenantID, logger)...)

	return checks, nil
}

// checkJTIRevocation implements Scenario 1: jti-based revocation.
func checkJTIRevocation(ctx context.Context, gwURL, provURL string, minter *auth.Minter, tenantID string, logger zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult
	jti := "revoke-jti-" + uuid.NewString()[:8]

	// Mint token with known jti
	token, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "jti-test-user",
		JTI:     jti,
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "jti-force-disconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	}

	// Connect and start ReadLoop via wg.Go (§VII — no bare goroutines).
	client, err := connectAndReadLoop(ctx, gwURL, token, logger)
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "jti-force-disconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", err)})
	}
	codeCh := make(chan ws.StatusCode, 1)
	var readWg sync.WaitGroup
	readWg.Go(func() {
		defer logging.RecoverPanic(logger, "jti-revocation-read-loop", nil)
		code, _ := client.ReadLoop(ctx)
		codeCh <- code
	})
	defer func() {
		_ = client.Close() // triggers ReadLoop exit if not already complete
		readWg.Wait()
	}()

	// Revoke the jti
	status, err := revokeToken(ctx, provURL, token, tenantID, revokeRequest{JTI: jti})
	if err != nil || status != http.StatusOK {
		return append(checks, metrics.CheckResult{Name: "jti-force-disconnect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", status, err)})
	}

	// AC1: verify close code 1008
	checks = append(checks, waitForCloseCode("jti-force-disconnect", codeCh, revocationTimeout, ws.StatusPolicyViolation))

	// AC2: same jti rejected on reconnect (401)
	_, connStatus, connErr := tryConnect(ctx, gwURL, token, logger)
	switch {
	case connErr != nil:
		checks = append(checks, metrics.CheckResult{Name: "jti-reject-reconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", connErr)})
	case connStatus == http.StatusUnauthorized:
		checks = append(checks, metrics.CheckResult{Name: "jti-reject-reconnect", Status: metrics.CheckStatusPass})
	default:
		checks = append(checks, metrics.CheckResult{Name: "jti-reject-reconnect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 401, got %d", connStatus)})
	}

	// AC3: different jti succeeds
	otherJTI := "revoke-jti-other-" + uuid.NewString()[:8]
	otherToken, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "jti-test-user-other",
		JTI:     otherJTI,
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "jti-unaffected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	} else {
		otherClient, otherStatus, otherErr := tryConnect(ctx, gwURL, otherToken, logger)
		switch {
		case otherErr != nil:
			checks = append(checks, metrics.CheckResult{Name: "jti-unaffected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", otherErr)})
		case otherStatus == http.StatusSwitchingProtocols:
			checks = append(checks, metrics.CheckResult{Name: "jti-unaffected", Status: metrics.CheckStatusPass})
			_ = otherClient.Close()
		default:
			checks = append(checks, metrics.CheckResult{Name: "jti-unaffected", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("expected 101, got %d", otherStatus)})
		}
	}

	return checks
}

// checkSubRevocation implements Scenario 2: sub-based revocation.
func checkSubRevocation(ctx context.Context, gwURL, provURL string, minter *auth.Minter, tenantID string, logger zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult
	sub := "revoke-user-" + uuid.NewString()[:8]
	oldIAT := time.Now().Add(-1 * time.Hour)

	// Connect 3 connections for the same sub with old iat (§VII — goroutines via wg.Go).
	type connResult struct {
		client *testerws.Client
		codeCh <-chan ws.StatusCode
	}
	conns := make([]connResult, 3)
	var connWg sync.WaitGroup
	for i := range 3 {
		jti := fmt.Sprintf("sub-test-jti-%d-%s", i, uuid.NewString()[:8])
		token, err := minter.MintWithClaims(auth.MintOptions{
			Subject:  sub,
			JTI:      jti,
			IssuedAt: oldIAT,
		})
		if err != nil {
			for j := range i {
				_ = conns[j].client.Close()
			}
			connWg.Wait()
			return append(checks, metrics.CheckResult{Name: "sub-force-disconnect-all", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint conn %d: %v", i, err)})
		}

		client, err := connectAndReadLoop(ctx, gwURL, token, logger)
		if err != nil {
			for j := range i {
				_ = conns[j].client.Close()
			}
			connWg.Wait()
			return append(checks, metrics.CheckResult{Name: "sub-force-disconnect-all", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect conn %d: %v", i, err)})
		}

		codeCh := make(chan ws.StatusCode, 1)
		c := client
		connWg.Go(func() {
			defer logging.RecoverPanic(logger, "sub-revocation-read-loop", nil)
			code, _ := c.ReadLoop(ctx)
			codeCh <- code
		})
		conns[i] = connResult{client: client, codeCh: codeCh}
	}
	defer func() {
		for _, c := range conns {
			if c.client != nil {
				_ = c.client.Close()
			}
		}
		connWg.Wait()
	}()

	// Revoke by sub
	callerToken, err := minter.MintWithClaims(auth.MintOptions{
		Subject: sub,
		JTI:     "revoke-caller-" + uuid.NewString()[:8],
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "sub-force-disconnect-all", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint revoke token: %v", err)})
	}

	status, revokeErr := revokeToken(ctx, provURL, callerToken, tenantID, revokeRequest{Sub: sub})
	if revokeErr != nil || status != http.StatusOK {
		return append(checks, metrics.CheckResult{Name: "sub-force-disconnect-all", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", status, revokeErr)})
	}

	// AC1: verify all 3 close codes in parallel (§VII — goroutines via wg.Go).
	type closeResult struct {
		index int
		check metrics.CheckResult
	}
	resultCh := make(chan closeResult, 3)
	var verifyWg sync.WaitGroup
	for i, c := range conns {
		verifyWg.Go(func() {
			defer logging.RecoverPanic(logger, "sub-close-verify", nil)
			r := waitForCloseCode(fmt.Sprintf("sub-force-disconnect-%d", i), c.codeCh, revocationTimeout, ws.StatusPolicyViolation)
			resultCh <- closeResult{index: i, check: r}
		})
	}
	verifyWg.Wait()

	allPass := true
	for range 3 {
		r := <-resultCh
		checks = append(checks, r.check)
		if r.check.Status != metrics.CheckStatusPass {
			allPass = false
		}
	}
	if !allPass {
		return checks
	}

	// AC2: new token (iat > revoked_at) succeeds
	newToken, err := minter.MintWithClaims(auth.MintOptions{
		Subject: sub,
		JTI:     "sub-new-" + uuid.NewString()[:8],
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "sub-new-token-allowed", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	} else {
		newClient, connStatus, connErr := tryConnect(ctx, gwURL, newToken, logger)
		switch {
		case connErr != nil:
			checks = append(checks, metrics.CheckResult{Name: "sub-new-token-allowed", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", connErr)})
		case connStatus == http.StatusSwitchingProtocols:
			checks = append(checks, metrics.CheckResult{Name: "sub-new-token-allowed", Status: metrics.CheckStatusPass})
			_ = newClient.Close()
		default:
			checks = append(checks, metrics.CheckResult{Name: "sub-new-token-allowed", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("expected 101, got %d", connStatus)})
		}
	}

	// AC3: old token (iat < revoked_at) rejected
	oldToken, err := minter.MintWithClaims(auth.MintOptions{
		Subject:  sub,
		JTI:      "sub-old-" + uuid.NewString()[:8],
		IssuedAt: time.Now().Add(-2 * time.Hour),
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "sub-old-token-rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	} else {
		_, connStatus, connErr := tryConnect(ctx, gwURL, oldToken, logger)
		switch {
		case connErr != nil:
			checks = append(checks, metrics.CheckResult{Name: "sub-old-token-rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", connErr)})
		case connStatus == http.StatusUnauthorized:
			checks = append(checks, metrics.CheckResult{Name: "sub-old-token-rejected", Status: metrics.CheckStatusPass})
		default:
			checks = append(checks, metrics.CheckResult{Name: "sub-old-token-rejected", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("expected 401, got %d", connStatus)})
		}
	}

	// Edge: iat == revoked_at boundary (strict <, not <=)
	// Mint with iat = now (approximately equal to revoked_at since revocation just happened).
	// This should succeed because the gateway uses strict < comparison.
	boundaryToken, err := minter.MintWithClaims(auth.MintOptions{
		Subject: sub,
		JTI:     "sub-boundary-" + uuid.NewString()[:8],
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "sub-iat-boundary", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	} else {
		boundaryClient, connStatus, connErr := tryConnect(ctx, gwURL, boundaryToken, logger)
		switch {
		case connErr != nil:
			checks = append(checks, metrics.CheckResult{Name: "sub-iat-boundary", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("connect: %v", connErr)})
		case connStatus == http.StatusSwitchingProtocols:
			checks = append(checks, metrics.CheckResult{Name: "sub-iat-boundary", Status: metrics.CheckStatusPass})
			_ = boundaryClient.Close()
		default:
			checks = append(checks, metrics.CheckResult{Name: "sub-iat-boundary", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("expected 101, got %d", connStatus)})
		}
	}

	return checks
}

// checkSSERevocation implements Scenario 3: SSE force-disconnect.
func checkSSERevocation(ctx context.Context, gwURL, provURL string, minter *auth.Minter, tenantID string, provClient *auth.ProvisioningClient, logger zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult
	jti := "revoke-sse-" + uuid.NewString()[:8]
	testChannel := tenantID + ".revoke-test"

	// Setup channel + routing rules
	if err := provClient.SetChannelRules(ctx, tenantID, map[string]any{
		"public": []string{"revoke-test"},
	}); err != nil {
		return append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("set channel rules: %v", err)})
	}
	// Route to the pre-provisioned DefaultTopicSuffix ("default"): a fresh tenant only has the
	// default + dead-letter topics, and SetRoutingRules validates every referenced topic against
	// provisioned topics (TOPIC_NOT_PROVISIONED otherwise — the gate is on for both backends). The
	// scenario asserts force-disconnect on revocation, not delivery, so any valid topic suffices;
	// this mirrors validate_sse.go / validate_pubsub.go's testRoutingRules.
	if err := provClient.SetRoutingRules(ctx, tenantID, []map[string]any{
		{"pattern": "revoke-test", "ingress_topic": routing.DefaultTopicSuffix, "priority": 1},
	}); err != nil {
		return append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("set routing rules: %v", err)})
	}
	defer func() { //nolint:contextcheck // cleanup must survive parent cancellation
		cleanupCtx := context.Background()
		_ = provClient.DeleteRoutingRules(cleanupCtx, tenantID)
	}()

	token, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "sse-revoke-user",
		JTI:     jti,
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	}

	// Connect SSE
	sseClient, sseStatus, err := testersse.Connect(ctx, testersse.ConnectConfig{
		GatewayURL: httpURL(gwURL), // SSE is HTTP; gwURL is ws://
		Channels:   []string{testChannel},
		Token:      token,
		Logger:     logger,
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("SSE connect: HTTP %d, err: %v", sseStatus, err)})
	}
	// Start reading in background via wg.Go (§VII — no bare goroutines).
	// Cleanup order: close socket (unblocks ReadEvent scanner) → wait.
	eofCh := make(chan error, 1)
	var sseWg sync.WaitGroup
	sseWg.Go(func() {
		defer logging.RecoverPanic(logger, "sse-revocation-read", nil)
		_, readErr := sseClient.ReadEvent(ctx)
		eofCh <- readErr
	})
	defer func() {
		_ = sseClient.Close()
		sseWg.Wait()
	}()

	// Revoke the jti
	status, revokeErr := revokeToken(ctx, provURL, token, tenantID, revokeRequest{JTI: jti})
	if revokeErr != nil || status != http.StatusOK {
		return append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", status, revokeErr)})
	}

	// Wait for EOF/error
	start := time.Now()
	select {
	case readErr := <-eofCh:
		latency := time.Since(start).Round(time.Millisecond).String()
		// EOF or any error means stream terminated — that's what we want
		_ = readErr
		checks = append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusPass, Latency: latency})
	case <-time.After(revocationTimeout):
		checks = append(checks, metrics.CheckResult{Name: "sse-force-disconnect", Status: metrics.CheckStatusFail,
			Error: "timeout waiting for SSE stream termination"})
	}

	return checks
}

// checkPushRevocation implements Scenario 4: push registration cleanup.
func checkPushRevocation(ctx context.Context, gwURL, provURL string, minter *auth.Minter, tenantID string, provClient *auth.ProvisioningClient, logger zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult
	gwURL = httpURL(gwURL) // push subscribe/unsubscribe are HTTP; gwURL is ws://

	// Setup VAPID credentials and push channels (matching validate_push.go pattern)
	vapidCreds := `{"public_key":"BDummy_VAPID_Public_Key_For_Testing_Only","private_key":"dummy-vapid-private-key-for-testing"}` //nolint:gosec // G101: fake test credentials for push validation — not real secrets
	if err := provClient.SetPushCredentials(ctx, tenantID, "vapid", vapidCreds); err != nil {
		return append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("set push credentials: %v", err)})
	}
	if err := provClient.SetPushChannels(ctx, tenantID, []string{"revoke-push-test"}, 3600, "normal"); err != nil {
		return append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("set push channels: %v", err)})
	}
	defer func() { //nolint:contextcheck // cleanup must survive parent cancellation
		cleanupCtx := context.Background()
		_ = provClient.DeletePushChannels(cleanupCtx, tenantID)
		_ = provClient.DeletePushCredentials(cleanupCtx, tenantID, "vapid")
	}()

	jti := "revoke-push-" + uuid.NewString()[:8]
	token, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "push-revoke-user",
		JTI:     jti,
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	}

	// Subscribe push device
	pushChannel := tenantID + ".revoke-push-test"
	deviceID1, err := pushSubscribe(ctx, gwURL, token, pushSubscribeRequest{ //nolint:gosec // G101: fake test tokens for push revocation validation
		Platform:   "web",
		Endpoint:   "https://push.example.com/test/" + jti,
		P256dhKey:  "BDummy_P256DH_Key_For_Testing",
		AuthSecret: "dummy-auth-secret",
		Channels:   []string{pushChannel},
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("push subscribe: %v", err)})
	}
	logger.Info().Int64("device_id", deviceID1).Str("jti", jti).Msg("push device registered for revocation test")

	// Revoke the jti
	status, revokeErr := revokeToken(ctx, provURL, token, tenantID, revokeRequest{JTI: jti})
	if revokeErr != nil || status != http.StatusOK {
		return append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("revoke: HTTP %d, err: %v", status, revokeErr)})
	}

	// Retry loop: re-subscribe until deviceID changes (confirms old registration deleted)
	start := time.Now()
	for attempt := range pushRetryAttempts {
		select {
		case <-time.After(pushRetryInterval):
		case <-ctx.Done():
			return checks
		}

		newJTI := fmt.Sprintf("revoke-push-retry-%d-%s", attempt, uuid.NewString()[:8])
		newToken, err := minter.MintWithClaims(auth.MintOptions{
			Subject: "push-revoke-user",
			JTI:     newJTI,
		})
		if err != nil {
			continue
		}

		deviceID2, err := pushSubscribe(ctx, gwURL, newToken, pushSubscribeRequest{ //nolint:gosec // G101: fake test tokens for push revocation validation
			Platform:   "web",
			Endpoint:   "https://push.example.com/test/" + jti, // same endpoint
			P256dhKey:  "BDummy_P256DH_Key_For_Testing",
			AuthSecret: "dummy-auth-secret",
			Channels:   []string{pushChannel},
		})
		if err != nil {
			continue
		}

		if deviceID2 != deviceID1 {
			latency := time.Since(start).Round(time.Millisecond).String()
			checks = append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusPass, Latency: latency})
			// Cleanup: unsubscribe the new registration
			_ = pushUnsubscribe(context.Background(), gwURL, newToken, deviceID2) //nolint:contextcheck // best-effort cleanup
			return checks
		}
	}

	checks = append(checks, metrics.CheckResult{Name: "push-registration-deleted", Status: metrics.CheckStatusFail,
		Error: fmt.Sprintf("device_id unchanged after %d attempts — registration not deleted", pushRetryAttempts)})
	return checks
}

// checkInvalidRevocations implements Scenario 5: invalid requests.
func checkInvalidRevocations(ctx context.Context, run *TestRun, provURL string, minter *auth.Minter, tenantID string, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	var checks []metrics.CheckResult

	token, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "invalid-revoke-user",
		JTI:     "invalid-test-" + uuid.NewString()[:8],
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "revoke-missing-fields", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)}), nil
	}

	// AC1: neither sub nor jti → 400
	status, _ := revokeToken(ctx, provURL, token, tenantID, revokeRequest{})
	if status == http.StatusBadRequest {
		checks = append(checks, metrics.CheckResult{Name: "revoke-missing-fields", Status: metrics.CheckStatusPass})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "revoke-missing-fields", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 400, got %d", status)})
	}

	// AC2: both sub and jti → 400
	status, _ = revokeToken(ctx, provURL, token, tenantID, revokeRequest{Sub: "user", JTI: "token"})
	if status == http.StatusBadRequest {
		checks = append(checks, metrics.CheckResult{Name: "revoke-both-fields", Status: metrics.CheckStatusPass})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "revoke-both-fields", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 400, got %d", status)})
	}

	// AC3: cross-tenant mismatch → 403.
	// RequireAdminProvider: true — cross-tenant test requires a registered keypair.
	setupB, err := auth.Setup(ctx, auth.SetupConfig{
		TestID:               run.ID + "-revoke-tenant-b",
		ProvisioningURL:      run.Config.ProvisioningURL,
		Logger:               logger,
		AdminProvider:        run.authResult.AdminProvider,
		RequireAdminProvider: true,
	})
	if err != nil {
		return nil, fmt.Errorf("setup tenant B: %w", err)
	}
	defer setupB.Cleanup(context.Background()) //nolint:contextcheck // cleanup must survive parent cancellation

	tenantBToken, err := setupB.Minter.MintWithClaims(auth.MintOptions{
		Subject: "tenant-b-user",
		JTI:     "tenant-b-jti-" + uuid.NewString()[:8],
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "revoke-tenant-mismatch", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint tenant B: %v", err)}), nil
	}

	// Send tenant B's JWT to tenant A's revocation endpoint
	status, _ = revokeToken(ctx, provURL, tenantBToken, tenantID, revokeRequest{JTI: "some-jti"})
	if status == http.StatusForbidden {
		checks = append(checks, metrics.CheckResult{Name: "revoke-tenant-mismatch", Status: metrics.CheckStatusPass})
	} else {
		checks = append(checks, metrics.CheckResult{Name: "revoke-tenant-mismatch", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 403, got %d", status)})
	}

	return checks, nil
}

// checkEdgeCases tests revocation edge cases.
func checkEdgeCases(ctx context.Context, provURL string, minter *auth.Minter, tenantID string, _ zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult

	token, err := minter.MintWithClaims(auth.MintOptions{
		Subject: "edge-case-user",
		JTI:     "edge-test-" + uuid.NewString()[:8],
	})
	if err != nil {
		return append(checks, metrics.CheckResult{Name: "revoke-no-active-conn", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint: %v", err)})
	}

	// Edge: revoke jti with no active connection → 200
	noConnJTI := "no-conn-" + uuid.NewString()[:8]
	status, revokeErr := revokeToken(ctx, provURL, token, tenantID, revokeRequest{JTI: noConnJTI})
	switch {
	case revokeErr != nil:
		checks = append(checks, metrics.CheckResult{Name: "revoke-no-active-conn", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("revoke: %v", revokeErr)})
	case status == http.StatusOK:
		checks = append(checks, metrics.CheckResult{Name: "revoke-no-active-conn", Status: metrics.CheckStatusPass})
	default:
		checks = append(checks, metrics.CheckResult{Name: "revoke-no-active-conn", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 200, got %d", status)})
	}

	// Edge: revoke same jti twice → 200 (idempotent)
	status, revokeErr = revokeToken(ctx, provURL, token, tenantID, revokeRequest{JTI: noConnJTI})
	switch {
	case revokeErr != nil:
		checks = append(checks, metrics.CheckResult{Name: "revoke-idempotent", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("revoke: %v", revokeErr)})
	case status == http.StatusOK:
		checks = append(checks, metrics.CheckResult{Name: "revoke-idempotent", Status: metrics.CheckStatusPass})
	default:
		checks = append(checks, metrics.CheckResult{Name: "revoke-idempotent", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 200, got %d", status)})
	}

	// Edge: API-key-only connection survives revocation.
	// The tester primarily connects via JWT. API key connections require a provisioned
	// API key — test this only if the test run has one available.
	checks = append(checks, metrics.CheckResult{Name: "apikey-survives-revoke", Status: metrics.CheckStatusSkip,
		Error: "API key not available in tester config — covered by auth suite"})

	return checks
}
