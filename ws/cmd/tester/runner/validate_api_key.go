package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// respTypeSubscribeError is the server→client frame type sent when a subscribe is rejected
// (e.g. every requested channel was filtered out by the gateway as unauthorized). Defined
// locally: the tester treats frame types as bare strings (ws/client.go) and imports no
// internal/server code — a local const avoids tester→server coupling (§X/§XVIII).
const respTypeSubscribeError = "subscribe_error"

// validateAPIKey runs the api-key validation suite.
// Validates that an API-key-only client can subscribe to public channels,
// is denied private channels, and cannot REST-publish.
// Uses run.apiKey (set by execute()) — does not call CreateAPIKey internally.
func validateAPIKey(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	var checks []metrics.CheckResult

	if run.apiKey == "" {
		return nil, errors.New("validateAPIKey: no api key configured (set TESTER_API_KEY or pass api_key in request)")
	}

	// Seed precise channel rules for this run's tenant. An API-key-only client has nil JWT
	// claims, so the gateway authorizes its subscribes against rules.Public *only*
	// (permissions_tenant.go). With the default permissive public:["*"] the private channel
	// would be allowed and check 3 ("private channel denied") could never be exercised — so
	// narrow public to just the suite's public channel. Nil-guarded like
	// seedDefaultChannelRules; a failure surfaces as a failing check, not a hard error
	// (the tester's error contract — §XVIII with validate.go).
	if run.authResult != nil && run.authResult.ProvClient != nil {
		if err := run.authResult.ProvClient.SetChannelRules(ctx, run.Config.TenantID, map[string]any{
			"public":         []string{validatePublicChannel},
			"default":        []string{"*"},
			"publish_public": []string{"*"},
		}); err != nil {
			checks = append(checks, metrics.CheckResult{
				Name:   "seed channel rules",
				Status: metrics.CheckStatusFail,
				Error:  fmt.Sprintf("seed channel rules: %v", err),
			})
			return checks, nil
		}
	}

	// errCh receives error / subscribe_error / publish_error frames (the deny signals) from the
	// gateway/server. Buffered(1) so the ReadLoop goroutine is never blocked sending to it (§VII).
	errCh := make(chan testerws.Message, 1)

	suiteLogger := logger.With().Str("suite", "api-key").Logger()

	// Check 1: Connect with API key
	client, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		APIKey:     run.apiKey,
		Logger:     suiteLogger,
		OnMessage: func(msg testerws.Message) {
			// Deny signals: a denied subscribe → subscribe_error (gateway filters the unauthorized
			// channel → empty subscribe → server subscribe_error); a denied publish → publish_error.
			// Capture both (plus generic error) onto errCh; the deny checks discriminate on the
			// exact (type, code) pair via pollDenyFromErrCh, so a leftover of the wrong type/code
			// never false-passes.
			if msg.Type == "error" || msg.Type == respTypeSubscribeError || msg.Type == respTypePublishError {
				select {
				case errCh <- msg:
				default:
				}
				return
			}
			run.Collector.MessagesReceived.Add(1)
		},
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name:   "api key accepted",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("connect: %v", err),
		})
		return checks, nil
	}
	run.Collector.ConnectionsAPIKey.Add(1)
	run.Collector.ConnectionsTotal.Add(1)
	run.Collector.ConnectionsActive.Add(1)
	var readWg sync.WaitGroup
	readWg.Go(func() {
		defer logging.RecoverPanic(logger, "validate_api_key_readloop", nil)
		_, _ = client.ReadLoop(ctx)
	})
	defer func() {
		_ = client.Close()
		readWg.Wait()
		run.Collector.ConnectionsActive.Add(-1)
	}()

	checks = append(checks, metrics.CheckResult{
		Name:   "api key accepted",
		Status: metrics.CheckStatusPass,
	})

	// Check 2: Subscribe to public channel
	if err := client.Subscribe([]string{tenantChannel(run.Config.TenantID, validatePublicChannel)}); err != nil {
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

	// Check 3: Subscribe to private channel — expect the gateway to deny it. The gateway
	// filters the unauthorized channel out, leaving an empty subscribe, and the server
	// replies with a subscribe_error frame (captured on errCh). The deny is asynchronous, so
	// a single subscribe-then-wait flaked under load (#216) — waitForDeny re-sends the
	// subscribe on a bounded interval until a deny arrives or the deadline elapses.
	// Drain stale frames exactly once before the wait (drain-once); the bounded-retry
	// waitForDeny owns the re-issue + deny-wins probe and never re-clears.
	drainMessages(errCh)
	privateChannel := run.Config.TenantID + privateChannelSuffix
	outcome, denyErr := waitForDeny(ctx,
		func() error { return client.Subscribe([]string{privateChannel}) },
		pollDenyFromErrCh(errCh, respTypeSubscribeError, wsErrCodeInvalidRequest),
		run.denyWaitDeadline, run.denyWaitRetryInterval, suiteLogger)
	if outcome == denyOutcomeCancelled {
		return checks, nil // parent context canceled (test stopped) — not a genuine failure
	}
	checks = append(checks, denyCheckResult("private channel denied", outcome, denyErr))

	// Check 4: REST publish with API key — expect HTTP 403 FORBIDDEN.
	// API keys cannot REST-publish; the gateway rejects with 403.
	// 403 here is the expected success outcome — do NOT increment RESTPublishErrors.
	restStatus, _, restErr := restpublish.NewClient(httpURL(run.Config.GatewayURL)).PublishRaw(
		ctx, validPublishBody, restpublish.AuthConfig{APIKey: run.apiKey}, "application/json",
	)
	switch {
	case restErr != nil:
		checks = append(checks, metrics.CheckResult{
			Name:   "api key REST publish blocked",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("REST publish request failed: %v", restErr),
		})
	case restStatus == http.StatusForbidden:
		checks = append(checks, metrics.CheckResult{
			Name:   "api key REST publish blocked",
			Status: metrics.CheckStatusPass,
		})
	default:
		run.Collector.RESTPublishErrors.Add(1)
		checks = append(checks, metrics.CheckResult{
			Name:   "api key REST publish blocked",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("expected HTTP 403, got %d", restStatus),
		})
	}

	// Checks 5-7: additional scoping deny checks for the API-key-only client.
	// Each isolates a single unauthorized channel and asserts the platform's real wire deny signal.
	// The gateway filters the unauthorized subscribe to empty → server subscribe_error/invalid_request;
	// a forbidden WS publish → publish_error/forbidden. drainMessages once before each wait so a stale
	// frame from a prior check cannot false-pass (drain-once); waitForDeny owns the re-issue.
	subscribeDenyChecks := []struct {
		name    string
		channel string
	}{
		{"group channel denied", tenantChannel(run.Config.TenantID, "room.vip")},
		{"user channel denied", tenantChannel(run.Config.TenantID, "dm.denied-user")},
	}
	for _, dc := range subscribeDenyChecks {
		drainMessages(errCh)
		outcome, denyErr := waitForDeny(ctx,
			func() error { return client.Subscribe([]string{dc.channel}) },
			pollDenyFromErrCh(errCh, respTypeSubscribeError, wsErrCodeInvalidRequest),
			run.denyWaitDeadline, run.denyWaitRetryInterval, suiteLogger)
		if outcome == denyOutcomeCancelled {
			return checks, nil // parent context canceled (test stopped) — not a genuine failure
		}
		checks = append(checks, denyCheckResult(dc.name, outcome, denyErr))
	}

	// Check 7: WS publish denied — an API-key-only client cannot publish over WS.
	drainMessages(errCh)
	publishChannel := tenantChannel(run.Config.TenantID, "general.test")
	pubOutcome, pubDenyErr := waitForDeny(ctx,
		func() error { return client.Publish(publishChannel, []byte(`{"msg_id":"apikey-ws-publish","ts":0}`)) },
		pollDenyFromErrCh(errCh, respTypePublishError, wsErrCodeForbidden),
		run.denyWaitDeadline, run.denyWaitRetryInterval, suiteLogger)
	if pubOutcome == denyOutcomeCancelled {
		return checks, nil // parent context canceled (test stopped) — not a genuine failure
	}
	checks = append(checks, denyCheckResult("ws publish denied", pubOutcome, pubDenyErr))

	return checks, nil
}
