package runner

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

func validateTenantIsolation(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	// Setup: create two throwaway tenants with identical channel/routing rules.
	// RequireAdminProvider: true — isolation suite is only valid in remote mode.
	setupA, err := auth.Setup(ctx, auth.SetupConfig{
		TestID:               run.ID + "-isolation-a",
		ProvisioningURL:      run.Config.ProvisioningURL,
		Logger:               logger,
		AdminProvider:        run.authResult.AdminProvider,
		RequireAdminProvider: true,
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "setup tenant A", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer setupA.Cleanup(context.Background()) //nolint:contextcheck // cleanup must survive parent cancellation

	setupB, err := auth.Setup(ctx, auth.SetupConfig{
		TestID:               run.ID + "-isolation-b",
		ProvisioningURL:      run.Config.ProvisioningURL,
		Logger:               logger,
		AdminProvider:        run.authResult.AdminProvider,
		RequireAdminProvider: true,
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "setup tenant B", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer setupB.Cleanup(context.Background()) //nolint:contextcheck // cleanup must survive parent cancellation

	// Set identical channel rules + routing rules on both tenants
	for _, setup := range []*auth.SetupResult{setupA, setupB} {
		if err := setup.ProvClient.SetChannelRules(ctx, setup.TenantID, testChannelRules); err != nil {
			return []metrics.CheckResult{{Name: "set channel rules " + setup.TenantID, Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
		}
		// Routing rules are ungated (ChannelTopicRouting, ADR-0014), so this is fail-closed on
		// every edition. Matches the pubsub/rest-publish parallel — the isolation suite was the
		// lone raw caller.
		if err := setupSuiteRoutingRules(ctx, setup.ProvClient, setup.TenantID, logger); err != nil {
			return []metrics.CheckResult{{Name: "set routing rules " + setup.TenantID, Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
		}
	}

	// Create engine + users
	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
	})

	userA, err := engine.CreateUser(ctx, setupA.Minter, auth.MintOptions{
		Subject: "isolation-user-a",
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "create user A", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() { _ = userA.Client.Close() }()

	userB, err := engine.CreateUser(ctx, setupB.Minter, auth.MintOptions{
		Subject: "isolation-user-b",
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "create user B", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	defer func() { _ = userB.Client.Close() }()

	// Each user subscribes to the same channel suffix under its OWN tenant prefix — isolation
	// depends on the full channel names differing by tenant.
	//
	// Confirmed subscribes (#242): these tenants were provisioned moments ago, and
	// the gateway silently FILTERS channels whose rules have not yet propagated —
	// a fire-and-forget subscribe then leaves no subscription at all, and the
	// delivery warmup below is dead forever (the root cause of the tenant-B
	// warmup flake). Retry until the subscription_ack confirms the channel.
	chanA := tenantChannel(setupA.TenantID, "general.test")
	chanB := tenantChannel(setupB.TenantID, "general.test")
	if err := userA.SubscribeConfirmed(ctx, []string{chanA}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe user A", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}
	if err := userB.SubscribeConfirmed(ctx, []string{chanB}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe user B", Status: metrics.CheckStatusFail, Error: err.Error()}}, nil
	}

	allUsers := []*TestUser{userA, userB}

	// Warm each tenant's OWN delivery path before the measured isolation publishes,
	// replacing a fixed sleep. In kafka mode a freshly-provisioned tenant's consumer joins its topic
	// at AtEnd only after control-plane propagation, so an early publish is dropped and the expected
	// receiver misses its own message (the flake this fix targets). Both warmups are attempted so a
	// cold tenant is named; waitForDeliveryLive self-resets only its OWN user, so clearAll
	// then clears BOTH trackers before measurement. Warmup never crosses the tenant boundary.
	var warmupChecks []metrics.CheckResult
	if err := waitForDeliveryLive(ctx, userA, chanA, logger); err != nil {
		warmupChecks = append(warmupChecks, metrics.CheckResult{Name: "delivery warmup tenant A", Status: metrics.CheckStatusFail, Error: err.Error()})
	}
	if err := waitForDeliveryLive(ctx, userB, chanB, logger); err != nil {
		warmupChecks = append(warmupChecks, metrics.CheckResult{Name: "delivery warmup tenant B", Status: metrics.CheckStatusFail, Error: err.Error()})
	}
	if len(warmupChecks) > 0 {
		return warmupChecks, nil // fail-closed: explicit failed check, never a silent skip
	}
	clearAll(allUsers)

	checks := make([]metrics.CheckResult, 0, 2)

	// Check 1: Publish from tenant A → only user A receives
	result := engine.PublishAndVerify(ctx, userA.AsPublisher(), chanA, []*TestUser{userA}, allUsers)
	checks = append(checks, deliveryCheck("tenant A → only A receives", result))
	clearAll(allUsers)

	// Check 2: Publish from tenant B → only user B receives
	result = engine.PublishAndVerify(ctx, userB.AsPublisher(), chanB, []*TestUser{userB}, allUsers)
	checks = append(checks, deliveryCheck("tenant B → only B receives", result))
	clearAll(allUsers)

	// Check 3: cross-tenant subscribe actively rejected — userA (tenant A) subscribes to
	// tenant B's channel and must be denied at the gateway, not merely receive nothing. The gateway
	// filters the cross-tenant channel to empty → server subscribe_error/invalid_request. ClearErrors
	// drains userA's captured-error store once before the wait (drain-once); waitForDeny owns
	// the re-issue + deny-wins probe.
	userA.ClearErrors()
	outcome, denyErr := waitForDeny(ctx,
		func() error { return userA.Client.Subscribe([]string{chanB}) },
		func() bool { return userA.HasErrorMatching(respTypeSubscribeError, wsErrCodeInvalidRequest) },
		run.denyWaitDeadline, run.denyWaitRetryInterval, logger)
	if outcome == denyOutcomeCancelled {
		return checks, nil // parent context canceled (test stopped) — not a genuine failure
	}
	checks = append(checks, denyCheckResult("cross-tenant subscribe denied", outcome, denyErr))

	return checks, nil
}
