package runner

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

func validateReconnect(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID

	// Setup: routing rules so publishes are routed to the bus.
	// Error ignored: if this fails, delivery checks below will fail with clear errors.
	_ = provClient.SetRoutingRules(ctx, tenantID, testRoutingRules)

	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
	})

	checks := make([]metrics.CheckResult, 0, 4)

	// Phase 1: Connect, subscribe, verify delivery
	user1, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{
		Subject: "reconnect-test",
	})
	if err != nil {
		return []metrics.CheckResult{{Name: "initial connect", Status: "fail", Error: err.Error()}}, nil
	}

	// Confirmed subscribe: a fire-and-forget Subscribe on a freshly-provisioned tenant
	// races channel-rule propagation — the gateway silently filters the channel and
	// nothing ever retries, so the delivery check below fails with the message missing
	// (the #242 class; same fix as the tenant-isolation suite).
	if err := user1.SubscribeConfirmed(ctx, []string{tenantChannel(tenantID, "general.test")}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		_ = user1.Client.Close()
		return []metrics.CheckResult{{Name: "initial subscribe", Status: "fail", Error: err.Error()}}, nil
	}

	result := engine.PublishAndVerify(ctx, user1.AsPublisher(), tenantChannel(tenantID, "general.test"), []*TestUser{user1}, []*TestUser{user1})
	checks = append(checks, deliveryCheck("pre-disconnect delivery", result))

	// Phase 2: Disconnect
	_ = user1.Client.Close()
	time.Sleep(500 * time.Millisecond) // allow gateway to process disconnect

	// Phase 3: Reconnect with same subject (fresh token, same identity)
	user2, err := engine.CreateUser(ctx, run.authResult.Minter, auth.MintOptions{
		Subject: "reconnect-test",
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{Name: "reconnect", Status: "fail", Error: err.Error()})
		return checks, nil
	}
	defer func() { _ = user2.Client.Close() }()

	checks = append(checks, metrics.CheckResult{Name: "reconnect", Status: "pass"})

	// Phase 4: Re-subscribe (confirmed, as above) and verify delivery
	if err := user2.SubscribeConfirmed(ctx, []string{tenantChannel(tenantID, "general.test")}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		checks = append(checks, metrics.CheckResult{Name: "re-subscribe", Status: "fail", Error: err.Error()})
		return checks, nil
	}

	result = engine.PublishAndVerify(ctx, user2.AsPublisher(), tenantChannel(tenantID, "general.test"), []*TestUser{user2}, []*TestUser{user2})
	checks = append(checks, deliveryCheck("post-reconnect delivery", result))

	return checks, nil
}
