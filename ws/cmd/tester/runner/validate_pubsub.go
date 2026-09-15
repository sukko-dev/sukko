package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// Channel rules fixture for pub-sub validation.
// dm.{principal} resolves to JWT subject — each user can subscribe/publish to their own dm.<subject>.
var testChannelRules = map[string]any{
	"public": []string{"general.*", "announce.*", "dm.{principal}"},
	"group_mappings": map[string][]string{
		"vip":     {"room.vip"},
		"traders": {"room.traders"},
	},
	"default":        []string{"general.*"},
	"publish_public": []string{"general.*", "dm.{principal}"},
	"publish_group_mappings": map[string][]string{
		"vip":     {"room.vip"},
		"traders": {"room.traders"},
	},
	"publish_default": []string{"general.*"},
}

// Catch-all routing rule for test messages.
var testRoutingRules = []map[string]any{
	{"pattern": "**", "topics": []string{routing.DefaultTopicSuffix}, "priority": routing.DefaultCatchAllPriority},
}

// setupSuiteRoutingRules applies the catch-all test routing rules to the suite tenant.
// Routing rules are ungated on every edition (ADR-0014), so this is fail-closed on every
// edition: there is no longer an edition-gate response to tolerate, and tolerating one
// would hide a regression of the ungating. Failing loudly matters because on the Kafka
// backend a missing catch-all rule silently drops every publish, which would otherwise
// surface as opaque delivery failures in later checks.
func setupSuiteRoutingRules(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string, _ zerolog.Logger) error {
	if err := provClient.SetRoutingRules(ctx, tenantID, testRoutingRules); err != nil {
		return fmt.Errorf("suite routing rules: %w", err)
	}
	return nil
}

// Test user profiles for scoping checks.
var testUserProfiles = []auth.MintOptions{
	{Subject: "test-user-a", Groups: []string{"vip"}},
	{Subject: "test-user-b", Groups: []string{"traders"}},
	{Subject: "test-user-c", Groups: nil}, // no groups — default rules only
}

func validatePubSub(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID

	// Setup: channel rules + routing rules on throwaway tenant
	if err := provClient.SetChannelRules(ctx, tenantID, testChannelRules); err != nil {
		return []metrics.CheckResult{{Name: "setup channel rules", Status: "fail", Error: err.Error()}}, nil
	}
	if err := setupSuiteRoutingRules(ctx, provClient, tenantID, logger); err != nil {
		return []metrics.CheckResult{{Name: "setup routing rules", Status: "fail", Error: err.Error()}}, nil
	}

	// Create pub-sub engine
	engine := NewPubSubEngine(PubSubEngineConfig{
		GatewayURL: run.Config.GatewayURL,
		Logger:     logger,
	})

	// Create test users
	users := make([]*TestUser, len(testUserProfiles))
	for i, profile := range testUserProfiles {
		profile.ConnIndex = i
		user, err := engine.CreateUser(ctx, run.authResult.Minter, profile)
		if err != nil {
			return []metrics.CheckResult{{Name: "create user " + profile.Subject, Status: "fail", Error: err.Error()}}, nil
		}
		users[i] = user
	}

	// Cleanup: close all clients on exit (context.Background to survive cancellation)
	defer func() {
		for _, u := range users {
			if u != nil && u.Client != nil {
				_ = u.Client.Close() // best-effort: test cleanup, multi-step continues on failure
			}
		}
	}()

	userA, userB, userC := users[0], users[1], users[2]

	// Subscribe users to their authorized channels
	generalChannel := tenantChannel(tenantID, "general.test")
	dmAChannel := tenantChannel(tenantID, "dm.test-user-a")
	vipChannel := tenantChannel(tenantID, "room.vip")
	tradersChannel := tenantChannel(tenantID, "room.traders")
	// Confirmed subscribes (#242 class): a fire-and-forget subscribe on this
	// freshly-provisioned tenant races channel-rule propagation — a silently-filtered
	// channel would make the scoping checks below fail on absence with nothing retrying.
	// The deny-path probes further down intentionally keep raw Subscribe (they assert a
	// subscribe_error, which confirmation would wait past).
	if err := userA.SubscribeConfirmed(ctx, []string{generalChannel, dmAChannel, vipChannel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe userA", Status: "fail", Error: err.Error()}}, nil
	}
	if err := userB.SubscribeConfirmed(ctx, []string{generalChannel, tradersChannel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe userB", Status: "fail", Error: err.Error()}}, nil
	}
	if err := userC.SubscribeConfirmed(ctx, []string{generalChannel}, deliveryWarmupTimeout, deliveryWarmupInterval); err != nil {
		return []metrics.CheckResult{{Name: "subscribe userC", Status: "fail", Error: err.Error()}}, nil
	}

	// Warm up the delivery loop before the scoping checks. A fixed sleep is a direct-mode
	// assumption; in Kafka mode the ws-server consumer joins this freshly-provisioned tenant's topic
	// at AtEnd only after a cold-start window, so early publishes are dropped. All test channels for
	// this tenant route to one topic, so proving liveness on generalChannel covers every check below.
	// (The scoping checks verify unique per-publish UUIDs, so a warmup straggler cannot corrupt them;
	// clearAll resets trackers anyway.)
	if err := waitForDeliveryLive(ctx, userA, generalChannel, logger); err != nil {
		return []metrics.CheckResult{{Name: "delivery warmup", Status: "fail", Error: err.Error()}}, nil
	}
	clearAll(users)

	var checks []metrics.CheckResult

	// Check 1: Public channel round-trip — all 3 receive
	result := engine.PublishAndVerify(ctx, userA.AsPublisher(), generalChannel, []*TestUser{userA, userB, userC}, users)
	checks = append(checks, deliveryCheck("public round-trip", result))
	clearAll(users)

	// Check 2: User-scoped isolation — only userA receives dm.test-user-a
	result = engine.PublishAndVerify(ctx, userA.AsPublisher(), dmAChannel, []*TestUser{userA}, users)
	checks = append(checks, deliveryCheck("user-scoped isolation", result))
	clearAll(users)

	// Check 3: Group-scoped vip — only userA receives room.vip
	result = engine.PublishAndVerify(ctx, userA.AsPublisher(), vipChannel, []*TestUser{userA}, users)
	checks = append(checks, deliveryCheck("group-scoped vip", result))
	clearAll(users)

	// Check 4: Group-scoped traders — only userB receives room.traders
	result = engine.PublishAndVerify(ctx, userB.AsPublisher(), tradersChannel, []*TestUser{userB}, users)
	checks = append(checks, deliveryCheck("group-scoped traders", result))
	clearAll(users)

	// Deny half: subscribe-deny checks first, then the strengthened
	// publish-deny check LAST. Each isolates the single unauthorized channel and asserts the
	// platform's real wire deny signal via the bounded waitForDeny helper. ClearErrors drains the
	// user's captured-error store exactly once before each wait (drain-once); waitForDeny
	// owns the re-issue + deny-wins probe and never re-clears. No positive delivery check may follow
	// these without an intervening clearAll — the deny checks are the last in this suite.

	// Check 5: Group subscribe denied — userC (no groups) subscribes to room.vip → subscribe_error.
	// The gateway filters the unauthorized channel to empty; the server replies subscribe_error
	// (invalid_request). Isolate the single unauthorized channel so the deny frame is unambiguous.
	userC.ClearErrors()
	outcome, denyErr := waitForDeny(ctx,
		func() error { return userC.Client.Subscribe([]string{vipChannel}) },
		func() bool { return userC.HasErrorMatching(respTypeSubscribeError, wsErrCodeInvalidRequest) },
		run.denyWaitDeadline, run.denyWaitRetryInterval, logger)
	if outcome == denyOutcomeCancelled {
		return checks, nil // parent context canceled (test stopped) — not a genuine failure
	}
	checks = append(checks, denyCheckResult("group subscribe denied", outcome, denyErr))

	// Check 6: User subscribe denied — userB subscribes to userA's dm channel → subscribe_error.
	// dm.{principal} resolves to the JWT subject, so userB has no rule granting dm.test-user-a.
	userB.ClearErrors()
	outcome, denyErr = waitForDeny(ctx,
		func() error { return userB.Client.Subscribe([]string{dmAChannel}) },
		func() bool { return userB.HasErrorMatching(respTypeSubscribeError, wsErrCodeInvalidRequest) },
		run.denyWaitDeadline, run.denyWaitRetryInterval, logger)
	if outcome == denyOutcomeCancelled {
		return checks, nil
	}
	checks = append(checks, denyCheckResult("user subscribe denied", outcome, denyErr))

	// Check 7 (strengthened, LAST): Publish authorization — userC publishes to room.vip,
	// which it may not publish to → publish_error/forbidden. Positive vip publish stays covered by
	// check 3 (group-scoped vip). ClearErrors here genuinely drains userC's leftover subscribe_error
	// from check 5 (drain-once exercise). No transport-error-pass or silently-dropped-pass branch: the
	// sole pass path is the observed publish_error/forbidden deny frame.
	userC.ClearErrors()
	outcome, denyErr = waitForDeny(ctx,
		func() error { return userC.Client.Publish(vipChannel, []byte(`{"msg_id":"auth-test","ts":0}`)) },
		func() bool { return userC.HasErrorMatching(respTypePublishError, wsErrCodeForbidden) },
		run.denyWaitDeadline, run.denyWaitRetryInterval, logger)
	if outcome == denyOutcomeCancelled {
		return checks, nil
	}
	checks = append(checks, denyCheckResult("publish auth rejection", outcome, denyErr))

	return checks, nil
}

func deliveryCheck(name string, result DeliveryResult) metrics.CheckResult {
	if result.Delivered && len(result.MisroutedTo) == 0 {
		return metrics.CheckResult{
			Name:    name,
			Status:  "pass",
			Latency: result.Latency.Round(time.Millisecond).String(),
		}
	}

	errMsg := ""
	if !result.Delivered {
		if result.PublishErr != nil {
			errMsg = fmt.Sprintf("publish failed: %v", result.PublishErr)
		} else {
			errMsg = fmt.Sprintf("missing: %v", result.Missing)
		}
	}
	if len(result.MisroutedTo) > 0 {
		if errMsg != "" {
			errMsg += "; "
		}
		errMsg += fmt.Sprintf("misrouted to: %v", result.MisroutedTo)
	}

	return metrics.CheckResult{
		Name:   name,
		Status: "fail",
		Error:  errMsg,
	}
}

func clearAll(users []*TestUser) {
	for _, u := range users {
		u.ClearReceived()
	}
}
