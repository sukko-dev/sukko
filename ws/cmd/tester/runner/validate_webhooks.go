package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/routing"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

const (
	webhookTestChannel = "webhook.test.event"
	// webhookTestChannelPatt is the bare-channel pattern used for webhook subscription.
	// The webhook-worker matches against the bare channel (without tenant prefix), so
	// "webhook.test.*" is correct here.
	webhookTestChannelPatt = "webhook.test.*"
	// webhookTestRoutingPatt is the Kafka routing rule pattern. Routing is matched against
	// the full tenant-qualified channel (e.g. "tenant-slug.webhook.test.event"), so a bare
	// "webhook.test.*" would fail on the first segment. "**" catches all channels, routing
	// any publish to the test topic where the webhook-worker picks it up.
	webhookTestRoutingPatt = "**"
	webhookTestTopic       = "webhook-test"
	webhookTestSecretLen   = 32
	webhookPollInterval    = 500 * time.Millisecond
)

func validateWebhooks(
	ctx context.Context,
	run *TestRun,
	logger zerolog.Logger,
	fetchEditionFn func(context.Context, string) (license.Edition, error),
) ([]metrics.CheckResult, error) {
	// Gate 1: skip when no webhook receiver URL is configured.
	if run.webhookBaseURL == "" {
		return []metrics.CheckResult{{
			Name:   "webhooks",
			Status: metrics.CheckStatusSkip,
			Error:  "TESTER_WEBHOOK_BASE_URL not set; skipping webhook suite",
		}}, nil
	}

	// Gate 2: skip when the current edition does not include webhook delivery.
	edition, err := fetchEditionFn(ctx, run.Config.GatewayURL)
	if err != nil {
		return []metrics.CheckResult{{
			Name:   "webhooks/edition-check",
			Status: metrics.CheckStatusFail,
			Error:  fmt.Sprintf("fetch edition: %v", err),
		}}, nil
	}
	if !license.EditionHasFeature(edition, license.Webhooks) {
		return []metrics.CheckResult{{
			Name:   "webhooks",
			Status: metrics.CheckStatusSkip,
			Error:  fmt.Sprintf("edition %q does not include webhook delivery", edition),
		}}, nil
	}

	var checks []metrics.CheckResult

	// T-006: happy-path — single delivery, receiver returns 200.
	happyChecks, err := runWebhookScenario(ctx, run, logger, "happy-path", 0, 0, 1, false)
	checks = append(checks, happyChecks...)
	if err != nil {
		return checks, err
	}

	// T-007: retry-success — first two attempts fail (500), third succeeds (200).
	retryChecks, err := runWebhookScenario(ctx, run, logger, "retry-success", 2, 0, 3, false)
	checks = append(checks, retryChecks...)
	if err != nil {
		return checks, err
	}

	// T-008: degraded — receiver always returns 500; maxRetries=2 forces degraded transition.
	degradedChecks, err := runWebhookScenario(ctx, run, logger, "degraded", -1, 2, 2, true)
	checks = append(checks, degradedChecks...)
	if err != nil {
		return checks, err
	}

	return checks, nil
}

// runWebhookScenario provisions a webhook, publishes a message, and asserts delivery behavior.
//
//   - failFirstN: 0=always 200; N>0=first N fail; -1=always 500
//   - maxRetries: 0 = use provisioning default (5); passed as-is to CreateWebhook
//   - wantCount: expected number of delivery attempts in the store
//   - wantDegraded: if true, polls GetWebhookByID for degraded status instead of counting
func runWebhookScenario(
	ctx context.Context,
	run *TestRun,
	logger zerolog.Logger,
	name string,
	failFirstN, maxRetries, wantCount int,
	wantDegraded bool,
) ([]metrics.CheckResult, error) {
	provClient := run.authResult.ProvClient
	tenantID := run.authResult.TenantID
	token := run.authResult.TokenFunc(0)
	pfx := "webhooks/" + name + "/"

	// Step 1: generate a unique runID and secret for this scenario.
	runID := uuid.NewString()
	secret := make([]byte, webhookTestSecretLen)
	if _, err := io.ReadFull(rand.Reader, secret); err != nil {
		return fail(pfx+"setup", fmt.Sprintf("generate secret: %v", err)), nil
	}

	// Step 2: register in the store; defer removal (zeroes secret bytes).
	run.webhookStore.register(runID, secret, failFirstN)
	defer run.webhookStore.delete(runID)

	// Step 3: provision routing rule so the webhook-worker receives broadcasts.
	if err := provClient.SetRoutingRules(ctx, tenantID, []map[string]any{
		{"pattern": webhookTestRoutingPatt, "topics": []string{webhookTestTopic}, "priority": routing.DefaultCatchAllPriority},
	}); err != nil {
		return fail(pfx+"routing-rule", fmt.Sprintf("set routing rules: %v", err)), nil
	}
	defer func() { _ = provClient.DeleteRoutingRules(context.Background(), tenantID) }() //nolint:contextcheck // context.Background(): request ctx may be canceled at defer time; error: best-effort cleanup, test already reported its result

	// Step 4: register webhook with the provisioning service.
	webhookURL := run.webhookBaseURL + "/webhook-receive/" + runID
	webhookID, err := provClient.CreateWebhook(
		ctx, tenantID, webhookURL, webhookTestChannelPatt,
		hex.EncodeToString(secret), maxRetries,
	)
	if err != nil {
		return fail(pfx+"create-webhook", fmt.Sprintf("create webhook: %v", err)), nil
	}
	defer func() { _ = provClient.DeleteWebhook(context.Background(), tenantID, webhookID) }() //nolint:contextcheck // context.Background(): request ctx may be canceled at defer time; error: best-effort cleanup, test already reported its result

	// Step 5: publish to the test channel to trigger delivery.
	restClient := restpublish.NewClient(httpURL(run.Config.GatewayURL))
	publish := func() error {
		_, pubErr := restClient.Publish(ctx, restpublish.Request{
			Channel: tenantID + "." + webhookTestChannel,
			Data:    []byte(`{"type":"webhook-test"}`),
		}, restpublish.AuthConfig{Token: token})
		if pubErr != nil {
			return fmt.Errorf("rest publish: %w", pubErr)
		}
		return nil
	}
	if err := publish(); err != nil {
		return fail(pfx+"publish", fmt.Sprintf("publish: %v", err)), nil
	}

	// Step 6: poll for delivery or degraded status.
	var checks []metrics.CheckResult
	if wantDegraded {
		checks = append(checks, pollDegradedStatus(ctx, run, logger, name, pfx, provClient, tenantID, webhookID)...)
	} else {
		checks = append(checks, pollDeliveries(ctx, run, pfx, runID, publish, failFirstN, wantCount)...)
	}

	// Step 7: assert delivery count and signature/status-code correctness.
	checks = append(checks, assertDeliveries(run, pfx, runID, failFirstN, wantCount, wantDegraded)...)
	return checks, nil
}

// pollDegradedStatus polls GetWebhookByID until the webhook reaches "degraded" status.
func pollDegradedStatus(
	ctx context.Context,
	run *TestRun,
	logger zerolog.Logger,
	name string,
	pfx string,
	provClient interface {
		GetWebhookByID(context.Context, string, string) (string, error)
	},
	tenantID, webhookID string,
) []metrics.CheckResult {
	timeout := run.webhookRetryTimeout
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(webhookPollInterval)
	defer ticker.Stop()

	var finalStatus string
	for {
		select {
		case <-ctx.Done():
			return fail(pfx+"degraded-status", "context canceled waiting for degraded status")
		case <-deadline.C:
			return fail(pfx+"degraded-status",
				fmt.Sprintf("webhook did not reach degraded status within %s (last: %q)", timeout, finalStatus))
		case <-ticker.C:
			status, err := provClient.GetWebhookByID(ctx, tenantID, webhookID)
			if err != nil {
				logger.Debug().Err(err).Str("scenario", name).Msg("GetWebhookByID error during poll")
				continue
			}
			finalStatus = status
			if status == types.WebhookStatusDegraded {
				return []metrics.CheckResult{{Name: pfx + "degraded-status", Status: metrics.CheckStatusPass}}
			}
		}
	}
}

// pollDeliveries polls the webhook store until wantCount deliveries arrive or timeout.
// For happy-path (failFirstN==0), a cache-hydration re-publish is attempted if no
// deliveries arrive by the first timeout.
func pollDeliveries(
	ctx context.Context,
	run *TestRun,
	pfx string,
	runID string,
	publish func() error,
	failFirstN, wantCount int,
) []metrics.CheckResult {
	timeout := run.webhookDeliveryTimeout
	if failFirstN != 0 {
		timeout = run.webhookRetryTimeout
	}

	countDeliveries := func() int {
		entry := run.webhookStore.get(runID)
		if entry == nil {
			return 0
		}
		return func() int {
			entry.mu.RLock()
			defer entry.mu.RUnlock()
			return len(entry.deliveries)
		}()
	}

	if waitForCount(ctx, countDeliveries, wantCount, timeout) {
		return nil
	}

	// Cache-hydration re-publish for happy-path only.
	if failFirstN == 0 && countDeliveries() == 0 {
		if err := publish(); err == nil {
			if waitForCount(ctx, countDeliveries, wantCount, timeout) {
				return nil
			}
		}
	}

	return fail(pfx+"delivery-timeout",
		fmt.Sprintf("got %d deliveries after %s, want %d", countDeliveries(), timeout, wantCount))
}

// waitForCount polls countFn at webhookPollInterval until it returns >= target or timeout.
func waitForCount(ctx context.Context, countFn func() int, target int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(webhookPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return countFn() >= target
		case <-ticker.C:
			if countFn() >= target {
				return true
			}
		}
	}
}

// assertDeliveries reads delivery records and emits check results for count, signatures,
// and status codes.
func assertDeliveries(
	run *TestRun,
	pfx string,
	runID string,
	failFirstN, wantCount int,
	wantDegraded bool,
) []metrics.CheckResult {
	entry := run.webhookStore.get(runID)
	var deliveries []WebhookDelivery
	if entry != nil {
		func() {
			entry.mu.RLock()
			defer entry.mu.RUnlock()
			deliveries = make([]WebhookDelivery, len(entry.deliveries))
			copy(deliveries, entry.deliveries)
		}()
	}

	var checks []metrics.CheckResult

	// Count check (always, including degraded path).
	if len(deliveries) == wantCount {
		checks = append(checks, metrics.CheckResult{Name: pfx + "delivery-count", Status: metrics.CheckStatusPass})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: pfx + "delivery-count", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("got %d deliveries, want %d", len(deliveries), wantCount),
		})
	}

	if wantDegraded || len(deliveries) == 0 {
		return checks
	}

	// Signature check.
	invalidSigs := 0
	for _, d := range deliveries {
		if !d.SignatureOK {
			invalidSigs++
		}
	}
	if invalidSigs > 0 {
		checks = append(checks, metrics.CheckResult{
			Name: pfx + "signatures", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("%d of %d deliveries had invalid HMAC signatures", invalidSigs, len(deliveries)),
		})
	} else {
		checks = append(checks, metrics.CheckResult{Name: pfx + "signatures", Status: metrics.CheckStatusPass})
	}

	// Status code check.
	statusOK := true
	for i, d := range deliveries {
		want := http200orFail(i, failFirstN)
		if d.StatusCode != want {
			checks = append(checks, metrics.CheckResult{
				Name: pfx + "status-codes", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("delivery %d: status=%d, want %d", i+1, d.StatusCode, want),
			})
			statusOK = false
			break
		}
	}
	if statusOK {
		checks = append(checks, metrics.CheckResult{Name: pfx + "status-codes", Status: metrics.CheckStatusPass})
	}

	return checks
}

// http200orFail returns the expected HTTP status code for the i-th delivery (0-indexed).
// failFirstN: 0=all 200; N>0=first N are 500; -1=all 500.
func http200orFail(i, failFirstN int) int {
	if failFirstN < 0 || (failFirstN > 0 && i < failFirstN) {
		return 500
	}
	return 200
}

// fail is a helper that returns a single failing check result in a slice.
func fail(name, errMsg string) []metrics.CheckResult {
	return []metrics.CheckResult{{Name: name, Status: metrics.CheckStatusFail, Error: errMsg}}
}
