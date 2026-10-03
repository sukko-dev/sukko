package runner

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// stubEditionFn returns a fixed edition for testing gate logic.
func stubEditionFn(edition license.Edition) func(context.Context, string) (license.Edition, error) {
	return func(_ context.Context, _ string) (license.Edition, error) {
		return edition, nil
	}
}

// TestValidateWebhooks_SkipWhenNoBaseURL verifies Gate 1: when webhookBaseURL is empty the
// suite returns a single skip result without contacting any external service.
func TestValidateWebhooks_SkipWhenNoBaseURL(t *testing.T) {
	t.Parallel()

	run := &TestRun{webhookBaseURL: ""}
	logger := zerolog.Nop()
	fetchFn := func(_ context.Context, _ string) (license.Edition, error) {
		t.Error("fetchEditionFn must not be called when webhookBaseURL is empty")
		return license.Community, nil
	}

	checks, err := validateWebhooks(context.Background(), run, logger, fetchFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("got %d checks, want 1", len(checks))
	}
	if checks[0].Status != metrics.CheckStatusSkip {
		t.Errorf("status = %q, want %q", checks[0].Status, metrics.CheckStatusSkip)
	}
}

// TestValidateWebhooks_SkipCommunityEdition verifies Gate 2: when the license is Community
// (no webhook feature) the suite returns a single skip result.
func TestValidateWebhooks_SkipCommunityEdition(t *testing.T) {
	t.Parallel()

	run := &TestRun{
		webhookBaseURL: "http://tester.internal",
		webhookStore:   newWebhookStore(),
	}
	logger := zerolog.Nop()

	checks, err := validateWebhooks(context.Background(), run, logger, stubEditionFn(license.Community))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("got %d checks, want 1", len(checks))
	}
	if checks[0].Status != metrics.CheckStatusSkip {
		t.Errorf("status = %q, want %q", checks[0].Status, metrics.CheckStatusSkip)
	}
}

// TestValidateWebhooks_EditionFetchUsesHTTPScheme verifies the suite rewrites the gateway
// ws:// URL to http:// before fetching /edition. fetchCurrentEdition does an HTTP GET and
// Go's http.Client rejects a ws:// scheme ("unsupported protocol scheme \"ws\""), so passing
// run.Config.GatewayURL raw fails the edition check. Regression guard for the pro-webhooks grid
// cell, which first exercised this path against a real gateway.
func TestValidateWebhooks_EditionFetchUsesHTTPScheme(t *testing.T) {
	t.Parallel()

	run := &TestRun{
		webhookBaseURL: "http://tester.internal",
		webhookStore:   newWebhookStore(),
		Config:         TestConfig{GatewayURL: "ws://ws-gateway:3000"},
	}
	logger := zerolog.Nop()

	var gotURL string
	fetchFn := func(_ context.Context, baseURL string) (license.Edition, error) {
		gotURL = baseURL
		return license.Community, nil // short-circuit to a clean skip after the edition check
	}

	if _, err := validateWebhooks(context.Background(), run, logger, fetchFn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "http://ws-gateway:3000"; gotURL != want {
		t.Errorf("edition fetch URL = %q, want %q (ws:// must be rewritten to http:// — http.Get rejects the ws scheme)", gotURL, want)
	}
}

// TestWebhookRoutingRules_TargetsDefaultTopic pins the webhook suite's routing rule to the
// tenant's always-provisioned default topic. A custom ingress topic trips the ADR-0006
// topic-universe gate (HTTP 400 TOPIC_NOT_PROVISIONED) — the failure that surfaced the first
// time the pro-webhooks grid cell ran the suite against a real provisioning service. Mirrors the
// working kafka_ingest suite, which routes "**" to routing.DefaultTopicSuffix.
func TestWebhookRoutingRules_TargetsDefaultTopic(t *testing.T) {
	t.Parallel()

	rules := webhookRoutingRules()
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(rules))
	}
	if got := rules[0]["ingress_topic"]; got != routing.DefaultTopicSuffix {
		t.Errorf("ingress_topic = %v, want %q (a custom topic trips the topic-universe gate)", got, routing.DefaultTopicSuffix)
	}
	if got := rules[0]["pattern"]; got != webhookTestRoutingPatt {
		t.Errorf("pattern = %v, want %q", got, webhookTestRoutingPatt)
	}
}

func TestHttp200orFail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		i          int
		failFirstN int
		want       int
	}{
		{"all_pass_delivery_0", 0, 0, 200},
		{"all_pass_delivery_5", 5, 0, 200},
		{"first_two_fail_delivery_0", 0, 2, 500},
		{"first_two_fail_delivery_1", 1, 2, 500},
		{"first_two_fail_delivery_2", 2, 2, 200},
		{"first_two_fail_delivery_3", 3, 2, 200},
		{"always_fail_delivery_0", 0, -1, 500},
		{"always_fail_delivery_10", 10, -1, 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := http200orFail(tt.i, tt.failFirstN); got != tt.want {
				t.Errorf("http200orFail(%d, %d) = %d, want %d", tt.i, tt.failFirstN, got, tt.want)
			}
		})
	}
}

// storeWithDeliveries populates a WebhookStore with pre-built deliveries for runID.
func storeWithDeliveries(runID string, deliveries []WebhookDelivery) *WebhookStore {
	s := newWebhookStore()
	s.register(runID, []byte("secret"), 0)
	entry := s.get(runID)
	func() {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.deliveries = deliveries
	}()
	return s
}

func TestAssertDeliveries_CountPass(t *testing.T) {
	t.Parallel()
	runID := "test-run-1"
	deliveries := []WebhookDelivery{
		{Sequence: 1, SignatureOK: true, StatusCode: 200},
	}
	run := &TestRun{webhookStore: storeWithDeliveries(runID, deliveries)}
	checks := assertDeliveries(run, "pfx/", runID, 0, 1, false)
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Status != metrics.CheckStatusPass {
		t.Errorf("delivery-count: got %q, want pass; error: %s", checks[0].Status, checks[0].Error)
	}
}

func TestAssertDeliveries_CountMismatch(t *testing.T) {
	t.Parallel()
	runID := "test-run-2"
	run := &TestRun{webhookStore: storeWithDeliveries(runID, nil)}
	checks := assertDeliveries(run, "pfx/", runID, 0, 3, false)
	if len(checks) == 0 {
		t.Fatal("expected at least one check")
	}
	if checks[0].Status != metrics.CheckStatusFail {
		t.Errorf("delivery-count: got %q, want fail", checks[0].Status)
	}
}

func TestAssertDeliveries_DegradedEarlyReturn(t *testing.T) {
	t.Parallel()
	runID := "test-run-3"
	deliveries := []WebhookDelivery{
		{Sequence: 1, SignatureOK: true, StatusCode: 500},
		{Sequence: 2, SignatureOK: true, StatusCode: 500},
	}
	run := &TestRun{webhookStore: storeWithDeliveries(runID, deliveries)}
	// wantDegraded=true: only delivery-count check is emitted; no sig/status checks.
	checks := assertDeliveries(run, "pfx/", runID, -1, 2, true)
	if len(checks) != 1 {
		t.Errorf("want 1 check (count only), got %d", len(checks))
	}
	if checks[0].Status != metrics.CheckStatusPass {
		t.Errorf("delivery-count: got %q, want pass", checks[0].Status)
	}
}

func TestAssertDeliveries_InvalidSignature(t *testing.T) {
	t.Parallel()
	runID := "test-run-4"
	deliveries := []WebhookDelivery{
		{Sequence: 1, SignatureOK: false, StatusCode: 200},
	}
	run := &TestRun{webhookStore: storeWithDeliveries(runID, deliveries)}
	checks := assertDeliveries(run, "pfx/", runID, 0, 1, false)
	hasSignatureFail := false
	for _, c := range checks {
		if c.Name == "pfx/signatures" && c.Status == metrics.CheckStatusFail {
			hasSignatureFail = true
		}
	}
	if !hasSignatureFail {
		t.Error("expected signatures check to fail")
	}
}

func TestAssertDeliveries_StatusCodeMismatch(t *testing.T) {
	t.Parallel()
	runID := "test-run-5"
	// failFirstN=2 means delivery 0 and 1 should be 500; delivery 2 should be 200.
	// We intentionally give delivery 0 a 200 to trigger a mismatch.
	deliveries := []WebhookDelivery{
		{Sequence: 1, SignatureOK: true, StatusCode: 200},
		{Sequence: 2, SignatureOK: true, StatusCode: 500},
		{Sequence: 3, SignatureOK: true, StatusCode: 200},
	}
	run := &TestRun{webhookStore: storeWithDeliveries(runID, deliveries)}
	checks := assertDeliveries(run, "pfx/", runID, 2, 3, false)
	hasStatusFail := false
	for _, c := range checks {
		if c.Name == "pfx/status-codes" && c.Status == metrics.CheckStatusFail {
			hasStatusFail = true
		}
	}
	if !hasStatusFail {
		t.Error("expected status-codes check to fail")
	}
}
