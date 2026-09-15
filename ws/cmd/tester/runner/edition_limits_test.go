package runner

import (
	"context"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/cmd/tester/metrics"
)

// TestClassifyRoutingRules pins the per-edition routing-rules classification, including the
// CROSSED negatives that the DISTINCT rejections make possible. Routing rules are ungated on
// every edition (ADR-0014), so the boundary is always a COUNT — but which rejection fires
// depends on how the edition limit compares to the config limit (MAX_ROUTING_RULES_PER_TENANT,
// envDefault 100), because the ReplaceRoutingRules handler's config validator runs BEFORE the
// service-level edition check:
//   - Community (edition limit 10 < config 100): the config validator passes an 11-rule request,
//     so the service edition check rejects it — 403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT.
//   - Paid (Pro edition limit 100 == config 100): the config validator rejects first —
//     400 TOO_MANY_ROUTING_RULES, leaving the edition check unreachable.
//
// Because EDITION_LIMIT is a PREFIX of the EDITION_LIMIT_* codes, matching is on the EXACT `code`
// field: a generic feature-gate 403 (which would mean the ungating regressed), the other edition's
// count rejection, and a setup failure (400 TOPIC_NOT_PROVISIONED) MUST NONE read as the asserted
// boundary for the edition under test.
func TestClassifyRoutingRules(t *testing.T) {
	t.Parallel()

	const (
		editionLimitBody = `HTTP 403: {"code":"EDITION_LIMIT","message":"This feature requires pro edition or higher"}`
		tooManyBody      = `HTTP 400: {"code":"TOO_MANY_ROUTING_RULES","message":"got 101, max 100"}`
		editionCountBody = `HTTP 403: {"code":"EDITION_LIMIT_ROUTING_RULES_PER_TENANT","message":"routing rules per tenant limit exceeded"}`
		topicNotProvBody = `HTTP 400: {"code":"TOPIC_NOT_PROVISIONED","message":"topic not provisioned: local.x.test0"}`
		otherForbidden   = `HTTP 403: {"code":"FORBIDDEN","message":"nope"}`
	)

	tests := []struct {
		name       string
		edition    string
		statusCode int
		errText    string
		wantName   string
		wantStatus string
	}{
		{
			name:       "community count boundary 403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT passes",
			edition:    "community",
			statusCode: http.StatusForbidden,
			errText:    editionCountBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusPass,
		},
		{
			name:       "community CROSSED: generic feature-gate 403 EDITION_LIMIT means the gate regressed, NOT the count boundary",
			edition:    "community",
			statusCode: http.StatusForbidden,
			errText:    editionLimitBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community CROSSED: config-validator 400 TOO_MANY_ROUTING_RULES is NOT the asserted (edition-manager 403) boundary",
			edition:    "community",
			statusCode: http.StatusBadRequest,
			errText:    tooManyBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community CROSSED: 400 TOPIC_NOT_PROVISIONED (setup failure) is NOT the count boundary",
			edition:    "community",
			statusCode: http.StatusBadRequest,
			errText:    topicNotProvBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community 403 without a recognized code fails",
			edition:    "community",
			statusCode: http.StatusForbidden,
			errText:    otherForbidden,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community over-count unexpectedly accepted (200) fails",
			edition:    "community",
			statusCode: http.StatusOK,
			errText:    "",
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "pro count boundary 400 TOO_MANY_ROUTING_RULES passes",
			edition:    "pro",
			statusCode: http.StatusBadRequest,
			errText:    tooManyBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusPass,
		},
		{
			name:       "pro CROSSED: generic feature-gate 403 EDITION_LIMIT is NOT the count boundary",
			edition:    "pro",
			statusCode: http.StatusForbidden,
			errText:    editionLimitBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "pro CROSSED: edition-manager 403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT is NOT the asserted (config-validator 400) boundary",
			edition:    "pro",
			statusCode: http.StatusForbidden,
			errText:    editionCountBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "pro CROSSED: 400 TOPIC_NOT_PROVISIONED (setup failure) is NOT the count boundary",
			edition:    "pro",
			statusCode: http.StatusBadRequest,
			errText:    topicNotProvBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "pro over-count unexpectedly accepted (200) fails",
			edition:    "pro",
			statusCode: http.StatusOK,
			errText:    "",
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "enterprise paid-branch count boundary 400 TOO_MANY_ROUTING_RULES passes",
			edition:    "enterprise",
			statusCode: http.StatusBadRequest,
			errText:    tooManyBody,
			wantName:   "routing rules limit rejection",
			wantStatus: metrics.CheckStatusPass,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyRoutingRules(tc.edition, tc.statusCode, tc.errText)
			if got.Name != tc.wantName {
				t.Errorf("classifyRoutingRules(%q, %d) name = %q, want %q", tc.edition, tc.statusCode, got.Name, tc.wantName)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("classifyRoutingRules(%q, %d) status = %q, want %q (result: %+v)", tc.edition, tc.statusCode, got.Status, tc.wantStatus, got)
			}
		})
	}
}

// TestEditionTestTenantSlugsAreValid guards against a regression where the test-tenant slug
// prefix produces slugs that provisioning rejects. Tenant slugs must match
// ^[a-z][a-z0-9-]{2,62}$ (see internal/provisioning/types.go); an underscore-based prefix
// yields an invalid slug rejected with HTTP 400 SLUG_INVALID, which would fail the
// edition-limits grid cell. The uuid.New().String()[:8] suffix is always 8 lowercase-hex chars (no hyphen).
func TestEditionTestTenantSlugsAreValid(t *testing.T) {
	t.Parallel()
	// Mirror provisioning's tenantSlugPattern.
	slugRe := regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)
	const sampleUUID8 = "550e8400" // representative uuid.New().String()[:8]
	for _, slug := range []string{
		editionTestTenantPrefix + sampleUUID8,             // shared/headroom test tenants
		editionTestTenantPrefix + "reject-" + sampleUUID8, // boundary-rejection tenant
	} {
		if !slugRe.MatchString(slug) {
			t.Errorf("test-tenant slug %q is not a valid tenant slug (must match %s)", slug, slugRe)
		}
	}
}

// TestConnLimitRejectionCheckName pins the exported check-name constant that the e2e skip
// allow-list depends on. taskfiles/e2e/battery_guard_test.sh greps this exact value out of
// edition_limits.go and asserts taskfiles/e2e.yml declares edition-limits:<value>;
// this test is the Go half of that cross-file binding — renaming the constant here forces a
// deliberate, test-breaking change that the bash half will then flag if e2e.yml lags.
func TestConnLimitRejectionCheckName(t *testing.T) {
	t.Parallel()
	if ConnLimitRejectionCheckName != "connection limit rejection" {
		t.Fatalf("ConnLimitRejectionCheckName = %q; the e2e ALLOWED_SKIPS entry (edition-limits:connection limit rejection) and battery_guard_test.sh must be updated in lockstep", ConnLimitRejectionCheckName)
	}
}

// TestExtractErrorCode pins exact-field code extraction. The critical case is distinguishing
// EDITION_LIMIT from EDITION_LIMIT_ROUTING_RULES_PER_TENANT — a substring match would conflate
// them, re-introducing the vacuity that lets a feature-gate 403 pass as the count boundary.
func TestExtractErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		errBody string
		want    string
	}{
		{
			name:    "feature gate EDITION_LIMIT",
			errBody: `set routing rules: HTTP 403: {"code":"EDITION_LIMIT","message":"This feature requires pro edition or higher"}`,
			want:    "EDITION_LIMIT",
		},
		{
			name:    "count boundary EDITION_LIMIT_ROUTING_RULES_PER_TENANT (NOT conflated with EDITION_LIMIT)",
			errBody: `set routing rules: HTTP 403: {"code":"EDITION_LIMIT_ROUTING_RULES_PER_TENANT","message":"limit exceeded"}`,
			want:    "EDITION_LIMIT_ROUTING_RULES_PER_TENANT",
		},
		{
			name:    "topic not provisioned",
			errBody: `HTTP 400: {"code":"TOPIC_NOT_PROVISIONED","message":"topic not provisioned"}`,
			want:    "TOPIC_NOT_PROVISIONED",
		},
		{name: "no JSON object", errBody: "sse connect: HTTP 400", want: ""},
		{name: "empty", errBody: "", want: ""},
		{name: "JSON without code field", errBody: `HTTP 403: {"message":"nope"}`, want: ""},
		{name: "malformed JSON after brace", errBody: `HTTP 403: {not json`, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := extractErrorCode(tc.errBody); got != tc.want {
				t.Errorf("extractErrorCode(%q) = %q, want %q", tc.errBody, got, tc.want)
			}
		})
	}
}

// TestProbeForEdition pins the Community-single-shot vs paid-settle-retry split — the mechanism
// that absorbs the async channel-rule propagation window without masking a genuine failure.
func TestProbeForEdition(t *testing.T) {
	t.Parallel()

	connected := gateProbeResult{statusCode: http.StatusOK, connected: true}
	blocked := gateProbeResult{statusCode: http.StatusForbidden, code: "FORBIDDEN"}

	t.Run("community probes exactly once, no retry", func(t *testing.T) {
		t.Parallel()
		calls := 0
		probe := func() gateProbeResult { calls++; return blocked }
		got := probeForEdition(context.Background(), "community", probe)
		if calls != 1 {
			t.Errorf("community probe calls = %d, want 1 (no retry)", calls)
		}
		if got.connected {
			t.Error("expected the blocked observation to be returned unmasked")
		}
	})

	t.Run("paid returns immediately when first probe connects", func(t *testing.T) {
		t.Parallel()
		calls := 0
		probe := func() gateProbeResult { calls++; return connected }
		got := probeForEdition(context.Background(), "pro", probe)
		if calls != 1 {
			t.Errorf("paid probe calls = %d, want 1 when it connects immediately", calls)
		}
		if !got.connected {
			t.Error("expected connected")
		}
	})
}

// TestProbeUntilAllowed drives the settle loop with a tiny budget so the deadline/tick/connect-on-
// Nth-try paths are covered fast, and asserts a persistent failure returns the LAST observation
// (never masked as success).
func TestProbeUntilAllowed(t *testing.T) {
	t.Parallel()

	connected := gateProbeResult{statusCode: http.StatusOK, connected: true}
	blocked := gateProbeResult{statusCode: http.StatusForbidden, code: "FORBIDDEN"}

	t.Run("connects on the 3rd try", func(t *testing.T) {
		t.Parallel()
		calls := 0
		probe := func() gateProbeResult {
			calls++
			if calls >= 3 {
				return connected
			}
			return blocked
		}
		got := probeUntilAllowed(context.Background(), probe, 2*time.Second, time.Millisecond)
		if !got.connected {
			t.Errorf("expected connected after retries, got %+v (calls=%d)", got, calls)
		}
		if calls < 3 {
			t.Errorf("calls = %d, want >= 3", calls)
		}
	})

	t.Run("never connects returns last observation after budget", func(t *testing.T) {
		t.Parallel()
		probe := func() gateProbeResult { return blocked }
		got := probeUntilAllowed(context.Background(), probe, 30*time.Millisecond, time.Millisecond)
		if got.connected {
			t.Error("a persistent failure must NOT be masked as connected")
		}
		if got.statusCode != http.StatusForbidden || got.code != "FORBIDDEN" {
			t.Errorf("last observation = %+v, want the blocked result", got)
		}
	})

	t.Run("canceled context returns promptly with last observation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		probe := func() gateProbeResult { return blocked }
		got := probeUntilAllowed(ctx, probe, time.Hour, time.Hour)
		if got.connected {
			t.Error("expected non-connected on canceled ctx")
		}
	})
}

// TestClassifyFeatureGate pins the SSE/REST-publish feature-gate discrimination, including the
// CROSSED negatives: on Community a setup 403 (FORBIDDEN, channel-tenant mismatch) MUST NOT pass
// as the gate 403 (EDITION_LIMIT), and a probe that unexpectedly succeeds MUST fail; on paid the
// gate is open so only a successful probe passes.
func TestClassifyFeatureGate(t *testing.T) {
	t.Parallel()

	const name = "sse feature gate"

	tests := []struct {
		name       string
		edition    string
		statusCode int
		code       string
		connected  bool
		wantStatus string
	}{
		{
			name:       "community gate 403 EDITION_LIMIT passes",
			edition:    "community",
			statusCode: http.StatusForbidden,
			code:       "EDITION_LIMIT",
			wantStatus: metrics.CheckStatusPass,
		},
		{
			name:       "community CROSSED: setup 403 FORBIDDEN is NOT the gate",
			edition:    "community",
			statusCode: http.StatusForbidden,
			code:       "FORBIDDEN",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community CROSSED: probe succeeded (gate regressed) fails",
			edition:    "community",
			statusCode: http.StatusOK,
			connected:  true,
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "community non-403 fails",
			edition:    "community",
			statusCode: http.StatusBadRequest,
			code:       "",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "pro gate open, probe connected passes",
			edition:    "pro",
			statusCode: http.StatusOK,
			connected:  true,
			wantStatus: metrics.CheckStatusPass,
		},
		{
			name:       "pro CROSSED: probe blocked (403) fails",
			edition:    "pro",
			statusCode: http.StatusForbidden,
			code:       "FORBIDDEN",
			wantStatus: metrics.CheckStatusFail,
		},
		{
			name:       "enterprise gate open, probe connected passes",
			edition:    "enterprise",
			statusCode: http.StatusOK,
			connected:  true,
			wantStatus: metrics.CheckStatusPass,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyFeatureGate(name, tc.edition, tc.statusCode, tc.code, tc.connected)
			if got.Name != name {
				t.Errorf("classifyFeatureGate name = %q, want %q", got.Name, name)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("classifyFeatureGate(%q, %d, %q, connected=%v) status = %q, want %q (result: %+v)",
					tc.edition, tc.statusCode, tc.code, tc.connected, got.Status, tc.wantStatus, got)
			}
		})
	}
}

// REST publish is open on all editions (RESTPublish=Community, ADR-0009) —
// the classifier must pass only on success, on every edition.
func TestClassifyOpenEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		edition    string
		statusCode int
		code       string
		connected  bool
		wantStatus string
	}{
		{name: "community open passes", edition: "community", statusCode: 200, connected: true, wantStatus: metrics.CheckStatusPass},
		{name: "community gate rejection is a regression", edition: "community", statusCode: 403, code: "EDITION_LIMIT", wantStatus: metrics.CheckStatusFail},
		{name: "community kafka not-routable passes (routing is Pro, not a gate)", edition: "community", statusCode: 409, code: "PUBLISH_NOT_ROUTABLE", wantStatus: metrics.CheckStatusPass},
		{name: "community other 409 fails", edition: "community", statusCode: 409, code: "CONFLICT", wantStatus: metrics.CheckStatusFail},
		{name: "pro not-routable fails (paid editions can provision rules)", edition: "pro", statusCode: 409, code: "PUBLISH_NOT_ROUTABLE", wantStatus: metrics.CheckStatusFail},
		{name: "pro open passes", edition: "pro", statusCode: 200, connected: true, wantStatus: metrics.CheckStatusPass},
		{name: "enterprise blocked fails", edition: "enterprise", statusCode: 403, code: "FORBIDDEN", wantStatus: metrics.CheckStatusFail},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyOpenEndpoint("rest publish open", tc.edition, tc.statusCode, tc.code, tc.connected)
			if got.Status != tc.wantStatus {
				t.Errorf("classifyOpenEndpoint(%q, %d, %q, connected=%v) status = %q, want %q",
					tc.edition, tc.statusCode, tc.code, tc.connected, got.Status, tc.wantStatus)
			}
		})
	}
}
