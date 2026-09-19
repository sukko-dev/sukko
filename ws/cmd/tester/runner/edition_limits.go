package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	testersse "github.com/sukko-dev/sukko/cmd/tester/sse"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// editionTestTenantPrefix is the recognizable prefix for test tenants. It MUST be a
// valid tenant slug prefix — slugs match ^[a-z][a-z0-9-]{2,62}$ (lowercase, start with a
// letter, hyphens only). An underscore-based prefix produces an invalid slug that provisioning
// rejects with HTTP 400 SLUG_INVALID (ValidateSlug wraps ErrSlugInvalid → 400).
const editionTestTenantPrefix = "edition-test-"

// editionHTTPTimeout is the HTTP client timeout for /edition and provisioning API calls.
const editionHTTPTimeout = 10 * time.Second

// Provisioning API error codes, mirrored here because they are unexported upstream. The tester
// matches them via extractErrorCode on the exact `code` field (never strings.Contains —
// EDITION_LIMIT is a substring of the EDITION_LIMIT_* limit codes) to discriminate the two
// DISTINCT routing-rules rejections:
//   - the feature gate (403 EDITION_LIMIT, from the RequireFeature middleware) — seen on Community;
//   - the per-tenant count boundary (400 TOO_MANY_ROUTING_RULES) — seen on paid editions. This
//     comes from the ValidateRoutingRules count check in the ReplaceRoutingRules HANDLER
//     (handlers.go), which runs BEFORE the handler calls the service; the service also has an
//     edition-manager check (403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT), but it is unreachable
//     while the config MAX_ROUTING_RULES_PER_TENANT (default 100) is ≤ the edition limit (Pro 100),
//     so the config validator is the effective boundary and 400 is what the paid cell observes.
const (
	errCodeEditionLimit        = "EDITION_LIMIT"          // feature gate (403)
	errCodeTooManyRoutingRules = "TOO_MANY_ROUTING_RULES" // routing-rule count boundary (400, handler config validator)
	// routing-rule count boundary (403, service edition check) — reachable only when the
	// edition limit is BELOW the config limit, i.e. Community (10 < 100).
	errCodeEditionLimitRoutingRules = "EDITION_LIMIT_ROUTING_RULES_PER_TENANT"

	// Additional routing-rules and auth error codes asserted by the provisioning
	// routing-rules coverage suite (validate_provisioning_routing.go). Mirrored from
	// the handler's service-private constants (internal/provisioning/api/handlers.go),
	// matched exactly via extractErrorCode on the `code` field.
	errCodeTopicNotProvisioned          = "TOPIC_NOT_PROVISIONED"           // routing write referencing an unprovisioned topic suffix (400)
	errCodeInsufficientRole             = "INSUFFICIENT_ROLE"               // RequireRole rejection on a user-role write (403)
	errCodeTenantMismatch               = "TENANT_MISMATCH"                 // RequireTenant cross-tenant rejection (403)
	errCodeRoutingRuleDuplicatePattern  = "ROUTING_RULE_DUPLICATE_PATTERN"  // POST dup pattern (409); PUT internal dup (400)
	errCodeRoutingRuleDuplicatePriority = "ROUTING_RULE_DUPLICATE_PRIORITY" // POST dup priority (409)
	errCodeRoutingRuleValidation        = "ROUTING_RULE_VALIDATION_ERROR"   // empty topics / too-many topics / bad pattern (400)

	// Rename error codes asserted by the provisioning rename coverage suite
	// (validate_provisioning_rename.go). Mirrored from the handler's service-private constants
	// (internal/provisioning/api/handlers.go), matched exactly via extractErrorCode on the `code`
	// field. errCodeInsufficientRole above is reused for the rename role gate (no duplicate).
	errCodeInvalidRequest       = "INVALID_REQUEST"         // malformed JSON body (400)
	errCodeSlugInvalid          = "SLUG_INVALID"            // slug fails ValidateSlug (400)
	errCodeSlugReserved         = "SLUG_RESERVED"           // slug is in the reserved set (400)
	errCodeSlugAlreadyTaken     = "SLUG_ALREADY_TAKEN"      // rename to an existing tenant's slug (409)
	errCodeSlugRenameInProgress = "SLUG_RENAME_IN_PROGRESS" // rename within the topic hold period (409)
)

// ConnLimitRejectionCheckName is the check name emitted by the connection-boundary
// sub-check. When the license headroom exceeds maxTestConnections the sub-check honestly
// reports a skip under this name; the e2e skip allow-list (taskfiles/e2e.yml) declares it
// as edition-limits:<this value>. Exported and asserted by a bash grep + a Go test so the
// cross-file coupling has a binding it cannot silently drift from.
const ConnLimitRejectionCheckName = "connection limit rejection"

// editionInfo holds parsed data from provisioning and gateway /edition endpoints.
type editionInfo struct {
	Edition string
	Limits  struct {
		MaxTenants               int `json:"max_tenants"`
		MaxTotalConnections      int `json:"max_total_connections"`
		MaxShards                int `json:"max_shards"`
		MaxTopicsPerTenant       int `json:"max_topics_per_tenant"`
		MaxRoutingRulesPerTenant int `json:"max_routing_rules_per_tenant"`
	}
	Usage struct {
		Tenants     *int `json:"tenants"`
		Connections *int `json:"connections"`
		Shards      *int `json:"shards"`
	}
}

// editionResponse mirrors the JSON from GET /edition.
type editionResponse struct {
	Edition string `json:"edition"`
	Limits  struct {
		MaxTenants               int `json:"max_tenants"`
		MaxTotalConnections      int `json:"max_total_connections"`
		MaxShards                int `json:"max_shards"`
		MaxTopicsPerTenant       int `json:"max_topics_per_tenant"`
		MaxRoutingRulesPerTenant int `json:"max_routing_rules_per_tenant"`
	} `json:"limits"`
	Usage *struct {
		Tenants     *int `json:"tenants"`
		Connections *int `json:"connections"`
		Shards      *int `json:"shards"`
	} `json:"usage"`
}

// validateEditionLimits runs the edition-limits boundary test suite.
func validateEditionLimits(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	provURL := run.Config.ProvisioningURL
	gwURL := run.Config.GatewayURL
	provClient := run.authResult.ProvClient // authenticated via admin keypair JWT

	// Fetch edition info from provisioning (tenants, limits)
	provInfo, err := fetchEditionFrom(ctx, provURL)
	if err != nil {
		return nil, fmt.Errorf("fetch provisioning /edition: %w", err)
	}

	// Merge: start with provisioning data
	info := &editionInfo{}
	info.Edition = provInfo.Edition
	info.Limits = provInfo.Limits
	if provInfo.Usage != nil {
		info.Usage.Tenants = provInfo.Usage.Tenants
	}

	// Fetch gateway /edition for connections + shards (best-effort)
	gwInfo, err := fetchEditionFrom(ctx, httpURL(gwURL))
	if err != nil {
		logger.Debug().Err(err).Msg("Gateway /edition unreachable — connection/shard data unavailable")
	} else if gwInfo.Usage != nil {
		info.Usage.Connections = gwInfo.Usage.Connections
		info.Usage.Shards = gwInfo.Usage.Shards
	}

	// Feature gate checks (run on all editions including Enterprise)
	checks := make([]metrics.CheckResult, 0, 12) //nolint:mnd // pre-allocate for ~12 checks across 4 dimensions + 2 feature gates

	// Provision the suite tenant's routing + channel rules so the probes can reach a
	// tenant-qualified channel (200). Reuses the shared setup, which is fail-closed on every
	// edition now that routing rules are ungated (ADR-0014) — §X, no bespoke setup.
	probeTenant := run.authResult.TenantID
	if err := setupSuiteRoutingRules(ctx, provClient, probeTenant, logger); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "feature gate setup", Status: metrics.CheckStatusFail, Error: err.Error(),
		})
	}
	_ = provClient.SetChannelRules(ctx, probeTenant, testChannelRules)

	// Feature gate: SSE Transport (Pro+ only)
	sseCheck := checkSSEFeatureGate(ctx, gwURL, run.authResult.TokenFunc(0), probeTenant, info.Edition, logger)
	checks = append(checks, sseCheck)

	// REST Publish is Community (license.RESTPublish, ADR-0009) — open on
	// every edition; the probe must succeed everywhere.
	restCheck := checkRESTPublishOpen(ctx, gwURL, run.authResult.TokenFunc(0), probeTenant, info.Edition)
	checks = append(checks, restCheck)

	// If all limits are 0 (Enterprise/unlimited), skip numeric boundary tests
	if isUnlimited(info) {
		checks = append(checks, metrics.CheckResult{
			Name: "edition", Status: "pass", Latency: "unlimited — no boundaries to test",
		})
		return checks, nil
	}

	// Create shared test tenant (used by routing rules check)
	sharedTenantID := editionTestTenantPrefix + uuid.New().String()[:8]
	if err := provClient.CreateTenant(ctx, sharedTenantID, "Edition boundary test tenant"); err != nil {
		return nil, fmt.Errorf("create shared test tenant: %w", err)
	}
	defer func() { //nolint:contextcheck // intentional: use background context so cleanup survives parent cancellation
		cleanupCtx, cancel := context.WithTimeout(context.Background(), editionHTTPTimeout)
		defer cancel()
		if delErr := provClient.DeleteTenant(cleanupCtx, sharedTenantID); delErr != nil {
			logger.Warn().Err(delErr).Str(logging.LogKeyTenantSlug, sharedTenantID).Msg("Failed to clean up shared test tenant")
		}
	}()

	// Tenant limit boundary
	tenantChecks := checkTenantLimit(ctx, provClient, info, logger)
	checks = append(checks, tenantChecks...)

	// Routing rules limit boundary (uses shared tenant)
	rulesChecks := checkRoutingRulesLimit(ctx, provClient, sharedTenantID, info, logger)
	checks = append(checks, rulesChecks...)

	// Connection limit boundary
	connChecks := checkConnectionLimit(ctx, gwURL, run.authResult.TokenFunc, info, logger)
	checks = append(checks, connChecks...)

	// Shard count verification (read-only)
	shardChecks := checkShardLimit(info)
	checks = append(checks, shardChecks...)

	return checks, nil
}

// --- Boundary checks ---

func checkTenantLimit(ctx context.Context, provClient *auth.ProvisioningClient, info *editionInfo, logger zerolog.Logger) []metrics.CheckResult {
	maxTenants := info.Limits.MaxTenants
	if maxTenants == 0 {
		return []metrics.CheckResult{{Name: "tenant limit", Status: "pass", Latency: "unlimited"}}
	}

	currentTenants := 0
	if info.Usage.Tenants != nil {
		currentTenants = *info.Usage.Tenants
	}

	// Account for the shared tenant already created by validateEditionLimits
	headroom := max(maxTenants-currentTenants-1, 0)

	var created []string
	defer func() { //nolint:contextcheck // intentional: use background context so cleanup survives parent cancellation
		cleanupCtx, cancel := context.WithTimeout(context.Background(), editionHTTPTimeout)
		defer cancel()
		for _, id := range created {
			if err := provClient.DeleteTenant(cleanupCtx, id); err != nil {
				logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, id).Msg("Failed to clean up test tenant")
			}
		}
	}()

	var checks []metrics.CheckResult

	// Fill headroom
	if headroom > 0 {
		allCreated := true
		for range headroom {
			tenantID := editionTestTenantPrefix + uuid.New().String()[:8]
			if err := provClient.CreateTenant(ctx, tenantID, "Edition boundary test tenant"); err != nil {
				allCreated = false
				checks = append(checks, metrics.CheckResult{
					Name: "tenant creation within limit", Status: "fail",
					Error: fmt.Sprintf("failed to create tenant %d/%d: %v", len(created)+1, headroom, err),
				})
				break
			}
			created = append(created, tenantID)
		}
		if allCreated {
			checks = append(checks, metrics.CheckResult{
				Name: "tenant creation within limit", Status: "pass",
				Latency: fmt.Sprintf("created %d test tenants", headroom),
			})
		}
	}

	// Attempt one more — expect rejection
	rejectID := editionTestTenantPrefix + "reject-" + uuid.New().String()[:8]
	err := provClient.CreateTenant(ctx, rejectID, "Edition rejection test")
	if err != nil {
		if strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), errCodeEditionLimit) {
			checks = append(checks, metrics.CheckResult{
				Name: "tenant limit rejection", Status: "pass",
				Latency: fmt.Sprintf("correctly rejected at %d/%d", maxTenants, maxTenants),
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "tenant limit rejection", Status: "fail",
				Error: fmt.Sprintf("expected 403/EDITION_LIMIT, got: %v", err),
			})
		}
	} else {
		// Unexpected success — clean up
		created = append(created, rejectID)
		checks = append(checks, metrics.CheckResult{
			Name: "tenant limit rejection", Status: "fail",
			Error: fmt.Sprintf("tenant created beyond limit (%d), expected rejection", maxTenants),
		})
	}

	return checks
}

func checkRoutingRulesLimit(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string, info *editionInfo, logger zerolog.Logger) []metrics.CheckResult {
	maxRules := info.Limits.MaxRoutingRulesPerTenant
	if maxRules == 0 {
		return []metrics.CheckResult{{Name: "routing rules limit", Status: metrics.CheckStatusPass, Latency: "unlimited"}}
	}

	defer func() {
		// Clean up rules on the shared tenant; not-found is fine (the run may have left none).
		if err := provClient.DeleteRoutingRules(ctx, tenantID); err != nil {
			logger.Debug().Err(err).Str(logging.LogKeyTenantSlug, tenantID).
				Msg("Routing rules cleanup (not-found is expected)")
		}
	}()

	// Routing rules are ungated on every edition (ADR-0014), so the count boundary is real
	// everywhere: maxRules accepted (200), maxRules+1 rejected. WHICH rejection fires depends on
	// how the edition limit compares to the config limit (MAX_ROUTING_RULES_PER_TENANT, default
	// 100), because the handler's config validator runs before the service-level edition check —
	// see classifyRoutingRules.
	var checks []metrics.CheckResult

	status, err := setTestRoutingRulesViaClient(ctx, provClient, tenantID, maxRules)
	if err == nil && status == http.StatusOK {
		checks = append(checks, metrics.CheckResult{
			Name: "routing rules within limit", Status: metrics.CheckStatusPass,
			Latency: fmt.Sprintf("set %d rules", maxRules),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "routing rules within limit", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 200 for %d rules, got status=%d err=%v", maxRules, status, err),
		})
	}

	status, err = setTestRoutingRulesViaClient(ctx, provClient, tenantID, maxRules+1)
	checks = append(checks, classifyRoutingRules(info.Edition, status, errText(err)))

	return checks
}

// classifyRoutingRules classifies an over-count routing-rules PUT for the given edition.
// Routing rules are ungated everywhere (ADR-0014), so the assertion is always a COUNT boundary —
// but the two rejections are DISTINCT and edition-dependent, because the ReplaceRoutingRules
// handler's config count validator (MAX_ROUTING_RULES_PER_TENANT, envDefault 100) runs BEFORE the
// service-level edition check:
//   - Community (edition limit 10 < config 100): the config validator passes an 11-rule request,
//     so the service edition check rejects — 403 EDITION_LIMIT_ROUTING_RULES_PER_TENANT.
//   - Paid (Pro edition limit 100 == config 100): the config validator rejects first —
//     400 TOO_MANY_ROUTING_RULES, leaving the edition check unreachable.
//
// Matching is on the EXACT `code` (extractErrorCode, never strings.Contains — EDITION_LIMIT is a
// PREFIX of EDITION_LIMIT_ROUTING_RULES_PER_TENANT). The status+code split keeps the other
// edition's rejection, a regressed feature gate (403 EDITION_LIMIT), and a setup failure
// (400 TOPIC_NOT_PROVISIONED) from passing as this edition's boundary. errBody carries the response
// body embedded by SetRoutingRulesRaw's readError, which contains the `code` field.
func classifyRoutingRules(edition string, statusCode int, errBody string) metrics.CheckResult {
	code := extractErrorCode(errBody)

	wantStatus, wantCode := http.StatusBadRequest, errCodeTooManyRoutingRules
	if edition == string(license.Community) {
		wantStatus, wantCode = http.StatusForbidden, errCodeEditionLimitRoutingRules
	}

	if statusCode == wantStatus && code == wantCode {
		return metrics.CheckResult{
			Name: "routing rules limit rejection", Status: metrics.CheckStatusPass,
			Latency: fmt.Sprintf("%s: count limit correctly enforced (%d %s)", edition, wantStatus, wantCode),
		}
	}
	return metrics.CheckResult{
		Name: "routing rules limit rejection", Status: metrics.CheckStatusFail,
		Error: fmt.Sprintf("%s: expected %d/%s (count limit), got status=%d code=%q", edition, wantStatus, wantCode, statusCode, code),
	}
}

// errText returns the error's message, or "" for nil — feeds the routing-rules classifier
// the response body that SetRoutingRulesRaw embeds in its error.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// extractErrorCode returns the `code` field from a provisioning/gateway error body. The body
// is embedded in a wrapper like `set routing rules: HTTP 403: {"code":"EDITION_LIMIT",...}`, so
// we scan from the first `{` and unmarshal. Returns "" when there is no JSON object or no code.
// Exact-field extraction (vs strings.Contains) is required because EDITION_LIMIT is a substring
// of EDITION_LIMIT_ROUTING_RULES_PER_TENANT — a substring match cannot tell the feature gate
// from the routing-rule count boundary.
func extractErrorCode(errBody string) string {
	start := strings.IndexByte(errBody, '{')
	if start < 0 {
		return ""
	}
	var parsed struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal([]byte(errBody[start:]), &parsed); err != nil {
		return ""
	}
	return parsed.Code
}

// classifyFeatureGate classifies an SSE/REST-publish feature-gate probe for the given edition,
// discriminating on HTTP status + exact error code (never status alone). On Community the SSE
// and REST-publish routes share the RequireFeature(SSETransport) gate, which returns 403
// EDITION_LIMIT before any channel validation — so the rejection MUST carry that exact code; a
// setup rejection (403 FORBIDDEN from channel-tenant mismatch) MUST NOT pass. On paid editions
// the gate is open, so a provisioned probe MUST connect/publish successfully (connected).
func classifyFeatureGate(name, edition string, statusCode int, code string, connected bool) metrics.CheckResult {
	if edition == string(license.Community) {
		if statusCode == http.StatusForbidden && code == errCodeEditionLimit {
			return metrics.CheckResult{
				Name: name, Status: metrics.CheckStatusPass,
				Latency: "community: correctly feature-gated (403 " + errCodeEditionLimit + ")",
			}
		}
		if connected {
			return metrics.CheckResult{
				Name: name, Status: metrics.CheckStatusFail,
				Error: "community: expected 403/" + errCodeEditionLimit + " (feature gate), but probe succeeded (gate regressed)",
			}
		}
		return metrics.CheckResult{
			Name: name, Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("community: expected 403/%s (feature gate), got status=%d code=%q", errCodeEditionLimit, statusCode, code),
		}
	}

	// Paid (Pro/Enterprise) — gate is open, probe must succeed.
	if connected {
		return metrics.CheckResult{
			Name: name, Status: metrics.CheckStatusPass,
			Latency: edition + ": accessible (200)",
		}
	}
	return metrics.CheckResult{
		Name: name, Status: metrics.CheckStatusFail,
		Error: fmt.Sprintf("%s: expected 200 (gate open), got status=%d code=%q", edition, statusCode, code),
	}
}

const maxTestConnections = 100 // cap to avoid resource exhaustion

func checkConnectionLimit(ctx context.Context, gwURL string, tokenFunc func(int) string, info *editionInfo, logger zerolog.Logger) []metrics.CheckResult {
	maxConns := info.Limits.MaxTotalConnections
	if maxConns == 0 {
		return []metrics.CheckResult{{Name: "connection limit", Status: "pass", Latency: "unlimited"}}
	}

	currentConns := 0
	if info.Usage.Connections != nil {
		currentConns = *info.Usage.Connections
	}

	headroom := max(min(maxConns-currentConns, maxTestConnections), 0)

	pool := testerws.NewPool(logger)
	defer pool.Drain()

	var checks []metrics.CheckResult

	if headroom > 0 {
		err := pool.RampUp(ctx, testerws.PoolConfig{
			GatewayURL: gwURL,
			TokenFunc:  tokenFunc,
		}, headroom, 50) // 50 connections/sec ramp rate
		if err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "connections within limit", Status: "fail",
				Error: fmt.Sprintf("ramp-up failed at headroom %d: %v", headroom, err),
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "connections within limit", Status: "pass",
				Latency: fmt.Sprintf("opened %d connections", headroom),
			})
		}
	}

	// If headroom was capped, note partial test
	if maxConns-currentConns > maxTestConnections {
		checks = append(checks, metrics.CheckResult{
			Name: ConnLimitRejectionCheckName, Status: metrics.CheckStatusSkip,
			Latency: fmt.Sprintf("partial check — headroom %d too large (capped at %d)", maxConns-currentConns, maxTestConnections),
		})
		return checks
	}

	// Attempt one more connection — expect rejection
	extraClient, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: gwURL,
		Token:      tokenFunc(headroom),
		Logger:     logger,
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: ConnLimitRejectionCheckName, Status: metrics.CheckStatusPass,
			Latency: fmt.Sprintf("correctly rejected at %d connections", maxConns),
		})
	} else {
		_ = extraClient.Close() // clean up unexpected connection
		checks = append(checks, metrics.CheckResult{
			Name: ConnLimitRejectionCheckName, Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("connection accepted beyond limit (%d)", maxConns),
		})
	}

	return checks
}

func checkShardLimit(info *editionInfo) []metrics.CheckResult {
	maxShards := info.Limits.MaxShards
	if maxShards == 0 {
		return []metrics.CheckResult{{Name: "shard count", Status: "pass", Latency: "unlimited"}}
	}

	if info.Usage.Shards == nil {
		return []metrics.CheckResult{{
			Name: "shard count", Status: "skip",
			Latency: "shard count not available from gateway /edition",
		}}
	}

	shards := *info.Usage.Shards
	if shards <= maxShards {
		return []metrics.CheckResult{{
			Name: "shard count within limit", Status: "pass",
			Latency: fmt.Sprintf("%d/%d shards", shards, maxShards),
		}}
	}

	return []metrics.CheckResult{{
		Name: "shard count within limit", Status: "fail",
		Error: fmt.Sprintf("shard count %d exceeds limit %d", shards, maxShards),
	}}
}

// gateProbeResult is one observation of a feature-gate probe: the HTTP status, the parsed
// error `code`, and whether the probe reached a success (connected/published).
type gateProbeResult struct {
	statusCode int
	code       string
	connected  bool
}

// featureGateSettleBudget / Tick mirror waitForRoutable's window. Channel rules reach the
// gateway via the async provisioning snapshot, so a probe fired immediately after
// SetChannelRules can race it and get a transient 403 FORBIDDEN. On paid editions the success
// path is retried within this budget; on Community the gate 403 is immediate and never retried.
const (
	featureGateSettleBudget = 2 * time.Second
	featureGateSettleTick   = 200 * time.Millisecond
)

// probeForEdition runs the feature-gate probe once on Community (the gate 403 is immediate and
// propagation-independent) and with a bounded settle retry on paid editions (to absorb the async
// channel-rule propagation window before the single-shot success assertion).
func probeForEdition(ctx context.Context, edition string, probe func() gateProbeResult) gateProbeResult {
	if edition == string(license.Community) {
		return probe()
	}
	return probeUntilAllowed(ctx, probe, featureGateSettleBudget, featureGateSettleTick)
}

// probeUntilAllowed retries probe until it reports connected (success) or the settle budget
// expires, returning the last observation. Used only on paid editions to absorb the channel-rule
// propagation window. A non-success result is never masked — the last status/code is returned so
// the caller can fail loudly. budget/tick are parameters (not the package constants directly) so
// unit tests can drive the loop with a tiny window.
func probeUntilAllowed(ctx context.Context, probe func() gateProbeResult, budget, tick time.Duration) gateProbeResult {
	deadline := time.After(budget)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	last := probe()
	for !last.connected {
		select {
		case <-ctx.Done():
			return last
		case <-deadline:
			return last
		case <-ticker.C:
			last = probe()
		}
	}
	return last
}

// probeSSEGate performs one SSE connect probe against the tenant-qualified gate channel.
func probeSSEGate(ctx context.Context, gwURL, token, channel string, logger zerolog.Logger) gateProbeResult {
	client, statusCode, body, err := testersse.ConnectRaw(ctx, testersse.ConnectConfig{
		GatewayURL: httpURL(gwURL),
		Channels:   []string{channel},
		Token:      token,
		Logger:     logger,
	})
	if err == nil && client != nil {
		_ = client.Close()
		return gateProbeResult{statusCode: http.StatusOK, connected: true}
	}
	return gateProbeResult{statusCode: statusCode, code: extractErrorCode(string(body))}
}

func checkSSEFeatureGate(ctx context.Context, gwURL, token, tenantID, edition string, logger zerolog.Logger) metrics.CheckResult {
	channel := tenantChannel(tenantID, validateSSEChannel)
	probe := func() gateProbeResult { return probeSSEGate(ctx, gwURL, token, channel, logger) }
	res := probeForEdition(ctx, edition, probe)
	return classifyFeatureGate("sse feature gate", edition, res.statusCode, res.code, res.connected)
}

// probeRESTGate performs one REST publish probe (endpoint is Community/open) against the tenant-qualified gate channel.
func probeRESTGate(ctx context.Context, client *restpublish.Client, token, channel string) gateProbeResult {
	body := fmt.Appendf(nil, `{"channel":%q,"data":{}}`, channel)
	statusCode, respBody, err := client.PublishRaw(ctx, body, restpublish.AuthConfig{Token: token}, "application/json")
	if err != nil {
		return gateProbeResult{statusCode: statusCode, code: ""}
	}
	if statusCode == http.StatusOK {
		return gateProbeResult{statusCode: statusCode, connected: true}
	}
	return gateProbeResult{statusCode: statusCode, code: extractErrorCode(string(respBody))}
}

func checkRESTPublishOpen(ctx context.Context, gwURL, token, tenantID, edition string) metrics.CheckResult {
	client := restpublish.NewClient(httpURL(gwURL))
	channel := tenantChannel(tenantID, validateSSEChannel)
	probe := func() gateProbeResult { return probeRESTGate(ctx, client, token, channel) }
	// Every edition gets the settle retry — Community also depends on async
	// channel-rule propagation now that the endpoint is ungated.
	res := probeUntilAllowed(ctx, probe, featureGateSettleBudget, featureGateSettleTick)
	return classifyOpenEndpoint("rest publish open", edition, res.statusCode, res.code, res.connected)
}

// classifyOpenEndpoint classifies a probe against an endpoint that is open on
// every edition (Community included). The probe must succeed regardless of
// edition; an edition-gate rejection is a regression. One deliberate exception:
// 409 PUBLISH_NOT_ROUTABLE — on the kafka backend publish needs a routing rule, and
// the rules snapshot reaches the gateway asynchronously, so a Community cell can still
// observe the ROUTING outcome. That reaching routing resolution at all is what proves
// the endpoint carries no edition gate, which is what this check asserts.
// NOTE: routing rules are ungated since ADR-0014, so this tolerance is about setup /
// snapshot propagation, NOT an edition limit — the reason it stays Community-only is
// now weaker than it was, and tightening it to fail on every edition is a follow-up
// that needs a grid run to confirm it does not flake on propagation lag.
func classifyOpenEndpoint(name, edition string, statusCode int, code string, connected bool) metrics.CheckResult {
	if connected {
		return metrics.CheckResult{
			Name: name, Status: metrics.CheckStatusPass,
			Latency: edition + ": accessible (200)",
		}
	}
	// Community-only: the paid cells provision rules and settle, so a 409 there means
	// broken provisioning — fail loudly (one-sided-green: discriminate on the cause AND
	// the edition).
	if edition == string(license.Community) && statusCode == http.StatusConflict && code == "PUBLISH_NOT_ROUTABLE" {
		return metrics.CheckResult{
			Name: name, Status: metrics.CheckStatusPass,
			Latency: edition + ": not edition-gated (409 PUBLISH_NOT_ROUTABLE — no routing rule in effect yet)",
		}
	}
	return metrics.CheckResult{
		Name: name, Status: metrics.CheckStatusFail,
		Error: fmt.Sprintf("%s: expected 200 or 409/PUBLISH_NOT_ROUTABLE (endpoint open on all editions), got status=%d code=%q", edition, statusCode, code),
	}
}

func isUnlimited(info *editionInfo) bool {
	l := info.Limits
	return l.MaxTenants == 0 && l.MaxTotalConnections == 0 && l.MaxShards == 0 &&
		l.MaxTopicsPerTenant == 0 && l.MaxRoutingRulesPerTenant == 0
}

// --- HTTP helpers ---

func fetchEditionFrom(ctx context.Context, baseURL string) (*editionResponse, error) {
	client := &http.Client{Timeout: editionHTTPTimeout}
	url := baseURL + "/edition"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result editionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

// setTestRoutingRulesViaClient sets routing rules using the ProvisioningClient for auth,
// returning the HTTP status code for boundary limit testing (needs 200 vs 403 distinction).
func setTestRoutingRulesViaClient(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string, count int) (int, error) {
	// All rules map to the single pre-provisioned DefaultTopicSuffix (created as a CreateTenant
	// side effect). The tester cannot provision arbitrary topics — NoopKafkaAdmin + no topics
	// endpoint — so distinct per-rule topics (test0..testN) would be rejected TOPIC_NOT_PROVISIONED.
	// Distinct patterns/priorities keep ValidateRoutingRules happy; only the rule COUNT varies,
	// isolating the count boundary.
	rules := make([]map[string]any, 0, count)
	for i := range count {
		rules = append(rules, map[string]any{
			"pattern":       fmt.Sprintf("test.%d.**", i),
			"ingress_topic": routing.DefaultTopicSuffix,
			"priority":      i + 1,
		})
	}

	status, err := provClient.SetRoutingRulesRaw(ctx, tenantID, rules)
	if err != nil {
		return status, fmt.Errorf("set test routing rules: %w", err)
	}
	return status, nil
}
