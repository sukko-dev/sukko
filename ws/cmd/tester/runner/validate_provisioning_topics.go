package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// Topic check names — single source of truth, used BOTH at the emit sites below and by the
// e2e.yml drift guard (TestTopicsCheckNamesInE2E), so a literal typo cannot diverge the emitted
// name from the gated name (mirrors the rename suite's constant discipline).
const (
	checkTopicsCreateList     = "topics create and list"
	checkTopicsReserved       = "topics reserved suffix"
	checkTopicsInvalid        = "topics invalid suffix"
	checkTopicsDuplicate      = "topics duplicate"
	checkTopicsDeleteNotFound = "topics delete not-found"
	checkTopicsReferenced     = "topics referenced by rule"
	checkTopicsRoleGateRead   = "topics role gate read"
	checkTopicsRoleGateWrite  = "topics role gate write"
	checkTopicsPagination     = "topics pagination"

	// TopicsQuotaCheckName is Pro-gated (PATCH /quotas) — REQUIRE_PASS on the Pro cells,
	// ALLOWED_SKIPS on community-direct where the probe records an edition skip.
	TopicsQuotaCheckName = "topics quota wall"
)

// TopicsAlwaysPassCheckNames are the ungated topics checks that must be present-and-pass in
// every provisioning grid cell. Frozen for the e2e.yml drift guard (TestTopicsCheckNamesInE2E).
var TopicsAlwaysPassCheckNames = []string{
	checkTopicsCreateList,
	checkTopicsReserved,
	checkTopicsInvalid,
	checkTopicsDuplicate,
	checkTopicsDeleteNotFound,
	checkTopicsReferenced,
	checkTopicsRoleGateRead,
	checkTopicsRoleGateWrite,
	checkTopicsPagination,
}

// topicsPage is the parsed shape of a provisioned-topics list response.
type topicsPage struct {
	Items []struct {
		Suffix string `json:"suffix"`
	} `json:"items"`
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

func parseTopicsPage(body []byte) topicsPage {
	var p topicsPage
	_ = json.Unmarshal(body, &p) // malformed body → zero values → the caller's assertion fails loudly
	return p
}

// quotaMaxTopics is the parsed max_topics field of a quota response.
type quotaMaxTopics struct {
	MaxTopics int `json:"max_topics"`
}

// validateProvisioningTopics runs the provisioned-topics API coverage (ADR-0006 Phase 2):
// create/list/delete happy paths plus every negative — reserved suffix, invalid suffix,
// duplicate, not-found, referenced-by-rule, the write role gate — and the per-tenant quota
// wall. Every negative asserts HTTP status AND the exact error code, and pairs a near-identical
// allowed (2xx) case differing only in the tested dimension ([[one-sided-green]]). Topics are
// durable rows (unlike routing rules, which are replaced), so every check deletes what it
// created; list/pagination assertions are relative to a freshly-read baseline, never absolute.
//
// Invoked from validateProvisioning after the routing block; runs in every grid cell that runs
// the `provisioning` suite. Topics are ungated (no RequireFeature), so the create/list/delete
// checks pass on all editions; only the quota probe is edition-tolerant (PATCH /quotas is Pro),
// handled by the shared provRoutingCheck wrapper (403 EDITION_LIMIT → skip, never pass).
//
//nolint:gocritic // appendCombine: checks are interleaved with cleanup/branches; sequential appends cannot be combined.
func validateProvisioningTopics(
	ctx context.Context,
	setupA *auth.SetupResult,
	logger zerolog.Logger,
) []metrics.CheckResult {
	provClient := setupA.ProvClient
	tenantA := setupA.TenantID
	var checks []metrics.CheckResult

	// --- create + list includes default first ---
	checks = append(checks, provRoutingCheck(checkTopicsCreateList, func() error {
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-analytics"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("create e2e-analytics: expected 201, got status=%d err=%w", status, err)
		}
		defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantA, "e2e-analytics") }()
		body, err := provClient.ListTopics(ctx, tenantA)
		if err != nil {
			return fmt.Errorf("list topics: %w", err)
		}
		page := parseTopicsPage(body)
		if len(page.Items) == 0 || page.Items[0].Suffix != routing.DefaultTopicSuffix {
			return fmt.Errorf("list: expected %q first, got items=%v (body=%s)", routing.DefaultTopicSuffix, page.Items, string(body))
		}
		if !topicsContain(page, "e2e-analytics") {
			return fmt.Errorf("list: created topic %q missing (body=%s)", "e2e-analytics", string(body))
		}
		return nil
	}))

	// --- reserved suffix (default / dead-letter → 400) ---
	checks = append(checks, provRoutingCheck(checkTopicsReserved, func() error {
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-reserved-ok"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("reserved allowed case: expected 201, got status=%d err=%w", status, err)
		}
		defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantA, "e2e-reserved-ok") }()
		for _, reserved := range []string{routing.DefaultTopicSuffix, routing.DeadLetterTopicSuffix} {
			status, err := provClient.CreateTopicRaw(ctx, tenantA, reserved)
			if e := expectReject("reserved "+reserved, status, err, http.StatusBadRequest, errCodeReservedTopicSuffix); e != nil {
				return e
			}
		}
		return nil
	}))

	// --- invalid suffix (empty / uppercase → 400) ---
	checks = append(checks, provRoutingCheck(checkTopicsInvalid, func() error {
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-valid-x"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("invalid allowed case: expected 201, got status=%d err=%w", status, err)
		}
		defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantA, "e2e-valid-x") }()
		for _, bad := range []string{"", "E2E-Upper"} {
			status, err := provClient.CreateTopicRaw(ctx, tenantA, bad)
			if e := expectReject(fmt.Sprintf("invalid %q", bad), status, err, http.StatusBadRequest, errCodeInvalidTopicSuffix); e != nil {
				return e
			}
		}
		return nil
	}))

	// --- duplicate (409) ---
	checks = append(checks, provRoutingCheck(checkTopicsDuplicate, func() error {
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-dup"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("dup first create: expected 201, got status=%d err=%w", status, err)
		}
		defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantA, "e2e-dup") }()
		status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-dup")
		return expectReject("duplicate create", status, err, http.StatusConflict, errCodeTopicAlreadyExists)
	}))

	// --- delete not-found (404); allowed: delete an existing topic → 200 ---
	checks = append(checks, provRoutingCheck(checkTopicsDeleteNotFound, func() error {
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-del"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("delete allowed setup: expected 201, got status=%d err=%w", status, err)
		}
		if status, err := provClient.DeleteTopicRaw(ctx, tenantA, "e2e-del"); err != nil || status != http.StatusOK {
			return fmt.Errorf("delete allowed: expected 200, got status=%d err=%w", status, err)
		}
		status, err := provClient.DeleteTopicRaw(ctx, tenantA, "e2e-ghost")
		return expectReject("delete missing", status, err, http.StatusNotFound, errCodeTopicNotFound)
	}))

	// --- referenced-by-rule (409): a topic an ingress rule references cannot be deleted ---
	// Cleanup runs INLINE (rule reset THEN topic delete — reversed order 409s) so a cleanup
	// failure fails the check; the defer is a logged backstop for the early-error paths only.
	checks = append(checks, provRoutingCheck(checkTopicsReferenced, func() error {
		cleaned := false
		defer func() {
			if cleaned {
				return
			}
			if err := resetRoutingRulesToFixture(ctx, provClient, tenantA); err != nil {
				logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenantA).Msg("topics referenced-by-rule cleanup: reset routing rules failed")
			}
			if status, err := provClient.DeleteTopicRaw(ctx, tenantA, "e2e-ref"); err != nil && status != http.StatusNotFound {
				logger.Warn().Err(err).Int("status", status).Str(logging.LogKeyTenantSlug, tenantA).Msg("topics referenced-by-rule cleanup: delete topic failed")
			}
		}()
		if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-ref"); err != nil || status != http.StatusCreated {
			return fmt.Errorf("ref topic create: expected 201, got status=%d err=%w", status, err)
		}
		if status, err := provClient.AddRoutingRuleRaw(ctx, tenantA, routingRule("e2e-ref.**", "e2e-ref", 300)); err != nil || status != http.StatusCreated {
			return fmt.Errorf("ref rule add: expected 201, got status=%d err=%w", status, err)
		}
		status, err := provClient.DeleteTopicRaw(ctx, tenantA, "e2e-ref")
		if e := expectReject("delete referenced topic", status, err, http.StatusConflict, errCodeTopicReferencedByRule); e != nil {
			return e
		}
		// Inline cleanup — a failure here fails the check (not a silent leak): remove the rule
		// first, then the now-unreferenced topic.
		if err := resetRoutingRulesToFixture(ctx, provClient, tenantA); err != nil {
			return fmt.Errorf("cleanup: reset routing rules: %w", err)
		}
		if err := deleteTopicOK(ctx, provClient, tenantA, "e2e-ref"); err != nil {
			return err
		}
		cleaned = true
		return nil
	}))

	// --- write role gate: user GET allowed (200), user create rejected (403) ---
	userToken, mintErr := setupA.Minter.MintWithClaims(auth.MintOptions{
		Subject: "topics-user-" + uuid.NewString()[:8],
		Roles:   []string{"user"},
	})
	if mintErr != nil {
		mintFail := fmt.Sprintf("mint user token: %v", mintErr)
		checks = append(checks,
			metrics.CheckResult{Name: checkTopicsRoleGateRead, Status: metrics.CheckStatusFail, Error: mintFail},
			metrics.CheckResult{Name: checkTopicsRoleGateWrite, Status: metrics.CheckStatusFail, Error: mintFail})
	} else {
		checks = append(checks, provRoutingCheck(checkTopicsRoleGateRead, func() error {
			status, err := provClient.ListTopicsWithToken(ctx, tenantA, userToken)
			return expectStatus("role gate read", status, err, http.StatusOK)
		}))
		checks = append(checks, provRoutingCheck(checkTopicsRoleGateWrite, func() error {
			if status, err := provClient.CreateTopicRaw(ctx, tenantA, "e2e-rg-admin"); err != nil || status != http.StatusCreated {
				return fmt.Errorf("role gate write (admin): expected 201, got status=%d err=%w", status, err)
			}
			defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantA, "e2e-rg-admin") }()
			status, err := provClient.CreateTopicRawWithToken(ctx, tenantA, userToken, "e2e-rg-user")
			return expectReject("role gate write (user)", status, err, http.StatusForbidden, errCodeInsufficientRole)
		}))
	}

	// --- pagination + limit cap (relative to a freshly-read baseline) ---
	checks = append(checks, provRoutingCheck(checkTopicsPagination, func() error {
		return checkTopicsPaginationFn(ctx, provClient, tenantA)
	}))

	// --- quota wall: the first real-stack proof of the slice-2a quota-row fix ---
	// max_topics binds at topic creation (not just the config default). Pro-gated PATCH /quotas
	// sets the wall relative to the live count; Community 403s EDITION_LIMIT → provRoutingCheck skips.
	checks = append(checks, provRoutingCheck(TopicsQuotaCheckName, func() error {
		return checkTopicsQuotaWall(ctx, provClient, tenantA)
	}))

	return checks
}

// topicsContain reports whether a suffix is present in a parsed topics page.
func topicsContain(p topicsPage, suffix string) bool {
	for _, it := range p.Items {
		if it.Suffix == suffix {
			return true
		}
	}
	return false
}

// deleteTopicOK deletes a topic and returns an error unless the status is 200.
func deleteTopicOK(ctx context.Context, provClient *auth.ProvisioningClient, tenantID, suffix string) error {
	if status, err := provClient.DeleteTopicRaw(ctx, tenantID, suffix); err != nil || status != http.StatusOK {
		return fmt.Errorf("cleanup delete %q: expected 200, got status=%d err=%w", suffix, status, err)
	}
	return nil
}

// nonDefaultTopicCount returns the number of provisioned topics excluding the synthetic default.
func nonDefaultTopicCount(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string) (int, error) {
	body, err := provClient.ListTopicsPage(ctx, tenantID, 200)
	if err != nil {
		return 0, fmt.Errorf("list topics: %w", err)
	}
	page := parseTopicsPage(body)
	n := 0
	for _, it := range page.Items {
		if it.Suffix != routing.DefaultTopicSuffix {
			n++
		}
	}
	return n, nil
}

// checkTopicsPaginationFn seeds 3 topics and asserts ?limit=1 honors the page size while total
// reflects the full set, and that an oversized ?limit is capped to the handler's max (200).
func checkTopicsPaginationFn(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string) error {
	base, err := provClient.ListTopics(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("baseline list: %w", err)
	}
	total0 := parseTopicsPage(base).Total

	seeded := []string{"e2e-page-a", "e2e-page-b", "e2e-page-c"}
	for _, s := range seeded {
		if status, err := provClient.CreateTopicRaw(ctx, tenantID, s); err != nil || status != http.StatusCreated {
			return fmt.Errorf("seed %q: expected 201, got status=%d err=%w", s, status, err)
		}
	}
	defer func() {
		for _, s := range seeded {
			_, _ = provClient.DeleteTopicRaw(ctx, tenantID, s)
		}
	}()

	pageBody, err := provClient.ListTopicsPage(ctx, tenantID, 1)
	if err != nil {
		return fmt.Errorf("get ?limit=1: %w", err)
	}
	page := parseTopicsPage(pageBody)
	if len(page.Items) != 1 {
		return fmt.Errorf("?limit=1: expected 1 item, got %d (body=%s)", len(page.Items), string(pageBody))
	}
	if page.Total != total0+len(seeded) {
		return fmt.Errorf("?limit=1: expected total=%d, got %d (body=%s)", total0+len(seeded), page.Total, string(pageBody))
	}

	capBody, err := provClient.ListTopicsPage(ctx, tenantID, routingRulesMaxPageLimit+300)
	if err != nil {
		return fmt.Errorf("get oversized ?limit: %w", err)
	}
	if capped := parseTopicsPage(capBody).Limit; capped != routingRulesMaxPageLimit {
		return fmt.Errorf("expected capped limit=%d, got %d (body=%s)", routingRulesMaxPageLimit, capped, string(capBody))
	}
	return nil
}

// checkTopicsQuotaWall proves the per-tenant max_topics quota row binds at creation. It reads
// the live count, sets max_topics one above it, confirms one more create succeeds and the next
// is rejected 400 TOO_MANY_TOPICS, then restores the count and the original quota — even on a
// mid-probe failure, so later same-run creates never false-fail. Community's PATCH /quotas is
// Pro-gated: the returned 403 EDITION_LIMIT propagates so provRoutingCheck records a skip.
func checkTopicsQuotaWall(ctx context.Context, provClient *auth.ProvisioningClient, tenantID string) error {
	qStatus, qBody, qErr := provClient.GetQuotaRaw(ctx, tenantID)
	if qErr != nil {
		// 403 EDITION_LIMIT on Community → propagate for the edition-tolerant skip
		// (wrapping preserves the embedded body so extractErrorCode still sees the code).
		return fmt.Errorf("get quota: %w", qErr)
	}
	if qStatus != http.StatusOK {
		return fmt.Errorf("get quota: expected 200, got %d (body=%s)", qStatus, string(qBody))
	}
	var q quotaMaxTopics
	if err := json.Unmarshal(qBody, &q); err != nil {
		return fmt.Errorf("parse quota: %w (body=%s)", err, string(qBody))
	}

	count, err := nonDefaultTopicCount(ctx, provClient, tenantID)
	if err != nil {
		return fmt.Errorf("count topics: %w", err)
	}

	// Restore the original quota no matter what.
	defer func() { _, _ = provClient.UpdateQuotaRaw(ctx, tenantID, map[string]any{"max_topics": q.MaxTopics}) }()

	if status, err := provClient.UpdateQuotaRaw(ctx, tenantID, map[string]any{"max_topics": count + 1}); err != nil || status != http.StatusOK {
		return fmt.Errorf("set quota to %d: expected 200, got status=%d err=%w", count+1, status, err)
	}

	// One more create fills the quota exactly (allowed).
	if status, err := provClient.CreateTopicRaw(ctx, tenantID, "e2e-quota-fill"); err != nil || status != http.StatusCreated {
		return fmt.Errorf("quota fill create: expected 201, got status=%d err=%w", status, err)
	}
	defer func() { _, _ = provClient.DeleteTopicRaw(ctx, tenantID, "e2e-quota-fill") }()

	// The next create exceeds the quota row → 400 TOO_MANY_TOPICS.
	status, err := provClient.CreateTopicRaw(ctx, tenantID, "e2e-quota-over")
	if err == nil && status == http.StatusCreated {
		_, _ = provClient.DeleteTopicRaw(ctx, tenantID, "e2e-quota-over") // unexpected success — clean it up
	}
	return expectReject("quota exceeded", status, err, http.StatusBadRequest, errCodeTooManyTopics)
}
