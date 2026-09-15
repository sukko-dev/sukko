package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Frozen check names — emitted verbatim by the Go suite AND copied verbatim into
// taskfiles/e2e.yml REQUIRE_PASS (3 cells) + ws/docs/e2e-testing.md. A byte-for-byte mismatch of
// any single name silently never-matches → a vacuous gate (the #205 lesson), so these are
// exported constants pinned against drift by TestRenameTestAccessCheckNamesInE2E
// (validate_provisioning_rename_test.go), which asserts each appears as provisioning:<name> in
// the REQUIRE_PASS of all three provisioning cells. No `;` (the REQUIRE_PASS separator) in any name.
const (
	RenameBadJSONCheckName      = "rename bad json"
	RenameInvalidSlugCheckName  = "rename invalid slug"
	RenameReservedSlugCheckName = "rename reserved slug"
	RenameSlugTakenCheckName    = "rename slug taken"
	RenameRoleGateCheckName     = "rename role gate"
	RenameHappyPathCheckName    = "rename happy path"
	RenameHoldPeriodCheckName   = "rename hold period"
	TestAccessComputedCheckName = "test-access computed patterns"
	TestAccessNoRulesCheckName  = "test-access no rules"
	TestAccessBadJSONCheckName  = "test-access bad json"
)

// RenameTestAccessCheckNames enumerates exactly the 10 frozen check names the suite emits, in a
// single ordering-independent set. The drift guard iterates this list; REQUIRE_PASS is
// order-independent so listing order is irrelevant.
var RenameTestAccessCheckNames = []string{
	RenameBadJSONCheckName,
	RenameInvalidSlugCheckName,
	RenameReservedSlugCheckName,
	RenameSlugTakenCheckName,
	RenameRoleGateCheckName,
	RenameHappyPathCheckName,
	RenameHoldPeriodCheckName,
	TestAccessComputedCheckName,
	TestAccessNoRulesCheckName,
	TestAccessBadJSONCheckName,
}

// renameInvalidSlug is a digit-leading input that fails ValidateSlug (must start with a letter,
// types.go:161) → 400 SLUG_INVALID. Distinct failure mode from reserved/taken.
const renameInvalidSlug = "1abc"

// renameReservedSlug is a member of the provisioning reserved-slug set (isReservedSlug,
// service.go) → 400 SLUG_RESERVED. "admin" is stable across the set.
const renameReservedSlug = "admin"

// testAccessGatedPattern is the group-gated channel pattern the test-access discriminator hinges
// on: testChannelRules maps group "vip" → ["room.vip"]. groups=["vip"] must yield it in
// allowed_patterns; groups=["nobody"] (no match, falls to default+public) must exclude it. Only
// this pattern's membership/absence is asserted — the placeholder public patterns (dm.{principal})
// are ignored to avoid placeholder brittleness.
const testAccessGatedPattern = "room.vip"

// testAccessMatchGroup / testAccessNoMatchGroup are the discriminating groups pair.
const (
	testAccessMatchGroup   = "vip"
	testAccessNoMatchGroup = "nobody"
)

// maxSlugLen mirrors ValidateSlug's 63-char cap (types.go:161).
const maxSlugLen = 63

// validateProvisioningRenameTestAccess runs the e2e-unique rename + test-access coverage checks.
// The rename block operates on a throwaway tenant B (never the suite's primary
// tenant A, which later checks depend on — the role-gate negative POSTs a rename OF A but is
// rejected by RequireRole before the handler, so A is never mutated). Every negative asserts HTTP
// status AND the exact error code, paired with a near-identical allowed case ([[one-sided-green]]).
//
// The block is invoked from validateProvisioning immediately after the routing block and BEFORE
// the suite's "set channel rules" check — so the test-access sub-block self-establishes its full
// channel-rule state (set testChannelRules → assert → delete → assert [] → restore) rather than
// assuming ambient rules, and leaves channel-rules in the state checks 11-12 expect.
//
// Peak-tenant invariant: throwaway B is provisioned and eagerly cleaned up NON-OVERLAPPING
// with the routing block's checkCrossTenantMismatch B → peak = A + 1 = 2 under Community MaxTenants=3.
//
// No edition parameter: both endpoints are all-editions (not gated), so there is no
// per-edition branching — matching the sibling validateProvisioningRouting (§XVIII).
func validateProvisioningRenameTestAccess(
	ctx context.Context,
	run *TestRun,
	setupA *auth.SetupResult,
	logger zerolog.Logger,
) []metrics.CheckResult {
	checks := renameChecks(ctx, run, setupA, logger)
	checks = append(checks, testAccessChecks(ctx, setupA)...)
	return checks
}

// renameChecks provisions a throwaway tenant B and runs the 7 rename checks in service step order:
// bad-json → invalid-slug → reserved-slug → slug-taken → role-gate → happy-path → hold-period.
// B is eagerly cleaned up by its CURRENT slug (the new slug after a successful rename — setupB.Cleanup
// captures the stale old slug, so relying on it would 404-leak the tenant and erode headroom).
//
//nolint:gocritic // appendCombine: each provCheck runs its closure eagerly and in order; the happy-path check mutates currentSlug that the hold-period check reads, so the sequential appends cannot be combined.
func renameChecks(
	ctx context.Context,
	run *TestRun,
	setupA *auth.SetupResult,
	logger zerolog.Logger,
) []metrics.CheckResult {
	names := []string{
		RenameBadJSONCheckName, RenameInvalidSlugCheckName, RenameReservedSlugCheckName,
		RenameSlugTakenCheckName, RenameRoleGateCheckName, RenameHappyPathCheckName,
		RenameHoldPeriodCheckName,
	}

	setupB, err := auth.Setup(ctx, auth.SetupConfig{
		TestID:               run.ID + "-prov-rename-b",
		ProvisioningURL:      run.Config.ProvisioningURL,
		Logger:               logger,
		AdminProvider:        run.authResult.AdminProvider,
		RequireAdminProvider: true,
	})
	if err != nil {
		return failAll(names, fmt.Sprintf("setup tenant B: %v", err))
	}
	// setupB.Cleanup captures the ORIGINAL slug — best-effort tolerated (a 404 no-op post-rename).
	defer setupB.Cleanup(context.Background()) //nolint:contextcheck // cleanup must survive parent cancellation

	provClient := setupB.ProvClient
	tenantA := setupA.TenantID
	tenantB := setupB.TenantID
	// currentSlug tracks tenant B's live slug so eager own-cleanup targets the NEW slug post-rename.
	currentSlug := tenantB
	defer func() { //nolint:contextcheck // cleanup must survive parent cancellation
		if err := provClient.DeleteTenant(context.Background(), currentSlug); err != nil {
			logger.Debug().Err(err).Str(logging.LogKeyTenantSlug, currentSlug).
				Msg("rename block: eager tenant B cleanup (404 tolerated)")
		}
	}()

	newSlug := buildRenameSlug(run.ID)

	var checks []metrics.CheckResult

	// --- rename bad json: malformed body → 400 INVALID_REQUEST (paired with a valid rename below) ---
	// Rejected at the handler before the service, so no tenant mutation. Paired allowed case is
	// the happy-path success (differs in the body being well-formed).
	checks = append(checks, provCheck(RenameBadJSONCheckName, func() error {
		status, rErr := renameRawBadBody(ctx, provClient, tenantB)
		return expectReject(RenameBadJSONCheckName, status, rErr, http.StatusBadRequest, errCodeInvalidRequest)
	}))

	// --- rename invalid slug: "1abc" (digit-leading) → 400 SLUG_INVALID ---
	checks = append(checks, provCheck(RenameInvalidSlugCheckName, func() error {
		status, rErr := provClient.RenameTenantRaw(ctx, tenantB, renameInvalidSlug)
		return expectReject(RenameInvalidSlugCheckName, status, rErr, http.StatusBadRequest, errCodeSlugInvalid)
	}))

	// --- rename reserved slug: "admin" → 400 SLUG_RESERVED ---
	checks = append(checks, provCheck(RenameReservedSlugCheckName, func() error {
		status, rErr := provClient.RenameTenantRaw(ctx, tenantB, renameReservedSlug)
		return expectReject(RenameReservedSlugCheckName, status, rErr, http.StatusBadRequest, errCodeSlugReserved)
	}))

	// --- rename slug taken: rename B → tenant A's live slug → 409 SLUG_ALREADY_TAKEN ---
	checks = append(checks, provCheck(RenameSlugTakenCheckName, func() error {
		status, rErr := provClient.RenameTenantRaw(ctx, tenantB, tenantA)
		return expectReject(RenameSlugTakenCheckName, status, rErr, http.StatusConflict, errCodeSlugAlreadyTaken)
	}))

	// --- rename role gate: an A-scoped user token renaming tenant A's OWN slug → 403 INSUFFICIENT_ROLE.
	// RequireTenant runs before RequireRole (router.go), so targeting A with an A-scoped token passes
	// tenant isolation and lets RequireRole reject BEFORE the handler — A is never mutated. Targeting B
	// with an A-scoped token would 403 TENANT_MISMATCH (wrong code). Paired allowed case = happy-path
	// admin rename below (differs only in the token's role). Mirrors #205's `routing role gate write`.
	checks = append(checks, provCheck(RenameRoleGateCheckName, func() error {
		userTokenA, mintErr := setupA.Minter.MintWithClaims(auth.MintOptions{
			Subject: "rename-user-" + uuid.NewString()[:8],
			Roles:   []string{"user"},
		})
		if mintErr != nil {
			return fmt.Errorf("mint user token: %w", mintErr)
		}
		status, rErr := setupA.ProvClient.RenameTenantRawWithToken(ctx, tenantA, userTokenA, newSlug)
		return expectReject(RenameRoleGateCheckName, status, rErr, http.StatusForbidden, errCodeInsufficientRole)
	}))

	// --- rename happy path: admin renames B → newSlug → 200, then an admin-signed GetTenantByID(newSlug)
	// read-back confirms B is addressable under the new slug. GetTenantByID is admin-signed
	// (NOT GetTenant, which would need a tenant token scoped to the stale old slug). Updates currentSlug
	// so eager cleanup targets the new slug.
	checks = append(checks, provCheck(RenameHappyPathCheckName, func() error {
		status, rErr := provClient.RenameTenantRaw(ctx, currentSlug, newSlug)
		if err := expectStatus(RenameHappyPathCheckName, status, rErr, http.StatusOK); err != nil {
			return err
		}
		currentSlug = newSlug // identity mutated — subsequent addressing (incl. cleanup) uses newSlug
		body, gErr := provClient.GetTenantByID(ctx, newSlug)
		if gErr != nil {
			return fmt.Errorf("read-back GetTenantByID(%s): %w", newSlug, gErr)
		}
		if !strings.Contains(string(body), newSlug) {
			return fmt.Errorf("read-back: new slug %q not present in tenant body (body=%s)", newSlug, string(body))
		}
		return nil
	}))

	// --- rename hold period: a second rename immediately after the successful one → 409
	// SLUG_RENAME_IN_PROGRESS (the old slug is in the topic hold period; no wait for expiry).
	// Paired allowed case = the happy-path success above (differs only in the hold-period precondition).
	checks = append(checks, provCheck(RenameHoldPeriodCheckName, func() error {
		secondTarget := buildRenameSlug(run.ID + "-2")
		status, rErr := provClient.RenameTenantRaw(ctx, currentSlug, secondTarget)
		return expectReject(RenameHoldPeriodCheckName, status, rErr, http.StatusConflict, errCodeSlugRenameInProgress)
	}))

	return checks
}

// testAccessChecks runs the 3 test-access checks against tenant A using a tenant token. It
// self-establishes and restores the channel-rule fixture so later suite checks (11 set / 12 get)
// see testChannelRules.
//
//nolint:gocritic // appendCombine: the no-rules check deletes and restores channel rules that the paired computed-patterns and bad-json checks depend on, so the three appends run in order and cannot be combined.
func testAccessChecks(ctx context.Context, setupA *auth.SetupResult) []metrics.CheckResult {
	names := []string{TestAccessComputedCheckName, TestAccessNoRulesCheckName, TestAccessBadJSONCheckName}

	provClient := setupA.ProvClient
	tenantA := setupA.TenantID

	tenantToken, mintErr := setupA.Minter.MintWithClaims(auth.MintOptions{
		Subject: "test-access-" + uuid.NewString()[:8],
	})
	if mintErr != nil {
		return failAll(names, fmt.Sprintf("mint tenant token: %v", mintErr))
	}

	// Establish the known rule state up front; restore it before returning regardless of outcome.
	if err := provClient.SetChannelRules(ctx, tenantA, testChannelRules); err != nil {
		return failAll(names, fmt.Sprintf("set channel rules fixture: %v", err))
	}
	defer func() { _ = provClient.SetChannelRules(ctx, tenantA, testChannelRules) }() // restore

	var checks []metrics.CheckResult

	// --- test-access computed patterns: matching groups=["vip"] → allowed_patterns CONTAINS room.vip;
	// non-matching groups=["nobody"] → EXCLUDES room.vip. The discriminator is the group-gated pattern's
	// presence/absence across the pair. ---
	checks = append(checks, provCheck(TestAccessComputedCheckName, func() error {
		matchStatus, matchBody, mErr := provClient.TestAccessRaw(ctx, tenantA, tenantToken, []string{testAccessMatchGroup})
		if err := expectStatus("test-access match", matchStatus, mErr, http.StatusOK); err != nil {
			return err
		}
		if !allowedPatternsContain(matchBody, testAccessGatedPattern) {
			return fmt.Errorf("test-access match: expected %q in allowed_patterns, body=%s", testAccessGatedPattern, string(matchBody))
		}
		noMatchStatus, noMatchBody, nmErr := provClient.TestAccessRaw(ctx, tenantA, tenantToken, []string{testAccessNoMatchGroup})
		if err := expectStatus("test-access no-match", noMatchStatus, nmErr, http.StatusOK); err != nil {
			return err
		}
		if allowedPatternsContain(noMatchBody, testAccessGatedPattern) {
			return fmt.Errorf("test-access no-match: %q must be excluded from allowed_patterns, body=%s", testAccessGatedPattern, string(noMatchBody))
		}
		return nil
	}))

	// --- test-access no rules: delete channel rules → 200 + empty allowed_patterns; restore fixture.
	// Paired allowed case = the computed-patterns 200 above (differs only in the ruleless state). ---
	checks = append(checks, provCheck(TestAccessNoRulesCheckName, func() error {
		if err := provClient.DeleteChannelRules(ctx, tenantA); err != nil {
			return fmt.Errorf("delete channel rules: %w", err)
		}
		status, body, taErr := provClient.TestAccessRaw(ctx, tenantA, tenantToken, []string{testAccessMatchGroup})
		if err := expectStatus("test-access no-rules", status, taErr, http.StatusOK); err != nil {
			return err
		}
		patterns, parseErr := parseAllowedPatterns(body)
		if parseErr != nil {
			return fmt.Errorf("test-access no-rules: parse body: %w (body=%s)", parseErr, string(body))
		}
		if len(patterns) != 0 {
			return fmt.Errorf("test-access no-rules: expected empty allowed_patterns, got %v (body=%s)", patterns, string(body))
		}
		// Restore the fixture so the paired computed-patterns state and later suite checks hold.
		if err := provClient.SetChannelRules(ctx, tenantA, testChannelRules); err != nil {
			return fmt.Errorf("test-access no-rules: restore fixture: %w", err)
		}
		return nil
	}))

	// --- test-access bad json: malformed body → 400 INVALID_REQUEST, paired with a well-formed 200. ---
	checks = append(checks, provCheck(TestAccessBadJSONCheckName, func() error {
		okStatus, _, okErr := provClient.TestAccessRaw(ctx, tenantA, tenantToken, []string{testAccessMatchGroup})
		if err := expectStatus("test-access bad-json allowed", okStatus, okErr, http.StatusOK); err != nil {
			return err
		}
		status, badErr := testAccessRawBadBody(ctx, provClient, tenantA, tenantToken)
		return expectReject(TestAccessBadJSONCheckName, status, badErr, http.StatusBadRequest, errCodeInvalidRequest)
	}))

	return checks
}

// --- Helpers ---

// buildRenameSlug constructs a letter-first, ≤63-char, ValidateSlug-safe target slug unique per
// run: renamed-<sanitized runID>-<rand8>. Sanitization lowercases and replaces any non-[a-z0-9-]
// with '-' so a runID with unusual characters cannot produce an invalid slug.
func buildRenameSlug(runID string) string {
	sanitized := sanitizeSlugComponent(runID)
	rand := uuid.NewString()[:8]
	slug := "renamed-" + sanitized + "-" + rand
	if len(slug) > maxSlugLen {
		// Trim the middle (sanitized runID) to fit; the letter-first prefix and rand suffix are kept.
		keep := max(maxSlugLen-len("renamed-")-len("-")-len(rand), 0)
		keep = min(keep, len(sanitized))
		slug = "renamed-" + sanitized[:keep] + "-" + rand
	}
	// Collapse any accidental leading/trailing hyphen artifacts from an empty sanitized component.
	return strings.Trim(slug, "-")
}

// sanitizeSlugComponent lowercases s and replaces every character outside [a-z0-9-] with '-'.
func sanitizeSlugComponent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// parseAllowedPatterns unmarshals a test-access success body's allowed_patterns array.
func parseAllowedPatterns(body []byte) ([]string, error) {
	var resp struct {
		AllowedPatterns []string `json:"allowed_patterns"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal allowed_patterns: %w", err)
	}
	return resp.AllowedPatterns, nil
}

// allowedPatternsContain reports whether pattern is a member of the body's allowed_patterns. A
// malformed body yields false (the caller's assertion then fails loudly with the raw body).
func allowedPatternsContain(body []byte, pattern string) bool {
	patterns, err := parseAllowedPatterns(body)
	if err != nil {
		return false
	}
	return slices.Contains(patterns, pattern)
}

// renameRawBadBody POSTs a deliberately malformed JSON body to the rename endpoint (admin-signed)
// and returns the status + error. The client's typed methods cannot send invalid JSON, so this
// issues the raw request directly to exercise the handler's 400 INVALID_REQUEST branch.
func renameRawBadBody(ctx context.Context, provClient *auth.ProvisioningClient, slug string) (int, error) {
	// The wrapped error preserves the embedded response body ({"code":...}) so expectReject's
	// extractErrorCode still recovers INVALID_REQUEST (it scans from the first '{').
	status, err := provClient.PostRawAdmin(ctx, "/api/v1/tenants/"+slug+"/rename", "rename tenant", []byte("{not json"))
	if err != nil {
		return status, fmt.Errorf("rename bad body: %w", err)
	}
	return status, nil
}

// testAccessRawBadBody POSTs a malformed JSON body to test-access with a tenant token, exercising
// the handler's 400 INVALID_REQUEST branch.
func testAccessRawBadBody(ctx context.Context, provClient *auth.ProvisioningClient, slug, token string) (int, error) {
	status, _, err := provClient.PostRawWithTokenBody(ctx, "/api/v1/tenants/"+slug+"/test-access", token, "test access", []byte("{not json"))
	if err != nil {
		return status, fmt.Errorf("test-access bad body: %w", err)
	}
	return status, nil
}

// failAll builds a fail result for every name with the same error (setup-failure fan-out).
func failAll(names []string, msg string) []metrics.CheckResult {
	results := make([]metrics.CheckResult, 0, len(names))
	for _, name := range names {
		results = append(results, metrics.CheckResult{Name: name, Status: metrics.CheckStatusFail, Error: msg})
	}
	return results
}
