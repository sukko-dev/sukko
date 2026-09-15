package repository_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// metricSeq produces a unique metric prefix per test, avoiding prometheus duplicate-registration
// panics when multiple tests in the same process construct a RoutingRulesRepository.
// promauto registers to the process-global prometheus.DefaultRegisterer, so each
// repo construction must use a distinct prefix to prevent "duplicate collector" panics.
var metricSeq atomic.Int64

// routingRulesFixture holds everything a routing-rules test needs.
type routingRulesFixture struct {
	repo     *repository.RoutingRulesRepository
	tenantID string
	ctx      context.Context
}

// newRoutingRulesFixture creates an isolated containerised Postgres DB, runs migrations,
// seeds the required tenant row (tenant_routing_rules.tenant_id has a FK to tenants.id),
// and returns a wired RoutingRulesRepository.
// slug is used as the human-readable slug; a fresh UUID is generated for tenants.id.
func newRoutingRulesFixture(t *testing.T, slug string) routingRulesFixture {
	t.Helper()
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	tenantUUID := uuid.New().String()
	// Seed the tenant row required by the FK constraint.
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata)
		 VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
		tenantUUID, slug, slug+"-name",
	); err != nil {
		t.Fatalf("seed tenant row: %v", err)
	}

	prefix := fmt.Sprintf("test%d", metricSeq.Add(1))
	repo := repository.NewRoutingRulesRepository(pool, zerolog.Nop(), prefix)
	return routingRulesFixture{repo: repo, tenantID: tenantUUID, ctx: ctx}
}

// ── Add ──────────────────────────────────────────────────────────────────────

func TestRoutingRulesRepository_Add_Success(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-add-ok")

	rule := provisioning.TopicRoutingRule{
		Pattern:  "orders.**.trade",
		Topics:   []string{"trades"},
		Priority: 10,
	}

	if err := f.repo.Add(f.ctx, f.tenantID, rule); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() after Add() error = %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("GetAll() len = %d, want 1", len(rules))
	}
	if rules[0].Priority != 10 {
		t.Errorf("Priority = %d, want 10", rules[0].Priority)
	}
	if rules[0].Pattern != "orders.**.trade" {
		t.Errorf("Pattern = %q, want %q", rules[0].Pattern, "orders.**.trade")
	}
}

func TestRoutingRulesRepository_Add_DuplicatePriority(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-add-dup")

	first := provisioning.TopicRoutingRule{Pattern: "**.trade", Topics: []string{"trades"}, Priority: 5}
	second := provisioning.TopicRoutingRule{Pattern: "**.quote", Topics: []string{"quotes"}, Priority: 5} // same priority

	if err := f.repo.Add(f.ctx, f.tenantID, first); err != nil {
		t.Fatalf("first Add() error = %v", err)
	}

	err := f.repo.Add(f.ctx, f.tenantID, second)
	if err == nil {
		t.Fatal("second Add() should return an error for duplicate priority, got nil")
	}
	if !errors.Is(err, provisioning.ErrDuplicatePriority) {
		t.Errorf("second Add() error = %v, want errors.Is(err, ErrDuplicatePriority)", err)
	}
}

func TestRoutingRulesRepository_Add_DuplicatePattern(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-add-dup-pattern")

	first := provisioning.TopicRoutingRule{Pattern: "trades.**", Topics: []string{"trades"}, Priority: 1}
	second := provisioning.TopicRoutingRule{Pattern: "trades.**", Topics: []string{"audit"}, Priority: 2} // same pattern, different priority

	if err := f.repo.Add(f.ctx, f.tenantID, first); err != nil {
		t.Fatalf("first Add() error = %v", err)
	}

	err := f.repo.Add(f.ctx, f.tenantID, second)
	if err == nil {
		t.Fatal("second Add() should return an error for duplicate pattern, got nil")
	}
	if !errors.Is(err, provisioning.ErrDuplicateRoutingPattern) {
		t.Errorf("second Add() error = %v, want errors.Is(err, ErrDuplicateRoutingPattern)", err)
	}
	// Verify priority conflict is NOT the cause (different priorities).
	if errors.Is(err, provisioning.ErrDuplicatePriority) {
		t.Errorf("error should be ErrDuplicateRoutingPattern, not ErrDuplicatePriority")
	}
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestRoutingRulesRepository_List_Empty(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-list-empty")

	rules, total, err := f.repo.List(f.ctx, f.tenantID, 10, 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(rules) != 0 {
		t.Errorf("len(rules) = %d, want 0", len(rules))
	}
}

func TestRoutingRulesRepository_List_OrderByPriority(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-list-order")

	// Insert in reverse priority order to confirm the DB orders by priority, not insertion order.
	for _, rule := range []provisioning.TopicRoutingRule{
		{Pattern: "**.c", Topics: []string{"c"}, Priority: 30},
		{Pattern: "**.a", Topics: []string{"a"}, Priority: 10},
		{Pattern: "**.b", Topics: []string{"b"}, Priority: 20},
	} {
		if err := f.repo.Add(f.ctx, f.tenantID, rule); err != nil {
			t.Fatalf("Add(priority=%d) error = %v", rule.Priority, err)
		}
	}

	rules, total, err := f.repo.List(f.ctx, f.tenantID, 10, 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(rules) != 3 {
		t.Fatalf("len(rules) = %d, want 3", len(rules))
	}
	for i, wantPriority := range []int{10, 20, 30} {
		if rules[i].Priority != wantPriority {
			t.Errorf("rules[%d].Priority = %d, want %d", i, rules[i].Priority, wantPriority)
		}
	}
}

func TestRoutingRulesRepository_List_Pagination(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-list-page")

	// Insert 5 rules with priorities 10, 20, 30, 40, 50.
	for i := 1; i <= 5; i++ {
		rule := provisioning.TopicRoutingRule{
			Pattern:  fmt.Sprintf("**.rule%d", i),
			Topics:   []string{fmt.Sprintf("topic%d", i)},
			Priority: i * 10,
		}
		if err := f.repo.Add(f.ctx, f.tenantID, rule); err != nil {
			t.Fatalf("Add(rule %d) error = %v", i, err)
		}
	}

	tests := []struct {
		name          string
		limit         int
		offset        int
		wantLen       int
		wantFirstPrio int // checked only when wantLen > 0
		wantTotal     int
	}{
		{name: "first page", limit: 2, offset: 0, wantLen: 2, wantFirstPrio: 10, wantTotal: 5},
		{name: "second page", limit: 2, offset: 2, wantLen: 2, wantFirstPrio: 30, wantTotal: 5},
		{name: "last page partial", limit: 2, offset: 4, wantLen: 1, wantFirstPrio: 50, wantTotal: 5},
		{name: "beyond end", limit: 2, offset: 10, wantLen: 0, wantFirstPrio: 0, wantTotal: 5},
		{name: "all at once", limit: 10, offset: 0, wantLen: 5, wantFirstPrio: 10, wantTotal: 5},
	}

	for _, tc := range tests { //nolint:paralleltest // sub-tests share a live DB fixture; t.Parallel() is forbidden by Constitution VIII
		t.Run(tc.name, func(t *testing.T) {
			rules, total, err := f.repo.List(f.ctx, f.tenantID, tc.limit, tc.offset)
			if err != nil {
				t.Fatalf("List(limit=%d, offset=%d) error = %v", tc.limit, tc.offset, err)
			}
			if total != tc.wantTotal {
				t.Errorf("total = %d, want %d", total, tc.wantTotal)
			}
			if len(rules) != tc.wantLen {
				t.Errorf("len(rules) = %d, want %d", len(rules), tc.wantLen)
			}
			if tc.wantLen > 0 && rules[0].Priority != tc.wantFirstPrio {
				t.Errorf("rules[0].Priority = %d, want %d", rules[0].Priority, tc.wantFirstPrio)
			}
		})
	}
}

// ── Replace ───────────────────────────────────────────────────────────────────

func TestRoutingRulesRepository_Replace_ReplacesAll(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-replace-ok")

	// Seed initial rules.
	for _, r := range []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trades"}, Priority: 1},
		{Pattern: "**.quote", Topics: []string{"quotes"}, Priority: 2},
	} {
		if err := f.repo.Add(f.ctx, f.tenantID, r); err != nil {
			t.Fatalf("seed Add(priority=%d) error = %v", r.Priority, err)
		}
	}

	// Replace with a completely different set.
	replacement := []provisioning.TopicRoutingRule{
		{Pattern: "**.order", Topics: []string{"orders"}, Priority: 100},
	}
	if err := f.repo.Replace(f.ctx, f.tenantID, replacement); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() after Replace() error = %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("len(rules) = %d, want 1", len(rules))
	}
	if rules[0].Priority != 100 {
		t.Errorf("Priority = %d, want 100", rules[0].Priority)
	}
	if rules[0].Pattern != "**.order" {
		t.Errorf("Pattern = %q, want %q", rules[0].Pattern, "**.order")
	}
}

func TestRoutingRulesRepository_Replace_EmptyDeletesAll(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-replace-empty")

	if err := f.repo.Add(f.ctx, f.tenantID, provisioning.TopicRoutingRule{
		Pattern: "**.trade", Topics: []string{"trades"}, Priority: 1,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	// Replace with empty slice — should delete all existing rules.
	if err := f.repo.Replace(f.ctx, f.tenantID, []provisioning.TopicRoutingRule{}); err != nil {
		t.Fatalf("Replace(empty) error = %v", err)
	}

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() after Replace(empty) error = %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("len(rules) = %d, want 0", len(rules))
	}
}

func TestRoutingRulesRepository_Replace_Atomicity(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-replace-atomic")

	// Seed initial rules at priorities 1 and 2.
	for _, r := range []provisioning.TopicRoutingRule{
		{Pattern: "**.trade", Topics: []string{"trades"}, Priority: 1},
		{Pattern: "**.quote", Topics: []string{"quotes"}, Priority: 2},
	} {
		if err := f.repo.Add(f.ctx, f.tenantID, r); err != nil {
			t.Fatalf("seed Add(priority=%d) error = %v", r.Priority, err)
		}
	}

	// Attempt Replace with two rules sharing the same priority — the second INSERT
	// will fail with a unique-constraint violation, causing the transaction to abort.
	// The pre-existing rules (priority 1 and 2) must survive unchanged.
	bad := []provisioning.TopicRoutingRule{
		{Pattern: "**.order", Topics: []string{"orders"}, Priority: 50},
		{Pattern: "**.cancel", Topics: []string{"cancels"}, Priority: 50}, // duplicate
	}
	err := f.repo.Replace(f.ctx, f.tenantID, bad)
	if err == nil {
		t.Fatal("Replace() with duplicate priority should return an error, got nil")
	}
	if !errors.Is(err, provisioning.ErrDuplicatePriority) {
		t.Errorf("Replace() error = %v, want errors.Is(err, ErrDuplicatePriority)", err)
	}

	// The transaction must have rolled back — original rules must be intact.
	rules, getErr := f.repo.GetAll(f.ctx, f.tenantID)
	if getErr != nil {
		t.Fatalf("GetAll() after failed Replace() error = %v", getErr)
	}
	if len(rules) != 2 {
		t.Fatalf("len(rules) = %d, want 2 (original rules must survive rollback)", len(rules))
	}
	if rules[0].Priority != 1 || rules[1].Priority != 2 {
		t.Errorf("priorities = [%d, %d], want [1, 2]", rules[0].Priority, rules[1].Priority)
	}
}

// ── DeleteAll ────────────────────────────────────────────────────────────────

func TestRoutingRulesRepository_DeleteAll_Idempotent(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-deleteall-idempotent")

	// First call: table is empty for this tenant — must not error.
	if err := f.repo.DeleteAll(f.ctx, f.tenantID); err != nil {
		t.Fatalf("DeleteAll() on empty table error = %v", err)
	}

	// Add a rule, then delete it.
	if err := f.repo.Add(f.ctx, f.tenantID, provisioning.TopicRoutingRule{
		Pattern: "**.trade", Topics: []string{"trades"}, Priority: 1,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := f.repo.DeleteAll(f.ctx, f.tenantID); err != nil {
		t.Fatalf("first DeleteAll() error = %v", err)
	}

	// Second call after the table is already empty — must not error.
	if err := f.repo.DeleteAll(f.ctx, f.tenantID); err != nil {
		t.Fatalf("second DeleteAll() (idempotent) error = %v", err)
	}

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() after DeleteAll() error = %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("len(rules) = %d, want 0", len(rules))
	}
}

func TestRoutingRulesRepository_DeleteAll_OnlyAffectsTargetTenant(t *testing.T) {
	t.Parallel()

	// Two tenants share the same DB — DeleteAll on one must not touch the other.
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("test%d", metricSeq.Add(1))
	repo := repository.NewRoutingRulesRepository(pool, zerolog.Nop(), prefix)

	tenantA := uuid.New().String()
	tenantB := uuid.New().String()
	for _, row := range []struct{ id, slug string }{{tenantA, "tenant-da-a"}, {tenantB, "tenant-da-b"}} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata) VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
			row.id, row.slug, row.slug+"-name",
		); err != nil {
			t.Fatalf("seed tenant %s: %v", row.slug, err)
		}
	}

	if err := repo.Add(ctx, tenantA, provisioning.TopicRoutingRule{
		Pattern: "**.trade", Topics: []string{"trades"}, Priority: 1,
	}); err != nil {
		t.Fatalf("Add(tenantA) error = %v", err)
	}
	if err := repo.Add(ctx, tenantB, provisioning.TopicRoutingRule{
		Pattern: "**.quote", Topics: []string{"quotes"}, Priority: 1,
	}); err != nil {
		t.Fatalf("Add(tenantB) error = %v", err)
	}

	// Delete only tenant-a's rules.
	if err := repo.DeleteAll(ctx, tenantA); err != nil {
		t.Fatalf("DeleteAll(tenantA) error = %v", err)
	}

	// Tenant A should have no rules.
	rulesA, err := repo.GetAll(ctx, tenantA)
	if err != nil {
		t.Fatalf("GetAll(tenantA) error = %v", err)
	}
	if len(rulesA) != 0 {
		t.Errorf("tenantA len(rules) = %d, want 0", len(rulesA))
	}

	// Tenant B's rules must be untouched.
	rulesB, err := repo.GetAll(ctx, tenantB)
	if err != nil {
		t.Fatalf("GetAll(tenantB) error = %v", err)
	}
	if len(rulesB) != 1 {
		t.Errorf("tenantB len(rules) = %d, want 1", len(rulesB))
	}
}

// ── GetAll ────────────────────────────────────────────────────────────────────

func TestRoutingRulesRepository_GetAll_Empty(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-getall-empty")

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("len(rules) = %d, want 0", len(rules))
	}
}

func TestRoutingRulesRepository_GetAll_OrderByPriority(t *testing.T) {
	t.Parallel()
	f := newRoutingRulesFixture(t, "tenant-getall-order")

	// Insert in non-priority order to confirm ordering is by column, not insertion.
	for _, rule := range []provisioning.TopicRoutingRule{
		{Pattern: "**.c", Topics: []string{"c"}, Priority: 30},
		{Pattern: "**.a", Topics: []string{"a"}, Priority: 10},
		{Pattern: "**.b", Topics: []string{"b"}, Priority: 20},
	} {
		if err := f.repo.Add(f.ctx, f.tenantID, rule); err != nil {
			t.Fatalf("Add(priority=%d) error = %v", rule.Priority, err)
		}
	}

	rules, err := f.repo.GetAll(f.ctx, f.tenantID)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(rules) != 3 {
		t.Fatalf("len(rules) = %d, want 3", len(rules))
	}
	for i, wantPriority := range []int{10, 20, 30} {
		if rules[i].Priority != wantPriority {
			t.Errorf("rules[%d].Priority = %d, want %d", i, rules[i].Priority, wantPriority)
		}
	}
}

func TestRoutingRulesRepository_GetAll_NormalizationApplied(t *testing.T) {
	t.Parallel()

	// Insert a bare-* pattern directly via SQL (bypassing Add which stores as-is).
	// scanRows must normalise bare * to ** on read.
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	tenantID := uuid.New().String()
	prefix := fmt.Sprintf("test%d", metricSeq.Add(1))
	repo := repository.NewRoutingRulesRepository(pool, zerolog.Nop(), prefix)

	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata) VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
		tenantID, "tenant-getall-norm", "tenant-getall-norm-name",
	); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	// Store pattern with bare * (unnormalized form).
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority) VALUES ($1, $2, $3, $4)`,
		tenantID, "orders.*.trade", []string{"trades"}, 1,
	); err != nil {
		t.Fatalf("direct insert unnormalized pattern: %v", err)
	}

	rules, err := repo.GetAll(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("len(rules) = %d, want 1", len(rules))
	}
	// NormalizePattern replaces bare * with **
	const wantPattern = "orders.**.trade"
	if rules[0].Pattern != wantPattern {
		t.Errorf("Pattern = %q, want %q (normalization must be applied on read)", rules[0].Pattern, wantPattern)
	}
}

func TestRoutingRulesRepository_GetAll_InvalidPatternSkipped(t *testing.T) {
	t.Parallel()

	// Insert a pattern containing two ** segments — invalid after normalization.
	// scanRows must skip it silently and return only the valid rule.
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	tenantID := uuid.New().String()
	prefix := fmt.Sprintf("test%d", metricSeq.Add(1))
	repo := repository.NewRoutingRulesRepository(pool, zerolog.Nop(), prefix)

	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, slug, name, status, consumer_type, metadata) VALUES ($1, $2, $3, 'active', 'shared', '{}')`,
		tenantID, "tenant-getall-skip", "tenant-getall-skip-name",
	); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	// Valid rule at priority 10.
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority) VALUES ($1, $2, $3, $4)`,
		tenantID, "**.trade", []string{"trades"}, 10,
	); err != nil {
		t.Fatalf("insert valid rule: %v", err)
	}

	// Invalid pattern (two ** segments) at priority 20 — MatchRoutingPattern returns
	// ErrMultipleDoubleWildcard, causing scanRows to skip this row.
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority) VALUES ($1, $2, $3, $4)`,
		tenantID, "**.**.foo", []string{"foo"}, 20,
	); err != nil {
		t.Fatalf("insert invalid rule: %v", err)
	}

	rules, err := repo.GetAll(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	// Only the valid rule should be returned; the invalid pattern is silently skipped.
	if len(rules) != 1 {
		t.Fatalf("len(rules) = %d, want 1 (invalid pattern must be skipped)", len(rules))
	}
	if rules[0].Priority != 10 {
		t.Errorf("Priority = %d, want 10", rules[0].Priority)
	}
}
