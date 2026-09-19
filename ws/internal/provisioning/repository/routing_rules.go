package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

const labelTenantID = "tenant"

// DB constraint names as named constants (§I — symbolic strings callers must match exactly).
const (
	constraintRoutingRuleTenantPriority = "uq_routing_rule_tenant_priority"
	constraintRoutingRuleTenantPattern  = "uq_routing_rule_tenant_pattern"
)

// TenantRoutingRulesLockQuery acquires the per-tenant, transaction-scoped advisory
// lock that serializes routing-rule writes (Add, Replace) for one tenant. $1 is the
// tenant UUID.
//
// Key derivation: hashtextextended is PostgreSQL's 64-bit text hash — it backs hash
// partitioning, so PostgreSQL guarantees it is stable across versions and platforms,
// giving a deterministic bigint key per tenant. The "routing-rules:" prefix
// namespaces this use away from any future advisory-lock caller hashing bare IDs.
// The only other advisory lock in the codebase is the migration lock
// (shared/database/migrate.go, literal int8 0x73756B6B6F); a hashtextextended output
// colliding with it — or two tenants colliding with each other — is a ~2^-64 event
// whose worst case is transient extra serialization, never corruption or deadlock
// (locks are always taken singly, so no ordering cycle can form).
//
// Exported so tests can hold the exact lock a write path will contend on (§I —
// defined once, referenced everywhere).
const TenantRoutingRulesLockQuery = `SELECT pg_advisory_xact_lock(hashtextextended('routing-rules:' || $1, 0))`

// RoutingRulesRepository implements RoutingRulesStore using PostgreSQL via pgxpool.
// All tenantID parameters are the tenant UUID primary key (FK → tenants.id), not the slug.
type RoutingRulesRepository struct {
	pool                *pgxpool.Pool
	logger              zerolog.Logger
	invalidPatternCount *prometheus.CounterVec
}

// NewRoutingRulesRepository creates a RoutingRulesRepository and registers its Prometheus counters.
// metricPrefix is prepended to all metric names (use "provisioning" in production; tests must use
// unique prefixes — see routing_rules_test.go:metricSeq — because the Prometheus default registerer
// is process-global and panics on duplicate names.
func NewRoutingRulesRepository(pool *pgxpool.Pool, logger zerolog.Logger, metricPrefix string) *RoutingRulesRepository {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: metricPrefix + "_" + routing.MetricInvalidPatternBase,
		Help: "Total number of routing rules skipped due to invalid pattern after normalization.",
	}, []string{labelTenantID})

	// Register with the default registerer; if already registered (e.g., in integration test suites
	// that reuse the same prefix), reuse the existing collector rather than panic.
	if err := prometheus.DefaultRegisterer.Register(cv); err != nil {
		are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
		if !ok {
			panic(err)
		}
		existing, ok := are.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			panic(fmt.Sprintf("routing_rules: metric %q already registered as wrong type %T",
				metricPrefix+"_"+routing.MetricInvalidPatternBase, are.ExistingCollector))
		}
		cv = existing
	}

	return &RoutingRulesRepository{
		pool:                pool,
		logger:              logger,
		invalidPatternCount: cv,
	}
}

// List returns paginated routing rules for a tenant ordered by priority ascending.
func (r *RoutingRulesRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]provisioning.TopicRoutingRule, int, error) {
	totalQuery := `SELECT COUNT(*) FROM tenant_routing_rules WHERE tenant_id = $1`
	var total int
	if err := r.pool.QueryRow(ctx, totalQuery, tenantID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count routing rules: %w", err)
	}

	if total == 0 {
		return nil, 0, nil
	}

	listQuery := `
		SELECT pattern, ingress_topic, egress_topics, priority
		FROM tenant_routing_rules
		WHERE tenant_id = $1
		ORDER BY priority ASC
		LIMIT $2 OFFSET $3
	`
	rows, err := r.pool.Query(ctx, listQuery, tenantID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list routing rules: %w", err)
	}
	defer rows.Close()

	rules, err := r.scanRows(ctx, tenantID, rows)
	if err != nil {
		return nil, 0, err
	}
	return rules, total, nil
}

// lockTenantRoutingRules takes the per-tenant advisory lock inside tx, blocking
// until any concurrent routing-rule write transaction for the same tenant commits
// or rolls back. The _xact_ variant releases automatically with the transaction
// (commit, rollback, connection loss) — no manual unlock, no leaked locks.
//
// Correctness requires the transaction to run at the default READ COMMITTED
// isolation: each statement after the lock takes a fresh snapshot and therefore
// sees the previous holder's committed rows. Do not raise the isolation level in
// callers — under REPEATABLE READ the snapshot would predate the competing commit
// and the read-validate-insert race would silently return.
func lockTenantRoutingRules(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, TenantRoutingRulesLockQuery, tenantID); err != nil {
		return fmt.Errorf("acquire tenant routing-rules lock: %w", err)
	}
	return nil
}

// Add inserts a single routing rule for a tenant, enforcing the tenant-wide
// ingress/egress disjointness invariant (ADR-0018) atomically: the per-tenant
// advisory lock, the existing-rules read, the disjointness validation, and the
// insert run in ONE transaction, so of two concurrent Adds the second blocks on
// the lock and then validates against the first's committed row. The equivalent
// check in Service.AddRoutingRule is a fast-fail pre-check only (§II defense in
// depth) — this is the authoritative one.
// Returns ErrDuplicatePriority on priority conflict, ErrDuplicateRoutingPattern
// on pattern conflict, ErrEgressIngressOverlap on a disjointness violation.
func (r *RoutingRulesRepository) Add(ctx context.Context, tenantID string, rule provisioning.TopicRoutingRule) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Unconditional rollback: idempotent after a successful Commit (returns
	// ErrTxClosed, ignored per §III), and it MUST run even when the failure is
	// carried in an if-scoped err (the lock acquisition below) or a panic occurs
	// between Begin and Commit — a conditional rollback would leak the pool
	// connection on exactly the contended-timeout path the advisory lock creates.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockTenantRoutingRules(ctx, tx, tenantID); err != nil {
		return err
	}

	existing, err := r.getAll(ctx, tx, tenantID)
	if err != nil {
		return err
	}
	if err = provisioning.ValidateEgressDisjoint(append(existing, rule)); err != nil {
		return fmt.Errorf("add routing rule: %w", err)
	}

	const insertQuery = `
		INSERT INTO tenant_routing_rules (tenant_id, pattern, ingress_topic, egress_topics, priority)
		VALUES ($1, $2, $3, $4, $5)
	`
	if _, err = tx.Exec(ctx, insertQuery, tenantID, rule.Pattern, rule.IngressTopic, egressOrEmpty(rule.EgressTopics), rule.Priority); err != nil {
		switch pgUniqueConstraintName(err) {
		case constraintRoutingRuleTenantPriority:
			return fmt.Errorf("add routing rule: %w", provisioning.ErrDuplicatePriority)
		case constraintRoutingRuleTenantPattern:
			return fmt.Errorf("add routing rule: %w", provisioning.ErrDuplicateRoutingPattern)
		}
		return fmt.Errorf("add routing rule: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit routing rule add: %w", err)
	}
	return nil
}

// Replace atomically replaces all routing rules for a tenant (DELETE then batch INSERT).
func (r *RoutingRulesRepository) Replace(ctx context.Context, tenantID string, rules []provisioning.TopicRoutingRule) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Unconditional rollback: idempotent after a successful Commit (returns
	// ErrTxClosed, ignored per §III), and it MUST run even when the failure is
	// carried in an if-scoped err (the lock acquisition below) or a panic occurs
	// between Begin and Commit — a conditional rollback would leak the pool
	// connection on exactly the contended-timeout path the advisory lock creates.
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize with concurrent Adds for this tenant. Without the lock, an Add
	// validated against pre-Replace rows can commit alongside this Replace: the
	// DELETE below cannot see the Add's uncommitted row, so that rule survives
	// the overwrite and may overlap the self-validated replacement set,
	// breaking ADR-0018 disjointness.
	if err := lockTenantRoutingRules(ctx, tx, tenantID); err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `DELETE FROM tenant_routing_rules WHERE tenant_id = $1`, tenantID); err != nil {
		return fmt.Errorf("delete routing rules: %w", err)
	}

	for _, rule := range rules {
		const insertQuery = `
			INSERT INTO tenant_routing_rules (tenant_id, pattern, ingress_topic, egress_topics, priority)
			VALUES ($1, $2, $3, $4, $5)
		`
		if _, err = tx.Exec(ctx, insertQuery, tenantID, rule.Pattern, rule.IngressTopic, egressOrEmpty(rule.EgressTopics), rule.Priority); err != nil {
			switch pgUniqueConstraintName(err) {
			case constraintRoutingRuleTenantPriority:
				return fmt.Errorf("replace routing rules: %w", provisioning.ErrDuplicatePriority)
			case constraintRoutingRuleTenantPattern:
				return fmt.Errorf("replace routing rules: %w", provisioning.ErrDuplicateRoutingPattern)
			}
			return fmt.Errorf("replace routing rules: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit routing rules replace: %w", err)
	}
	return nil
}

// DeleteAll deletes all routing rules for a tenant.
func (r *RoutingRulesRepository) DeleteAll(ctx context.Context, tenantID string) error {
	if _, err := r.pool.Exec(ctx, `DELETE FROM tenant_routing_rules WHERE tenant_id = $1`, tenantID); err != nil {
		return fmt.Errorf("delete all routing rules: %w", err)
	}
	return nil
}

// GetAll returns all routing rules for a tenant ordered by priority, with normalization applied.
// Rules that fail pattern validation after normalization are skipped and counted.
func (r *RoutingRulesRepository) GetAll(ctx context.Context, tenantID string) ([]provisioning.TopicRoutingRule, error) {
	return r.getAll(ctx, r.pool, tenantID)
}

// pgxQuerier abstracts *pgxpool.Pool and pgx.Tx so getAll can read either pooled
// (GetAll) or inside a write transaction (Add's in-tx disjointness re-read).
type pgxQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// getAll implements GetAll against any querier (pool or open transaction).
func (r *RoutingRulesRepository) getAll(ctx context.Context, q pgxQuerier, tenantID string) ([]provisioning.TopicRoutingRule, error) {
	rows, err := q.Query(ctx, `
		SELECT pattern, ingress_topic, egress_topics, priority
		FROM tenant_routing_rules
		WHERE tenant_id = $1
		ORDER BY priority ASC
	`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get all routing rules: %w", err)
	}
	defer rows.Close()

	return r.scanRows(ctx, tenantID, rows)
}

// scanRows scans query rows into TopicRoutingRule, normalizing and validating patterns.
func (r *RoutingRulesRepository) scanRows(_ context.Context, tenantID string, rows pgx.Rows) ([]provisioning.TopicRoutingRule, error) {
	var rules []provisioning.TopicRoutingRule
	for rows.Next() {
		var pattern string
		var ingressTopic string
		var egressTopics []string
		var priority int

		if err := rows.Scan(&pattern, &ingressTopic, &egressTopics, &priority); err != nil {
			return nil, fmt.Errorf("scan routing rule: %w", err)
		}

		normalized := routing.NormalizePattern(pattern)
		if _, err := routing.MatchRoutingPattern(normalized, "probe"); err != nil {
			r.logger.Warn().
				Str(logging.LogKeyTenantUUID, tenantID).
				Str("pattern", pattern).
				Err(err).
				Msg("Skipping invalid routing rule pattern after normalization")
			r.invalidPatternCount.WithLabelValues(tenantID).Inc()
			continue
		}

		rules = append(rules, provisioning.TopicRoutingRule{
			Pattern:      normalized,
			IngressTopic: ingressTopic,
			EgressTopics: egressTopics,
			Priority:     priority,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate routing rules: %w", err)
	}
	return rules, nil
}

// egressOrEmpty maps a nil egress slice to an empty array so the NOT NULL
// egress_topics column never receives a SQL NULL from a rule without egress.
func egressOrEmpty(egress []string) []string {
	if egress == nil {
		return []string{}
	}
	return egress
}

// pgUniqueConstraintName returns the constraint name when err is a PostgreSQL unique violation
// (SQLSTATE 23505), or an empty string otherwise. Uses the Go 1.26+ errors.AsType pattern.
func pgUniqueConstraintName(err error) string {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != pgerrcode.UniqueViolation {
		return ""
	}
	return pgErr.ConstraintName
}

var _ provisioning.RoutingRulesStore = (*RoutingRulesRepository)(nil)
