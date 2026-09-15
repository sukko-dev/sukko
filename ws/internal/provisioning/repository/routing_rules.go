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
		SELECT pattern, topics, priority
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

// Add inserts a single routing rule for a tenant.
// Returns ErrDuplicatePriority on priority conflict, ErrDuplicateRoutingPattern on pattern conflict.
func (r *RoutingRulesRepository) Add(ctx context.Context, tenantID string, rule provisioning.TopicRoutingRule) error {
	query := `
		INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority)
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.pool.Exec(ctx, query, tenantID, rule.Pattern, rule.Topics, rule.Priority)
	if err != nil {
		switch pgUniqueConstraintName(err) {
		case constraintRoutingRuleTenantPriority:
			return fmt.Errorf("add routing rule: %w", provisioning.ErrDuplicatePriority)
		case constraintRoutingRuleTenantPattern:
			return fmt.Errorf("add routing rule: %w", provisioning.ErrDuplicateRoutingPattern)
		}
		return fmt.Errorf("add routing rule: %w", err)
	}
	return nil
}

// Replace atomically replaces all routing rules for a tenant (DELETE then batch INSERT).
func (r *RoutingRulesRepository) Replace(ctx context.Context, tenantID string, rules []provisioning.TopicRoutingRule) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, `DELETE FROM tenant_routing_rules WHERE tenant_id = $1`, tenantID); err != nil {
		return fmt.Errorf("delete routing rules: %w", err)
	}

	for _, rule := range rules {
		const insertQuery = `
			INSERT INTO tenant_routing_rules (tenant_id, pattern, topics, priority)
			VALUES ($1, $2, $3, $4)
		`
		if _, err = tx.Exec(ctx, insertQuery, tenantID, rule.Pattern, rule.Topics, rule.Priority); err != nil {
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
	rows, err := r.pool.Query(ctx, `
		SELECT pattern, topics, priority
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
		var topics []string
		var priority int

		if err := rows.Scan(&pattern, &topics, &priority); err != nil {
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
			Pattern:  normalized,
			Topics:   topics,
			Priority: priority,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate routing rules: %w", err)
	}
	return rules, nil
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
