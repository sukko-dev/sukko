package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
)

// TopicRepository implements provisioning.TopicStore using PostgreSQL via pgxpool.
// It is the durable provisioned-topics source of truth (ADR-0006 Phase 2), so
// TOPIC_NOT_PROVISIONED validation survives a provisioning restart. All tenantID
// parameters are the tenant UUID primary key (FK → tenants.id), not the slug.
type TopicRepository struct {
	pool *pgxpool.Pool
}

// NewTopicRepository creates a TopicRepository.
func NewTopicRepository(pool *pgxpool.Pool) *TopicRepository {
	return &TopicRepository{pool: pool}
}

// Exists reports whether a non-default topic suffix is provisioned for a tenant.
func (r *TopicRepository) Exists(ctx context.Context, tenantID, suffix string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM topics WHERE tenant_id = $1 AND suffix = $2)`,
		tenantID, suffix).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check topic exists: %w", err)
	}
	return exists, nil
}

// Create records a provisioned topic and returns its created_at. Returns
// provisioning.ErrTopicAlreadyExists on a primary-key conflict so the API can
// surface a 409 rather than a 500.
func (r *TopicRepository) Create(ctx context.Context, tenantID, suffix string) (time.Time, error) {
	var createdAt time.Time
	err := r.pool.QueryRow(ctx,
		`INSERT INTO topics (tenant_id, suffix) VALUES ($1, $2) RETURNING created_at`,
		tenantID, suffix).Scan(&createdAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			return time.Time{}, fmt.Errorf("%w: %s", provisioning.ErrTopicAlreadyExists, suffix)
		}
		return time.Time{}, fmt.Errorf("insert topic: %w", err)
	}
	return createdAt, nil
}

// List returns a tenant's provisioned topics ordered by suffix.
func (r *TopicRepository) List(ctx context.Context, tenantID string) ([]provisioning.Topic, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT suffix, created_at FROM topics WHERE tenant_id = $1 ORDER BY suffix`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query topics: %w", err)
	}
	defer rows.Close()

	var topics []provisioning.Topic
	for rows.Next() {
		var t provisioning.Topic
		if err := rows.Scan(&t.Suffix, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan topic: %w", err)
		}
		topics = append(topics, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate topics: %w", err)
	}
	return topics, nil
}

// Delete removes a provisioned topic. Returns provisioning.ErrTopicNotFound when
// no row matched, so a DELETE of an absent topic is a 404 rather than a silent 200.
func (r *TopicRepository) Delete(ctx context.Context, tenantID, suffix string) error {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM topics WHERE tenant_id = $1 AND suffix = $2`, tenantID, suffix)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrTopicNotFound, suffix)
	}
	return nil
}

// Count returns the number of provisioned topics for a tenant (the MaxTopics
// quota denominator; the deterministic default topic and the DLQ are not stored,
// so they never count).
func (r *TopicRepository) Count(ctx context.Context, tenantID string) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topics WHERE tenant_id = $1`, tenantID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count topics: %w", err)
	}
	return n, nil
}

// compile-time assertion: TopicRepository satisfies provisioning.TopicStore.
var _ provisioning.TopicStore = (*TopicRepository)(nil)
