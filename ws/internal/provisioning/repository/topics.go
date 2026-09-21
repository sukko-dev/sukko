package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
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
