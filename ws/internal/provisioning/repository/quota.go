package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
)

// QuotaRepository implements QuotaStore using PostgreSQL via pgxpool.
type QuotaRepository struct {
	pool *pgxpool.Pool
}

// NewQuotaRepository creates a QuotaRepository.
func NewQuotaRepository(pool *pgxpool.Pool) *QuotaRepository {
	return &QuotaRepository{pool: pool}
}

// Get retrieves quotas for a tenant.
func (r *QuotaRepository) Get(ctx context.Context, tenantID string) (*provisioning.TenantQuota, error) {
	query := `
		SELECT tenant_id, max_topics, max_partitions, max_storage_bytes,
		       producer_byte_rate, consumer_byte_rate, max_connections, updated_at
		FROM tenant_quotas
		WHERE tenant_id = $1
	`

	quota := &provisioning.TenantQuota{}
	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&quota.TenantID,
		&quota.MaxTopics,
		&quota.MaxPartitions,
		&quota.MaxStorageBytes,
		&quota.ProducerByteRate,
		&quota.ConsumerByteRate,
		&quota.MaxConnections,
		&quota.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", provisioning.ErrQuotaNotFound, tenantID)
	}
	if err != nil {
		return nil, fmt.Errorf("query quota: %w", err)
	}

	return quota, nil
}

// Create creates quota record for a tenant.
func (r *QuotaRepository) Create(ctx context.Context, quota *provisioning.TenantQuota) error {
	query := `
		INSERT INTO tenant_quotas (tenant_id, max_topics, max_partitions, max_storage_bytes,
		                           producer_byte_rate, consumer_byte_rate, max_connections, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	now := time.Now()
	if quota.UpdatedAt.IsZero() {
		quota.UpdatedAt = now
	}

	_, err := r.pool.Exec(ctx, query,
		quota.TenantID,
		quota.MaxTopics,
		quota.MaxPartitions,
		quota.MaxStorageBytes,
		quota.ProducerByteRate,
		quota.ConsumerByteRate,
		quota.MaxConnections,
		quota.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert quota: %w", err)
	}

	return nil
}

// Update updates quota record for a tenant.
func (r *QuotaRepository) Update(ctx context.Context, quota *provisioning.TenantQuota) error {
	now := time.Now()
	query := `
		UPDATE tenant_quotas
		SET max_topics = $2, max_partitions = $3, max_storage_bytes = $4,
		    producer_byte_rate = $5, consumer_byte_rate = $6, max_connections = $7, updated_at = $8
		WHERE tenant_id = $1
	`

	result, err := r.pool.Exec(ctx, query,
		quota.TenantID,
		quota.MaxTopics,
		quota.MaxPartitions,
		quota.MaxStorageBytes,
		quota.ProducerByteRate,
		quota.ConsumerByteRate,
		quota.MaxConnections,
		now,
	)
	if err != nil {
		return fmt.Errorf("update quota: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrQuotaNotFound, quota.TenantID)
	}

	return nil
}

var _ provisioning.QuotaStore = (*QuotaRepository)(nil)
