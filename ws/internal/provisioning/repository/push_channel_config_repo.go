package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrChannelConfigNotFound indicates no push channel config exists for the tenant.
var ErrChannelConfigNotFound = errors.New("push channel config not found")

// PushChannelConfig defines which channels are eligible for push delivery for a tenant.
type PushChannelConfig struct {
	ID             int64
	TenantID       string
	Patterns       []string
	DefaultTTL     int
	DefaultUrgency string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ChannelConfigRepository manages push channel configuration per tenant.
type ChannelConfigRepository struct {
	pool *pgxpool.Pool
}

// NewChannelConfigRepository creates a ChannelConfigRepository.
func NewChannelConfigRepository(pool *pgxpool.Pool) *ChannelConfigRepository {
	return &ChannelConfigRepository{pool: pool}
}

// Upsert inserts or updates the push channel configuration for a tenant.
func (r *ChannelConfigRepository) Upsert(ctx context.Context, config *PushChannelConfig) error {
	now := time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = now
	}
	config.UpdatedAt = now

	query := `
		INSERT INTO push_channel_configs (tenant_id, patterns, default_ttl, default_urgency, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id) DO UPDATE SET
			patterns = $2,
			default_ttl = $3,
			default_urgency = $4,
			updated_at = $6
		RETURNING id
	`

	// patterns is a Postgres TEXT[] — pgx encodes []string natively; do NOT JSON-encode.
	err := r.pool.QueryRow(ctx, query,
		config.TenantID,
		config.Patterns,
		config.DefaultTTL,
		config.DefaultUrgency,
		config.CreatedAt,
		config.UpdatedAt,
	).Scan(&config.ID)
	if err != nil {
		return fmt.Errorf("upsert push channel config: %w", err)
	}

	return nil
}

// Get retrieves the push channel configuration for a tenant.
func (r *ChannelConfigRepository) Get(ctx context.Context, tenantID string) (*PushChannelConfig, error) {
	query := `
		SELECT id, tenant_id, patterns, default_ttl, default_urgency, created_at, updated_at
		FROM push_channel_configs
		WHERE tenant_id = $1
	`

	config := &PushChannelConfig{}

	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&config.ID,
		&config.TenantID,
		&config.Patterns, // TEXT[] → []string (pgx native)
		&config.DefaultTTL,
		&config.DefaultUrgency,
		&config.CreatedAt,
		&config.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("tenant %s: %w", tenantID, ErrChannelConfigNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query push channel config: %w", err)
	}

	return config, nil
}

// ListAll returns all push channel configurations.
func (r *ChannelConfigRepository) ListAll(ctx context.Context) ([]*PushChannelConfig, error) {
	query := `
		SELECT id, tenant_id, patterns, default_ttl, default_urgency, created_at, updated_at
		FROM push_channel_configs
		ORDER BY created_at DESC
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query push channel configs: %w", err)
	}
	defer rows.Close()

	configs := []*PushChannelConfig{}
	for rows.Next() {
		config := &PushChannelConfig{}

		err := rows.Scan(
			&config.ID,
			&config.TenantID,
			&config.Patterns, // TEXT[] → []string (pgx native)
			&config.DefaultTTL,
			&config.DefaultUrgency,
			&config.CreatedAt,
			&config.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan push channel config: %w", err)
		}

		configs = append(configs, config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push channel configs: %w", err)
	}

	return configs, nil
}

// Delete removes the push channel configuration for a tenant.
func (r *ChannelConfigRepository) Delete(ctx context.Context, tenantID string) error {
	query := `DELETE FROM push_channel_configs WHERE tenant_id = $1`

	result, err := r.pool.Exec(ctx, query, tenantID)
	if err != nil {
		return fmt.Errorf("delete push channel config: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("tenant %s: %w", tenantID, ErrChannelConfigNotFound)
	}

	return nil
}
