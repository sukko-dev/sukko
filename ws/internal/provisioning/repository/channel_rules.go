package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// ChannelRulesRepository implements ChannelRulesStore using PostgreSQL via pgxpool.
type ChannelRulesRepository struct {
	pool *pgxpool.Pool
}

// NewChannelRulesRepository creates a ChannelRulesRepository.
func NewChannelRulesRepository(pool *pgxpool.Pool) *ChannelRulesRepository {
	return &ChannelRulesRepository{pool: pool}
}

// Create creates channel rules for a tenant.
func (r *ChannelRulesRepository) Create(ctx context.Context, tenantID string, rules *types.ChannelRules) error {
	rulesJSON, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("marshal rules: %w", err)
	}

	now := time.Now()
	query := `
		INSERT INTO tenant_channel_rules (tenant_id, rules, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
	`

	_, err = r.pool.Exec(ctx, query, tenantID, rulesJSON, now)
	if err != nil {
		return fmt.Errorf("create channel rules: %w", err)
	}

	return nil
}

// Get retrieves channel rules by tenant ID.
func (r *ChannelRulesRepository) Get(ctx context.Context, tenantID string) (*types.TenantChannelRules, error) {
	query := `
		SELECT tenant_id, rules, created_at, updated_at
		FROM tenant_channel_rules
		WHERE tenant_id = $1
	`

	tcr := &types.TenantChannelRules{}
	var rulesJSON []byte

	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&tcr.TenantID,
		&rulesJSON,
		&tcr.CreatedAt,
		&tcr.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.ErrChannelRulesNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query channel rules: %w", err)
	}

	if err := json.Unmarshal(rulesJSON, &tcr.Rules); err != nil {
		return nil, fmt.Errorf("unmarshal rules: %w", err)
	}

	return tcr, nil
}

// GetRules retrieves just the channel rules (not the wrapper) by tenant ID.
// This is a convenience method for the common use case.
func (r *ChannelRulesRepository) GetRules(ctx context.Context, tenantID string) (*types.ChannelRules, error) {
	tcr, err := r.Get(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return &tcr.Rules, nil
}

// Update updates channel rules for a tenant (upsert).
func (r *ChannelRulesRepository) Update(ctx context.Context, tenantID string, rules *types.ChannelRules) error {
	rulesJSON, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("marshal rules: %w", err)
	}

	// Use upsert (INSERT ON CONFLICT) to handle both create and update
	now := time.Now()
	query := `
		INSERT INTO tenant_channel_rules (tenant_id, rules, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (tenant_id)
		DO UPDATE SET rules = $2, updated_at = $3
	`

	_, err = r.pool.Exec(ctx, query, tenantID, rulesJSON, now)
	if err != nil {
		return fmt.Errorf("update channel rules: %w", err)
	}

	return nil
}

// Delete deletes channel rules for a tenant.
func (r *ChannelRulesRepository) Delete(ctx context.Context, tenantID string) error {
	query := `DELETE FROM tenant_channel_rules WHERE tenant_id = $1`

	result, err := r.pool.Exec(ctx, query, tenantID)
	if err != nil {
		return fmt.Errorf("delete channel rules: %w", err)
	}

	if result.RowsAffected() == 0 {
		return types.ErrChannelRulesNotFound
	}

	return nil
}

// List returns all channel rules (used by gateway to build cache).
func (r *ChannelRulesRepository) List(ctx context.Context) ([]*types.TenantChannelRules, error) {
	query := `
		SELECT tenant_id, rules, created_at, updated_at
		FROM tenant_channel_rules
		ORDER BY created_at ASC
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query channel rules: %w", err)
	}
	defer rows.Close()

	results := []*types.TenantChannelRules{}
	for rows.Next() {
		tcr := &types.TenantChannelRules{}
		var rulesJSON []byte

		err := rows.Scan(
			&tcr.TenantID,
			&rulesJSON,
			&tcr.CreatedAt,
			&tcr.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan channel rules: %w", err)
		}

		if err := json.Unmarshal(rulesJSON, &tcr.Rules); err != nil {
			return nil, fmt.Errorf("unmarshal rules for tenant %s: %w", tcr.TenantID, err)
		}

		results = append(results, tcr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel rules: %w", err)
	}

	return results, nil
}

var _ provisioning.ChannelRulesStore = (*ChannelRulesRepository)(nil)
