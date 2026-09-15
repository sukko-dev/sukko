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

// APIKeyStore implements provisioning.APIKeyStore using PostgreSQL via pgxpool.
type APIKeyStore struct {
	pool *pgxpool.Pool
}

// NewAPIKeyStore creates an APIKeyStore.
func NewAPIKeyStore(pool *pgxpool.Pool) *APIKeyStore {
	return &APIKeyStore{pool: pool}
}

// Create creates a new API key record.
func (r *APIKeyStore) Create(ctx context.Context, key *provisioning.APIKey) error {
	query := `
		INSERT INTO api_keys (key_id, tenant_id, name, is_active, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	now := time.Now()
	if key.CreatedAt.IsZero() {
		key.CreatedAt = now
	}
	key.IsActive = true

	_, err := r.pool.Exec(ctx, query,
		key.KeyID,
		key.TenantID,
		key.Name,
		key.IsActive,
		key.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert api key: %w", err)
	}

	return nil
}

// Get retrieves an API key by key ID.
func (r *APIKeyStore) Get(ctx context.Context, keyID string) (*provisioning.APIKey, error) {
	query := `
		SELECT key_id, tenant_id, name, is_active, created_at, revoked_at
		FROM api_keys
		WHERE key_id = $1
	`

	key := &provisioning.APIKey{}

	err := r.pool.QueryRow(ctx, query, keyID).Scan(
		&key.KeyID,
		&key.TenantID,
		&key.Name,
		&key.IsActive,
		&key.CreatedAt,
		&key.RevokedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, provisioning.ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query api key: %w", err)
	}

	return key, nil
}

// ListByTenant returns API keys for a tenant with pagination.
func (r *APIKeyStore) ListByTenant(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.APIKey, int, error) {
	// Count total
	var total int
	countQuery := `SELECT COUNT(*) FROM api_keys WHERE tenant_id = $1`
	if err := r.pool.QueryRow(ctx, countQuery, tenantID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count api keys: %w", err)
	}

	query := `
		SELECT key_id, tenant_id, name, is_active, created_at, revoked_at
		FROM api_keys
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.pool.Query(ctx, query, tenantID, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("query api keys: %w", err)
	}
	defer rows.Close()

	keys := []*provisioning.APIKey{}
	for rows.Next() {
		key := &provisioning.APIKey{}

		err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Name,
			&key.IsActive,
			&key.CreatedAt,
			&key.RevokedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan api key: %w", err)
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate api keys: %w", err)
	}

	return keys, total, nil
}

// Revoke revokes an API key by setting its revoked_at timestamp.
func (r *APIKeyStore) Revoke(ctx context.Context, keyID string) error {
	query := `
		UPDATE api_keys
		SET is_active = false, revoked_at = $2
		WHERE key_id = $1 AND is_active = true
	`

	now := time.Now()
	result, err := r.pool.Exec(ctx, query, keyID, now)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}

	if result.RowsAffected() == 0 {
		return provisioning.ErrAPIKeyNotFound
	}

	return nil
}

// GetActiveAPIKeys returns all active, non-revoked API keys.
// Used by the gateway to populate its in-memory lookup map.
func (r *APIKeyStore) GetActiveAPIKeys(ctx context.Context) ([]*provisioning.APIKey, error) {
	query := `
		SELECT key_id, tenant_id, name, is_active, created_at, revoked_at
		FROM api_keys
		WHERE is_active = true
		  AND revoked_at IS NULL
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query active api keys: %w", err)
	}
	defer rows.Close()

	keys := []*provisioning.APIKey{}
	for rows.Next() {
		key := &provisioning.APIKey{}

		err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Name,
			&key.IsActive,
			&key.CreatedAt,
			&key.RevokedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan api key: %w", err)
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api keys: %w", err)
	}

	return keys, nil
}

var _ provisioning.APIKeyStore = (*APIKeyStore)(nil)
