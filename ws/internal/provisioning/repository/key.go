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

// KeyRepository implements KeyStore using PostgreSQL via pgxpool.
type KeyRepository struct {
	pool *pgxpool.Pool
}

// NewKeyRepository creates a KeyRepository.
func NewKeyRepository(pool *pgxpool.Pool) *KeyRepository {
	return &KeyRepository{pool: pool}
}

// Create creates a new key record.
func (r *KeyRepository) Create(ctx context.Context, key *provisioning.TenantKey) error {
	query := `
		INSERT INTO tenant_keys (key_id, tenant_id, algorithm, public_key, is_active, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	now := time.Now()
	if key.CreatedAt.IsZero() {
		key.CreatedAt = now
	}
	key.IsActive = true

	_, err := r.pool.Exec(ctx, query,
		key.KeyID,
		key.TenantID,
		key.Algorithm,
		key.PublicKey,
		key.IsActive,
		key.CreatedAt,
		key.ExpiresAt,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: %s", provisioning.ErrKeyAlreadyExists, key.KeyID)
		}
		return fmt.Errorf("insert key: %w", err)
	}

	return nil
}

// Get retrieves a key by ID.
func (r *KeyRepository) Get(ctx context.Context, keyID string) (*provisioning.TenantKey, error) {
	query := `
		SELECT key_id, tenant_id, algorithm, public_key, is_active, created_at, expires_at, revoked_at
		FROM tenant_keys
		WHERE key_id = $1
	`

	key := &provisioning.TenantKey{}

	err := r.pool.QueryRow(ctx, query, keyID).Scan(
		&key.KeyID,
		&key.TenantID,
		&key.Algorithm,
		&key.PublicKey,
		&key.IsActive,
		&key.CreatedAt,
		&key.ExpiresAt,
		&key.RevokedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", provisioning.ErrKeyNotFound, keyID)
	}
	if err != nil {
		return nil, fmt.Errorf("query key: %w", err)
	}

	return key, nil
}

// ListByTenant returns keys for a tenant with pagination.
func (r *KeyRepository) ListByTenant(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.TenantKey, int, error) {
	// Count total
	var total int
	countQuery := `SELECT COUNT(*) FROM tenant_keys WHERE tenant_id = $1`
	if err := r.pool.QueryRow(ctx, countQuery, tenantID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count keys: %w", err)
	}

	query := `
		SELECT key_id, tenant_id, algorithm, public_key, is_active, created_at, expires_at, revoked_at
		FROM tenant_keys
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.pool.Query(ctx, query, tenantID, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("query keys: %w", err)
	}
	defer rows.Close()

	keys := []*provisioning.TenantKey{}
	for rows.Next() {
		key := &provisioning.TenantKey{}

		err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Algorithm,
			&key.PublicKey,
			&key.IsActive,
			&key.CreatedAt,
			&key.ExpiresAt,
			&key.RevokedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("scan key: %w", err)
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate keys: %w", err)
	}

	return keys, total, nil
}

// Revoke revokes a key by setting its revoked_at timestamp.
func (r *KeyRepository) Revoke(ctx context.Context, keyID string) error {
	query := `
		UPDATE tenant_keys
		SET is_active = false, revoked_at = $2
		WHERE key_id = $1 AND is_active = true
	`

	now := time.Now()
	result, err := r.pool.Exec(ctx, query, keyID, now)
	if err != nil {
		return fmt.Errorf("revoke key: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrKeyNotFound, keyID)
	}

	return nil
}

// RevokeAllForTenant revokes all active keys for a tenant.
func (r *KeyRepository) RevokeAllForTenant(ctx context.Context, tenantID string) error {
	query := `
		UPDATE tenant_keys
		SET is_active = false, revoked_at = $2
		WHERE tenant_id = $1 AND is_active = true
	`

	now := time.Now()
	_, err := r.pool.Exec(ctx, query, tenantID, now)
	if err != nil {
		return fmt.Errorf("revoke keys for tenant: %w", err)
	}

	return nil
}

// GetActiveKeys returns all active, non-expired, non-revoked keys.
// Used by WS Gateway to refresh its key cache.
func (r *KeyRepository) GetActiveKeys(ctx context.Context) ([]*provisioning.TenantKey, error) {
	query := `
		SELECT key_id, tenant_id, algorithm, public_key, is_active, created_at, expires_at, revoked_at
		FROM tenant_keys
		WHERE is_active = true
		  AND revoked_at IS NULL
		  AND (expires_at IS NULL OR expires_at > $1)
	`

	now := time.Now()
	rows, err := r.pool.Query(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("query active keys: %w", err)
	}
	defer rows.Close()

	keys := []*provisioning.TenantKey{}
	for rows.Next() {
		key := &provisioning.TenantKey{}

		err := rows.Scan(
			&key.KeyID,
			&key.TenantID,
			&key.Algorithm,
			&key.PublicKey,
			&key.IsActive,
			&key.CreatedAt,
			&key.ExpiresAt,
			&key.RevokedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate keys: %w", err)
	}

	return keys, nil
}

var _ provisioning.KeyStore = (*KeyRepository)(nil)
