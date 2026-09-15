package repository

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AdminKey represents an admin public key stored in the database.
type AdminKey struct {
	ID           int
	KeyID        string
	Name         string
	Algorithm    string
	PublicKey    string
	RegisteredBy string
	CreatedAt    time.Time
	RevokedAt    *time.Time
}

// AdminKeyRepository implements admin key storage using PostgreSQL via pgxpool.
type AdminKeyRepository struct {
	pool *pgxpool.Pool
}

// NewAdminKeyRepository creates an AdminKeyRepository.
func NewAdminKeyRepository(pool *pgxpool.Pool) *AdminKeyRepository {
	return &AdminKeyRepository{pool: pool}
}

// Create inserts a new admin key. The key ID is auto-generated with ak_ prefix.
func (r *AdminKeyRepository) Create(ctx context.Context, key *AdminKey) error {
	if key.KeyID == "" {
		id, err := generateAdminKeyID()
		if err != nil {
			return fmt.Errorf("generate key ID: %w", err)
		}
		key.KeyID = id
	}

	query := `
		INSERT INTO admin_keys (key_id, name, algorithm, public_key, registered_by, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	now := time.Now()
	if key.CreatedAt.IsZero() {
		key.CreatedAt = now
	}

	_, err := r.pool.Exec(ctx, query,
		key.KeyID,
		key.Name,
		key.Algorithm,
		key.PublicKey,
		key.RegisteredBy,
		key.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert admin key: %w", err)
	}

	return nil
}

// GetByKeyID retrieves an admin key by its key ID.
func (r *AdminKeyRepository) GetByKeyID(ctx context.Context, keyID string) (*AdminKey, error) {
	query := `
		SELECT id, key_id, name, algorithm, public_key, registered_by, created_at, revoked_at
		FROM admin_keys
		WHERE key_id = $1
	`

	var key AdminKey
	err := r.pool.QueryRow(ctx, query, keyID).Scan(
		&key.ID, &key.KeyID, &key.Name, &key.Algorithm,
		&key.PublicKey, &key.RegisteredBy, &key.CreatedAt, &key.RevokedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("admin key not found: %s", keyID)
		}
		return nil, fmt.Errorf("get admin key: %w", err)
	}

	return &key, nil
}

// ListActive returns all active (non-revoked) admin keys.
func (r *AdminKeyRepository) ListActive(ctx context.Context) ([]*AdminKey, error) {
	query := `
		SELECT id, key_id, name, algorithm, public_key, registered_by, created_at, revoked_at
		FROM admin_keys
		WHERE revoked_at IS NULL
		ORDER BY created_at ASC
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list active admin keys: %w", err)
	}
	defer rows.Close()

	var keys []*AdminKey
	for rows.Next() {
		var key AdminKey
		if err := rows.Scan(
			&key.ID, &key.KeyID, &key.Name, &key.Algorithm,
			&key.PublicKey, &key.RegisteredBy, &key.CreatedAt, &key.RevokedAt,
		); err != nil {
			return nil, fmt.Errorf("scan admin key: %w", err)
		}
		keys = append(keys, &key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin keys: %w", err)
	}
	return keys, nil
}

// Revoke marks an admin key as revoked by setting revoked_at.
func (r *AdminKeyRepository) Revoke(ctx context.Context, keyID string) error {
	query := `
		UPDATE admin_keys
		SET revoked_at = NOW()
		WHERE key_id = $1 AND revoked_at IS NULL
	`

	tag, err := r.pool.Exec(ctx, query, keyID)
	if err != nil {
		return fmt.Errorf("revoke admin key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("admin key not found or already revoked: %s", keyID)
	}

	return nil
}

// CountActive returns the number of active (non-revoked) admin keys.
func (r *AdminKeyRepository) CountActive(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM admin_keys WHERE revoked_at IS NULL`

	var count int
	if err := r.pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active admin keys: %w", err)
	}

	return count, nil
}

// generateAdminKeyID creates a unique key ID with ak_ prefix + 12-char base32 random.
func generateAdminKeyID() (string, error) {
	b := make([]byte, 8) // 8 bytes → 13 base32 chars, we take 12
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random bytes: %w", err)
	}
	encoded := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
	if len(encoded) > 12 {
		encoded = encoded[:12]
	}
	return "ak_" + encoded, nil
}
