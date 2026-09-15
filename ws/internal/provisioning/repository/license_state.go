package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/shared/crypto"
)

// LicenseStateRepository manages the single-row license_state table.
// The license key is stored encrypted with AES-256-GCM.
type LicenseStateRepository struct {
	pool          *pgxpool.Pool
	encryptionKey []byte // 32-byte AES-256 key
}

// NewLicenseStateRepository creates a LicenseStateRepository.
// The encryptionKey must be a valid 32-byte AES-256 key (parsed via parseEncryptionKey).
func NewLicenseStateRepository(pool *pgxpool.Pool, encryptionKey []byte) *LicenseStateRepository {
	return &LicenseStateRepository{
		pool:          pool,
		encryptionKey: encryptionKey,
	}
}

// Upsert encrypts and stores the license key. Uses INSERT ... ON CONFLICT DO UPDATE
// since the table has a single row (id=1).
func (r *LicenseStateRepository) Upsert(ctx context.Context, licenseKey, edition, org string, expiresAt *time.Time) error {
	encrypted, err := crypto.EncryptCredential(licenseKey, r.encryptionKey)
	if err != nil {
		return fmt.Errorf("encrypt license key: %w", err)
	}

	query := `
		INSERT INTO license_state (id, encrypted_key, edition, org, expires_at, updated_at)
		VALUES (1, $1, $2, $3, $4, NOW())
		ON CONFLICT (id) DO UPDATE SET
			encrypted_key = EXCLUDED.encrypted_key,
			edition = EXCLUDED.edition,
			org = EXCLUDED.org,
			expires_at = EXCLUDED.expires_at,
			updated_at = NOW()
	`

	_, err = r.pool.Exec(ctx, query, encrypted, edition, org, expiresAt)
	if err != nil {
		return fmt.Errorf("upsert license state: %w", err)
	}

	return nil
}

// Load retrieves and decrypts the stored license key.
// Returns empty string if no license is stored (first deploy).
func (r *LicenseStateRepository) Load(ctx context.Context) (string, error) {
	query := `SELECT encrypted_key FROM license_state WHERE id = 1`

	var encrypted string
	err := r.pool.QueryRow(ctx, query).Scan(&encrypted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil // no stored license — first deploy, use env var
		}
		return "", fmt.Errorf("load license state: %w", err)
	}

	decrypted, err := crypto.DecryptCredential(encrypted, r.encryptionKey)
	if err != nil {
		return "", fmt.Errorf("decrypt license key: %w", err)
	}

	return decrypted, nil
}
