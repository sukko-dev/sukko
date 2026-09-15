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

// ErrCredentialAlreadyExists indicates a push credential already exists for the tenant+provider.
var ErrCredentialAlreadyExists = errors.New("push credential already exists")

// ErrCredentialNotFound indicates no push credential exists for the tenant+provider.
var ErrCredentialNotFound = errors.New("push credential not found")

// PushCredential represents encrypted push provider credentials for a tenant.
type PushCredential struct {
	ID             int64
	TenantID       string
	Provider       string
	CredentialData string
	CreatedAt      time.Time
}

// CredentialsRepository manages push credentials with optional at-rest encryption.
type CredentialsRepository struct {
	pool          *pgxpool.Pool
	encryptionKey []byte // nil when encryption is disabled (local dev without push)
}

// NewCredentialsRepository creates a CredentialsRepository. If encryptionKeyStr is empty,
// encryption is disabled — credentials are stored in plaintext (for local dev without push).
func NewCredentialsRepository(pool *pgxpool.Pool, encryptionKeyStr string) (*CredentialsRepository, error) {
	repo := &CredentialsRepository{pool: pool}

	if encryptionKeyStr != "" {
		key, err := crypto.ParseEncryptionKey(encryptionKeyStr)
		if err != nil {
			return nil, fmt.Errorf("parse credentials encryption key: %w", err)
		}
		repo.encryptionKey = key
	}

	return repo, nil
}

// Create inserts a new push credential. If encryption is enabled, credential_data
// is encrypted before storage.
func (r *CredentialsRepository) Create(ctx context.Context, cred *PushCredential) error {
	credData := cred.CredentialData
	if r.encryptionKey != nil {
		encrypted, err := crypto.EncryptCredential(credData, r.encryptionKey)
		if err != nil {
			return fmt.Errorf("encrypt credential data: %w", err)
		}
		credData = encrypted
	}

	now := time.Now()
	if cred.CreatedAt.IsZero() {
		cred.CreatedAt = now
	}

	query := `
		INSERT INTO push_credentials (tenant_id, provider, credential_data, created_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`

	err := r.pool.QueryRow(ctx, query,
		cred.TenantID,
		cred.Provider,
		credData,
		cred.CreatedAt,
	).Scan(&cred.ID)
	if err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("tenant %s provider %s: %w", cred.TenantID, cred.Provider, ErrCredentialAlreadyExists)
		}
		return fmt.Errorf("insert push credential: %w", err)
	}

	return nil
}

// Get retrieves a push credential by tenant ID and provider.
// If encryption is enabled, credential_data is decrypted before returning.
func (r *CredentialsRepository) Get(ctx context.Context, tenantID, provider string) (*PushCredential, error) {
	query := `
		SELECT id, tenant_id, provider, credential_data, created_at
		FROM push_credentials
		WHERE tenant_id = $1 AND provider = $2
	`

	cred := &PushCredential{}
	err := r.pool.QueryRow(ctx, query, tenantID, provider).Scan(
		&cred.ID,
		&cred.TenantID,
		&cred.Provider,
		&cred.CredentialData,
		&cred.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("tenant %s provider %s: %w", tenantID, provider, ErrCredentialNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query push credential: %w", err)
	}

	if r.encryptionKey != nil {
		decrypted, err := crypto.DecryptCredential(cred.CredentialData, r.encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("decrypt credential data: %w", err)
		}
		cred.CredentialData = decrypted
	}

	return cred, nil
}

// ListByTenant returns all push credentials for a tenant.
// If encryption is enabled, credential_data is decrypted for each row.
func (r *CredentialsRepository) ListByTenant(ctx context.Context, tenantID string) ([]*PushCredential, error) {
	query := `
		SELECT id, tenant_id, provider, credential_data, created_at
		FROM push_credentials
		WHERE tenant_id = $1
		ORDER BY created_at DESC
	`

	rows, err := r.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query push credentials: %w", err)
	}
	defer rows.Close()

	creds := []*PushCredential{}
	for rows.Next() {
		cred := &PushCredential{}
		err := rows.Scan(
			&cred.ID,
			&cred.TenantID,
			&cred.Provider,
			&cred.CredentialData,
			&cred.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan push credential: %w", err)
		}

		if r.encryptionKey != nil {
			decrypted, err := crypto.DecryptCredential(cred.CredentialData, r.encryptionKey)
			if err != nil {
				return nil, fmt.Errorf("decrypt credential data: %w", err)
			}
			cred.CredentialData = decrypted
		}

		creds = append(creds, cred)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push credentials: %w", err)
	}

	return creds, nil
}

// ListAll returns all push credentials.
// If encryption is enabled, credential_data is decrypted for each row.
func (r *CredentialsRepository) ListAll(ctx context.Context) ([]*PushCredential, error) {
	query := `
		SELECT id, tenant_id, provider, credential_data, created_at
		FROM push_credentials
		ORDER BY created_at DESC
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query all push credentials: %w", err)
	}
	defer rows.Close()

	creds := []*PushCredential{}
	for rows.Next() {
		cred := &PushCredential{}
		err := rows.Scan(
			&cred.ID,
			&cred.TenantID,
			&cred.Provider,
			&cred.CredentialData,
			&cred.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan push credential: %w", err)
		}

		if r.encryptionKey != nil {
			decrypted, err := crypto.DecryptCredential(cred.CredentialData, r.encryptionKey)
			if err != nil {
				return nil, fmt.Errorf("decrypt credential data: %w", err)
			}
			cred.CredentialData = decrypted
		}

		creds = append(creds, cred)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push credentials: %w", err)
	}

	return creds, nil
}

// Update updates the credential_data for an existing push credential identified
// by tenant ID and provider. If encryption is enabled, credential_data is
// encrypted before storage.
func (r *CredentialsRepository) Update(ctx context.Context, cred *PushCredential) error {
	credData := cred.CredentialData
	if r.encryptionKey != nil {
		encrypted, err := crypto.EncryptCredential(credData, r.encryptionKey)
		if err != nil {
			return fmt.Errorf("encrypt credential data: %w", err)
		}
		credData = encrypted
	}

	query := `
		UPDATE push_credentials
		SET credential_data = $1
		WHERE tenant_id = $2 AND provider = $3
	`

	result, err := r.pool.Exec(ctx, query, credData, cred.TenantID, cred.Provider)
	if err != nil {
		return fmt.Errorf("update push credential: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("tenant %s provider %s: %w", cred.TenantID, cred.Provider, ErrCredentialNotFound)
	}

	return nil
}

// Delete removes a push credential by tenant ID and provider.
func (r *CredentialsRepository) Delete(ctx context.Context, tenantID, provider string) error {
	query := `DELETE FROM push_credentials WHERE tenant_id = $1 AND provider = $2`

	result, err := r.pool.Exec(ctx, query, tenantID, provider)
	if err != nil {
		return fmt.Errorf("delete push credential: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("tenant %s provider %s: %w", tenantID, provider, ErrCredentialNotFound)
	}

	return nil
}
