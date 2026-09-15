package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sukko-dev/sukko/internal/provisioning"
)

// isDuplicateKeyError detects unique constraint violations from PostgreSQL.
func isDuplicateKeyError(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == pgerrcode.UniqueViolation
}

// defaultListLimit is the fallback page size when the caller provides no limit.
const defaultListLimit = 50

// TenantRepository implements TenantStore using PostgreSQL via pgxpool.
type TenantRepository struct {
	pool *pgxpool.Pool
}

// NewTenantRepository creates a TenantRepository.
func NewTenantRepository(pool *pgxpool.Pool) *TenantRepository {
	return &TenantRepository{pool: pool}
}

// tenantCols is the ordered SELECT column list shared by all tenant queries.
// scanTenant MUST scan in this exact column order.
const tenantCols = `
	id, slug, name, status, consumer_type, metadata,
	created_at, updated_at, suspended_at, deprovision_at, deleted_at,
	previous_slug, slug_rename_state, slug_renamed_at
`

// scanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query iteration).
type scanner interface {
	Scan(dest ...any) error
}

// scanTenant scans a row (columns in tenantCols order) into a Tenant.
// Nullable columns (previous_slug, slug_rename_state, slug_renamed_at) are handled
// by scanning into pointer types; nil values produce zero/empty Go fields.
func scanTenant(row scanner, tenant *provisioning.Tenant) error {
	var previousSlug *string
	var slugRenameState *string
	if err := row.Scan(
		&tenant.ID,
		&tenant.Slug,
		&tenant.Name,
		&tenant.Status,
		&tenant.ConsumerType,
		&tenant.Metadata,
		&tenant.CreatedAt,
		&tenant.UpdatedAt,
		&tenant.SuspendedAt,
		&tenant.DeprovisionAt,
		&tenant.DeletedAt,
		&previousSlug,
		&slugRenameState,
		&tenant.SlugRenamedAt,
	); err != nil {
		return fmt.Errorf("scan tenant row: %w", err)
	}
	if previousSlug != nil {
		tenant.PreviousSlug = *previousSlug
	}
	if slugRenameState != nil {
		tenant.SlugRenameState = provisioning.SlugRenameState(*slugRenameState)
	}
	return nil
}

// Ping verifies database connectivity.
func (r *TenantRepository) Ping(ctx context.Context) error {
	if err := r.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	return nil
}

// Create creates a new tenant record.
// The UUID primary key is DB-generated; it is scanned back into tenant.ID via RETURNING.
func (r *TenantRepository) Create(ctx context.Context, tenant *provisioning.Tenant) error {
	query := `
		INSERT INTO tenants (slug, name, status, consumer_type, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`

	now := time.Now()
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = now
	}
	if tenant.UpdatedAt.IsZero() {
		tenant.UpdatedAt = now
	}
	if tenant.Status == "" {
		tenant.Status = provisioning.StatusActive
	}
	if tenant.ConsumerType == "" {
		tenant.ConsumerType = provisioning.ConsumerShared
	}
	if tenant.Metadata == nil {
		tenant.Metadata = make(provisioning.Metadata)
	}

	err := r.pool.QueryRow(ctx, query,
		tenant.Slug,
		tenant.Name,
		tenant.Status,
		tenant.ConsumerType,
		tenant.Metadata,
		tenant.CreatedAt,
		tenant.UpdatedAt,
	).Scan(&tenant.ID)
	if err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: %s", provisioning.ErrSlugAlreadyTaken, tenant.Slug)
		}
		return fmt.Errorf("insert tenant: %w", err)
	}

	return nil
}

// GetBySlug retrieves a tenant by slug.
func (r *TenantRepository) GetBySlug(ctx context.Context, slug string) (*provisioning.Tenant, error) {
	query := `SELECT` + tenantCols + `FROM tenants WHERE slug = $1 AND deleted_at IS NULL`

	tenant := &provisioning.Tenant{}
	if err := scanTenant(r.pool.QueryRow(ctx, query, slug), tenant); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, slug)
		}
		return nil, fmt.Errorf("query tenant: %w", err)
	}

	return tenant, nil
}

// GetIDBySlugWithGrace resolves a tenant slug to its stable UUID, accepting the
// current slug OR a previous slug that is still within the rename hold window.
// It is the grace-aware slug->UUID lookup backing the provisioning tenant-binding
// resolver, so old-slug JWTs continue to validate during the hold period exactly
// as RequireTenant's provisioning-API grace check allows. Returns
// provisioning.ErrTenantNotFound when no active tenant matches.
//
// The previous-slug branch mirrors RequireTenant's grace predicate
// (status=active, slug_rename_state='complete', slug_renamed_at within hold).
func (r *TenantRepository) GetIDBySlugWithGrace(ctx context.Context, slug string, holdPeriod time.Duration) (string, error) {
	query := `
		SELECT id FROM tenants
		WHERE deleted_at IS NULL
		  AND (
		    slug = $1
		    OR (
		      previous_slug = $1
		      AND status = 'active'
		      AND slug_rename_state = 'complete'
		      AND slug_renamed_at IS NOT NULL
		      AND slug_renamed_at > $2
		    )
		  )`

	var id string
	if err := r.pool.QueryRow(ctx, query, slug, time.Now().Add(-holdPeriod)).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, slug)
		}
		return "", fmt.Errorf("query tenant id by slug: %w", err)
	}
	return id, nil
}

// Update updates an existing tenant's mutable metadata fields.
func (r *TenantRepository) Update(ctx context.Context, tenant *provisioning.Tenant) error {
	query := `
		UPDATE tenants
		SET name = $2, consumer_type = $3, metadata = $4, updated_at = $5
		WHERE id = $1 AND status != 'deleted'
	`

	now := time.Now()
	result, err := r.pool.Exec(ctx, query,
		tenant.ID,
		tenant.Name,
		tenant.ConsumerType,
		tenant.Metadata,
		now,
	)
	if err != nil {
		return fmt.Errorf("update tenant: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, tenant.ID)
	}

	return nil
}

// UpdateSlug commits a slug rename and returns the updated tenant via RETURNING.
// The DB atomically records: new slug, previous_slug=old_slug, slug_rename_state='complete',
// slug_renamed_at=NOW(). Returns ErrSlugAlreadyTaken when the unique constraint fires
// (23505 TOCTOU race between the "slug available" check and this UPDATE).
func (r *TenantRepository) UpdateSlug(ctx context.Context, tenantUUID, newSlug string) (*provisioning.Tenant, error) {
	query := `
		UPDATE tenants
		SET slug             = $2,
		    slug_rename_state = 'complete',
		    previous_slug    = slug,
		    slug_renamed_at  = NOW(),
		    updated_at       = NOW()
		WHERE id = $1
		RETURNING ` + tenantCols

	tenant := &provisioning.Tenant{}
	if err := scanTenant(r.pool.QueryRow(ctx, query, tenantUUID, newSlug), tenant); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, tenantUUID)
		}
		if isDuplicateKeyError(err) {
			return nil, provisioning.ErrSlugAlreadyTaken
		}
		return nil, fmt.Errorf("update slug: %w", err)
	}

	return tenant, nil
}

// SetRenameState sets the slug rename state with a CAS guard.
// The UPDATE fires only when slug_rename_state IS NULL OR slug_rename_state='complete'.
// Returns ErrCASFailed when zero rows are affected (concurrent rename in progress).
func (r *TenantRepository) SetRenameState(ctx context.Context, tenantUUID string, state provisioning.SlugRenameState, previousSlug string) error {
	query := `
		UPDATE tenants
		SET slug_rename_state = $2,
		    previous_slug     = $3,
		    updated_at        = NOW()
		WHERE id = $1
		  AND (slug_rename_state IS NULL OR slug_rename_state = 'complete')
	`

	result, err := r.pool.Exec(ctx, query, tenantUUID, string(state), previousSlug)
	if err != nil {
		return fmt.Errorf("set rename state: %w", err)
	}

	if result.RowsAffected() == 0 {
		return provisioning.ErrCASFailed
	}

	return nil
}

// ClearRenameState unconditionally clears slug_rename_state, previous_slug, and slug_renamed_at.
// Clearing slug_renamed_at ensures a future rename finds a clean hold-period baseline.
func (r *TenantRepository) ClearRenameState(ctx context.Context, tenantUUID string) error {
	query := `
		UPDATE tenants
		SET slug_rename_state = NULL,
		    previous_slug     = NULL,
		    slug_renamed_at   = NULL,
		    updated_at        = NOW()
		WHERE id = $1
	`

	if _, err := r.pool.Exec(ctx, query, tenantUUID); err != nil {
		return fmt.Errorf("clear rename state: %w", err)
	}

	return nil
}

// ListPendingRenames returns tenants with slug_rename_state='pending' OR
// (slug_rename_state='complete' AND previous_slug IS NOT NULL).
// Used by the startup scan to detect saga residue and re-emit config events.
func (r *TenantRepository) ListPendingRenames(ctx context.Context) ([]*provisioning.Tenant, error) {
	query := `
		SELECT ` + tenantCols + `
		FROM tenants
		WHERE slug_rename_state = 'pending'
		   OR (slug_rename_state = 'complete' AND previous_slug IS NOT NULL)
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list pending renames: %w", err)
	}
	defer rows.Close()

	var tenants []*provisioning.Tenant
	for rows.Next() {
		tenant := &provisioning.Tenant{}
		if err := scanTenant(rows, tenant); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenant)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending renames: %w", err)
	}

	return tenants, nil
}

// List returns tenants matching the given options.
func (r *TenantRepository) List(ctx context.Context, opts provisioning.ListOptions) ([]*provisioning.Tenant, int, error) {
	whereClause := "WHERE status != 'deleted'"
	args := []any{}
	argNum := 1

	if opts.Status != nil {
		whereClause += fmt.Sprintf(" AND status = $%d", argNum)
		args = append(args, *opts.Status)
		argNum++
	}

	countQuery := "SELECT COUNT(*) FROM tenants " + whereClause
	var total int
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count tenants: %w", err)
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	offset := max(opts.Offset, 0)

	query := fmt.Sprintf(`
		SELECT `+tenantCols+`
		FROM tenants
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argNum, argNum+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query tenants: %w", err)
	}
	defer rows.Close()

	tenants := []*provisioning.Tenant{}
	for rows.Next() {
		tenant := &provisioning.Tenant{}
		if err := scanTenant(rows, tenant); err != nil {
			return nil, 0, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenant)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate tenants: %w", err)
	}

	return tenants, total, nil
}

// UpdateStatus updates a tenant's status. tenantID MUST be the tenant's UUID primary key.
func (r *TenantRepository) UpdateStatus(ctx context.Context, tenantID string, status provisioning.TenantStatus) error {
	var query string
	var args []any

	now := time.Now()

	switch status {
	case provisioning.StatusSuspended:
		query = `
			UPDATE tenants
			SET status = $2, suspended_at = $3, updated_at = $3
			WHERE id = $1 AND status = 'active'
		`
		args = []any{tenantID, status, now}
	case provisioning.StatusActive:
		query = `
			UPDATE tenants
			SET status = $2, suspended_at = NULL, updated_at = $3
			WHERE id = $1 AND status IN ('suspended')
		`
		args = []any{tenantID, status, now}
	case provisioning.StatusDeprovisioning:
		query = `
			UPDATE tenants
			SET status = $2, updated_at = $3
			WHERE id = $1 AND status IN ('active', 'suspended')
		`
		args = []any{tenantID, status, now}
	case provisioning.StatusDeleted:
		// Valid sources: active/suspended (force-delete — no grace period) and
		// deprovisioning (grace-period sweeper + force-delete of a deprovisioning
		// tenant). Only deleted→deleted is excluded: deleting an already-deleted
		// tenant must stay a not-found error (idempotency is handled above this
		// layer; a second delete would double-decrement the active-tenants gauge).
		query = `
			UPDATE tenants
			SET status = $2, deleted_at = $3, updated_at = $3
			WHERE id = $1 AND status IN ('active', 'suspended', 'deprovisioning')
		`
		args = []any{tenantID, status, now}
	default:
		return fmt.Errorf("invalid status transition to: %s", status)
	}

	result, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, tenantID)
	}

	return nil
}

// SetDeprovisionAt sets the deprovision deadline for a tenant. tenantID MUST be the UUID.
func (r *TenantRepository) SetDeprovisionAt(ctx context.Context, tenantID string, deprovisionAt *provisioning.Time) error {
	query := `
		UPDATE tenants
		SET deprovision_at = $2, updated_at = $3
		WHERE id = $1 AND status = 'deprovisioning'
	`

	now := time.Now()
	result, err := r.pool.Exec(ctx, query, tenantID, deprovisionAt, now)
	if err != nil {
		return fmt.Errorf("set deprovision_at: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", provisioning.ErrTenantNotFound, tenantID)
	}

	return nil
}

// GetTenantsForDeletion returns tenants past their deprovision deadline.
func (r *TenantRepository) GetTenantsForDeletion(ctx context.Context) ([]*provisioning.Tenant, error) {
	query := `
		SELECT ` + tenantCols + `
		FROM tenants
		WHERE status = 'deprovisioning'
		  AND deprovision_at IS NOT NULL
		  AND deprovision_at <= $1
	`

	now := time.Now()
	rows, err := r.pool.Query(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("query tenants for deletion: %w", err)
	}
	defer rows.Close()

	tenants := []*provisioning.Tenant{}
	for rows.Next() {
		tenant := &provisioning.Tenant{}
		if err := scanTenant(rows, tenant); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		tenants = append(tenants, tenant)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}

	return tenants, nil
}

// Count returns the number of active (non-deleted) tenants.
func (r *TenantRepository) Count(ctx context.Context) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tenants WHERE status != $1", provisioning.StatusDeleted).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count tenants: %w", err)
	}
	return count, nil
}

// CountActive returns the number of strictly active (status='active') tenants.
// Used to initialize the provisioning_active_tenants Prometheus gauge at startup.
func (r *TenantRepository) CountActive(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tenants WHERE status = $1", provisioning.StatusActive).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active tenants: %w", err)
	}
	return count, nil
}

var _ provisioning.TenantStore = (*TenantRepository)(nil)
