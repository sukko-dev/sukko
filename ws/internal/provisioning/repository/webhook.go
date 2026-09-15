package repository

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// WebhookRepository implements provisioning.WebhookStore using PostgreSQL.
type WebhookRepository struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// NewWebhookRepository creates a WebhookRepository.
func NewWebhookRepository(pool *pgxpool.Pool, logger zerolog.Logger) *WebhookRepository {
	return &WebhookRepository{
		pool:   pool,
		logger: logger.With().Str("component", "webhook_repository").Logger(),
	}
}

// Create inserts a new webhook registration. w.ID must be pre-set by the caller.
func (r *WebhookRepository) Create(ctx context.Context, w *provisioning.Webhook) error {
	const query = `
		INSERT INTO webhooks (id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, retry_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	now := time.Now().UTC()
	w.CreatedAt = now
	w.UpdatedAt = now
	if w.Status == "" {
		w.Status = types.WebhookStatusEnabled
	}

	_, err := r.pool.Exec(ctx, query,
		w.ID, w.TenantID, w.URL, w.ChannelPattern, w.SecretEnc,
		w.Status, w.MaxRetries, w.RetryCount, w.CreatedAt, w.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert webhook: %w", err)
	}
	return nil
}

// GetByID retrieves a webhook by ID scoped to tenantID.
func (r *WebhookRepository) GetByID(ctx context.Context, id, tenantID string) (*provisioning.Webhook, error) {
	const query = `
		SELECT id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, retry_count,
		       last_delivery_at, last_status, created_at, updated_at
		FROM webhooks
		WHERE id = $1 AND tenant_id = $2
	`
	row := r.pool.QueryRow(ctx, query, id, tenantID)
	w, err := scanWebhook(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, provisioning.ErrWebhookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get webhook: %w", err)
	}
	return w, nil
}

// List returns paginated webhooks for a tenant ordered by created_at DESC.
func (r *WebhookRepository) List(ctx context.Context, tenantID string, opts provisioning.ListOptions) ([]*provisioning.Webhook, int, error) {
	const countQuery = `SELECT COUNT(*) FROM webhooks WHERE tenant_id = $1`
	const dataQuery = `
		SELECT id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, retry_count,
		       last_delivery_at, last_status, created_at, updated_at
		FROM webhooks
		WHERE tenant_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	var total int
	if err := r.pool.QueryRow(ctx, countQuery, tenantID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count webhooks: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	rows, err := r.pool.Query(ctx, dataQuery, tenantID, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list webhooks: %w", err)
	}
	defer rows.Close()

	var webhooks []*provisioning.Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan webhook row: %w", err)
		}
		webhooks = append(webhooks, w)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate webhook rows: %w", err)
	}
	return webhooks, total, nil
}

// Update applies partial update from UpdateWebhookRequest.
func (r *WebhookRepository) Update(ctx context.Context, req provisioning.UpdateWebhookRequest) (*provisioning.Webhook, error) {
	// Build SET clause dynamically from non-nil fields.
	setClauses := []string{"updated_at = NOW()"}
	args := []any{req.ID, req.TenantID}
	argIdx := 3

	if req.URL != nil {
		setClauses = append(setClauses, fmt.Sprintf("url = $%d", argIdx))
		args = append(args, *req.URL)
		argIdx++
	}
	if req.ChannelPattern != nil {
		setClauses = append(setClauses, fmt.Sprintf("channel_pattern = $%d", argIdx))
		args = append(args, *req.ChannelPattern)
		argIdx++
	}
	if req.MaxRetries != nil {
		setClauses = append(setClauses, fmt.Sprintf("max_retries = $%d", argIdx))
		args = append(args, *req.MaxRetries)
		argIdx++
	}
	if req.Status != nil {
		setClauses = append(setClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, *req.Status)
		// Reset retry_count to 0 on any →enabled transition.
		if *req.Status == types.WebhookStatusEnabled {
			setClauses = append(setClauses, "retry_count = 0")
		}
	}

	query := fmt.Sprintf(`
		UPDATE webhooks
		SET %s
		WHERE id = $1 AND tenant_id = $2
		RETURNING id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, retry_count,
		          last_delivery_at, last_status, created_at, updated_at
	`, strings.Join(setClauses, ", "))

	row := r.pool.QueryRow(ctx, query, args...)
	w, err := scanWebhook(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, provisioning.ErrWebhookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("update webhook: %w", err)
	}
	return w, nil
}

// Delete removes a webhook scoped to tenantID.
func (r *WebhookRepository) Delete(ctx context.Context, id, tenantID string) error {
	const query = `DELETE FROM webhooks WHERE id = $1 AND tenant_id = $2`
	tag, err := r.pool.Exec(ctx, query, id, tenantID)
	if err != nil {
		return fmt.Errorf("delete webhook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return provisioning.ErrWebhookNotFound
	}
	return nil
}

// CountActive returns the number of non-suspended webhooks for a tenant.
func (r *WebhookRepository) CountActive(ctx context.Context, tenantID string) (int, error) {
	const query = `SELECT COUNT(*) FROM webhooks WHERE tenant_id = $1 AND status != $2`
	var count int
	if err := r.pool.QueryRow(ctx, query, tenantID, types.WebhookStatusSuspended).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active webhooks: %w", err)
	}
	return count, nil
}

// ListTenantIDs returns all distinct tenant IDs with at least one active webhook.
func (r *WebhookRepository) ListTenantIDs(ctx context.Context) ([]string, error) {
	const query = `SELECT DISTINCT tenant_id FROM webhooks WHERE status != $1`
	rows, err := r.pool.Query(ctx, query, types.WebhookStatusSuspended)
	if err != nil {
		return nil, fmt.Errorf("list webhook tenant IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan tenant ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook tenant IDs: %w", err)
	}
	return ids, nil
}

// ListByTenantForWorker returns webhook records for worker cache hydration.
// SecretEnc in the returned records is raw AES-256-GCM ciphertext (binary, not base64) —
// this function decodes the base64 TEXT column from the DB before setting this field.
// The webhook-worker passes it directly to crypto.DecryptRawToBytes(rec.SecretEnc, key).
func (r *WebhookRepository) ListByTenantForWorker(ctx context.Context, tenantID string) ([]*provisioning.WebhookRecord, error) {
	const query = `
		SELECT id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, last_delivery_at
		FROM webhooks
		WHERE tenant_id = $1
	`
	rows, err := r.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list webhooks for worker: %w", err)
	}
	defer rows.Close()

	var records []*provisioning.WebhookRecord
	for rows.Next() {
		var (
			rec       provisioning.WebhookRecord
			secretEnc string
		)
		if err := rows.Scan(&rec.ID, &rec.TenantID, &rec.URL, &rec.ChannelPattern,
			&secretEnc, &rec.Status, &rec.MaxRetries, &rec.LastDeliveryAt); err != nil {
			return nil, fmt.Errorf("scan worker webhook row: %w", err)
		}
		// Decode from base64 TEXT stored in DB to raw ciphertext bytes for gRPC transport.
		// The proto field is bytes (raw AES-256-GCM ciphertext), not base64.
		rawCiphertext, err := base64.StdEncoding.DecodeString(secretEnc)
		if err != nil {
			return nil, fmt.Errorf("decode secret_enc for webhook %s: %w", rec.ID, err)
		}
		rec.SecretEnc = rawCiphertext
		records = append(records, &rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker webhook rows: %w", err)
	}
	return records, nil
}

// UpdateStatus transitions a webhook's status and sets retry_count.
func (r *WebhookRepository) UpdateStatus(ctx context.Context, id, tenantID, status string, retryCount int) error {
	const query = `
		UPDATE webhooks SET status = $3, retry_count = $4, updated_at = NOW()
		WHERE id = $1 AND tenant_id = $2
	`
	tag, err := r.pool.Exec(ctx, query, id, tenantID, status, retryCount)
	if err != nil {
		return fmt.Errorf("update webhook status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return provisioning.ErrWebhookNotFound
	}
	return nil
}

// RecordDelivery inserts a delivery attempt, updates last_delivery_at/last_status,
// and prunes to keep at most 50 records per webhook — all in one transaction.
// ON CONFLICT (id) DO NOTHING makes the INSERT idempotent: duplicate delivery_id
// replays are no-ops (tag.RowsAffected() == 0) and the prune/update are skipped
// . A three-statement transaction is used instead of a CTE because
// the CTE's DELETE subquery runs against the pre-INSERT snapshot in PostgreSQL,
// preventing the prune from seeing the newly inserted row.
func (r *WebhookRepository) RecordDelivery(ctx context.Context, d *provisioning.WebhookDelivery) error {
	const maxDeliveries = 50

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	const insertDelivery = `
		INSERT INTO webhook_deliveries (id, webhook_id, tenant_id, attempt, status_code, latency_ms, error, delivered_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING
	`
	tag, txErr := tx.Exec(ctx, insertDelivery,
		d.ID, d.WebhookID, d.TenantID, d.Attempt,
		d.StatusCode, d.LatencyMS, d.Error, d.DeliveredAt,
	)
	if txErr != nil {
		err = fmt.Errorf("insert delivery: %w", txErr)
		return err
	}
	// Duplicate delivery_id — idempotent no-op.
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		return nil
	}

	// Prune: the DELETE reads the current transaction state (including the new row),
	// so OFFSET $1 correctly skips the maxDeliveries newest and deletes the rest.
	const pruneDeliveries = `
		DELETE FROM webhook_deliveries
		WHERE id IN (
			SELECT id FROM webhook_deliveries
			WHERE webhook_id = $1
			ORDER BY delivered_at DESC
			OFFSET $2
		)
	`
	if _, txErr = tx.Exec(ctx, pruneDeliveries, d.WebhookID, maxDeliveries); txErr != nil {
		err = fmt.Errorf("prune deliveries: %w", txErr)
		return err
	}

	lastStatus := strconv.Itoa(d.StatusCode)
	const updateWebhook = `
		UPDATE webhooks
		SET last_delivery_at = $2, last_status = $3, updated_at = NOW()
		WHERE id = $1 AND tenant_id = $4
	`
	if _, txErr = tx.Exec(ctx, updateWebhook, d.WebhookID, d.DeliveredAt, lastStatus, d.TenantID); txErr != nil {
		err = fmt.Errorf("update last delivery: %w", txErr)
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery transaction: %w", err)
	}
	return nil
}

// SuspendAllForDowngrade bulk-suspends all non-suspended webhooks.
// cutoff is recorded for audit purposes but does not filter rows — all non-suspended
// webhooks are suspended regardless of creation date (feature gate prevents new
// registrations after downgrade; defense-in-depth catches any edge cases).
func (r *WebhookRepository) SuspendAllForDowngrade(ctx context.Context, _ time.Time) (int, error) {
	const query = `
		UPDATE webhooks SET status = $1, updated_at = NOW()
		WHERE status != $1
	`
	tag, err := r.pool.Exec(ctx, query, types.WebhookStatusSuspended)
	if err != nil {
		return 0, fmt.Errorf("suspend all for downgrade: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ListForDowngrade returns all non-suspended webhooks eligible for suspension.
// cutoff is kept for interface compatibility and audit logging at the call site.
func (r *WebhookRepository) ListForDowngrade(ctx context.Context, _ time.Time) ([]*provisioning.Webhook, error) {
	const query = `
		SELECT id, tenant_id, url, channel_pattern, secret_enc, status, max_retries, retry_count,
		       last_delivery_at, last_status, created_at, updated_at
		FROM webhooks
		WHERE status != $1
	`
	rows, err := r.pool.Query(ctx, query, types.WebhookStatusSuspended)
	if err != nil {
		return nil, fmt.Errorf("list for downgrade: %w", err)
	}
	defer rows.Close()

	var webhooks []*provisioning.Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, fmt.Errorf("scan webhook row: %w", err)
		}
		webhooks = append(webhooks, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate downgrade webhook rows: %w", err)
	}
	return webhooks, nil
}

// scanner is satisfied by both pgx.Row and pgx.Rows.
type webhookScanner interface {
	Scan(dest ...any) error
}

func scanWebhook(s webhookScanner) (*provisioning.Webhook, error) {
	var w provisioning.Webhook
	err := s.Scan(
		&w.ID, &w.TenantID, &w.URL, &w.ChannelPattern, &w.SecretEnc,
		&w.Status, &w.MaxRetries, &w.RetryCount,
		&w.LastDeliveryAt, &w.LastStatus,
		&w.CreatedAt, &w.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan webhook: %w", err)
	}
	return &w, nil
}
