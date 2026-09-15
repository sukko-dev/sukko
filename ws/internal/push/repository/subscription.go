package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// subscriptionRepo implements SubscriptionRepository using PostgreSQL via pgxpool.
type subscriptionRepo struct {
	pool *pgxpool.Pool
}

// NewSubscriptionRepository creates a SubscriptionRepository backed by PostgreSQL (pgxpool).
// Channels are stored as a TEXT[] array column, scanned natively by pgx.
func NewSubscriptionRepository(pool *pgxpool.Pool) SubscriptionRepository {
	return &subscriptionRepo{pool: pool}
}

// Create inserts a new push subscription and returns its ID via RETURNING.
func (r *subscriptionRepo) Create(ctx context.Context, sub *PushSubscription) (int64, error) {
	query := `
		INSERT INTO push_subscriptions (tenant_id, principal, platform, token, endpoint, p256dh_key, auth_secret, channels, jti, token_iat)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`

	var id int64
	err := r.pool.QueryRow(ctx, query,
		sub.TenantID,
		sub.Principal,
		sub.Platform,
		sub.Token,
		sub.Endpoint,
		sub.P256dhKey,
		sub.AuthSecret,
		sub.Channels,
		sub.JTI,
		sub.TokenIAT,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert push subscription: %w", err)
	}

	return id, nil
}

// Delete removes a push subscription by ID.
func (r *subscriptionRepo) Delete(ctx context.Context, id int64, tenantID string) error {
	query := `DELETE FROM push_subscriptions WHERE id = $1 AND tenant_id = $2`

	_, err := r.pool.Exec(ctx, query, id, tenantID)
	if err != nil {
		return fmt.Errorf("delete push subscription: %w", err)
	}

	return nil
}

// DeleteByToken removes a push subscription matching the tenant ID and device token or endpoint.
// For FCM/APNs, matches the token column. For Web Push, matches the endpoint column.
func (r *subscriptionRepo) DeleteByToken(ctx context.Context, tenantID, token string) error {
	query := `DELETE FROM push_subscriptions WHERE tenant_id = $1 AND (token = $2 OR endpoint = $2)`

	_, err := r.pool.Exec(ctx, query, tenantID, token)
	if err != nil {
		return fmt.Errorf("delete push subscription by token: %w", err)
	}

	return nil
}

// FindByTenant returns all push subscriptions for the given tenant.
func (r *subscriptionRepo) FindByTenant(ctx context.Context, tenantID string) ([]PushSubscription, error) {
	query := `
		SELECT id, tenant_id, principal, platform, token, endpoint, p256dh_key, auth_secret, channels, created_at, last_success_at
		FROM push_subscriptions
		WHERE tenant_id = $1
	`

	rows, err := r.pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query push subscriptions: %w", err)
	}
	defer rows.Close()

	var subs []PushSubscription
	for rows.Next() {
		var sub PushSubscription
		var lastSuccess *time.Time

		err := rows.Scan(
			&sub.ID,
			&sub.TenantID,
			&sub.Principal,
			&sub.Platform,
			&sub.Token,
			&sub.Endpoint,
			&sub.P256dhKey,
			&sub.AuthSecret,
			&sub.Channels,
			&sub.CreatedAt,
			&lastSuccess,
		)
		if err != nil {
			return nil, fmt.Errorf("scan push subscription: %w", err)
		}

		sub.LastSuccessAt = lastSuccess
		subs = append(subs, sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push subscriptions: %w", err)
	}

	return subs, nil
}

// UpdateLastSuccess sets the last_success_at timestamp to the current time.
func (r *subscriptionRepo) UpdateLastSuccess(ctx context.Context, id int64) error {
	query := `UPDATE push_subscriptions SET last_success_at = $1 WHERE id = $2`

	_, err := r.pool.Exec(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("update last success: %w", err)
	}

	return nil
}

// DeleteByJTI removes push subscriptions matching the given JWT ID, scoped to tenant.
// Returns the count of deleted rows.
func (r *subscriptionRepo) DeleteByJTI(ctx context.Context, tenantID, jti string) (int, error) {
	query := `DELETE FROM push_subscriptions WHERE tenant_id = $1 AND jti = $2`

	result, err := r.pool.Exec(ctx, query, tenantID, jti)
	if err != nil {
		return 0, fmt.Errorf("delete by jti: %w", err)
	}

	return int(result.RowsAffected()), nil
}

// DeleteBySub removes push subscriptions for a user where token_iat < revokedAt.
// Only deletes registrations created with tokens issued before the revocation.
// Returns the count of deleted rows.
func (r *subscriptionRepo) DeleteBySub(ctx context.Context, tenantID, principal string, revokedAt int64) (int, error) {
	query := `DELETE FROM push_subscriptions WHERE tenant_id = $1 AND principal = $2 AND token_iat < to_timestamp($3)`

	result, err := r.pool.Exec(ctx, query, tenantID, principal, revokedAt)
	if err != nil {
		return 0, fmt.Errorf("delete by sub: %w", err)
	}

	return int(result.RowsAffected()), nil
}
