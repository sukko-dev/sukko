package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// RollupManager promotes minute-bucket rows into hour and day rollup tables.
// Runs in the provisioning AnalyticsManager on promoteHourInterval and promoteDayInterval.
type RollupManager struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// NewRollupManager creates a RollupManager.
func NewRollupManager(pool *pgxpool.Pool, logger zerolog.Logger) *RollupManager {
	return &RollupManager{
		pool:   pool,
		logger: logger.With().Str("component", "analytics_rollup_manager").Logger(),
	}
}

// PromoteHour aggregates the last complete hour of minute rows into hour rollup tables.
// Uses a PostgreSQL advisory lock to prevent concurrent promotion from multiple pods.
// Returns nil without error when another pod holds the lock (skips the tick).
func (rm *RollupManager) PromoteHour(ctx context.Context) error {
	var locked bool
	if err := rm.pool.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock($1)`, rollupHourLockID,
	).Scan(&locked); err != nil {
		return fmt.Errorf("acquire hour rollup lock: %w", err)
	}
	if !locked {
		return nil // another pod is promoting; skip
	}

	hourStart := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)

	if err := rm.promoteConnectionsHour(ctx, hourStart); err != nil {
		return err
	}
	if err := rm.promoteMessagesHour(ctx, hourStart); err != nil {
		return err
	}
	if err := rm.promotePushHour(ctx, hourStart); err != nil {
		return err
	}
	rm.logger.Debug().Time("hour", hourStart).Msg("hour rollup promotion complete")
	return nil
}

// PromoteDay aggregates the last complete day of hour rows into day rollup tables.
func (rm *RollupManager) PromoteDay(ctx context.Context) error {
	var locked bool
	if err := rm.pool.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock($1)`, rollupDayLockID,
	).Scan(&locked); err != nil {
		return fmt.Errorf("acquire day rollup lock: %w", err)
	}
	if !locked {
		return nil
	}

	dayStart := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)

	if err := rm.promoteConnectionsDay(ctx, dayStart); err != nil {
		return err
	}
	if err := rm.promoteMessagesDay(ctx, dayStart); err != nil {
		return err
	}
	if err := rm.promotePushDay(ctx, dayStart); err != nil {
		return err
	}
	rm.logger.Debug().Time("day", dayStart).Msg("day rollup promotion complete")
	return nil
}

func (rm *RollupManager) promoteConnectionsHour(ctx context.Context, hourStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_connections_hour
			(tenant_id, bucket_start, bucket_size, transport,
			 active_count, connect_count, disconnect_count, error_count)
		SELECT
			tenant_id,
			date_trunc('hour', bucket_start) AS bucket_start,
			'hour' AS bucket_size,
			transport,
			SUM(active_count), SUM(connect_count), SUM(disconnect_count), SUM(error_count)
		FROM analytics_connections
		WHERE bucket_size = 'minute'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('hour', bucket_start), transport
		ON CONFLICT (tenant_id, bucket_start, bucket_size, transport)
		DO UPDATE SET
			active_count     = EXCLUDED.active_count,
			connect_count    = EXCLUDED.connect_count,
			disconnect_count = EXCLUDED.disconnect_count,
			error_count      = EXCLUDED.error_count`,
		hourStart, hourStart.Add(time.Hour))
	if err != nil {
		return fmt.Errorf("promote connections hour: %w", err)
	}
	return nil
}

func (rm *RollupManager) promoteMessagesHour(ctx context.Context, hourStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_messages_hour
			(tenant_id, bucket_start, bucket_size, channel_prefix,
			 published_count, delivered_count, failed_count, total_latency_ms, sample_count)
		SELECT
			tenant_id,
			date_trunc('hour', bucket_start),
			'hour',
			channel_prefix,
			SUM(published_count), SUM(delivered_count), SUM(failed_count),
			SUM(total_latency_ms), SUM(sample_count)
		FROM analytics_messages
		WHERE bucket_size = 'minute'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('hour', bucket_start), channel_prefix
		ON CONFLICT (tenant_id, bucket_start, bucket_size, channel_prefix)
		DO UPDATE SET
			published_count  = EXCLUDED.published_count,
			delivered_count  = EXCLUDED.delivered_count,
			failed_count     = EXCLUDED.failed_count,
			total_latency_ms = EXCLUDED.total_latency_ms,
			sample_count     = EXCLUDED.sample_count`,
		hourStart, hourStart.Add(time.Hour))
	if err != nil {
		return fmt.Errorf("promote messages hour: %w", err)
	}
	return nil
}

func (rm *RollupManager) promotePushHour(ctx context.Context, hourStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_push_hour
			(tenant_id, bucket_start, bucket_size, provider,
			 sent_count, success_count, failed_count, expired_count,
			 rate_limited_count, total_latency_ms, sample_count)
		SELECT
			tenant_id,
			date_trunc('hour', bucket_start),
			'hour',
			provider,
			SUM(sent_count), SUM(success_count), SUM(failed_count),
			SUM(expired_count), SUM(rate_limited_count),
			SUM(total_latency_ms), SUM(sample_count)
		FROM analytics_push
		WHERE bucket_size = 'minute'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('hour', bucket_start), provider
		ON CONFLICT (tenant_id, bucket_start, bucket_size, provider)
		DO UPDATE SET
			sent_count         = EXCLUDED.sent_count,
			success_count      = EXCLUDED.success_count,
			failed_count       = EXCLUDED.failed_count,
			expired_count      = EXCLUDED.expired_count,
			rate_limited_count = EXCLUDED.rate_limited_count,
			total_latency_ms   = EXCLUDED.total_latency_ms,
			sample_count       = EXCLUDED.sample_count`,
		hourStart, hourStart.Add(time.Hour))
	if err != nil {
		return fmt.Errorf("promote push hour: %w", err)
	}
	return nil
}

func (rm *RollupManager) promoteConnectionsDay(ctx context.Context, dayStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_connections_day
			(tenant_id, bucket_start, bucket_size, transport,
			 active_count, connect_count, disconnect_count, error_count)
		SELECT
			tenant_id,
			date_trunc('day', bucket_start),
			'day',
			transport,
			SUM(active_count), SUM(connect_count), SUM(disconnect_count), SUM(error_count)
		FROM analytics_connections_hour
		WHERE bucket_size = 'hour'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('day', bucket_start), transport
		ON CONFLICT (tenant_id, bucket_start, bucket_size, transport)
		DO UPDATE SET
			active_count     = EXCLUDED.active_count,
			connect_count    = EXCLUDED.connect_count,
			disconnect_count = EXCLUDED.disconnect_count,
			error_count      = EXCLUDED.error_count`,
		dayStart, dayStart.AddDate(0, 0, 1))
	if err != nil {
		return fmt.Errorf("promote connections day: %w", err)
	}
	return nil
}

func (rm *RollupManager) promoteMessagesDay(ctx context.Context, dayStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_messages_day
			(tenant_id, bucket_start, bucket_size, channel_prefix,
			 published_count, delivered_count, failed_count, total_latency_ms, sample_count)
		SELECT
			tenant_id,
			date_trunc('day', bucket_start),
			'day',
			channel_prefix,
			SUM(published_count), SUM(delivered_count), SUM(failed_count),
			SUM(total_latency_ms), SUM(sample_count)
		FROM analytics_messages_hour
		WHERE bucket_size = 'hour'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('day', bucket_start), channel_prefix
		ON CONFLICT (tenant_id, bucket_start, bucket_size, channel_prefix)
		DO UPDATE SET
			published_count  = EXCLUDED.published_count,
			delivered_count  = EXCLUDED.delivered_count,
			failed_count     = EXCLUDED.failed_count,
			total_latency_ms = EXCLUDED.total_latency_ms,
			sample_count     = EXCLUDED.sample_count`,
		dayStart, dayStart.AddDate(0, 0, 1))
	if err != nil {
		return fmt.Errorf("promote messages day: %w", err)
	}
	return nil
}

func (rm *RollupManager) promotePushDay(ctx context.Context, dayStart time.Time) error {
	_, err := rm.pool.Exec(ctx, `
		INSERT INTO analytics_push_day
			(tenant_id, bucket_start, bucket_size, provider,
			 sent_count, success_count, failed_count, expired_count,
			 rate_limited_count, total_latency_ms, sample_count)
		SELECT
			tenant_id,
			date_trunc('day', bucket_start),
			'day',
			provider,
			SUM(sent_count), SUM(success_count), SUM(failed_count),
			SUM(expired_count), SUM(rate_limited_count),
			SUM(total_latency_ms), SUM(sample_count)
		FROM analytics_push_hour
		WHERE bucket_size = 'hour'
		  AND bucket_start >= $1 AND bucket_start < $2
		GROUP BY tenant_id, date_trunc('day', bucket_start), provider
		ON CONFLICT (tenant_id, bucket_start, bucket_size, provider)
		DO UPDATE SET
			sent_count         = EXCLUDED.sent_count,
			success_count      = EXCLUDED.success_count,
			failed_count       = EXCLUDED.failed_count,
			expired_count      = EXCLUDED.expired_count,
			rate_limited_count = EXCLUDED.rate_limited_count,
			total_latency_ms   = EXCLUDED.total_latency_ms,
			sample_count       = EXCLUDED.sample_count`,
		dayStart, dayStart.AddDate(0, 0, 1))
	if err != nil {
		return fmt.Errorf("promote push day: %w", err)
	}
	return nil
}
