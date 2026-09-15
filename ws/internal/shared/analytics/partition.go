package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// PartitionManager creates future partitions and drops expired ones for analytics tables.
// Used by the provisioning AnalyticsManager; run every ANALYTICS_PARTMAN_INTERVAL.
type PartitionManager struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// NewPartitionManager creates a PartitionManager.
func NewPartitionManager(pool *pgxpool.Pool, logger zerolog.Logger) *PartitionManager {
	return &PartitionManager{
		pool:   pool,
		logger: logger.With().Str("component", "analytics_partition_manager").Logger(),
	}
}

// RunMaintenance creates future partitions and drops expired ones.
// All errors are logged at Warn and skipped — §IV: non-fatal per partition.
func (pm *PartitionManager) RunMaintenance(ctx context.Context) error {
	now := time.Now().UTC().Truncate(24 * time.Hour)

	// Create T+1 and T+2 daily partitions for all minute/hour tables.
	for _, days := range []int{1, 2} {
		target := now.AddDate(0, 0, days)
		for _, table := range append(minuteTables, hourTables...) {
			if err := pm.createDayPartition(ctx, table, target); err != nil {
				pm.logger.Warn().Err(err).
					Str("table", table).
					Str("date", target.Format("2006-01-02")).
					Msg("failed to create day partition; skipping")
			}
		}
	}

	// Create next month's monthly partition for day tables.
	nextMonth := now.AddDate(0, 1, 0).Truncate(24 * time.Hour)
	for _, table := range dayTables {
		if err := pm.createMonthPartition(ctx, table, nextMonth); err != nil {
			pm.logger.Warn().Err(err).
				Str("table", table).
				Str("month", nextMonth.Format("2006-01")).
				Msg("failed to create month partition; skipping")
		}
	}

	// Drop minute table partitions older than 48h.
	for _, table := range minuteTables {
		cutoff := now.AddDate(0, 0, -retentionMinutePartitions)
		pm.dropPartitionsOlderThan(ctx, table, cutoff, "day")
	}

	// Drop hour table partitions older than 30 days.
	for _, table := range hourTables {
		cutoff := now.AddDate(0, 0, -retentionHourPartitions)
		pm.dropPartitionsOlderThan(ctx, table, cutoff, "day")
	}

	// Drop day table monthly partitions older than 365 days (~12 months).
	for _, table := range dayTables {
		cutoff := now.AddDate(0, -retentionDayPartitions, 0)
		pm.dropPartitionsOlderThan(ctx, table, cutoff, "month")
	}

	return nil
}

// createDayPartition creates a daily partition for the given date if it doesn't exist.
func (pm *PartitionManager) createDayPartition(ctx context.Context, table string, date time.Time) error {
	name := fmt.Sprintf("%s_%s", table, date.Format("20060102"))
	from := date.Format("2006-01-02")
	to := date.AddDate(0, 0, 1).Format("2006-01-02")
	_, err := pm.pool.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
			FOR VALUES FROM ('%s') TO ('%s')`,
			name, table, from, to))
	if err != nil {
		return fmt.Errorf("create day partition %s: %w", name, err)
	}
	return nil
}

// createMonthPartition creates a monthly partition for the given month if it doesn't exist.
func (pm *PartitionManager) createMonthPartition(ctx context.Context, table string, month time.Time) error {
	// Normalize to first of month.
	first := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	name := fmt.Sprintf("%s_%s", table, first.Format("200601"))
	from := first.Format("2006-01-02")
	to := first.AddDate(0, 1, 0).Format("2006-01-02")
	_, err := pm.pool.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
			FOR VALUES FROM ('%s') TO ('%s')`,
			name, table, from, to))
	if err != nil {
		return fmt.Errorf("create month partition %s: %w", name, err)
	}
	return nil
}

// dropPartitionsOlderThan drops child partitions whose name suffix (date) is before cutoff.
// granularity is "day" (suffix YYYYMMDD) or "month" (suffix YYYYMM).
func (pm *PartitionManager) dropPartitionsOlderThan(ctx context.Context, table string, cutoff time.Time, granularity string) {
	// Query pg_class for child partitions of the parent table.
	rows, err := pm.pool.Query(ctx,
		`SELECT c.relname
		FROM pg_inherits i
		JOIN pg_class p ON i.inhparent = p.oid
		JOIN pg_class c ON i.inhrelid  = c.oid
		WHERE p.relname = $1`, table)
	if err != nil {
		pm.logger.Warn().Err(err).Str("table", table).Msg("failed to list child partitions for retention")
		return
	}
	defer rows.Close()

	var toDrop []string
	for rows.Next() {
		var childName string
		if err := rows.Scan(&childName); err != nil {
			continue
		}
		prefix := table + "_"
		if !strings.HasPrefix(childName, prefix) {
			continue // skip partitions with unexpected naming (e.g. default partition)
		}
		suffix := childName[len(prefix):]
		var partDate time.Time
		var parseErr error
		switch granularity {
		case "day":
			partDate, parseErr = time.Parse("20060102", suffix)
		case "month":
			partDate, parseErr = time.Parse("200601", suffix)
		}
		if parseErr != nil {
			continue // skip partitions with unexpected name format
		}
		if partDate.Before(cutoff) {
			toDrop = append(toDrop, childName)
		}
	}
	if err := rows.Err(); err != nil {
		pm.logger.Warn().Err(err).Str("table", table).Msg("partition list scan error")
	}

	for _, name := range toDrop {
		// name is derived from pg_class.relname + validated prefix + date parse — low risk,
		// but we still guard against any chars that could break the identifier.
		if !strings.HasPrefix(name, table+"_") {
			continue // defensive: skip any name that slipped past the filter above
		}
		if _, err := pm.pool.Exec(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
			pm.logger.Warn().Err(err).Str("partition", name).Msg("failed to drop expired partition")
		} else {
			pm.logger.Info().Str("partition", name).Msg("dropped expired analytics partition")
		}
	}
}
