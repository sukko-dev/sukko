package platform

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Bounds for AnalyticsConfig fields (§I: magic numbers must be named constants).
const (
	analyticsFlushIntervalFloor   = 5 * time.Second
	analyticsFlushIntervalCeiling = time.Hour

	analyticsBufferSizeFloor   = 1
	analyticsBufferSizeCeiling = 1_000_000

	analyticsDBMaxConnsMin = 1
	analyticsDBMaxConnsMax = 50

	analyticsPartmanIntervalFloor   = time.Minute
	analyticsPartmanIntervalCeiling = 24 * time.Hour

	analyticsRawRetentionFloor   = 5 * time.Minute
	analyticsRawRetentionCeiling = 24 * time.Hour

	analyticsDowngradePollFloor   = time.Minute
	analyticsDowngradePollCeiling = time.Hour

	analyticsSSEMaxConnsMin = 1
	analyticsSSEMaxConnsMax = 100
)

// AnalyticsConfig holds all analytics pipeline configuration shared across services.
// Embedded by GatewayConfig, ServerConfig, PushConfig, and ProvisioningConfig.
// Bounds checks in Validate() are skipped when Enabled=false to avoid spurious errors
// for operators who have disabled analytics (analogous to WebhookInternalTokenConfig guard).
type AnalyticsConfig struct {
	Enabled               bool          `env:"ANALYTICS_ENABLED"                 envDefault:"false"` // Enable event analytics collection. Requires ANALYTICS_DB_URL.
	DBURL                 string        `env:"ANALYTICS_DB_URL"                  redact:"true"`      // PostgreSQL connection URL for the analytics database.
	DBMaxConns            int           `env:"ANALYTICS_DB_MAX_CONNS"            envDefault:"5"`     // Maximum database connections for the analytics writer pool.
	FlushInterval         time.Duration `env:"ANALYTICS_FLUSH_INTERVAL"          envDefault:"60s"`   // Interval at which buffered analytics events are flushed to the database.
	BufferSize            int           `env:"ANALYTICS_BUFFER_SIZE"             envDefault:"10000"` // In-memory buffer for analytics events before database flush. Events dropped when full.
	RawEvents             bool          `env:"ANALYTICS_RAW_EVENTS"              envDefault:"false"` // Store raw (un-aggregated) events in addition to aggregates. Increases storage requirements.
	RawRetention          time.Duration `env:"ANALYTICS_RAW_RETENTION"           envDefault:"1h"`    // Retention period for raw analytics events before automatic cleanup.
	DowngradePollInterval time.Duration `env:"ANALYTICS_DOWNGRADE_POLL_INTERVAL" envDefault:"5m"`    // Interval for polling provisioning service for analytics-affecting edition downgrades.
}

// Validate checks AnalyticsConfig for correctness.
// Bounds checks are only applied when Enabled=true; disabled configurations skip them.
func (c *AnalyticsConfig) Validate() error {
	if c.Enabled && c.DBURL == "" {
		return errors.New("ANALYTICS_DB_URL is required when ANALYTICS_ENABLED=true")
	}
	// Bounds checks only apply when analytics is enabled — operators with
	// ANALYTICS_ENABLED=false must not get spurious errors for unused fields (§I, §IV).
	if !c.Enabled {
		return nil
	}
	if c.DBMaxConns < analyticsDBMaxConnsMin || c.DBMaxConns > analyticsDBMaxConnsMax {
		return fmt.Errorf("ANALYTICS_DB_MAX_CONNS must be %d–%d, got %d",
			analyticsDBMaxConnsMin, analyticsDBMaxConnsMax, c.DBMaxConns)
	}
	if c.FlushInterval < analyticsFlushIntervalFloor || c.FlushInterval > analyticsFlushIntervalCeiling {
		return fmt.Errorf("ANALYTICS_FLUSH_INTERVAL must be %s–%s, got %s",
			analyticsFlushIntervalFloor, analyticsFlushIntervalCeiling, c.FlushInterval)
	}
	if c.BufferSize < analyticsBufferSizeFloor || c.BufferSize > analyticsBufferSizeCeiling {
		return fmt.Errorf("ANALYTICS_BUFFER_SIZE must be %d–%d, got %d",
			analyticsBufferSizeFloor, analyticsBufferSizeCeiling, c.BufferSize)
	}
	if c.RawRetention < analyticsRawRetentionFloor || c.RawRetention > analyticsRawRetentionCeiling {
		return fmt.Errorf("ANALYTICS_RAW_RETENTION must be %s–%s, got %s",
			analyticsRawRetentionFloor, analyticsRawRetentionCeiling, c.RawRetention)
	}
	if c.DowngradePollInterval < analyticsDowngradePollFloor || c.DowngradePollInterval > analyticsDowngradePollCeiling {
		return fmt.Errorf("ANALYTICS_DOWNGRADE_POLL_INTERVAL must be %s–%s, got %s",
			analyticsDowngradePollFloor, analyticsDowngradePollCeiling, c.DowngradePollInterval)
	}
	return nil
}

// OpenAnalyticsPool opens a dedicated pgxpool connection pool for analytics writes.
// Returns (nil, nil) when cfg.Enabled=false so callers can pass the nil pool to
// analytics.NewCollector, which will return a NoopCollector automatically.
func OpenAnalyticsPool(ctx context.Context, cfg AnalyticsConfig) (*pgxpool.Pool, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.DBURL == "" {
		return nil, errors.New("OpenAnalyticsPool: ANALYTICS_DB_URL is required when ANALYTICS_ENABLED=true")
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DBURL)
	if err != nil {
		return nil, fmt.Errorf("OpenAnalyticsPool: parse config: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.DBMaxConns) //nolint:gosec // DBMaxConns validated to 1–50, safe int32 conversion
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("OpenAnalyticsPool: create pool: %w", err)
	}
	return pool, nil
}
