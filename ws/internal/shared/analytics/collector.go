package analytics

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// Collector is the interface all services use to record analytics events.
// The hot-path methods (Increment*, Record*) are zero-allocation atomic increments.
// NoopCollector implements this interface when analytics is disabled or ungated.
type Collector interface {
	// IncrementConnections records a connection delta (+1 on connect, -1 on disconnect).
	// transport is "websocket" or "sse". Must be called AFTER tenantID is known (post-auth).
	IncrementConnections(tenantID, transport string, delta int64)

	// IncrementMessages records message throughput for one Kafka dispatch event.
	// channelPrefix is the first path segment before "." (e.g., "user" from "user.trade.BTC").
	IncrementMessages(tenantID, channelPrefix string, published, delivered, failed, latencyMs int64)

	// RecordPushDelivery records one push notification delivery attempt.
	// provider is the platform label: "web", "android", or "ios" (from PushJob.Platform).
	RecordPushDelivery(tenantID, provider string, success, failed, expired, rateLimited, latencyMs int64)

	// Start launches the background flush goroutine. Called once at service startup.
	// The goroutine exits when ctx is canceled and performs a final flush before returning.
	Start(ctx context.Context, wg *sync.WaitGroup) error

	// Flush executes one synchronous flush. Used by the flush goroutine internally;
	// external callers should NOT call this on shutdown — the goroutine handles it.
	Flush(ctx context.Context) error
}

// NoopCollector is a zero-allocation implementation of Collector that does nothing.
// Returned by NewCollector when the pool is nil (analytics disabled or ungated).
type NoopCollector struct{}

// IncrementConnections is a no-op satisfying Collector.
func (NoopCollector) IncrementConnections(_, _ string, _ int64) {}

// IncrementMessages is a no-op satisfying Collector.
func (NoopCollector) IncrementMessages(_, _ string, _, _, _, _ int64) {}

// RecordPushDelivery is a no-op satisfying Collector.
func (NoopCollector) RecordPushDelivery(_, _ string, _, _, _, _, _ int64) {}

// Start is a no-op satisfying Collector — no goroutine is launched, no pool is opened.
func (NoopCollector) Start(_ context.Context, _ *sync.WaitGroup) error { return nil }

// Flush is a no-op satisfying Collector.
func (NoopCollector) Flush(_ context.Context) error { return nil }

// CollectorConfig configures the analytics Collector.
type CollectorConfig struct {
	// ServicePrefix is the Prometheus metric prefix: "gateway", "ws", "provisioning", or "push".
	ServicePrefix string
	// PodID is the stable pod identity (from PodIdentityConfig.PodID()).
	PodID string
	// BufferSize is the maximum number of tenant entries in memory between flushes.
	BufferSize int
	// FlushInterval controls how often the background goroutine flushes to DB.
	FlushInterval time.Duration
	// FlushDeadline is the timeout for the final shutdown flush. Defaults to defaultFlushDeadline.
	FlushDeadline time.Duration
	// DowngradePoll controls how often the edition gate is re-checked.
	DowngradePoll time.Duration
	// EditionManager is used for the edition gate check at flush time. Nil disables gating.
	EditionManager *license.Manager
	// Feature is the license.Feature constant to check (license.Analytics or license.AnalyticsPush).
	Feature license.Feature
}

// NewCollector returns a real Collector backed by the given pool, or NoopCollector
// if pool is nil (analytics disabled, ANALYTICS_ENABLED=false, or OpenAnalyticsPool returned nil).
func NewCollector(cfg CollectorConfig, pool *pgxpool.Pool, logger zerolog.Logger) Collector {
	if pool == nil {
		return NoopCollector{}
	}
	if cfg.FlushDeadline == 0 {
		cfg.FlushDeadline = defaultFlushDeadline
	}
	// BufferSize must be validated by AnalyticsConfig.Validate() at startup.
	// If it reaches here as 0, use a safe fallback rather than a silent zero-capacity map.
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = analyticsDefaultBufferSize
	}
	return newRealCollector(cfg, pool, logger)
}
