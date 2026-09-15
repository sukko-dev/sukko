// Package analytics provides the per-tenant analytics data pipeline for Sukko.
//
// Architecture: each service maintains atomic in-memory counters per tenant per
// minute bucket. A background goroutine flushes minute rollups to PostgreSQL every
// ANALYTICS_FLUSH_INTERVAL via batch upsert. The provisioning service runs hourly/daily
// rollup promotion and app-managed partition maintenance.
//
// Usage:
//
//	pool, err := platform.OpenAnalyticsPool(ctx, cfg.AnalyticsConfig)
//	collector := analytics.NewCollector(analytics.CollectorConfig{
//	    ServicePrefix: "gateway",
//	    PodID:         cfg.PodIdentityConfig.PodID(),
//	    // ...
//	}, pool, logger)
//	wg.Go(func() {
//	    defer logging.RecoverPanic(logger, "analytics.collector", nil)
//	    collector.Start(ctx, wg)
//	})
//
// The Collector interface is nil-safe via NoopCollector — passing a nil pool or
// disabling ANALYTICS_ENABLED returns a NoopCollector that is safe to call from
// hot paths with zero overhead.
package analytics

// Compile-time interface checks ensure both implementations satisfy the contract.
var (
	_ Collector = NoopCollector{}
	_ Collector = (*realCollector)(nil)
)
