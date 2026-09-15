// Package metrics provides shared monitoring utilities for Sukko WebSocket services.
//
// This package standardizes histogram bucket definitions, label value constants,
// and callback interfaces used across server, gateway, and provisioning services.
//
// # Design Principles
//
//   - Shared buckets ensure consistent histogram resolution across services
//   - Constants prevent typos and enable refactoring
//   - Interfaces enable dependency injection without circular imports
//   - No-op implementations allow optional metrics
//
// # Bucket Definitions
//
// Standard histogram buckets are provided for common latency patterns:
//
//   - ConnectionDurationBuckets: Long-lived connections (1s to 1hr)
//   - AuthLatencyBuckets: Fast auth operations (sub-ms to 250ms)
//   - BackendLatencyBuckets: Backend calls (10ms to 1s)
//   - APILatencyBuckets: General API calls (matches prometheus.DefBuckets)
//   - BufferSaturationBuckets: Buffer fill levels (0 to 512)
//
// # Callback Interfaces
//
// Interfaces allow packages to report metrics without importing the monitoring package:
//
//   - CacheMetrics: Cache hit/miss/refresh tracking
//   - AccessDenialMetrics: Permission denial recording
//   - PoolMetrics: Multi-tenant pool metrics
//
// Each interface has a corresponding Noop* implementation for optional metrics.
//
// # Usage
//
// Import with an alias to avoid conflicts with service-specific metrics:
//
//	import pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
//
//	var latency = prometheus.NewHistogram(prometheus.HistogramOpts{
//	    Name:    "my_latency_seconds",
//	    Buckets: pkgmetrics.AuthLatencyBuckets,
//	})
package metrics
