package analytics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// analyticsMetrics holds Prometheus metrics for the analytics Collector.
// Registered under the calling service's prefix: gateway_analytics_*, ws_analytics_*, etc.
type analyticsMetrics struct {
	flushDuration    prometheus.Observer // histogram
	flushErrors      prometheus.Counter
	bufferTenants    prometheus.Gauge
	eventsDropped    prometheus.Counter
	snapshotsDropped prometheus.Counter
	sseDropped       prometheus.Counter // provisioning only; registered but may be unused
}

// newAnalyticsMetrics registers all analytics Prometheus metrics under servicePrefix.
// servicePrefix should be "gateway", "ws", "provisioning", or "push" (no trailing underscore).
func newAnalyticsMetrics(servicePrefix string) *analyticsMetrics {
	prefix := servicePrefix + "_analytics"
	return &analyticsMetrics{
		flushDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    prefix + "_flush_duration_seconds",
			Help:    "Duration of each analytics batch flush to PostgreSQL.",
			Buckets: prometheus.DefBuckets,
		}),
		flushErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_flush_errors_total",
			Help: "Total number of failed analytics flush attempts.",
		}),
		bufferTenants: promauto.NewGauge(prometheus.GaugeOpts{
			Name: prefix + "_buffer_tenants",
			Help: "Current number of tenant entries in the analytics in-memory buffer.",
		}),
		eventsDropped: promauto.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_events_dropped_total",
			Help: "Total analytics events dropped due to buffer capacity limit.",
		}),
		snapshotsDropped: promauto.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_flush_snapshots_dropped_total",
			Help: "Total analytics flush snapshots discarded during extended DB outage.",
		}),
		sseDropped: promauto.NewCounter(prometheus.CounterOpts{
			Name: prefix + "_sse_dropped_total",
			Help: "Total SSE analytics events dropped due to slow client (provisioning service only).",
		}),
	}
}
