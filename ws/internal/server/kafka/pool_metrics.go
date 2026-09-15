package kafka

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// poolMetrics is the set of Prometheus counters shared by the fan-out and DLQ worker pools
// plus the producer's DLQ-enqueue-drop leg. Registration is centralized here (mirroring
// consumerRoutingMetrics) so the pool constructors receive already-registered counters
// instead of calling promauto themselves — a second Producer in one process (e.g. a test
// binary) would otherwise panic on duplicate registration (#179 P1b-C2).
//
// Label arities match the existing sibling registrations and MUST be preserved:
//   - fanoutWriteFailed {tenant,topic} — per-topic produce failure (may be DLQ-retried)
//   - fanoutDropped     {tenant}       — fan-out enqueue-backpressure drop (still routed to DLQ)
//   - dlqWriteFailed    {tenant,reason}— DLQ write failed after retry exhaustion (terminal loss)
//   - dlqDropped        {tenant}       — DLQ enqueue full, write never attempted (terminal loss)
type poolMetrics struct {
	fanoutWriteFailed *prometheus.CounterVec
	fanoutDropped     *prometheus.CounterVec
	dlqWriteFailed    *prometheus.CounterVec
	dlqDropped        *prometheus.CounterVec
}

// routingPoolMetricsSingleton holds the package-level pool counters, registered exactly
// once against the default Prometheus registry.
var routingPoolMetricsSingleton = struct {
	metrics poolMetrics
	once    sync.Once
}{}

// newPoolMetrics returns the counter set for the fan-out/DLQ pools. When reg is non-nil
// (tests), a fresh set is registered against reg for registry isolation. When reg is nil
// (production), the package singleton is lazily registered against the default registry
// exactly once and returned on every call — so repeated NewProducer calls share one
// registration and never panic.
func newPoolMetrics(reg prometheus.Registerer) poolMetrics {
	if reg != nil {
		return buildPoolMetrics(promauto.With(reg))
	}
	routingPoolMetricsSingleton.once.Do(func() {
		routingPoolMetricsSingleton.metrics = buildPoolMetrics(promauto.With(prometheus.DefaultRegisterer))
	})
	return routingPoolMetricsSingleton.metrics
}

func buildPoolMetrics(f promauto.Factory) poolMetrics {
	return poolMetrics{
		fanoutWriteFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: MetricFanoutWriteFailedTotal,
			Help: "Total fan-out topic writes that failed",
		}, []string{LabelTenant, LabelTopic}),
		fanoutDropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: MetricFanoutDroppedTotal,
			Help: "Total fan-out jobs dropped because the queue was full (still routed to DLQ)",
		}, []string{LabelTenant}),
		dlqWriteFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: MetricDLQWriteFailedTotal,
			Help: "Total dead-letter writes that failed after all retries",
		}, []string{LabelTenant, LabelReason}),
		dlqDropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: MetricDLQDroppedTotal,
			Help: "Total records dropped because the DLQ queue was full (write never attempted)",
		}, []string{LabelTenant}),
	}
}
