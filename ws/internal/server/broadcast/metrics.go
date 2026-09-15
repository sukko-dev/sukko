package broadcast

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metricDroppedTotal              = "ws_broadcast_bus_dropped_total"
	metricSubscribeCommandsTotal    = "ws_broadcast_subscribe_commands_total"
	metricReconcileCorrectionsTotal = "ws_broadcast_reconcile_corrections_total"

	metricTenantLabelAll     = "_all"
	metricTenantLabelEmpty   = ""
	metricTenantLabelInvalid = "_invalid" // tenant ID rejected at publish (contains separator)

	metricResultSuccess = "success"
	metricResultRetry   = "retry"
	metricResultDropped = "dropped"
)

// busMetrics holds all Prometheus instruments for valkeyBus.
// Registered once per bus instance via promauto.With(reg).
type busMetrics struct {
	// tenant_id cardinality bounded by license.MaxTenants (see license/limits.go);
	// unbounded for Enterprise — revisit if Enterprise tenant counts become large.
	// TODO(enterprise-cardinality): introduce BROADCAST_DROP_METRICS_TENANT_LABEL_ENABLED
	// config bool (default true) to allow operators to disable per-tenant label for large
	// Enterprise deployments. Track as follow-up task.
	droppedTotal *prometheus.CounterVec

	subscribeCommandsTotal    *prometheus.CounterVec
	reconcileCorrectionsTotal prometheus.Counter
}

func newBusMetrics(reg prometheus.Registerer) *busMetrics {
	return &busMetrics{
		droppedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: metricDroppedTotal,
			Help: "Messages dropped due to a full subscriber channel, by tenant.",
		}, []string{"tenant_id"}),
		subscribeCommandsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: metricSubscribeCommandsTotal,
			Help: "SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE commands issued by the subscription management goroutine, by result (success, retry, dropped).",
		}, []string{"result"}),
		reconcileCorrectionsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: metricReconcileCorrectionsTotal,
			Help: "Number of subscriptions re-issued by the periodic reconciliation tick (detected as missing from Valkey but expected).",
		}),
	}
}
