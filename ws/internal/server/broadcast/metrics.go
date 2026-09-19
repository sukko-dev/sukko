package broadcast

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metricDroppedTotal              = "ws_broadcast_bus_dropped_total"
	metricPublishFailuresTotal      = "ws_broadcast_publish_failures_total"
	metricSubscribeCommandsTotal    = "ws_broadcast_subscribe_commands_total"
	metricReconcileCorrectionsTotal = "ws_broadcast_reconcile_corrections_total"
	metricSubscriptionsDesired      = "ws_broadcast_subscriptions_desired"
	metricSubscriptionsEstablished  = "ws_broadcast_subscriptions_established"

	metricTenantLabelAll     = "_all"
	metricTenantLabelEmpty   = ""
	metricTenantLabelInvalid = "_invalid" // tenant ID rejected at publish (contains separator)

	metricResultSuccess = "success"
	metricResultRetry   = "retry"
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

	// subscriptionsDesired / subscriptionsEstablished make the silent-dark
	// failure mode visible (§VI, ADR-0016): the failure this pair exposes is
	// publishes succeeding while established sits below desired. One unit per
	// tenant SUBSCRIBE plus one for the all-tenant PSUBSCRIBE when active.
	subscriptionsDesired     prometheus.Gauge
	subscriptionsEstablished prometheus.Gauge

	// publishFailuresTotal makes a Valkey publish outage visible on dashboards
	// (§VI): before it existed, failed PUBLISH commands were tracked only by an
	// atomic surfaced through GetMetrics(), invisible to Prometheus alerting.
	// No tenant label — a bus outage is global, and the failure cause is not
	// per-tenant (validation rejects are counted in droppedTotal instead).
	publishFailuresTotal prometheus.Counter
}

func newBusMetrics(reg prometheus.Registerer) *busMetrics {
	return &busMetrics{
		droppedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: metricDroppedTotal,
			Help: "Messages dropped due to a full subscriber channel, by tenant.",
		}, []string{"tenant_id"}),
		subscribeCommandsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: metricSubscribeCommandsTotal,
			// The retry label counts individual command failures inside a
			// convergence pass; the failed command is retried by the next pass
			// (capped exponential backoff). Historically this label counted
			// "failed and became the sole retry candidate" — the single retry
			// slot is gone (ADR-0016), so every failure now leads to a retry.
			Help: "SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE commands issued by the subscription convergence loop, by result (success, retry).",
		}, []string{"result"}),
		reconcileCorrectionsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: metricReconcileCorrectionsTotal,
			Help: "Subscription commands actually issued by the periodic reconciliation backstop tick (divergence the event-driven convergence path had not already fixed).",
		}),
		subscriptionsDesired: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: metricSubscriptionsDesired,
			Help: "Valkey subscriptions this pod wants: active tenant channels plus the all-tenant pattern subscription when in use.",
		}),
		subscriptionsEstablished: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: metricSubscriptionsEstablished,
			Help: "Valkey subscriptions confirmed on the current pub/sub connection. Established below desired means this pod is not receiving some broadcasts even though publishes succeed.",
		}),
		publishFailuresTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: metricPublishFailuresTotal,
			Help: "Broadcast messages that failed to publish to the backend (Valkey PUBLISH command failure or serialization failure).",
		}),
	}
}
