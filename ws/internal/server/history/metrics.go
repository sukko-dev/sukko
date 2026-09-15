package history

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds all Prometheus metrics for the history subsystem.
// Fields are exported so delivery handlers in other packages can record observations.
type Metrics struct {
	WriteDropped                   *prometheus.CounterVec
	DeliveryDropped                *prometheus.CounterVec
	ExpireFailure                  prometheus.Counter
	WriterActive                   prometheus.Gauge
	WriterRestartTotal             prometheus.Counter
	LockFailuresTotal              prometheus.Counter
	DeliveryDuration               *prometheus.HistogramVec
	SkipTotal                      *prometheus.CounterVec
	ValkeyBusKafkaFallbackDisabled prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		WriteDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_history_write_dropped_total",
			Help: "Number of history write attempts dropped.",
		}, []string{"reason"}),

		DeliveryDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_history_delivery_dropped_total",
			Help: "Number of history delivery messages dropped.",
		}, []string{"reason"}),

		ExpireFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_history_expire_failure_total",
			Help: "Number of EXPIRE command failures during history writes.",
		}),

		WriterActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ws_history_writer_active",
			Help: "1 if this pod holds the active writer lock, 0 otherwise.",
		}),

		WriterRestartTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_history_writer_restart_total",
			Help: "Number of times the history writer runOnce loop has restarted.",
		}),

		LockFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ws_history_lock_failure_total",
			Help: "Number of consecutive heartbeat lock failures.",
		}),

		DeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ws_history_delivery_duration_seconds",
			Help:    "Duration of history delivery requests.",
			Buckets: prometheus.DefBuckets,
		}, []string{"source"}),

		SkipTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_history_skip_total",
			Help: "Number of history entries skipped during delivery.",
		}, []string{"reason"}),

		ValkeyBusKafkaFallbackDisabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ws_history_valkey_bus_kafka_fallback_disabled",
			Help: "1 when the broadcast bus is Valkey-based and Kafka fallback is unavailable.",
		}),
	}

	reg.MustRegister(
		m.WriteDropped,
		m.DeliveryDropped,
		m.ExpireFailure,
		m.WriterActive,
		m.WriterRestartTotal,
		m.LockFailuresTotal,
		m.DeliveryDuration,
		m.SkipTotal,
		m.ValkeyBusKafkaFallbackDisabled,
	)

	return m
}
