// Package metrics provides Prometheus metrics for the WebSocket server.
// All metrics use promauto for automatic registration.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sukko-dev/sukko/internal/server/stats"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Buffer percentile label values (package-local).
const bufferPercentileAll = "all"

// Resource type label values for capacity metrics (package-local).
const (
	resourceCPU    = "cpu"
	resourceMemory = "memory"
)

// =============================================================================
// Connection Metrics
// =============================================================================

var (
	connectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_connections_total",
		Help: "Total number of connections established by transport type",
	}, []string{"transport"})

	connectionsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ws_connections_active",
		Help: "Current number of active connections by transport type",
	}, []string{"transport"})

	connectionsMax = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_connections_max",
		Help: "Maximum allowed WebSocket connections",
	})

	// ConnectionsFailed tracks total failed connection attempts.
	ConnectionsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_connections_failed_total",
		Help: "Total number of failed connection attempts",
	})

	disconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_disconnects_total",
		Help: "Total disconnections by transport, reason, and who initiated",
	}, []string{"transport", "reason", "initiated_by"})

	connectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ws_connection_duration_seconds",
		Help:    "Connection duration before disconnect by transport and reason",
		Buckets: pkgmetrics.ConnectionDurationBuckets,
	}, []string{"transport", "reason"})
)

// =============================================================================
// Message Metrics
// =============================================================================

var (
	messagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_messages_sent_total",
		Help: "Total number of messages sent to clients by transport type",
	}, []string{"transport"})

	messagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_messages_received_total",
		Help: "Total number of messages received from clients",
	})

	bytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_bytes_sent_total",
		Help: "Total number of bytes sent to clients",
	})

	bytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_bytes_received_total",
		Help: "Total number of bytes received from clients",
	})
)

// =============================================================================
// Reliability Metrics
// =============================================================================

// slowClientAttemptsBuckets defines histogram buckets for tracking send attempts
// before a slow client is disconnected. Values 1-5 cover common disconnect counts,
// 10 and 20 capture outliers.
var slowClientAttemptsBuckets = []float64{1, 2, 3, 4, 5, 10, 20}

var (
	slowClientsDisconnected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_slow_clients_disconnected_total",
		Help: "Total number of slow clients disconnected by transport type",
	}, []string{"transport"})

	rateLimitedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_rate_limited_messages_total",
		Help: "Total number of rate limited messages",
	})

	// Renamed from ws_replay_requests_total to distinguish history/reconnect replays
	// from live-connection gap recovery replays (ws_live_replay_requests_total).
	replayRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_history_replay_requests_total",
		Help: "Total number of history replay requests served (reconnect path).",
	})

	// GapNotificationsEnqueued counts gap notifications successfully sent to a client's gapChan.
	GapNotificationsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_gap_notifications_enqueued_total",
		Help: "Gap notifications successfully enqueued on a client's low-priority gap channel.",
	})

	// GapNotificationsDropped counts gap notifications that could not be delivered.
	// Three drop causes: buffer_full (gapChan at capacity), coalesce_loss (cross-channel
	// item consumed during merging), send_full (c.send full when pump tried to forward).
	GapNotificationsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_gap_notifications_dropped_total",
		Help: "Gap notifications dropped, by reason (buffer_full, coalesce_loss, or send_full).",
	}, []string{"reason"})

	// LiveReplayRequestsTotal counts live replay requests by outcome label.
	LiveReplayRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_live_replay_requests_total",
		Help: "Live replay requests by outcome.",
	}, []string{"outcome"})

	// LiveReplayResponseDropped counts replay terminal responses (replay_complete or error)
	// dropped because the client's send channel was full when the write was attempted.
	LiveReplayResponseDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_live_replay_response_dropped_total",
		Help: "Replay terminal responses dropped because c.send was full, by kind (complete or error).",
	}, []string{"kind"})

	// LiveReplayDurationSeconds measures live replay end-to-end latency (Kafka fetch + delivery).
	// Observed for completed and timeout outcomes; not observed for fast rejections.
	LiveReplayDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ws_live_replay_duration_seconds",
		Help:    "Duration of live replay requests from goroutine start to completion or timeout.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	})

	connectionRateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_connection_rate_limited_total",
		Help: "Total number of connections rate limited by type (per_ip or global)",
	}, []string{"type"})

	droppedBroadcastsDetailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_dropped_broadcasts_detailed_total",
		Help: "Total broadcast messages dropped by channel and reason",
	}, []string{"channel", "reason"})

	clientSendBufferSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ws_client_send_buffer_size",
		Help:    "Distribution of client send buffer usage",
		Buckets: pkgmetrics.BufferSaturationBuckets,
	}, []string{"percentile"})

	slowClientAttempts = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ws_slow_client_attempts_before_disconnect",
		Help:    "Distribution of send attempts before slow client disconnect",
		Buckets: slowClientAttemptsBuckets,
	})
)

// =============================================================================
// System Metrics
// =============================================================================

var (
	memoryUsageBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_memory_bytes",
		Help: "Current memory usage in bytes",
	})

	memoryLimitBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_memory_limit_bytes",
		Help: "Memory limit in bytes (from cgroup)",
	})

	// CPUUsagePercent tracks current CPU usage as a percentage.
	CPUUsagePercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_cpu_usage_percent",
		Help: "Current CPU usage percentage (container-aware: % of allocated CPUs)",
	})

	// CPUContainerPercent tracks CPU usage as a percentage of container allocation.
	CPUContainerPercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_cpu_container_percent",
		Help: "CPU usage as percentage of container allocation (0-100%)",
	})

	// CPUHostPercent tracks CPU usage as a percentage of total host CPUs.
	CPUHostPercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_cpu_host_percent",
		Help: "CPU usage as percentage of total host CPUs (for reference)",
	})

	// CPUSmoothedPercent tracks EWMA-smoothed CPU usage for load-shedding decisions.
	// Smoothing prevents transient spikes from triggering false emergency brakes.
	CPUSmoothedPercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_cpu_smoothed_percent",
		Help: "EWMA-smoothed CPU usage percentage used for load-shedding decisions",
	})

	// CPUAllocationCores tracks the number of CPU cores allocated to the container.
	CPUAllocationCores = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_cpu_allocation_cores",
		Help: "Number of CPU cores allocated to container",
	})

	// CPUThrottledSecondsTotal tracks the total time container CPU was throttled.
	CPUThrottledSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_cpu_throttled_seconds_total",
		Help: "Total time (seconds) container CPU was throttled by cgroup",
	})

	// CPUThrottleEventsTotal tracks the number of times the container hit its CPU limit.
	CPUThrottleEventsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_cpu_throttle_events_total",
		Help: "Total number of times container hit CPU limit and was throttled",
	})

	goroutinesActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_goroutines_active",
		Help: "Current number of active goroutines",
	})
)

// =============================================================================
// Kafka Metrics
// =============================================================================

var (
	kafkaConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_kafka_connected",
		Help: "Kafka consumer status (1=running, 0=stopped)",
	})

	kafkaMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_kafka_messages_received_total",
		Help: "Total number of messages received from Kafka",
	}, []string{"topic", "consumer_group"})

	kafkaMessagesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_kafka_messages_dropped_total",
		Help: "Total number of Kafka messages dropped due to backpressure",
	}, []string{"topic", "consumer_group"})

	kafkaMessagesPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_kafka_messages_published_total",
		Help: "Total number of messages published to Kafka by clients",
	})

	kafkaPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_kafka_publish_errors_total",
		Help: "Total number of failed publish attempts to Kafka",
	})

	kafkaConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ws_kafka_consumer_lag",
		Help: "Kafka consumer lag (high watermark - committed offset) per topic and consumer group.",
	}, []string{"topic", "consumer_group"})
)

// =============================================================================
// Backend-Agnostic Metrics
// =============================================================================

var (
	backendMessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_messages_published_total",
		Help: "Total messages published through the message backend",
	}, []string{"backend"})

	backendMessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_messages_consumed_total",
		Help: "Total messages consumed from the message backend",
	}, []string{"backend"})

	backendPublishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_publish_errors_total",
		Help: "Total publish errors by message backend",
	}, []string{"backend"})

	backendPublishRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_publish_rejected_total",
		Help: "Total publishes rejected as not routable (no applicable routing rule) by message backend. Distinct from ws_backend_publish_errors_total (infrastructure failures) so tenant misconfiguration does not pollute the error alert series.",
	}, []string{"backend"})

	backendReplayRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_replay_requests_total",
		Help: "Total replay requests by message backend",
	}, []string{"backend"})

	backendReplayMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_backend_replay_messages_total",
		Help: "Total messages replayed by message backend",
	}, []string{"backend"})

	backendPublishLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ws_backend_publish_latency_seconds",
		Help:    "Publish latency by message backend",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"backend"})

	backendHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ws_backend_healthy",
		Help: "Message backend health status (1=healthy, 0=unhealthy)",
	}, []string{"backend"})
)

// =============================================================================
// Capacity Metrics
// =============================================================================

var (
	capacityMaxConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_capacity_max_connections",
		Help: "Current dynamic maximum connections allowed",
	})

	capacityCPUThreshold = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_capacity_cpu_threshold_percent",
		Help: "CPU threshold for rejecting new connections",
	})

	capacityRejectionsCPU = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_capacity_rejections_total",
		Help: "Total connection rejections by reason",
	}, []string{"reason"})

	capacityAvailableHeadroom = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ws_capacity_headroom_percent",
		Help: "Available resource headroom (CPU and memory)",
	}, []string{"resource"})
)

// =============================================================================
// Error Metrics
// =============================================================================

var (
	errorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_errors_total",
		Help: "Total errors by type and severity",
	}, []string{"type", "severity"})
)

// =============================================================================
// Multi-Tenant Pool Metrics
// =============================================================================

var (
	multitenantTopicsSubscribed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_multitenant_topics_subscribed",
		Help: "Number of topics subscribed by shared consumer",
	})

	multitenantDedicatedConsumers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_multitenant_dedicated_consumers",
		Help: "Number of dedicated consumer instances for isolated tenants",
	})

	multitenantMessagesRouted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_multitenant_messages_routed_total",
		Help: "Total messages routed through multi-tenant pool",
	})

	multitenantRefreshTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_multitenant_refresh_total",
		Help: "Topic refresh operations by result",
	}, []string{"result"})
)

// =============================================================================
// Connection Helper Functions
// =============================================================================

// UpdateConnectionMetrics increments the connection counter.
func UpdateConnectionMetrics(transportType string) {
	connectionsTotal.WithLabelValues(transportType).Inc()
}

// SetAggregatedConnectionMetrics sets the aggregated connection metrics from LoadBalancer.
// Uses empty transport label — aggregated across all transports.
func SetAggregatedConnectionMetrics(totalConnections, totalMaxConnections int64) {
	// Aggregated metrics don't have a transport label — they're set by the LoadBalancer
	// which sums across all shards and transport types.
	connectionsActive.WithLabelValues("").Set(float64(totalConnections))
	connectionsMax.Set(float64(totalMaxConnections))
}

// RecordDisconnect tracks a disconnect with transport, reason, initiator, and duration.
func RecordDisconnect(transportType, reason, initiatedBy string, duration time.Duration) {
	disconnectsTotal.WithLabelValues(transportType, reason, initiatedBy).Inc()
	connectionDuration.WithLabelValues(transportType, reason).Observe(duration.Seconds())
}

// RecordDisconnectWithStats tracks a disconnect and updates both Prometheus and Stats.
func RecordDisconnectWithStats(s *stats.Stats, transportType, reason, initiatedBy string, duration time.Duration) {
	disconnectsTotal.WithLabelValues(transportType, reason, initiatedBy).Inc()
	connectionDuration.WithLabelValues(transportType, reason).Observe(duration.Seconds())

	s.RecordDisconnect(reason)
}

// =============================================================================
// Message Helper Functions
// =============================================================================

// UpdateMessageMetrics updates message-related metrics.
// transportType is used for sent metrics (per-transport). Received is transport-agnostic
// (only WebSocket clients send messages — ReadLoop handles this).
func UpdateMessageMetrics(transportType string, sent, received int64) {
	if sent > 0 {
		messagesSent.WithLabelValues(transportType).Add(float64(sent))
	}
	if received > 0 {
		messagesReceived.Add(float64(received))
	}
}

// UpdateBytesMetrics updates bytes sent/received metrics.
func UpdateBytesMetrics(sent, received int64) {
	if sent > 0 {
		bytesSent.Add(float64(sent))
	}
	if received > 0 {
		bytesReceived.Add(float64(received))
	}
}

// =============================================================================
// Reliability Helper Functions
// =============================================================================

// IncrementSlowClientDisconnects increments slow client disconnect counter by transport type.
func IncrementSlowClientDisconnects(transportType string) {
	slowClientsDisconnected.WithLabelValues(transportType).Inc()
}

// IncrementRateLimitedMessages increments rate limited message counter.
func IncrementRateLimitedMessages() {
	rateLimitedMessages.Inc()
}

// IncrementReplayRequests increments replay request counter.
func IncrementReplayRequests() {
	replayRequests.Inc()
}

// IncrementConnectionRateLimit increments connection rate limit counter.
func IncrementConnectionRateLimit(limitType string) {
	connectionRateLimited.WithLabelValues(limitType).Inc()
}

// RecordDroppedBroadcast tracks a dropped broadcast message.
func RecordDroppedBroadcast(channel, reason string) {
	droppedBroadcastsDetailed.WithLabelValues(channel, reason).Inc()
}

// RecordDroppedBroadcastWithStats tracks a dropped broadcast and updates Stats.
func RecordDroppedBroadcastWithStats(s *stats.Stats, channel, reason string) {
	droppedBroadcastsDetailed.WithLabelValues(channel, reason).Inc()

	s.RecordDroppedBroadcast(channel)
}

// RecordDroppedGapNotification records a dropped gap notification with the reason label.
// reason must be one of GapDropReasonBufferFull, GapDropReasonCoalesceLoss, or GapDropReasonSendFull (server package).
func RecordDroppedGapNotification(reason string) {
	GapNotificationsDropped.WithLabelValues(reason).Inc()
}

// RecordDroppedReplayResponse records a dropped replay terminal message (complete or error)
// because the client's send channel was full. kind must be "complete" or "error".
func RecordDroppedReplayResponse(kind string) {
	LiveReplayResponseDropped.WithLabelValues(kind).Inc()
}

// RecordSlowClientAttempt records send attempts before slow client disconnect.
func RecordSlowClientAttempt(attempts int) {
	slowClientAttempts.Observe(float64(attempts))
}

// RecordClientBufferSize samples a client's send buffer usage.
func RecordClientBufferSize(bufferLen, _ int) {
	clientSendBufferSize.WithLabelValues(bufferPercentileAll).Observe(float64(bufferLen))
}

// RecordClientBufferSizeWithStats samples buffer and updates Stats.
func RecordClientBufferSizeWithStats(s *stats.Stats, bufferLen, bufferCap, maxSamples int) {
	clientSendBufferSize.WithLabelValues(bufferPercentileAll).Observe(float64(bufferLen))

	usagePercent := int(float64(bufferLen) / float64(bufferCap) * 100)
	s.AddBufferSample(usagePercent, maxSamples)
}

// =============================================================================
// Kafka Helper Functions
// =============================================================================

// SetKafkaConnected sets the Kafka connection status.
func SetKafkaConnected(connected bool) {
	if connected {
		kafkaConnected.Set(1)
	} else {
		kafkaConnected.Set(0)
	}
}

// IncrementKafkaMessages increments Kafka message counter with topic and consumer group labels.
func IncrementKafkaMessages(topic, consumerGroup string) {
	kafkaMessagesReceived.WithLabelValues(topic, consumerGroup).Inc()
}

// IncrementKafkaDropped increments dropped Kafka message counter with topic and consumer group labels.
func IncrementKafkaDropped(topic, consumerGroup string) {
	kafkaMessagesDropped.WithLabelValues(topic, consumerGroup).Inc()
}

// IncrementMessagesPublished increments Kafka publish success counter.
func IncrementMessagesPublished() {
	kafkaMessagesPublished.Inc()
}

// IncrementPublishErrors increments Kafka publish error counter.
func IncrementPublishErrors() {
	kafkaPublishErrors.Inc()
}

// SetKafkaConsumerLag sets the consumer lag for a topic and consumer group.
// Lag = high watermark - committed offset. Updated on periodic consumer refresh (cold path).
func SetKafkaConsumerLag(topic, consumerGroup string, lag int64) {
	kafkaConsumerLag.WithLabelValues(topic, consumerGroup).Set(float64(lag))
}

// =============================================================================
// Backend Helper Functions
// =============================================================================

// RecordBackendPublish increments the backend publish counter.
func RecordBackendPublish(backend string) {
	backendMessagesPublished.WithLabelValues(backend).Inc()
}

// RecordBackendConsume increments the backend consume counter.
func RecordBackendConsume(backend string) {
	backendMessagesConsumed.WithLabelValues(backend).Inc()
}

// RecordBackendPublishError increments the backend publish error counter.
func RecordBackendPublishError(backend string) {
	backendPublishErrors.WithLabelValues(backend).Inc()
}

// RecordBackendPublishRejected increments the not-routable reject counter. Reject-class
// publishes (no applicable routing rule) MUST use this instead of RecordBackendPublishError
// so tenant misconfiguration stays out of the infrastructure error alert series.
func RecordBackendPublishRejected(backend string) {
	backendPublishRejected.WithLabelValues(backend).Inc()
}

// RecordBackendReplayRequest increments the backend replay request counter.
func RecordBackendReplayRequest(backend string) {
	backendReplayRequests.WithLabelValues(backend).Inc()
}

// RecordBackendReplayMessages increments the replay messages counter.
func RecordBackendReplayMessages(backend string, count int) {
	backendReplayMessages.WithLabelValues(backend).Add(float64(count))
}

// RecordBackendPublishLatency records publish latency for a backend.
func RecordBackendPublishLatency(backend string, seconds float64) {
	backendPublishLatency.WithLabelValues(backend).Observe(seconds)
}

// SetBackendHealthy sets the backend health gauge.
func SetBackendHealthy(backend string, healthy bool) {
	if healthy {
		backendHealthy.WithLabelValues(backend).Set(1)
	} else {
		backendHealthy.WithLabelValues(backend).Set(0)
	}
}

// =============================================================================
// Capacity Helper Functions
// =============================================================================

// UpdateCapacityMetrics updates dynamic capacity metrics.
func UpdateCapacityMetrics(maxConnections int, cpuThreshold float64) {
	capacityMaxConnections.Set(float64(maxConnections))
	capacityCPUThreshold.Set(cpuThreshold)
}

// IncrementCapacityRejection records a connection rejection with reason.
func IncrementCapacityRejection(reason string) {
	capacityRejectionsCPU.WithLabelValues(reason).Inc()
}

// UpdateCapacityHeadroom updates available resource headroom.
func UpdateCapacityHeadroom(cpuHeadroom, memHeadroom float64) {
	capacityAvailableHeadroom.WithLabelValues(resourceCPU).Set(cpuHeadroom)
	capacityAvailableHeadroom.WithLabelValues(resourceMemory).Set(memHeadroom)
}

// =============================================================================
// Error Helper Functions
// =============================================================================

// RecordError tracks an error.
func RecordError(errorType, severity string) {
	errorsTotal.WithLabelValues(errorType, severity).Inc()
}

// RecordKafkaError tracks Kafka-related errors.
func RecordKafkaError(severity string) {
	errorsTotal.WithLabelValues(pkgmetrics.ErrorTypeKafka, severity).Inc()
}

// RecordBroadcastError tracks broadcast-related errors.
func RecordBroadcastError(severity string) {
	errorsTotal.WithLabelValues(pkgmetrics.ErrorTypeBroadcast, severity).Inc()
}

// RecordSerializationError tracks serialization errors.
func RecordSerializationError(severity string) {
	errorsTotal.WithLabelValues(pkgmetrics.ErrorTypeSerialization, severity).Inc()
}

// RecordConnectionError tracks connection errors.
func RecordConnectionError(severity string) {
	errorsTotal.WithLabelValues(pkgmetrics.ErrorTypeConnection, severity).Inc()
}

// =============================================================================
// Multi-Tenant Pool Metrics Adapter
// =============================================================================

// MultiTenantPoolMetricsAdapter implements pkgmetrics.PoolMetrics for Prometheus.
type MultiTenantPoolMetricsAdapter struct{}

// Interface compliance check.
var _ pkgmetrics.PoolMetrics = (*MultiTenantPoolMetricsAdapter)(nil)

// OnMessageRouted records a routed message.
func (a *MultiTenantPoolMetricsAdapter) OnMessageRouted() {
	multitenantMessagesRouted.Inc()
}

// OnRefresh records a topic refresh operation and updates gauges.
func (a *MultiTenantPoolMetricsAdapter) OnRefresh(success bool, topicsSubscribed, dedicatedConsumers int) {
	if success {
		multitenantRefreshTotal.WithLabelValues(pkgmetrics.ResultSuccess).Inc()
		multitenantTopicsSubscribed.Set(float64(topicsSubscribed))
		multitenantDedicatedConsumers.Set(float64(dedicatedConsumers))
	} else {
		multitenantRefreshTotal.WithLabelValues(pkgmetrics.ResultError).Inc()
	}
}

// =============================================================================
// Reconnect Metrics
// =============================================================================

// ReconnectPosDecodeFailures counts reconnect last_pos channels where the client-supplied
// pos could not be decoded (malformed pos or unknown topic). The channel is skipped — it
// is not replayed (ADR-0020: reconnect replay is scoped to the decodable, authorized
// channels the client named, never a fallback full replay).
var ReconnectPosDecodeFailures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ws_reconnect_pos_decode_failure_total",
	Help: "Reconnect last_pos channels where the client pos was undecodable or the topic was unknown; the channel is skipped (not replayed).",
})

// ReconnectChannelDenied counts reconnect last_pos channels rejected because the
// channel is not owned by the connection's authenticated tenant (ADR-0020). A
// persistent nonzero rate flags a misbehaving or hostile client probing other
// tenants' channels.
var ReconnectChannelDenied = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ws_reconnect_channel_denied_total",
	Help: "Reconnect last_pos channels denied because they are not owned by the connection's authenticated tenant.",
})

// =============================================================================
// HTTP Handler
// =============================================================================

// HandleMetrics serves Prometheus metrics at /metrics endpoint.
func HandleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}
