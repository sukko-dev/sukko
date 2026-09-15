package worker

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// All metrics use the webhook_ prefix (§VI).
var (
	// deliveriesTotal counts delivery attempts by outcome label.
	// Labels: status = "success"|"failure"|"timeout"|"ssrf_blocked"|"decrypt_failed"|"dropped"
	// NOTE: "decrypt_failed" is the METRIC LABEL; "decryption_failure" is the RecordDelivery error string.
	deliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_total",
		Help: "Total webhook delivery attempts by outcome.",
	}, []string{"status"})

	// deliveryDuration is a histogram of per-attempt latency (§VI: histograms for latency).
	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_delivery_duration_seconds",
		Help:    "Webhook delivery attempt latency in seconds.",
		Buckets: pkgmetrics.APILatencyBuckets,
	})

	// retryTotal counts retry attempts by 1-indexed attempt number.
	retryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_retry_total",
		Help: "Webhook retry attempts by attempt number.",
	}, []string{"attempt"}) // "1".."5" or "5+" for attempt >= 5

	degradedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_degraded_total",
		Help: "Total webhooks transitioned to degraded state after exhausting retries.",
	})

	degradedRecoveredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_degraded_recovered_total",
		Help: "Total webhooks recovered from degraded state after a successful delivery.",
	})

	// cacheSizeGauge reflects the current number of tenant entries in the webhook cache.
	// Updated inside WebhookCache.Refresh() and Hydrate() under write lock.
	cacheSizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_cache_size",
		Help: "Number of tenant entries currently in the webhook in-memory cache.",
	})

	recordDeliveryFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_record_delivery_failures_total",
		Help: "Total failures to record a delivery attempt in the provisioning store.",
	})

	statusUpdateFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_status_update_failures_total",
		Help: "Total failures to update a webhook status via the provisioning gRPC API.",
	})

	// gRPC metrics for the worker's internal TestDeliver server.
	// Uses webhook_ prefix (§VI) — NOT provisioning_grpc_* from grpcserver.MetricsUnaryInterceptor.
	webhookGRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_grpc_requests_total",
		Help: "Total gRPC requests handled by the webhook-worker internal server by method and code.",
	}, []string{"method", "code"})

	webhookGRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_grpc_request_duration_seconds",
		Help:    "Latency of gRPC requests handled by the webhook-worker internal server.",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"method"})
)

// WebhookMetricsUnaryInterceptor returns a gRPC server interceptor that records
// webhook_grpc_requests_total and webhook_grpc_request_duration_seconds metrics.
// Used instead of grpcserver.MetricsUnaryInterceptor (which emits provisioning_grpc_* names).
func WebhookMetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start).Seconds()
		code := status.Code(err).String()
		webhookGRPCRequestsTotal.WithLabelValues(info.FullMethod, code).Inc()
		webhookGRPCDuration.WithLabelValues(info.FullMethod).Observe(elapsed)
		return resp, err
	}
}
