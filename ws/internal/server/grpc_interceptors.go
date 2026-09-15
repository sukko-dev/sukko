package server

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// Prometheus metrics for ws-server gRPC. Uses ws_ prefix per Constitution VI.
var (
	grpcRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ws_grpc_request_duration_seconds",
		Help:    "Duration of gRPC requests in seconds",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"method"})

	grpcRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ws_grpc_requests_total",
		Help: "Total number of gRPC requests by method and status",
	}, []string{"method", "status"})
)

// =============================================================================
// Unary Interceptors (for Publish RPC)
// =============================================================================

// RecoveryUnaryInterceptor catches panics in unary handlers and returns
// codes.Internal. Logs the panic value with structured fields per Constitution V.
func RecoveryUnaryInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error().
					Interface("panic", r).
					Str("method", info.FullMethod).
					Str("location", "grpc_unary_"+info.FullMethod).
					Msg("Recovered from panic in gRPC unary handler")
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// LoggingUnaryInterceptor logs gRPC unary request lifecycle with method name,
// duration, and status code using zerolog.
func LoggingUnaryInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := status.Code(err)

		event := logger.Info()
		if err != nil {
			event = logger.Warn().Err(err)
		}

		event.
			Str("method", info.FullMethod).
			Dur("duration", duration).
			Str("status", code.String()).
			Msg("gRPC unary request")

		return resp, err
	}
}

// MetricsUnaryInterceptor records Prometheus metrics for gRPC unary requests:
// request duration histogram and request counter by method and status.
func MetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := status.Code(err)

		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
		grpcRequestsTotal.WithLabelValues(info.FullMethod, code.String()).Inc()

		return resp, err
	}
}

// =============================================================================
// Stream Interceptors (for Subscribe RPC)
// =============================================================================

// RecoveryStreamInterceptor catches panics in stream handlers and returns
// codes.Internal. Logs the panic value with structured fields per Constitution V.
// Note: Cannot use logging.RecoverPanic here because the interceptor must
// capture the panic to return a gRPC error status. logging.RecoverPanic
// re-panics would not propagate the gRPC status correctly.
func RecoveryStreamInterceptor(logger zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error().
					Interface("panic", r).
					Str("method", info.FullMethod).
					Str("location", "grpc_stream_"+info.FullMethod).
					Msg("Recovered from panic in gRPC stream handler")
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(srv, ss)
	}
}

// LoggingStreamInterceptor logs gRPC stream lifecycle with method name,
// duration, and status code using zerolog.
func LoggingStreamInterceptor(logger zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		logger.Info().
			Str("method", info.FullMethod).
			Msg("gRPC stream started")

		err := handler(srv, ss)

		duration := time.Since(start)
		code := status.Code(err)

		event := logger.Info()
		if err != nil {
			event = logger.Warn().Err(err)
		}

		event.
			Str("method", info.FullMethod).
			Dur("duration", duration).
			Str("status", code.String()).
			Msg("gRPC stream ended")

		return err
	}
}

// MetricsStreamInterceptor records Prometheus metrics for gRPC streams:
// request duration histogram and request counter by method and status.
func MetricsStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		err := handler(srv, ss)

		duration := time.Since(start)
		code := status.Code(err)

		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration.Seconds())
		grpcRequestsTotal.WithLabelValues(info.FullMethod, code.String()).Inc()

		return err
	}
}
