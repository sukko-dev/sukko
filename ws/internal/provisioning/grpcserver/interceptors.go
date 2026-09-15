// Package grpcserver provides the gRPC server for provisioning internal streaming.
package grpcserver

import (
	"context"
	"crypto/subtle"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// Prometheus metrics for gRPC.
var (
	grpcRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "provisioning_grpc_request_duration_seconds",
		Help:    "Duration of gRPC requests in seconds",
		Buckets: pkgmetrics.APILatencyBuckets,
	}, []string{"method"})

	grpcRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "provisioning_grpc_requests_total",
		Help: "Total number of gRPC requests",
	}, []string{"method", "status"})
)

// RecoveryStreamInterceptor catches panics in stream handlers and returns
// codes.Internal. Inline defer/recover is used (not logging.RecoverPanic) because
// this interceptor must assign the named return err from within the deferred function.
func RecoveryStreamInterceptor(logger zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error().
					Interface("panic", r).
					Str("method", info.FullMethod).
					Str("stack_trace", stack).
					Msg("panic recovered in gRPC stream handler")
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

// WebhookWorkerAuthUnaryInterceptor enforces shared-secret authentication on every
// call to the webhook-worker gRPC server. This interceptor is applied to a dedicated
// gRPC server (WEBHOOK_WORKER_GRPC_PORT) that serves only WebhookWorkerService — so
// no FullMethod filtering is needed. §IX: default deny if the configured token is empty.
func WebhookWorkerAuthUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "internal token not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		values := md.Get(platform.GRPCInternalTokenMetadataKey)
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing internal token")
		}
		if subtle.ConstantTimeCompare([]byte(values[0]), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid internal token")
		}
		return handler(ctx, req)
	}
}

// LoggingUnaryInterceptor logs gRPC unary calls at Info level (Warn on error),
// consistent with LoggingStreamInterceptor.
func LoggingUnaryInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		event := logger.Info()
		if err != nil {
			event = logger.Warn().Err(err)
		}
		event.
			Str("method", info.FullMethod).
			Str("code", code.String()).
			Dur("duration", time.Since(start)).
			Msg("gRPC unary call")
		return resp, err
	}
}

// MetricsUnaryInterceptor records Prometheus metrics for gRPC unary calls:
// request duration histogram and request counter by method and status.
func MetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		grpcRequestsTotal.WithLabelValues(info.FullMethod, code.String()).Inc()
		return resp, err
	}
}

// RecoveryUnaryInterceptor recovers from panics in unary handlers, logs the panic
// value and full stack trace, and returns codes.Internal to the caller.
// Inline defer/recover is used instead of logging.RecoverPanic because the interceptor
// must assign the named return err = status.Errorf(...) from within the deferred
// function — logging.RecoverPanic cannot assign to a caller's named return value.
func RecoveryUnaryInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				logger.Error().
					Str("method", info.FullMethod).
					Interface("panic", r).
					Str("stack_trace", stack).
					Msg("panic recovered in gRPC unary handler")
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
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
