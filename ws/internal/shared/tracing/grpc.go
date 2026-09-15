package tracing

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// StatsHandler returns a gRPC ServerOption that creates spans for each RPC.
// When tracing is disabled (noop TracerProvider), this adds zero overhead.
// Use with grpc.NewServer(tracing.StatsHandler()).
func StatsHandler() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}
