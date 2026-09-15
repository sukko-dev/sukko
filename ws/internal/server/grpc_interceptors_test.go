package server

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryUnaryInterceptor_NoPanic(t *testing.T) {
	t.Parallel()

	interceptor := RecoveryUnaryInterceptor(zerolog.Nop())

	handler := func(_ context.Context, _ any) (any, error) {
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
}

func TestRecoveryUnaryInterceptor_Panic(t *testing.T) {
	t.Parallel()

	interceptor := RecoveryUnaryInterceptor(zerolog.Nop())

	handler := func(_ context.Context, _ any) (any, error) {
		panic("test panic")
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected error after panic")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}

func TestRecoveryUnaryInterceptor_HandlerError(t *testing.T) {
	t.Parallel()

	interceptor := RecoveryUnaryInterceptor(zerolog.Nop())

	handler := func(_ context.Context, _ any) (any, error) {
		return nil, status.Error(codes.InvalidArgument, "bad input")
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestMetricsUnaryInterceptor(t *testing.T) {
	t.Parallel()

	interceptor := MetricsUnaryInterceptor()

	handler := func(_ context.Context, _ any) (any, error) {
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Method"}, handler)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
	// Metrics are recorded — can't easily assert Prometheus values in unit tests
	// but the interceptor ran without error
}

func TestLoggingUnaryInterceptor(t *testing.T) {
	t.Parallel()

	interceptor := LoggingUnaryInterceptor(zerolog.Nop())

	tests := []struct {
		name    string
		handler grpc.UnaryHandler
		wantErr bool
	}{
		{
			name: "success",
			handler: func(_ context.Context, _ any) (any, error) {
				return "ok", nil
			},
		},
		{
			name: "error",
			handler: func(_ context.Context, _ any) (any, error) {
				return nil, errors.New("fail")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, tt.handler)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Stream interceptor tests use a mock ServerStream.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func TestRecoveryStreamInterceptor_NoPanic(t *testing.T) {
	t.Parallel()

	interceptor := RecoveryStreamInterceptor(zerolog.Nop())

	handler := func(_ any, _ grpc.ServerStream) error {
		return nil
	}

	err := interceptor(nil, &mockServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRecoveryStreamInterceptor_Panic(t *testing.T) {
	t.Parallel()

	interceptor := RecoveryStreamInterceptor(zerolog.Nop())

	handler := func(_ any, _ grpc.ServerStream) error {
		panic("stream panic")
	}

	err := interceptor(nil, &mockServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected error after panic")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("expected gRPC status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}
