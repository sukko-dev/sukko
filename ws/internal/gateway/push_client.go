package gateway

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
)

// PushForwarder defines the push service operations used by gateway handlers.
// Implemented by PushClient (production) and mocks (tests).
type PushForwarder interface {
	RegisterDevice(ctx context.Context, req *pushv1.RegisterDeviceRequest) (*pushv1.RegisterDeviceResponse, error)
	UnregisterDevice(ctx context.Context, req *pushv1.UnregisterDeviceRequest) (*pushv1.UnregisterDeviceResponse, error)
	GetVAPIDKey(ctx context.Context, req *pushv1.GetVAPIDKeyRequest) (*pushv1.GetVAPIDKeyResponse, error)
}

// PushClient communicates with the push service's PushService via gRPC.
// Provides RegisterDevice, UnregisterDevice, and GetVAPIDKey operations.
type PushClient struct {
	conn   *grpc.ClientConn
	client pushv1.PushServiceClient
	logger zerolog.Logger
}

// Compile-time interface assertion.
var _ PushForwarder = (*PushClient)(nil)

// NewPushClient creates a gRPC client connected to the push service.
func NewPushClient(addr string, logger zerolog.Logger) (*PushClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial push service gRPC at %s: %w", addr, err)
	}

	return &PushClient{
		conn:   conn,
		client: pushv1.NewPushServiceClient(conn),
		logger: logger.With().Str("component", "push_client").Logger(),
	}, nil
}

// RegisterDevice forwards a device registration request to the push service.
func (pc *PushClient) RegisterDevice(ctx context.Context, req *pushv1.RegisterDeviceRequest) (*pushv1.RegisterDeviceResponse, error) {
	resp, err := pc.client.RegisterDevice(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("push RegisterDevice: %w", err)
	}
	return resp, nil
}

// UnregisterDevice forwards a device unregistration request to the push service.
func (pc *PushClient) UnregisterDevice(ctx context.Context, req *pushv1.UnregisterDeviceRequest) (*pushv1.UnregisterDeviceResponse, error) {
	resp, err := pc.client.UnregisterDevice(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("push UnregisterDevice: %w", err)
	}
	return resp, nil
}

// GetVAPIDKey forwards a VAPID key request to the push service.
func (pc *PushClient) GetVAPIDKey(ctx context.Context, req *pushv1.GetVAPIDKeyRequest) (*pushv1.GetVAPIDKeyResponse, error) {
	resp, err := pc.client.GetVAPIDKey(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("push GetVAPIDKey: %w", err)
	}
	return resp, nil
}

// Close closes the gRPC connection.
func (pc *PushClient) Close() error {
	if pc.conn == nil {
		return nil
	}
	if err := pc.conn.Close(); err != nil {
		return fmt.Errorf("close push client gRPC connection: %w", err)
	}
	return nil
}
