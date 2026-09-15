package gateway

import (
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
)

// ServerClient communicates with the ws-server's RealtimeService via gRPC.
// Provides Publish (unary) and Subscribe (server-streaming) operations.
type ServerClient struct {
	conn   *grpc.ClientConn
	client serverv1.RealtimeServiceClient
	logger zerolog.Logger
}

// NewServerClient creates a gRPC client connected to the ws-server.
func NewServerClient(addr string, logger zerolog.Logger) (*ServerClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial ws-server gRPC at %s: %w", addr, err)
	}

	return &ServerClient{
		conn:   conn,
		client: serverv1.NewRealtimeServiceClient(conn),
		logger: logger.With().Str("component", "server_client").Logger(),
	}, nil
}

// Client returns the underlying gRPC client for direct RPC calls.
func (sc *ServerClient) Client() serverv1.RealtimeServiceClient {
	return sc.client
}

// Close closes the gRPC connection.
func (sc *ServerClient) Close() error {
	if sc.conn == nil {
		return nil
	}
	if err := sc.conn.Close(); err != nil {
		return fmt.Errorf("close server client gRPC connection: %w", err)
	}
	return nil
}
