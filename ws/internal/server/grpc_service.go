package server

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
	"github.com/sukko-dev/sukko/internal/server/backend"
	srvmetrics "github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// GRPCService implements the RealtimeService gRPC server.
// Handles Publish (unary) and Subscribe (server-streaming) RPCs.
// Lives in the server package — direct access to Server internals, no interface indirection.
type GRPCService struct {
	serverv1.UnimplementedRealtimeServiceServer
	servers []*Server
	logger  zerolog.Logger
}

// NewGRPCService creates a RealtimeService implementation.
// Shard selection is handled internally — main.go just passes the server list.
func NewGRPCService(servers []*Server, logger zerolog.Logger) *GRPCService {
	return &GRPCService{
		servers: servers,
		logger:  logger,
	}
}

// selectServer picks a server for an incoming request.
// Single shard: returns the one server (zero overhead).
// Multi-shard: returns the shard with fewest active connections (same logic as WebSocket LoadBalancer).
func (svc *GRPCService) selectServer() *Server {
	if len(svc.servers) == 1 {
		return svc.servers[0]
	}
	best := svc.servers[0]
	bestLoad := best.stats.CurrentConnections.Load()
	for _, s := range svc.servers[1:] {
		load := s.stats.CurrentConnections.Load()
		if load < bestLoad {
			best = s
			bestLoad = load
		}
	}
	return best
}

// Publish sends a message to a channel via the message backend.
// The gateway validates auth/permissions before calling this RPC.
func (svc *GRPCService) Publish(ctx context.Context, req *serverv1.PublishRequest) (*serverv1.PublishResponse, error) {
	// Validate request
	if req.GetChannel() == "" {
		return nil, status.Error(codes.InvalidArgument, "channel is required")
	}
	if len(req.GetData()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "data is required")
	}

	// Select any server (message goes to Kafka → Valkey broadcast → all shards)
	s := svc.selectServer()

	// Publish to message backend (routing rules → Kafka topic)
	mid, err := s.backend.Publish(ctx, 0, req.GetTenantSlug(), req.GetChannel(), req.GetData())
	if err != nil {
		code := publishErrorCode(err)
		// Reject-class and unavailable are expected, non-fatal conditions (client
		// misconfiguration / degraded-but-recovering) — Warn, not Error, to keep the error
		// log clean (§V). Genuine produce failures stay Error.
		evt := svc.logger.Error()
		if code != codes.Internal {
			evt = svc.logger.Warn()
		}
		evt.Err(err).
			Str("channel", req.GetChannel()).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Str("principal", req.GetPrincipal()).
			Str(logging.LogKeyGRPCCode, code.String()).
			Msg("gRPC Publish failed")
		return nil, status.Errorf(code, "publish: %v", err)
	}

	svc.logger.Debug().
		Str("channel", req.GetChannel()).
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Msg("gRPC Publish accepted")

	return &serverv1.PublishResponse{
		Status:  "accepted",
		Channel: req.GetChannel(),
		Mid:     mid,
	}, nil
}

// publishErrorCode maps a backend publish error to a gRPC status code (§XII):
//   - not-routable (no applicable routing rule) → FailedPrecondition (gateway 409)
//   - invalid channel                            → InvalidArgument     (gateway 400)
//   - service unavailable (not-synced/breaker)   → Unavailable         (gateway 503)
//   - everything else (genuine produce failure)  → Internal            (gateway 500)
func publishErrorCode(err error) codes.Code {
	switch {
	case errors.Is(err, backend.ErrPublishNotRoutable):
		return codes.FailedPrecondition
	case errors.Is(err, protocol.ErrInvalidChannel):
		return codes.InvalidArgument
	case errors.Is(err, protocol.ErrServiceUnavailable):
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// Subscribe opens a server-streaming RPC for receiving messages.
// Each stream maps to one virtual client in a shard's subscription index.
func (svc *GRPCService) Subscribe(req *serverv1.SubscribeRequest, stream serverv1.RealtimeService_SubscribeServer) error {
	// Validate request
	if len(req.GetChannels()) == 0 {
		return status.Error(codes.InvalidArgument, "at least one channel is required")
	}

	// Select least-loaded shard (same distribution as WebSocket LoadBalancer)
	s := svc.selectServer()

	// Acquire connection slot (SSE counts toward same pool as WebSocket)
	select {
	case s.connectionsSem <- struct{}{}:
		// acquired
	default:
		return status.Error(codes.ResourceExhausted, "server at connection capacity")
	}

	// Record connection metrics
	srvmetrics.UpdateConnectionMetrics(string(TransportGRPCStream))

	// Create cancellable context for the virtual client's transport
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Create virtual client with gRPC stream transport
	client := s.connections.Get()
	client.transport = NewGRPCStreamTransport(stream, cancel, req.GetRemoteAddr())
	client.server = s
	client.id = s.clientCount.Add(1)
	client.remoteAddr = req.GetRemoteAddr()
	client.tenantID = req.GetTenantSlug()

	// Track client
	s.clients.Store(client, true)
	s.stats.TotalConnections.Add(1)
	s.stats.CurrentConnections.Add(1)

	// Register channel subscriptions
	for _, ch := range req.GetChannels() {
		client.subscriptions.Add(ch)
		s.subscriptionIndex.Add(ch, client)
	}

	// Wire per-tenant broadcast subscription so SSE clients receive broadcasts.
	// Mirrors the WebSocket OnTenantClientConnect call in handlers_message.go.
	if s.tenantHooks != nil && client.tenantID != "" {
		if err := s.tenantHooks.OnTenantClientConnect(client.tenantID); err != nil {
			cancel()
			client.closeOnce.Do(func() {
				if client.transport != nil {
					_ = client.transport.Close()
				}
			})
			client.closeSend()
			s.clients.Delete(client)
			s.stats.TotalConnections.Add(-1)
			s.stats.CurrentConnections.Add(-1)
			s.subscriptionIndex.RemoveMultiple(client.subscriptions.List(), client)
			s.connections.Put(client)
			<-s.connectionsSem
			return status.Error(codes.Unavailable, "broadcast channel unavailable")
		}
	}

	// Start write pump
	s.wg.Go(func() {
		defer logging.RecoverPanic(s.logger, "sse_writePump", nil)
		s.pump.WriteLoop(ctx, client)
	})

	svc.logger.Info().
		Int64("client_id", client.id).
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Str("principal", req.GetPrincipal()).
		Strs("channels", req.GetChannels()).
		Str("remote_addr", req.GetRemoteAddr()).
		Msg("SSE Subscribe stream started")

	// Block until stream closes (client disconnect or server shutdown)
	<-ctx.Done()

	// Cleanup — same pattern as WebSocket disconnectClient
	duration := time.Since(client.connectedAt)
	srvmetrics.RecordDisconnectWithStats(s.stats, string(TransportGRPCStream),
		pkgmetrics.DisconnectClientInitiated, pkgmetrics.InitiatedByClient, duration)

	client.closeOnce.Do(func() {
		if client.transport != nil {
			_ = client.transport.Close()
		}
	})
	if client.clientCancel != nil {
		client.clientCancel()
	}
	client.clientWg.Wait()
	client.closeSend()

	// Notify shard to release per-tenant broadcast channel.
	// Mirrors the OnTenantClientDisconnect call in disconnectClient (client_lifecycle.go).
	if s.tenantHooks != nil && client.tenantID != "" {
		s.tenantHooks.OnTenantClientDisconnect(client.tenantID)
	}

	s.clients.Delete(client)
	s.stats.CurrentConnections.Add(-1)
	s.subscriptionIndex.RemoveMultiple(client.subscriptions.List(), client)
	s.connections.Put(client)
	<-s.connectionsSem // Release connection slot

	svc.logger.Info().
		Int64("client_id", client.id).
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Dur("connection_duration", duration).
		Msg("SSE Subscribe stream ended")

	return nil
}
