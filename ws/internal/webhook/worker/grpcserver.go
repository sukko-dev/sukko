package worker

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// InternalServerConfig holds dependencies for the internal gRPC server.
// Defined explicitly so main.go can construct it by name.
// Note: no EncryptionKey — decryption is owned by Deliverer, not InternalServer.
type InternalServerConfig struct {
	Cache *WebhookCache
	// TestDeliverFn is injected so tests can replace the delivery implementation.
	// In production, pass (*Deliverer).TestDeliver.
	TestDeliverFn func(ctx context.Context, webhookID, tenantID string) DeliveryResult
	Logger        zerolog.Logger
}

// InternalServer implements WebhookWorkerInternalServiceServer.
// Hosts the TestDeliver RPC for the provisioning service to call synchronously.
type InternalServer struct {
	provisioningv1.UnimplementedWebhookWorkerInternalServiceServer
	cfg InternalServerConfig
}

// NewInternalServer creates an InternalServer.
func NewInternalServer(cfg InternalServerConfig) (*InternalServer, error) {
	if cfg.Cache == nil {
		return nil, errors.New("NewInternalServer: cache is required")
	}
	if cfg.TestDeliverFn == nil {
		return nil, errors.New("NewInternalServer: TestDeliverFn is required")
	}
	return &InternalServer{cfg: cfg}, nil
}

// TestDeliver performs a single synchronous delivery and returns the result.
// Authentication is enforced by WebhookWorkerAuthUnaryInterceptor on the server.
// The status gate does NOT apply — suspended/degraded webhooks are deliverable.
func (s *InternalServer) TestDeliver(ctx context.Context, req *provisioningv1.TestDeliverRequest) (*provisioningv1.TestDeliverResponse, error) {
	webhookID := req.GetWebhookId()
	tenantID := req.GetTenantUuid()

	if webhookID == "" || tenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "webhook_id and tenant_id are required")
	}

	// On cache miss: attempt on-demand hydration, then retry lookup.
	if s.cfg.Cache.GetByID(tenantID, webhookID) == nil {
		if err := s.cfg.Cache.Refresh(ctx, tenantID); err != nil {
			s.cfg.Logger.Warn().Err(err).
				Str("webhook_id", webhookID).Str(logging.LogKeyTenantUUID, tenantID).
				Msg("on-demand cache hydration failed")
			return nil, status.Errorf(codes.Internal, "cache hydration failed: %v", err)
		}
		if s.cfg.Cache.GetByID(tenantID, webhookID) == nil {
			return nil, status.Errorf(codes.NotFound, "webhook %s not found for tenant %s", webhookID, tenantID)
		}
	}

	result := s.cfg.TestDeliverFn(ctx, webhookID, tenantID)

	// StatusLabelNotFound means cache miss post-hydration (defined in delivery.go).
	if result.StatusLabel == StatusLabelNotFound {
		return nil, status.Errorf(codes.NotFound, "webhook %s not found for tenant %s", webhookID, tenantID)
	}

	return &provisioningv1.TestDeliverResponse{
		StatusCode:          int32(result.StatusCode), //nolint:gosec // HTTP status codes are always in [100,599]; int32 is safe.
		LatencyMs:           result.LatencyMS,
		ResponseBodyPreview: result.BodyPreview,
		Error:               result.Error,
	}, nil
}
