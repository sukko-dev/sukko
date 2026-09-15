package grpcserver

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// WebhookWorkerServer implements WebhookWorkerServiceServer using the provisioning service's
// webhook store. All RPCs are authenticated via WebhookWorkerAuthUnaryInterceptor.
type WebhookWorkerServer struct {
	provisioningv1.UnimplementedWebhookWorkerServiceServer
	store        provisioning.WebhookStore
	invalidation provisioning.WebhookCacheInvalidator
	logger       zerolog.Logger
}

// NewWebhookWorkerServer creates a WebhookWorkerServer backed by the given WebhookStore.
// Returns an error if store is nil (indicates Community edition — worker binary should not start).
// invalidation is optional (nil-safe): when set, UpdateWebhookStatus publishes a cache
// invalidation signal so other worker instances refresh their cache immediately.
func NewWebhookWorkerServer(store provisioning.WebhookStore, invalidation provisioning.WebhookCacheInvalidator, logger zerolog.Logger) (*WebhookWorkerServer, error) {
	if store == nil {
		return nil, errors.New("webhook store is required for WebhookWorkerServer (Pro/Enterprise editions only)")
	}
	return &WebhookWorkerServer{
		store:        store,
		invalidation: invalidation,
		logger:       logger.With().Str("component", "webhook_worker_grpc_server").Logger(),
	}, nil
}

// ListWebhookTenants returns all tenant IDs with at least one active webhook.
func (s *WebhookWorkerServer) ListWebhookTenants(ctx context.Context, _ *provisioningv1.ListWebhookTenantsRequest) (*provisioningv1.ListWebhookTenantsResponse, error) {
	ids, err := s.store.ListTenantIDs(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("ListWebhookTenants: store error")
		return nil, status.Errorf(codes.Internal, "list tenant IDs: %v", err)
	}
	return &provisioningv1.ListWebhookTenantsResponse{TenantUuids: ids}, nil
}

// ListWebhooksForTenant returns all webhook registrations for a tenant for worker cache hydration.
func (s *WebhookWorkerServer) ListWebhooksForTenant(ctx context.Context, req *provisioningv1.ListWebhooksForTenantRequest) (*provisioningv1.ListWebhooksForTenantResponse, error) {
	if req.GetTenantUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	records, err := s.store.ListByTenantForWorker(ctx, req.GetTenantUuid())
	if err != nil {
		s.logger.Error().Err(err).Str(logging.LogKeyTenantUUID, req.GetTenantUuid()).Msg("ListWebhooksForTenant: store error")
		return nil, status.Errorf(codes.Internal, "list webhooks: %v", err)
	}
	protoRecords := make([]*provisioningv1.WebhookRecord, len(records))
	for i, r := range records {
		var lastDeliveryAtMs int64
		if r.LastDeliveryAt != nil {
			lastDeliveryAtMs = r.LastDeliveryAt.UnixMilli()
		}
		protoRecords[i] = &provisioningv1.WebhookRecord{
			Id:               r.ID,
			TenantUuid:       r.TenantID,
			Url:              r.URL,
			ChannelPattern:   r.ChannelPattern,
			SecretEnc:        r.SecretEnc,
			Status:           r.Status,
			MaxRetries:       int32(r.MaxRetries), //nolint:gosec // MaxRetries is validated to 1–10 at write time; int32 overflow is impossible.
			LastDeliveryAtMs: lastDeliveryAtMs,    // 0 when nil — worker treats 0 as "no prior delivery"
		}
	}
	return &provisioningv1.ListWebhooksForTenantResponse{Webhooks: protoRecords}, nil
}

// UpdateWebhookStatus transitions a webhook's status and sets retry_count.
func (s *WebhookWorkerServer) UpdateWebhookStatus(ctx context.Context, req *provisioningv1.UpdateWebhookStatusRequest) (*provisioningv1.UpdateWebhookStatusResponse, error) {
	if req.GetWebhookId() == "" {
		return nil, status.Error(codes.InvalidArgument, "webhook_id is required")
	}
	if req.GetTenantUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}

	err := s.store.UpdateStatus(ctx, req.GetWebhookId(), req.GetTenantUuid(), req.GetStatus(), int(req.GetRetryCount()))
	if err != nil {
		if errors.Is(err, provisioning.ErrWebhookNotFound) {
			return nil, status.Errorf(codes.NotFound, "webhook %s not found", req.GetWebhookId())
		}
		s.logger.Error().Err(err).Str("webhook_id", req.GetWebhookId()).Msg("UpdateWebhookStatus: store error")
		return nil, status.Errorf(codes.Internal, "update status: %v", err)
	}
	if s.invalidation != nil {
		s.invalidation.Publish(req.GetTenantUuid())
	}
	return &provisioningv1.UpdateWebhookStatusResponse{}, nil
}

// RecordDelivery logs a delivery attempt, updates last_delivery_at, and prunes old records.
func (s *WebhookWorkerServer) RecordDelivery(ctx context.Context, req *provisioningv1.RecordDeliveryRequest) (*provisioningv1.RecordDeliveryResponse, error) {
	if req.GetDeliveryId() == "" {
		return nil, status.Error(codes.InvalidArgument, "delivery_id is required")
	}
	if req.GetWebhookId() == "" {
		return nil, status.Error(codes.InvalidArgument, "webhook_id is required")
	}
	if req.GetTenantUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetAttempt() < 1 {
		return nil, status.Error(codes.InvalidArgument, "attempt must be >= 1")
	}
	if sc := req.GetStatusCode(); sc != 0 && (sc < 100 || sc > 599) {
		return nil, status.Error(codes.InvalidArgument, "status_code must be a valid HTTP status (100–599) or 0 for network errors")
	}

	d := &provisioning.WebhookDelivery{
		ID:          req.GetDeliveryId(),
		WebhookID:   req.GetWebhookId(),
		TenantID:    req.GetTenantUuid(),
		Attempt:     int(req.GetAttempt()),
		StatusCode:  int(req.GetStatusCode()),
		LatencyMS:   req.GetLatencyMs(),
		Error:       req.GetError(),
		DeliveredAt: time.UnixMilli(req.GetDeliveredAtMs()).UTC(),
	}

	if err := s.store.RecordDelivery(ctx, d); err != nil {
		s.logger.Error().Err(err).Str("webhook_id", req.GetWebhookId()).Msg("RecordDelivery: store error")
		return nil, status.Errorf(codes.Internal, "record delivery: %v", err)
	}
	return &provisioningv1.RecordDeliveryResponse{}, nil
}
