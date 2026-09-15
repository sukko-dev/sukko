// Package grpcserver implements the PushService gRPC server for device
// registration, unregistration, and VAPID key management.
package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// ConfigCache provides push credential data from the provisioning system.
// Same interface as push.ConfigCache — scoped to the credential lookup needed
// by the gRPC server (VAPID key retrieval).
type ConfigCache interface {
	// GetCredential returns provider credentials for a tenant and provider type.
	// Returns nil, nil when no credential exists for the tenant/provider pair.
	GetCredential(tenantID, provider string) (json.RawMessage, error)
}

// validPlatforms enumerates the allowed push notification platforms.
var validPlatforms = map[string]bool{
	"web":     true,
	"android": true,
	"ios":     true,
}

// ServerConfig holds the dependencies for creating a push gRPC server.
type ServerConfig struct {
	Repo        repository.SubscriptionRepository
	ConfigCache ConfigCache
	ProvClient  provisioningv1.ProvisioningInternalServiceClient
	Logger      zerolog.Logger
	Manager     *license.Manager // required for per-request edition checks
}

// Server implements pushv1.PushServiceServer for device registration,
// unregistration, and VAPID key retrieval.
type Server struct {
	pushv1.UnimplementedPushServiceServer
	repo        repository.SubscriptionRepository
	configCache ConfigCache
	provClient  provisioningv1.ProvisioningInternalServiceClient
	logger      zerolog.Logger
	vapidMu     sync.Mutex // guards VAPID auto-generation (prevent concurrent generation for same tenant)
	manager     *license.Manager
}

// NewServer creates a PushService gRPC server.
// Returns an error if required dependencies are missing.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Repo == nil {
		return nil, errors.New("grpcserver.NewServer: repo is required")
	}
	if cfg.ConfigCache == nil {
		return nil, errors.New("grpcserver.NewServer: config cache is required")
	}
	if cfg.ProvClient == nil {
		return nil, errors.New("grpcserver.NewServer: provisioning client is required")
	}
	if cfg.Manager == nil {
		return nil, errors.New("grpcserver.NewServer: manager is required")
	}

	return &Server{
		repo:        cfg.Repo,
		configCache: cfg.ConfigCache,
		provClient:  cfg.ProvClient,
		logger:      cfg.Logger,
		manager:     cfg.Manager,
	}, nil
}

// RegisterDevice stores a push subscription for a device.
// Validates platform, platform-specific fields, channels, and tenant prefix.
func (s *Server) RegisterDevice(ctx context.Context, req *pushv1.RegisterDeviceRequest) (*pushv1.RegisterDeviceResponse, error) {
	if !s.manager.HasFeature(license.WebPush) {
		return nil, status.Errorf(codes.PermissionDenied, "push notifications require Pro edition or higher (current: %s)", s.manager.Edition())
	}

	// Validate tenant
	if req.GetTenantSlug() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	// Validate platform
	if !validPlatforms[req.GetPlatform()] {
		return nil, status.Errorf(codes.InvalidArgument, "platform must be one of: web, android, ios (got %q)", req.GetPlatform())
	}

	// FCM/APNs delivery is Enterprise (MobilePush, ADR-0009) — gate mobile
	// platforms per-registration; web stays at the Pro WebPush gate above.
	if req.GetPlatform() == "android" || req.GetPlatform() == "ios" {
		if !s.manager.HasFeature(license.MobilePush) {
			return nil, status.Errorf(codes.PermissionDenied, "mobile push (FCM/APNs) requires Enterprise edition (current: %s)", s.manager.Edition())
		}
	}

	// Validate platform-specific fields
	switch req.GetPlatform() {
	case "web":
		if req.GetEndpoint() == "" {
			return nil, status.Error(codes.InvalidArgument, "endpoint is required for web platform")
		}
		if req.GetP256DhKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "p256dh_key is required for web platform")
		}
		if req.GetAuthSecret() == "" {
			return nil, status.Error(codes.InvalidArgument, "auth_secret is required for web platform")
		}
	case "android", "ios":
		if req.GetToken() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "token is required for %s platform", req.GetPlatform())
		}
	}

	// Validate channels non-empty
	if len(req.GetChannels()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one channel is required")
	}

	// Validate tenant prefix on each channel
	for _, ch := range req.GetChannels() {
		if !auth.ValidateChannelTenant(ch, req.GetTenantSlug()) {
			return nil, status.Errorf(codes.InvalidArgument, "channel %q does not belong to tenant %q", ch, req.GetTenantSlug())
		}
	}

	// Build subscription and persist
	sub := &repository.PushSubscription{
		TenantID:   req.GetTenantSlug(),
		Principal:  req.GetPrincipal(),
		Platform:   req.GetPlatform(),
		Token:      req.GetToken(),
		Endpoint:   req.GetEndpoint(),
		P256dhKey:  req.GetP256DhKey(),
		AuthSecret: req.GetAuthSecret(),
		Channels:   req.GetChannels(),
	}

	id, err := s.repo.Create(ctx, sub)
	if err != nil {
		s.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Str("principal", req.GetPrincipal()).
			Str("platform", req.GetPlatform()).
			Msg("RegisterDevice: failed to create subscription")
		return nil, status.Errorf(codes.Internal, "create subscription: %v", err)
	}

	s.logger.Info().
		Int64("device_id", id).
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Str("principal", req.GetPrincipal()).
		Str("platform", req.GetPlatform()).
		Int("channel_count", len(req.GetChannels())).
		Msg("RegisterDevice: subscription created")

	return &pushv1.RegisterDeviceResponse{
		DeviceId: id,
	}, nil
}

// UnregisterDevice removes a push subscription by device ID.
func (s *Server) UnregisterDevice(ctx context.Context, req *pushv1.UnregisterDeviceRequest) (*pushv1.UnregisterDeviceResponse, error) {
	if !s.manager.HasFeature(license.WebPush) {
		return nil, status.Errorf(codes.PermissionDenied, "push notifications require Pro edition or higher (current: %s)", s.manager.Edition())
	}

	if req.GetTenantSlug() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetDeviceId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "device_id is required")
	}

	if err := s.repo.Delete(ctx, req.GetDeviceId(), req.GetTenantSlug()); err != nil {
		s.logger.Error().Err(err).
			Int64("device_id", req.GetDeviceId()).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Msg("UnregisterDevice: failed to delete subscription")
		return nil, status.Errorf(codes.Internal, "delete subscription: %v", err)
	}

	s.logger.Info().
		Int64("device_id", req.GetDeviceId()).
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Msg("UnregisterDevice: subscription deleted")

	return &pushv1.UnregisterDeviceResponse{}, nil
}

// vapidCredential represents the JSON structure stored for VAPID credentials.
// Field names MUST match provider/webpush.go vapidCredentials for deserialization.
type vapidCredential struct {
	PublicKey  string `json:"vapidPublicKey"`
	PrivateKey string `json:"vapidPrivateKey"`
	Contact    string `json:"vapidContact"`
}

// GetVAPIDKey returns the tenant's VAPID public key for Web Push.
// Auto-generates an ECDSA P-256 key pair on first use.
func (s *Server) GetVAPIDKey(ctx context.Context, req *pushv1.GetVAPIDKeyRequest) (*pushv1.GetVAPIDKeyResponse, error) {
	if !s.manager.HasFeature(license.WebPush) {
		return nil, status.Errorf(codes.PermissionDenied, "push notifications require Pro edition or higher (current: %s)", s.manager.Edition())
	}

	if req.GetTenantSlug() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	// Check cache first (lock-free fast path)
	publicKey, err := s.lookupVAPIDPublicKey(req.GetTenantSlug())
	if err != nil {
		s.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Msg("GetVAPIDKey: failed to lookup VAPID credential")
		return nil, status.Errorf(codes.Internal, "lookup VAPID credential: %v", err)
	}
	if publicKey != "" {
		return &pushv1.GetVAPIDKeyResponse{PublicKey: publicKey}, nil
	}

	// Not found — acquire lock and auto-generate
	s.vapidMu.Lock()
	defer s.vapidMu.Unlock()

	// Double-check after acquiring lock (another goroutine may have generated it)
	publicKey, err = s.lookupVAPIDPublicKey(req.GetTenantSlug())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lookup VAPID credential: %v", err)
	}
	if publicKey != "" {
		return &pushv1.GetVAPIDKeyResponse{PublicKey: publicKey}, nil
	}

	// Generate ECDSA P-256 key pair
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		s.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Msg("GetVAPIDKey: failed to generate ECDSA key pair")
		return nil, status.Errorf(codes.Internal, "generate VAPID key: %v", err)
	}

	// Convert ECDSA key to ECDH for standard encoding (avoids deprecated elliptic.Marshal and D.Bytes).
	ecdhKey, err := privateKey.ECDH()
	if err != nil {
		s.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Msg("GetVAPIDKey: failed to convert ECDSA key to ECDH")
		return nil, status.Errorf(codes.Internal, "convert VAPID key to ECDH: %v", err)
	}

	// Encode public key as uncompressed point per Web Push spec (65 bytes: 0x04 || X || Y)
	publicKeyB64 := base64.RawURLEncoding.EncodeToString(ecdhKey.PublicKey().Bytes())

	// Encode private key as raw bytes (32 bytes), base64url (no padding)
	privateKeyB64 := base64.RawURLEncoding.EncodeToString(ecdhKey.Bytes())

	// Build credential JSON
	cred := vapidCredential{
		PublicKey:  publicKeyB64,
		PrivateKey: privateKeyB64,
		Contact:    "mailto:push@" + req.GetTenantSlug() + ".sukko.local",
	}
	credJSON, err := json.Marshal(cred) //nolint:gosec // G117: VAPID private key is intentionally serialized for encrypted storage via provisioning
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal VAPID credential: %v", err)
	}

	// Store via provisioning service
	_, err = s.provClient.StorePushCredentials(ctx, &provisioningv1.StorePushCredentialsRequest{
		TenantSlug:     req.GetTenantSlug(),
		Provider:       "vapid",
		CredentialData: string(credJSON),
	})
	if err != nil {
		s.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
			Msg("GetVAPIDKey: failed to store VAPID credential")
		return nil, status.Errorf(codes.Internal, "store VAPID credential: %v", err)
	}

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, req.GetTenantSlug()).
		Msg("GetVAPIDKey: auto-generated VAPID key pair")

	return &pushv1.GetVAPIDKeyResponse{PublicKey: publicKeyB64}, nil
}

// lookupVAPIDPublicKey checks the config cache for an existing VAPID credential
// and extracts the public key. Returns ("", nil) if no credential exists.
func (s *Server) lookupVAPIDPublicKey(tenantID string) (string, error) {
	credJSON, err := s.configCache.GetCredential(tenantID, "vapid")
	if err != nil {
		return "", fmt.Errorf("config cache lookup: %w", err)
	}
	if credJSON == nil {
		return "", nil
	}

	var cred vapidCredential
	if err := json.Unmarshal(credJSON, &cred); err != nil {
		return "", fmt.Errorf("unmarshal VAPID credential: %w", err)
	}
	if cred.PublicKey == "" {
		return "", errors.New("VAPID credential missing public_key field")
	}
	return cred.PublicKey, nil
}
