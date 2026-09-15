package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/provisioning/revocation"
	"github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// ServerConfig holds configuration for the gRPC stream server.
type ServerConfig struct {
	// MaxTenantsFetchLimit is the upper bound for listing all tenants in bulk
	// operations (snapshot/delta loading). This prevents unbounded queries while
	// being high enough to cover all tenants in practice.
	MaxTenantsFetchLimit int

	// PushCredentialsRepo is the repository for push provider credentials.
	// Optional — when nil, WatchPushConfig and StorePushCredentials return Unimplemented.
	PushCredentialsRepo *repository.CredentialsRepository

	// PushChannelConfigRepo is the repository for push channel configurations.
	// Optional — when nil, WatchPushConfig and StorePushCredentials return Unimplemented.
	PushChannelConfigRepo *repository.ChannelConfigRepository

	// CurrentLicenseKey returns the raw license key string for WatchLicense snapshot.
	// Optional — when nil, WatchLicense returns Unimplemented.
	CurrentLicenseKey func() string

	// RevocationStore is the in-memory token revocation store. REQUIRED: provisioning always
	// constructs it (main.go), so a nil store is a wiring bug, not a valid optional-absence —
	// NewServer rejects nil (a re-omission becomes a startup Fatal rather than a silent
	// Unimplemented that disables the §IX revocation feature).
	RevocationStore *revocation.Store

	// SlugRenameHoldPeriod is how long after a slug rename the previous slug is
	// still emitted (as TenantConfig.previous_slug) so old-slug JWTs keep
	// resolving on the gateway during the hold window. Mirrors the value
	// RequireTenant uses for its provisioning-API grace check.
	SlugRenameHoldPeriod time.Duration
}

// Server implements the ProvisioningInternalServiceServer gRPC interface.
// It streams provisioning data (keys, tenant config, topics) to gateway and ws-server
// using an event bus for real-time change notifications.
type Server struct {
	provisioningv1.UnimplementedProvisioningInternalServiceServer

	service               *provisioning.Service
	eventBus              *eventbus.Bus
	logger                zerolog.Logger
	maxTenantsFetchLimit  int
	pushCredentialsRepo   *repository.CredentialsRepository
	pushChannelConfigRepo *repository.ChannelConfigRepository

	// currentLicenseKey returns the current license key string for WatchLicense snapshot.
	// Set by provisioning main.go. Nil if license hot-reload is not configured.
	currentLicenseKey func() string

	// revocationStore is the in-memory token revocation store for WatchTokenRevocations.
	revocationStore *revocation.Store

	// slugRenameHoldPeriod bounds how long previous_slug is emitted after a rename.
	slugRenameHoldPeriod time.Duration
}

// NewServer creates a new gRPC stream server.
func NewServer(service *provisioning.Service, eventBus *eventbus.Bus, logger zerolog.Logger, cfg ServerConfig) (*Server, error) {
	if service == nil {
		return nil, errors.New("grpc server: service is required")
	}
	if eventBus == nil {
		return nil, errors.New("grpc server: event bus is required")
	}
	if cfg.MaxTenantsFetchLimit <= 0 {
		return nil, errors.New("grpc server: MaxTenantsFetchLimit must be > 0")
	}
	if cfg.RevocationStore == nil {
		return nil, errors.New("grpc server: revocation store is required")
	}

	return &Server{
		service:               service,
		eventBus:              eventBus,
		logger:                logger.With().Str("component", "grpc_server").Logger(),
		maxTenantsFetchLimit:  cfg.MaxTenantsFetchLimit,
		pushCredentialsRepo:   cfg.PushCredentialsRepo,
		pushChannelConfigRepo: cfg.PushChannelConfigRepo,
		currentLicenseKey:     cfg.CurrentLicenseKey,
		revocationStore:       cfg.RevocationStore,
		slugRenameHoldPeriod:  cfg.SlugRenameHoldPeriod,
	}, nil
}

// WatchKeys streams active keys to the caller. Sends a snapshot on connect,
// then streams deltas when keys change via event bus.
func (s *Server) WatchKeys(_ *provisioningv1.WatchKeysRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchKeysResponse]) error {
	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchKeys").Logger()

	// Load and send initial snapshot
	keys, err := s.service.GetActiveKeys(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "load active keys: %v", err)
	}

	snapshot := &provisioningv1.WatchKeysResponse{
		IsSnapshot: true,
		Keys:       convertKeys(keys),
	}
	if err := stream.Send(snapshot); err != nil {
		return status.Errorf(codes.Unavailable, "send keys snapshot: %v", err)
	}

	logger.Info().Int("key_count", len(keys)).Msg("sent keys snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	// Stream deltas on change
	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.KeysChanged {
				continue
			}

			// Reload all active keys and send as delta
			updatedKeys, err := s.service.GetActiveKeys(ctx)
			if err != nil {
				logger.Error().Err(err).Msg("failed to reload keys for delta")
				continue
			}

			delta := &provisioningv1.WatchKeysResponse{
				IsSnapshot: false,
				Keys:       convertKeys(updatedKeys),
			}
			if err := stream.Send(delta); err != nil {
				return status.Errorf(codes.Unavailable, "send keys delta: %v", err)
			}

			logger.Debug().Int("key_count", len(updatedKeys)).Msg("sent keys delta")
		}
	}
}

// WatchTenantConfig streams tenant configuration (channel rules, routing rules) to the caller.
func (s *Server) WatchTenantConfig(_ *provisioningv1.WatchTenantConfigRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchTenantConfigResponse]) error {
	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchTenantConfig").Logger()

	// Load and send initial snapshot
	tenantConfigs, err := s.loadTenantConfigs(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "load tenant configs: %v", err)
	}

	snapshot := &provisioningv1.WatchTenantConfigResponse{
		IsSnapshot: true,
		Tenants:    tenantConfigs,
	}
	if err := stream.Send(snapshot); err != nil {
		return status.Errorf(codes.Unavailable, "send tenant config snapshot: %v", err)
	}

	logger.Info().Int("tenant_count", len(tenantConfigs)).Msg("sent tenant config snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.TenantConfigChanged {
				continue
			}

			updatedConfigs, err := s.loadTenantConfigs(ctx)
			if err != nil {
				logger.Error().Err(err).Msg("failed to reload tenant configs for delta")
				continue
			}

			delta := &provisioningv1.WatchTenantConfigResponse{
				IsSnapshot: false,
				Tenants:    updatedConfigs,
			}
			if err := stream.Send(delta); err != nil {
				return status.Errorf(codes.Unavailable, "send tenant config delta: %v", err)
			}

			logger.Debug().Int("tenant_count", len(updatedConfigs)).Msg("sent tenant config delta")
		}
	}
}

// WatchTopics streams topic discovery data to the caller.
func (s *Server) WatchTopics(req *provisioningv1.WatchTopicsRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchTopicsResponse]) error {
	ctx := stream.Context()
	namespace := req.GetNamespace()
	logger := s.logger.With().Str("rpc", "WatchTopics").Str("namespace", namespace).Logger()

	// Load and send initial snapshot
	topicsResp, err := s.loadTopicsUpdate(ctx, namespace)
	if err != nil {
		return status.Errorf(codes.Internal, "load topics: %v", err)
	}

	topicsResp.IsSnapshot = true
	if err := stream.Send(topicsResp); err != nil {
		return status.Errorf(codes.Unavailable, "send topics snapshot: %v", err)
	}

	logger.Info().
		Int("shared_topics", len(topicsResp.GetSharedTopics())).
		Int("dedicated_tenants", len(topicsResp.GetDedicatedTenants())).
		Msg("sent topics snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.TopicsChanged {
				continue
			}

			updatedTopics, err := s.loadTopicsUpdate(ctx, namespace)
			if err != nil {
				logger.Error().Err(err).Msg("failed to reload topics for delta")
				continue
			}

			updatedTopics.IsSnapshot = false
			if err := stream.Send(updatedTopics); err != nil {
				return status.Errorf(codes.Unavailable, "send topics delta: %v", err)
			}

			logger.Debug().
				Int("shared_topics", len(updatedTopics.GetSharedTopics())).
				Int("dedicated_tenants", len(updatedTopics.GetDedicatedTenants())).
				Msg("sent topics delta")
		}
	}
}

// WatchAPIKeys streams active API keys to the caller. Sends a snapshot on connect,
// then streams deltas when API keys change via event bus.
func (s *Server) WatchAPIKeys(_ *provisioningv1.WatchAPIKeysRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchAPIKeysResponse]) error {
	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchAPIKeys").Logger()

	// Load and send initial snapshot
	keys, err := s.service.GetActiveAPIKeys(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "load active api keys: %v", err)
	}

	snapshot := &provisioningv1.WatchAPIKeysResponse{
		IsSnapshot: true,
		ApiKeys:    convertAPIKeys(keys),
	}
	if err := stream.Send(snapshot); err != nil {
		return status.Errorf(codes.Unavailable, "send api keys snapshot: %v", err)
	}

	logger.Info().Int("key_count", len(keys)).Msg("sent api keys snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	// Stream deltas on change
	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.APIKeysChanged {
				continue
			}

			// Reload all active API keys and send as delta
			updatedKeys, err := s.service.GetActiveAPIKeys(ctx)
			if err != nil {
				logger.Error().Err(err).Msg("failed to reload api keys for delta")
				continue
			}

			delta := &provisioningv1.WatchAPIKeysResponse{
				IsSnapshot: false,
				ApiKeys:    convertAPIKeys(updatedKeys),
			}
			if err := stream.Send(delta); err != nil {
				return status.Errorf(codes.Unavailable, "send api keys delta: %v", err)
			}

			logger.Debug().Int("key_count", len(updatedKeys)).Msg("sent api keys delta")
		}
	}
}

// WatchPushConfig streams push configuration (credentials + channel configs) to the caller.
// Sends a snapshot on connect, then streams deltas when push config changes via event bus.
func (s *Server) WatchPushConfig(_ *provisioningv1.WatchPushConfigRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchPushConfigResponse]) error {
	if s.pushCredentialsRepo == nil || s.pushChannelConfigRepo == nil {
		return status.Error(codes.Unimplemented, "push configuration repositories not configured")
	}

	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchPushConfig").Logger()

	// Load and send initial snapshot
	resp, err := s.loadPushConfig(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "load push config: %v", err)
	}

	resp.IsSnapshot = true
	if err := stream.Send(resp); err != nil {
		return status.Errorf(codes.Unavailable, "send push config snapshot: %v", err)
	}

	logger.Info().
		Int("credential_count", len(resp.GetPushCredentials())).
		Int("channel_config_count", len(resp.GetPushChannelConfigs())).
		Msg("sent push config snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.PushConfigChanged {
				continue
			}

			updatedResp, err := s.loadPushConfig(ctx)
			if err != nil {
				logger.Error().Err(err).Msg("failed to reload push config for delta")
				continue
			}

			updatedResp.IsSnapshot = false
			if err := stream.Send(updatedResp); err != nil {
				return status.Errorf(codes.Unavailable, "send push config delta: %v", err)
			}

			logger.Debug().
				Int("credential_count", len(updatedResp.GetPushCredentials())).
				Int("channel_config_count", len(updatedResp.GetPushChannelConfigs())).
				Msg("sent push config delta")
		}
	}
}

// StorePushCredentials stores provider credentials for a tenant. If credentials
// already exist for the tenant+provider combination, they are updated.
func (s *Server) StorePushCredentials(ctx context.Context, req *provisioningv1.StorePushCredentialsRequest) (*provisioningv1.StorePushCredentialsResponse, error) {
	if s.pushCredentialsRepo == nil {
		return nil, status.Error(codes.Unimplemented, "push credentials repository not configured")
	}

	tenantSlug := req.GetTenantSlug()
	provider := req.GetProvider()
	credentialData := req.GetCredentialData()

	if tenantSlug == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_slug is required")
	}
	if provider == "" {
		return nil, status.Error(codes.InvalidArgument, "provider is required")
	}
	if credentialData == "" {
		return nil, status.Error(codes.InvalidArgument, "credential_data is required")
	}

	// Push is slug-native, but push_credentials.tenant_id is a UUID FK — resolve the
	// slug to the owning tenant's UUID here (provisioning owns the tenant table).
	tenant, err := s.service.GetTenantBySlug(ctx, tenantSlug)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "resolve tenant slug %q: %v", tenantSlug, err)
	}

	logger := s.logger.With().
		Str("rpc", "StorePushCredentials").
		Str(logging.LogKeyTenantSlug, tenantSlug).
		Str(logging.LogKeyTenantUUID, tenant.ID).
		Str("provider", provider).
		Logger()

	cred := &repository.PushCredential{
		TenantID:       tenant.ID,
		Provider:       provider,
		CredentialData: credentialData,
	}

	// Attempt create; if duplicate, update instead.
	err = s.pushCredentialsRepo.Create(ctx, cred)
	if err != nil {
		if !errors.Is(err, repository.ErrCredentialAlreadyExists) {
			logger.Error().Err(err).Msg("failed to create push credentials")
			return nil, status.Errorf(codes.Internal, "store push credentials: %v", err)
		}
		// Duplicate — update existing credential.
		if updateErr := s.pushCredentialsRepo.Update(ctx, cred); updateErr != nil {
			logger.Error().Err(updateErr).Str("create_err", err.Error()).Msg("failed to update push credentials")
			return nil, status.Errorf(codes.Internal, "update push credentials: %v", updateErr)
		}
		logger.Info().Msg("updated existing push credentials")
	} else {
		logger.Info().Msg("created push credentials")
	}

	// Notify watchers
	s.eventBus.Publish(eventbus.Event{Type: eventbus.PushConfigChanged})

	return &provisioningv1.StorePushCredentialsResponse{Success: true}, nil
}

// loadPushConfig loads all push credentials and channel configs for streaming.
func (s *Server) loadPushConfig(ctx context.Context) (*provisioningv1.WatchPushConfigResponse, error) {
	creds, err := s.pushCredentialsRepo.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list push credentials: %w", err)
	}

	configs, err := s.pushChannelConfigRepo.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list push channel configs: %w", err)
	}

	// Push is slug-native, but the push tables key tenant by UUID FK. Build a
	// UUID->slug map so the stream carries the slug the push runtime keys by.
	tenants, _, err := s.service.ListTenants(ctx, provisioning.ListOptions{Limit: s.maxTenantsFetchLimit})
	if err != nil {
		return nil, fmt.Errorf("list tenants for push slug mapping: %w", err)
	}
	slugByUUID := make(map[string]string, len(tenants))
	for _, t := range tenants {
		slugByUUID[t.ID] = t.Slug
	}

	return &provisioningv1.WatchPushConfigResponse{
		PushCredentials:    s.convertPushCredentials(creds, slugByUUID),
		PushChannelConfigs: s.convertPushChannelConfigs(configs, slugByUUID),
	}, nil
}

// convertPushCredentials converts repository PushCredential to proto PushCredentialInfo,
// emitting the tenant slug (push runtime is slug-native). A credential whose tenant
// UUID has no slug mapping is skipped and logged — never emitted with an empty slug.
func (s *Server) convertPushCredentials(creds []*repository.PushCredential, slugByUUID map[string]string) []*provisioningv1.PushCredentialInfo {
	result := make([]*provisioningv1.PushCredentialInfo, 0, len(creds))
	for _, c := range creds {
		slug, ok := slugByUUID[c.TenantID]
		if !ok {
			s.logger.Warn().Str(logging.LogKeyTenantUUID, c.TenantID).Str("provider", c.Provider).
				Msg("push credential skipped: tenant UUID has no slug mapping")
			continue
		}
		result = append(result, &provisioningv1.PushCredentialInfo{
			TenantSlug:     slug,
			Provider:       c.Provider,
			CredentialData: c.CredentialData,
		})
	}
	return result
}

// convertPushChannelConfigs converts repository PushChannelConfig to proto PushChannelConfig,
// emitting the tenant slug. A config whose tenant UUID has no slug mapping is skipped and logged.
func (s *Server) convertPushChannelConfigs(configs []*repository.PushChannelConfig, slugByUUID map[string]string) []*provisioningv1.PushChannelConfig {
	result := make([]*provisioningv1.PushChannelConfig, 0, len(configs))
	for _, c := range configs {
		slug, ok := slugByUUID[c.TenantID]
		if !ok {
			s.logger.Warn().Str(logging.LogKeyTenantUUID, c.TenantID).
				Msg("push channel config skipped: tenant UUID has no slug mapping")
			continue
		}
		result = append(result, &provisioningv1.PushChannelConfig{
			TenantSlug:     slug,
			Patterns:       c.Patterns,
			DefaultTtl:     clampInt32(c.DefaultTTL),
			DefaultUrgency: c.DefaultUrgency,
		})
	}
	return result
}

// clampInt32 safely converts an int to int32, clamping to math.MaxInt32 on overflow.
func clampInt32(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v) // #nosec G115 — overflow prevented by bounds check above
}

// loadTenantConfigs loads all tenant channel rules and routing rules.
func (s *Server) loadTenantConfigs(ctx context.Context) ([]*provisioningv1.TenantConfig, error) {
	tenants, _, err := s.service.ListTenants(ctx, provisioning.ListOptions{Limit: s.maxTenantsFetchLimit})
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}

	var configs []*provisioningv1.TenantConfig

	for _, tenant := range tenants {
		if tenant.Status != provisioning.StatusActive {
			continue
		}

		tc := &provisioningv1.TenantConfig{
			TenantSlug: tenant.Slug,
			TenantUuid: tenant.ID, // stable UUID for the gateway's slug->UUID binding resolver
		}

		// During the rename hold window, also emit the previous slug so old-slug
		// JWTs still resolve to this tenant's UUID on the gateway. Same predicate
		// as RequireTenant's provisioning-API grace check.
		if s.slugRenameHoldPeriod > 0 &&
			tenant.SlugRenameState == provisioning.SlugRenameStateComplete &&
			tenant.PreviousSlug != "" &&
			tenant.SlugRenamedAt != nil &&
			time.Since(*tenant.SlugRenamedAt) < s.slugRenameHoldPeriod {
			tc.PreviousSlug = tenant.PreviousSlug
		}

		// Load channel rules (optional — not all tenants have rules configured)
		channelRules, err := s.service.GetChannelRules(ctx, tenant.Slug)
		if err != nil && !errors.Is(err, types.ErrChannelRulesNotFound) && !errors.Is(err, provisioning.ErrChannelRulesNotConfigured) {
			s.logger.Warn().Err(err).Str(logging.LogKeyTenantUUID, tenant.ID).Msg("failed to load channel rules")
		}
		if err == nil && channelRules != nil {
			tc.ChannelRules = convertChannelRules(&channelRules.Rules)
		}

		// Load routing rules (optional — not all tenants have rules configured)
		routingRules, err := s.service.GetRoutingRules(ctx, tenant.Slug)
		if err != nil && !errors.Is(err, provisioning.ErrRoutingRulesNotConfigured) && !errors.Is(err, provisioning.ErrRoutingRulesNotFound) {
			s.logger.Debug().Err(err).Str(logging.LogKeyTenantUUID, tenant.ID).Msg("failed to load routing rules")
		}
		if err == nil && len(routingRules) > 0 {
			tc.RoutingRules = convertRoutingRules(routingRules)
		}

		configs = append(configs, tc)
	}

	return configs, nil
}

// loadTopicsUpdate builds a WatchTopicsResponse from service data.
func (s *Server) loadTopicsUpdate(ctx context.Context, namespace string) (*provisioningv1.WatchTopicsResponse, error) {
	tenants, _, err := s.service.ListTenants(ctx, provisioning.ListOptions{Limit: s.maxTenantsFetchLimit})
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}

	var sharedTopics []string
	var sharedTopicTenants []*provisioningv1.SharedTopic
	var dedicatedTenants []*provisioningv1.DedicatedTenant

	for _, tenant := range tenants {
		if tenant.Status != provisioning.StatusActive {
			continue
		}

		// ADR-0006: the consume/create set = the tenant's deterministic default
		// topic ∪ the suffixes its routing rules reference. Every active tenant
		// contributes its default topic — routing rules (ChannelTopicRouting, ungated)
		// only ever EXTEND the set, never gate membership. Deriving membership from
		// rules alone (the previous #179 P3 coupling) made Community ingest
		// structurally dead: rule-less tenants' topics were never physically
		// created (ensureTopicsExist) nor consumed. The DLQ is deliberately not in
		// this set — it is written to, never consumed.
		topics := []string{kafka.BuildTopicName(namespace, tenant.Slug, routing.DefaultTopicSuffix)}
		seenSuffixes := map[string]struct{}{routing.DefaultTopicSuffix: {}}

		rules, err := s.service.GetRoutingRules(ctx, tenant.Slug)
		if err != nil {
			// No rules is not exclusion: the tenant still ingests via its default
			// topic. Rules simply cannot extend the set for this tenant.
			s.logger.Debug().Err(err).Str(logging.LogKeyTenantUUID, tenant.ID).
				Msg("Tenant has no routing rules; contributing default topic only")
			rules = nil
		}
		for _, rule := range rules {
			for _, suffix := range rule.Topics {
				if _, ok := seenSuffixes[suffix]; ok {
					continue
				}
				seenSuffixes[suffix] = struct{}{}
				topics = append(topics, kafka.BuildTopicName(namespace, tenant.Slug, suffix))
			}
		}

		switch tenant.ConsumerType {
		case provisioning.ConsumerDedicated:
			dedicatedTenants = append(dedicatedTenants, &provisioningv1.DedicatedTenant{
				TenantSlug: tenant.Slug,
				Topics:     topics,
			})
		default: // ConsumerShared and unspecified both use shared consumers
			sharedTopics = append(sharedTopics, topics...)
			for _, topic := range topics {
				sharedTopicTenants = append(sharedTopicTenants, &provisioningv1.SharedTopic{
					TenantSlug: tenant.Slug,
					Topic:      topic,
				})
			}
		}
	}

	return &provisioningv1.WatchTopicsResponse{
		SharedTopics:       sharedTopics,
		SharedTopicTenants: sharedTopicTenants,
		DedicatedTenants:   dedicatedTenants,
	}, nil
}

// convertKeys converts provisioning keys to proto KeyInfo messages.
func convertKeys(keys []*provisioning.TenantKey) []*provisioningv1.KeyInfo {
	result := make([]*provisioningv1.KeyInfo, 0, len(keys))
	for _, k := range keys {
		ki := &provisioningv1.KeyInfo{
			KeyId:        k.KeyID,
			TenantUuid:   k.TenantID,
			Algorithm:    string(k.Algorithm),
			PublicKeyPem: k.PublicKey,
			IsActive:     k.RevokedAt == nil,
		}
		if k.ExpiresAt != nil {
			ki.ExpiresAtUnix = k.ExpiresAt.Unix()
		}
		result = append(result, ki)
	}
	return result
}

// convertRoutingRules converts provisioning.TopicRoutingRule to proto TopicRoutingRule.
func convertRoutingRules(rules []provisioning.TopicRoutingRule) []*provisioningv1.TopicRoutingRule {
	result := make([]*provisioningv1.TopicRoutingRule, 0, len(rules))
	for _, r := range rules {
		result = append(result, &provisioningv1.TopicRoutingRule{
			Pattern:     r.Pattern,
			TopicSuffix: firstOrEmpty(r.Topics), // deprecated field — backward compat
			Topics:      r.Topics,
			Priority:    int32(r.Priority), //nolint:gosec // G115: priority is validated at write time to be a small positive integer, never exceeds int32 max
		})
	}
	return result
}

// firstOrEmpty returns the first element of s, or empty string if s is empty.
func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// convertAPIKeys converts provisioning API keys to proto APIKeyInfo messages.
func convertAPIKeys(keys []*provisioning.APIKey) []*provisioningv1.APIKeyInfo {
	result := make([]*provisioningv1.APIKeyInfo, 0, len(keys))
	for _, k := range keys {
		result = append(result, &provisioningv1.APIKeyInfo{
			KeyId:      k.KeyID,
			TenantUuid: k.TenantID,
			Name:       k.Name,
			IsActive:   k.IsActive,
		})
	}
	return result
}

// convertChannelRules converts types.ChannelRules to proto ChannelRules.
func convertChannelRules(rules *types.ChannelRules) *provisioningv1.ChannelRules {
	cr := &provisioningv1.ChannelRules{
		PublicChannels:         rules.Public,
		DefaultChannels:        rules.Default,
		PublishPublicChannels:  rules.PublishPublic,
		PublishDefaultChannels: rules.PublishDefault,
	}

	if len(rules.GroupMappings) > 0 {
		cr.GroupMappings = make(map[string]*provisioningv1.GroupChannels)
		for group, channels := range rules.GroupMappings {
			cr.GroupMappings[group] = &provisioningv1.GroupChannels{
				Channels: channels,
			}
		}
	}

	if len(rules.PublishGroupMappings) > 0 {
		cr.PublishGroupMappings = make(map[string]*provisioningv1.GroupChannels)
		for group, channels := range rules.PublishGroupMappings {
			cr.PublishGroupMappings[group] = &provisioningv1.GroupChannels{
				Channels: channels,
			}
		}
	}

	return cr
}

// WatchLicense streams the current license key to subscribers.
// Sends the current key on connect, then sends updates on LicenseChanged events.
func (s *Server) WatchLicense(_ *provisioningv1.WatchLicenseRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchLicenseResponse]) error {
	if s.currentLicenseKey == nil {
		return status.Error(codes.Unimplemented, "license hot-reload not configured")
	}

	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchLicense").Logger()

	// Snapshot: always send current key (empty = Community / no license)
	currentKey := s.currentLicenseKey()
	if err := stream.Send(&provisioningv1.WatchLicenseResponse{LicenseKey: currentKey}); err != nil {
		return status.Errorf(codes.Unavailable, "send license snapshot: %v", err)
	}
	logger.Info().Bool("has_key", currentKey != "").Msg("sent license snapshot")

	// Subscribe to license change events
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.LicenseChanged {
				continue
			}

			key := s.currentLicenseKey()
			if err := stream.Send(&provisioningv1.WatchLicenseResponse{LicenseKey: key}); err != nil {
				return status.Errorf(codes.Unavailable, "send license update: %v", err)
			}
			logger.Info().Msg("sent license update")
		}
	}
}

// WatchTokenRevocations streams token revocations. Sends a snapshot on connect,
// then streams deltas when tokens are revoked via event bus.
func (s *Server) WatchTokenRevocations(_ *provisioningv1.WatchTokenRevocationsRequest, stream grpc.ServerStreamingServer[provisioningv1.WatchTokenRevocationsResponse]) error {
	// s.revocationStore is guaranteed non-nil (NewServer requires it) — no Unimplemented gate.
	ctx := stream.Context()
	logger := s.logger.With().Str("rpc", "WatchTokenRevocations").Logger()

	// Send initial snapshot
	entries := s.revocationStore.Snapshot()
	snapshot := &provisioningv1.WatchTokenRevocationsResponse{
		IsSnapshot:  true,
		Revocations: convertRevocationEntries(entries),
	}
	if err := stream.Send(snapshot); err != nil {
		return status.Errorf(codes.Unavailable, "send revocation snapshot: %v", err)
	}
	logger.Info().Int("entry_count", len(entries)).Msg("sent revocation snapshot")

	// Subscribe to event bus for changes
	subID, events := s.eventBus.Subscribe()
	defer s.eventBus.Unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Msg("stream context canceled")
			return nil

		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Type != eventbus.TokenRevocationsChanged {
				continue
			}

			// Send current snapshot as delta (revocation store handles expiry filtering)
			updated := s.revocationStore.Snapshot()
			delta := &provisioningv1.WatchTokenRevocationsResponse{
				IsSnapshot:  false,
				Revocations: convertRevocationEntries(updated),
			}
			if err := stream.Send(delta); err != nil {
				return status.Errorf(codes.Unavailable, "send revocation delta: %v", err)
			}
			logger.Debug().Int("entry_count", len(updated)).Msg("sent revocation delta")
		}
	}
}

// convertRevocationEntries converts store entries to proto messages.
func convertRevocationEntries(entries []*revocation.Entry) []*provisioningv1.TokenRevocation {
	result := make([]*provisioningv1.TokenRevocation, len(entries))
	for i, e := range entries {
		result[i] = &provisioningv1.TokenRevocation{
			TenantSlug: e.TenantID,
			Type:       e.Type,
			Sub:        e.Sub,
			Jti:        e.JTI,
			RevokedAt:  e.RevokedAt,
			ExpiresAt:  e.ExpiresAt,
		}
	}
	return result
}
