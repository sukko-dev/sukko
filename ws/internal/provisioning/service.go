package provisioning

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/routing"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// Sentinel errors for provisioning operations.
var (
	ErrTenantDeleted             = errors.New("cannot modify deleted tenant")
	ErrTenantNotActive           = errors.New("tenant is not active")
	ErrTenantNotFound            = errors.New("tenant not found")
	ErrQuotaNotFound             = errors.New("quota not found")
	ErrKeyNotFound               = errors.New("key not found")
	ErrKeyNotOwnedByTenant       = errors.New("key does not belong to tenant")
	ErrInvalidQuota              = errors.New("invalid quota")
	ErrChannelRulesNotConfigured = errors.New("channel rules store not configured")
	ErrRoutingRulesNotConfigured = errors.New("routing rules store not configured")

	// Tenant/key input validation sentinel errors. Validators wrap their bare
	// formatted errors with these (fmt.Errorf("%w: <detail>", ErrXxx, ...)) so the
	// API layer's errors.Is classification maps each to its own 400 code while the
	// detail message is preserved.
	ErrNameInvalid         = errors.New("name is invalid")
	ErrConsumerTypeInvalid = errors.New("consumer type is invalid")
	ErrKeyInvalid          = errors.New("key is invalid")

	// Slug rename sentinel errors.
	ErrSlugAlreadyTaken      = errors.New("slug already taken")
	ErrKeyAlreadyExists      = errors.New("key id already exists")
	ErrSlugReserved          = errors.New("slug is reserved")
	ErrSlugInvalid           = errors.New("slug is invalid")
	ErrSlugUnchanged         = errors.New("new slug is identical to current slug")
	ErrSlugRenameInProgress  = errors.New("slug rename already in progress or within hold period")
	ErrSlugImmutableViaPatch = errors.New("slug cannot be changed via PATCH; use the /rename endpoint")
	// ErrCASFailed is returned by TenantStore.SetRenameState when the CAS guard
	// matches zero rows (another rename is already pending). This is an internal
	// store-layer sentinel — it MUST NOT propagate to HTTP handlers; the service
	// layer maps it to ErrSlugRenameInProgress before returning to callers.
	ErrCASFailed = errors.New("CAS update matched zero rows")

	ErrWebhookNotFound           = errors.New("webhook not found")
	ErrWebhookQuotaExceeded      = errors.New("webhook quota exceeded")
	ErrWebhookStoreNotConfigured = errors.New("webhook store not configured")
	ErrWebhookInvalidInput       = errors.New("invalid webhook input")
)

const (
	// msPerDay is the number of milliseconds in one day.
	msPerDay = 24 * 60 * 60 * 1000
)

// Reserved slug segments — all HTTP route segments that a tenant slug must not shadow.
// These must be kept in sync with the route tree in api/router.go.
const (
	slugReservedAdmin        = "admin"
	slugReservedAPI          = "api"
	slugReservedHealth       = "health"
	slugReservedReady        = "ready"
	slugReservedMetrics      = "metrics"
	slugReservedVersion      = "version"
	slugReservedEdition      = "edition"
	slugReservedConfig       = "config"
	slugReservedDebug        = "debug"
	slugReservedInternal     = "internal"
	slugReservedSystem       = "system"
	slugReservedSukko        = "sukko"
	slugReservedKeys         = "keys"
	slugReservedAPIKeys      = "api-keys"
	slugReservedRoutingRules = "routing-rules"
	slugReservedQuotas       = "quotas"
	slugReservedAudit        = "audit"
	slugReservedChannelRules = "channel-rules"
	slugReservedTokens       = "tokens"
	slugReservedSuspend      = "suspend"
	slugReservedReactivate   = "reactivate"
	slugReservedRename       = "rename"
	slugReservedTestAccess   = "test-access"
)

// ServiceConfig holds the configuration for the provisioning service.
type ServiceConfig struct {
	TenantStore       TenantStore
	KeyStore          KeyStore
	APIKeyStore       APIKeyStore
	RoutingRulesStore RoutingRulesStore
	QuotaStore        QuotaStore
	AuditStore        AuditStore
	ChannelRulesStore ChannelRulesStore
	KafkaAdmin        KafkaAdmin
	EventBus          *eventbus.Bus
	Logger            zerolog.Logger

	// Topic configuration
	TopicNamespace     string
	DefaultPartitions  int
	DefaultRetentionMs int64

	// Quota defaults
	MaxTopicsPerTenant  int
	MaxStorageBytes     int64
	DefaultProducerRate int64
	DefaultConsumerRate int64

	// Lifecycle
	DeprovisionGraceDays int

	// MaxRoutingRulesPerTenant limits the number of routing rules per tenant.
	// Wired from env var MAX_ROUTING_RULES_PER_TENANT (default: 100).
	MaxRoutingRulesPerTenant    int
	MaxTopicsPerRule            int
	DeadLetterTopicPartitions   int
	DeadLetterTopicRetentionMs  int64
	InfraTopicReplicationFactor int // int (not int16) — cast to int16 at CreateTopic call sites

	// EditionManager provides expiry-aware edition limits for runtime gates.
	// May be nil in tests — edition gates are skipped when nil.
	EditionManager *license.Manager

	// Clock is an injectable time source for the rename saga. Nil → time.Now.
	Clock func() time.Time

	// SlugRenameTopicHoldPeriod is how long to keep old topics/ACLs after a rename
	// and accept old-slug JWTs. Forwarded from platform.ProvisioningConfig.
	SlugRenameTopicHoldPeriod time.Duration

	// Webhook configuration (Pro edition).
	// WebhookStore is nil for Community deployments — webhook methods return ErrWebhookStoreNotConfigured.
	WebhookStore         WebhookStore
	EncryptionKey        []byte // AES-256 key for encrypting webhook secrets at rest.
	MaxWebhooksPerTenant int    // Quota cap; 0 means unset (no enforcement).
	WebhookAllowHTTP     bool   // Allow http:// webhook URLs (local/dev only).

	// InvalidationPublisher sends Valkey pub/sub signals after webhook writes so the
	// webhook-worker refreshes its in-memory cache within ~1s. Nil for Community deployments
	// or tests that don't wire Valkey — Publish() is guarded by nil check in service methods.
	InvalidationPublisher WebhookCacheInvalidator
}

// Service provides tenant lifecycle management and provisioning operations.
type Service struct {
	tenants        TenantStore
	keys           KeyStore
	apiKeys        APIKeyStore
	routingRules   RoutingRulesStore
	quotas         QuotaStore
	audit          AuditStore
	channelRules   ChannelRulesStore
	kafka          KafkaAdmin
	eventBus       *eventbus.Bus
	logger         zerolog.Logger
	config         ServiceConfig
	editionManager *license.Manager
	webhooks       WebhookStore // nil for Community — methods guard against nil
}

// NewService creates a new provisioning Service.
// Returns an error if required stores (TenantStore, KeyStore, QuotaStore,
// AuditStore, KafkaAdmin, EventBus) are nil.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.TenantStore == nil {
		return nil, errors.New("provisioning service: tenant store is required")
	}
	if cfg.KeyStore == nil {
		return nil, errors.New("provisioning service: key store is required")
	}
	if cfg.QuotaStore == nil {
		return nil, errors.New("provisioning service: quota store is required")
	}
	if cfg.AuditStore == nil {
		return nil, errors.New("provisioning service: audit store is required")
	}
	if cfg.APIKeyStore == nil {
		return nil, errors.New("provisioning service: api key store is required")
	}
	if cfg.KafkaAdmin == nil {
		return nil, errors.New("provisioning service: kafka admin is required")
	}
	if cfg.EventBus == nil {
		return nil, errors.New("provisioning service: event bus is required")
	}
	if cfg.TopicNamespace == "" {
		return nil, errors.New("provisioning service: topic namespace is required")
	}
	if cfg.WebhookStore != nil && len(cfg.EncryptionKey) != crypto.KeySize {
		return nil, fmt.Errorf("provisioning service: EncryptionKey must be %d bytes when WebhookStore is configured (got %d)", crypto.KeySize, len(cfg.EncryptionKey))
	}

	return &Service{
		tenants:        cfg.TenantStore,
		keys:           cfg.KeyStore,
		apiKeys:        cfg.APIKeyStore,
		routingRules:   cfg.RoutingRulesStore,
		quotas:         cfg.QuotaStore,
		audit:          cfg.AuditStore,
		channelRules:   cfg.ChannelRulesStore,
		kafka:          cfg.KafkaAdmin,
		eventBus:       cfg.EventBus,
		logger:         cfg.Logger,
		config:         cfg,
		editionManager: cfg.EditionManager,
		webhooks:       cfg.WebhookStore,
	}, nil
}

// clock returns the current time using the injectable clock or time.Now.
func (s *Service) clock() time.Time {
	if s.config.Clock != nil {
		return s.config.Clock()
	}
	return time.Now()
}

// Ready checks if the service is ready to handle requests.
// Returns nil if database is accessible, otherwise returns the error.
func (s *Service) Ready(ctx context.Context) error {
	if err := s.tenants.Ping(ctx); err != nil {
		return fmt.Errorf("ping tenant store: %w", err)
	}
	return nil
}

// CountTenants returns the number of active (non-deleted) tenants.
// Used by the /edition endpoint for usage reporting.
func (s *Service) CountTenants(ctx context.Context) (int, error) {
	count, err := s.tenants.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("count tenants: %w", err)
	}
	return count, nil
}

// infraTopicSpec defines the creation parameters for one infrastructure topic.
type infraTopicSpec struct {
	suffix     string
	topicType  string
	partitions int
	cfg        map[string]string
}

// infraTopicSpecs returns the ordered list of infrastructure topics to create for a tenant:
// DLQ first, default second. Both CreateTenant (saga) and ReactivateTenant (idempotent) use
// this as the single source of truth for which topics get created and with what config.
func (s *Service) infraTopicSpecs() []infraTopicSpec {
	return []infraTopicSpec{
		{
			suffix:     routing.DeadLetterTopicSuffix,
			topicType:  topicTypeDLQ,
			partitions: s.config.DeadLetterTopicPartitions,
			cfg:        map[string]string{kafka.RetentionMsConfigKey: strconv.FormatInt(s.config.DeadLetterTopicRetentionMs, 10)},
		},
		{
			suffix:     routing.DefaultTopicSuffix,
			topicType:  topicTypeDefault,
			partitions: s.config.DefaultPartitions,
			cfg:        map[string]string{kafka.RetentionMsConfigKey: strconv.FormatInt(s.config.DefaultRetentionMs, 10)},
		},
	}
}

// CreateTenant creates a new tenant with optional initial key and topics.
func (s *Service) CreateTenant(ctx context.Context, req CreateTenantRequest) (*CreateTenantResponse, error) {
	// Validate tenant
	tenant := &Tenant{
		Slug:         req.Slug,
		Name:         req.Name,
		Status:       StatusActive,
		ConsumerType: req.ConsumerType,
		Metadata:     req.Metadata,
	}

	if tenant.ConsumerType == "" {
		tenant.ConsumerType = ConsumerShared
	}

	if err := tenant.Validate(); err != nil {
		return nil, fmt.Errorf("invalid tenant: %w", err)
	}

	// Validate the embedded initial-key INPUT up front (before any side effect) so an invalid
	// key returns 400 KEY_INVALID with no partially-created tenant. Only the user-supplied
	// fields are checked here; the server-generated TenantID is verified after the insert.
	if req.PublicKey != nil {
		if err := ValidateKeyInput(req.PublicKey.KeyID, req.PublicKey.Algorithm, req.PublicKey.PublicKey); err != nil {
			return nil, fmt.Errorf("invalid key: %w", err)
		}
	}

	if isReservedSlug(req.Slug) {
		return nil, ErrSlugReserved
	}

	// Edition limit: tenant count
	if s.editionManager != nil {
		limits := s.editionManager.Limits()
		count, err := s.tenants.Count(ctx)
		if err != nil {
			return nil, fmt.Errorf("count tenants: %w", err)
		}
		if err := limits.CheckTenants(count); err != nil {
			return nil, fmt.Errorf("edition limit: %w", err)
		}
	}

	// Create infrastructure topics before DB insert (saga pattern — rollback on DB failure).
	// Track whether each topic was created by this saga step (vs. pre-existing) so rollback
	// only deletes topics it actually created — not pre-existing topics from a prior attempt.
	specs := s.infraTopicSpecs()
	rf := int16(s.config.InfraTopicReplicationFactor) //nolint:gosec // G115: value validated 1–32767 at startup; cannot overflow int16
	deadLetterName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, specs[0].suffix)
	defaultName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, specs[1].suffix)
	var dlqCreatedHere, defaultCreatedHere bool

	if err := s.kafka.CreateTopic(ctx, deadLetterName, specs[0].partitions, rf, specs[0].cfg); err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			recordCreateTenantTopicProvisionError(topicTypeDLQ, sagaPhaseCreate)
			return nil, fmt.Errorf("create dead-letter topic: %w", err)
		}
		// TopicAlreadyExists: pre-existing, not created by this saga step.
	} else {
		dlqCreatedHere = true
	}

	if err := s.kafka.CreateTopic(ctx, defaultName, specs[1].partitions, rf, specs[1].cfg); err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			recordCreateTenantTopicProvisionError(topicTypeDefault, sagaPhaseCreate)
			// Saga rollback: only delete the DLQ if this saga step created it.
			if dlqCreatedHere {
				recordCreateTenantTopicProvisionError(topicTypeDLQ, sagaPhaseRollback)
				if dlqErr := s.kafka.DeleteTopic(ctx, deadLetterName); dlqErr != nil {
					recordCreateTenantTopicProvisionError(topicTypeDLQ, sagaPhaseRollbackFailed)
					s.logger.Error().Err(dlqErr).
						Str(logging.LogKeyTenantSlug, tenant.Slug).
						Str("topic", deadLetterName).
						Msg("saga rollback: failed to delete dead-letter topic after default topic creation failure")
				}
			}
			return nil, fmt.Errorf("create default topic: %w", err)
		}
		// TopicAlreadyExists: pre-existing.
	} else {
		defaultCreatedHere = true
	}

	// Create tenant record
	if err := s.tenants.Create(ctx, tenant); err != nil {
		// Saga rollback: delete only the topics created by this saga step, in reverse order.
		if defaultCreatedHere {
			recordCreateTenantTopicProvisionError(topicTypeDefault, sagaPhaseRollback)
			if defErr := s.kafka.DeleteTopic(ctx, defaultName); defErr != nil {
				recordCreateTenantTopicProvisionError(topicTypeDefault, sagaPhaseRollbackFailed)
				s.logger.Error().Err(defErr).
					Str(logging.LogKeyTenantSlug, tenant.Slug).
					Str("topic", defaultName).
					Msg("saga rollback: failed to delete default topic")
			}
		}
		if dlqCreatedHere {
			recordCreateTenantTopicProvisionError(topicTypeDLQ, sagaPhaseRollback)
			if dlqErr := s.kafka.DeleteTopic(ctx, deadLetterName); dlqErr != nil {
				recordCreateTenantTopicProvisionError(topicTypeDLQ, sagaPhaseRollbackFailed)
				s.logger.Error().Err(dlqErr).
					Str(logging.LogKeyTenantSlug, tenant.Slug).
					Str("topic", deadLetterName).
					Msg("saga rollback: failed to delete dead-letter topic")
			}
		}
		return nil, fmt.Errorf("create tenant: %w", err)
	}
	// Tenant is now StatusActive in the DB — increment the gauge immediately so that any
	// subsequent return does not under-count. Key INPUT is already validated up front, but
	// the key store-write (s.keys.Create) and the residual internal TenantID assertion in
	// key.Validate() still return after this point, so the gauge-before-return ordering
	// remains load-bearing for those paths.
	IncActiveTenants()

	// Create default quotas
	quota := &TenantQuota{
		TenantID:         tenant.ID,
		MaxTopics:        s.config.MaxTopicsPerTenant,
		MaxPartitions:    s.config.MaxTopicsPerTenant * s.config.DefaultPartitions,
		MaxStorageBytes:  s.config.MaxStorageBytes,
		ProducerByteRate: s.config.DefaultProducerRate,
		ConsumerByteRate: s.config.DefaultConsumerRate,
	}
	if err := s.quotas.Create(ctx, quota); err != nil {
		// Quotas are non-critical: tenant is usable without quotas (they default to unlimited).
		// Failing the entire CreateTenant for a quota write failure would be worse than
		// operating without quotas. The error is logged at Error level for operator visibility.
		s.logger.Error().Err(err).Str(logging.LogKeyTenantUUID, tenant.ID).Msg("Failed to create quotas")
	}

	response := &CreateTenantResponse{
		Tenant: tenant,
	}

	// Register initial key if provided
	if req.PublicKey != nil {
		key := &TenantKey{
			KeyID:     req.PublicKey.KeyID,
			TenantID:  tenant.ID,
			Algorithm: req.PublicKey.Algorithm,
			PublicKey: req.PublicKey.PublicKey,
			ExpiresAt: req.PublicKey.ExpiresAt,
		}
		if err := key.Validate(); err != nil {
			return nil, fmt.Errorf("invalid key: %w", err)
		}

		if err := s.keys.Create(ctx, key); err != nil {
			return nil, fmt.Errorf("create key: %w", err)
		}
		response.Key = key
	}

	// Audit log
	s.auditLog(ctx, tenant.ID, ActionCreateTenant, Metadata{
		"name":          tenant.Name,
		"consumer_type": tenant.ConsumerType,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantUUID, tenant.ID).
		Str("name", tenant.Name).
		Msg("Tenant created")

	s.emitEvent(eventbus.TopicsChanged)
	s.emitEvent(eventbus.TenantConfigChanged)

	return response, nil
}

// GetTenantBySlug retrieves a tenant by slug. It satisfies TenantLookupFunc for
// RequireTenant middleware wiring and is also called directly by HTTP handlers (e.g., GetTenant).
func (s *Service) GetTenantBySlug(ctx context.Context, slug string) (*Tenant, error) {
	tenant, err := s.tenants.GetBySlug(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return tenant, nil
}

// ListTenants returns tenants matching the given options.
func (s *Service) ListTenants(ctx context.Context, opts ListOptions) ([]*Tenant, int, error) {
	tenants, total, err := s.tenants.List(ctx, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list tenants: %w", err)
	}
	return tenants, total, nil
}

// UpdateTenant updates tenant metadata.
func (s *Service) UpdateTenant(ctx context.Context, tenantID string, req UpdateTenantRequest) (*Tenant, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}

	if tenant.Status == StatusDeleted {
		return nil, ErrTenantDeleted
	}

	if req.Slug != nil {
		return nil, ErrSlugImmutableViaPatch
	}

	if req.Name != nil {
		tenant.Name = *req.Name
	}
	if req.ConsumerType != nil {
		tenant.ConsumerType = *req.ConsumerType
	}
	if req.Metadata != nil {
		tenant.Metadata = req.Metadata
	}

	// Re-validate after applying partial updates (defense in depth)
	if err := tenant.Validate(); err != nil {
		return nil, fmt.Errorf("validate updated tenant: %w", err)
	}

	if err := s.tenants.Update(ctx, tenant); err != nil {
		return nil, fmt.Errorf("update tenant: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionUpdateTenant, Metadata{
		"name":          tenant.Name,
		"consumer_type": tenant.ConsumerType,
	})

	s.emitEvent(eventbus.TenantConfigChanged)

	return tenant, nil
}

// SuspendTenant temporarily disables a tenant.
func (s *Service) SuspendTenant(ctx context.Context, tenantID string) error {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status == StatusDeleted {
		return ErrTenantDeleted
	}
	if tenant.Status != StatusActive {
		return fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	if err := s.tenants.UpdateStatus(ctx, tenant.ID, StatusSuspended); err != nil {
		return fmt.Errorf("suspend tenant: %w", err)
	}

	// Tenant transitioned from StatusActive → StatusSuspended; always safe to Dec
	// because SuspendTenant only accepts StatusActive tenants (checked above).
	DecActiveTenants()

	s.auditLog(ctx, tenant.ID, ActionSuspendTenant, nil)

	s.logger.Info().Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Tenant suspended")

	s.emitEvent(eventbus.TopicsChanged)
	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// ReactivateTenant reactivates a suspended tenant.
// Infrastructure topics (DLQ and default) are created idempotently before updating status.
// Topic creation errors are logged and metered but do NOT block reactivation (§IV).
func (s *Service) ReactivateTenant(ctx context.Context, tenantID string) error {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status == StatusDeleted {
		return ErrTenantDeleted
	}
	if tenant.Status != StatusSuspended {
		return fmt.Errorf("%w: current status is %s", ErrTenantNotActive, tenant.Status)
	}

	// Idempotently recreate infrastructure topics that may be absent for tenants
	// that were suspended before the channel-topic routing feature was deployed.
	rf := int16(s.config.InfraTopicReplicationFactor) //nolint:gosec // G115: value validated 1–32767 at startup; cannot overflow int16
	for _, spec := range s.infraTopicSpecs() {
		name := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, spec.suffix)
		if err := s.kafka.CreateTopic(ctx, name, spec.partitions, rf, spec.cfg); err != nil {
			if errors.Is(err, kerr.TopicAlreadyExists) {
				continue // idempotent — topic already exists, nothing to do
			}
			s.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, tenant.Slug).
				Str("topic", name).
				Str("topic_type", spec.topicType).
				Msg("failed to provision infrastructure topic during reactivation")
			recordReactivateTopicProvisionError(spec.topicType)
			// Continue with reactivation regardless (§IV: graceful degradation).
		}
	}

	if err := s.tenants.UpdateStatus(ctx, tenant.ID, StatusActive); err != nil {
		return fmt.Errorf("reactivate tenant: %w", err)
	}

	// Tenant transitioned from StatusSuspended → StatusActive; always safe to Inc
	// because ReactivateTenant only accepts StatusSuspended tenants (checked above).
	IncActiveTenants()

	s.auditLog(ctx, tenant.ID, ActionReactivateTenant, nil)

	s.logger.Info().Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Tenant reactivated")

	s.emitEvent(eventbus.TopicsChanged)
	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// DeprovisionTenant initiates tenant deletion. When force=false, sets status to
// deprovisioning with a grace period (not reactivatable via API). When force=true, sets
// status directly to deleted — no grace period, immediate cleanup, no reactivation.
// Valid source states: active, suspended (and deprovisioning when force=true).
// Multi-step cleanup continues on individual failures (Constitution IV).
func (s *Service) DeprovisionTenant(ctx context.Context, tenantID string, force bool) error {
	// Validate tenant state before deprovisioning
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status == StatusDeleted {
		return ErrTenantDeleted
	}
	if !force && tenant.Status == StatusDeprovisioning {
		return fmt.Errorf("%w: already deprovisioning", ErrTenantNotActive)
	}

	if force {
		if err := s.forceDeleteTenant(ctx, tenant); err != nil {
			return err
		}
		// Only Dec when the tenant was active; a suspended tenant was already Dec'd by SuspendTenant.
		if tenant.Status == StatusActive {
			DecActiveTenants()
		}
		return nil
	}

	// --- Grace-period deprovision (existing behavior) ---

	// Update status to deprovisioning
	if err := s.tenants.UpdateStatus(ctx, tenant.ID, StatusDeprovisioning); err != nil {
		return fmt.Errorf("set deprovisioning status: %w", err)
	}
	// Only Dec when the tenant was active; a suspended tenant was already Dec'd by SuspendTenant.
	if tenant.Status == StatusActive {
		DecActiveTenants()
	}

	// Revoke all keys immediately
	if err := s.keys.RevokeAllForTenant(ctx, tenant.ID); err != nil {
		s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Failed to revoke keys")
	} else {
		s.emitEvent(eventbus.KeysChanged)
	}

	// Set deprovision deadline
	gracePeriod := time.Duration(s.config.DeprovisionGraceDays) * 24 * time.Hour
	deprovisionAt := time.Now().Add(gracePeriod)
	if err := s.tenants.SetDeprovisionAt(ctx, tenant.ID, &deprovisionAt); err != nil {
		s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Failed to set deprovision deadline")
	}

	// Update topic retention to grace period (Kafka will delete after)
	if s.routingRules != nil {
		rules, err := s.routingRules.GetAll(ctx, tenant.ID)
		if err != nil {
			s.logger.Warn().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).
				Msg("Failed to get routing rules for topic retention update")
		} else {
			gracePeriodMs := int64(s.config.DeprovisionGraceDays) * msPerDay
			seen := make(map[string]struct{})
			for _, rule := range rules {
				for _, suffix := range rule.AllTopicSuffixes() {
					if _, ok := seen[suffix]; ok {
						continue
					}
					seen[suffix] = struct{}{}
					topicName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, suffix)
					if err := s.kafka.SetTopicConfig(ctx, topicName, map[string]string{
						kafka.RetentionMsConfigKey: strconv.FormatInt(gracePeriodMs, 10),
					}); err != nil {
						s.logger.Error().Err(err).
							Str(logging.LogKeyTenantSlug, tenant.Slug).
							Str("topic", topicName).
							Msg("Failed to update topic retention")
					}
				}
			}
		}
	}

	s.auditLog(ctx, tenant.ID, ActionDeprovisionTenant, Metadata{
		"grace_days":     s.config.DeprovisionGraceDays,
		"deprovision_at": deprovisionAt,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Time("deprovision_at", deprovisionAt).
		Msg("Tenant deprovisioning initiated")

	s.emitEvent(eventbus.TopicsChanged)
	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// forceDeleteTenant immediately deletes a tenant — no grace period, no reactivation.
// Used by test cleanup and admin force-delete. Multi-step cleanup continues on
// individual failures (Constitution IV).
func (s *Service) forceDeleteTenant(ctx context.Context, tenant *Tenant) error {
	// Read routing rules BEFORE deleting them — needed for Kafka topic cleanup below.
	var topicSuffixes []string
	if s.routingRules != nil {
		rules, err := s.routingRules.GetAll(ctx, tenant.ID)
		if err != nil {
			s.logger.Debug().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).
				Msg("Force-delete: no routing rules to clean up") // not-found is expected
		} else {
			seen := make(map[string]struct{})
			for _, rule := range rules {
				for _, suffix := range rule.AllTopicSuffixes() {
					if _, ok := seen[suffix]; !ok {
						seen[suffix] = struct{}{}
						topicSuffixes = append(topicSuffixes, suffix)
					}
				}
			}
		}
	}

	// Set status to deleted immediately
	if err := s.tenants.UpdateStatus(ctx, tenant.ID, StatusDeleted); err != nil {
		return fmt.Errorf("set deleted status: %w", err)
	}

	// Revoke all JWT signing keys (continue on failure)
	if err := s.keys.RevokeAllForTenant(ctx, tenant.ID); err != nil {
		s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Force-delete: failed to revoke signing keys")
	} else {
		s.emitEvent(eventbus.KeysChanged)
	}

	// Revoke all API keys (continue on failure — Constitution IX: deleted tenant must not authenticate)
	if s.apiKeys != nil {
		if apiKeys, _, err := s.apiKeys.ListByTenant(ctx, tenant.ID, ListOptions{Limit: 10000}); err != nil {
			s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Force-delete: failed to list API keys for revocation")
		} else {
			anyRevoked := false
			for _, key := range apiKeys {
				if key.IsActive {
					if err := s.apiKeys.Revoke(ctx, key.KeyID); err != nil {
						s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Str("key_id", key.KeyID).
							Msg("Force-delete: failed to revoke API key")
					} else {
						anyRevoked = true
					}
				}
			}
			if anyRevoked {
				s.emitEvent(eventbus.KeysChanged)
			}
		}
	}

	// Delete routing rules (continue on failure)
	if s.routingRules != nil {
		if err := s.routingRules.DeleteAll(ctx, tenant.ID); err != nil {
			s.logger.Debug().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).
				Msg("Force-delete: routing rules delete (not-found is expected)")
		}
	}

	// Delete channel rules (continue on failure)
	if s.channelRules != nil {
		if err := s.channelRules.Delete(ctx, tenant.ID); err != nil {
			s.logger.Debug().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).
				Msg("Force-delete: channel rules delete (not-found is expected)")
		}
	}

	// Delete Kafka topics using pre-read suffixes (continue on failure)
	for _, suffix := range topicSuffixes {
		topicName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, suffix)
		if err := s.kafka.DeleteTopic(ctx, topicName); err != nil {
			s.logger.Error().Err(err).
				Str(logging.LogKeyTenantSlug, tenant.Slug).Str("topic", topicName).
				Msg("Force-delete: failed to delete topic")
		}
	}

	s.auditLog(ctx, tenant.ID, ActionDeprovisionTenant, Metadata{
		"force": true,
	})

	s.logger.Info().Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Tenant force-deleted")

	s.emitEvent(eventbus.TopicsChanged)
	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// CreateKey registers a new public key for a tenant.
func (s *Service) CreateKey(ctx context.Context, tenantID string, req CreateKeyRequest) (*TenantKey, error) {
	// Verify tenant exists and is active
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status != StatusActive {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	key := &TenantKey{
		KeyID:     req.KeyID,
		TenantID:  tenant.ID,
		Algorithm: req.Algorithm,
		PublicKey: req.PublicKey,
		ExpiresAt: req.ExpiresAt,
	}

	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	if err := s.keys.Create(ctx, key); err != nil {
		return nil, fmt.Errorf("create key: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionCreateKey, Metadata{
		"key_id":    key.KeyID,
		"algorithm": key.Algorithm,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Str("key_id", key.KeyID).
		Msg("Key created")

	s.emitEvent(eventbus.KeysChanged)

	return key, nil
}

// ListKeys returns keys for a tenant with pagination.
func (s *Service) ListKeys(ctx context.Context, tenantID string, opts ListOptions) ([]*TenantKey, int, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, 0, fmt.Errorf("get tenant: %w", err)
	}
	keys, total, err := s.keys.ListByTenant(ctx, tenant.ID, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list keys: %w", err)
	}
	return keys, total, nil
}

// RevokeKey revokes a key.
func (s *Service) RevokeKey(ctx context.Context, tenantID, keyID string) error {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	// Verify key belongs to tenant (compare UUID against UUID)
	key, err := s.keys.Get(ctx, keyID)
	if err != nil {
		return fmt.Errorf("get key: %w", err)
	}
	if key.TenantID != tenant.ID {
		return ErrKeyNotOwnedByTenant
	}

	if err := s.keys.Revoke(ctx, keyID); err != nil {
		return fmt.Errorf("revoke key: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionRevokeKey, Metadata{
		"key_id": keyID,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Str("key_id", keyID).
		Msg("Key revoked")

	s.emitEvent(eventbus.KeysChanged)

	return nil
}

// GetActiveKeys returns all active keys (for WS Gateway cache refresh).
func (s *Service) GetActiveKeys(ctx context.Context) ([]*TenantKey, error) {
	keys, err := s.keys.GetActiveKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active keys: %w", err)
	}
	return keys, nil
}

// TopicNamespace returns the configured Kafka topic namespace.
// Used to build full topic names: {namespace}.{tenantID}.{suffix}
func (s *Service) TopicNamespace() string {
	return s.config.TopicNamespace
}

// GetQuota returns quotas for a tenant.
func (s *Service) GetQuota(ctx context.Context, tenantID string) (*TenantQuota, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	quota, err := s.quotas.Get(ctx, tenant.ID)
	if err != nil {
		return nil, fmt.Errorf("get quota: %w", err)
	}
	return quota, nil
}

// UpdateQuota updates quotas for a tenant.
func (s *Service) UpdateQuota(ctx context.Context, tenantID string, req UpdateQuotaRequest) (*TenantQuota, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}

	quota, err := s.quotas.Get(ctx, tenant.ID)
	if err != nil {
		return nil, fmt.Errorf("get quota: %w", err)
	}

	if req.MaxTopics != nil {
		quota.MaxTopics = *req.MaxTopics
	}
	if req.MaxPartitions != nil {
		quota.MaxPartitions = *req.MaxPartitions
	}
	if req.MaxStorageBytes != nil {
		quota.MaxStorageBytes = *req.MaxStorageBytes
	}
	if req.ProducerByteRate != nil {
		quota.ProducerByteRate = *req.ProducerByteRate
	}
	if req.ConsumerByteRate != nil {
		quota.ConsumerByteRate = *req.ConsumerByteRate
	}
	if req.MaxConnections != nil {
		quota.MaxConnections = *req.MaxConnections
	}

	// Validate non-negative values (defense in depth)
	if quota.MaxTopics < 0 || quota.MaxPartitions < 0 || quota.MaxStorageBytes < 0 ||
		quota.ProducerByteRate < 0 || quota.ConsumerByteRate < 0 || quota.MaxConnections < 0 {
		return nil, fmt.Errorf("%w: values must be non-negative", ErrInvalidQuota)
	}

	if err := s.quotas.Update(ctx, quota); err != nil {
		return nil, fmt.Errorf("update quota: %w", err)
	}

	// Apply Kafka quotas (keyed by slug — Kafka principal is based on slug)
	if err := s.kafka.SetQuota(ctx, tenant.Slug, QuotaConfig{
		ProducerByteRate: quota.ProducerByteRate,
		ConsumerByteRate: quota.ConsumerByteRate,
	}); err != nil {
		s.logger.Error().Err(err).Str(logging.LogKeyTenantSlug, tenant.Slug).Msg("Failed to apply Kafka quotas")
	}

	s.auditLog(ctx, tenant.ID, ActionUpdateQuota, Metadata{
		"max_topics":         quota.MaxTopics,
		"max_partitions":     quota.MaxPartitions,
		"producer_byte_rate": quota.ProducerByteRate,
		"consumer_byte_rate": quota.ConsumerByteRate,
	})

	return quota, nil
}

// GetAuditLog returns audit entries for a tenant.
func (s *Service) GetAuditLog(ctx context.Context, tenantID string, opts ListOptions) ([]*AuditEntry, int, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, 0, fmt.Errorf("get tenant: %w", err)
	}
	entries, total, err := s.audit.ListByTenant(ctx, tenant.ID, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list audit log: %w", err)
	}
	return entries, total, nil
}

// auditLog records an audit entry.
func (s *Service) auditLog(ctx context.Context, tenantID, action string, details Metadata) {
	// Get actor type, defaulting to system if not set
	actorType := auth.GetActorType(ctx)
	if actorType == auth.DefaultActorType {
		actorType = ActorTypeSystem
	}

	entry := &AuditEntry{
		TenantID:  tenantID,
		Action:    action,
		Actor:     auth.GetActor(ctx),
		ActorType: actorType,
		IPAddress: auth.GetClientIPFromContext(ctx),
		Details:   details,
	}
	if err := s.audit.Log(ctx, entry); err != nil {
		s.logger.Error().Err(err).Str("action", action).Msg("Failed to write audit log")
	}
}

// AuditLog writes an audit log entry with the actor resolved from ctx.
// Exported so handlers outside the service (e.g., ConnectionsHandler) can write audit entries.
func (s *Service) AuditLog(ctx context.Context, tenantID, action string, details Metadata) {
	s.auditLog(ctx, tenantID, action, details)
}

// emitEvent publishes a provisioning change event to the event bus.
// EventBus is guaranteed non-nil by NewService constructor validation.
func (s *Service) emitEvent(eventType eventbus.EventType) {
	s.eventBus.Publish(eventbus.Event{Type: eventType})
}

// WithActor adds actor information to context.
// This is an alias for auth.WithActor for backwards compatibility.
var WithActor = auth.WithActor

// GetChannelRules retrieves channel rules for a tenant.
func (s *Service) GetChannelRules(ctx context.Context, tenantID string) (*types.TenantChannelRules, error) {
	if s.channelRules == nil {
		return nil, ErrChannelRulesNotConfigured
	}

	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}

	rules, err := s.channelRules.Get(ctx, tenant.ID)
	if err != nil {
		return nil, fmt.Errorf("get channel rules: %w", err)
	}
	return rules, nil
}

// SetChannelRules creates or updates channel rules for a tenant.
func (s *Service) SetChannelRules(ctx context.Context, tenantID string, rules *types.ChannelRules) error {
	if s.channelRules == nil {
		return ErrChannelRulesNotConfigured
	}

	// Verify tenant exists and is active
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status != StatusActive {
		return fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	// Validate rules
	if err := rules.Validate(); err != nil {
		return fmt.Errorf("invalid channel rules: %w", err)
	}

	// Upsert in store
	if err := s.channelRules.Update(ctx, tenant.ID, rules); err != nil {
		return fmt.Errorf("set channel rules: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionSetChannelRules, Metadata{
		"public_patterns": len(rules.Public),
		"group_mappings":  len(rules.GroupMappings),
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Int("public_patterns", len(rules.Public)).
		Int("group_mappings", len(rules.GroupMappings)).
		Msg("Channel rules set")

	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// DeleteChannelRules deletes channel rules for a tenant.
func (s *Service) DeleteChannelRules(ctx context.Context, tenantID string) error {
	if s.channelRules == nil {
		return ErrChannelRulesNotConfigured
	}

	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	// Verify rules exist (will return error if not found)
	if _, err := s.channelRules.Get(ctx, tenant.ID); err != nil {
		return fmt.Errorf("get channel rules: %w", err)
	}

	// Delete from store
	if err := s.channelRules.Delete(ctx, tenant.ID); err != nil {
		return fmt.Errorf("delete channel rules: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionDeleteChannelRules, nil)

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Msg("Channel rules deleted")

	s.emitEvent(eventbus.TenantConfigChanged)

	return nil
}

// checkEgressTopicsAllowed enforces the Pro gate on routing-rule egress topics
// (license.EgressTopics, ADR-0018). Only rules that actually carry egress topics
// are gated: an ingress-only rule set is Community routing (ADR-0014) and MUST
// pass on every edition. nil editionManager (tests / no license wiring) means no
// gate — consistent with the other edition checks in this service.
func (s *Service) checkEgressTopicsAllowed(rules []TopicRoutingRule) error {
	if s.editionManager == nil {
		return nil
	}
	for _, rule := range rules {
		if len(rule.EgressTopics) == 0 {
			continue
		}
		if !s.editionManager.HasFeature(license.EgressTopics) {
			return fmt.Errorf("edition gate: %w", license.NewFeatureError(license.EgressTopics, s.editionManager.Edition()))
		}
		return nil // feature present — one check covers the whole set
	}
	return nil
}

// GetRoutingRules retrieves routing rules for a tenant.
func (s *Service) GetRoutingRules(ctx context.Context, tenantID string) ([]TopicRoutingRule, error) {
	if s.routingRules == nil {
		return nil, ErrRoutingRulesNotConfigured
	}

	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}

	rules, err := s.routingRules.GetAll(ctx, tenant.ID)
	if err != nil {
		return nil, fmt.Errorf("get routing rules: %w", err)
	}
	return rules, nil
}

// ReplaceRoutingRules atomically replaces all routing rules for a tenant.
func (s *Service) ReplaceRoutingRules(ctx context.Context, tenantID string, rules []TopicRoutingRule) error {
	if s.routingRules == nil {
		return ErrRoutingRulesNotConfigured
	}

	// Verify tenant exists and is active.
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status != StatusActive {
		return fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	// Edition limit: routing rules per tenant.
	if s.editionManager != nil {
		limits := s.editionManager.Limits()
		if err := limits.CheckRoutingRulesPerTenant(len(rules)); err != nil {
			return fmt.Errorf("edition limit: %w", err)
		}
	}

	// Normalize bare * to ** before validation so patterns are stored in canonical form.
	for i := range rules {
		rules[i].Pattern = routing.NormalizePattern(rules[i].Pattern)
	}

	// Validate rule structure (count, patterns, topics, priorities).
	if err := ValidateRoutingRules(rules, s.config.MaxRoutingRulesPerTenant, s.config.MaxTopicsPerRule); err != nil {
		return fmt.Errorf("invalid routing rules: %w", err)
	}

	// Edition gate: egress topics are Pro (license.EgressTopics, ADR-0018). Checked
	// AFTER shape validation so malformed rules keep returning 400 on every edition,
	// and ONLY when a rule actually carries egress topics — an ingress-only rule set
	// must never trip this gate (Community keeps full routing per ADR-0014).
	if err := s.checkEgressTopicsAllowed(rules); err != nil {
		return err
	}

	// Verify all referenced topics are provisioned (deduped by suffix).
	seen := make(map[string]struct{})
	for _, rule := range rules {
		for _, suffix := range rule.AllTopicSuffixes() {
			if _, ok := seen[suffix]; ok {
				continue
			}
			seen[suffix] = struct{}{}
			topicName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, suffix)
			exists, err := s.kafka.TopicExists(ctx, topicName)
			if err != nil {
				return fmt.Errorf("check topic %s: %w", topicName, err)
			}
			if !exists {
				return fmt.Errorf("%w: %s", ErrTopicNotProvisioned, topicName)
			}
		}
	}

	// Atomically replace all rules in store.
	if err := s.routingRules.Replace(ctx, tenant.ID, rules); err != nil {
		return fmt.Errorf("replace routing rules: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionReplaceRoutingRules, Metadata{
		"rule_count": len(rules),
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Int("rule_count", len(rules)).
		Msg("Routing rules replaced")

	s.emitEvent(eventbus.TenantConfigChanged)
	s.emitEvent(eventbus.TopicsChanged)

	return nil
}

// AddRoutingRule adds a single routing rule for a tenant.
func (s *Service) AddRoutingRule(ctx context.Context, tenantID string, rule TopicRoutingRule) error {
	if s.routingRules == nil {
		return ErrRoutingRulesNotConfigured
	}

	// Verify tenant exists and is active.
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status != StatusActive {
		return fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	// Normalize bare * to ** before validation so patterns are stored in canonical form.
	rule.Pattern = routing.NormalizePattern(rule.Pattern)

	// Validate rule structure (pattern syntax, ingress topic, per-rule topic limit).
	if err := ValidateRoutingRules([]TopicRoutingRule{rule}, 0, s.config.MaxTopicsPerRule); err != nil {
		return fmt.Errorf("invalid routing rule: %w", err)
	}

	// Edition gate: egress topics are Pro (license.EgressTopics, ADR-0018) — see
	// ReplaceRoutingRules. Ingress-only rules never trip it.
	if err := s.checkEgressTopicsAllowed([]TopicRoutingRule{rule}); err != nil {
		return err
	}

	// Enforce per-tenant rule count limits (Constitution II: every boundary validates inputs).
	// BOTH the config limit and the edition limit bind here, whichever is lower. The edition
	// limit can sit BELOW the config limit (Community 10 < MAX_ROUTING_RULES_PER_TENANT's
	// default 100), so this incremental path must check it too — otherwise a tenant walks
	// past its edition cap one POST at a time and the tier wall (ADR-0014) is void.
	// Known residual: this count read is not serialized with the insert — two
	// concurrent Adds can both observe N-1 and land N+1. Bounded overshoot only
	// (by the concurrency degree), and the next Add re-checks; closing it would
	// require pushing the config+edition limits into the repository transaction.
	if s.config.MaxRoutingRulesPerTenant > 0 || s.editionManager != nil {
		_, total, err := s.routingRules.List(ctx, tenant.ID, 1, 0)
		if err != nil {
			return fmt.Errorf("count routing rules: %w", err)
		}
		if s.config.MaxRoutingRulesPerTenant > 0 && total >= s.config.MaxRoutingRulesPerTenant {
			return fmt.Errorf("%w: got %d, max %d", ErrTooManyRoutingRules, total+1, s.config.MaxRoutingRulesPerTenant)
		}
		if s.editionManager != nil {
			if err := s.editionManager.Limits().CheckRoutingRulesPerTenant(total + 1); err != nil {
				return fmt.Errorf("edition limit: %w", err)
			}
		}
	}

	// Tenant-wide ingress/egress disjointness (both directions): the new rule's
	// egress topics must not be existing ingress topics, and the new rule's
	// ingress topic must not be an existing egress topic. Either overlap would
	// pull an egress topic into the consume set and deliver its copies (ADR-0018).
	// This read-validate pair is a fast-fail pre-check only (§II defense in
	// depth): it is NOT atomic with the insert. The authoritative check runs
	// inside RoutingRulesRepository.Add's transaction under a per-tenant
	// advisory lock, where a concurrent Add's committed row is visible.
	existing, err := s.routingRules.GetAll(ctx, tenant.ID)
	if err != nil {
		return fmt.Errorf("get routing rules for disjointness check: %w", err)
	}
	if err := ValidateEgressDisjoint(append(existing, rule)); err != nil {
		return fmt.Errorf("invalid routing rule: %w", err)
	}

	// Verify all referenced topics are provisioned.
	for _, suffix := range rule.AllTopicSuffixes() {
		topicName := kafka.BuildTopicName(s.config.TopicNamespace, tenant.Slug, suffix)
		exists, err := s.kafka.TopicExists(ctx, topicName)
		if err != nil {
			return fmt.Errorf("check topic %s: %w", topicName, err)
		}
		if !exists {
			return fmt.Errorf("%w: %s", ErrTopicNotProvisioned, topicName)
		}
	}

	if err := s.routingRules.Add(ctx, tenant.ID, rule); err != nil {
		return fmt.Errorf("add routing rule: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionAddRoutingRule, Metadata{
		"pattern":       rule.Pattern,
		"ingress_topic": rule.IngressTopic,
		"egress_topics": rule.EgressTopics,
		"priority":      rule.Priority,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Str("pattern", rule.Pattern).
		Int("priority", rule.Priority).
		Msg("Routing rule added")

	s.emitEvent(eventbus.TenantConfigChanged)
	s.emitEvent(eventbus.TopicsChanged)

	return nil
}

// ListRoutingRules returns paginated routing rules for a tenant ordered by priority ASC.
func (s *Service) ListRoutingRules(ctx context.Context, tenantID string, limit, offset int) ([]TopicRoutingRule, int, error) {
	if s.routingRules == nil {
		return nil, 0, ErrRoutingRulesNotConfigured
	}

	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, 0, fmt.Errorf("get tenant: %w", err)
	}

	rules, total, err := s.routingRules.List(ctx, tenant.ID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list routing rules: %w", err)
	}
	return rules, total, nil
}

// DeleteRoutingRules deletes routing rules for a tenant.
func (s *Service) DeleteRoutingRules(ctx context.Context, tenantID string) error {
	if s.routingRules == nil {
		return ErrRoutingRulesNotConfigured
	}

	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	if err := s.routingRules.DeleteAll(ctx, tenant.ID); err != nil {
		return fmt.Errorf("delete routing rules: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionDeleteRoutingRules, nil)

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Msg("Routing rules deleted")

	s.emitEvent(eventbus.TenantConfigChanged)
	s.emitEvent(eventbus.TopicsChanged)

	return nil
}

// CreateAPIKey generates and registers a new API key for a tenant.
func (s *Service) CreateAPIKey(ctx context.Context, tenantID string, req CreateAPIKeyRequest) (*APIKey, error) {
	// Verify tenant exists and is active
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	if tenant.Status != StatusActive {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotActive, tenant.Status)
	}

	// Generate server-side key ID
	keyID, err := GenerateAPIKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate api key id: %w", err)
	}

	key := &APIKey{
		KeyID:    keyID,
		TenantID: tenant.ID,
		Name:     req.Name,
	}

	if err := s.apiKeys.Create(ctx, key); err != nil {
		return nil, fmt.Errorf("create api key: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionCreateAPIKey, Metadata{
		"key_id": key.KeyID,
		"name":   key.Name,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Str("key_id", key.KeyID).
		Msg("API key created")

	s.emitEvent(eventbus.APIKeysChanged)

	return key, nil
}

// ListAPIKeys returns API keys for a tenant with pagination.
func (s *Service) ListAPIKeys(ctx context.Context, tenantID string, opts ListOptions) ([]*APIKey, int, error) {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return nil, 0, fmt.Errorf("get tenant: %w", err)
	}
	keys, total, err := s.apiKeys.ListByTenant(ctx, tenant.ID, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list api keys: %w", err)
	}
	return keys, total, nil
}

// RevokeAPIKey revokes an API key.
func (s *Service) RevokeAPIKey(ctx context.Context, tenantID, keyID string) error {
	tenant, err := s.tenants.GetBySlug(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	// Verify key belongs to tenant (compare UUID against UUID)
	key, err := s.apiKeys.Get(ctx, keyID)
	if err != nil {
		return fmt.Errorf("get api key: %w", err)
	}
	if key.TenantID != tenant.ID {
		return ErrAPIKeyNotOwnedByTenant
	}

	if err := s.apiKeys.Revoke(ctx, keyID); err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}

	s.auditLog(ctx, tenant.ID, ActionRevokeAPIKey, Metadata{
		"key_id": keyID,
	})

	s.logger.Info().
		Str(logging.LogKeyTenantSlug, tenant.Slug).
		Str("key_id", keyID).
		Msg("API key revoked")

	s.emitEvent(eventbus.APIKeysChanged)

	return nil
}

// GetActiveAPIKeys returns all active API keys (for gateway cache refresh).
func (s *Service) GetActiveAPIKeys(ctx context.Context) ([]*APIKey, error) {
	keys, err := s.apiKeys.GetActiveAPIKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active api keys: %w", err)
	}
	return keys, nil
}

// isReservedSlug reports whether s is a reserved path segment.
// All router sub-route segments (action endpoints, resource collections) are reserved
// so that no tenant slug can shadow an API route.
func isReservedSlug(s string) bool {
	switch s {
	case slugReservedAdmin, slugReservedAPI, slugReservedHealth, slugReservedReady,
		slugReservedMetrics, slugReservedVersion, slugReservedEdition, slugReservedConfig,
		slugReservedDebug, slugReservedInternal, slugReservedSystem, slugReservedSukko,
		slugReservedKeys, slugReservedAPIKeys, slugReservedRoutingRules, slugReservedQuotas,
		slugReservedAudit, slugReservedChannelRules, slugReservedTokens,
		slugReservedSuspend, slugReservedReactivate, slugReservedRename, slugReservedTestAccess:
		return true
	}
	return false
}

// RenameTenant renames a tenant slug, migrating Kafka ACLs and topics.
// The old slug's topics and ACLs remain accessible until the hold period expires.
func (s *Service) RenameTenant(ctx context.Context, tenantSlug, newSlug string) (*Tenant, error) {
	start := s.clock()

	// Step 1: validate new slug format
	if err := ValidateSlug(newSlug); err != nil {
		// ValidateSlug already wraps ErrSlugInvalid; no need to re-wrap.
		return nil, fmt.Errorf("rename: %w", err)
	}

	// Step 2: reserved slug check
	if isReservedSlug(newSlug) {
		return nil, ErrSlugReserved
	}

	// Step 3: tenant lookup
	tenant, err := s.tenants.GetBySlug(ctx, tenantSlug)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}

	// Step 4: must be active
	if tenant.Status != StatusActive {
		return nil, fmt.Errorf("%w: current status is %s", ErrTenantNotActive, tenant.Status)
	}

	// Step 5: same-slug check
	if newSlug == tenant.Slug {
		return nil, ErrSlugUnchanged
	}

	// Step 6 & 7: hold period gate (with lazy clear)
	if tenant.SlugRenamedAt != nil {
		elapsed := s.clock().Sub(*tenant.SlugRenamedAt)
		if elapsed >= s.config.SlugRenameTopicHoldPeriod {
			// Hold period expired — clear residue so the rename starts clean
			if clearErr := s.tenants.ClearRenameState(ctx, tenant.ID); clearErr != nil {
				s.logger.Warn().Err(clearErr).Str(logging.LogKeyTenantUUID, tenant.ID).
					Msg("RenameTenant: lazy clear of expired rename state failed, proceeding anyway")
			}
		} else {
			return nil, ErrSlugRenameInProgress
		}
	}

	// Step 8: check new slug availability
	if existing, err := s.tenants.GetBySlug(ctx, newSlug); err == nil && existing.Status != StatusDeleted {
		return nil, ErrSlugAlreadyTaken
	}

	// Step 9: CAS lock — prevent concurrent renames
	if err := s.tenants.SetRenameState(ctx, tenant.ID, SlugRenameStatePending, tenant.Slug); err != nil {
		if errors.Is(err, ErrCASFailed) {
			return nil, ErrSlugRenameInProgress
		}
		return nil, fmt.Errorf("set rename state: %w", err)
	}

	namespace := s.config.TopicNamespace

	// Step 10: create new topic ACLs
	if err := s.kafka.CreateTopicACLs(ctx, newSlug, namespace); err != nil {
		return nil, fmt.Errorf("create topic ACLs for new slug: %w", err)
	}

	// Step 11: create topics under new slug
	rf := int16(s.config.InfraTopicReplicationFactor) //nolint:gosec // G115: value validated 1–32767 at startup
	for _, spec := range s.infraTopicSpecs() {
		topicName := kafka.BuildTopicName(namespace, newSlug, spec.suffix)
		if err := s.kafka.CreateTopic(ctx, topicName, spec.partitions, rf, spec.cfg); err != nil {
			if !errors.Is(err, kerr.TopicAlreadyExists) {
				return nil, fmt.Errorf("create topic %s for new slug: %w", topicName, err)
			}
		}
	}

	// Step 12: commit slug in DB
	updatedTenant, err := s.tenants.UpdateSlug(ctx, tenant.ID, newSlug)
	if err != nil {
		if errors.Is(err, ErrSlugAlreadyTaken) {
			// TOCTOU: another create/rename won the race — compensate
			if aclErr := s.kafka.DeleteTopicACLs(ctx, newSlug, namespace); aclErr != nil {
				s.logger.Error().Err(aclErr).Str("new_slug", newSlug).
					Msg("RenameTenant: TOCTOU compensation: failed to delete new slug ACLs")
			}
			for _, spec := range s.infraTopicSpecs() {
				topicName := kafka.BuildTopicName(namespace, newSlug, spec.suffix)
				if topErr := s.kafka.DeleteTopic(ctx, topicName); topErr != nil {
					s.logger.Error().Err(topErr).Str("topic", topicName).
						Msg("RenameTenant: TOCTOU compensation: failed to delete new slug topic")
				}
			}
			if clearErr := s.tenants.ClearRenameState(ctx, tenant.ID); clearErr != nil {
				s.logger.Error().Err(clearErr).Str(logging.LogKeyTenantUUID, tenant.ID).
					Msg("RenameTenant: TOCTOU compensation: failed to clear rename state")
			}
			return nil, ErrSlugAlreadyTaken
		}
		// Non-TOCTOU failure — tenant stays in 'pending' for startup scan
		s.logger.Error().Err(err).Str(logging.LogKeyTenantUUID, tenant.ID).Str("new_slug", newSlug).
			Msg("RenameTenant: UpdateSlug failed; tenant rename state is 'pending' for operator inspection")
		return nil, fmt.Errorf("commit slug rename: %w", err)
	}

	// Step 13: audit
	s.auditLog(ctx, tenant.ID, ActionRenameTenant, Metadata{
		"old_slug": tenant.Slug,
		"new_slug": newSlug,
	})

	// Step 14: remove old slug ACLs
	if err := s.kafka.DeleteTopicACLs(ctx, tenant.Slug, namespace); err != nil {
		s.logger.Error().Err(err).Str("old_slug", tenant.Slug).
			Msg("RenameTenant: failed to delete old slug ACLs (non-fatal, operator cleanup required)")
	}

	// Step 15: quota re-apply (non-fatal — quota failure does not roll back the rename)
	quotaFailed := false
	quota, quotaErr := s.quotas.Get(ctx, tenant.ID)
	if quotaErr != nil {
		quotaFailed = true
		s.logger.Warn().Err(quotaErr).Str(logging.LogKeyTenantUUID, tenant.ID).
			Msg("RenameTenant: failed to fetch quota for re-apply (skipping SetQuota)")
	} else {
		if err := s.kafka.DeleteQuota(ctx, tenant.Slug, namespace); err != nil {
			quotaFailed = true
			s.logger.Warn().Err(err).Str("old_slug", tenant.Slug).
				Msg("RenameTenant: failed to delete old slug quota")
		}
		if err := s.kafka.SetQuota(ctx, newSlug, QuotaConfig{
			ProducerByteRate: quota.ProducerByteRate,
			ConsumerByteRate: quota.ConsumerByteRate,
		}); err != nil {
			quotaFailed = true
			s.logger.Warn().Err(err).Str("new_slug", newSlug).
				Msg("RenameTenant: failed to set new slug quota")
		}
	}

	// Step 16: notify consumers
	// Re-emission on startup is intentional during the hold period — consumers MUST be idempotent.
	s.emitEvent(eventbus.TenantConfigChanged)

	// Step 17: log hold expiry using the DB-authoritative timestamp from UpdateSlug RETURNING
	if updatedTenant.SlugRenamedAt != nil {
		holdExpiry := updatedTenant.SlugRenamedAt.Add(s.config.SlugRenameTopicHoldPeriod)
		s.logger.Info().
			Str("old_slug", tenant.Slug).
			Str("new_slug", newSlug).
			Time("hold_expires_at", holdExpiry).
			Msg("Tenant slug renamed; old slug topics and ACLs retained until hold expiry")
	}

	// Step 18: record metrics
	status := renameStatusSuccess
	if quotaFailed {
		status = renameStatusPartialFailure
	}
	recordTenantRenamed(status, s.clock().Sub(start))

	return updatedTenant, nil
}

// Start performs the startup scan for pending/complete slug rename state residue.
// On start: detects incomplete sagas and re-emits config events for crash recovery.
// A scan failure only logs a warning — it MUST NOT block the service from serving requests.
func (s *Service) Start(ctx context.Context) error {
	tenants, err := s.tenants.ListPendingRenames(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Startup scan: failed to list pending renames; skipping scan")
		return nil
	}

	var pendingCount, completeCount int
	for _, t := range tenants {
		switch t.SlugRenameState {
		case SlugRenameStatePending:
			pendingCount++
			s.logger.Warn().
				Str(logging.LogKeyTenantUUID, t.ID).
				Str("previous_slug", t.PreviousSlug).
				Msg("Startup scan: tenant rename is in 'pending' state — operator investigation required")
		case SlugRenameStateComplete:
			if t.PreviousSlug != "" {
				completeCount++
				// Re-emit so ws-server refreshes its config (idempotent — consumers must handle duplicate events)
				s.emitEvent(eventbus.TenantConfigChanged)
			}
		}
	}

	recordStartupScanFindings(pendingCount, completeCount)

	// Initialize the provisioning_active_tenants gauge. A failure here is non-fatal —
	// we log at Warn and leave the gauge at its promauto default of 0 (not "No data" —
	// promauto always registers the metric; 0 is indistinguishable from zero tenants).
	// Service-layer Inc/Dec calls will correct the value over time.
	if n, err := s.tenants.CountActive(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Startup: failed to count active tenants for gauge init; gauge will read 0 until corrected by next operation")
	} else {
		SetActiveTenants(n)
	}

	return nil
}

const (
	// defaultWebhookMaxRetries is used when CreateWebhookRequest.MaxRetries is 0.
	defaultWebhookMaxRetries = 5
	// maxWebhookMaxRetries is the upper bound for the max_retries field.
	maxWebhookMaxRetries = 10
)

// privateBlocks is parsed once at init and used by isPrivateHost to check for SSRF.
var privateBlocks []*net.IPNet

func init() {
	cidrs := []string{
		"127.0.0.0/8",    // loopback
		"::1/128",        // IPv6 loopback
		"10.0.0.0/8",     // RFC-1918 private
		"172.16.0.0/12",  // RFC-1918 private
		"192.168.0.0/16", // RFC-1918 private
		"169.254.0.0/16", // link-local
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 ULA
		"100.64.0.0/10",  // shared address (RFC-6598, carrier-grade NAT)
		"0.0.0.0/8",      // "this" network
	}
	for _, cidr := range cidrs {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Sprintf("invalid private CIDR %q: %v", cidr, err))
		}
		privateBlocks = append(privateBlocks, block)
	}
}

// CreateWebhook creates a new webhook registration for a tenant.
// Validates the URL, enforces the per-tenant quota, encrypts the secret, and writes to the store.
func (s *Service) CreateWebhook(ctx context.Context, req CreateWebhookRequest) (*Webhook, error) {
	if s.webhooks == nil {
		return nil, ErrWebhookStoreNotConfigured
	}

	if err := validateWebhookURL(req.URL, s.config.WebhookAllowHTTP); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWebhookInvalidInput, err)
	}

	if req.ChannelPattern == "" {
		return nil, fmt.Errorf("%w: channel_pattern is required", ErrWebhookInvalidInput)
	}
	if req.Secret == "" {
		return nil, fmt.Errorf("%w: secret is required", ErrWebhookInvalidInput)
	}

	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = defaultWebhookMaxRetries
	}
	if maxRetries < 1 || maxRetries > maxWebhookMaxRetries {
		return nil, fmt.Errorf("%w: max_retries must be between 1 and %d, got %d", ErrWebhookInvalidInput, maxWebhookMaxRetries, maxRetries)
	}

	// Enforce per-tenant quota.
	if s.config.MaxWebhooksPerTenant > 0 {
		count, err := s.webhooks.CountActive(ctx, req.TenantID)
		if err != nil {
			return nil, fmt.Errorf("count active webhooks: %w", err)
		}
		if count >= s.config.MaxWebhooksPerTenant {
			return nil, ErrWebhookQuotaExceeded
		}
	}

	// Encrypt the secret at rest.
	secretEnc, err := crypto.EncryptCredential(req.Secret, s.config.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt webhook secret: %w", err)
	}

	// Generate ID.
	id, err := generateWebhookID()
	if err != nil {
		return nil, fmt.Errorf("generate webhook ID: %w", err)
	}

	w := &Webhook{
		ID:             id,
		TenantID:       req.TenantID,
		URL:            req.URL,
		ChannelPattern: req.ChannelPattern,
		SecretEnc:      secretEnc,
		Status:         types.WebhookStatusEnabled,
		MaxRetries:     maxRetries,
	}
	if err := s.webhooks.Create(ctx, w); err != nil {
		return nil, fmt.Errorf("create webhook: %w", err)
	}

	if s.config.InvalidationPublisher != nil {
		s.config.InvalidationPublisher.Publish(req.TenantID)
	}
	s.auditLog(ctx, req.TenantID, ActionCreateWebhook, Metadata{"webhook_id": w.ID, "url": w.URL})
	return w, nil
}

// GetWebhook retrieves a webhook by ID scoped to a tenant.
func (s *Service) GetWebhook(ctx context.Context, id, tenantID string) (*Webhook, error) {
	if s.webhooks == nil {
		return nil, ErrWebhookStoreNotConfigured
	}
	w, err := s.webhooks.GetByID(ctx, id, tenantID)
	if err != nil {
		return nil, fmt.Errorf("get webhook: %w", err)
	}
	return w, nil
}

// ListWebhooks returns paginated webhooks for a tenant.
func (s *Service) ListWebhooks(ctx context.Context, tenantID string, opts ListOptions) ([]*Webhook, int, error) {
	if s.webhooks == nil {
		return nil, 0, ErrWebhookStoreNotConfigured
	}
	webhooks, total, err := s.webhooks.List(ctx, tenantID, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list webhooks: %w", err)
	}
	return webhooks, total, nil
}

// UpdateWebhook applies a partial update to a webhook.
func (s *Service) UpdateWebhook(ctx context.Context, req UpdateWebhookRequest) (*Webhook, error) {
	if s.webhooks == nil {
		return nil, ErrWebhookStoreNotConfigured
	}

	if req.URL == nil && req.ChannelPattern == nil && req.MaxRetries == nil && req.Status == nil {
		return nil, fmt.Errorf("%w: at least one field must be provided", ErrWebhookInvalidInput)
	}

	if req.URL != nil {
		if err := validateWebhookURL(*req.URL, s.config.WebhookAllowHTTP); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrWebhookInvalidInput, err)
		}
	}
	if req.MaxRetries != nil && (*req.MaxRetries < 1 || *req.MaxRetries > maxWebhookMaxRetries) {
		return nil, fmt.Errorf("%w: max_retries must be between 1 and %d, got %d", ErrWebhookInvalidInput, maxWebhookMaxRetries, *req.MaxRetries)
	}
	if req.Status != nil {
		switch *req.Status {
		case types.WebhookStatusEnabled, types.WebhookStatusSuspended:
			// valid tenant-settable transitions
		default:
			return nil, fmt.Errorf("%w: status must be %q or %q, got %q",
				ErrWebhookInvalidInput, types.WebhookStatusEnabled, types.WebhookStatusSuspended, *req.Status)
		}
	}

	w, err := s.webhooks.Update(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("update webhook: %w", err)
	}
	if s.config.InvalidationPublisher != nil {
		s.config.InvalidationPublisher.Publish(req.TenantID)
	}
	s.auditLog(ctx, req.TenantID, ActionUpdateWebhook, Metadata{"webhook_id": req.ID})
	return w, nil
}

// DeleteWebhook removes a webhook.
func (s *Service) DeleteWebhook(ctx context.Context, id, tenantID string) error {
	if s.webhooks == nil {
		return ErrWebhookStoreNotConfigured
	}
	if err := s.webhooks.Delete(ctx, id, tenantID); err != nil {
		return fmt.Errorf("delete webhook: %w", err)
	}
	if s.config.InvalidationPublisher != nil {
		s.config.InvalidationPublisher.Publish(tenantID)
	}
	s.auditLog(ctx, tenantID, ActionDeleteWebhook, Metadata{"webhook_id": id})
	return nil
}

// validateWebhookURL checks that the URL is valid, meets the https requirement,
// and does not target private/loopback/link-local addresses (SSRF protection).
func validateWebhookURL(rawURL string, allowHTTP bool) error {
	if rawURL == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Host == "" {
		return errors.New("url must include a non-empty host")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "https" && (scheme != "http" || !allowHTTP) {
		return fmt.Errorf("webhook url must use https (got %q)", u.Scheme)
	}
	// u.Hostname() strips brackets from IPv6 literals and strips the port,
	// so net.ParseIP works correctly for both "[::1]" and "192.168.1.1:8080".
	if isPrivateHost(u.Hostname()) {
		return errors.New("webhook url must not target private, loopback, or link-local addresses")
	}
	return nil
}

// isPrivateHost returns true when host is an IP address that falls in a private,
// loopback, or link-local range. Expects the bare hostname with no brackets or
// port (pass url.URL.Hostname(), not url.URL.Host). Domain names are allowed at
// registration time; dial-time SSRF protection is the webhook-worker's responsibility.
func isPrivateHost(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false // domain name — checked at dial time by the worker
	}
	for _, block := range privateBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

// generateWebhookID creates a unique webhook ID: "wh_" + 13-char lowercase base32.
// 8 random bytes → 13 base32 chars with no padding, consistent with admin_key.go.
func generateWebhookID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate webhook ID: %w", err)
	}
	encoded := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
	return "wh_" + encoded, nil
}
