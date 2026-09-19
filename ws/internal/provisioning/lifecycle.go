package provisioning

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// LifecycleManager handles background tenant lifecycle operations.
// It periodically processes tenants that are past their deprovisioning deadline.
type LifecycleManager struct {
	service         *Service
	interval        time.Duration
	deletionTimeout time.Duration
	logger          zerolog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// LifecycleManagerConfig configures the lifecycle manager.
type LifecycleManagerConfig struct {
	Service         *Service
	Interval        time.Duration // How often to check for tenants to delete
	DeletionTimeout time.Duration // Timeout for each deletion processing cycle
	Logger          zerolog.Logger
}

// NewLifecycleManager creates a new lifecycle manager.
// Returns an error if Service is nil or durations are non-positive.
func NewLifecycleManager(cfg LifecycleManagerConfig) (*LifecycleManager, error) {
	if cfg.Service == nil {
		return nil, errors.New("lifecycle manager: service is required")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("lifecycle manager: interval must be positive")
	}
	if cfg.DeletionTimeout <= 0 {
		return nil, errors.New("lifecycle manager: deletion timeout must be positive")
	}

	return &LifecycleManager{
		service:         cfg.Service,
		interval:        cfg.Interval,
		deletionTimeout: cfg.DeletionTimeout,
		logger:          cfg.Logger.With().Str("component", "lifecycle_manager").Logger(),
	}, nil
}

// Start begins the background lifecycle processing.
func (lm *LifecycleManager) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	lm.cancel = cancel

	lm.wg.Go(func() {
		lm.run(ctx)
	})
	lm.logger.Info().
		Dur("interval", lm.interval).
		Msg("Lifecycle manager started")
}

// Stop gracefully stops the lifecycle manager.
func (lm *LifecycleManager) Stop() {
	lm.cancel()
	lm.wg.Wait()
	lm.logger.Info().Msg("Lifecycle manager stopped")
}

// run is the main loop that periodically processes tenant deletions.
func (lm *LifecycleManager) run(ctx context.Context) {
	defer logging.RecoverPanic(lm.logger, "lifecycle_manager", nil)

	// Run immediately on start, then on interval
	lm.processDeletions(ctx)

	ticker := time.NewTicker(lm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.processDeletions(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// processDeletions finds and deletes tenants past their deprovisioning deadline.
func (lm *LifecycleManager) processDeletions(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, lm.deletionTimeout)
	defer cancel()

	// Get tenants ready for deletion
	tenants, err := lm.service.tenants.GetTenantsForDeletion(ctx)
	if err != nil {
		lm.logger.Error().Err(err).Msg("Failed to get tenants for deletion")
		return
	}

	if len(tenants) == 0 {
		lm.logger.Debug().Msg("No tenants pending deletion")
		return
	}

	lm.logger.Info().
		Int("count", len(tenants)).
		Msg("Processing tenants for deletion")

	for _, tenant := range tenants {
		if err := lm.deleteTenant(ctx, tenant); err != nil {
			lm.logger.Error().
				Err(err).
				Str(logging.LogKeyTenantUUID, tenant.ID).
				Msg("Failed to delete tenant")
			continue
		}

		lm.logger.Info().
			Str(logging.LogKeyTenantUUID, tenant.ID).
			Str("tenant_name", tenant.Name).
			Msg("Tenant deleted successfully")
	}
}

// deleteTenant performs the full deletion of a tenant.
func (lm *LifecycleManager) deleteTenant(ctx context.Context, tenant *Tenant) error {
	tenantID := tenant.ID     // UUID — used for all store operations (WHERE id = $1)
	tenantSlug := tenant.Slug // slug — used for all Kafka/ACL operations

	// 1. Delete Kafka topics derived from routing rules (nil-guarded: store is optional)
	if lm.service.routingRules != nil {
		rules, err := lm.service.routingRules.GetAll(ctx, tenantID)
		if err != nil {
			lm.logger.Warn().
				Err(err).
				Str(logging.LogKeyTenantSlug, tenantSlug).
				Msg("Failed to get routing rules for topic deletion, continuing anyway")
		} else {
			seen := make(map[string]struct{})
			for _, rule := range rules {
				for _, suffix := range rule.AllTopicSuffixes() {
					if _, ok := seen[suffix]; ok {
						continue
					}
					seen[suffix] = struct{}{}
					topicName := kafka.BuildTopicName(lm.service.config.TopicNamespace, tenantSlug, suffix)
					if err := lm.service.kafka.DeleteTopic(ctx, topicName); err != nil {
						lm.logger.Warn().
							Err(err).
							Str("topic", topicName).
							Msg("Failed to delete Kafka topic, continuing anyway")
					}
				}
			}
		}

		// Delete routing rules from database
		if err := lm.service.routingRules.DeleteAll(ctx, tenantID); err != nil {
			lm.logger.Warn().
				Err(err).
				Str(logging.LogKeyTenantSlug, tenantSlug).
				Msg("Failed to delete routing rules, continuing anyway")
		}
	}

	// 2. Delete Kafka ACLs for the tenant
	// Delete topic ACLs
	topicACL := ACLBinding{
		Principal:    FormatPrincipal(tenantSlug),
		ResourceType: ACLResourceTopic,
		ResourceName: lm.service.config.TopicNamespace + "." + tenantSlug,
		PatternType:  ACLPatternPrefixed,
		Operation:    ACLOpAll,
		Permission:   ACLPermissionAllow,
	}
	if err := lm.service.kafka.DeleteACL(ctx, topicACL); err != nil {
		lm.logger.Warn().
			Err(err).
			Str(logging.LogKeyTenantSlug, tenantSlug).
			Msg("Failed to delete topic ACL, continuing anyway")
	}

	// Delete consumer group ACLs
	groupACL := ACLBinding{
		Principal:    FormatPrincipal(tenantSlug),
		ResourceType: ACLResourceGroup,
		ResourceName: tenantSlug,
		PatternType:  ACLPatternPrefixed,
		Operation:    ACLOpAll,
		Permission:   ACLPermissionAllow,
	}
	if err := lm.service.kafka.DeleteACL(ctx, groupACL); err != nil {
		lm.logger.Warn().
			Err(err).
			Str(logging.LogKeyTenantSlug, tenantSlug).
			Msg("Failed to delete group ACL, continuing anyway")
	}

	// 3. Log the deletion
	if err := lm.service.audit.Log(ctx, &AuditEntry{
		TenantID:  tenantID,
		Action:    ActionDeleteTenant,
		Actor:     "system",
		ActorType: ActorTypeSystem,
		Details: map[string]any{
			"reason": "deprovisioning_deadline_reached",
		},
	}); err != nil {
		lm.logger.Warn().
			Err(err).
			Str(logging.LogKeyTenantSlug, tenantSlug).
			Msg("Failed to log deletion audit entry")
	}

	// 4. Update tenant status to deleted
	if err := lm.service.tenants.UpdateStatus(ctx, tenantID, StatusDeleted); err != nil {
		return fmt.Errorf("update tenant status to deleted: %w", err)
	}

	return nil
}
