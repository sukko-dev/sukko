// Package provisioning provides tenant lifecycle management, key registration,
// and Kafka topic/ACL provisioning for multi-tenant WebSocket infrastructure.
package provisioning

import (
	"context"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/types"
)

// TenantStore handles tenant persistence operations.
type TenantStore interface {
	// Ping verifies database connectivity.
	Ping(ctx context.Context) error

	// Create creates a new tenant record.
	Create(ctx context.Context, tenant *Tenant) error

	// GetBySlug retrieves a tenant by slug.
	GetBySlug(ctx context.Context, slug string) (*Tenant, error)

	// Update updates an existing tenant record.
	Update(ctx context.Context, tenant *Tenant) error

	// UpdateSlug commits a slug rename and returns the updated tenant via RETURNING.
	// Returns ErrSlugAlreadyTaken if the unique constraint fires (23505 TOCTOU race).
	UpdateSlug(ctx context.Context, tenantUUID, newSlug string) (*Tenant, error)

	// SetRenameState sets the slug rename state with a CAS guard.
	// The UPDATE fires only when slug_rename_state IS NULL OR slug_rename_state='complete'.
	// Returns ErrCASFailed when zero rows are affected (concurrent rename in progress).
	SetRenameState(ctx context.Context, tenantUUID string, state SlugRenameState, previousSlug string) error

	// ClearRenameState unconditionally clears slug_rename_state, previous_slug, and slug_renamed_at.
	ClearRenameState(ctx context.Context, tenantUUID string) error

	// ListPendingRenames returns tenants with slug_rename_state='pending' OR
	// (slug_rename_state='complete' AND previous_slug IS NOT NULL).
	// Used by the startup scan to detect saga residue and re-emit config events.
	ListPendingRenames(ctx context.Context) ([]*Tenant, error)

	// List returns tenants matching the given options.
	List(ctx context.Context, opts ListOptions) ([]*Tenant, int, error)

	// UpdateStatus updates a tenant's status.
	UpdateStatus(ctx context.Context, tenantID string, status TenantStatus) error

	// SetDeprovisionAt sets the deprovision deadline for a tenant.
	SetDeprovisionAt(ctx context.Context, tenantID string, deprovisionAt *Time) error

	// GetTenantsForDeletion returns tenants past their deprovision deadline.
	GetTenantsForDeletion(ctx context.Context) ([]*Tenant, error)

	// Count returns the number of active (non-deleted) tenants.
	// Used by edition limit enforcement to check tenant count before creation.
	Count(ctx context.Context) (int, error)

	// CountActive returns the number of strictly active (status='active') tenants.
	// Used by the provisioning_active_tenants Prometheus gauge at startup.
	// Note: Count() includes suspended tenants; CountActive() excludes them.
	CountActive(ctx context.Context) (int64, error)
}

// KeyStore handles public key persistence operations.
type KeyStore interface {
	// Create creates a new key record.
	Create(ctx context.Context, key *TenantKey) error

	// Get retrieves a key by ID.
	Get(ctx context.Context, keyID string) (*TenantKey, error)

	// ListByTenant returns keys for a tenant with pagination.
	ListByTenant(ctx context.Context, tenantID string, opts ListOptions) ([]*TenantKey, int, error)

	// Revoke revokes a key by setting its revoked_at timestamp.
	Revoke(ctx context.Context, keyID string) error

	// RevokeAllForTenant revokes all active keys for a tenant.
	RevokeAllForTenant(ctx context.Context, tenantID string) error

	// GetActiveKeys returns all active, non-expired, non-revoked keys.
	// Used by WS Gateway to refresh its key cache.
	GetActiveKeys(ctx context.Context) ([]*TenantKey, error)
}

// APIKeyStore handles API key persistence operations.
type APIKeyStore interface {
	// Create creates a new API key record.
	Create(ctx context.Context, key *APIKey) error

	// Get retrieves an API key by key ID.
	Get(ctx context.Context, keyID string) (*APIKey, error)

	// ListByTenant returns API keys for a tenant with pagination.
	ListByTenant(ctx context.Context, tenantID string, opts ListOptions) ([]*APIKey, int, error)

	// Revoke revokes an API key by setting its revoked_at timestamp.
	Revoke(ctx context.Context, keyID string) error

	// GetActiveAPIKeys returns all active, non-revoked API keys.
	// Used by the gateway to populate its in-memory lookup map.
	GetActiveAPIKeys(ctx context.Context) ([]*APIKey, error)
}

// RoutingRulesStore handles per-tenant topic routing rules persistence.
// Rules are stored per-row with pattern, topics[], and priority.
type RoutingRulesStore interface {
	// List returns paginated routing rules for a tenant ordered by priority ASC.
	List(ctx context.Context, tenantID string, limit, offset int) ([]TopicRoutingRule, int, error)

	// Add inserts a single routing rule. Returns ErrDuplicatePriority on conflict.
	Add(ctx context.Context, tenantID string, rule TopicRoutingRule) error

	// Replace atomically replaces all routing rules for a tenant (DELETE + batch INSERT).
	Replace(ctx context.Context, tenantID string, rules []TopicRoutingRule) error

	// DeleteAll deletes all routing rules for a tenant.
	DeleteAll(ctx context.Context, tenantID string) error

	// GetAll returns all routing rules for a tenant (used for WatchTenantConfig push).
	// NormalizePattern is applied on read; invalid patterns are skipped and counted.
	GetAll(ctx context.Context, tenantID string) ([]TopicRoutingRule, error)
}

// TopicStore is the durable source of truth for a tenant's non-default
// provisioned topics (ADR-0006 Phase 2). It replaces the volatile in-memory
// KafkaAdmin map for TOPIC_NOT_PROVISIONED validation, so a provisioning restart
// no longer loses the record of which topics a tenant has provisioned. The
// per-tenant 'default' topic is deterministic and validated implicitly, so it
// is not stored here. tenantID is the tenant UUID (FK → tenants.id), suffix is
// the topic suffix (never the namespaced name, never 'default'/'dead-letter').
type TopicStore interface {
	// Exists reports whether a non-default topic suffix is provisioned for a tenant.
	Exists(ctx context.Context, tenantID, suffix string) (bool, error)

	// Create records a provisioned topic and returns its created_at. Returns
	// ErrTopicAlreadyExists when the (tenant, suffix) already exists.
	Create(ctx context.Context, tenantID, suffix string) (time.Time, error)

	// List returns a tenant's provisioned topics ordered by suffix.
	List(ctx context.Context, tenantID string) ([]Topic, error)

	// Delete removes a provisioned topic. Returns ErrTopicNotFound when absent.
	Delete(ctx context.Context, tenantID, suffix string) error

	// Count returns the number of provisioned topics for a tenant (the MaxTopics quota denominator).
	Count(ctx context.Context, tenantID string) (int, error)
}

// Topic is a durable provisioned topic: a non-default, non-DLQ topic suffix a
// tenant may reference from routing rules (ADR-0006 Phase 2). The deterministic
// 'default' topic is not stored and is surfaced separately by the topics API.
type Topic struct {
	Suffix    string    `json:"suffix"`
	CreatedAt time.Time `json:"created_at"`
}

// QuotaStore handles tenant quota operations.
type QuotaStore interface {
	// Get retrieves quotas for a tenant.
	Get(ctx context.Context, tenantID string) (*TenantQuota, error)

	// Create creates quota record for a tenant.
	Create(ctx context.Context, quota *TenantQuota) error

	// Update updates quota record for a tenant.
	Update(ctx context.Context, quota *TenantQuota) error
}

// AuditStore handles audit log operations.
type AuditStore interface {
	// Log records an audit entry.
	Log(ctx context.Context, entry *AuditEntry) error

	// ListByTenant returns audit entries for a tenant.
	ListByTenant(ctx context.Context, tenantID string, opts ListOptions) ([]*AuditEntry, int, error)
}

// ChannelRulesStore handles per-tenant channel access rules persistence.
// Used for mapping JWT groups to allowed channel patterns.
type ChannelRulesStore interface {
	// Create creates channel rules for a tenant.
	Create(ctx context.Context, tenantID string, rules *types.ChannelRules) error

	// Get retrieves channel rules with metadata by tenant ID.
	// Returns types.ErrChannelRulesNotFound if not found.
	Get(ctx context.Context, tenantID string) (*types.TenantChannelRules, error)

	// GetRules retrieves just the channel rules (not metadata) by tenant ID.
	// Returns types.ErrChannelRulesNotFound if not found.
	GetRules(ctx context.Context, tenantID string) (*types.ChannelRules, error)

	// Update updates channel rules for a tenant (upsert).
	Update(ctx context.Context, tenantID string, rules *types.ChannelRules) error

	// Delete deletes channel rules for a tenant.
	Delete(ctx context.Context, tenantID string) error

	// List returns all channel rules (used by gateway to build cache).
	List(ctx context.Context) ([]*types.TenantChannelRules, error)
}

// LicenseStateStore handles encrypted license key persistence (single-row table).
type LicenseStateStore interface {
	// Upsert encrypts and stores the license key. Creates or updates the single row.
	Upsert(ctx context.Context, licenseKey, edition, org string, expiresAt *time.Time) error

	// Load retrieves and decrypts the stored license key.
	// Returns empty string if no license is stored (first deploy).
	Load(ctx context.Context) (string, error)
}

// KafkaAdmin handles Redpanda/Kafka topic and ACL management.
type KafkaAdmin interface {
	// CreateTopic creates a new Kafka topic.
	// The production adapter may return kerr.TopicAlreadyExists when the topic already exists;
	// NoopKafkaAdmin never returns it. Callers that need idempotent creation must swallow
	// kerr.TopicAlreadyExists explicitly.
	CreateTopic(ctx context.Context, name string, partitions int, replicationFactor int16, config map[string]string) error

	// DeleteTopic deletes a Kafka topic.
	DeleteTopic(ctx context.Context, name string) error

	// SetTopicConfig updates topic configuration.
	SetTopicConfig(ctx context.Context, name string, config map[string]string) error

	// CreateACL creates an ACL for a tenant.
	CreateACL(ctx context.Context, acl ACLBinding) error

	// DeleteACL deletes an ACL.
	DeleteACL(ctx context.Context, acl ACLBinding) error

	// SetQuota sets resource quotas for a tenant principal.
	SetQuota(ctx context.Context, tenantID string, quota QuotaConfig) error

	// CreateTopicACLs creates all topic and consumer-group ACL rules for the given slug.
	CreateTopicACLs(ctx context.Context, slug, namespace string) error

	// DeleteTopicACLs removes all topic and consumer-group ACL rules for the given slug.
	DeleteTopicACLs(ctx context.Context, slug, namespace string) error

	// DeleteQuota removes resource quotas for the given slug principal.
	DeleteQuota(ctx context.Context, slug, namespace string) error
}

// QuotaEnforcer checks resource limits before provisioning.
type QuotaEnforcer interface {
	// CheckTopicQuota checks if creating additional topics would exceed quota.
	CheckTopicQuota(ctx context.Context, tenantID string, additionalTopics int) error

	// CheckPartitionQuota checks if creating additional partitions would exceed quota.
	CheckPartitionQuota(ctx context.Context, tenantID string, additionalPartitions int) error
}

// ListOptions defines pagination and filtering for list operations.
type ListOptions struct {
	// Limit is the maximum number of results to return.
	Limit int

	// Offset is the number of results to skip.
	Offset int

	// Status filters by tenant status (optional).
	Status *TenantStatus
}

// QuotaConfig defines Kafka quotas for a tenant.
type QuotaConfig struct {
	// ProducerByteRate is the maximum bytes/second for producers.
	ProducerByteRate int64

	// ConsumerByteRate is the maximum bytes/second for consumers.
	ConsumerByteRate int64
}

// ACLBinding defines an ACL rule.
type ACLBinding struct {
	// Principal is the Kafka principal (e.g., "User:acme").
	// MUST use "User:" prefix per Kafka protocol - use FormatPrincipal() helper.
	Principal string

	// ResourceType is the resource type (e.g., "TOPIC", "GROUP", "CLUSTER").
	ResourceType string

	// ResourceName is the resource name or pattern (e.g., "main.acme.*").
	ResourceName string

	// PatternType is the pattern type (e.g., "PREFIXED", "LITERAL").
	PatternType string

	// Operation is the allowed operation (e.g., "ALL", "READ", "WRITE").
	Operation string

	// Permission is ALLOW or DENY.
	Permission string
}

// ACL constants for resource types.
const (
	ACLResourceTopic           = "TOPIC"
	ACLResourceGroup           = "GROUP"
	ACLResourceCluster         = "CLUSTER"
	ACLResourceTransactionalID = "TRANSACTIONAL_ID"
)

// ACL constants for pattern types.
const (
	ACLPatternLiteral  = "LITERAL"
	ACLPatternPrefixed = "PREFIXED"
)

// ACL constants for operations.
const (
	ACLOpAll             = "ALL"
	ACLOpRead            = "READ"
	ACLOpWrite           = "WRITE"
	ACLOpCreate          = "CREATE"
	ACLOpDelete          = "DELETE"
	ACLOpAlter           = "ALTER"
	ACLOpDescribe        = "DESCRIBE"
	ACLOpClusterAction   = "CLUSTER_ACTION"
	ACLOpDescribeConfigs = "DESCRIBE_CONFIGS"
	ACLOpAlterConfigs    = "ALTER_CONFIGS"
	ACLOpIdempotentWrite = "IDEMPOTENT_WRITE"
)

// ACL constants for permissions.
const (
	ACLPermissionAllow = "ALLOW"
	ACLPermissionDeny  = "DENY"
)

// FormatPrincipal formats a tenant ID as a Kafka principal.
// Kafka ACL principals MUST use the format "User:{username}" for SASL/SCRAM auth.
// This is a Kafka protocol requirement, not optional.
func FormatPrincipal(tenantID string) string {
	return "User:" + tenantID
}

// ParsePrincipal extracts the tenant ID from a Kafka principal.
// Returns empty string if the principal is not in "User:{id}" format.
func ParsePrincipal(principal string) string {
	const prefix = "User:"
	if len(principal) > len(prefix) && principal[:len(prefix)] == prefix {
		return principal[len(prefix):]
	}
	return ""
}

// ValidatePrincipal checks if a principal is in valid Kafka format.
func ValidatePrincipal(principal string) bool {
	return len(principal) > 5 && principal[:5] == "User:"
}

// WebhookStore handles webhook registration persistence.
type WebhookStore interface {
	// Create inserts a new webhook registration with an auto-generated ID.
	Create(ctx context.Context, w *Webhook) error

	// GetByID retrieves a webhook by ID, scoped to the given tenantID.
	// Returns ErrWebhookNotFound if not found or wrong tenant.
	GetByID(ctx context.Context, id, tenantID string) (*Webhook, error)

	// List returns webhooks for a tenant with pagination.
	List(ctx context.Context, tenantID string, opts ListOptions) ([]*Webhook, int, error)

	// Update applies partial update from UpdateWebhookRequest.
	// Returns ErrWebhookNotFound if not found or wrong tenant.
	Update(ctx context.Context, req UpdateWebhookRequest) (*Webhook, error)

	// Delete removes a webhook, scoped to tenantID.
	// Returns ErrWebhookNotFound if not found or wrong tenant.
	Delete(ctx context.Context, id, tenantID string) error

	// CountActive returns the number of webhooks with status != 'suspended' for a tenant.
	// Used for quota enforcement.
	CountActive(ctx context.Context, tenantID string) (int, error)

	// ListTenantIDs returns all distinct tenant IDs that have at least one
	// non-suspended webhook. Used at webhook-worker startup.
	ListTenantIDs(ctx context.Context) ([]string, error)

	// ListByTenantForWorker returns webhooks for a tenant for cache hydration.
	// SecretEnc in WebhookRecord is raw AES-256-GCM ciphertext (binary, not base64) —
	// the repository decodes the base64 TEXT from the DB before setting this field.
	// The webhook-worker passes it directly to crypto.DecryptRaw(rec.SecretEnc, key).
	ListByTenantForWorker(ctx context.Context, tenantID string) ([]*WebhookRecord, error)

	// UpdateStatus transitions a webhook's status and resets retry_count.
	// retryCount is set to 0 on any →enabled transition.
	UpdateStatus(ctx context.Context, id, tenantID, status string, retryCount int) error

	// RecordDelivery inserts a delivery attempt into webhook_deliveries and
	// updates webhooks.last_delivery_at and webhooks.last_status.
	// Prunes oldest rows to keep at most 50 per webhook.
	// INSERT + prune + UPDATE MUST execute in a single transaction.
	RecordDelivery(ctx context.Context, d *WebhookDelivery) error

	// SuspendAllForDowngrade bulk-suspends all non-suspended webhooks.
	// cutoff is kept for interface compatibility and audit logging at the call site;
	// implementations MUST suspend all non-suspended webhooks regardless of creation date
	// (feature gate prevents new registrations after downgrade; defense-in-depth catches edge cases).
	// Returns the count of rows updated. Called after the grace period has elapsed.
	SuspendAllForDowngrade(ctx context.Context, cutoff time.Time) (int, error)

	// ListForDowngrade returns all non-suspended webhooks eligible for suspension.
	// cutoff is kept for interface compatibility and audit logging at the call site.
	ListForDowngrade(ctx context.Context, cutoff time.Time) ([]*Webhook, error)
}
