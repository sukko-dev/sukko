// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"context"
	"fmt"
	"time"

	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// TenantIsolator provides unified tenant isolation for channels and topics.
// It combines channel and topic isolation with audit logging capabilities.
type TenantIsolator struct {
	config        TenantIsolationConfig
	topicIsolator *TopicIsolator
	auditLogger   AuditLogger
	metrics       pkgmetrics.AccessDenialMetrics
}

// TenantIsolationConfig configures tenant isolation behavior.
// Always fail-secure: requests without proper tenant context are rejected.
type TenantIsolationConfig struct {
	// Environment is the deployment environment (e.g., "local", "dev", "prod").
	// Required when no custom TopicIsolator is provided via WithTopicIsolator.
	Environment string `yaml:"environment" json:"environment"`

	// SharedChannelPatterns are channels accessible by all tenants.
	SharedChannelPatterns []string `yaml:"shared_channel_patterns" json:"shared_channel_patterns"`

	// SharedTopicPatterns are topics accessible by all tenants.
	SharedTopicPatterns []string `yaml:"shared_topic_patterns" json:"shared_topic_patterns"`

	// AuditDenials logs all denied access attempts.
	AuditDenials bool `yaml:"audit_denials" json:"audit_denials"`

	// AuditAllAccess logs all access attempts (for debugging).
	AuditAllAccess bool `yaml:"audit_all_access" json:"audit_all_access"`
}

// DefaultTenantIsolationConfig returns sensible defaults.
// Always fail-secure by design - no "non-strict" mode.
func DefaultTenantIsolationConfig() TenantIsolationConfig {
	return TenantIsolationConfig{
		Environment:           "local",
		SharedChannelPatterns: []string{"system.*"},
		SharedTopicPatterns:   []string{},
		AuditDenials:          true,
		AuditAllAccess:        false,
	}
}

// TenantIsolatorOption configures the TenantIsolator.
type TenantIsolatorOption func(*TenantIsolator)

// WithAuditLogger sets a custom audit logger.
func WithAuditLogger(logger AuditLogger) TenantIsolatorOption {
	return func(t *TenantIsolator) {
		t.auditLogger = logger
	}
}

// WithTopicIsolator sets a custom topic isolator.
// If isolator is nil, the default topic isolator creation proceeds (requires Environment).
func WithTopicIsolator(isolator *TopicIsolator) TenantIsolatorOption {
	return func(t *TenantIsolator) {
		if isolator != nil {
			t.topicIsolator = isolator
		}
	}
}

// WithAccessDenialMetrics sets a metrics callback for access denials.
func WithAccessDenialMetrics(metrics pkgmetrics.AccessDenialMetrics) TenantIsolatorOption {
	return func(t *TenantIsolator) {
		t.metrics = metrics
	}
}

// NewTenantIsolator creates a tenant isolator with the given configuration.
// Returns an error if no custom TopicIsolator is provided and Environment is empty.
func NewTenantIsolator(config TenantIsolationConfig, opts ...TenantIsolatorOption) (*TenantIsolator, error) {
	t := &TenantIsolator{
		config:      config,
		auditLogger: &noopAuditLogger{},
		metrics:     pkgmetrics.NoopAccessDenialMetrics{},
	}

	// Apply options
	for _, opt := range opts {
		opt(t)
	}

	// Create default topic isolator if not provided
	if t.topicIsolator == nil {
		topicIso, err := NewTopicIsolator(TopicIsolationConfig{
			Environment:         config.Environment,
			TenantPosition:      1,
			Separator:           ".",
			SharedTopicPatterns: config.SharedTopicPatterns,
		})
		if err != nil {
			return nil, fmt.Errorf("create default topic isolator: %w", err)
		}
		t.topicIsolator = topicIso
	}

	return t, nil
}

// AccessAction represents the type of access being requested.
type AccessAction string

// AccessAction constants for tenant isolation checks.
const (
	ActionSubscribe AccessAction = "subscribe"
	ActionPublish   AccessAction = "publish"
	ActionConsume   AccessAction = "consume"
	ActionPresence  AccessAction = "presence"
)

// AccessCheckResult contains the result of an access check.
type AccessCheckResult struct {
	// Allowed indicates whether access is permitted.
	Allowed bool

	// Reason explains why access was allowed or denied.
	Reason string

	// ResourceType indicates what type of resource was checked.
	ResourceType string // "channel" or "topic"

	// Resource is the resource that was checked.
	Resource string

	// ClaimsTenant is the tenant from JWT claims.
	ClaimsTenant string

	// ResourceTenant is the tenant extracted from the resource.
	ResourceTenant string

	// IsShared indicates the resource is shared across tenants.
	IsShared bool

	// Duration is how long the check took.
	Duration time.Duration
}

// CheckChannelAccess verifies that claims allow access to a channel.
// The channel should be in internal format (tenant-prefixed).
func (t *TenantIsolator) CheckChannelAccess(ctx context.Context, claims *Claims, channel string, action AccessAction) *AccessCheckResult {
	start := time.Now()

	result := &AccessCheckResult{
		ResourceType: "channel",
		Resource:     channel,
	}

	// Extract claims tenant
	if claims != nil {
		result.ClaimsTenant = claims.TenantID
	}

	// Check if auth is disabled (no claims or no tenant)
	if claims == nil || claims.TenantID == "" {
		result.Allowed = true
		result.Reason = "no tenant in claims (API-key-only connection)"
		result.Duration = time.Since(start)
		t.logAccess(ctx, result, action)
		return result
	}

	// Check if shared channel
	if IsSharedChannel(channel, t.config.SharedChannelPatterns) {
		result.Allowed = true
		result.IsShared = true
		result.Reason = "shared channel"
		result.Duration = time.Since(start)
		t.logAccess(ctx, result, action)
		return result
	}

	// Extract tenant from channel
	channelTenant := extractChannelTenant(channel, ".")
	result.ResourceTenant = channelTenant

	// No tenant in channel = shared
	if channelTenant == "" {
		result.Allowed = true
		result.Reason = "no tenant in channel (shared)"
		result.Duration = time.Since(start)
		t.logAccess(ctx, result, action)
		return result
	}

	// Check tenant match
	if channelTenant == claims.TenantID {
		result.Allowed = true
		result.Reason = "tenant match"
		result.Duration = time.Since(start)
		t.logAccess(ctx, result, action)
		return result
	}

	// Denied
	result.Allowed = false
	result.Reason = fmt.Sprintf("tenant mismatch: claims=%s, channel=%s",
		claims.TenantID, channelTenant)
	result.Duration = time.Since(start)
	t.logAccess(ctx, result, action)
	return result
}

// CheckTopicAccess verifies that claims allow access to a Kafka topic.
func (t *TenantIsolator) CheckTopicAccess(ctx context.Context, claims *Claims, topic string, action AccessAction) *AccessCheckResult {
	start := time.Now()

	// Convert action to TopicAction
	var topicAction TopicAction
	switch action {
	case ActionPublish:
		topicAction = TopicActionPublish
	case ActionSubscribe, ActionConsume, ActionPresence:
		topicAction = TopicActionConsume
	}

	// Delegate to topic isolator
	topicResult := t.topicIsolator.CheckTopicAccess(claims, topic, topicAction)

	result := &AccessCheckResult{
		Allowed:        topicResult.Allowed,
		Reason:         topicResult.Reason,
		ResourceType:   "topic",
		Resource:       topic,
		ClaimsTenant:   topicResult.ClaimsTenant,
		ResourceTenant: topicResult.TopicTenant,
		IsShared:       topicResult.IsSharedTopic,
		Duration:       time.Since(start),
	}

	t.logAccess(ctx, result, action)
	return result
}

// logAccess logs the access check result if auditing is enabled.
func (t *TenantIsolator) logAccess(ctx context.Context, result *AccessCheckResult, action AccessAction) {
	if t.auditLogger == nil {
		return
	}

	// Always log denials if enabled
	if !result.Allowed && t.config.AuditDenials {
		t.auditLogger.LogDenied(ctx, &AuditEntry{
			Action:         string(action),
			ResourceType:   result.ResourceType,
			Resource:       result.Resource,
			ClaimsTenant:   result.ClaimsTenant,
			ResourceTenant: result.ResourceTenant,
			Reason:         result.Reason,
			Duration:       result.Duration,
		})
		return
	}

	// Log all access if enabled (including allowed)
	if t.config.AuditAllAccess {
		t.auditLogger.LogAllowed(ctx, &AuditEntry{
			Action:         string(action),
			ResourceType:   result.ResourceType,
			Resource:       result.Resource,
			ClaimsTenant:   result.ClaimsTenant,
			ResourceTenant: result.ResourceTenant,
			Reason:         result.Reason,
			IsShared:       result.IsShared,
			Duration:       result.Duration,
		})
	}
}

// AuditEntry represents an audit log entry.
type AuditEntry struct {
	Action         string
	ResourceType   string
	Resource       string
	ClaimsTenant   string
	ResourceTenant string
	Reason         string
	IsShared       bool
	Duration       time.Duration
}

// AuditLogger interface for audit logging.
type AuditLogger interface {
	LogDenied(ctx context.Context, entry *AuditEntry)
	LogAllowed(ctx context.Context, entry *AuditEntry)
}

// noopAuditLogger is a no-op implementation of AuditLogger.
type noopAuditLogger struct{}

func (n *noopAuditLogger) LogDenied(_ context.Context, _ *AuditEntry)  {}
func (n *noopAuditLogger) LogAllowed(_ context.Context, _ *AuditEntry) {}

// TenantContext provides tenant information for a request.
type TenantContext struct {
	TenantID string
	UserID   string
	Roles    []string
	Groups   []string
}

// ExtractTenantContext extracts tenant context from claims.
func ExtractTenantContext(claims *Claims) *TenantContext {
	if claims == nil {
		return &TenantContext{}
	}

	return &TenantContext{
		TenantID: claims.TenantID,
		UserID:   claims.Subject,
		Roles:    claims.Roles,
		Groups:   claims.Groups,
	}
}

// CanAccessResource is a convenience method for quick access checks.
// Returns true if the claims can access the resource.
func (t *TenantIsolator) CanAccessResource(claims *Claims, resource, resourceType string, action AccessAction) bool {
	ctx := context.Background()

	switch resourceType {
	case "channel":
		return t.CheckChannelAccess(ctx, claims, resource, action).Allowed
	case "topic":
		return t.CheckTopicAccess(ctx, claims, resource, action).Allowed
	default:
		return false
	}
}
