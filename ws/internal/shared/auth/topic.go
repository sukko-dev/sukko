// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"errors"
	"fmt"
	"strings"
)

// TopicAction represents an action on a Kafka topic.
type TopicAction string

const (
	// TopicActionPublish allows publishing to a topic.
	TopicActionPublish TopicAction = "publish"
	// TopicActionConsume allows consuming from a topic.
	TopicActionConsume TopicAction = "consume"
)

// TopicIsolator enforces tenant boundaries on Kafka/Redpanda topics.
// Topics follow the format: {environment}.{tenant_id}.{suffix}
//
// Example:
//
//	prod.acme.trade     - Production, Acme tenant, trade events
//	dev.globex.liquidity - Development, Globex tenant, liquidity events
type TopicIsolator struct {
	config TopicIsolationConfig
}

// TopicIsolationConfig configures topic isolation behavior.
type TopicIsolationConfig struct {
	// Environment is the deployment environment (dev, stag, prod).
	// Used as the first part of topic names.
	Environment string `yaml:"environment" json:"environment"`

	// TenantPosition is the position of tenant in topic name (0-indexed).
	// For format {env}.{tenant}.{suffix}, position is 1.
	TenantPosition int `yaml:"tenant_position" json:"tenant_position"`

	// Separator between topic parts (default: ".").
	Separator string `yaml:"separator" json:"separator"`

	// SharedTopicPatterns are topics accessible by all tenants.
	// These must be explicitly configured - no implicit sharing.
	// Example: ["*.system.*", "prod.shared.*"]
	SharedTopicPatterns []string `yaml:"shared_topic_patterns" json:"shared_topic_patterns"`
}

// DefaultTopicIsolationConfig returns sensible defaults.
// Always fail-secure: topics without valid tenant are rejected.
func DefaultTopicIsolationConfig() TopicIsolationConfig {
	return TopicIsolationConfig{
		Environment:         "local",
		TenantPosition:      1, // {env}.{tenant}.{suffix}
		Separator:           ".",
		SharedTopicPatterns: []string{},
	}
}

// NewTopicIsolator creates a topic isolator with the given configuration.
// Returns an error if Environment is empty — callers must provide this explicitly.
func NewTopicIsolator(config TopicIsolationConfig) (*TopicIsolator, error) {
	if config.Environment == "" {
		return nil, errors.New("TopicIsolationConfig.Environment must not be empty")
	}
	if config.Separator == "" {
		config.Separator = "."
	}
	return &TopicIsolator{config: config}, nil
}

// TopicCheckResult contains the result of a topic access check.
type TopicCheckResult struct {
	// Allowed indicates whether access is permitted.
	Allowed bool

	// TopicTenant is the tenant extracted from the topic name.
	TopicTenant string

	// ClaimsTenant is the tenant from the JWT claims.
	ClaimsTenant string

	// Reason explains why access was allowed or denied.
	Reason string

	// IsSharedTopic indicates the topic is shared across tenants.
	IsSharedTopic bool
}

// CheckTopicAccess verifies that the claims allow access to the topic.
// Returns a detailed result explaining the decision.
func (t *TopicIsolator) CheckTopicAccess(claims *Claims, topic string, _ TopicAction) *TopicCheckResult {
	result := &TopicCheckResult{
		ClaimsTenant: "",
	}

	// Extract claims tenant if available
	if claims != nil {
		result.ClaimsTenant = claims.TenantID
	}

	// No claims = API-key-only connection, allow all topics
	if claims == nil || claims.TenantID == "" {
		result.Allowed = true
		result.Reason = "no tenant in claims (API-key-only connection)"
		return result
	}

	// Check if shared topic (must be explicitly configured)
	if t.isSharedTopic(topic) {
		result.Allowed = true
		result.IsSharedTopic = true
		result.Reason = "shared topic accessible by all tenants"
		return result
	}

	// Extract tenant from topic
	topicTenant := t.ExtractTenantFromTopic(topic)
	result.TopicTenant = topicTenant

	// Fail-secure: reject topics without valid tenant segment
	// Topics must either be in SharedTopicPatterns or have a valid tenant
	if topicTenant == "" {
		result.Allowed = false
		result.Reason = "topic missing tenant segment (not in shared patterns)"
		return result
	}

	// Check tenant match
	if topicTenant == claims.TenantID {
		result.Allowed = true
		result.Reason = "tenant match"
		return result
	}

	// Denied: tenant mismatch
	result.Allowed = false
	result.Reason = fmt.Sprintf("tenant mismatch: claims=%s, topic=%s",
		claims.TenantID, topicTenant)
	return result
}

// ExtractTenantFromTopic extracts the tenant ID from a topic name.
// Returns empty string if tenant cannot be extracted.
//
// Example:
//
//	"prod.acme.trade" → "acme" (position 1)
//	"acme.trade" → "" (not enough parts for position 1)
func (t *TopicIsolator) ExtractTenantFromTopic(topic string) string {
	parts := strings.Split(topic, t.config.Separator)

	if len(parts) <= t.config.TenantPosition {
		return ""
	}

	return parts[t.config.TenantPosition]
}

// isSharedTopic checks if the topic matches any shared topic pattern.
func (t *TopicIsolator) isSharedTopic(topic string) bool {
	for _, pattern := range t.config.SharedTopicPatterns {
		if matchTopicPattern(pattern, topic, t.config.Separator) {
			return true
		}
	}
	return false
}

// matchTopicPattern checks if a topic matches a pattern.
// Supports * as wildcard for any single segment.
func matchTopicPattern(pattern, topic, separator string) bool {
	if pattern == topic {
		return true
	}

	patternParts := strings.Split(pattern, separator)
	topicParts := strings.Split(topic, separator)

	if len(patternParts) != len(topicParts) {
		return false
	}

	for i, pp := range patternParts {
		if pp == "*" {
			continue // Wildcard matches any segment
		}
		if pp != topicParts[i] {
			return false
		}
	}

	return true
}

// TopicAccessChecker provides a simplified interface for topic access checks.
// Useful when you don't need the full TopicCheckResult.
type TopicAccessChecker interface {
	CanPublish(claims *Claims, topic string) bool
	CanConsume(claims *Claims, topic string) bool
}

// CanPublish checks if claims allow publishing to the topic.
func (t *TopicIsolator) CanPublish(claims *Claims, topic string) bool {
	return t.CheckTopicAccess(claims, topic, TopicActionPublish).Allowed
}

// CanConsume checks if claims allow consuming from the topic.
func (t *TopicIsolator) CanConsume(claims *Claims, topic string) bool {
	return t.CheckTopicAccess(claims, topic, TopicActionConsume).Allowed
}
