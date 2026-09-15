// Package kafka provides Kafka/Redpanda integration for the WebSocket server.
// This file contains the shared BuildTopicName function for constructing topic names.
package kafka

import (
	"fmt"
)

// RetentionMsConfigKey is the Kafka topic configuration key for retention in milliseconds.
// Used when creating or reconfiguring topics via the Kafka admin API.
// Defined here (not in each consumer) to prevent magic-string duplication (§I).
const RetentionMsConfigKey = "retention.ms"

// BuildTopicName constructs a Kafka topic name from components.
// This is the single source of truth for topic name format across the codebase.
//
// Format: {namespace}.{tenantID}.{suffix}
//
// Example:
//
//	BuildTopicName("prod", "sukko", "trade") -> "prod.sukko.trade"
//	BuildTopicName("dev", "acme", "analytics") -> "dev.acme.analytics"
//
// Components:
//   - namespace: the explicit KAFKA_TOPIC_NAMESPACE value (e.g., "local", "dev", "stag", "prod")
//   - tenantID: Tenant identifier (e.g., "sukko", "acme")
//   - suffix: Topic suffix from a tenant's routing rules (e.g., "trade", "liquidity", "metadata")
//
// This function is used by:
//   - Producer (producer.go): Building topic names when publishing messages
//   - TenantRegistry (topic_registry.go): Building topic names when querying suffixes
//   - Provisioning Service (service.go): Building topic names when creating Kafka topics
//
// The namespace is NOT stored in the database - only the suffix is stored.
// This allows the same database to be used across environments, with the
// namespace determined at runtime from configuration.
func BuildTopicName(namespace, tenantID, suffix string) string {
	return fmt.Sprintf("%s.%s.%s", namespace, tenantID, suffix)
}
