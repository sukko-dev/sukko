package publisher

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// RoutingRule maps a channel pattern to its ingress topic suffix — the topic
// an external producer writes to so the platform consumes and delivers the
// record (ADR-0018; egress topics are irrelevant to the test publisher).
// Pattern supports routing semantics: ** = any segments, * = single segment.
type RoutingRule struct {
	Pattern      string
	IngressTopic string
}

// TopicResolver resolves channel names to fully qualified Kafka topic names
// using tenant routing rules and the platform topic naming convention.
// Cached per test run — routing rules are fetched once at setup.
type TopicResolver struct {
	namespace string
	tenantID  string
	rules     []RoutingRule
}

// NewTopicResolver creates a resolver with the given namespace, tenant, and rules.
func NewTopicResolver(namespace, tenantID string, rules []RoutingRule) *TopicResolver {
	return &TopicResolver{
		namespace: namespace,
		tenantID:  tenantID,
		rules:     rules,
	}
}

// Resolve maps a channel name to a fully qualified Kafka topic name.
// Evaluates routing rules in order (first match wins) and builds the topic
// using kafka.BuildTopicName(namespace, tenantID, topicSuffix).
// Returns the matched rule's ingress topic — the only topic the platform
// consumes for the channel (ADR-0018).
func (r *TopicResolver) Resolve(channel string) (string, error) {
	if len(r.rules) == 0 {
		return "", errors.New("topic resolver: no routing rules configured")
	}

	for _, rule := range r.rules {
		matched, err := routing.MatchRoutingPattern(rule.Pattern, channel)
		if err != nil || !matched {
			continue
		}
		if rule.IngressTopic == "" {
			continue
		}
		return kafka.BuildTopicName(r.namespace, r.tenantID, rule.IngressTopic), nil
	}

	return "", fmt.Errorf("topic resolver: no routing rule matches channel %q", channel)
}

// ParseRoutingRules converts the provisioning API routing rules response
// into RoutingRule structs. Expects the paginated wrapper format: {"items": [...]}.
func ParseRoutingRules(rulesJSON []byte) ([]RoutingRule, error) {
	type ruleItem struct {
		Pattern      string `json:"pattern"`
		IngressTopic string `json:"ingress_topic"`
		Priority     int    `json:"priority"`
	}
	type wrapper struct {
		Items []ruleItem `json:"items"`
	}

	var w wrapper
	if err := json.Unmarshal(rulesJSON, &w); err != nil {
		return nil, fmt.Errorf("parse routing rules: %w", err)
	}

	rules := make([]RoutingRule, len(w.Items))
	for i, r := range w.Items {
		rules[i] = RoutingRule{Pattern: r.Pattern, IngressTopic: r.IngressTopic}
	}
	return rules, nil
}
