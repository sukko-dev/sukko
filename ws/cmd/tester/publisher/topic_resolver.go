package publisher

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// RoutingRule maps a channel pattern to one or more Kafka topic suffixes.
// Pattern supports routing semantics: ** = any segments, * = single segment.
type RoutingRule struct {
	Pattern string
	Topics  []string
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
// Returns the first topic suffix in the matched rule (fan-out not needed in test publisher).
func (r *TopicResolver) Resolve(channel string) (string, error) {
	if len(r.rules) == 0 {
		return "", errors.New("topic resolver: no routing rules configured")
	}

	for _, rule := range r.rules {
		matched, err := routing.MatchRoutingPattern(rule.Pattern, channel)
		if err != nil || !matched {
			continue
		}
		if len(rule.Topics) == 0 {
			continue
		}
		return kafka.BuildTopicName(r.namespace, r.tenantID, rule.Topics[0]), nil
	}

	return "", fmt.Errorf("topic resolver: no routing rule matches channel %q", channel)
}

// ParseRoutingRules converts the provisioning API routing rules response
// into RoutingRule structs. Expects the paginated wrapper format: {"items": [...]}.
func ParseRoutingRules(rulesJSON []byte) ([]RoutingRule, error) {
	type ruleItem struct {
		Pattern  string   `json:"pattern"`
		Topics   []string `json:"topics"`
		Priority int      `json:"priority"`
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
		rules[i] = RoutingRule{Pattern: r.Pattern, Topics: r.Topics}
	}
	return rules, nil
}
