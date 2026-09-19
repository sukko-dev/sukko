package provisioning

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// TopicRoutingRule maps a channel glob pattern to Kafka topic suffixes with two
// explicit, mutually exclusive roles (§XV):
//
//   - IngressTopic — the single topic the platform consumes; its records are
//     delivered to subscribers, replayed, and served as history. The publish ack's
//     mid names the ingress record (ADR-0018).
//   - EgressTopics — additional copies written for external consumers (audit,
//     analytics, compliance retention). Created on the broker but NEVER consumed:
//     an egress copy must never reach a subscriber.
//
// Rules are evaluated in priority order (lower = higher priority); first match wins.
type TopicRoutingRule struct {
	// Pattern is a channel glob pattern (e.g., "acme.**.trade").
	// Supports * (one segment) and ** (one or more segments).
	Pattern string `json:"pattern"`

	// IngressTopic is the Kafka topic suffix whose records are consumed and
	// delivered to subscribers when this rule matches. Exactly one per rule.
	IngressTopic string `json:"ingress_topic"`

	// EgressTopics are Kafka topic suffixes that receive additional copies of the
	// message for external consumers. Never consumed by the platform.
	EgressTopics []string `json:"egress_topics,omitempty"`

	// Priority determines evaluation order. Lower value = higher priority.
	Priority int `json:"priority"`
}

// AllTopicSuffixes returns the rule's ingress topic followed by its egress
// topics — every suffix the rule writes to. Used for topic-existence checks and
// tenant deprovisioning cleanup.
func (r TopicRoutingRule) AllTopicSuffixes() []string {
	return append([]string{r.IngressTopic}, r.EgressTopics...)
}

const (
	// MaxRoutingPatternLength is the maximum length of a routing rule pattern.
	MaxRoutingPatternLength = 256
)

// literalSegmentRegex validates individual literal (non-wildcard) pattern segments.
// Only lowercase alphanumeric and hyphens are allowed.
var literalSegmentRegex = regexp.MustCompile(`^[a-z0-9-]+$`)

// Sentinel errors for topic routing.
var (
	// ErrRoutingRulesNotFound indicates routing rules could not be found in the registry.
	ErrRoutingRulesNotFound = errors.New("routing rules not found")

	// ErrTopicNotProvisioned indicates a referenced Kafka topic does not exist.
	ErrTopicNotProvisioned = errors.New("topic not provisioned")

	// Validation sentinel errors for ValidateRoutingRules.
	ErrEmptyRoutingRules       = errors.New("routing rules cannot be empty")
	ErrEmptyRoutingPattern     = errors.New("pattern cannot be empty")
	ErrMissingIngressTopic     = errors.New("ingress_topic is required")
	ErrTooManyTopics           = errors.New("too many topics per rule")
	ErrDuplicateEgressTopic    = errors.New("duplicate egress topic")
	ErrReservedTopicSuffix     = errors.New("reserved topic suffix")
	ErrEgressIngressOverlap    = errors.New("egress topic overlaps a ingress topic")
	ErrDuplicateRoutingPattern = errors.New("duplicate pattern")
	ErrDuplicatePriority       = errors.New("duplicate priority")
	ErrInvalidRoutingPattern   = errors.New("invalid pattern syntax")
	ErrTooManyRoutingRules     = errors.New("too many routing rules")
)

// ValidateRoutingRules validates a slice of topic routing rules.
// Enforces: count limit, non-empty patterns, valid pattern syntax, unique
// priorities, per-rule destination count limit (1 delivery + N egress), reserved
// suffixes, and the tenant-wide ingress/egress disjointness invariant
// (ValidateEgressDisjoint). maxRules=0 and maxTopicsPerRule=0 skip the
// respective limit checks.
func ValidateRoutingRules(rules []TopicRoutingRule, maxRules, maxTopicsPerRule int) error {
	if len(rules) == 0 {
		return ErrEmptyRoutingRules
	}
	if maxRules > 0 && len(rules) > maxRules {
		return fmt.Errorf("%w: got %d, max %d", ErrTooManyRoutingRules, len(rules), maxRules)
	}

	seenPatterns := make(map[string]struct{}, len(rules))
	seenPriorities := make(map[int]struct{}, len(rules))

	for i, rule := range rules {
		if rule.Pattern == "" {
			return fmt.Errorf("routing rule %d: %w", i, ErrEmptyRoutingPattern)
		}
		if err := validateRuleTopics(rule, maxTopicsPerRule); err != nil {
			return fmt.Errorf("routing rule %d: %w", i, err)
		}

		if _, ok := seenPatterns[rule.Pattern]; ok {
			return fmt.Errorf("routing rule %d: %w: %s", i, ErrDuplicateRoutingPattern, rule.Pattern)
		}
		seenPatterns[rule.Pattern] = struct{}{}

		if _, ok := seenPriorities[rule.Priority]; ok {
			return fmt.Errorf("routing rule %d: %w: %d", i, ErrDuplicatePriority, rule.Priority)
		}
		seenPriorities[rule.Priority] = struct{}{}

		if err := validateRoutingPattern(rule.Pattern); err != nil {
			return fmt.Errorf("routing rule %d: %w", i, err)
		}
	}

	return ValidateEgressDisjoint(rules)
}

// validateRuleTopics validates a single rule's topic destinations: delivery
// required, total destinations within the per-rule cap, no duplicate egress
// entries, no reserved suffixes, and no per-rule ingress/egress overlap.
func validateRuleTopics(rule TopicRoutingRule, maxTopicsPerRule int) error {
	if rule.IngressTopic == "" {
		return ErrMissingIngressTopic
	}
	// The DLQ is infrastructure: never a routing destination. A delivery rule
	// naming it would deliver dead-lettered records to subscribers; an egress
	// rule naming it would mix egress copies into the failure stream.
	if rule.IngressTopic == routing.DeadLetterTopicSuffix {
		return fmt.Errorf("%w: %q cannot be a ingress topic", ErrReservedTopicSuffix, rule.IngressTopic)
	}
	total := 1 + len(rule.EgressTopics)
	if maxTopicsPerRule > 0 && total > maxTopicsPerRule {
		return fmt.Errorf("%w: got %d, max %d", ErrTooManyTopics, total, maxTopicsPerRule)
	}
	seen := make(map[string]struct{}, len(rule.EgressTopics))
	for _, suffix := range rule.EgressTopics {
		switch suffix {
		case "":
			return fmt.Errorf("%w: egress topic cannot be empty", ErrReservedTopicSuffix)
		// The default topic and the DLQ are always consumed / infrastructure —
		// an egress copy landing in either would be delivered to subscribers or
		// pollute the failure stream.
		case routing.DefaultTopicSuffix, routing.DeadLetterTopicSuffix:
			return fmt.Errorf("%w: %q cannot be an egress topic", ErrReservedTopicSuffix, suffix)
		case rule.IngressTopic:
			return fmt.Errorf("%w: %q is this rule's ingress topic", ErrEgressIngressOverlap, suffix)
		}
		if _, dup := seen[suffix]; dup {
			return fmt.Errorf("%w: %s", ErrDuplicateEgressTopic, suffix)
		}
		seen[suffix] = struct{}{}
	}
	return nil
}

// ValidateEgressDisjoint enforces the tenant-wide invariant that no topic suffix
// is both a ingress topic (consumed) and an egress topic (never consumed)
// across the full rule set. Violating it silently turns an egress copy into a
// delivered duplicate: the moment any rule's ingress topic equals another
// rule's egress topic, that topic joins the consume set and every egress copy
// written to it reaches subscribers.
func ValidateEgressDisjoint(rules []TopicRoutingRule) error {
	deliveries := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		deliveries[rule.IngressTopic] = struct{}{}
	}
	for _, rule := range rules {
		for _, suffix := range rule.EgressTopics {
			if _, clash := deliveries[suffix]; clash {
				return fmt.Errorf("%w: %q is a ingress topic of another rule", ErrEgressIngressOverlap, suffix)
			}
		}
	}
	return nil
}

// validateRoutingPattern validates a routing pattern's syntax.
// Splits by "." and validates literal segments against [a-z0-9-]+.
// Wildcard segments ("*" and "**") are exempt from character validation.
// Returns ErrInvalidRoutingPattern or ErrMultipleDoubleWildcard on failure.
func validateRoutingPattern(pattern string) error {
	if len(pattern) > MaxRoutingPatternLength {
		return fmt.Errorf("%w: pattern exceeds %d characters", ErrInvalidRoutingPattern, MaxRoutingPatternLength)
	}
	segments := strings.SplitSeq(pattern, ".")
	for seg := range segments {
		if seg == "*" || seg == "**" {
			continue
		}
		if !literalSegmentRegex.MatchString(seg) {
			return fmt.Errorf("%w: invalid segment %q (must match [a-z0-9-]+)", ErrInvalidRoutingPattern, seg)
		}
	}
	// Normalize first (converts bare * to **) so the wildcard semantic check
	// reflects exactly what will be stored and evaluated at load time.
	// Without this, patterns like "a.*.b.*" pass validation but silently fail
	// after normalization converts them to "a.**.b.**" (two ** = invalid).
	normalized := routing.NormalizePattern(pattern)
	if _, err := routing.MatchRoutingPattern(normalized, "probe"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRoutingPattern, err)
	}
	return nil
}

// ConsumeTopicSuffixes returns the deduplicated, order-preserving set of topic
// suffixes the platform consumes (and therefore delivers to subscribers) for a
// tenant with the given routing rules: the default topic ∪ each rule's delivery
// topic. The tenant's default topic is always a member — rule-less tenants still
// ingest via the default topic (ADR-0006). Egress topics MUST NEVER appear here:
// a consumed egress topic re-delivers every copy to every subscriber (ADR-0018).
func ConsumeTopicSuffixes(rules []TopicRoutingRule) []string {
	suffixes := []string{routing.DefaultTopicSuffix}
	seen := map[string]struct{}{routing.DefaultTopicSuffix: {}}
	for _, rule := range rules {
		if rule.IngressTopic == "" {
			continue // defense in depth (§II): rules are validated at write time
		}
		if _, ok := seen[rule.IngressTopic]; ok {
			continue
		}
		seen[rule.IngressTopic] = struct{}{}
		suffixes = append(suffixes, rule.IngressTopic)
	}
	return suffixes
}

// CreateOnlyTopicSuffixes returns the deduplicated, order-preserving set of
// egress topic suffixes for the given rules — topics that must exist on the
// broker but MUST NEVER be consumed. Suffixes that also appear in the consume
// set are excluded defensively (§II); validation rejects that overlap at write
// time (ValidateEgressDisjoint).
func CreateOnlyTopicSuffixes(rules []TopicRoutingRule) []string {
	consumed := make(map[string]struct{})
	for _, suffix := range ConsumeTopicSuffixes(rules) {
		consumed[suffix] = struct{}{}
	}
	var suffixes []string
	seen := make(map[string]struct{})
	for _, rule := range rules {
		for _, suffix := range rule.EgressTopics {
			if suffix == "" {
				continue // defense in depth (§II)
			}
			if _, ok := consumed[suffix]; ok {
				continue
			}
			if _, ok := seen[suffix]; ok {
				continue
			}
			seen[suffix] = struct{}{}
			suffixes = append(suffixes, suffix)
		}
	}
	return suffixes
}
