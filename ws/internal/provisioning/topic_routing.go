package provisioning

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// TopicRoutingRule maps a channel glob pattern to one or more Kafka topic suffixes.
// Rules are evaluated in priority order (lower = higher priority); first match wins.
type TopicRoutingRule struct {
	// Pattern is a channel glob pattern (e.g., "acme.**.trade").
	// Supports * (one segment) and ** (one or more segments).
	Pattern string `json:"pattern"`

	// Topics is the list of Kafka topic suffixes to write to when this rule matches.
	// Fan-out: all topics in the list receive the message.
	Topics []string `json:"topics"`

	// Priority determines evaluation order. Lower value = higher priority.
	Priority int `json:"priority"`
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
	ErrEmptyTopics             = errors.New("topics cannot be empty")
	ErrTooManyTopics           = errors.New("too many topics per rule")
	ErrDuplicateRoutingPattern = errors.New("duplicate pattern")
	ErrDuplicatePriority       = errors.New("duplicate priority")
	ErrInvalidRoutingPattern   = errors.New("invalid pattern syntax")
	ErrTooManyRoutingRules     = errors.New("too many routing rules")
)

// ValidateRoutingRules validates a slice of topic routing rules.
// Enforces: count limit, non-empty patterns/topics, valid pattern syntax,
// unique priorities, and per-rule topic count limit.
// maxRules=0 and maxTopicsPerRule=0 skip the respective limit checks.
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
		if len(rule.Topics) == 0 {
			return fmt.Errorf("routing rule %d: %w", i, ErrEmptyTopics)
		}
		if maxTopicsPerRule > 0 && len(rule.Topics) > maxTopicsPerRule {
			return fmt.Errorf("routing rule %d: %w: got %d, max %d", i, ErrTooManyTopics, len(rule.Topics), maxTopicsPerRule)
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

// ResolveTopics evaluates routing rules in priority order and returns the topics
// from the first matching rule. Returns nil if no rule matches.
func ResolveTopics(rules []TopicRoutingRule, channel string) ([]string, error) {
	for _, rule := range rules {
		matched, err := routing.MatchRoutingPattern(rule.Pattern, channel)
		if err != nil {
			continue // skip rules with invalid patterns
		}
		if matched {
			return rule.Topics, nil
		}
	}
	return nil, nil
}
