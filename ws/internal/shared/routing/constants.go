package routing

import (
	"errors"
	"strings"
)

const (
	// DeadLetterTopicSuffix is the Kafka topic suffix for the per-tenant dead-letter topic.
	DeadLetterTopicSuffix = "dead-letter"

	// DefaultTopicSuffix is the Kafka topic suffix for the per-tenant default topic.
	// Used as the catch-all routing target for the default routing rule created by sukko up.
	DefaultTopicSuffix = "default"

	// DefaultCatchAllPriority is the priority for the default catch-all routing rule.
	// Rules with lower priority values match first; the default rule sits at 100 so
	// tenant-specific rules (0–99) always take precedence.
	DefaultCatchAllPriority = 100

	// MetricInvalidPatternBase is the base metric name without service prefix.
	// Callers prepend their MetricPrefix (e.g., "ws", "gateway", "provisioning")
	// and a separator to produce the full name per Constitution VI.
	MetricInvalidPatternBase = "routing_invalid_pattern_total"
)

// ErrMultipleDoubleWildcard is returned when a pattern contains more than one ** segment.
var ErrMultipleDoubleWildcard = errors.New("routing: pattern contains more than one ** wildcard")

// NormalizePattern replaces bare * tokens with **; ** is left unchanged (idempotent).
func NormalizePattern(pattern string) string {
	if pattern == "" {
		return pattern
	}
	segs := strings.Split(pattern, ".")
	for i, s := range segs {
		if s == "*" {
			segs[i] = "**"
		}
	}
	return strings.Join(segs, ".")
}

// MatchRoutingPattern reports whether pattern matches channel using routing semantics:
//   - * matches exactly one segment (no dots)
//   - ** matches one or more segments (including dots)
//
// Returns (false, ErrMultipleDoubleWildcard) when the pattern contains >1 ** token.
// Lives in shared/routing (not provisioning) because ws-server also needs it and
// MUST NOT import the service-specific provisioning package (Constitution X).
func MatchRoutingPattern(pattern, channel string) (bool, error) {
	if channel == "" {
		return false, nil
	}

	pSegs := strings.Split(pattern, ".")
	cSegs := strings.Split(channel, ".")

	// Count ** tokens; >1 is invalid.
	doubleCount := 0
	doubleIdx := -1
	for i, s := range pSegs {
		if s == "**" {
			doubleCount++
			doubleIdx = i
		}
	}
	if doubleCount > 1 {
		return false, ErrMultipleDoubleWildcard
	}

	if doubleCount == 0 {
		// No **: segment counts must match exactly.
		if len(pSegs) != len(cSegs) {
			return false, nil
		}
		for i, ps := range pSegs {
			if ps == "*" {
				continue // single-segment wildcard always matches
			}
			if ps != cSegs[i] {
				return false, nil
			}
		}
		return true, nil
	}

	// Exactly one **: split pattern around the ** position.
	// prefix = pSegs[:doubleIdx], suffix = pSegs[doubleIdx+1:]
	prefix := pSegs[:doubleIdx]
	suffix := pSegs[doubleIdx+1:]

	// Channel must have enough segments for prefix + at least one middle + suffix.
	minLen := len(prefix) + 1 + len(suffix)
	if len(cSegs) < minLen {
		return false, nil
	}

	// Match prefix segments.
	for i, ps := range prefix {
		if ps != "*" && ps != cSegs[i] {
			return false, nil
		}
	}

	// Match suffix segments from the end.
	for j, ps := range suffix {
		ci := len(cSegs) - len(suffix) + j
		if ps != "*" && ps != cSegs[ci] {
			return false, nil
		}
	}

	return true, nil
}
