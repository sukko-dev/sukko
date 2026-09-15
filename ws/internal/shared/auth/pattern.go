package auth

import "strings"

// MatchWildcard matches a pattern against a value using simple wildcard rules.
// This is a fast, non-regex alternative to MatchPattern for simple cases.
//
// Supports:
//   - Exact match: "foo.bar" matches "foo.bar"
//   - Catch-all: "*" matches anything
//   - Prefix wildcard: "*.suffix" matches "anything.suffix"
//   - Suffix wildcard: "prefix.*" matches "prefix.anything"
//   - Middle wildcard: "prefix*suffix" matches "prefixANYsuffix"
//
// For patterns with placeholders like "{tenant_id}.*.trade", use MatchPattern instead.
func MatchWildcard(pattern, value string) bool {
	// Handle exact match
	if pattern == value {
		return true
	}

	// Handle simple wildcard (matches anything)
	if pattern == "*" {
		return true
	}

	// Handle prefix wildcard: *.suffix
	if after, found := strings.CutPrefix(pattern, "*"); found {
		return strings.HasSuffix(value, after)
	}

	// Handle suffix wildcard: prefix.*
	if before, found := strings.CutSuffix(pattern, "*"); found {
		return strings.HasPrefix(value, before)
	}

	// Handle middle wildcard: prefix*suffix
	if idx := strings.Index(pattern, "*"); idx > 0 {
		prefix := pattern[:idx]
		suffix := pattern[idx+1:]
		return strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix)
	}

	return false
}

// MatchAnyWildcard returns true if value matches any pattern using wildcard rules.
func MatchAnyWildcard(patterns []string, value string) bool {
	for _, p := range patterns {
		if MatchWildcard(p, value) {
			return true
		}
	}
	return false
}
