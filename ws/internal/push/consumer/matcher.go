package consumer

import "github.com/sukko-dev/sukko/internal/shared/auth"

// MatchChannel returns true if channelKey matches any of the given patterns.
// It delegates to the shared auth.MatchAnyWildcard implementation which supports:
//   - Exact match: "acme.alerts.BTC" matches "acme.alerts.BTC"
//   - Catch-all: "*" matches anything
//   - Prefix wildcard: "acme.alerts.*" matches "acme.alerts.BTC"
//   - Suffix wildcard: "*.trade" matches "acme.trade"
//   - Middle wildcard: "acme.*.trade" matches "acme.BTC.trade"
//
// This is the single entry point for push channel matching — avoids duplicating
// the wildcard logic from shared/auth (Constitution X).
func MatchChannel(channelKey string, patterns []string) bool {
	return auth.MatchAnyWildcard(patterns, channelKey)
}
