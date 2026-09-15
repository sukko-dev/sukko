// Package auth provides JWT authentication for WebSocket connections.
package auth

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// Sentinel errors for channel validation.
var (
	// ErrEmptyChannel indicates the channel string is empty.
	ErrEmptyChannel = errors.New("channel cannot be empty")

	// ErrEmptyChannelSegment indicates a dot-separated segment is empty.
	ErrEmptyChannelSegment = errors.New("empty channel segment")

	// ErrInsufficientChannelParts indicates the channel has fewer parts than required.
	ErrInsufficientChannelParts = errors.New("insufficient channel parts")
)

// extractChannelTenant extracts the tenant prefix from an internal channel name.
// Returns empty string if no separator is found.
//
// Example:
//
//	extractChannelTenant("acme.BTC.trade", ".") → "acme"
//	extractChannelTenant("channel", ".") → ""
func extractChannelTenant(channel, separator string) string {
	before, _, ok := strings.Cut(channel, separator)
	if !ok {
		return ""
	}
	return before
}

// IsSharedChannel checks if a channel pattern indicates a shared (cross-tenant) channel.
// Shared channels don't have tenant prefixes and are accessible by all tenants.
//
// Example patterns for shared channels:
//   - "system.*"
//   - "broadcast.*"
func IsSharedChannel(channel string, sharedPatterns []string) bool {
	return MatchAnyWildcard(sharedPatterns, channel)
}

// ValidateChannelSegments checks that no segment of a dot-separated channel is empty.
// This catches patterns like "acme..trade", ".acme.trade", or "acme.trade.".
// Used standalone for channel suffixes (any number of segments) and as the base
// for ValidateInternalChannel (which adds a minimum-parts check).
func ValidateChannelSegments(channel string) error {
	if channel == "" {
		return ErrEmptyChannel
	}

	parts := strings.Split(channel, ".")
	for i, part := range parts {
		if part == "" {
			return fmt.Errorf("segment %d: %w", i, ErrEmptyChannelSegment)
		}
	}

	return nil
}

// ValidateInternalChannel validates internal channel format.
// Internal channels must have at least 2 dot-separated parts: {tenant_id}.{suffix}
// Parts cannot be empty.
//
// Examples:
//   - "acme.trade" → valid (tenant_id=acme, suffix=trade)
//   - "acme.BTC.trade" → valid (tenant_id=acme, suffix=BTC.trade)
//   - "trade" → invalid (only 1 part, missing tenant prefix)
func ValidateInternalChannel(channel string) error {
	if err := ValidateChannelSegments(channel); err != nil {
		return err
	}

	// Count parts by counting separators (avoids redundant strings.Split)
	parts := strings.Count(channel, ".") + 1
	if parts < protocol.MinInternalChannelParts {
		return fmt.Errorf("%w: internal channel requires at least %d parts, got %d",
			ErrInsufficientChannelParts, protocol.MinInternalChannelParts, parts)
	}

	return nil
}

// IsValidInternalChannel checks if a channel has valid internal format.
// This is a convenience wrapper around ValidateInternalChannel for boolean checks.
func IsValidInternalChannel(channel string) bool {
	return ValidateInternalChannel(channel) == nil
}
