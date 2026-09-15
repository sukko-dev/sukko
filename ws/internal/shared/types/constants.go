// Package types defines core type definitions used across the WebSocket server.
package types

// Validation constants for channel rules.
// Configurable limits are in platform config; these are fixed protocol limits.
const (
	// MaxGroupNameLength is the maximum length of a group name in channel rules.
	MaxGroupNameLength = 128

	// MaxChannelPatternLength is the maximum length of a channel pattern.
	MaxChannelPatternLength = 256

	// MaxGroupsPerMapping is the maximum number of groups in a channel rules mapping.
	MaxGroupsPerMapping = 100

	// MaxPatternsPerGroup is the maximum number of channel patterns per group.
	MaxPatternsPerGroup = 50

	// MaxPublicPatterns is the maximum number of public channel patterns.
	MaxPublicPatterns = 50
)
