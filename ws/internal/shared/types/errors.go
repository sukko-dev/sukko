package types

import "errors"

// Sentinel errors for channel rules.
var (
	// ErrChannelRulesNotFound indicates channel rules are not configured for the tenant.
	ErrChannelRulesNotFound = errors.New("channel rules not found")

	// ErrInvalidChannelPattern indicates a channel pattern is malformed.
	ErrInvalidChannelPattern = errors.New("invalid channel pattern")

	// ErrEmptyGroupName indicates a group name is empty.
	ErrEmptyGroupName = errors.New("group name cannot be empty")

	// ErrGroupNameTooLong indicates a group name exceeds the maximum length.
	ErrGroupNameTooLong = errors.New("group name too long")

	// ErrTooManyGroups indicates the channel rules have too many group mappings.
	ErrTooManyGroups = errors.New("too many group mappings")

	// ErrTooManyPatterns indicates a group has too many channel patterns.
	ErrTooManyPatterns = errors.New("too many patterns for group")

	// ErrTooManyPublicPatterns indicates there are too many public channel patterns.
	ErrTooManyPublicPatterns = errors.New("too many public patterns")
)
