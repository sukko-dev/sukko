package types

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// ChannelRules represents per-tenant channel access rules.
// Used by provisioning (storage) and gateway (authorization).
type ChannelRules struct {
	// Public contains channel patterns accessible to all authenticated users of this tenant.
	Public []string `json:"public"`

	// GroupMappings maps IdP group names to allowed channel patterns.
	// Example: {"traders": ["*.trade", "*.liquidity"], "premium": ["*.realtime"]}
	GroupMappings map[string][]string `json:"group_mappings"`

	// Default contains channel patterns allowed when no groups match.
	// Used as a fallback for users without any mapped groups.
	Default []string `json:"default,omitempty"`

	// PublishPublic contains channel patterns any authenticated user can publish to.
	PublishPublic []string `json:"publish_public,omitempty"`

	// PublishGroupMappings maps IdP group names to allowed publish channel patterns.
	PublishGroupMappings map[string][]string `json:"publish_group_mappings,omitempty"`

	// PublishDefault contains publish channel patterns when no groups match.
	PublishDefault []string `json:"publish_default,omitempty"`
}

// TenantChannelRules represents stored channel rules for a tenant.
type TenantChannelRules struct {
	// TenantID is the tenant these rules belong to.
	TenantID string `json:"tenant_id"`

	// Rules contains the channel access rules.
	Rules ChannelRules `json:"rules"`

	// CreatedAt is when the rules were created.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the rules were last updated.
	UpdatedAt time.Time `json:"updated_at"`
}

// channelPatternRegex validates channel patterns for ChannelRules.
// Allows: alphanumeric, dots, hyphens, underscores, wildcards (*), and curly braces ({})
// for {principal} placeholder patterns.
var channelPatternRegex = regexp.MustCompile(`^[-a-zA-Z0-9.*_{}]{1,256}$`)

// placeholderTokenRegex extracts {name} tokens from patterns for validation.
var placeholderTokenRegex = regexp.MustCompile(`\{([^}]+)\}`)

// Validate validates channel rules. Defense in depth.
func (r *ChannelRules) Validate() error {
	// Validate public patterns count
	if len(r.Public) > MaxPublicPatterns {
		return fmt.Errorf("%w: got %d, max %d", ErrTooManyPublicPatterns, len(r.Public), MaxPublicPatterns)
	}

	// Validate public patterns
	for _, pattern := range r.Public {
		if !IsValidChannelPattern(pattern) {
			return fmt.Errorf("%w: %q", ErrInvalidChannelPattern, pattern)
		}
	}

	// Validate group mappings count
	if len(r.GroupMappings) > MaxGroupsPerMapping {
		return fmt.Errorf("%w: got %d, max %d", ErrTooManyGroups, len(r.GroupMappings), MaxGroupsPerMapping)
	}

	// Validate group mappings
	for group, patterns := range r.GroupMappings {
		if group == "" {
			return ErrEmptyGroupName
		}
		if len(group) > MaxGroupNameLength {
			return fmt.Errorf("%w: %q (max %d)", ErrGroupNameTooLong, group, MaxGroupNameLength)
		}
		if len(patterns) > MaxPatternsPerGroup {
			return fmt.Errorf("%w: group %q has %d patterns, max %d", ErrTooManyPatterns, group, len(patterns), MaxPatternsPerGroup)
		}
		for _, pattern := range patterns {
			if !IsValidChannelPattern(pattern) {
				return fmt.Errorf("%w for group %q: %q", ErrInvalidChannelPattern, group, pattern)
			}
		}
	}

	// Validate default patterns
	if len(r.Default) > MaxPatternsPerGroup {
		return fmt.Errorf("%w: default has %d patterns, max %d", ErrTooManyPatterns, len(r.Default), MaxPatternsPerGroup)
	}
	for _, pattern := range r.Default {
		if !IsValidChannelPattern(pattern) {
			return fmt.Errorf("%w in default: %q", ErrInvalidChannelPattern, pattern)
		}
	}

	// Validate publish public patterns
	if len(r.PublishPublic) > MaxPublicPatterns {
		return fmt.Errorf("%w: publish_public got %d, max %d", ErrTooManyPublicPatterns, len(r.PublishPublic), MaxPublicPatterns)
	}
	for _, pattern := range r.PublishPublic {
		if !IsValidChannelPattern(pattern) {
			return fmt.Errorf("%w in publish_public: %q", ErrInvalidChannelPattern, pattern)
		}
	}

	// Validate publish group mappings
	if len(r.PublishGroupMappings) > MaxGroupsPerMapping {
		return fmt.Errorf("%w: publish_group_mappings got %d, max %d", ErrTooManyGroups, len(r.PublishGroupMappings), MaxGroupsPerMapping)
	}
	for group, patterns := range r.PublishGroupMappings {
		if group == "" {
			return fmt.Errorf("%w in publish_group_mappings", ErrEmptyGroupName)
		}
		if len(group) > MaxGroupNameLength {
			return fmt.Errorf("%w in publish_group_mappings: %q (max %d)", ErrGroupNameTooLong, group, MaxGroupNameLength)
		}
		if len(patterns) > MaxPatternsPerGroup {
			return fmt.Errorf("%w: publish group %q has %d patterns, max %d", ErrTooManyPatterns, group, len(patterns), MaxPatternsPerGroup)
		}
		for _, pattern := range patterns {
			if !IsValidChannelPattern(pattern) {
				return fmt.Errorf("%w for publish group %q: %q", ErrInvalidChannelPattern, group, pattern)
			}
		}
	}

	// Validate publish default patterns
	if len(r.PublishDefault) > MaxPatternsPerGroup {
		return fmt.Errorf("%w: publish_default has %d patterns, max %d", ErrTooManyPatterns, len(r.PublishDefault), MaxPatternsPerGroup)
	}
	for _, pattern := range r.PublishDefault {
		if !IsValidChannelPattern(pattern) {
			return fmt.Errorf("%w in publish_default: %q", ErrInvalidChannelPattern, pattern)
		}
	}

	return nil
}

// ComputeAllowedPatterns returns all channel patterns a user can access based on their groups.
func (r *ChannelRules) ComputeAllowedPatterns(groups []string) []string {
	// Pre-allocate with estimated capacity
	capacity := len(r.Public)
	for _, g := range groups {
		if patterns, ok := r.GroupMappings[g]; ok {
			capacity += len(patterns)
		}
	}
	allowed := make([]string, 0, capacity)

	// Add public channels (always allowed)
	allowed = append(allowed, r.Public...)

	// Add channels for each group
	matched := false
	for _, group := range groups {
		if patterns, ok := r.GroupMappings[group]; ok {
			allowed = append(allowed, patterns...)
			matched = true
		}
	}

	// Add default if no groups matched
	if !matched && len(r.Default) > 0 {
		allowed = append(allowed, r.Default...)
	}

	return deduplicate(allowed)
}

// ComputeAllowedPublishPatterns returns all publish channel patterns a user can access based on their groups.
// Mirrors ComputeAllowedPatterns for publish rules.
func (r *ChannelRules) ComputeAllowedPublishPatterns(groups []string) []string {
	capacity := len(r.PublishPublic)
	for _, g := range groups {
		if patterns, ok := r.PublishGroupMappings[g]; ok {
			capacity += len(patterns)
		}
	}
	allowed := make([]string, 0, capacity)

	allowed = append(allowed, r.PublishPublic...)

	matched := false
	for _, group := range groups {
		if patterns, ok := r.PublishGroupMappings[group]; ok {
			allowed = append(allowed, patterns...)
			matched = true
		}
	}

	if !matched && len(r.PublishDefault) > 0 {
		allowed = append(allowed, r.PublishDefault...)
	}

	return deduplicate(allowed)
}

// HasPublishRules returns true if any publish rule field is non-empty.
// When false, client publishing is denied (secure by default).
func (r *ChannelRules) HasPublishRules() bool {
	return len(r.PublishPublic) > 0 || len(r.PublishGroupMappings) > 0 || len(r.PublishDefault) > 0
}

// ValidateWithPlaceholders validates that all {placeholder} tokens in pattern fields
// are recognized names. Accepts valid names as a parameter to avoid types importing auth
// (leaf package stays a leaf). Callers pass auth.PlaceholderNames().
func (r *ChannelRules) ValidateWithPlaceholders(validNames []string) error {
	valid := make(map[string]struct{}, len(validNames))
	for _, name := range validNames {
		valid[name] = struct{}{}
	}

	allPatterns := make([]string, 0, len(r.Public)+len(r.Default)+len(r.PublishPublic)+len(r.PublishDefault))
	allPatterns = append(allPatterns, r.Public...)
	allPatterns = append(allPatterns, r.Default...)
	allPatterns = append(allPatterns, r.PublishPublic...)
	allPatterns = append(allPatterns, r.PublishDefault...)
	for _, patterns := range r.GroupMappings {
		allPatterns = append(allPatterns, patterns...)
	}
	for _, patterns := range r.PublishGroupMappings {
		allPatterns = append(allPatterns, patterns...)
	}

	for _, pattern := range allPatterns {
		matches := placeholderTokenRegex.FindAllStringSubmatch(pattern, -1)
		for _, match := range matches {
			if len(match) > 1 {
				name := match[1]
				if _, ok := valid[name]; !ok {
					return fmt.Errorf("unknown placeholder {%s} in pattern %q; valid: [%s]",
						name, pattern, strings.Join(validNames, ", "))
				}
			}
		}
	}

	return nil
}

// deduplicate removes duplicate strings from a slice while preserving order.
func deduplicate(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	seen := make(map[string]struct{}, len(s))
	result := make([]string, 0, len(s))
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// IsValidChannelPattern checks if a pattern is valid.
// Exported for use in tests and other packages.
func IsValidChannelPattern(pattern string) bool {
	if pattern == "" || len(pattern) > MaxChannelPatternLength {
		return false
	}
	return channelPatternRegex.MatchString(pattern)
}

// NewChannelRules creates a new ChannelRules with all fields initialized.
func NewChannelRules() *ChannelRules {
	return &ChannelRules{
		Public:               []string{},
		GroupMappings:        make(map[string][]string),
		Default:              []string{},
		PublishPublic:        []string{},
		PublishGroupMappings: make(map[string][]string),
		PublishDefault:       []string{},
	}
}
