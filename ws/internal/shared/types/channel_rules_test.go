package types

import (
	"errors"
	"strings"
	"testing"
)

func TestChannelRules_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   ChannelRules
		wantErr error
	}{
		{
			name: "valid_rules_empty",
			rules: ChannelRules{
				Public:        []string{},
				GroupMappings: map[string][]string{},
			},
			wantErr: nil,
		},
		{
			name: "valid_rules_with_public",
			rules: ChannelRules{
				Public:        []string{"*.trade", "*.liquidity", "*.metadata"},
				GroupMappings: map[string][]string{},
			},
			wantErr: nil,
		},
		{
			name: "valid_rules_with_groups",
			rules: ChannelRules{
				Public: []string{"*.metadata"},
				GroupMappings: map[string][]string{
					"traders":       {"*.trade", "*.liquidity"},
					"premium":       {"*.realtime"},
					"market-makers": {"*.orderbook", "*.depth"},
				},
			},
			wantErr: nil,
		},
		{
			name: "valid_rules_with_default",
			rules: ChannelRules{
				Public:        []string{"*.metadata"},
				GroupMappings: map[string][]string{},
				Default:       []string{"*.basic"},
			},
			wantErr: nil,
		},
		{
			name: "invalid_public_pattern",
			rules: ChannelRules{
				Public:        []string{"*.trade", "invalid pattern!"},
				GroupMappings: map[string][]string{},
			},
			wantErr: ErrInvalidChannelPattern,
		},
		{
			name: "invalid_group_pattern",
			rules: ChannelRules{
				Public: []string{},
				GroupMappings: map[string][]string{
					"traders": {"*.trade", "has spaces"},
				},
			},
			wantErr: ErrInvalidChannelPattern,
		},
		{
			name: "empty_group_name",
			rules: ChannelRules{
				Public: []string{},
				GroupMappings: map[string][]string{
					"":        {"*.trade"},
					"traders": {"*.liquidity"},
				},
			},
			wantErr: ErrEmptyGroupName,
		},
		{
			name: "group_name_too_long",
			rules: ChannelRules{
				Public: []string{},
				GroupMappings: map[string][]string{
					strings.Repeat("a", MaxGroupNameLength+1): {"*.trade"},
				},
			},
			wantErr: ErrGroupNameTooLong,
		},
		{
			name: "invalid_default_pattern",
			rules: ChannelRules{
				Public:        []string{},
				GroupMappings: map[string][]string{},
				Default:       []string{"valid", "in valid!"},
			},
			wantErr: ErrInvalidChannelPattern,
		},
		{
			name: "too_many_public_patterns",
			rules: ChannelRules{
				Public:        make([]string, MaxPublicPatterns+1),
				GroupMappings: map[string][]string{},
			},
			wantErr: ErrTooManyPublicPatterns,
		},
	}

	// Initialize the too_many_public_patterns test case
	for i := range tests {
		if tests[i].name == "too_many_public_patterns" {
			for j := range tests[i].rules.Public {
				tests[i].rules.Public[j] = "*.test"
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rules.Validate()

			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("Validate() unexpected error: %v", err)
				}
				return
			}

			if err == nil {
				t.Errorf("Validate() expected error %v, got nil", tt.wantErr)
				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestChannelRules_ComputeAllowedPatterns(t *testing.T) {
	t.Parallel()

	rules := ChannelRules{
		Public: []string{"*.metadata", "*.analytics"},
		GroupMappings: map[string][]string{
			"traders": {"*.trade", "*.liquidity"},
			"premium": {"*.realtime"},
			"admins":  {"*"},
		},
		Default: []string{"*.basic"},
	}

	tests := []struct {
		name     string
		groups   []string
		expected []string
	}{
		{
			name:     "no_groups_uses_default",
			groups:   []string{},
			expected: []string{"*.metadata", "*.analytics", "*.basic"},
		},
		{
			name:     "single_group",
			groups:   []string{"traders"},
			expected: []string{"*.metadata", "*.analytics", "*.trade", "*.liquidity"},
		},
		{
			name:     "multiple_groups",
			groups:   []string{"traders", "premium"},
			expected: []string{"*.metadata", "*.analytics", "*.trade", "*.liquidity", "*.realtime"},
		},
		{
			name:     "admin_group",
			groups:   []string{"admins"},
			expected: []string{"*.metadata", "*.analytics", "*"},
		},
		{
			name:     "unknown_group_uses_default",
			groups:   []string{"unknown"},
			expected: []string{"*.metadata", "*.analytics", "*.basic"},
		},
		{
			name:     "mixed_known_unknown_groups",
			groups:   []string{"unknown", "traders"},
			expected: []string{"*.metadata", "*.analytics", "*.trade", "*.liquidity"},
		},
		{
			name:     "nil_groups_uses_default",
			groups:   nil,
			expected: []string{"*.metadata", "*.analytics", "*.basic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := rules.ComputeAllowedPatterns(tt.groups)

			if len(got) != len(tt.expected) {
				t.Errorf("ComputeAllowedPatterns() got %d patterns, want %d", len(got), len(tt.expected))
				t.Logf("got: %v", got)
				t.Logf("expected: %v", tt.expected)
				return
			}

			// Check all expected patterns are present
			gotMap := make(map[string]bool)
			for _, p := range got {
				gotMap[p] = true
			}

			for _, exp := range tt.expected {
				if !gotMap[exp] {
					t.Errorf("ComputeAllowedPatterns() missing pattern %q", exp)
				}
			}
		})
	}
}

func TestChannelRules_ComputeAllowedPatterns_Deduplication(t *testing.T) {
	t.Parallel()

	rules := ChannelRules{
		Public: []string{"*.trade"},
		GroupMappings: map[string][]string{
			"group1": {"*.trade", "*.liquidity"}, // *.trade duplicated with public
			"group2": {"*.trade", "*.analytics"}, // *.trade duplicated again
		},
	}

	got := rules.ComputeAllowedPatterns([]string{"group1", "group2"})

	// Count occurrences
	counts := make(map[string]int)
	for _, p := range got {
		counts[p]++
	}

	for pattern, count := range counts {
		if count > 1 {
			t.Errorf("Pattern %q appears %d times, should be deduplicated to 1", pattern, count)
		}
	}

	// Should have: *.trade, *.liquidity, *.analytics
	if len(got) != 3 {
		t.Errorf("ComputeAllowedPatterns() got %d patterns, want 3 (after deduplication)", len(got))
		t.Logf("got: %v", got)
	}
}

func TestChannelRules_ComputeAllowedPublishPatterns(t *testing.T) {
	t.Parallel()

	rules := ChannelRules{
		PublishPublic: []string{"*.announcements"},
		PublishGroupMappings: map[string][]string{
			"traders": {"*.trade", "*.orders"},
			"admins":  {"*"},
		},
		PublishDefault: []string{"*.feedback"},
	}

	tests := []struct {
		name     string
		groups   []string
		expected []string
	}{
		{
			name:     "no_groups_uses_publish_default",
			groups:   []string{},
			expected: []string{"*.announcements", "*.feedback"},
		},
		{
			name:     "single_group",
			groups:   []string{"traders"},
			expected: []string{"*.announcements", "*.trade", "*.orders"},
		},
		{
			name:     "multiple_groups",
			groups:   []string{"traders", "admins"},
			expected: []string{"*.announcements", "*.trade", "*.orders", "*"},
		},
		{
			name:     "unknown_group_uses_default",
			groups:   []string{"unknown"},
			expected: []string{"*.announcements", "*.feedback"},
		},
		{
			name:     "nil_groups_uses_default",
			groups:   nil,
			expected: []string{"*.announcements", "*.feedback"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := rules.ComputeAllowedPublishPatterns(tt.groups)

			if len(got) != len(tt.expected) {
				t.Errorf("ComputeAllowedPublishPatterns() got %d patterns, want %d", len(got), len(tt.expected))
				t.Logf("got: %v", got)
				t.Logf("expected: %v", tt.expected)
				return
			}

			gotMap := make(map[string]bool)
			for _, p := range got {
				gotMap[p] = true
			}
			for _, exp := range tt.expected {
				if !gotMap[exp] {
					t.Errorf("ComputeAllowedPublishPatterns() missing pattern %q", exp)
				}
			}
		})
	}
}

func TestChannelRules_HasPublishRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rules    ChannelRules
		expected bool
	}{
		{
			name:     "no_publish_rules",
			rules:    ChannelRules{},
			expected: false,
		},
		{
			name:     "empty_publish_fields",
			rules:    ChannelRules{PublishPublic: []string{}, PublishGroupMappings: map[string][]string{}},
			expected: false,
		},
		{
			name:     "has_publish_public",
			rules:    ChannelRules{PublishPublic: []string{"*.trade"}},
			expected: true,
		},
		{
			name:     "has_publish_group_mappings",
			rules:    ChannelRules{PublishGroupMappings: map[string][]string{"traders": {"*.trade"}}},
			expected: true,
		},
		{
			name:     "has_publish_default",
			rules:    ChannelRules{PublishDefault: []string{"*.feedback"}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.rules.HasPublishRules()
			if got != tt.expected {
				t.Errorf("HasPublishRules() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestChannelRules_ValidateWithPlaceholders(t *testing.T) {
	t.Parallel()

	validNames := []string{"tenant_id", "user_id", "sub", "principal"}

	tests := []struct {
		name    string
		rules   ChannelRules
		wantErr bool
	}{
		{
			name: "no_placeholders",
			rules: ChannelRules{
				Public:        []string{"*.trade"},
				GroupMappings: map[string][]string{},
			},
			wantErr: false,
		},
		{
			name: "valid_placeholders",
			rules: ChannelRules{
				Public:        []string{"{tenant_id}.*.trade"},
				GroupMappings: map[string][]string{"g": {"{user_id}.private"}},
				PublishPublic: []string{"{principal}.announcements"},
			},
			wantErr: false,
		},
		{
			name: "unknown_placeholder_in_public",
			rules: ChannelRules{
				Public:        []string{"{unknown}.*.trade"},
				GroupMappings: map[string][]string{},
			},
			wantErr: true,
		},
		{
			name: "unknown_placeholder_in_group",
			rules: ChannelRules{
				Public:        []string{},
				GroupMappings: map[string][]string{"g": {"{bad_name}.private"}},
			},
			wantErr: true,
		},
		{
			name: "unknown_placeholder_in_publish_default",
			rules: ChannelRules{
				PublishDefault: []string{"{nope}.feedback"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rules.ValidateWithPlaceholders(validNames)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateWithPlaceholders() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestChannelRules_Validate_PublishFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   ChannelRules
		wantErr bool
	}{
		{
			name: "valid_publish_public",
			rules: ChannelRules{
				PublishPublic: []string{"*.trade", "*.orders"},
			},
			wantErr: false,
		},
		{
			name: "valid_publish_group_mappings",
			rules: ChannelRules{
				PublishGroupMappings: map[string][]string{
					"traders": {"*.trade"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid_publish_public_pattern",
			rules: ChannelRules{
				PublishPublic: []string{"invalid pattern!"},
			},
			wantErr: true,
		},
		{
			name: "empty_publish_group_name",
			rules: ChannelRules{
				PublishGroupMappings: map[string][]string{
					"": {"*.trade"},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid_publish_default_pattern",
			rules: ChannelRules{
				PublishDefault: []string{"bad pattern!"},
			},
			wantErr: true,
		},
		{
			name: "too_many_publish_public_patterns",
			rules: ChannelRules{
				PublishPublic: make([]string, MaxPublicPatterns+1),
			},
			wantErr: true,
		},
	}

	// Initialize the too_many test case
	for i := range tests {
		if tests[i].name == "too_many_publish_public_patterns" {
			for j := range tests[i].rules.PublishPublic {
				tests[i].rules.PublishPublic[j] = "*.test"
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.rules.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsValidChannelPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		valid   bool
	}{
		// Valid patterns
		{"*.trade", true},
		{"BTC.trade", true},
		{"BTC.*", true},
		{"*", true},
		{"trade", true},
		{"BTC-USD.trade", true},
		{"BTC_USD.trade", true},
		{"a", true},
		{strings.Repeat("a", MaxChannelPatternLength), true},

		// Invalid patterns
		{"", false},
		{"has space", false},
		{"has\ttab", false},
		{"has\nnewline", false},
		{"*.trade!", false},
		{"@invalid", false},
		{"#invalid", false},
		{strings.Repeat("a", MaxChannelPatternLength+1), false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()

			got := IsValidChannelPattern(tt.pattern)
			if got != tt.valid {
				t.Errorf("IsValidChannelPattern(%q) = %v, want %v", tt.pattern, got, tt.valid)
			}
		})
	}
}

func TestNewChannelRules(t *testing.T) {
	t.Parallel()

	rules := NewChannelRules()

	if rules == nil {
		t.Fatal("NewChannelRules() returned nil")
	}

	if rules.Public == nil {
		t.Error("NewChannelRules().Public should not be nil")
	}

	if rules.GroupMappings == nil {
		t.Error("NewChannelRules().GroupMappings should not be nil")
	}

	if rules.Default == nil {
		t.Error("NewChannelRules().Default should not be nil")
	}

	// Should validate without errors
	if err := rules.Validate(); err != nil {
		t.Errorf("NewChannelRules().Validate() unexpected error: %v", err)
	}
}

func TestDeduplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "single",
			input:    []string{"a"},
			expected: []string{"a"},
		},
		{
			name:     "no_duplicates",
			input:    []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "with_duplicates",
			input:    []string{"a", "b", "a", "c", "b"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "all_same",
			input:    []string{"a", "a", "a"},
			expected: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := deduplicate(tt.input)

			if len(got) != len(tt.expected) {
				t.Errorf("deduplicate() got %d items, want %d", len(got), len(tt.expected))
				return
			}

			for i, v := range tt.expected {
				if got[i] != v {
					t.Errorf("deduplicate()[%d] = %q, want %q", i, got[i], v)
				}
			}
		})
	}
}
