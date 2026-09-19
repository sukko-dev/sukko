package provisioning_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/sukko-dev/sukko/internal/provisioning"
)

// ---------------------------------------------------------------------------
// ValidateRoutingRules
// ---------------------------------------------------------------------------

func TestValidateRoutingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rules        []provisioning.TopicRoutingRule
		maxRules     int
		maxTopics    int
		wantSentinel error
		wantContains string
	}{
		{
			name: "valid_single_rule",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 1},
			},
		},
		{
			name: "valid_multiple_rules",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "crypto.**.trade", IngressTopic: "crypto-trade", Priority: 1},
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 2},
				{Pattern: "**.analytics", IngressTopic: "analytics", Priority: 3},
			},
		},
		{
			name: "valid_delivery_plus_egress",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"trade-replica"}, Priority: 1},
			},
		},
		{
			name: "valid_exact_literal_segments",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "btc.trade", IngressTopic: "btc-trade", Priority: 1},
			},
		},
		{
			name: "valid_single_wildcard",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.*.trade", IngressTopic: "trades", Priority: 1},
			},
		},
		{
			name: "valid_double_wildcard",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.**.trade", IngressTopic: "trades", Priority: 1},
			},
		},
		{
			name:         "empty_rules_slice",
			rules:        []provisioning.TopicRoutingRule{},
			wantSentinel: provisioning.ErrEmptyRoutingRules,
		},
		{
			name:         "nil_rules_slice",
			rules:        nil,
			wantSentinel: provisioning.ErrEmptyRoutingRules,
		},
		{
			name: "empty_pattern",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "", IngressTopic: "trade", Priority: 1},
			},
			wantSentinel: provisioning.ErrEmptyRoutingPattern,
		},
		{
			name: "missing_ingress_topic",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "", Priority: 1},
			},
			wantSentinel: provisioning.ErrMissingIngressTopic,
		},
		{
			name: "missing_ingress_topic_with_egress_present",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", EgressTopics: []string{"audit"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrMissingIngressTopic,
		},
		{
			name:      "too_many_topics_delivery_plus_egress",
			maxTopics: 2,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "t1", EgressTopics: []string{"t2", "t3"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrTooManyTopics,
		},
		{
			name:      "delivery_plus_egress_at_cap_is_valid",
			maxTopics: 3,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "t1", EgressTopics: []string{"t2", "t3"}, Priority: 1},
			},
		},
		{
			name:      "zero_maxTopicsPerRule_skips_limit",
			maxTopics: 0,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "t1", EgressTopics: []string{"t2", "t3", "t4", "t5"}, Priority: 1},
			},
		},
		{
			name: "duplicate_egress_topic",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"audit", "audit"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrDuplicateEgressTopic,
		},
		{
			name: "empty_egress_topic_rejected",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{""}, Priority: 1},
			},
			wantSentinel: provisioning.ErrReservedTopicSuffix,
		},
		{
			name: "dlq_suffix_rejected_as_delivery",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "dead-letter", Priority: 1},
			},
			wantSentinel: provisioning.ErrReservedTopicSuffix,
		},
		{
			name: "dlq_suffix_rejected_as_egress",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"dead-letter"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrReservedTopicSuffix,
		},
		{
			name: "default_suffix_rejected_as_egress",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"default"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrReservedTopicSuffix,
		},
		{
			name: "default_suffix_allowed_as_delivery",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "default", Priority: 1},
			},
		},
		{
			name: "egress_equal_to_own_delivery_rejected",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrEgressIngressOverlap,
		},
		{
			name: "egress_equal_to_another_rules_delivery_rejected",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"orders"}, Priority: 1},
				{Pattern: "**.order", IngressTopic: "orders", Priority: 2},
			},
			wantSentinel: provisioning.ErrEgressIngressOverlap,
		},
		{
			name: "same_egress_topic_across_rules_is_valid",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", EgressTopics: []string{"audit"}, Priority: 1},
				{Pattern: "**.order", IngressTopic: "orders", EgressTopics: []string{"audit"}, Priority: 2},
			},
		},
		{
			name:     "too_many_rules",
			maxRules: 2,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 1},
				{Pattern: "**.orderbook", IngressTopic: "ob", Priority: 2},
				{Pattern: "**.analytics", IngressTopic: "an", Priority: 3},
			},
			wantSentinel: provisioning.ErrTooManyRoutingRules,
		},
		{
			name:     "zero_maxRules_skips_count_check",
			maxRules: 0,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 1},
				{Pattern: "**.ob", IngressTopic: "ob", Priority: 2},
				{Pattern: "**.an", IngressTopic: "an", Priority: 3},
			},
		},
		{
			name: "duplicate_pattern",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 1},
				{Pattern: "**.trade", IngressTopic: "trade-alt", Priority: 2},
			},
			wantSentinel: provisioning.ErrDuplicateRoutingPattern,
		},
		{
			name: "duplicate_priority_distinct_patterns",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", IngressTopic: "t1", Priority: 1},
				{Pattern: "acme.quote", IngressTopic: "t2", Priority: 1},
			},
			wantSentinel: provisioning.ErrDuplicatePriority,
		},
		{
			name: "invalid_pattern_multiple_double_wildcards",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**.b.**", IngressTopic: "trade", Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_bad_segment_char",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.Trade!", IngressTopic: "trade", Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_uppercase_literal",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "BTC.trade", IngressTopic: "trade", Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_underscore_in_segment",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme_corp.trade", IngressTopic: "trade", Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "second_rule_invalid_reports_rule_index",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", IngressTopic: "trade", Priority: 1},
				{Pattern: "", IngressTopic: "analytics", Priority: 2},
			},
			wantSentinel: provisioning.ErrEmptyRoutingPattern,
			wantContains: "routing rule 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := provisioning.ValidateRoutingRules(tt.rules, tt.maxRules, tt.maxTopics)

			if tt.wantSentinel == nil {
				if err != nil {
					t.Errorf("ValidateRoutingRules() unexpected error: %v", err)
				}
				return
			}

			if err == nil {
				t.Errorf("ValidateRoutingRules() expected error %v, got nil", tt.wantSentinel)
				return
			}

			if !errors.Is(err, tt.wantSentinel) {
				t.Errorf("ValidateRoutingRules() error = %v, want sentinel %v", err, tt.wantSentinel)
			}

			if tt.wantContains != "" && !strings.Contains(err.Error(), tt.wantContains) {
				t.Errorf("ValidateRoutingRules() error = %q, want containing %q", err.Error(), tt.wantContains)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidateEgressDisjoint — tenant-wide bidirectional invariant
// ---------------------------------------------------------------------------

func TestValidateEgressDisjoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []provisioning.TopicRoutingRule
		wantErr bool
	}{
		{
			name: "disjoint_sets_valid",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**", IngressTopic: "trade", EgressTopics: []string{"audit"}, Priority: 1},
				{Pattern: "b.**", IngressTopic: "orders", EgressTopics: []string{"analytics"}, Priority: 2},
			},
		},
		{
			name: "new_rules_egress_hits_existing_delivery",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**", IngressTopic: "trade", Priority: 1},
				{Pattern: "b.**", IngressTopic: "orders", EgressTopics: []string{"trade"}, Priority: 2},
			},
			wantErr: true,
		},
		{
			name: "new_rules_delivery_hits_existing_egress",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**", IngressTopic: "trade", EgressTopics: []string{"orders"}, Priority: 1},
				{Pattern: "b.**", IngressTopic: "orders", Priority: 2},
			},
			wantErr: true,
		},
		{
			name:  "empty_rules_valid",
			rules: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := provisioning.ValidateEgressDisjoint(tt.rules)
			if tt.wantErr && !errors.Is(err, provisioning.ErrEgressIngressOverlap) {
				t.Errorf("ValidateEgressDisjoint() = %v, want ErrEgressIngressOverlap", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateEgressDisjoint() unexpected error: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ErrDuplicatePriority — errors.Is sentinel behavior
// ---------------------------------------------------------------------------

func TestErrDuplicatePrioritySentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rules  []provisioning.TopicRoutingRule
		target error
		want   bool
	}{
		{
			name: "errors_is_detects_duplicate_priority",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", IngressTopic: "t1", Priority: 5},
				{Pattern: "acme.quote", IngressTopic: "t2", Priority: 5},
			},
			target: provisioning.ErrDuplicatePriority,
			want:   true,
		},
		{
			name: "errors_is_false_for_distinct_priorities",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", IngressTopic: "t1", Priority: 1},
				{Pattern: "acme.quote", IngressTopic: "t2", Priority: 2},
			},
			target: provisioning.ErrDuplicatePriority,
			want:   false,
		},
		{
			name: "duplicate_priority_does_not_match_unrelated_sentinel",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", IngressTopic: "t1", Priority: 7},
				{Pattern: "acme.quote", IngressTopic: "t2", Priority: 7},
			},
			target: provisioning.ErrMissingIngressTopic,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := provisioning.ValidateRoutingRules(tt.rules, 0, 0)
			got := errors.Is(err, tt.target)
			if got != tt.want {
				t.Errorf("errors.Is(err, %v) = %v, want %v (err = %v)", tt.target, got, tt.want, err)
			}
		})
	}
}
