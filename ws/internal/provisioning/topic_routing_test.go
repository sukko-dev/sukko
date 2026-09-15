package provisioning_test

import (
	"errors"
	"strings"
	"testing"
	"time"

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
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
			},
		},
		{
			name: "valid_multiple_rules",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "crypto.**.trade", Topics: []string{"crypto-trade"}, Priority: 1},
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 2},
				{Pattern: "**.analytics", Topics: []string{"analytics"}, Priority: 3},
			},
		},
		{
			name: "valid_multi_topic_fan_out",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade", "trade-replica"}, Priority: 1},
			},
		},
		{
			name: "valid_exact_literal_segments",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "btc.trade", Topics: []string{"btc-trade"}, Priority: 1},
			},
		},
		{
			name: "valid_single_wildcard",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.*.trade", Topics: []string{"trades"}, Priority: 1},
			},
		},
		{
			name: "valid_double_wildcard",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.**.trade", Topics: []string{"trades"}, Priority: 1},
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
				{Pattern: "", Topics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrEmptyRoutingPattern,
		},
		{
			name: "empty_topics_slice",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{}, Priority: 1},
			},
			wantSentinel: provisioning.ErrEmptyTopics,
		},
		{
			name: "nil_topics_slice",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: nil, Priority: 1},
			},
			wantSentinel: provisioning.ErrEmptyTopics,
		},
		{
			name:      "too_many_topics",
			maxTopics: 2,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"t1", "t2", "t3"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrTooManyTopics,
		},
		{
			name:      "zero_maxTopicsPerRule_skips_limit",
			maxTopics: 0,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"t1", "t2", "t3", "t4", "t5"}, Priority: 1},
			},
		},
		{
			name:     "too_many_rules",
			maxRules: 2,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
				{Pattern: "**.orderbook", Topics: []string{"ob"}, Priority: 2},
				{Pattern: "**.analytics", Topics: []string{"an"}, Priority: 3},
			},
			wantSentinel: provisioning.ErrTooManyRoutingRules,
		},
		{
			name:     "zero_maxRules_skips_count_check",
			maxRules: 0,
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
				{Pattern: "**.ob", Topics: []string{"ob"}, Priority: 2},
				{Pattern: "**.an", Topics: []string{"an"}, Priority: 3},
			},
		},
		{
			name: "duplicate_pattern",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
				{Pattern: "**.trade", Topics: []string{"trade-alt"}, Priority: 2},
			},
			wantSentinel: provisioning.ErrDuplicateRoutingPattern,
		},
		{
			name: "duplicate_priority_distinct_patterns",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", Topics: []string{"t1"}, Priority: 1},
				{Pattern: "acme.quote", Topics: []string{"t2"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrDuplicatePriority,
		},
		{
			name: "invalid_pattern_multiple_double_wildcards",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**.b.**", Topics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_bad_segment_char",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.Trade!", Topics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_uppercase_literal",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "BTC.trade", Topics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "invalid_pattern_underscore_in_segment",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme_corp.trade", Topics: []string{"trade"}, Priority: 1},
			},
			wantSentinel: provisioning.ErrInvalidRoutingPattern,
		},
		{
			name: "second_rule_invalid_reports_rule_index",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 1},
				{Pattern: "", Topics: []string{"analytics"}, Priority: 2},
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
// ResolveTopics
// ---------------------------------------------------------------------------

func TestResolveTopics(t *testing.T) {
	t.Parallel()

	// Shared ordered ruleset (caller is responsible for priority ordering).
	// Rules are listed in ascending priority order (lower = higher priority),
	// as a real caller would sort them before calling ResolveTopics.
	sharedRules := []provisioning.TopicRoutingRule{
		{Pattern: "acme.crypto.**.trade", Topics: []string{"crypto-trade"}, Priority: 1},
		{Pattern: "acme.nft.**.trade", Topics: []string{"nft-trade"}, Priority: 2},
		{Pattern: "**.trade", Topics: []string{"trade"}, Priority: 3},
		{Pattern: "**.analytics", Topics: []string{"analytics"}, Priority: 4},
	}

	tests := []struct {
		name    string
		rules   []provisioning.TopicRoutingRule
		channel string
		want    []string
		wantNil bool
	}{
		{
			name:    "specific_match_returns_correct_topics",
			rules:   sharedRules,
			channel: "acme.crypto.btc.trade",
			want:    []string{"crypto-trade"},
		},
		{
			name:    "nft_match",
			rules:   sharedRules,
			channel: "acme.nft.punk.trade",
			want:    []string{"nft-trade"},
		},
		{
			name:    "catch_all_trade",
			rules:   sharedRules,
			channel: "acme.forex.eur.trade",
			want:    []string{"trade"},
		},
		{
			name:    "analytics_match",
			rules:   sharedRules,
			channel: "acme.crypto.btc.analytics",
			want:    []string{"analytics"},
		},
		{
			name:    "no_match_returns_nil",
			rules:   sharedRules,
			channel: "acme.unknown.data",
			wantNil: true,
		},
		{
			name:    "empty_rules_returns_nil",
			rules:   []provisioning.TopicRoutingRule{},
			channel: "acme.btc.trade",
			wantNil: true,
		},
		{
			name: "fan_out_multiple_topics",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "**.trade", Topics: []string{"trade", "trade-replica"}, Priority: 1},
			},
			channel: "acme.btc.trade",
			want:    []string{"trade", "trade-replica"},
		},
		{
			// Rules are iterated in slice order; caller sorts by priority.
			// The first rule in slice order wins when multiple rules match.
			name: "first_priority_wins_when_multiple_could_match",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.**.trade", Topics: []string{"high-priority"}, Priority: 1},
				{Pattern: "**.trade", Topics: []string{"low-priority"}, Priority: 2},
			},
			channel: "acme.nyse.trade",
			want:    []string{"high-priority"},
		},
		{
			name: "second_rule_matches_when_first_does_not",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.quote", Topics: []string{"quotes"}, Priority: 1},
				{Pattern: "acme.**.trade", Topics: []string{"trades"}, Priority: 2},
			},
			channel: "acme.nyse.trade",
			want:    []string{"trades"},
		},
		{
			// An invalid-pattern rule must be silently skipped;
			// subsequent valid rules must still be evaluated.
			name: "invalid_pattern_rule_skipped_silently",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "a.**.b.**", Topics: []string{"bad"}, Priority: 1},
				{Pattern: "acme.trade", Topics: []string{"trades"}, Priority: 2},
			},
			channel: "acme.trade",
			want:    []string{"trades"},
		},
		{
			// Single-wildcard pattern requires exactly one segment per position —
			// a 2-segment channel must NOT match a 3-segment pattern.
			name: "single_wildcard_segment_count_mismatch_no_match",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.*.trade", Topics: []string{"trades"}, Priority: 1},
			},
			channel: "acme.trade",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := provisioning.ResolveTopics(tt.rules, tt.channel)
			if err != nil {
				t.Fatalf("ResolveTopics() unexpected error: %v", err)
			}

			if tt.wantNil {
				if got != nil {
					t.Errorf("ResolveTopics() = %v, want nil", got)
				}
				return
			}

			if len(got) != len(tt.want) {
				t.Fatalf("ResolveTopics() returned %d topics, want %d; got %v", len(got), len(tt.want), got)
			}
			for i, topic := range got {
				if topic != tt.want[i] {
					t.Errorf("ResolveTopics()[%d] = %q, want %q", i, topic, tt.want[i])
				}
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
				{Pattern: "acme.trade", Topics: []string{"t1"}, Priority: 5},
				{Pattern: "acme.quote", Topics: []string{"t2"}, Priority: 5},
			},
			target: provisioning.ErrDuplicatePriority,
			want:   true,
		},
		{
			name: "errors_is_false_for_distinct_priorities",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", Topics: []string{"t1"}, Priority: 1},
				{Pattern: "acme.quote", Topics: []string{"t2"}, Priority: 2},
			},
			target: provisioning.ErrDuplicatePriority,
			want:   false,
		},
		{
			name: "duplicate_priority_does_not_match_unrelated_sentinel",
			rules: []provisioning.TopicRoutingRule{
				{Pattern: "acme.trade", Topics: []string{"t1"}, Priority: 7},
				{Pattern: "acme.quote", Topics: []string{"t2"}, Priority: 7},
			},
			target: provisioning.ErrEmptyTopics,
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

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

func BenchmarkResolveTopics(b *testing.B) {
	// Build a 100-rule ruleset where the matching rule is last (worst-case scan).
	// Non-matching rules use a literal pattern so each comparison is cheap but
	// still exercises real pattern evaluation logic.
	const numRules = 100
	rules := make([]provisioning.TopicRoutingRule, numRules)
	for i := range numRules {
		rules[i] = provisioning.TopicRoutingRule{
			// Distinct literal patterns that will NOT match the benchmark channel.
			Pattern:  "no-match.segment",
			Topics:   []string{"bucket"},
			Priority: i + 1,
		}
	}
	// The last rule uses a ** wildcard and matches the benchmark channel — it is
	// reached only after all 99 non-matching rules are evaluated.
	rules[numRules-1] = provisioning.TopicRoutingRule{
		Pattern:  "bench.**.event",
		Topics:   []string{"events", "audit"},
		Priority: numRules,
	}

	channel := "bench.us.east.event"

	b.ResetTimer()

	n := 0
	for b.Loop() {
		_, _ = provisioning.ResolveTopics(rules, channel)
		n++
	}

	if n > 0 {
		perOp := b.Elapsed() / time.Duration(n)
		if perOp > time.Millisecond {
			b.Fatalf("ResolveTopics too slow: %v per op (want ≤1ms)", perOp)
		}
	}
}
