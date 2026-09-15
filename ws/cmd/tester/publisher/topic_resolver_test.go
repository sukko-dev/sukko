package publisher

import (
	"slices"
	"strings"
	"testing"
)

func TestTopicResolver_ExactMatch(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("prod", "acme", []RoutingRule{
		{Pattern: "general.test", Topics: []string{"general"}},
	})

	topic, err := r.Resolve("general.test")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if topic != "prod.acme.general" {
		t.Errorf("topic = %q, want %q", topic, "prod.acme.general")
	}
}

func TestTopicResolver_WildcardMatch(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("local", "tenant1", []RoutingRule{
		{Pattern: "**", Topics: []string{"default"}},
	})

	topic, err := r.Resolve("general.test")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if topic != "local.tenant1.default" {
		t.Errorf("topic = %q, want %q", topic, "local.tenant1.default")
	}
}

func TestTopicResolver_FirstMatchWins(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("dev", "t1", []RoutingRule{
		{Pattern: "room.**", Topics: []string{"rooms"}},
		{Pattern: "**", Topics: []string{"default"}},
	})

	// "room.vip" matches first rule
	topic, err := r.Resolve("room.vip")
	if err != nil {
		t.Fatalf("Resolve room.vip: %v", err)
	}
	if topic != "dev.t1.rooms" {
		t.Errorf("topic = %q, want %q", topic, "dev.t1.rooms")
	}

	// "general.test" doesn't match first, matches second
	topic, err = r.Resolve("general.test")
	if err != nil {
		t.Fatalf("Resolve general.test: %v", err)
	}
	if topic != "dev.t1.default" {
		t.Errorf("topic = %q, want %q", topic, "dev.t1.default")
	}
}

func TestTopicResolver_NoMatch(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("prod", "t1", []RoutingRule{
		{Pattern: "room.**", Topics: []string{"rooms"}},
	})

	_, err := r.Resolve("general.test")
	if err == nil {
		t.Fatal("expected error for no matching rule")
	}
	if !strings.Contains(err.Error(), "no routing rule matches") {
		t.Errorf("error = %q, want 'no routing rule matches'", err.Error())
	}
}

func TestTopicResolver_EmptyRules(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("prod", "t1", nil)

	_, err := r.Resolve("any.channel")
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
	if !strings.Contains(err.Error(), "no routing rules") {
		t.Errorf("error = %q, want 'no routing rules'", err.Error())
	}
}

func TestParseRoutingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantRules []RoutingRule
		wantErr   bool
	}{
		{
			name:  "canonical single catch-all",
			input: `{"items":[{"pattern":"**","topics":["default"],"priority":100}],"total":1,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", Topics: []string{"default"}},
			},
		},
		{
			name:  "multi-rule response",
			input: `{"items":[{"pattern":"**","topics":["default"],"priority":100},{"pattern":"room.**","topics":["rooms"],"priority":1}],"total":2,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", Topics: []string{"default"}},
				{Pattern: "room.**", Topics: []string{"rooms"}},
			},
		},
		{
			name:      "empty items array",
			input:     `{"items":[],"total":0,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{},
		},
		{
			name:    "malformed JSON",
			input:   `{not-valid`,
			wantErr: true,
		},
		{
			name:  "item with empty topics",
			input: `{"items":[{"pattern":"**","topics":[],"priority":100}],"total":1,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", Topics: []string{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules, err := ParseRoutingRules([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRoutingRules: %v", err)
			}
			if len(rules) != len(tt.wantRules) {
				t.Fatalf("len = %d, want %d", len(rules), len(tt.wantRules))
			}
			for i, want := range tt.wantRules {
				if rules[i].Pattern != want.Pattern {
					t.Errorf("rules[%d].Pattern = %q, want %q", i, rules[i].Pattern, want.Pattern)
				}
				if !slices.Equal(rules[i].Topics, want.Topics) {
					t.Errorf("rules[%d].Topics = %v, want %v", i, rules[i].Topics, want.Topics)
				}
			}
		})
	}
}

func TestTopicResolver_MultiTopicRule(t *testing.T) {
	t.Parallel()

	// Resolve() returns Topics[0] when multiple topics are present.
	// Full fan-out (all topics) is out of scope for the test publisher — filed as follow-up.
	r := NewTopicResolver("prod", "acme", []RoutingRule{
		{Pattern: "**", Topics: []string{"primary", "audit"}},
	})

	topic, err := r.Resolve("any.channel")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if topic != "prod.acme.primary" {
		t.Errorf("topic = %q, want %q", topic, "prod.acme.primary")
	}
}

func TestTopicResolver_EmptyTopicsRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []RoutingRule
		channel string
		want    string
		wantErr bool
	}{
		{
			name: "skip empty-topics rule fall through to valid rule",
			rules: []RoutingRule{
				{Pattern: "**", Topics: []string{}},
				{Pattern: "**", Topics: []string{"fallback"}},
			},
			channel: "any.channel",
			want:    "prod.acme.fallback",
		},
		{
			name: "all matching rules have empty topics returns no-match error",
			rules: []RoutingRule{
				{Pattern: "**", Topics: []string{}},
			},
			channel: "any.channel",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewTopicResolver("prod", "acme", tt.rules)
			topic, err := r.Resolve(tt.channel)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "no routing rule matches") {
					t.Errorf("error = %q, want 'no routing rule matches'", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if topic != tt.want {
				t.Errorf("topic = %q, want %q", topic, tt.want)
			}
		})
	}
}
