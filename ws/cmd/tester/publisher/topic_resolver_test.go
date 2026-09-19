package publisher

import (
	"strings"
	"testing"
)

func TestTopicResolver_ExactMatch(t *testing.T) {
	t.Parallel()

	r := NewTopicResolver("prod", "acme", []RoutingRule{
		{Pattern: "general.test", IngressTopic: "general"},
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
		{Pattern: "**", IngressTopic: "default"},
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
		{Pattern: "room.**", IngressTopic: "rooms"},
		{Pattern: "**", IngressTopic: "default"},
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
		{Pattern: "room.**", IngressTopic: "rooms"},
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
			input: `{"items":[{"pattern":"**","ingress_topic":"default","priority":100}],"total":1,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", IngressTopic: "default"},
			},
		},
		{
			name:  "multi-rule response",
			input: `{"items":[{"pattern":"**","ingress_topic":"default","priority":100},{"pattern":"room.**","ingress_topic":"rooms","priority":1}],"total":2,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", IngressTopic: "default"},
				{Pattern: "room.**", IngressTopic: "rooms"},
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
			name:  "item with empty ingress topic",
			input: `{"items":[{"pattern":"**","ingress_topic":"","priority":100}],"total":1,"limit":50,"offset":0}`,
			wantRules: []RoutingRule{
				{Pattern: "**", IngressTopic: ""},
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
				if rules[i].IngressTopic != want.IngressTopic {
					t.Errorf("rules[%d].IngressTopic = %q, want %q", i, rules[i].IngressTopic, want.IngressTopic)
				}
			}
		})
	}
}

func TestTopicResolver_MultiTopicRule(t *testing.T) {
	t.Parallel()

	// Resolve() targets the rule's ingress topic (ADR-0018): the one topic the
	// platform consumes. Egress topics are irrelevant to the test publisher.
	r := NewTopicResolver("prod", "acme", []RoutingRule{
		{Pattern: "**", IngressTopic: "primary"},
	})

	topic, err := r.Resolve("any.channel")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if topic != "prod.acme.primary" {
		t.Errorf("topic = %q, want %q", topic, "prod.acme.primary")
	}
}

func TestTopicResolver_EmptyIngressRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []RoutingRule
		channel string
		want    string
		wantErr bool
	}{
		{
			name: "skip empty-ingress rule fall through to valid rule",
			rules: []RoutingRule{
				{Pattern: "**", IngressTopic: ""},
				{Pattern: "**", IngressTopic: "fallback"},
			},
			channel: "any.channel",
			want:    "prod.acme.fallback",
		},
		{
			name: "all matching rules have empty ingress returns no-match error",
			rules: []RoutingRule{
				{Pattern: "**", IngressTopic: ""},
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
