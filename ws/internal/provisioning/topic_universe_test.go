package provisioning

import (
	"slices"
	"testing"
)

// TestConsumeTopicSuffixes pins the ingress/egress split at the consume-set
// boundary: the platform consumes (and therefore delivers to subscribers) ONLY
// the tenant default topic plus each rule's ingress topic. Egress topics are
// written to but MUST NEVER enter the consume set — a consumed egress topic
// re-delivers every copy of a message to every subscriber (duplicate delivery).
func TestConsumeTopicSuffixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		rules []TopicRoutingRule
		want  []string
	}{
		{
			name:  "no rules: default topic only (rule-less tenants still ingest)",
			rules: nil,
			want:  []string{"default"},
		},
		{
			name: "single-topic rule extends the set",
			rules: []TopicRoutingRule{
				{Pattern: "**", IngressTopic: "trades", Priority: 1},
			},
			want: []string{"default", "trades"},
		},
		{
			name: "egress topics are NOT consumed",
			rules: []TopicRoutingRule{
				{Pattern: "**", IngressTopic: "trades", EgressTopics: []string{"audit"}, Priority: 1},
			},
			want: []string{"default", "trades"},
		},
		{
			name: "ingress topics deduped across rules; egress never enters",
			rules: []TopicRoutingRule{
				{Pattern: "a.**", IngressTopic: "trades", EgressTopics: []string{"audit"}, Priority: 1},
				{Pattern: "b.**", IngressTopic: "trades", EgressTopics: []string{"analytics"}, Priority: 2},
				{Pattern: "c.**", IngressTopic: "orders", Priority: 3},
			},
			want: []string{"default", "trades", "orders"},
		},
		{
			name: "ingress topic 'default' does not duplicate the always-on default",
			rules: []TopicRoutingRule{
				{Pattern: "**", IngressTopic: "default", EgressTopics: []string{"audit"}, Priority: 1},
			},
			want: []string{"default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ConsumeTopicSuffixes(tt.rules)
			if !slices.Equal(got, tt.want) {
				t.Errorf("ConsumeTopicSuffixes() = %v, want %v", got, tt.want)
			}
		})
	}
}
