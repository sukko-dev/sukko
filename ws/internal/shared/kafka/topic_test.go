package kafka

import "testing"

func TestBuildTopicName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace string
		tenantID  string
		suffix    string
		expected  string
	}{
		// Standard cases
		{"prod", "sukko", "trade", "prod.sukko.trade"},
		{"dev", "sukko", "liquidity", "dev.sukko.liquidity"},
		{"stag", "acme", "metadata", "stag.acme.metadata"},
		{"local", "test", "analytics", "local.test.analytics"},

		// All 8 default suffixes
		{"prod", "sukko", "trade", "prod.sukko.trade"},
		{"prod", "sukko", "liquidity", "prod.sukko.liquidity"},
		{"prod", "sukko", "metadata", "prod.sukko.metadata"},
		{"prod", "sukko", "social", "prod.sukko.social"},
		{"prod", "sukko", "community", "prod.sukko.community"},
		{"prod", "sukko", "creation", "prod.sukko.creation"},
		{"prod", "sukko", "analytics", "prod.sukko.analytics"},
		{"prod", "sukko", "balances", "prod.sukko.balances"},

		// Edge cases
		{"", "sukko", "trade", ".sukko.trade"},         // Empty namespace
		{"prod", "", "trade", "prod..trade"},           // Empty tenant
		{"prod", "sukko", "", "prod.sukko."},           // Empty suffix
		{"PROD", "SUKKO", "TRADE", "PROD.SUKKO.TRADE"}, // Uppercase (preserved)
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			result := BuildTopicName(tt.namespace, tt.tenantID, tt.suffix)
			if result != tt.expected {
				t.Errorf("BuildTopicName(%q, %q, %q) = %q, want %q",
					tt.namespace, tt.tenantID, tt.suffix, result, tt.expected)
			}
		})
	}
}
func BenchmarkBuildTopicName(b *testing.B) {
	for b.Loop() {
		_ = BuildTopicName("prod", "sukko", "trade")
	}
}
