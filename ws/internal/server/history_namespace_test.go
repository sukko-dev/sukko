package server

import "testing"

// TestHistoryNamespace guards the history writer's Valkey stream/lock-key prefix derives
// from a normalized ENVIRONMENT, deliberately decoupled from KAFKA_TOPIC_NAMESPACE. Because the
// effective value is unchanged today (the deleted override was set nowhere), only this test would
// catch a future miswire pointing historyEnv at the topic namespace — in kafka mode the two differ.
func TestHistoryNamespace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		environment string
		want        string
	}{
		{"already canonical", "prod", "prod"},
		{"uppercase normalized", "Staging", "staging"},
		{"whitespace trimmed", "  dev  ", "dev"},
		{"mixed", " STAG ", "stag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := historyNamespace(tt.environment); got != tt.want {
				t.Errorf("historyNamespace(%q) = %q, want %q", tt.environment, got, tt.want)
			}
		})
	}
}
