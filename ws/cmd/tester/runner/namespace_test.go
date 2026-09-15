package runner

import "testing"

// TestResolveRunNamespace covers the per-run namespace precedence and normalization.
// A precedence/normalization bug here would surface only as a silent kafka-ingest e2e timeout
// (publisher/consumer namespace mismatch), so it MUST be caught by a fast unit test.
func TestResolveRunNamespace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		perRequest    string
		testerDefault string
		want          string
	}{
		{"per-request wins", "prod", "dev", "prod"},
		{"falls back to tester env", "", "dev", "dev"},
		{"both empty", "", "", ""},
		{"per-request normalized", " PROD ", "dev", "prod"},
		{"tester default normalized", "", " Dev ", "dev"},
		{"per-request wins over default even when default set", "stag", "prod", "stag"},
		{"whitespace-only per-request does not wipe valid default", "   ", "dev", "dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveRunNamespace(tt.perRequest, tt.testerDefault); got != tt.want {
				t.Errorf("resolveRunNamespace(%q, %q) = %q, want %q",
					tt.perRequest, tt.testerDefault, got, tt.want)
			}
		})
	}
}
