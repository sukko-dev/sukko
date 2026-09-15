package platform_test

import (
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

func TestPodIdentityConfig_PodID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        platform.PodIdentityConfig
		wantPrefix string // just check prefix since hostname varies
		exact      string // if non-empty, require exact match
	}{
		{
			name:  "returns SUKKO_POD_ID when set",
			cfg:   platform.PodIdentityConfig{SukkoPodID: "pod-abc-123"},
			exact: "pod-abc-123",
		},
		{
			name:  "falls back to WSPodID when SukkoPodID empty",
			cfg:   platform.PodIdentityConfig{WSPodID: "ws-pod-456"},
			exact: "ws-pod-456",
		},
		{
			name:  "prefers SukkoPodID over WSPodID when both set",
			cfg:   platform.PodIdentityConfig{SukkoPodID: "new-pod", WSPodID: "old-pod"},
			exact: "new-pod",
		},
		{
			name: "falls back to hostname or unknown-pod when both empty",
			cfg:  platform.PodIdentityConfig{},
			// hostname is non-empty in normal environments; unknown-pod as fallback
			wantPrefix: "", // just verify it's non-empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cfg.PodID()
			if tt.exact != "" {
				if got != tt.exact {
					t.Errorf("PodID() = %q, want %q", got, tt.exact)
				}
				return
			}
			if got == "" {
				t.Error("PodID() returned empty string, want non-empty (hostname or unknown-pod)")
			}
		})
	}
}
