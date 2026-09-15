package platform

import (
	"strings"
	"testing"
)

func TestWebhookInternalTokenConfig_Validate(t *testing.T) {
	t.Parallel()
	validToken := strings.Repeat("a", 32)
	tests := []struct {
		name      string
		token     string
		wantErr   bool
		errSubstr string
	}{
		{"valid 32-char token", validToken, false, ""},
		{"valid long token", strings.Repeat("x", 64), false, ""},
		{"empty token", "", true, "WEBHOOK_INTERNAL_TOKEN is required"},
		{"too short (31 chars)", strings.Repeat("a", 31), true, "must be at least 32 characters"},
		{"exactly 32 chars", strings.Repeat("b", 32), false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := WebhookInternalTokenConfig{WebhookInternalToken: tt.token}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errSubstr != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			}
		})
	}
}
