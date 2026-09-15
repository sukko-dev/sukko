package auth

import "testing"

func TestValidateChannelTenant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		channel  string
		tenantID string
		want     bool
	}{
		{
			name:     "valid tenant prefix",
			channel:  "acme.alerts.BTC",
			tenantID: "acme",
			want:     true,
		},
		{
			name:     "valid tenant prefix with deep channel",
			channel:  "acme.notifications.user1",
			tenantID: "acme",
			want:     true,
		},
		{
			name:     "wrong tenant prefix",
			channel:  "other.alerts.BTC",
			tenantID: "acme",
			want:     false,
		},
		{
			name:     "no dot separator",
			channel:  "acme",
			tenantID: "acme",
			want:     false,
		},
		{
			name:     "prefix without dot boundary",
			channel:  "acmex.alerts",
			tenantID: "acme",
			want:     false,
		},
		{
			name:     "empty channel",
			channel:  "",
			tenantID: "acme",
			want:     false,
		},
		{
			name:     "empty tenant",
			channel:  "acme.alerts",
			tenantID: "",
			want:     false,
		},
		{
			name:     "both empty",
			channel:  "",
			tenantID: "",
			want:     false,
		},
		{
			name:     "minimal valid channel",
			channel:  "a.b",
			tenantID: "a",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ValidateChannelTenant(tt.channel, tt.tenantID)
			if got != tt.want {
				t.Errorf("ValidateChannelTenant(%q, %q) = %v, want %v",
					tt.channel, tt.tenantID, got, tt.want)
			}
		})
	}
}
