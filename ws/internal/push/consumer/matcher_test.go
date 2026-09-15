package consumer

import "testing"

func TestMatchChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		channelKey string
		patterns   []string
		want       bool
	}{
		{
			name:       "suffix wildcard matches",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{"acme.alerts.*"},
			want:       true,
		},
		{
			name:       "suffix wildcard does not match different prefix",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{"acme.notifications.*"},
			want:       false,
		},
		{
			name:       "matches first pattern in list",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{"acme.alerts.*", "acme.notifications.*"},
			want:       true,
		},
		{
			name:       "no match for different category",
			channelKey: "acme.market.BTC.trade",
			patterns:   []string{"acme.alerts.*"},
			want:       false,
		},
		{
			name:       "empty patterns returns false",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{},
			want:       false,
		},
		{
			name:       "catch-all wildcard matches everything",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{"*"},
			want:       true,
		},
		{
			name:       "exact match",
			channelKey: "acme.alerts.BTC",
			patterns:   []string{"acme.alerts.BTC"},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := MatchChannel(tt.channelKey, tt.patterns)
			if got != tt.want {
				t.Errorf("MatchChannel(%q, %v) = %v, want %v",
					tt.channelKey, tt.patterns, got, tt.want)
			}
		})
	}
}
