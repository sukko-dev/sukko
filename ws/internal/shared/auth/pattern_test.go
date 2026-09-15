package auth

import "testing"

func TestMatchWildcard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		// Exact match
		{name: "exact match", pattern: "foo.bar", value: "foo.bar", want: true},
		{name: "exact match fails", pattern: "foo.bar", value: "foo.baz", want: false},

		// Catch-all wildcard
		{name: "catch-all matches anything", pattern: "*", value: "anything", want: true},
		{name: "catch-all matches complex", pattern: "*", value: "foo.bar.baz", want: true},
		{name: "catch-all matches empty", pattern: "*", value: "", want: true},

		// Prefix wildcard: *.suffix
		{name: "prefix wildcard matches", pattern: "*.trade", value: "BTC.trade", want: true},
		{name: "prefix wildcard matches longer", pattern: "*.trade", value: "prefix.BTC.trade", want: true},
		{name: "prefix wildcard fails wrong suffix", pattern: "*.trade", value: "BTC.liquidity", want: false},
		{name: "prefix wildcard fails no suffix", pattern: "*.trade", value: "trade", want: false},

		// Suffix wildcard: prefix.*
		{name: "suffix wildcard matches", pattern: "sukko.*", value: "sukko.trades", want: true},
		{name: "suffix wildcard matches longer", pattern: "sukko.*", value: "sukko.trades.live", want: true},
		{name: "suffix wildcard fails wrong prefix", pattern: "sukko.*", value: "acme.trades", want: false},
		{name: "suffix wildcard fails no prefix", pattern: "sukko.*", value: "trades", want: false},

		// Middle wildcard: prefix*suffix
		{name: "middle wildcard matches", pattern: "foo*bar", value: "fooXXXbar", want: true},
		{name: "middle wildcard matches empty middle", pattern: "foo*bar", value: "foobar", want: true},
		{name: "middle wildcard fails wrong prefix", pattern: "foo*bar", value: "bazXXXbar", want: false},
		{name: "middle wildcard fails wrong suffix", pattern: "foo*bar", value: "fooXXXbaz", want: false},

		// Edge cases
		{name: "empty pattern matches empty value", pattern: "", value: "", want: true},
		{name: "empty pattern fails non-empty value", pattern: "", value: "foo", want: false},
		{name: "pattern equals asterisk only", pattern: "*", value: "*", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := MatchWildcard(tt.pattern, tt.value)
			if got != tt.want {
				t.Errorf("MatchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.value, got, tt.want)
			}
		})
	}
}

func TestMatchAnyWildcard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		patterns []string
		value    string
		want     bool
	}{
		{
			name:     "matches first pattern",
			patterns: []string{"*.trade", "*.liquidity"},
			value:    "BTC.trade",
			want:     true,
		},
		{
			name:     "matches second pattern",
			patterns: []string{"*.trade", "*.liquidity"},
			value:    "ETH.liquidity",
			want:     true,
		},
		{
			name:     "matches none",
			patterns: []string{"*.trade", "*.liquidity"},
			value:    "BTC.order",
			want:     false,
		},
		{
			name:     "empty patterns matches nothing",
			patterns: []string{},
			value:    "anything",
			want:     false,
		},
		{
			name:     "nil patterns matches nothing",
			patterns: nil,
			value:    "anything",
			want:     false,
		},
		{
			name:     "exact and wildcard mix",
			patterns: []string{"exact.match", "prefix.*"},
			value:    "prefix.something",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := MatchAnyWildcard(tt.patterns, tt.value)
			if got != tt.want {
				t.Errorf("MatchAnyWildcard(%v, %q) = %v, want %v", tt.patterns, tt.value, got, tt.want)
			}
		})
	}
}
