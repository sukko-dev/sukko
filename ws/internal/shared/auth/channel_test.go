package auth

import (
	"errors"
	"testing"
)

func TestExtractChannelTenant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		channel   string
		separator string
		expected  string
	}{
		{"acme.BTC.trade", ".", "acme"},
		{"globex.notifications", ".", "globex"},
		{"channel", ".", ""},
		{"", ".", ""},
		{"acme/BTC/trade", "/", "acme"},
	}

	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			t.Parallel()
			result := extractChannelTenant(tt.channel, tt.separator)
			if result != tt.expected {
				t.Errorf("extractChannelTenant(%q, %q) = %q, want %q",
					tt.channel, tt.separator, result, tt.expected)
			}
		})
	}
}

func TestIsSharedChannel(t *testing.T) {
	t.Parallel()
	sharedPatterns := []string{"system.*", "broadcast.*", "*.global"}

	tests := []struct {
		channel  string
		expected bool
	}{
		{"system.notifications", true},
		{"system.alerts", true},
		{"broadcast.all", true},
		{"acme.global", true},
		{"acme.BTC.trade", false},
		{"user.notifications", false},
		{"system", false}, // Exact "system" doesn't match "system.*"
	}

	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			t.Parallel()
			result := IsSharedChannel(tt.channel, sharedPatterns)
			if result != tt.expected {
				t.Errorf("IsSharedChannel(%q) = %v, want %v", tt.channel, result, tt.expected)
			}
		})
	}
}

func TestMatchWildcard_SharedChannel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern  string
		value    string
		expected bool
	}{
		// Exact match
		{"system.broadcast", "system.broadcast", true},
		{"system.broadcast", "system.other", false},

		// Trailing wildcard
		{"system.*", "system.notifications", true},
		{"system.*", "system.alerts", true},
		{"system.*", "other.notifications", false},
		{"system.*", "system", false},

		// Leading wildcard
		{"*.broadcast", "system.broadcast", true},
		{"*.broadcast", "acme.broadcast", true},
		{"*.broadcast", "system.notifications", false},
	}

	for _, tt := range tests {
		name := tt.pattern + "_" + tt.value
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result := MatchWildcard(tt.pattern, tt.value)
			if result != tt.expected {
				t.Errorf("MatchWildcard(%q, %q) = %v, want %v",
					tt.pattern, tt.value, result, tt.expected)
			}
		})
	}
}

func TestValidateInternalChannel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		channel string
		wantErr bool
	}{
		{
			name:    "valid 2-part channel",
			channel: "acme.trade",
			wantErr: false,
		},
		{
			name:    "valid 3-part channel",
			channel: "acme.BTC.trade",
			wantErr: false,
		},
		{
			name:    "valid 4-part channel",
			channel: "acme.user123.private.balances",
			wantErr: false,
		},
		{
			name:    "empty channel",
			channel: "",
			wantErr: true,
		},
		{
			name:    "only 1 part",
			channel: "trade",
			wantErr: true,
		},
		{
			name:    "empty first part",
			channel: ".trade",
			wantErr: true,
		},
		{
			name:    "empty middle part",
			channel: "acme..trade",
			wantErr: true,
		},
		{
			name:    "empty last part",
			channel: "acme.trade.",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateInternalChannel(tt.channel)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateInternalChannel(%q) error = %v, wantErr %v", tt.channel, err, tt.wantErr)
			}
		})
	}
}

func TestIsValidInternalChannel(t *testing.T) {
	t.Parallel()
	// Valid channels (2+ parts)
	if !IsValidInternalChannel("acme.trade") {
		t.Error("IsValidInternalChannel('acme.trade') = false, want true")
	}
	if !IsValidInternalChannel("acme.BTC.trade") {
		t.Error("IsValidInternalChannel('acme.BTC.trade') = false, want true")
	}

	// Invalid channels (1 part)
	if IsValidInternalChannel("trade") {
		t.Error("IsValidInternalChannel('trade') = true, want false")
	}
}

func TestValidateChannelSegments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		channel      string
		wantErr      bool
		wantSentinel error
	}{
		{
			name:    "valid single segment",
			channel: "trade",
			wantErr: false,
		},
		{
			name:    "valid two segments",
			channel: "acme.trade",
			wantErr: false,
		},
		{
			name:    "valid three segments",
			channel: "acme.BTC.trade",
			wantErr: false,
		},
		{
			name:         "empty channel",
			channel:      "",
			wantErr:      true,
			wantSentinel: ErrEmptyChannel,
		},
		{
			name:         "empty first segment",
			channel:      ".acme.trade",
			wantErr:      true,
			wantSentinel: ErrEmptyChannelSegment,
		},
		{
			name:         "empty middle segment",
			channel:      "acme..trade",
			wantErr:      true,
			wantSentinel: ErrEmptyChannelSegment,
		},
		{
			name:         "empty last segment",
			channel:      "acme.trade.",
			wantErr:      true,
			wantSentinel: ErrEmptyChannelSegment,
		},
		{
			name:         "multiple empty segments",
			channel:      "acme...trade",
			wantErr:      true,
			wantSentinel: ErrEmptyChannelSegment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateChannelSegments(tt.channel)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateChannelSegments(%q) error = %v, wantErr %v", tt.channel, err, tt.wantErr)
			}
			if tt.wantSentinel != nil && err != nil && !errors.Is(err, tt.wantSentinel) {
				t.Errorf("ValidateChannelSegments(%q) error = %v, want sentinel %v", tt.channel, err, tt.wantSentinel)
			}
		})
	}
}

func TestValidateInternalChannel_SentinelErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		channel      string
		wantSentinel error
	}{
		{"empty", "", ErrEmptyChannel},
		{"empty_segment", "acme..trade", ErrEmptyChannelSegment},
		{"single_part", "trade", ErrInsufficientChannelParts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateInternalChannel(tt.channel)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tt.wantSentinel) {
				t.Errorf("ValidateInternalChannel(%q) error = %v, want sentinel %v", tt.channel, err, tt.wantSentinel)
			}
		})
	}
}
