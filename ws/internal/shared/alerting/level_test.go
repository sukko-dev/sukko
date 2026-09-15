package alerting

import "testing"

func TestLevel_Value(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level    Level
		expected int
	}{
		{DEBUG, 0},
		{INFO, 1},
		{WARNING, 2},
		{ERROR, 3},
		{CRITICAL, 4},
		{Level("UNKNOWN"), 0}, // Unknown defaults to 0
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			t.Parallel()
			if got := tt.level.Value(); got != tt.expected {
				t.Errorf("Level(%s).Value() = %d, want %d", tt.level, got, tt.expected)
			}
		})
	}
}

func TestLevel_ShouldAlert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level    Level
		expected bool
	}{
		{DEBUG, false},
		{INFO, false},
		{WARNING, true},
		{ERROR, true},
		{CRITICAL, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			t.Parallel()
			if got := tt.level.ShouldAlert(); got != tt.expected {
				t.Errorf("Level(%s).ShouldAlert() = %v, want %v", tt.level, got, tt.expected)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected Level
	}{
		{"DEBUG", DEBUG},
		{"debug", DEBUG},
		{"INFO", INFO},
		{"info", INFO},
		{"WARNING", WARNING},
		{"warning", WARNING},
		{"WARN", WARNING},
		{"warn", WARNING},
		{"ERROR", ERROR},
		{"error", ERROR},
		{"CRITICAL", CRITICAL},
		{"critical", CRITICAL},
		{"FATAL", CRITICAL},
		{"fatal", CRITICAL},
		{"unknown", INFO}, // Unknown defaults to INFO
		{"", INFO},        // Empty defaults to INFO
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			if got := ParseLevel(tt.input); got != tt.expected {
				t.Errorf("ParseLevel(%q) = %s, want %s", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLevel_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level    Level
		expected string
	}{
		{DEBUG, "DEBUG"},
		{INFO, "INFO"},
		{WARNING, "WARNING"},
		{ERROR, "ERROR"},
		{CRITICAL, "CRITICAL"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("Level.String() = %s, want %s", got, tt.expected)
			}
		})
	}
}
