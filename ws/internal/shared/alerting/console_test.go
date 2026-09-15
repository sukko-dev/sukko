package alerting

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewConsoleAlerter(t *testing.T) {
	t.Parallel()
	alerter := NewConsoleAlerter()

	if alerter == nil {
		t.Fatal("NewConsoleAlerter should return non-nil")
	}
}

func TestConsoleAlerter_Alert(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	alerter := NewConsoleAlerterWithWriter(&buf)

	alerter.Alert(ERROR, "Test error message", map[string]any{
		"key": "value",
	})

	output := buf.String()

	// Verify output contains expected content
	if !strings.Contains(output, "ALERT") {
		t.Error("Output should contain 'ALERT'")
	}
	if !strings.Contains(output, "ERROR") {
		t.Error("Output should contain 'ERROR'")
	}
	if !strings.Contains(output, "Test error message") {
		t.Error("Output should contain the message")
	}
	if !strings.Contains(output, "key") {
		t.Error("Output should contain metadata key")
	}
	if !strings.Contains(output, "value") {
		t.Error("Output should contain metadata value")
	}
}

func TestConsoleAlerter_NoMetadata(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	alerter := NewConsoleAlerterWithWriter(&buf)

	alerter.Alert(INFO, "Test message", nil)

	output := buf.String()

	// Verify output contains alert but no "Metadata:" section
	if !strings.Contains(output, "INFO") {
		t.Error("Output should contain 'INFO'")
	}
	if !strings.Contains(output, "Test message") {
		t.Error("Output should contain the message")
	}
	if strings.Contains(output, "Metadata:") {
		t.Error("Output should NOT contain 'Metadata:' when nil metadata passed")
	}
}

func TestConsoleAlerter_EmptyMetadata(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	alerter := NewConsoleAlerterWithWriter(&buf)

	alerter.Alert(INFO, "Test message", map[string]any{})

	output := buf.String()

	// Verify output contains alert but no "Metadata:" section for empty map
	if !strings.Contains(output, "INFO") {
		t.Error("Output should contain 'INFO'")
	}
	if strings.Contains(output, "Metadata:") {
		t.Error("Output should NOT contain 'Metadata:' when empty metadata passed")
	}
}

func TestConsoleAlerter_AllLevels(t *testing.T) {
	t.Parallel()
	levels := []Level{DEBUG, INFO, WARNING, ERROR, CRITICAL}

	for _, level := range levels {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			alerter := NewConsoleAlerterWithWriter(&buf)

			alerter.Alert(level, "test message", nil)

			output := buf.String()

			// Verify each level is properly formatted in output
			if !strings.Contains(output, string(level)) {
				t.Errorf("Output should contain level %q, got: %s", level, output)
			}
			if !strings.Contains(output, "test message") {
				t.Errorf("Output should contain message, got: %s", output)
			}
		})
	}
}

func TestConsoleAlerter_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	var alerter Alerter = NewConsoleAlerterWithWriter(&buf)

	alerter.Alert(WARNING, "Test message", map[string]any{"server": "ws-1"})

	output := buf.String()

	// Verify interface implementation produces expected output
	if !strings.Contains(output, "WARNING") {
		t.Error("Output should contain 'WARNING'")
	}
	if !strings.Contains(output, "Test message") {
		t.Error("Output should contain the message")
	}
	if !strings.Contains(output, "server") {
		t.Error("Output should contain metadata key")
	}
}
