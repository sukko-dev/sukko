package platform

import (
	"strings"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// =============================================================================
// validLogLevels Map Tests
// =============================================================================

func TestValidLogLevels_Contents(t *testing.T) {
	t.Parallel()
	expectedLevels := []string{"debug", "info", "warn", "error", "fatal"}

	for _, level := range expectedLevels {
		if !validLogLevels[logging.LogLevel(level)] {
			t.Errorf("validLogLevels[%q] = false, want true", level)
		}
	}

	// Verify map size
	if len(validLogLevels) != len(expectedLevels) {
		t.Errorf("len(validLogLevels) = %d, want %d", len(validLogLevels), len(expectedLevels))
	}
}

func TestValidLogLevels_InvalidLevels(t *testing.T) {
	t.Parallel()
	invalidLevels := []string{"DEBUG", "INFO", "WARN", "ERROR", "trace", ""}

	for _, level := range invalidLevels {
		if validLogLevels[logging.LogLevel(level)] {
			t.Errorf("validLogLevels[%q] = true, want false", level)
		}
	}
}

// =============================================================================
// validLogFormats Map Tests
// =============================================================================

func TestValidLogFormats_Contents(t *testing.T) {
	t.Parallel()
	expectedFormats := []string{"json", "pretty"}

	for _, format := range expectedFormats {
		if !validLogFormats[logging.LogFormat(format)] {
			t.Errorf("validLogFormats[%q] = false, want true", format)
		}
	}

	// Verify map size
	if len(validLogFormats) != len(expectedFormats) {
		t.Errorf("len(validLogFormats) = %d, want %d", len(validLogFormats), len(expectedFormats))
	}
}

func TestValidLogFormats_InvalidFormats(t *testing.T) {
	t.Parallel()
	invalidFormats := []string{"JSON", "TEXT", "PRETTY", "xml", "text", "console", ""}

	for _, format := range invalidFormats {
		if validLogFormats[logging.LogFormat(format)] {
			t.Errorf("validLogFormats[%q] = true, want false", format)
		}
	}
}

// =============================================================================
// validKafkaSASLMechanisms Map Tests
// =============================================================================

func TestValidKafkaSASLMechanisms_Contents(t *testing.T) {
	t.Parallel()
	expectedMechanisms := []string{"plain", "scram-sha-256", "scram-sha-512"}

	for _, mechanism := range expectedMechanisms {
		if !validKafkaSASLMechanisms[mechanism] {
			t.Errorf("validKafkaSASLMechanisms[%q] = false, want true", mechanism)
		}
	}

	// Verify map size
	if len(validKafkaSASLMechanisms) != len(expectedMechanisms) {
		t.Errorf("len(validKafkaSASLMechanisms) = %d, want %d", len(validKafkaSASLMechanisms), len(expectedMechanisms))
	}
}

func TestValidKafkaSASLMechanisms_InvalidMechanisms(t *testing.T) {
	t.Parallel()
	invalidMechanisms := []string{"SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN", "oauthbearer", ""}

	for _, mechanism := range invalidMechanisms {
		if validKafkaSASLMechanisms[mechanism] {
			t.Errorf("validKafkaSASLMechanisms[%q] = true, want false", mechanism)
		}
	}
}

// =============================================================================
// validateLogLevel Tests
// =============================================================================

func TestValidateLogLevel_ValidLevels(t *testing.T) {
	t.Parallel()
	validLevels := []string{"debug", "info", "warn", "error", "fatal"}

	for _, level := range validLevels {
		t.Run(level, func(t *testing.T) {
			t.Parallel()
			err := validateLogLevel(level)
			if err != nil {
				t.Errorf("validateLogLevel(%q) = %v, want nil", level, err)
			}
		})
	}
}

func TestValidateLogLevel_InvalidLevels(t *testing.T) {
	t.Parallel()
	invalidLevels := []string{"DEBUG", "trace", "invalid", "", "Info", "WARNING"}

	for _, level := range invalidLevels {
		t.Run(level, func(t *testing.T) {
			t.Parallel()
			err := validateLogLevel(level)
			if err == nil {
				t.Errorf("validateLogLevel(%q) = nil, want error", level)
			}
		})
	}
}

func TestValidateLogLevel_ErrorMessage(t *testing.T) {
	t.Parallel()
	err := validateLogLevel("invalid_level")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	errMsg := err.Error()
	// Check that the error message contains useful information
	if !strings.Contains(errMsg, "LOG_LEVEL") {
		t.Errorf("Error message should contain 'LOG_LEVEL', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "invalid_level") {
		t.Errorf("Error message should contain the invalid value, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "debug") {
		t.Errorf("Error message should list valid options, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "fatal") {
		t.Errorf("Error message should list fatal as valid option, got: %s", errMsg)
	}
}

// =============================================================================
// validateLogFormat Tests
// =============================================================================

func TestValidateLogFormat_ValidFormats(t *testing.T) {
	t.Parallel()
	validFormats := []string{"json", "pretty"}

	for _, format := range validFormats {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			err := validateLogFormat(format)
			if err != nil {
				t.Errorf("validateLogFormat(%q) = %v, want nil", format, err)
			}
		})
	}
}

func TestValidateLogFormat_InvalidFormats(t *testing.T) {
	t.Parallel()
	invalidFormats := []string{"JSON", "xml", "text", "console", "", "Text", "PRETTY"}

	for _, format := range invalidFormats {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			err := validateLogFormat(format)
			if err == nil {
				t.Errorf("validateLogFormat(%q) = nil, want error", format)
			}
		})
	}
}

func TestValidateLogFormat_ErrorMessage(t *testing.T) {
	t.Parallel()
	err := validateLogFormat("xml")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	errMsg := err.Error()
	// Check that the error message contains useful information
	if !strings.Contains(errMsg, "LOG_FORMAT") {
		t.Errorf("Error message should contain 'LOG_FORMAT', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "xml") {
		t.Errorf("Error message should contain the invalid value, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "json") {
		t.Errorf("Error message should list valid options, got: %s", errMsg)
	}
}

// =============================================================================
// validateKafkaSASLMechanism Tests
// =============================================================================

func TestValidateKafkaSASLMechanism_ValidMechanisms(t *testing.T) {
	t.Parallel()
	validMechanisms := []string{"plain", "scram-sha-256", "scram-sha-512"}

	for _, mechanism := range validMechanisms {
		t.Run(mechanism, func(t *testing.T) {
			t.Parallel()
			err := validateKafkaSASLMechanism(mechanism)
			if err != nil {
				t.Errorf("validateKafkaSASLMechanism(%q) = %v, want nil", mechanism, err)
			}
		})
	}
}

func TestValidateKafkaSASLMechanism_InvalidMechanisms(t *testing.T) {
	t.Parallel()
	invalidMechanisms := []string{"SCRAM-SHA-256", "PLAIN", "oauthbearer", "", "scram-sha-384"}

	for _, mechanism := range invalidMechanisms {
		t.Run(mechanism, func(t *testing.T) {
			t.Parallel()
			err := validateKafkaSASLMechanism(mechanism)
			if err == nil {
				t.Errorf("validateKafkaSASLMechanism(%q) = nil, want error", mechanism)
			}
		})
	}
}

func TestValidateKafkaSASLMechanism_ErrorMessage(t *testing.T) {
	t.Parallel()
	err := validateKafkaSASLMechanism("oauthbearer")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	errMsg := err.Error()
	// Check that the error message contains useful information
	if !strings.Contains(errMsg, "KAFKA_SASL_MECHANISM") {
		t.Errorf("Error message should contain 'KAFKA_SASL_MECHANISM', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "oauthbearer") {
		t.Errorf("Error message should contain the invalid value, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "scram-sha-256") {
		t.Errorf("Error message should list valid options, got: %s", errMsg)
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestValidation_WhitespaceInputs(t *testing.T) {
	t.Parallel()
	whitespaceInputs := []string{" ", "  ", "\t", "\n", " debug", "debug "}

	for _, input := range whitespaceInputs {
		t.Run("level_"+input, func(t *testing.T) {
			t.Parallel()
			err := validateLogLevel(input)
			if err == nil {
				t.Errorf("validateLogLevel(%q) should reject whitespace input", input)
			}
		})
	}
}

func TestValidation_CaseSensitivity(t *testing.T) {
	t.Parallel()
	// All validation is case-sensitive (lowercase only)
	testCases := []struct {
		name     string
		validate func(string) error
		valid    string
		invalid  string
	}{
		{"LogLevel", validateLogLevel, "debug", "DEBUG"},
		{"LogFormat", validateLogFormat, "json", "JSON"},
		{"KafkaSASL", validateKafkaSASLMechanism, "scram-sha-256", "SCRAM-SHA-256"},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_valid", func(t *testing.T) {
			t.Parallel()
			if err := tc.validate(tc.valid); err != nil {
				t.Errorf("%s(%q) = %v, want nil", tc.name, tc.valid, err)
			}
		})
		t.Run(tc.name+"_invalid", func(t *testing.T) {
			t.Parallel()
			if err := tc.validate(tc.invalid); err == nil {
				t.Errorf("%s(%q) = nil, want error (case-sensitive)", tc.name, tc.invalid)
			}
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkValidateLogLevel_Valid(b *testing.B) {
	for b.Loop() {
		_ = validateLogLevel("info")
	}
}

func BenchmarkValidateLogLevel_Invalid(b *testing.B) {
	for b.Loop() {
		_ = validateLogLevel("invalid")
	}
}

func BenchmarkValidateLogFormat_Valid(b *testing.B) {
	for b.Loop() {
		_ = validateLogFormat("json")
	}
}

func BenchmarkValidateKafkaSASLMechanism_Valid(b *testing.B) {
	for b.Loop() {
		_ = validateKafkaSASLMechanism("scram-sha-256")
	}
}

func BenchmarkMapLookup_Direct(b *testing.B) {
	for b.Loop() {
		_ = validLogLevels[logging.LogLevelInfo]
	}
}
