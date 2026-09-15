package logging

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// =============================================================================
// NewLogger Tests
// =============================================================================

func TestNewLogger_DefaultLevel(t *testing.T) {
	t.Parallel()
	// Test with empty level defaults to info
	logger := NewLogger(LoggerConfig{
		Level:       "", // Empty defaults to info
		Format:      LogFormatJSON,
		ServiceName: "test-service",
	})

	// Logger should be created without panic
	_ = logger
}

func TestNewLogger_AllLevels(t *testing.T) {
	t.Parallel()
	levels := []LogLevel{
		LogLevelDebug,
		LogLevelInfo,
		LogLevelWarn,
		LogLevelError,
		LogLevelFatal,
	}

	for _, level := range levels {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			logger := NewLogger(LoggerConfig{
				Level:       level,
				Format:      LogFormatJSON,
				ServiceName: "test-service",
			})

			// Logger should be created without panic
			_ = logger
		})
	}
}

func TestNewLogger_JSONFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:       LogLevelInfo,
		Format:      LogFormatJSON,
		ServiceName: "test-service",
		Writer:      &buf,
	})

	logger.Info().Str("test", "value").Msg("test message")

	output := buf.String()
	if !strings.Contains(output, `"test":"value"`) {
		t.Errorf("JSON format should contain test field: %s", output)
	}
	if !strings.Contains(output, `"message":"test message"`) {
		t.Errorf("JSON format should contain message: %s", output)
	}
}

func TestNewLogger_IncludesServiceField(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:       LogLevelInfo,
		Format:      LogFormatJSON,
		ServiceName: "ws-server",
		Writer:      &buf,
	})

	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"service":"ws-server"`) {
		t.Errorf("Logger should include service field: %s", output)
	}
}

func TestNewLogger_CustomServiceName(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:       LogLevelInfo,
		Format:      LogFormatJSON,
		ServiceName: "ws-gateway",
		Writer:      &buf,
	})

	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"service":"ws-gateway"`) {
		t.Errorf("Logger should include custom service field: %s", output)
	}
}

func TestNewLogger_PanicsOnEmptyServiceName(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewLogger should panic on empty ServiceName")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value should be string, got %T", r)
		}
		if !strings.Contains(msg, "ServiceName") {
			t.Errorf("panic message should mention ServiceName: %s", msg)
		}
	}()

	NewLogger(LoggerConfig{})
}

func TestNewLogger_LevelFiltering(t *testing.T) {
	t.Parallel()

	// Create two loggers at different levels writing to separate buffers
	var debugBuf, warnBuf bytes.Buffer
	debugLogger := NewLogger(LoggerConfig{
		Level:       LogLevelDebug,
		Format:      LogFormatJSON,
		ServiceName: "test-debug",
		Writer:      &debugBuf,
	})
	warnLogger := NewLogger(LoggerConfig{
		Level:       LogLevelWarn,
		Format:      LogFormatJSON,
		ServiceName: "test-warn",
		Writer:      &warnBuf,
	})

	tests := []struct {
		name     string
		logger   zerolog.Logger
		buf      *bytes.Buffer
		logFunc  func(zerolog.Logger)
		expectOK bool
	}{
		{"debug_on_debug_logger", debugLogger, &debugBuf, func(l zerolog.Logger) { l.Debug().Msg("debug msg") }, true},
		{"info_on_debug_logger", debugLogger, &debugBuf, func(l zerolog.Logger) { l.Info().Msg("info msg") }, true},
		{"debug_on_warn_logger", warnLogger, &warnBuf, func(l zerolog.Logger) { l.Debug().Msg("debug msg") }, false},
		{"info_on_warn_logger", warnLogger, &warnBuf, func(l zerolog.Logger) { l.Info().Msg("info msg") }, false},
		{"warn_on_warn_logger", warnLogger, &warnBuf, func(l zerolog.Logger) { l.Warn().Msg("warn msg") }, true},
	}

	for _, tt := range tests { //nolint:paralleltest // subtests share debugBuf/warnBuf — cannot run in parallel
		t.Run(tt.name, func(t *testing.T) {
			tt.buf.Reset()
			tt.logFunc(tt.logger)
			hasOutput := tt.buf.Len() > 0
			if hasOutput != tt.expectOK {
				t.Errorf("expected output=%v, got output=%v (buf=%q)", tt.expectOK, hasOutput, tt.buf.String())
			}
		})
	}
}

func TestNewLogger_DefaultWriter(t *testing.T) {
	t.Parallel()

	// Should not panic with nil Writer (defaults to stdout)
	logger := NewLogger(LoggerConfig{
		Level:       LogLevelInfo,
		Format:      LogFormatJSON,
		ServiceName: "test-service",
	})
	_ = logger
}

func TestNewLogger_PrettyFormatWithWriter(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:       LogLevelInfo,
		Format:      LogFormatPretty,
		ServiceName: "test-service",
		Writer:      &buf,
	})

	logger.Info().Msg("pretty test")

	if buf.Len() == 0 {
		t.Error("Pretty format with Writer should produce output in the buffer")
	}
}

// =============================================================================
// RecoverPanic Tests
// =============================================================================

func TestRecoverPanic_CapturesPanic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	func() {
		defer RecoverPanic(logger, "testGoroutine", nil)
		panic("test panic")
	}()

	output := buf.String()
	if !strings.Contains(output, "test panic") {
		t.Errorf("Should capture panic value: %s", output)
	}
	if !strings.Contains(output, "testGoroutine") {
		t.Errorf("Should include goroutine name: %s", output)
	}
	if !strings.Contains(output, "stack_trace") {
		t.Errorf("Should include stack trace: %s", output)
	}
}

func TestRecoverPanic_WithContextFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	func() {
		defer RecoverPanic(logger, "writePump", map[string]any{
			"client_id": 12345,
		})
		panic("write failed")
	}()

	output := buf.String()
	if !strings.Contains(output, "client_id") {
		t.Errorf("Should include context fields: %s", output)
	}
	if !strings.Contains(output, "12345") {
		t.Errorf("Should include client_id value: %s", output)
	}
}

func TestRecoverPanic_NoPanic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	func() {
		defer RecoverPanic(logger, "normalFunction", nil)
		// No panic - normal return
	}()

	output := buf.String()
	if output != "" {
		t.Errorf("Should not log when no panic occurs: %s", output)
	}
}

func TestRecoverPanic_ServerStaysRunning(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	// Track that goroutine completed
	done := make(chan bool, 1)

	// Simulate a goroutine that panics but is recovered
	// NOTE: RecoverPanic must be used DIRECTLY as a defer, not wrapped
	go func() {
		defer func() { done <- true }()                    // This runs after RecoverPanic
		defer RecoverPanic(logger, "workerGoroutine", nil) // This catches panic
		panic("worker panic")
	}()

	// Wait for goroutine to complete
	select {
	case <-done:
		// Success - goroutine recovered and continued
	case <-time.After(1 * time.Second):
		t.Error("Goroutine should have recovered and signaled completion")
	}

	// Verify the panic was logged
	output := buf.String()
	if !strings.Contains(output, "worker panic") {
		t.Errorf("Should log panic: %s", output)
	}
}

// =============================================================================
// LoggerConfig Tests
// =============================================================================

func TestLoggerConfig_Fields(t *testing.T) {
	t.Parallel()
	config := LoggerConfig{
		Level:  LogLevelDebug,
		Format: LogFormatPretty,
	}

	if config.Level != LogLevelDebug {
		t.Errorf("Level: got %s, want debug", config.Level)
	}
	if config.Format != LogFormatPretty {
		t.Errorf("Format: got %s, want pretty", config.Format)
	}
}

// =============================================================================
// LogLevel Tests
// =============================================================================

func TestLogLevel_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level    LogLevel
		expected string
	}{
		{LogLevelDebug, "debug"},
		{LogLevelInfo, "info"},
		{LogLevelWarn, "warn"},
		{LogLevelError, "error"},
		{LogLevelFatal, "fatal"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if string(tt.level) != tt.expected {
				t.Errorf("LogLevel: got %s, want %s", tt.level, tt.expected)
			}
		})
	}
}

// =============================================================================
// LogFormat Tests
// =============================================================================

func TestLogFormat_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		format   LogFormat
		expected string
	}{
		{LogFormatJSON, "json"},
		{LogFormatPretty, "pretty"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if string(tt.format) != tt.expected {
				t.Errorf("LogFormat: got %s, want %s", tt.format, tt.expected)
			}
		})
	}
}
