package logging

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// LoggerConfig holds logger configuration.
type LoggerConfig struct {
	Level       LogLevel  // Minimum log level
	Format      LogFormat // Output format
	ServiceName string    // Service name for log identification (required)
	Writer      io.Writer // Output destination (nil defaults to os.Stdout)
}

// NewLogger creates a structured logger configured for Loki integration.
//
// Features:
//   - Structured JSON output (Loki-compatible)
//   - Stack traces on errors
//   - Contextual fields for filtering
//   - Timestamp in RFC3339 format
//   - Caller information for debugging
//   - Configurable service name for multi-service deployments
//
// Example:
//
//	logger := NewLogger(LoggerConfig{
//	    Level:       LogLevelInfo,
//	    Format:      LogFormatJSON,
//	    ServiceName: "ws-gateway",
//	})
//	logger.Info().
//	    Str("component", "server").
//	    Int("connections", 100).
//	    Msg("Server started")
func NewLogger(config LoggerConfig) zerolog.Logger {
	if config.ServiceName == "" {
		panic("LoggerConfig.ServiceName is required")
	}

	// Determine output writer (default: stdout)
	output := config.Writer
	if output == nil {
		output = os.Stdout
	}

	// Apply pretty format wrapper if requested
	if config.Format == LogFormatPretty {
		output = zerolog.ConsoleWriter{
			Out:        output,
			TimeFormat: time.RFC3339,
		}
	}

	// Map LogLevel to zerolog.Level
	var level zerolog.Level
	switch config.Level {
	case LogLevelDebug:
		level = zerolog.DebugLevel
	case LogLevelInfo:
		level = zerolog.InfoLevel
	case LogLevelWarn:
		level = zerolog.WarnLevel
	case LogLevelError:
		level = zerolog.ErrorLevel
	case LogLevelFatal:
		level = zerolog.FatalLevel
	default:
		level = zerolog.InfoLevel
	}

	// Per-logger level — no global state mutation
	logger := zerolog.New(output).
		Level(level).
		With().
		Timestamp().
		Caller().
		Str("service", config.ServiceName).
		Logger()

	return logger
}

// BootstrapLogger creates a minimal zerolog logger for use before configuration
// is loaded. Uses info level and JSON format with no config dependency.
func BootstrapLogger(serviceName string) zerolog.Logger {
	return zerolog.New(os.Stdout).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Str("service", serviceName).
		Logger()
}
