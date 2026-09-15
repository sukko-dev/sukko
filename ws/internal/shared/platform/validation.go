package platform

import (
	"fmt"
	"time"

	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// Shared validation bounds used across gateway, server, and provisioning configs.
const (
	// Size bounds.
	MinFrameSize      = 1024
	MaxFrameSizeLimit = 10 * 1024 * 1024 // 10MB
	MinMemoryLimit    = 64 * 1024 * 1024 // 64MB
	MinStorageBytes   = 1048576          // 1MB
	MinByteRate       = 1024
	MinMaxGoroutines  = 100
	MaxPort           = 65535

	// Duration bounds for HTTP timeouts.
	MinTimeout          = time.Second
	MaxReadWriteTimeout = 120 * time.Second
	MaxIdleTimeout      = 300 * time.Second
	MaxDialTimeout      = 60 * time.Second
	MaxMessageTimeout   = 300 * time.Second
)

// validLogLevels derived from logging.LogLevel constants (single source of truth).
var validLogLevels = map[logging.LogLevel]bool{
	logging.LogLevelDebug: true,
	logging.LogLevelInfo:  true,
	logging.LogLevelWarn:  true,
	logging.LogLevelError: true,
	logging.LogLevelFatal: true,
}

// validLogFormats derived from logging.LogFormat constants (single source of truth).
var validLogFormats = map[logging.LogFormat]bool{
	logging.LogFormatJSON:   true,
	logging.LogFormatPretty: true,
}

var validKafkaSASLMechanisms = map[string]bool{
	kafkashared.MechanismPLAIN:       true,
	kafkashared.MechanismSCRAMSHA256: true,
	kafkashared.MechanismSCRAMSHA512: true,
}

// validateLogLevel checks if the log level is valid.
func validateLogLevel(level string) error {
	if !validLogLevels[logging.LogLevel(level)] {
		return fmt.Errorf("LOG_LEVEL must be one of: debug, info, warn, error, fatal (got: %s)", level)
	}
	return nil
}

// validateLogFormat checks if the log format is valid.
func validateLogFormat(format string) error {
	if !validLogFormats[logging.LogFormat(format)] {
		return fmt.Errorf("LOG_FORMAT must be one of: json, pretty (got: %s)", format)
	}
	return nil
}

// validateKafkaSASLMechanism checks if the SASL mechanism is valid.
func validateKafkaSASLMechanism(mechanism string) error {
	if !validKafkaSASLMechanisms[mechanism] {
		return fmt.Errorf("KAFKA_SASL_MECHANISM must be %q, %q, or %q, got: %s", kafkashared.MechanismPLAIN, kafkashared.MechanismSCRAMSHA256, kafkashared.MechanismSCRAMSHA512, mechanism)
	}
	return nil
}
