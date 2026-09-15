// Package logging provides structured logging utilities for the WebSocket services
// including configurable log levels and panic recovery helpers.
//
// This package is designed to be used by all services (server, gateway, provisioning)
// without pulling in service-specific dependencies.
package logging

// LogLevel represents log verbosity level.
type LogLevel string

// LogLevel constants for configuring log verbosity.
const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelFatal LogLevel = "fatal"
)

// LogFormat represents log output format.
type LogFormat string

// LogFormat constants for configuring log output format.
const (
	LogFormatJSON   LogFormat = "json"   // JSON format for Loki
	LogFormatPretty LogFormat = "pretty" // Human-readable for local dev
)
