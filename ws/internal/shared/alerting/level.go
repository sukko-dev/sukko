// Package alerting provides alert notification capabilities for services.
// It supports multiple backends (Slack, console) and rate limiting to prevent spam.
package alerting

// Level represents the severity of an alert.
type Level string

// Level constants for alert severity.
const (
	DEBUG    Level = "DEBUG"    // Detailed debug information
	INFO     Level = "INFO"     // Normal operations
	WARNING  Level = "WARNING"  // Warning but service continues
	ERROR    Level = "ERROR"    // Error occurred, may affect some users
	CRITICAL Level = "CRITICAL" // Critical issue, service degraded/down
)

// Value returns the numeric value of the level for comparison.
func (l Level) Value() int {
	switch l {
	case DEBUG:
		return 0
	case INFO:
		return 1
	case WARNING:
		return 2
	case ERROR:
		return 3
	case CRITICAL:
		return 4
	default:
		return 0
	}
}

// ShouldAlert returns true if this level should trigger an alert.
// By default, WARNING and above trigger alerts.
func (l Level) ShouldAlert() bool {
	return l.Value() >= WARNING.Value()
}

// String returns the string representation of the level.
func (l Level) String() string {
	return string(l)
}

// ParseLevel parses a string into a Level, defaulting to INFO if unknown.
func ParseLevel(s string) Level {
	switch s {
	case "DEBUG", "debug":
		return DEBUG
	case "INFO", "info":
		return INFO
	case "WARNING", "warning", "WARN", "warn":
		return WARNING
	case "ERROR", "error":
		return ERROR
	case "CRITICAL", "critical", "FATAL", "fatal":
		return CRITICAL
	default:
		return INFO
	}
}
