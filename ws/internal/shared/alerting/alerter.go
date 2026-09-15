package alerting

// Alerter is the interface for sending alert notifications.
// Implementations: Slack, Console, Email, PagerDuty, etc.
type Alerter interface {
	// Alert sends an alert with the given level, message, and metadata.
	// Implementations should be non-blocking and handle errors internally.
	Alert(level Level, message string, metadata map[string]any)
}

// NoopAlerter is an alerter that does nothing.
// Useful for testing or when alerting is disabled.
type NoopAlerter struct{}

// Alert does nothing.
func (n *NoopAlerter) Alert(_ Level, _ string, _ map[string]any) {}

// Ensure NoopAlerter implements Alerter.
var _ Alerter = (*NoopAlerter)(nil)
