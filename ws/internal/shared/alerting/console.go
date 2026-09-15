package alerting

import (
	"fmt"
	"io"
	"os"
)

// ConsoleAlerter prints alerts to console (for development/testing).
type ConsoleAlerter struct {
	writer io.Writer
}

// NewConsoleAlerter creates a ConsoleAlerter that writes to stdout.
func NewConsoleAlerter() *ConsoleAlerter {
	return &ConsoleAlerter{writer: os.Stdout}
}

// NewConsoleAlerterWithWriter creates a ConsoleAlerter with a custom writer (for testing).
func NewConsoleAlerterWithWriter(w io.Writer) *ConsoleAlerter {
	return &ConsoleAlerter{writer: w}
}

// Alert outputs an alert message to the console with metadata.
func (c *ConsoleAlerter) Alert(level Level, message string, metadata map[string]any) {
	w := c.writer
	if w == nil {
		w = os.Stdout
	}

	// Write alert header
	_, _ = fmt.Fprintf(w, "\n🔔 ALERT [%s]: %s\n", level, message)

	// Write metadata if present
	if len(metadata) > 0 {
		_, _ = fmt.Fprintln(w, "  Metadata:")
		for k, v := range metadata {
			_, _ = fmt.Fprintf(w, "    %s: %v\n", k, v)
		}
	}
	_, _ = fmt.Fprintln(w)
}

// Ensure ConsoleAlerter implements Alerter.
var _ Alerter = (*ConsoleAlerter)(nil)
