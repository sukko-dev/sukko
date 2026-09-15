package alerting

import (
	"sync"
	"testing"
)

// mockAlerter is a test helper that counts alerts.
type mockAlerter struct {
	mu         sync.Mutex
	alertCount int
	lastLevel  Level
	lastMsg    string
	lastMeta   map[string]any
}

func (m *mockAlerter) Alert(level Level, message string, metadata map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertCount++
	m.lastLevel = level
	m.lastMsg = message
	m.lastMeta = metadata
}

func (m *mockAlerter) getAlerts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alertCount
}

func (m *mockAlerter) getLastAlert() (level Level, msg string, meta map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLevel, m.lastMsg, m.lastMeta
}

func TestNoopAlerter(t *testing.T) {
	t.Parallel()

	alerter := &NoopAlerter{}

	// Should not panic
	alerter.Alert(ERROR, "Test message", map[string]any{"key": "value"})
	alerter.Alert(CRITICAL, "Another message", nil)
}

func TestNoopAlerter_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var alerter Alerter = &NoopAlerter{}

	// Should compile and not panic
	alerter.Alert(INFO, "Test", nil)
}
