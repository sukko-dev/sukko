package alerting

import (
	"testing"
	"time"
)

func TestNewRateLimitedAlerterWithConfig(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 10 * time.Second,
		Max:    5,
	})

	if rl.window != 10*time.Second {
		t.Errorf("Window should be 10 seconds, got %v", rl.window)
	}
	if rl.max != 5 {
		t.Errorf("Max should be 5, got %d", rl.max)
	}
}

func TestRateLimitedAlerter_AllowsUnderLimit(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Second,
		Max:    3,
	})

	// Send 3 alerts (should all pass through)
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(ERROR, "Test", nil)

	if mock.getAlerts() != 3 {
		t.Errorf("Expected 3 alerts, got %d", mock.getAlerts())
	}
}

func TestRateLimitedAlerter_SuppressesOverLimit(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Hour, // Long window to ensure no reset
		Max:    3,
	})

	// Send 5 alerts (only 3 should pass through)
	for range 5 {
		rl.Alert(ERROR, "Test", nil)
	}

	if mock.getAlerts() != 3 {
		t.Errorf("Expected 3 alerts (suppressed 2), got %d", mock.getAlerts())
	}
}

func TestRateLimitedAlerter_DifferentMessagesNotSuppressed(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Hour,
		Max:    1, // Limit to 1 per message
	})

	// Send different messages
	rl.Alert(ERROR, "Message 1", nil)
	rl.Alert(ERROR, "Message 2", nil)
	rl.Alert(ERROR, "Message 3", nil)

	// All different messages should pass through
	if mock.getAlerts() != 3 {
		t.Errorf("Expected 3 alerts (different messages), got %d", mock.getAlerts())
	}
}

func TestRateLimitedAlerter_DifferentLevelsNotSuppressed(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Hour,
		Max:    1, // Limit to 1 per unique key
	})

	// Send same message but different levels
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(WARNING, "Test", nil)
	rl.Alert(CRITICAL, "Test", nil)

	// Different levels create different keys
	if mock.getAlerts() != 3 {
		t.Errorf("Expected 3 alerts (different levels), got %d", mock.getAlerts())
	}
}

func TestRateLimitedAlerter_ResetsAfterWindow(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 50 * time.Millisecond, // Very short window for testing
		Max:    2,
	})

	// Send 3 alerts (2 pass, 1 suppressed)
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(ERROR, "Test", nil)

	if mock.getAlerts() != 2 {
		t.Errorf("Expected 2 alerts initially, got %d", mock.getAlerts())
	}

	// Wait for window to expire
	time.Sleep(100 * time.Millisecond)

	// Send more alerts (should reset)
	rl.Alert(ERROR, "Test", nil)
	rl.Alert(ERROR, "Test", nil)

	if mock.getAlerts() != 4 {
		t.Errorf("Expected 4 alerts after reset, got %d", mock.getAlerts())
	}
}

func TestRateLimitedAlerter_GetSuppressedCount(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Hour,
		Max:    2,
	})

	// Send 5 alerts (2 pass, 3 suppressed)
	for range 5 {
		rl.Alert(ERROR, "Test", nil)
	}

	suppressed := rl.GetSuppressedCount(ERROR, "Test")
	if suppressed != 3 {
		t.Errorf("Expected 3 suppressed, got %d", suppressed)
	}
}

func TestRateLimitedAlerter_SendsSuppressionNotice(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 1 * time.Hour,
		Max:    1, // Low limit to trigger suppression quickly
	})

	// Send 11 alerts (1 normal + 10 suppressed should trigger notice)
	for range 11 {
		rl.Alert(ERROR, "Test", nil)
	}

	// Should have 2 alerts: original + suppression notice at 10
	if mock.getAlerts() != 2 {
		t.Errorf("Expected 2 alerts (1 + suppression notice), got %d", mock.getAlerts())
	}

	level, msg, meta := mock.getLastAlert()
	if level != ERROR {
		t.Errorf("Suppression notice should have same level, got %s", level)
	}
	if meta["suppressed_count"] != 10 {
		t.Errorf("Suppression notice should include count, got %v", meta["suppressed_count"])
	}
	if msg == "" || msg == "Test" {
		t.Error("Suppression notice should modify message")
	}
}

func TestRateLimitedAlerter_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var alerter Alerter = NewRateLimitedAlerterWithConfig(&mockAlerter{}, RateLimitConfig{
		Window: 5 * time.Minute,
		Max:    3,
	})

	alerter.Alert(ERROR, "Test", nil)
}

func TestRateLimitedAlerter_Cleanup(t *testing.T) {
	t.Parallel()
	mock := &mockAlerter{}
	rl := NewRateLimitedAlerterWithConfig(mock, RateLimitConfig{
		Window: 10 * time.Millisecond,
		Max:    1,
	})

	// Send many different alerts to populate the map
	for i := range 100 {
		rl.Alert(ERROR, "Message "+string(rune('A'+i%26)), nil)
	}

	// Wait for cleanup window
	time.Sleep(50 * time.Millisecond)

	// Trigger cleanup via new alert
	rl.Alert(ERROR, "Trigger cleanup", nil)

	// Map should have fewer entries after cleanup
	rl.mu.Lock()
	count := len(rl.alerts)
	rl.mu.Unlock()

	// Should have cleaned up most old entries
	if count > 50 {
		t.Errorf("Expected cleanup to reduce entries, still have %d", count)
	}
}
