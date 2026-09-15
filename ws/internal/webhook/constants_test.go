package webhook

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"attempt zero (clamped to first)", 0, 1 * time.Second},
		{"attempt 1 (first retry)", 1, 1 * time.Second},
		{"attempt 2", 2, 5 * time.Second},
		{"attempt 3", 3, 30 * time.Second},
		{"attempt 4", 4, 2 * time.Minute},
		{"attempt 5 (last schedule entry)", 5, 10 * time.Minute},
		{"attempt 6 (clamped to last)", 6, 10 * time.Minute},
		{"attempt 100 (clamped to last)", 100, 10 * time.Minute},
		{"negative attempt (clamped to first)", -1, 1 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := RetryDelay(tt.attempt)
			if got != tt.want {
				t.Errorf("RetryDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestWebhookTestDeliverDeadline(t *testing.T) {
	t.Parallel()
	// Verify the constant matches the documented derivation: WEBHOOK_DELIVERY_TIMEOUT max (30s) + 500ms.
	want := 30*time.Second + 500*time.Millisecond
	if WebhookTestDeliverDeadline != want {
		t.Errorf("WebhookTestDeliverDeadline = %v, want %v", WebhookTestDeliverDeadline, want)
	}
}
