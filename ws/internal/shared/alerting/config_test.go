package alerting

import (
	"testing"
	"time"
)

func TestNewFromConfig_NoopWhenDisabled(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled: false,
	}

	alerter := NewFromConfig(cfg)

	if _, ok := alerter.(*NoopAlerter); !ok {
		t.Errorf("Should return NoopAlerter when disabled, got %T", alerter)
	}
}

func TestNewFromConfig_NoopWhenNoAlertersConfigured(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled:         true,
		RateLimitWindow: 5 * time.Minute,
		RateLimitMax:    3,
	}

	alerter := NewFromConfig(cfg)

	if _, ok := alerter.(*NoopAlerter); !ok {
		t.Errorf("Should return NoopAlerter when no alerters configured, got %T", alerter)
	}
}

func TestNewFromConfig_SlackOnly(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled:         true,
		SlackWebhookURL: "https://hooks.slack.com/test",
		SlackTimeout:    5 * time.Second,
		RateLimitWindow: 5 * time.Minute,
		RateLimitMax:    3,
	}

	alerter := NewFromConfig(cfg)

	if _, ok := alerter.(*RateLimitedAlerter); !ok {
		t.Errorf("Should return RateLimitedAlerter, got %T", alerter)
	}
}

func TestNewFromConfig_ConsoleOnly(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled:         true,
		ConsoleEnabled:  true,
		RateLimitWindow: 5 * time.Minute,
		RateLimitMax:    3,
	}

	alerter := NewFromConfig(cfg)

	if _, ok := alerter.(*RateLimitedAlerter); !ok {
		t.Errorf("Should return RateLimitedAlerter, got %T", alerter)
	}
}

func TestNewFromConfig_MultipleAlerters(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled:         true,
		SlackWebhookURL: "https://hooks.slack.com/test",
		SlackTimeout:    5 * time.Second,
		ConsoleEnabled:  true,
		RateLimitWindow: 5 * time.Minute,
		RateLimitMax:    3,
	}

	alerter := NewFromConfig(cfg)

	if rl, ok := alerter.(*RateLimitedAlerter); ok {
		if _, ok := rl.alerter.(*MultiAlerter); !ok {
			t.Errorf("Underlying alerter should be MultiAlerter, got %T", rl.alerter)
		}
	} else {
		t.Errorf("Should return RateLimitedAlerter, got %T", alerter)
	}
}
