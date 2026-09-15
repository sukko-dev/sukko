package alerting

import (
	"time"

	"github.com/rs/zerolog"
)

// Config holds configuration for creating alerters.
// Config values MUST be validated (e.g., via ServerConfig.Validate()) before use.
type Config struct {
	// Enabled determines if alerting is active.
	Enabled bool

	// MinLevel is the minimum level that triggers alerts.
	MinLevel Level

	// Slack configuration
	SlackWebhookURL string
	SlackChannel    string
	SlackUsername   string

	// Service context
	ServiceName string
	Environment string

	// Rate limiting
	RateLimitWindow time.Duration
	RateLimitMax    int

	// SlackTimeout is the HTTP client and context timeout for Slack webhook calls.
	SlackTimeout time.Duration

	// ConsoleEnabled outputs alerts to console (for development).
	ConsoleEnabled bool

	// Logger is the structured logger for panic recovery in concurrent alerters.
	Logger zerolog.Logger
}

// NewFromConfig creates an Alerter from a Config.
// Returns a NoopAlerter if alerting is disabled.
func NewFromConfig(cfg Config) Alerter {
	if !cfg.Enabled {
		return &NoopAlerter{}
	}

	var alerters []Alerter

	// Add Slack alerter if configured
	if cfg.SlackWebhookURL != "" {
		slack := NewSlackAlerterWithConfig(SlackConfig{
			WebhookURL:  cfg.SlackWebhookURL,
			Channel:     cfg.SlackChannel,
			Username:    cfg.SlackUsername,
			ServiceName: cfg.ServiceName,
			Environment: cfg.Environment,
			Timeout:     cfg.SlackTimeout,
		})
		alerters = append(alerters, slack)
	}

	// Add console alerter if enabled
	if cfg.ConsoleEnabled {
		alerters = append(alerters, NewConsoleAlerter())
	}

	// If no alerters configured, return noop
	if len(alerters) == 0 {
		return &NoopAlerter{}
	}

	// Combine alerters
	var alerter Alerter
	if len(alerters) == 1 {
		alerter = alerters[0]
	} else {
		alerter = NewMultiAlerter(cfg.Logger, alerters...)
	}

	// Wrap with rate limiting
	return NewRateLimitedAlerterWithConfig(alerter, RateLimitConfig{
		Window: cfg.RateLimitWindow,
		Max:    cfg.RateLimitMax,
	})
}
