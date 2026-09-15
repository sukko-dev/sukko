package alerting

import (
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// MultiAlerter sends alerts to multiple alerters.
// Example: Send to both Slack and Email.
type MultiAlerter struct {
	alerters []Alerter
	logger   zerolog.Logger
}

// NewMultiAlerter creates a MultiAlerter that fans out to multiple alerters.
func NewMultiAlerter(logger zerolog.Logger, alerters ...Alerter) *MultiAlerter {
	return &MultiAlerter{
		alerters: alerters,
		logger:   logger,
	}
}

// Alert sends an alert to all configured alerters concurrently.
func (m *MultiAlerter) Alert(level Level, message string, metadata map[string]any) {
	for _, alerter := range m.alerters {
		go func(a Alerter) {
			defer logging.RecoverPanic(m.logger, "MultiAlerter.Alert", nil)
			a.Alert(level, message, metadata)
		}(alerter)
	}
}

// Ensure MultiAlerter implements Alerter.
var _ Alerter = (*MultiAlerter)(nil)
