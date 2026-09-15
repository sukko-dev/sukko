package provider

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// DryRunProvider logs push notification payloads without making any network
// calls. Useful for development, testing, and staging environments.
type DryRunProvider struct {
	logger zerolog.Logger
}

// NewDryRunProvider creates a dry-run provider that logs all notifications.
func NewDryRunProvider(logger zerolog.Logger) *DryRunProvider {
	return &DryRunProvider{
		logger: logger.With().Str("provider", "dryrun").Logger(),
	}
}

// Name returns "dryrun".
func (p *DryRunProvider) Name() string { return "dryrun" }

// InvalidateClient is a no-op for dry-run (no cached clients).
func (p *DryRunProvider) InvalidateClient(_ string) {}

// Close is a no-op for dry-run.
func (p *DryRunProvider) Close() error { return nil }

// Send logs the notification payload with structured fields and returns nil.
func (p *DryRunProvider) Send(_ context.Context, job PushJob) error {
	p.logJob(job)
	return nil
}

// SendBatch logs each notification payload and returns nil.
func (p *DryRunProvider) SendBatch(_ context.Context, jobs []PushJob) error {
	for i := range jobs {
		p.logJob(jobs[i])
	}
	return nil
}

// logJob emits a structured log entry for a single push job.
func (p *DryRunProvider) logJob(job PushJob) {
	event := p.logger.Info().
		Str(logging.LogKeyTenantSlug, job.TenantID).
		Str("principal", job.Principal).
		Str("platform", job.Platform).
		Str("title", job.Title).
		Str("body", job.Body).
		Str("channel", job.Channel)

	// Include platform-specific identifiers.
	switch job.Platform {
	case "web":
		event.Str("endpoint", job.Endpoint)
	case "android", "ios":
		event.Str("token", truncateToken(job.Token))
	}

	if job.URL != "" {
		event.Str("url", job.URL)
	}
	if job.Icon != "" {
		event.Str("icon", job.Icon)
	}

	event.Msg("[DRY RUN] push notification")
}
