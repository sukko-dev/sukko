package platform

import "errors"

const webhookInternalTokenMinLen = 32

// WebhookInternalTokenConfig holds the shared secret used for
// webhook-worker ↔ provisioning gRPC authentication (bidirectional).
// Embed in WebhookWorkerConfig (unconditional) and ProvisioningConfig
// (call Validate() inside the EditionHasFeature(Webhooks) guard only).
type WebhookInternalTokenConfig struct {
	WebhookInternalToken string `env:"WEBHOOK_INTERNAL_TOKEN" redact:"true"` // Shared secret for authenticating internal webhook reload calls between the provisioning service and webhook worker.
}

// Validate checks the token is present and meets minimum length.
// Always called unconditionally by WebhookWorkerConfig.Validate().
// ProvisioningConfig.Validate() wraps this in an EditionHasFeature guard.
func (c WebhookInternalTokenConfig) Validate() error {
	if c.WebhookInternalToken == "" {
		return errors.New("WEBHOOK_INTERNAL_TOKEN is required")
	}
	if len(c.WebhookInternalToken) < webhookInternalTokenMinLen {
		return errors.New("WEBHOOK_INTERNAL_TOKEN must be at least 32 characters")
	}
	return nil
}
