package types

// Webhook status constants — defined here so webhook-worker can import them
// without importing the provisioning package (which would create a circular dependency).
// Constitution §X: types used by multiple services live in shared/types.
const (
	WebhookStatusEnabled   = "enabled"
	WebhookStatusDegraded  = "degraded"
	WebhookStatusSuspended = "suspended"
)
