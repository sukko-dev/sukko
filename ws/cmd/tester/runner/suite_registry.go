package runner

// SuiteInfo describes a validation suite for the capabilities endpoint.
type SuiteInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SuiteRegistry maps suite names to their metadata.
// Used by the capabilities endpoint and the validate runner.
var SuiteRegistry = map[string]SuiteInfo{
	"auth":             {Name: "auth", Description: "JWT authentication validation (valid, expired, wrong kid, wrong tenant, revoked, missing)"},
	"channels":         {Name: "channels", Description: "Channel subscribe/unsubscribe operations"},
	"ordering":         {Name: "ordering", Description: "Message FIFO ordering verification"},
	"reconnect":        {Name: "reconnect", Description: "Disconnect/reconnect session recovery"},
	"ratelimit":        {Name: "ratelimit", Description: "Rate limit enforcement detection"},
	"edition-limits":   {Name: "edition-limits", Description: "Edition boundary limit testing"},
	"pubsub":           {Name: "pubsub", Description: "Pub-sub delivery with channel scoping (public, user, group)"},
	"tenant-isolation": {Name: "tenant-isolation", Description: "Cross-tenant message isolation"},
	"provisioning":     {Name: "provisioning", Description: "Provisioning API validation"},
	"sse":              {Name: "sse", Description: "SSE transport receive and auth validation"},
	"rest-publish":     {Name: "rest-publish", Description: "REST publish endpoint delivery and input validation"},
	"push":             {Name: "push", Description: "Push notification subscription and pipeline validation"},
	"license-reload":   {Name: "license-reload", Description: "License hot-reload propagation, gating, and connection survival"},
	"token-revocation": {Name: "token-revocation", Description: "Token revocation force-disconnect and rejection validation"},
	"api-key":          {Name: "api-key", Description: "validates static API key auth in isolation — no JWT provisioning; distinct from the 'auth' suite which validates both auth methods within a single run"},
	"upgrade":          {Name: "upgrade", Description: "validates the auth upgrade flow — connect with API key, upgrade to JWT via auth message, access private channels"},
	SuiteRevocation:    {Name: SuiteRevocation, Description: "Token revocation load testing — stress (mass force-disconnect at 1,000 connections) and soak (repeated revoke/reconnect over hours, memory/goroutine drift monitoring). Requires Pro+ edition."},
	SuiteWebhooks:      {Name: SuiteWebhooks, Description: "Webhook delivery validation — happy-path single delivery, retry-on-failure recovery, and degraded-state transition after max retries exhausted. Requires Pro+ edition and TESTER_WEBHOOK_BASE_URL."},
	SuiteKafkaIngest:   {Name: SuiteKafkaIngest, Description: "Direct-to-Kafka ingestion verification — publishes straight to the message backend (SASL/TLS) and confirms a gateway-subscribed client receives it. Requires the target server to run MESSAGE_BACKEND=kafka; skips when KAFKA_BROKERS is unset."},
	SuiteHistory:       {Name: SuiteHistory, Description: "Message-history delivery proof — ingests records directly to Kafka, then asserts a history{channel,limit} request returns them with identical ADR-0008 mids, terminated by history_complete. Requires MESSAGE_BACKEND=kafka + WS_HISTORY_ENABLED=true; skips when KAFKA_BROKERS is unset."},
	SuiteGapRecovery:   {Name: SuiteGapRecovery, Description: "Gap-recovery delivery proof, both legs — reconnect{client_id,last_pos} replay after a hard kill, and live replay{channel,from_pos} with replay_message/replay_complete — asserting lossless exclusive-cursor delivery, order, and mid identity against a control subscriber. Requires MESSAGE_BACKEND=kafka; skips when KAFKA_BROKERS is unset."},
}
