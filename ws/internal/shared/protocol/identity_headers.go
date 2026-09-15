package protocol

// Identity headers forwarded by the gateway to ws-server on each WebSocket upgrade.
// These are client-facing forwarded identity headers set by the gateway after auth.
const (
	// HeaderTenantID carries the authenticated tenant slug.
	HeaderTenantID = "X-Sukko-Tenant-ID"

	// HeaderAPIKeyID carries the stable database ID of the API key used to authenticate.
	// Omitted (not sent) for JWT-only auth paths where no API key is involved.
	HeaderAPIKeyID = "X-Sukko-API-Key-ID" //nolint:gosec // G101: header name constant, not a credential value

	// HeaderUserID carries the JWT subject claim for user-authenticated connections.
	// Omitted for API-key-only auth (no user identity to forward).
	HeaderUserID = "X-Sukko-User-ID"

	// HeaderInternalSecret is used for gateway → ws-server mutual authentication.
	// internal: not part of client WebSocket protocol — this header is stripped from
	// inbound client requests by the gateway before forwarding, and clients never see it.
	HeaderInternalSecret = "X-Sukko-Internal-Secret" //nolint:gosec // G101: header name constant, not a credential value
)
