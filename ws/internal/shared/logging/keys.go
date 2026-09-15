package logging

// Structured-log field keys for tenant identifiers.
//
// The Sukko identity model (see #161) distinguishes two tenant identifiers, and logs MUST
// name them explicitly so operators can correlate reliably — the legacy ambiguous
// "tenant_id" key is forbidden for tenant identifiers:
//
//   - LogKeyTenantSlug  — the tenant SLUG, the client-facing routing label used by the
//     data plane (ws-server, gateway, broadcast, registry, push runtime, revocation) and
//     carried in the JWT tenant_id claim.
//   - LogKeyTenantUUID  — the tenant UUID, the identity-of-record used by provisioning DB
//     rows, the audit log, keys, webhooks, and the webhook worker.
//
// Pick the key by the VALUE being logged, not by the variable name.
const (
	LogKeyTenantSlug = "tenant_slug"
	LogKeyTenantUUID = "tenant_uuid"
)

// LogKeyGRPCCode is the structured-log field key for a gRPC status code. Used by both
// ws-server and the gateway when logging a failed Publish RPC, so log correlation across
// the two services relies on a single shared key (not per-service literals).
const LogKeyGRPCCode = "grpc_code"
