package platform

// GRPCInternalTokenMetadataKey is the gRPC metadata header key used for
// service-to-service authentication between the webhook-worker and the
// provisioning service. Exported so both the server interceptor
// (provisioning/grpcserver) and the client (webhook-worker) reference the
// same constant without duplicating the string.
// Constitution §I: code-level symbolic strings used in multiple packages
// MUST be named constants defined once, referenced everywhere.
const GRPCInternalTokenMetadataKey = "x-internal-token"
