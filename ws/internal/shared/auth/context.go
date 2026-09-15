package auth

import "context"

// contextKey is a private type for context keys to avoid collisions.
type contextKey string

// DefaultActor is the default actor identifier when no actor is set in context.
const DefaultActor = "system"

// DefaultActorType is the default actor type when no actor type is set in context.
const DefaultActorType = "system"

// Context keys for authentication information.
const (
	claimsKey    contextKey = "auth.claims"
	actorKey     contextKey = "auth.actor"
	actorTypeKey contextKey = "auth.actor_type"
	clientIPKey  contextKey = "auth.client_ip"
)

// WithClaims adds JWT claims to context.
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsKey, claims)
}

// GetClaims retrieves JWT claims from context.
// Returns nil if no claims are present.
func GetClaims(ctx context.Context) *Claims {
	if v := ctx.Value(claimsKey); v != nil {
		if claims, ok := v.(*Claims); ok {
			return claims
		}
	}
	return nil
}

// WithActor adds actor information to context.
// actor is typically formatted as "tenant_id:user_id".
// actorType indicates the type of actor (e.g., "user", "service", "system").
// clientIP is the client's IP address.
func WithActor(ctx context.Context, actor, actorType, clientIP string) context.Context {
	ctx = context.WithValue(ctx, actorKey, actor)
	ctx = context.WithValue(ctx, actorTypeKey, actorType)
	ctx = context.WithValue(ctx, clientIPKey, clientIP)
	return ctx
}

// GetActor retrieves actor identifier from context.
// Returns "system" if no actor is present.
func GetActor(ctx context.Context) string {
	if v := ctx.Value(actorKey); v != nil {
		if actor, ok := v.(string); ok {
			return actor
		}
	}
	return DefaultActor
}

// GetActorType retrieves actor type from context.
// Returns DefaultActorType if no actor type is present.
func GetActorType(ctx context.Context) string {
	if v := ctx.Value(actorTypeKey); v != nil {
		if actorType, ok := v.(string); ok {
			return actorType
		}
	}
	return DefaultActorType
}

// GetClientIPFromContext retrieves client IP from context.
// Returns empty string if no client IP is present.
func GetClientIPFromContext(ctx context.Context) string {
	if v := ctx.Value(clientIPKey); v != nil {
		if ip, ok := v.(string); ok {
			return ip
		}
	}
	return ""
}
