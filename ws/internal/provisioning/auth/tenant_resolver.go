package auth

import (
	"context"
	"errors"
	"time"

	"github.com/sukko-dev/sukko/internal/provisioning"
	sharedauth "github.com/sukko-dev/sukko/internal/shared/auth"
)

// TenantUUIDLookup resolves a tenant slug (current or previous-within-hold) to
// its stable UUID. Backed by repository.TenantRepository.GetIDBySlugWithGrace.
type TenantUUIDLookup func(ctx context.Context, slug string, holdPeriod time.Duration) (string, error)

// GraceTenantResolver implements sharedauth.TenantResolver for the provisioning
// tenant path. It maps the JWT tenant_id claim (slug) to the tenant UUID via a
// grace-aware DB lookup, so old-slug tokens keep resolving during the rename hold
// window (matching RequireTenant's provisioning-API grace). A slug that resolves
// to no active tenant returns sharedauth.ErrTenantNotResolvable (fail closed,
// distinguishable from a transient DB error, which is surfaced verbatim).
type GraceTenantResolver struct {
	lookup     TenantUUIDLookup
	holdPeriod time.Duration
}

// NewGraceTenantResolver builds a resolver over the given lookup and hold period.
func NewGraceTenantResolver(lookup TenantUUIDLookup, holdPeriod time.Duration) *GraceTenantResolver {
	return &GraceTenantResolver{lookup: lookup, holdPeriod: holdPeriod}
}

// ResolveTenantUUID resolves slug -> tenant UUID (grace-aware).
func (r *GraceTenantResolver) ResolveTenantUUID(ctx context.Context, slug string) (string, error) {
	id, err := r.lookup(ctx, slug, r.holdPeriod)
	if err != nil {
		if errors.Is(err, provisioning.ErrTenantNotFound) {
			// Unknown/forged slug — reject as unresolvable, not transient.
			return "", sharedauth.ErrTenantNotResolvable
		}
		// Transient (DB down, ctx canceled) — surface so the binding fails closed
		// while remaining distinguishable from a cross-tenant forgery.
		return "", err
	}
	return id, nil
}
