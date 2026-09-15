package api

import (
	"context"

	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// identityTenantResolver resolves a tenant slug to itself. Provisioning API tests
// register a key whose TenantID equals the token's tenant_id slug, so an identity
// resolver makes the tenant binding a pass-through for legitimately-matching tokens.
type identityTenantResolver struct{}

func (identityTenantResolver) ResolveTenantUUID(_ context.Context, slug string) (string, error) {
	if slug == "" {
		return "", auth.ErrTenantNotResolvable
	}
	return slug, nil
}
