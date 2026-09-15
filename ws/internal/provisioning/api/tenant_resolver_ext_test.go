package api_test

import (
	"context"

	"github.com/sukko-dev/sukko/internal/shared/auth"
)

// identityTenantResolver resolves a tenant slug to itself (external api_test
// package variant). Router tests register keys whose TenantID equals the token
// tenant_id slug, so this makes the tenant binding a pass-through.
type identityTenantResolver struct{}

func (identityTenantResolver) ResolveTenantUUID(_ context.Context, slug string) (string, error) {
	if slug == "" {
		return "", auth.ErrTenantNotResolvable
	}
	return slug, nil
}
