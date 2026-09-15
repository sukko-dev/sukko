package auth

import "testing"

// TestClaims_MatchesTenantUUID verifies the tenant match is against the resolved UUID,
// never the TenantID slug — the exact distinction that broke the auth-upgrade flow.
func TestClaims_MatchesTenantUUID(t *testing.T) {
	t.Parallel()

	const (
		tenantUUID = "3700aa59-5546-4dc0-97dd-fca1f9e303a2"
		otherUUID  = "11111111-2222-3333-4444-555555555555"
		tenantSlug = "acme"
	)

	tests := []struct {
		name   string
		claims Claims
		arg    string
		want   bool
	}{
		{
			name:   "equal resolved UUID matches",
			claims: Claims{TenantID: tenantSlug, ResolvedTenantUUID: tenantUUID},
			arg:    tenantUUID,
			want:   true,
		},
		{
			name:   "different resolved UUID does not match",
			claims: Claims{TenantID: tenantSlug, ResolvedTenantUUID: tenantUUID},
			arg:    otherUUID,
			want:   false,
		},
		{
			// The bug: comparing the slug would "match" here, but MatchesTenantUUID
			// compares the resolved UUID — so a slug that equals the arg must NOT match.
			name:   "matching slug but differing UUID does not match",
			claims: Claims{TenantID: tenantSlug, ResolvedTenantUUID: otherUUID},
			arg:    tenantSlug,
			want:   false,
		},
		{
			name:   "empty resolved UUID never matches a non-empty arg",
			claims: Claims{TenantID: tenantSlug, ResolvedTenantUUID: ""},
			arg:    tenantUUID,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.claims.MatchesTenantUUID(tt.arg); got != tt.want {
				t.Errorf("MatchesTenantUUID(%q) = %v, want %v (resolved=%q slug=%q)",
					tt.arg, got, tt.want, tt.claims.ResolvedTenantUUID, tt.claims.TenantID)
			}
		})
	}
}
