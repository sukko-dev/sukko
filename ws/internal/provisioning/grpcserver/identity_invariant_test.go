package grpcserver

import (
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/provisioning/revocation"
)

// TestIdentityFieldInvariant locks the #161 identity-unification rule: a proto
// field named tenant_uuid MUST carry a parseable UUID, and a field named
// tenant_slug MUST carry a non-UUID routing label. It exercises each provisioning
// producer with a UUID-shaped ID and a deliberately non-parseable slug, so a
// future edit that maps tenant.ID / tenant.Slug into the wrong field is caught
// (the compiler cannot catch a string→string field swap). It also asserts
// value-flow: the exact input value reaches the renamed output field.
func TestIdentityFieldInvariant(t *testing.T) {
	t.Parallel()
	const (
		tenantUUID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
		tenantSlug = "acme-corp" // intentionally NOT a UUID
	)

	t.Run("convertKeys emits tenant_uuid as a UUID", func(t *testing.T) {
		t.Parallel()
		out := convertKeys([]*provisioning.TenantKey{
			{KeyID: "k1", TenantID: tenantUUID, Algorithm: "ES256", PublicKey: "pem"},
		})
		if len(out) != 1 {
			t.Fatalf("convertKeys returned %d, want 1", len(out))
		}
		if got := out[0].GetTenantUuid(); got != tenantUUID {
			t.Errorf("value-flow: KeyInfo.tenant_uuid = %q, want %q", got, tenantUUID)
		}
		if _, err := uuid.Parse(out[0].GetTenantUuid()); err != nil {
			t.Errorf("KeyInfo.tenant_uuid %q must parse as a UUID: %v", out[0].GetTenantUuid(), err)
		}
	})

	t.Run("convertAPIKeys emits tenant_uuid as a UUID", func(t *testing.T) {
		t.Parallel()
		out := convertAPIKeys([]*provisioning.APIKey{
			{KeyID: "pk_live_x", TenantID: tenantUUID, Name: "prod", IsActive: true},
		})
		if len(out) != 1 {
			t.Fatalf("convertAPIKeys returned %d, want 1", len(out))
		}
		if got := out[0].GetTenantUuid(); got != tenantUUID {
			t.Errorf("value-flow: APIKeyInfo.tenant_uuid = %q, want %q", got, tenantUUID)
		}
		if _, err := uuid.Parse(out[0].GetTenantUuid()); err != nil {
			t.Errorf("APIKeyInfo.tenant_uuid %q must parse as a UUID: %v", out[0].GetTenantUuid(), err)
		}
	})

	t.Run("convertRevocationEntries emits tenant_slug (never a UUID)", func(t *testing.T) {
		t.Parallel()
		out := convertRevocationEntries([]*revocation.Entry{
			{TenantID: tenantSlug, Type: "token", JTI: "j1", ExpiresAt: 1},
		})
		if len(out) != 1 {
			t.Fatalf("convertRevocationEntries returned %d, want 1", len(out))
		}
		if got := out[0].GetTenantSlug(); got != tenantSlug {
			t.Errorf("value-flow: TokenRevocation.tenant_slug = %q, want %q", got, tenantSlug)
		}
		if _, err := uuid.Parse(out[0].GetTenantSlug()); err == nil {
			t.Errorf("TokenRevocation.tenant_slug %q must NOT parse as a UUID (it is the routing label)", out[0].GetTenantSlug())
		}
	})
}

// TestIdentityFieldInvariant_PushStreamsEmitSlug asserts the push config producers
// emit the tenant SLUG (push runtime is slug-native), resolved from the UUID FK via
// the slug map, and skip — never emit empty — a row whose tenant UUID has no mapping.
func TestIdentityFieldInvariant_PushStreamsEmitSlug(t *testing.T) {
	t.Parallel()
	const (
		tenantUUID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
		tenantSlug = "acme-corp" // intentionally NOT a UUID
		orphanUUID = "11111111-1111-1111-1111-111111111111"
	)
	s := &Server{logger: zerolog.Nop()}
	slugByUUID := map[string]string{tenantUUID: tenantSlug}

	t.Run("convertPushCredentials emits slug and skips an unmapped tenant", func(t *testing.T) {
		t.Parallel()
		out := s.convertPushCredentials([]*repository.PushCredential{
			{TenantID: tenantUUID, Provider: "vapid", CredentialData: "{}"},
			{TenantID: orphanUUID, Provider: "fcm", CredentialData: "{}"},
		}, slugByUUID)
		if len(out) != 1 {
			t.Fatalf("got %d credentials, want 1 (unmapped tenant skipped)", len(out))
		}
		if out[0].GetTenantSlug() != tenantSlug {
			t.Errorf("value-flow: PushCredentialInfo.tenant_slug = %q, want %q", out[0].GetTenantSlug(), tenantSlug)
		}
		if _, err := uuid.Parse(out[0].GetTenantSlug()); err == nil {
			t.Errorf("PushCredentialInfo.tenant_slug %q must NOT parse as a UUID", out[0].GetTenantSlug())
		}
	})

	t.Run("convertPushChannelConfigs emits slug and skips an unmapped tenant", func(t *testing.T) {
		t.Parallel()
		out := s.convertPushChannelConfigs([]*repository.PushChannelConfig{
			{TenantID: tenantUUID, Patterns: []string{tenantSlug + ".alerts"}},
			{TenantID: orphanUUID, Patterns: []string{"x.y"}},
		}, slugByUUID)
		if len(out) != 1 {
			t.Fatalf("got %d configs, want 1 (unmapped tenant skipped)", len(out))
		}
		if out[0].GetTenantSlug() != tenantSlug {
			t.Errorf("value-flow: PushChannelConfig.tenant_slug = %q, want %q", out[0].GetTenantSlug(), tenantSlug)
		}
		if _, err := uuid.Parse(out[0].GetTenantSlug()); err == nil {
			t.Errorf("PushChannelConfig.tenant_slug %q must NOT parse as a UUID", out[0].GetTenantSlug())
		}
	})
}
