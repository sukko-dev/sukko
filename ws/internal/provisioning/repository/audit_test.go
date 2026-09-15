package repository_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/shared/testutil"
)

// TestAuditRepository_Log_TenantIDColumnIsUUID documents and enforces the provisioning_audit
// tenant_id UUID-column contract that motivates #166: audit entries MUST carry the tenant
// UUID, not a slug. A slug is not valid UUID syntax and is rejected by Postgres — which is
// exactly why the connections force-disconnect handlers must pass the stashed tenant UUID
// (not claims.TenantID, a slug) to AuditLog.
func TestAuditRepository_Log_TenantIDColumnIsUUID(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewAuditRepository(pool)
	ctx := context.Background()

	// A tenant UUID (no FK on provisioning_audit.tenant_id, so any valid UUID persists).
	tenantUUID := mustCreateTenant(t, pool, "audit-"+uniqueSuffix(t))

	if err := repo.Log(ctx, &provisioning.AuditEntry{
		TenantID:  tenantUUID,
		Action:    "test.action",
		Actor:     "tester",
		ActorType: provisioning.ActorTypeSystem,
	}); err != nil {
		t.Fatalf("log with tenant UUID: unexpected error: %v", err)
	}

	// A slug (non-UUID string) must be rejected by the UUID column.
	err := repo.Log(ctx, &provisioning.AuditEntry{
		TenantID:  "acme-slug",
		Action:    "test.action",
		Actor:     "tester",
		ActorType: provisioning.ActorTypeSystem,
	})
	if err == nil {
		t.Fatal("logging a slug into the UUID tenant_id column must fail, got nil error")
	}
}

// TestAuditRepository_Log_NormalizesIPAddress is the §II backstop for the INET
// ip_address column: the middleware now strips ports, but the audit layer must
// never lose a whole audit record to a malformed IP a future caller passes in.
// host:port forms are normalized to the bare IP; junk that is not an IP at all
// is stored as NULL (the action is §IX-mandatory to record; the IP is metadata).
func TestAuditRepository_Log_NormalizesIPAddress(t *testing.T) {
	t.Parallel()
	pool := testutil.NewTestPool(t)
	repo := repository.NewAuditRepository(pool)
	ctx := context.Background()
	tenantUUID := mustCreateTenant(t, pool, "auditip-"+uniqueSuffix(t))

	tests := []struct {
		name   string
		ip     string
		wantIP string // "" ⇒ stored NULL
	}{
		{"ipv4 host:port", "172.18.0.7:60328", "172.18.0.7"},
		{"ipv6 bracket host:port", "[2001:db8::1]:443", "2001:db8::1"},
		{"bare ipv4 unchanged", "10.0.0.1", "10.0.0.1"},
		{"bare ipv6 unchanged", "2001:db8::2", "2001:db8::2"},
		{"junk stored as NULL, entry kept", "not-an-ip", ""},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			action := fmt.Sprintf("test.ip.%d", i)
			entry := &provisioning.AuditEntry{
				TenantID:  tenantUUID,
				Action:    action,
				Actor:     "tester",
				ActorType: provisioning.ActorTypeSystem,
				IPAddress: tc.ip,
			}
			if err := repo.Log(ctx, entry); err != nil {
				t.Fatalf("Log with IP %q must not fail (the audit record is mandatory): %v", tc.ip, err)
			}
			var stored *string
			if err := pool.QueryRow(ctx,
				`SELECT host(ip_address) FROM provisioning_audit WHERE id = $1`, entry.ID,
			).Scan(&stored); err != nil {
				t.Fatalf("read back: %v", err)
			}
			switch {
			case tc.wantIP == "" && stored != nil:
				t.Errorf("ip_address = %q, want NULL", *stored)
			case tc.wantIP != "" && (stored == nil || *stored != tc.wantIP):
				got := "<NULL>"
				if stored != nil {
					got = *stored
				}
				t.Errorf("ip_address = %s, want %q", got, tc.wantIP)
			}
		})
	}
}
