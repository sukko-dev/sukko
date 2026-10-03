package provisioning

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
)

// failingAuditStore is an AuditStore whose Log always errors — used to drive the audit-write-failure
// metric path.
type failingAuditStore struct{}

func (failingAuditStore) Log(context.Context, *AuditEntry) error { return errors.New("db down") }
func (failingAuditStore) ListByTenant(context.Context, string, ListOptions) ([]*AuditEntry, int, error) {
	return nil, 0, nil
}

// TestAuditWriteFailureMetric verifies that a failed audit write increments
// provisioning_audit_write_failures_total for the action (§VI/§IX): the operation still proceeds,
// but the dropped audit record must be alertable, not merely logged.
func TestAuditWriteFailureMetric(t *testing.T) {
	t.Parallel()
	s := &Service{audit: failingAuditStore{}, logger: zerolog.Nop()}
	const action = "test.audit.write.failure" // unique label → isolated from other tests

	before := testutil.ToFloat64(auditWriteFailures.WithLabelValues(action))
	s.auditLog(context.Background(), "00000000-0000-0000-0000-000000000001", action, nil)
	if got := testutil.ToFloat64(auditWriteFailures.WithLabelValues(action)); got != before+1 {
		t.Errorf("provisioning_audit_write_failures_total{action=%q} = %v, want %v", action, got, before+1)
	}
}

// TestActiveTenantGauge verifies the provisioning_active_tenants gauge helpers.
//
//nolint:paralleltest // sub-tests share the package-level activeTenantsGauge — sequential is required.
func TestActiveTenantGauge(t *testing.T) {
	t.Run("SetActiveTenants sets absolute value", func(t *testing.T) {
		SetActiveTenants(47)
		if got := testutil.ToFloat64(activeTenantsGauge); got != 47.0 {
			t.Errorf("SetActiveTenants(47): got %v, want 47.0", got)
		}
	})

	t.Run("IncActiveTenants increments by 1", func(t *testing.T) {
		SetActiveTenants(10)
		IncActiveTenants()
		if got := testutil.ToFloat64(activeTenantsGauge); got != 11.0 {
			t.Errorf("after Inc from 10: got %v, want 11.0", got)
		}
	})

	t.Run("DecActiveTenants decrements by 1", func(t *testing.T) {
		SetActiveTenants(5)
		DecActiveTenants()
		if got := testutil.ToFloat64(activeTenantsGauge); got != 4.0 {
			t.Errorf("after Dec from 5: got %v, want 4.0", got)
		}
	})
}
