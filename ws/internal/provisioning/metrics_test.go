package provisioning

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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
