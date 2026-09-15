package gateway

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TenantConnectionTracker tracks active WebSocket connections per tenant.
// It enforces per-tenant connection limits and provides metrics.
type TenantConnectionTracker struct {
	mu sync.RWMutex

	// connections tracks the number of active connections per tenant
	connections map[string]*atomic.Int64

	// limits stores per-tenant connection limits (0 = use default)
	limits map[string]int

	// defaultLimit is used when no tenant-specific limit is set
	defaultLimit int
}

// Prometheus metrics for per-tenant connection tracking
var (
	tenantConnectionsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_tenant_connections_active",
		Help: "Current active connections per tenant",
	}, []string{"tenant_id"})

	tenantConnectionsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_tenant_connections_rejected_total",
		Help: "Connections rejected due to tenant limit",
	}, []string{"tenant_id"})
)

// NewTenantConnectionTracker creates a new connection tracker.
// Config values MUST be validated (e.g., via GatewayConfig.Validate()) before calling.
func NewTenantConnectionTracker(defaultLimit int) *TenantConnectionTracker {
	return &TenantConnectionTracker{
		connections:  make(map[string]*atomic.Int64),
		limits:       make(map[string]int),
		defaultLimit: defaultLimit,
	}
}

// SetLimit sets the connection limit for a specific tenant.
// Use limit=0 to use the default limit.
func (t *TenantConnectionTracker) SetLimit(tenantID string, limit int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if limit <= 0 {
		delete(t.limits, tenantID)
	} else {
		t.limits[tenantID] = limit
	}
}

// GetLimit returns the connection limit for a tenant.
func (t *TenantConnectionTracker) GetLimit(tenantID string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if limit, ok := t.limits[tenantID]; ok && limit > 0 {
		return limit
	}
	return t.defaultLimit
}

// TryAcquire attempts to acquire a connection slot for the tenant.
// Returns true if the connection was allowed, false if the limit was reached.
func (t *TenantConnectionTracker) TryAcquire(tenantID string) bool {
	counter, limit := t.getOrCreateCounter(tenantID)

	// Atomically check and increment
	for {
		current := counter.Load()
		if int(current) >= limit {
			// Limit reached
			tenantConnectionsRejected.WithLabelValues(tenantID).Inc()
			return false
		}

		if counter.CompareAndSwap(current, current+1) {
			// Successfully incremented
			tenantConnectionsActive.WithLabelValues(tenantID).Set(float64(current + 1))
			return true
		}
		// CAS failed, retry
	}
}

// getOrCreateCounter returns the connection counter and limit for a tenant,
// creating the counter if it doesn't exist.
func (t *TenantConnectionTracker) getOrCreateCounter(tenantID string) (counter *atomic.Int64, limit int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var exists bool
	counter, exists = t.connections[tenantID]
	if !exists {
		counter = &atomic.Int64{}
		t.connections[tenantID] = counter
	}
	limit = t.defaultLimit
	if l, ok := t.limits[tenantID]; ok && l > 0 {
		limit = l
	}
	return counter, limit
}

// Release releases a connection slot for the tenant.
// Should be called when a connection disconnects.
func (t *TenantConnectionTracker) Release(tenantID string) {
	t.mu.RLock()
	counter, exists := t.connections[tenantID]
	t.mu.RUnlock()

	if !exists {
		return
	}

	newVal := counter.Add(-1)
	if newVal < 0 {
		// This shouldn't happen, but protect against underflow
		counter.Store(0)
		newVal = 0
	}

	tenantConnectionsActive.WithLabelValues(tenantID).Set(float64(newVal))
}

// GetConnectionCount returns the current connection count for a tenant.
func (t *TenantConnectionTracker) GetConnectionCount(tenantID string) int64 {
	t.mu.RLock()
	counter, exists := t.connections[tenantID]
	t.mu.RUnlock()

	if !exists {
		return 0
	}

	return counter.Load()
}

// GetAllCounts returns a snapshot of all tenant connection counts.
func (t *TenantConnectionTracker) GetAllCounts() map[string]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]int64, len(t.connections))
	for tenantID, counter := range t.connections {
		result[tenantID] = counter.Load()
	}
	return result
}

// SetDefaultLimit updates the default connection limit.
func (t *TenantConnectionTracker) SetDefaultLimit(limit int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if limit > 0 {
		t.defaultLimit = limit
	}
}
