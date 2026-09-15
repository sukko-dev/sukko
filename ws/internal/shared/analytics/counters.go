package analytics

import (
	"sync"
	"sync/atomic"
)

// tenantKey identifies a unique time-bucket row: one tenant + one secondary dimension
// (transport for connections, channelPrefix for messages, provider for push).
type tenantKey struct {
	tenantID  string
	secondary string
}

// connectionCounters holds atomic counters for one (tenant, transport) time bucket.
type connectionCounters struct {
	active      atomic.Int64
	connects    atomic.Int64
	disconnects atomic.Int64
	errors      atomic.Int64
}

// messageCounters holds atomic counters for one (tenant, channelPrefix) time bucket.
type messageCounters struct {
	published      atomic.Int64
	delivered      atomic.Int64
	failed         atomic.Int64
	totalLatencyMs atomic.Int64
}

// pushCounters holds atomic counters for one (tenant, provider) time bucket.
type pushCounters struct {
	sent           atomic.Int64
	success        atomic.Int64
	failed         atomic.Int64
	expired        atomic.Int64
	rateLimited    atomic.Int64
	totalLatencyMs atomic.Int64
}

// counterMap holds all in-memory counters for a single flush window.
// The mu protects map structure (adding new keys); individual counter fields
// are atomic.Int64 and do not need the lock for increments.
type counterMap struct {
	mu          sync.Mutex
	connections map[tenantKey]*connectionCounters
	messages    map[tenantKey]*messageCounters
	push        map[tenantKey]*pushCounters
	bufferSize  int
	dropCounter *dropper // for incrementing analytics_events_dropped_total
}

// dropper is a callback to increment the events_dropped counter.
// It is set after counterMap creation to avoid circular dependency.
type dropper func()

// newCounterMap creates an empty counterMap with the given buffer size.
func newCounterMap(bufferSize int) *counterMap {
	return &counterMap{
		connections: make(map[tenantKey]*connectionCounters),
		messages:    make(map[tenantKey]*messageCounters),
		push:        make(map[tenantKey]*pushCounters),
		bufferSize:  bufferSize,
	}
}

// totalTenants returns the approximate number of unique tenants across all maps.
// Must hold mu because producers that loaded the old pointer before an atomic.Pointer
// swap can still insert new keys under mu — ranging without the lock is a data race.
func (m *counterMap) totalTenants() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{})
	for k := range m.connections {
		seen[k.tenantID] = struct{}{}
	}
	for k := range m.messages {
		seen[k.tenantID] = struct{}{}
	}
	for k := range m.push {
		seen[k.tenantID] = struct{}{}
	}
	return len(seen)
}

// isEmpty returns true if all maps are empty.
// Must hold mu for the same reason as totalTenants.
func (m *counterMap) isEmpty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.connections) == 0 && len(m.messages) == 0 && len(m.push) == 0
}

// getOrCreateConnection returns the counter entry for the given key, creating it
// if absent. Returns nil when the buffer is full (event dropped).
func (m *counterMap) getOrCreateConnection(k tenantKey) *connectionCounters {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.connections[k]; ok {
		return c
	}
	if len(m.connections)+len(m.messages)+len(m.push) >= m.bufferSize {
		if m.dropCounter != nil {
			(*m.dropCounter)()
		}
		return nil
	}
	c := &connectionCounters{}
	m.connections[k] = c
	return c
}

// getOrCreateMessage returns the counter entry for the given key, creating it if absent.
func (m *counterMap) getOrCreateMessage(k tenantKey) *messageCounters {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.messages[k]; ok {
		return c
	}
	if len(m.connections)+len(m.messages)+len(m.push) >= m.bufferSize {
		if m.dropCounter != nil {
			(*m.dropCounter)()
		}
		return nil
	}
	c := &messageCounters{}
	m.messages[k] = c
	return c
}

// getOrCreatePush returns the counter entry for the given key, creating it if absent.
func (m *counterMap) getOrCreatePush(k tenantKey) *pushCounters {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.push[k]; ok {
		return c
	}
	if len(m.connections)+len(m.messages)+len(m.push) >= m.bufferSize {
		if m.dropCounter != nil {
			(*m.dropCounter)()
		}
		return nil
	}
	c := &pushCounters{}
	m.push[k] = c
	return c
}
