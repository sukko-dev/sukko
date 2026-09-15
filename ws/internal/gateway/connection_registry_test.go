package gateway

import (
	"sync"
	"testing"
)

// mockConnection implements Connection for testing.
type mockConnection struct {
	sub, jti   string
	iat        int64
	transport  string // "" normalizes to "ws" (see Transport) so existing call sites keep working
	closed     bool
	closeCount int
	lastCode   int
	lastReason string
	mu         sync.Mutex
}

func (m *mockConnection) ForceClose(code int, reason string) {
	m.mu.Lock()
	m.closed = true
	m.closeCount++
	m.lastCode = code
	m.lastReason = reason
	m.mu.Unlock()
}

func (m *mockConnection) ConnectionClaims() (sub, jti string, iat int64) {
	return m.sub, m.jti, m.iat
}

func (m *mockConnection) Transport() string {
	if m.transport == "" {
		return TransportWS
	}
	return m.transport
}

func TestConnectionRegistry_RegisterAndFindByJTI(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()
	conn := &mockConnection{sub: "alice", jti: "tok-1", iat: 100}

	r.Register(conn, "acme", "alice", "tok-1")

	found := r.FindByJTI("tok-1")
	if found != conn {
		t.Error("FindByJTI should return the registered connection")
	}
}

func TestConnectionRegistry_FindByJTI_NotFound(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()

	if r.FindByJTI("nonexistent") != nil {
		t.Error("FindByJTI should return nil for unknown jti")
	}
}

func TestConnectionRegistry_FindBySub(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()
	conn1 := &mockConnection{sub: "alice", jti: "tok-1", iat: 100}
	conn2 := &mockConnection{sub: "alice", jti: "tok-2", iat: 200}

	r.Register(conn1, "acme", "alice", "tok-1")
	r.Register(conn2, "acme", "alice", "tok-2")

	conns := r.FindBySub("acme", "alice")
	if len(conns) != 2 {
		t.Errorf("FindBySub returned %d connections, want 2", len(conns))
	}
}

func TestConnectionRegistry_FindBySub_TenantIsolation(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()
	conn1 := &mockConnection{sub: "alice", jti: "tok-1", iat: 100}
	conn2 := &mockConnection{sub: "alice", jti: "tok-2", iat: 200}

	r.Register(conn1, "acme", "alice", "tok-1")
	r.Register(conn2, "globex", "alice", "tok-2")

	acmeConns := r.FindBySub("acme", "alice")
	if len(acmeConns) != 1 {
		t.Errorf("FindBySub(acme) = %d, want 1", len(acmeConns))
	}

	globexConns := r.FindBySub("globex", "alice")
	if len(globexConns) != 1 {
		t.Errorf("FindBySub(globex) = %d, want 1", len(globexConns))
	}
}

func TestConnectionRegistry_Unregister(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()
	conn := &mockConnection{sub: "alice", jti: "tok-1", iat: 100}

	r.Register(conn, "acme", "alice", "tok-1")
	r.Unregister(conn, "acme", "alice", "tok-1")

	if r.FindByJTI("tok-1") != nil {
		t.Error("FindByJTI should return nil after unregister")
	}
	if len(r.FindBySub("acme", "alice")) != 0 {
		t.Error("FindBySub should return empty after unregister")
	}
}

func TestConnectionRegistry_Unregister_CleansEmptySubMap(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()
	conn := &mockConnection{sub: "alice", jti: "tok-1", iat: 100}

	r.Register(conn, "acme", "alice", "tok-1")
	r.Unregister(conn, "acme", "alice", "tok-1")

	r.mu.RLock()
	_, exists := r.bySubject["acme:alice"]
	r.mu.RUnlock()
	if exists {
		t.Error("empty subject map entry should be cleaned up")
	}
}

func TestConnectionRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	r := NewConnectionRegistry()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			conn := &mockConnection{sub: "alice", jti: "tok-" + string(rune('0'+i%10)), iat: int64(i)}
			r.Register(conn, "acme", "alice", conn.jti)
			_ = r.FindByJTI(conn.jti)
			_ = r.FindBySub("acme", "alice")
			r.Unregister(conn, "acme", "alice", conn.jti)
		})
	}
	wg.Wait()
}
