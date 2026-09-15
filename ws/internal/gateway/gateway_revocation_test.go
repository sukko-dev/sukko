package gateway

import (
	"testing"

	"github.com/rs/zerolog"
	provapi "github.com/sukko-dev/sukko/internal/shared/provapi"
)

func TestGateway_HandleRevocation_NilRegistry(t *testing.T) {
	t.Parallel()
	gw := &Gateway{connectionRegistry: nil, logger: zerolog.Nop()}
	// Must not panic with a nil registry.
	gw.HandleRevocation(provapi.RevocationEntry{Type: "token", JTI: "jti-1"})
}

func TestGateway_HandleRevocation_TokenType(t *testing.T) {
	t.Parallel()

	reg := NewConnectionRegistry()
	conn1 := &mockConnection{jti: "jti-1", sub: "user-1", iat: 1000}
	conn2 := &mockConnection{jti: "jti-2", sub: "user-1", iat: 1000}
	reg.Register(conn1, "t1", "user-1", "jti-1")
	reg.Register(conn2, "t1", "user-1", "jti-2")

	gw := &Gateway{connectionRegistry: reg, logger: zerolog.Nop()}
	gw.HandleRevocation(provapi.RevocationEntry{
		Type: "token", TenantID: "t1", JTI: "jti-1",
	})

	conn1.mu.Lock()
	closed1 := conn1.closed
	conn1.mu.Unlock()
	if !closed1 {
		t.Error("conn1 (matching JTI) should be force-closed")
	}

	conn2.mu.Lock()
	closed2 := conn2.closed
	conn2.mu.Unlock()
	if closed2 {
		t.Error("conn2 (different JTI) should not be closed")
	}
}

func TestGateway_HandleRevocation_TokenNotFound(t *testing.T) {
	t.Parallel()

	reg := NewConnectionRegistry()
	conn := &mockConnection{jti: "jti-1", sub: "user-1", iat: 1000}
	reg.Register(conn, "t1", "user-1", "jti-1")

	gw := &Gateway{connectionRegistry: reg, logger: zerolog.Nop()}
	// Revoke a JTI that doesn't exist — must not panic or close anything.
	gw.HandleRevocation(provapi.RevocationEntry{
		Type: "token", TenantID: "t1", JTI: "jti-unknown",
	})

	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Error("unrelated connection should not be closed")
	}
}

func TestGateway_HandleRevocation_UserType_ClosesOldTokens(t *testing.T) {
	t.Parallel()

	reg := NewConnectionRegistry()
	// iat=900 was issued before revocation — should be closed.
	old := &mockConnection{jti: "jti-old", sub: "user-1", iat: 900}
	// iat=1100 was issued after revocation (re-enabled user) — should NOT be closed.
	recent := &mockConnection{jti: "jti-new", sub: "user-1", iat: 1100}
	// Same iat as RevokedAt — boundary: iat >= RevokedAt means re-enabled, skip.
	boundary := &mockConnection{jti: "jti-boundary", sub: "user-1", iat: 1000}
	reg.Register(old, "t1", "user-1", "jti-old")
	reg.Register(recent, "t1", "user-1", "jti-new")
	reg.Register(boundary, "t1", "user-1", "jti-boundary")

	gw := &Gateway{connectionRegistry: reg, logger: zerolog.Nop()}
	gw.HandleRevocation(provapi.RevocationEntry{
		Type: "user", TenantID: "t1", Sub: "user-1", RevokedAt: 1000,
	})

	old.mu.Lock()
	closedOld := old.closed
	old.mu.Unlock()
	if !closedOld {
		t.Error("connection with iat < revokedAt should be force-closed")
	}

	recent.mu.Lock()
	closedRecent := recent.closed
	recent.mu.Unlock()
	if closedRecent {
		t.Error("connection with iat > revokedAt (re-enabled user) should not be closed")
	}

	boundary.mu.Lock()
	closedBoundary := boundary.closed
	boundary.mu.Unlock()
	if closedBoundary {
		t.Error("connection with iat == revokedAt should not be closed (re-enabled boundary)")
	}
}

func TestGateway_HandleRevocation_UserType_CrossTenantIsolation(t *testing.T) {
	t.Parallel()

	reg := NewConnectionRegistry()
	t1Conn := &mockConnection{jti: "jti-t1", sub: "user-1", iat: 900}
	t2Conn := &mockConnection{jti: "jti-t2", sub: "user-1", iat: 900}
	reg.Register(t1Conn, "t1", "user-1", "jti-t1")
	reg.Register(t2Conn, "t2", "user-1", "jti-t2")

	gw := &Gateway{connectionRegistry: reg, logger: zerolog.Nop()}
	// Revoke user-1 in tenant t1 only.
	gw.HandleRevocation(provapi.RevocationEntry{
		Type: "user", TenantID: "t1", Sub: "user-1", RevokedAt: 1000,
	})

	t1Conn.mu.Lock()
	closedT1 := t1Conn.closed
	t1Conn.mu.Unlock()
	if !closedT1 {
		t.Error("t1 connection should be force-closed")
	}

	t2Conn.mu.Lock()
	closedT2 := t2Conn.closed
	t2Conn.mu.Unlock()
	if closedT2 {
		t.Error("t2 connection (different tenant) should not be closed")
	}
}
