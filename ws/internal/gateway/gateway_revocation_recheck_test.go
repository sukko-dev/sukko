package gateway

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	provapi "github.com/sukko-dev/sukko/internal/shared/provapi"
)

// mockRevocationChecker is a snapshot-backed RevocationChecker for deterministic tests. Its
// IsRevoked mirrors the real StreamRevocationRegistry.IsRevoked semantics exactly: jti match is
// tenant-scoped and NOT iat-gated; sub match requires iat < revokedAt. Mutators let a test store a
// revocation at a controlled point (so the recovery case discriminates on cause, not tautology).
type mockRevocationChecker struct {
	jti map[string]string // jti -> tenantID
	sub map[string]int64  // "tenant:sub" -> revokedAt
}

func newMockRevocationChecker() *mockRevocationChecker {
	return &mockRevocationChecker{jti: map[string]string{}, sub: map[string]int64{}}
}

func (m *mockRevocationChecker) revokeJTI(jti, tenant string)           { m.jti[jti] = tenant }
func (m *mockRevocationChecker) revokeSub(tenant, sub string, at int64) { m.sub[tenant+":"+sub] = at }
func (m *mockRevocationChecker) Close() error                           { return nil }

func (m *mockRevocationChecker) IsRevoked(jti, sub, tenantID string, iat int64) bool {
	if jti != "" {
		if t, ok := m.jti[jti]; ok && t == tenantID {
			return true
		}
	}
	if sub != "" {
		if revokedAt, ok := m.sub[tenantID+":"+sub]; ok && iat < revokedAt {
			return true
		}
	}
	return false
}

// TestGateway_PostRegisterRecheck_Matrix covers the re-check verdict across
// {jti-revocation, sub-revocation} × {ws, sse} × {revoked→closed, unrevoked→not, iat-boundary}.
func TestGateway_PostRegisterRecheck_Matrix(t *testing.T) {
	t.Parallel()
	const tenant = "t1"
	tests := []struct {
		name       string
		transport  string
		seed       func(c *mockRevocationChecker)
		conn       *mockConnection
		wantClosed bool
		wantReason string
	}{
		{"jti revoked / ws", TransportWS,
			func(c *mockRevocationChecker) { c.revokeJTI("jti-1", tenant) },
			&mockConnection{jti: "jti-1", sub: "u1", iat: 100}, true, CloseReasonTokenRevoked},
		{"jti revoked / sse", TransportSSE,
			func(c *mockRevocationChecker) { c.revokeJTI("jti-1", tenant) },
			&mockConnection{jti: "jti-1", sub: "u1", iat: 100, transport: TransportSSE}, true, CloseReasonTokenRevoked},
		{"jti revoked in OTHER tenant / not closed", TransportWS,
			func(c *mockRevocationChecker) { c.revokeJTI("jti-1", "other") },
			&mockConnection{jti: "jti-1", sub: "u1", iat: 100}, false, ""},
		{"sub revoked, iat < revokedAt / ws", TransportWS,
			func(c *mockRevocationChecker) { c.revokeSub(tenant, "u1", 1000) },
			&mockConnection{jti: "jti-x", sub: "u1", iat: 900}, true, CloseReasonUserRevoked},
		{"sub revoked, iat < revokedAt / sse", TransportSSE,
			func(c *mockRevocationChecker) { c.revokeSub(tenant, "u1", 1000) },
			&mockConnection{jti: "jti-x", sub: "u1", iat: 900, transport: TransportSSE}, true, CloseReasonUserRevoked},
		{"sub revoked, iat == revokedAt / not closed (boundary)", TransportWS,
			func(c *mockRevocationChecker) { c.revokeSub(tenant, "u1", 1000) },
			&mockConnection{jti: "jti-x", sub: "u1", iat: 1000}, false, ""},
		{"sub revoked, iat > revokedAt / not closed", TransportWS,
			func(c *mockRevocationChecker) { c.revokeSub(tenant, "u1", 1000) },
			&mockConnection{jti: "jti-x", sub: "u1", iat: 1100}, false, ""},
		{"unrevoked / not closed", TransportWS,
			func(c *mockRevocationChecker) {},
			&mockConnection{jti: "jti-x", sub: "u1", iat: 100}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			chk := newMockRevocationChecker()
			tt.seed(chk)
			gw := &Gateway{revocationRegistry: chk, logger: zerolog.Nop()}
			gw.recheckRevocationAfterRegister(tt.conn, tenant)

			tt.conn.mu.Lock()
			closed, reason, code := tt.conn.closed, tt.conn.lastReason, tt.conn.lastCode
			tt.conn.mu.Unlock()
			if closed != tt.wantClosed {
				t.Fatalf("closed=%v, want %v", closed, tt.wantClosed)
			}
			if tt.wantClosed {
				if reason != tt.wantReason {
					t.Errorf("reason=%q, want %q", reason, tt.wantReason)
				}
				if code != 1008 { // ws.StatusPolicyViolation
					t.Errorf("close code=%d, want 1008", code)
				}
			}
		})
	}
}

// TestGateway_PostRegisterRecheck_RecoversMissedConnection is the direct bug reproduction: the
// fan-out runs while the connection is UNregistered (so FindByJTI misses it — the connection is
// NOT closed by the fan-out), then the connection registers and the post-register re-check closes
// it. The close is credited to the pre-stored snapshot (cause), not a tautology.
func TestGateway_PostRegisterRecheck_RecoversMissedConnection(t *testing.T) {
	t.Parallel()
	const tenant = "t1"
	reg := NewConnectionRegistry()
	chk := newMockRevocationChecker()
	gw := &Gateway{connectionRegistry: reg, revocationRegistry: chk, logger: zerolog.Nop()}

	conn := &mockConnection{jti: "jti-late", sub: "u1", iat: 100}

	// Revoke fans out BEFORE the connection registers → fan-out FindByJTI misses it.
	chk.revokeJTI("jti-late", tenant)
	gw.HandleRevocation(provapi.RevocationEntry{Type: provapi.RevocationTypeToken, TenantID: tenant, JTI: "jti-late"})
	conn.mu.Lock()
	missedByFanout := !conn.closed
	conn.mu.Unlock()
	if !missedByFanout {
		t.Fatal("precondition: unregistered connection must be MISSED by the fan-out")
	}

	// Now it registers and the post-register re-check must recover it.
	reg.Register(conn, tenant, "u1", "jti-late")
	gw.recheckRevocationAfterRegister(conn, tenant)
	conn.mu.Lock()
	closed, count := conn.closed, conn.closeCount
	conn.mu.Unlock()
	if !closed {
		t.Fatal("post-register re-check MUST force-close the connection the fan-out missed")
	}
	if count != 1 {
		t.Errorf("closeCount=%d, want 1 (only the re-check closes)", count)
	}
}

// TestGateway_PostRegisterRecheck_NilRevocationRegistry: a gateway with a nil revocationRegistry
// (Community / provisioning not wired) must make the re-check a no-op — no panic. Distinct from
// the existing nil-connectionRegistry test; gates Community-edition connection liveness.
func TestGateway_PostRegisterRecheck_NilRevocationRegistry(t *testing.T) {
	t.Parallel()
	gw := &Gateway{connectionRegistry: NewConnectionRegistry(), revocationRegistry: nil, logger: zerolog.Nop()}
	conn := &mockConnection{jti: "j", sub: "u", iat: 1}
	gw.recheckRevocationAfterRegister(conn, "t1") // must not panic
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Error("re-check with nil revocationRegistry must be a no-op")
	}
}

// TestGateway_PostRegisterRecheck_Attribution: revocation_type is token when jti matches, user
// when only sub matches, token when both match (jti-first, mirrors IsRevoked order).
func TestGateway_PostRegisterRecheck_Attribution(t *testing.T) {
	t.Parallel()
	const tenant = "t1"
	tests := []struct {
		name     string
		seed     func(c *mockRevocationChecker)
		wantType string
	}{
		{"jti only → token", func(c *mockRevocationChecker) { c.revokeJTI("j1", tenant) }, provapi.RevocationTypeToken},
		{"sub only → user", func(c *mockRevocationChecker) { c.revokeSub(tenant, "u1", 1000) }, provapi.RevocationTypeUser},
		{"both → token (jti-first)", func(c *mockRevocationChecker) {
			c.revokeJTI("j1", tenant)
			c.revokeSub(tenant, "u1", 1000)
		}, provapi.RevocationTypeToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			chk := newMockRevocationChecker()
			tt.seed(chk)
			var buf bytes.Buffer
			gw := &Gateway{revocationRegistry: chk, logger: zerolog.New(&buf)}
			gw.recheckRevocationAfterRegister(&mockConnection{jti: "j1", sub: "u1", iat: 100}, tenant)
			if !strings.Contains(buf.String(), `"revocation_type":"`+tt.wantType+`"`) {
				t.Errorf("log revocation_type != %q; got %s", tt.wantType, buf.String())
			}
		})
	}
}

// TestGateway_PostRegisterRecheck_Observability asserts both the detection metric label and the
// structured recovery log (structured recovery log). Non-parallel: reads a package-global CounterVec child.
func TestGateway_PostRegisterRecheck_Observability(t *testing.T) { //nolint:paralleltest // reads a global metric child
	const tenant = "t1"
	chk := newMockRevocationChecker()
	chk.revokeJTI("j1", tenant)
	var buf bytes.Buffer
	gw := &Gateway{revocationRegistry: chk, logger: zerolog.New(&buf)}

	postChild := tokenForceDisconnectsTotal.WithLabelValues(provapi.RevocationTypeToken, TransportWS, DetectionPostRegister)
	fanChild := tokenForceDisconnectsTotal.WithLabelValues(provapi.RevocationTypeToken, TransportWS, DetectionFanOut)
	beforePost := testutil.ToFloat64(postChild)
	beforeFan := testutil.ToFloat64(fanChild)

	gw.recheckRevocationAfterRegister(&mockConnection{jti: "j1", sub: "u1", iat: 100}, tenant)

	if got := testutil.ToFloat64(postChild) - beforePost; got != 1 {
		t.Errorf("detection=post_register delta=%v, want 1", got)
	}
	if got := testutil.ToFloat64(fanChild) - beforeFan; got != 0 {
		t.Errorf("detection=fan_out delta=%v, want 0 (re-check must not attribute to fan_out)", got)
	}
	// Structured recovery log: mirrors fan-out fields + detection discriminator.
	s := buf.String()
	for _, want := range []string{`"detection":"post_register"`, `"transport":"ws"`, `"revocation_type":"token"`, `"jti":"j1"`, `"sub":"u1"`} {
		if !strings.Contains(s, want) {
			t.Errorf("recovery log missing %s; got %s", want, s)
		}
	}
	// SSE transport label is exercised too.
	gw.recheckRevocationAfterRegister(&mockConnection{jti: "j1", sub: "u1", iat: 100, transport: TransportSSE}, tenant)
	sseChild := tokenForceDisconnectsTotal.WithLabelValues(provapi.RevocationTypeToken, TransportSSE, DetectionPostRegister)
	if got := testutil.ToFloat64(sseChild); got < 1 {
		t.Errorf("detection=post_register transport=sse child=%v, want >=1", got)
	}
}

// TestGateway_PostRegisterRecheck_CrossPathIatBoundary: at iat == RevokedAt, the fan-out inline
// verdict and the re-check IsRevoked verdict AGREE (both skip). Note the jti tenant-scope
// asymmetry: the fan-out's FindByJTI is unscoped, whereas IsRevoked's jti branch is tenant-scoped.
func TestGateway_PostRegisterRecheck_CrossPathIatBoundary(t *testing.T) {
	t.Parallel()
	const tenant = "t1"
	// Fan-out path: registered conn at the boundary is NOT closed (iat >= RevokedAt skip).
	reg := NewConnectionRegistry()
	fanConn := &mockConnection{jti: "jb", sub: "u1", iat: 1000}
	reg.Register(fanConn, tenant, "u1", "jb")
	gwFan := &Gateway{connectionRegistry: reg, logger: zerolog.Nop()}
	gwFan.HandleRevocation(provapi.RevocationEntry{Type: provapi.RevocationTypeUser, TenantID: tenant, Sub: "u1", RevokedAt: 1000})
	fanConn.mu.Lock()
	fanClosed := fanConn.closed
	fanConn.mu.Unlock()

	// Re-check path: IsRevoked at the boundary also does NOT close.
	chk := newMockRevocationChecker()
	chk.revokeSub(tenant, "u1", 1000)
	gwRe := &Gateway{revocationRegistry: chk, logger: zerolog.Nop()}
	reConn := &mockConnection{jti: "jb", sub: "u1", iat: 1000}
	gwRe.recheckRevocationAfterRegister(reConn, tenant)
	reConn.mu.Lock()
	reClosed := reConn.closed
	reConn.mu.Unlock()

	if fanClosed || reClosed {
		t.Errorf("at iat==RevokedAt both paths must skip: fanClosed=%v reClosed=%v", fanClosed, reClosed)
	}
}

// --- real-Proxy double-close / before-Run tests (fakeConn — net.Pipe would block on the close-frame write) ---

// fakeConn is a non-blocking net.Conn: writes are discarded, reads return EOF, Close is counted.
type fakeConn struct {
	closeCount int
}

func (c *fakeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *fakeConn) Close() error                     { c.closeCount++; return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func newFakeConnProxy(client, backend net.Conn) *Proxy {
	return NewProxy(ProxyConfig{
		ClientConn: client, BackendConn: backend, Logger: zerolog.Nop(),
		PublishRateLimit: 10, PublishBurst: 10, AuthRefreshRateInterval: time.Minute,
	})
}

// TestProxy_ForceClose_DoubleClose_Idempotent: calling ForceClose twice must not panic
// and must close each underlying conn exactly once (guarded by closeOnce). The close-frame write
// running twice is expected/acceptable (sendCloseToClient is outside closeOnce, spec-accepted).
func TestProxy_ForceClose_DoubleClose_Idempotent(t *testing.T) {
	t.Parallel()
	cc, bc := &fakeConn{}, &fakeConn{}
	p := newFakeConnProxy(cc, bc)
	p.ForceClose(1008, CloseReasonUserRevoked)  // first caller
	p.ForceClose(1008, CloseReasonTokenRevoked) // second — must not panic, must NOT overwrite the reason
	if cc.closeCount != 1 || bc.closeCount != 1 {
		t.Errorf("closeOnce failed: client=%d backend=%d, want 1/1", cc.closeCount, bc.closeCount)
	}
	// First-reason-wins: the store lives inside closeOnce.Do, so the second (distinct) reason is dropped.
	if got := p.ForceCloseReason(); got != CloseReasonUserRevoked {
		t.Errorf("ForceCloseReason() = %q, want first reason %q (last-writer-wins bug)", got, CloseReasonUserRevoked)
	}
}

// TestProxy_ForceClose_ConcurrentWithRun: a late fan-out ForceClose fires concurrently with
// a normal Run teardown. Because the reason store lives inside the shared closeOnce.Do, this is
// race-free by construction (no mutex/atomic on the getter). The getter is read ONLY after BOTH
// goroutines join — never mid-flight. Run under -race.
func TestProxy_ForceClose_ConcurrentWithRun(t *testing.T) {
	t.Parallel()
	p := newFakeConnProxy(&fakeConn{}, &fakeConn{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.Run(context.Background()) }()
	go func() { defer wg.Done(); p.ForceClose(1008, CloseReasonTokenRevoked) }()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run/ForceClose did not complete")
	}
	// Read AFTER both goroutines joined. Either Run's closeOnce won (reason "") or ForceClose's did
	// ("token revoked") — both are valid; the point is no data race on the field.
	if r := p.ForceCloseReason(); r != "" && r != CloseReasonTokenRevoked {
		t.Errorf("ForceCloseReason() = %q, want \"\" or %q", r, CloseReasonTokenRevoked)
	}
}

// TestProxy_ForceCloseBeforeRun (edge): if the re-check force-closes before Run starts, Run must
// return promptly (pumps error on the closed conns), no panic, no goroutine leak.
func TestProxy_ForceCloseBeforeRun(t *testing.T) {
	t.Parallel()
	p := newFakeConnProxy(&fakeConn{}, &fakeConn{})
	p.ForceClose(1008, CloseReasonTokenRevoked)
	done := make(chan struct{})
	go func() { p.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy.Run did not return after a pre-Run ForceClose")
	}
}
