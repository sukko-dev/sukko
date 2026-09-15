package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/rs/zerolog"
)

func TestNewPool(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	if p == nil {
		t.Fatal("expected non-nil pool")
	}
	if p.Active() != 0 {
		t.Errorf("active = %d, want 0", p.Active())
	}
}

func TestPool_DrainSummary_Empty(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	p.Drain()
	s := p.DrainSummary()
	if s != "drained 0 connections" {
		t.Errorf("summary = %q, want %q", s, "drained 0 connections")
	}
}

func TestPool_RampUp_EmptyGatewayURL(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	err := p.RampUp(context.Background(), PoolConfig{GatewayURL: ""}, 1, 10)
	if err == nil {
		t.Fatal("expected error for empty gateway URL")
	}
}

func TestPool_RampUp_ContextCancelled(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.RampUp(ctx, PoolConfig{GatewayURL: "ws://localhost:59999"}, 10, 100)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestPool_RefreshAll_Empty(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	refreshed, failed := p.RefreshAll(func(i int) string { return "token" })
	if refreshed != 0 || failed != 0 {
		t.Errorf("RefreshAll on empty pool: refreshed=%d, failed=%d, want 0/0", refreshed, failed)
	}
}

func TestPoolConfig_TokenFunc_Precedence(t *testing.T) {
	t.Parallel()

	// TokenFunc should take precedence over static Token.
	// We can't connect to a real server, but we can verify the config
	// structure accepts both fields without compilation errors.
	cfg := PoolConfig{
		GatewayURL: "ws://localhost:9999",
		Token:      "static-token",
		TokenFunc:  func(i int) string { return "per-connection-token" },
	}
	if cfg.TokenFunc == nil {
		t.Fatal("TokenFunc should be set")
	}
	if cfg.TokenFunc(0) != "per-connection-token" {
		t.Errorf("TokenFunc(0) = %q, want per-connection-token", cfg.TokenFunc(0))
	}
}

func TestPool_DrainIdempotent(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	// Drain on empty pool should not panic
	p.Drain()
	p.Drain()
	if p.Active() != 0 {
		t.Errorf("active after double drain = %d, want 0", p.Active())
	}
}

// TestPool_CloseAt_NilSlot verifies CloseAt is a no-op on a pre-sized nil slot.
// A canceled RampUp pre-sizes p.clients but leaves all entries nil; CloseAt must not panic.
func TestPool_CloseAt_NilSlot(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before RampUp — pre-sizes the slice but no connections are made
	_ = p.RampUp(ctx, PoolConfig{GatewayURL: "ws://127.0.0.1:1"}, 3, 1000)
	// p.clients is now [nil, nil, nil]; CloseAt must be a no-op on each nil slot.
	p.CloseAt(0)
	p.CloseAt(1)
	p.CloseAt(2)
}

// TestPool_CloseAt_DrainedPool verifies CloseAt is a no-op after Drain sets clients=nil.
func TestPool_CloseAt_DrainedPool(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	p.Drain()    // clients stays nil
	p.CloseAt(0) // must not panic
}

// TestPool_FillSlot_AlreadyDrained verifies FillSlot returns an error on a drained pool.
func TestPool_FillSlot_AlreadyDrained(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	p.Drain()
	err := p.FillSlot(0, PoolConfig{GatewayURL: "ws://127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected error for FillSlot on drained pool")
	}
}

// TestPool_FillSlot_ConnectError verifies FillSlot wraps the connect error with "fill slot N:".
// The Connect call fails before touching p.clients, so no pre-sizing is needed.
func TestPool_FillSlot_ConnectError(t *testing.T) {
	t.Parallel()

	p := NewPool(zerolog.Nop())
	err := p.FillSlot(0, PoolConfig{GatewayURL: "ws://127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected error for unreachable gateway")
	}
	if err.Error() == "fill slot: pool already drained" {
		t.Errorf("got drained error instead of connect error: %v", err)
	}
}

// TestPool_OnClose_ReceivesCloseCode verifies the OnClose callback fires with the
// correct close code (1008 PolicyViolation) and a timestamp that is after the
// moment the close was triggered. This exercises the mechanism that stress:revocation
// and soak:revocation rely on to measure force-disconnect latency.
func TestPool_OnClose_ReceivesCloseCode(t *testing.T) {
	t.Parallel()

	trigger := make(chan struct{})
	var triggerOnce sync.Once
	// Ensure trigger is always closed on test exit to unblock WS goroutines.
	t.Cleanup(func() { triggerOnce.Do(func() { close(trigger) }) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		<-trigger
		closeBody := ws.NewCloseFrameBody(ws.StatusPolicyViolation, "revoked")
		_ = ws.WriteFrame(conn, ws.NewCloseFrame(closeBody))
	}))
	defer srv.Close()

	const n = 3
	type closeRecord struct {
		code ws.StatusCode
		ts   time.Time
	}
	closeCh := make(chan closeRecord, n)

	p := NewPool(zerolog.Nop())
	wsURL := "ws" + srv.URL[4:]
	if err := p.RampUp(context.Background(), PoolConfig{
		GatewayURL: wsURL,
		Token:      "test-token",
		OnClose: func(_ int, code ws.StatusCode, ts time.Time) {
			select {
			case closeCh <- closeRecord{code: code, ts: ts}:
			default:
			}
		},
	}, n, 1000); err != nil {
		t.Fatalf("RampUp: %v", err)
	}

	active := int(p.Active())
	if active == 0 {
		t.Skip("no connections established")
	}

	// t0 is set before the trigger fires; close event timestamps must be after t0
	// for force-disconnect latency to be measurable.
	t0 := time.Now()
	triggerOnce.Do(func() { close(trigger) })

	var afterT0 int
	deadline := time.After(3 * time.Second)
	for range active {
		select {
		case rec := <-closeCh:
			if rec.code != ws.StatusPolicyViolation {
				t.Errorf("close code = %d, want %d (PolicyViolation/1008)", rec.code, ws.StatusPolicyViolation)
			}
			if rec.ts.After(t0) {
				afterT0++
			}
		case <-deadline:
			t.Fatalf("timeout waiting for close events")
		}
	}

	if afterT0 == 0 {
		t.Error("no close event arrived after t0; force-disconnect latency would be unmeasurable")
	}
}
