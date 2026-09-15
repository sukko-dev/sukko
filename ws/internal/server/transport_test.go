package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"google.golang.org/grpc/metadata"
)

// mockConn implements net.Conn for testing WebSocketTransport.
type mockConn struct {
	written    []byte
	writeErr   error
	closeErr   error
	closed     bool
	remoteAddr net.Addr
}

func (m *mockConn) Read(b []byte) (n int, err error) { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.written = append(m.written, b...)
	return len(b), nil
}
func (m *mockConn) Close() error {
	m.closed = true
	return m.closeErr
}
func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return m.remoteAddr }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func newMockConn() *mockConn {
	return &mockConn{
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345},
	}
}

func TestWebSocketTransport_Send(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		msg       OutgoingMsg
		writeErr  error
		wantBytes int
		wantErr   bool
	}{
		{
			name:      "raw message",
			msg:       RawMsg([]byte(`{"type":"ack"}`)),
			wantBytes: 14,
		},
		{
			name:      "nil message",
			msg:       OutgoingMsg{}, // zero value — Bytes() returns nil
			wantBytes: 0,
		},
		{
			name:     "write error on flush",
			msg:      RawMsg([]byte(`test`)),
			writeErr: errors.New("connection reset"),
			wantErr:  true,
			// wsutil writes to bufio.Writer (succeeds in buffer),
			// error surfaces on Flush() — bytes were "written" to buffer
			wantBytes: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn := newMockConn()
			conn.writeErr = tt.writeErr
			transport := NewWebSocketTransport(conn)

			n, err := transport.Send(tt.msg)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if n != tt.wantBytes {
				t.Errorf("bytes = %d, want %d", n, tt.wantBytes)
			}
		})
	}
}

func TestWebSocketTransport_SendBatch(t *testing.T) {
	t.Parallel()

	conn := newMockConn()
	transport := NewWebSocketTransport(conn)

	msgs := []OutgoingMsg{
		RawMsg([]byte(`{"type":"msg1"}`)),
		{}, // nil — should be skipped
		RawMsg([]byte(`{"type":"msg2"}`)),
	}

	n, err := transport.SendBatch(msgs)
	if err != nil {
		t.Fatalf("SendBatch error: %v", err)
	}

	// Two messages written (nil skipped)
	wantBytes := len(`{"type":"msg1"}`) + len(`{"type":"msg2"}`)
	if n != wantBytes {
		t.Errorf("bytes = %d, want %d", n, wantBytes)
	}
}

func TestWebSocketTransport_SendBatch_WriteError(t *testing.T) {
	t.Parallel()

	conn := newMockConn()
	conn.writeErr = errors.New("broken pipe")
	transport := NewWebSocketTransport(conn)

	msgs := []OutgoingMsg{
		RawMsg([]byte(`test`)),
	}

	_, err := transport.SendBatch(msgs)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestWebSocketTransport_Close(t *testing.T) {
	t.Parallel()

	conn := newMockConn()
	transport := NewWebSocketTransport(conn)

	err := transport.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
	if !conn.closed {
		t.Error("expected conn to be closed")
	}
}

func TestWebSocketTransport_Close_NilConn(t *testing.T) {
	t.Parallel()

	transport := &WebSocketTransport{conn: nil}
	err := transport.Close()
	if err != nil {
		t.Errorf("Close on nil conn should return nil, got: %v", err)
	}
}

func TestWebSocketTransport_Type(t *testing.T) {
	t.Parallel()

	conn := newMockConn()
	transport := NewWebSocketTransport(conn)

	if transport.Type() != TransportWebSocket {
		t.Errorf("Type() = %q, want %q", transport.Type(), TransportWebSocket)
	}
}

func TestWebSocketTransport_RemoteAddr(t *testing.T) {
	t.Parallel()

	conn := newMockConn()
	transport := NewWebSocketTransport(conn)

	addr := transport.RemoteAddr()
	if addr == "" {
		t.Error("RemoteAddr() should not be empty")
	}
}

func TestWebSocketTransport_RemoteAddr_NilConn(t *testing.T) {
	t.Parallel()

	transport := &WebSocketTransport{conn: nil}
	if transport.RemoteAddr() != "" {
		t.Error("RemoteAddr() on nil conn should be empty")
	}
}

func TestTransportType_Labels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tt      TransportType
		wantStr string
	}{
		{"websocket", TransportWebSocket, "ws"},
		{"grpc_stream", TransportGRPCStream, "sse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if string(tt.tt) != tt.wantStr {
				t.Errorf("TransportType = %q, want %q", string(tt.tt), tt.wantStr)
			}
		})
	}
}

func TestClient_TransportType(t *testing.T) {
	t.Parallel()

	t.Run("with transport", func(t *testing.T) {
		t.Parallel()
		conn := newMockConn()
		client := &Client{transport: NewWebSocketTransport(conn)}
		if client.TransportType() != TransportWebSocket {
			t.Errorf("TransportType() = %q, want %q", client.TransportType(), TransportWebSocket)
		}
	})

	t.Run("nil transport", func(t *testing.T) {
		t.Parallel()
		client := &Client{transport: nil}
		if client.TransportType() != "" {
			t.Errorf("TransportType() on nil transport should be empty, got %q", client.TransportType())
		}
	})
}

// =============================================================================
// GRPCStreamTransport Tests
// =============================================================================

// mockSubscribeServer implements RealtimeService_SubscribeServer for testing.
type mockSubscribeServer struct {
	mu      sync.Mutex
	sent    []*serverv1.SubscribeResponse
	sendErr error
}

func (m *mockSubscribeServer) Send(resp *serverv1.SubscribeResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	// Simulate gRPC's synchronous proto.Marshal: copy payload before storing so
	// tests remain correct when the transport reuses a buffer across SendBatch calls.
	payload := make([]byte, len(resp.GetPayload()))
	copy(payload, resp.GetPayload())
	m.mu.Lock()
	m.sent = append(m.sent, &serverv1.SubscribeResponse{
		Sequence: resp.GetSequence(),
		Payload:  payload,
	})
	m.mu.Unlock()
	return nil
}

// Implement the grpc.ServerStreamingServer[SubscribeResponse] interface stubs.
func (m *mockSubscribeServer) SendHeader(_ metadata.MD) error { return nil }
func (m *mockSubscribeServer) SetHeader(_ metadata.MD) error  { return nil }
func (m *mockSubscribeServer) SetTrailer(_ metadata.MD)       {}
func (m *mockSubscribeServer) Context() context.Context       { return context.Background() }
func (m *mockSubscribeServer) SendMsg(_ any) error            { return nil }
func (m *mockSubscribeServer) RecvMsg(_ any) error            { return nil }

func TestGRPCStreamTransport_Send(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		msg       OutgoingMsg
		sendErr   error
		wantBytes int
		wantErr   bool
		wantSent  int
	}{
		{
			name:      "raw message",
			msg:       RawMsg([]byte(`{"type":"ack"}`)),
			wantBytes: 14,
			wantSent:  1,
		},
		{
			name:      "nil message",
			msg:       OutgoingMsg{},
			wantBytes: 0,
			wantSent:  0,
		},
		{
			name:     "send error",
			msg:      RawMsg([]byte(`test`)),
			sendErr:  errors.New("stream closed"),
			wantErr:  true,
			wantSent: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock := &mockSubscribeServer{sendErr: tt.sendErr}
			_, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := NewGRPCStreamTransport(mock, cancel, "192.168.1.1:12345")

			n, err := transport.Send(tt.msg)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if n != tt.wantBytes {
				t.Errorf("bytes = %d, want %d", n, tt.wantBytes)
			}
			if len(mock.sent) != tt.wantSent {
				t.Errorf("sent count = %d, want %d", len(mock.sent), tt.wantSent)
			}
		})
	}
}

func TestGRPCStreamTransport_Send_SequenceAndPayload(t *testing.T) {
	t.Parallel()

	mock := &mockSubscribeServer{}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := NewGRPCStreamTransport(mock, cancel, "10.0.0.1")

	// Create an OutgoingMsg with a sequence number (simulating broadcast message)
	msg := OutgoingMsg{
		raw: []byte(`{"type":"message","seq":42,"channel":"test"}`),
		seq: 42,
	}

	n, err := transport.Send(msg)
	if err != nil {
		t.Fatalf("Send error: %v", err)
	}
	if n != len(msg.raw) {
		t.Errorf("bytes = %d, want %d", n, len(msg.raw))
	}
	if len(mock.sent) != 1 {
		t.Fatalf("sent count = %d, want 1", len(mock.sent))
	}
	if mock.sent[0].GetSequence() != 42 {
		t.Errorf("sequence = %d, want 42", mock.sent[0].GetSequence())
	}
	if !bytes.Equal(mock.sent[0].GetPayload(), msg.raw) {
		t.Errorf("payload = %q, want %q", mock.sent[0].GetPayload(), msg.raw)
	}
}

func TestGRPCStreamTransport_SendBatch(t *testing.T) {
	t.Parallel()

	mock := &mockSubscribeServer{}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := NewGRPCStreamTransport(mock, cancel, "10.0.0.1")

	msgs := []OutgoingMsg{
		RawMsg([]byte(`msg1`)),
		{}, // nil — skipped
		RawMsg([]byte(`msg2`)),
	}

	n, err := transport.SendBatch(msgs)
	if err != nil {
		t.Fatalf("SendBatch error: %v", err)
	}
	wantBytes := len("msg1") + len("msg2")
	if n != wantBytes {
		t.Errorf("bytes = %d, want %d", n, wantBytes)
	}
	if len(mock.sent) != 2 {
		t.Errorf("sent count = %d, want 2", len(mock.sent))
	}
}

func TestGRPCStreamTransport_Close(t *testing.T) {
	t.Parallel()

	canceled := false
	transport := NewGRPCStreamTransport(
		&mockSubscribeServer{},
		func() { canceled = true },
		"10.0.0.1",
	)

	err := transport.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
	if !canceled {
		t.Error("expected cancel to be called")
	}
}

func TestGRPCStreamTransport_Type(t *testing.T) {
	t.Parallel()

	transport := &GRPCStreamTransport{}
	if transport.Type() != TransportGRPCStream {
		t.Errorf("Type() = %q, want %q", transport.Type(), TransportGRPCStream)
	}
}

func TestGRPCStreamTransport_RemoteAddr(t *testing.T) {
	t.Parallel()

	transport := NewGRPCStreamTransport(&mockSubscribeServer{}, func() {}, "192.168.1.100:8080")
	if transport.RemoteAddr() != "192.168.1.100:8080" {
		t.Errorf("RemoteAddr() = %q, want %q", transport.RemoteAddr(), "192.168.1.100:8080")
	}
}

func TestGRPCStreamTransport_NoOps(t *testing.T) {
	t.Parallel()

	transport := &GRPCStreamTransport{}

	if err := transport.WritePing(); err != nil {
		t.Errorf("WritePing should be no-op, got: %v", err)
	}
	if err := transport.WritePong([]byte("test")); err != nil {
		t.Errorf("WritePong should be no-op, got: %v", err)
	}
	if err := transport.SetWriteDeadline(time.Now()); err != nil {
		t.Errorf("SetWriteDeadline should be no-op, got: %v", err)
	}
}

// =============================================================================
// Envelope dispatch tests (gate)
// =============================================================================

// discardConn is a net.Conn whose Write delegates to io.Discard.
// Used for allocation tests where mockConn would itself allocate (append to written slice).
type discardConn struct{}

func (discardConn) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (discardConn) Write(b []byte) (int, error)        { return io.Discard.Write(b) }
func (discardConn) Close() error                       { return nil }
func (discardConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (discardConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (discardConn) SetDeadline(_ time.Time) error      { return nil }
func (discardConn) SetReadDeadline(_ time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(_ time.Time) error { return nil }

// TestWebSocketTransport_EnvelopeSend verifies that envelope messages are
// assembled correctly and that SendBatch buffer reuse does not corrupt earlier frames.
func TestWebSocketTransport_EnvelopeSend(t *testing.T) {
	t.Parallel()

	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}
	want42 := env.AppendTo(nil, 42)
	want43 := env.AppendTo(nil, 43)

	t.Run("Send", func(t *testing.T) {
		t.Parallel()
		conn := newMockConn()
		tr := NewWebSocketTransport(conn)

		if _, err := tr.Send(OutgoingMsg{envelope: env, seq: 42}); err != nil {
			t.Fatalf("Send error: %v", err)
		}

		frame, err := ws.ReadFrame(bytes.NewReader(conn.written))
		if err != nil {
			t.Fatalf("ReadFrame error: %v", err)
		}
		if !bytes.Equal(frame.Payload, want42) {
			t.Errorf("frame payload = %q, want %q", frame.Payload, want42)
		}
	})

	t.Run("SendBatch buffer reuse", func(t *testing.T) {
		t.Parallel()
		conn := newMockConn()
		tr := NewWebSocketTransport(conn)

		msgs := []OutgoingMsg{
			{envelope: env, seq: 42},
			{envelope: env, seq: 43},
		}
		if _, err := tr.SendBatch(msgs); err != nil {
			t.Fatalf("SendBatch error: %v", err)
		}

		r := bytes.NewReader(conn.written)
		frame1, err := ws.ReadFrame(r)
		if err != nil {
			t.Fatalf("ReadFrame frame1 error: %v", err)
		}
		frame2, err := ws.ReadFrame(r)
		if err != nil {
			t.Fatalf("ReadFrame frame2 error: %v", err)
		}
		if !bytes.Equal(frame1.Payload, want42) {
			t.Errorf("frame1 payload = %q, want %q", frame1.Payload, want42)
		}
		if !bytes.Equal(frame2.Payload, want43) {
			t.Errorf("frame2 payload = %q, want %q", frame2.Payload, want43)
		}
	})
}

// TestGRPCStreamTransport_EnvelopeSend verifies envelope messages are
// delivered with the correct Sequence and Payload on the gRPC stream.
func TestGRPCStreamTransport_EnvelopeSend(t *testing.T) {
	t.Parallel()

	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}
	want42 := env.AppendTo(nil, 42)
	want43 := env.AppendTo(nil, 43)

	t.Run("Send", func(t *testing.T) {
		t.Parallel()
		mock := &mockSubscribeServer{}
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		tr := NewGRPCStreamTransport(mock, cancel, "10.0.0.1")

		if _, err := tr.Send(OutgoingMsg{envelope: env, seq: 42}); err != nil {
			t.Fatalf("Send error: %v", err)
		}

		if len(mock.sent) != 1 {
			t.Fatalf("sent count = %d, want 1", len(mock.sent))
		}
		if mock.sent[0].GetSequence() != 42 {
			t.Errorf("sequence = %d, want 42", mock.sent[0].GetSequence())
		}
		if !bytes.Equal(mock.sent[0].GetPayload(), want42) {
			t.Errorf("payload = %q, want %q", mock.sent[0].GetPayload(), want42)
		}
	})

	t.Run("SendBatch", func(t *testing.T) {
		t.Parallel()
		mock := &mockSubscribeServer{}
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		tr := NewGRPCStreamTransport(mock, cancel, "10.0.0.1")

		msgs := []OutgoingMsg{
			{envelope: env, seq: 42},
			{envelope: env, seq: 43},
		}
		if _, err := tr.SendBatch(msgs); err != nil {
			t.Fatalf("SendBatch error: %v", err)
		}

		if len(mock.sent) != 2 {
			t.Fatalf("sent count = %d, want 2", len(mock.sent))
		}
		if mock.sent[0].GetSequence() != 42 {
			t.Errorf("msg0 sequence = %d, want 42", mock.sent[0].GetSequence())
		}
		if !bytes.Equal(mock.sent[0].GetPayload(), want42) {
			t.Errorf("msg0 payload = %q, want %q", mock.sent[0].GetPayload(), want42)
		}
		if mock.sent[1].GetSequence() != 43 {
			t.Errorf("msg1 sequence = %d, want 43", mock.sent[1].GetSequence())
		}
		if !bytes.Equal(mock.sent[1].GetPayload(), want43) {
			t.Errorf("msg1 payload = %q, want %q", mock.sent[1].GetPayload(), want43)
		}
	})
}

// TestWebSocketTransport_BufMonotonicGrowth verifies that buf capacity is
// monotonically non-decreasing: buf[:0] resets length but preserves the backing array.
func TestWebSocketTransport_BufMonotonicGrowth(t *testing.T) {
	t.Parallel()

	// Large payload to force buf growth past initialBufCap (512).
	largePayload := append(append([]byte{'"'}, bytes.Repeat([]byte("x"), 600)...), '"')
	bigEnv, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, largePayload, "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope large: %v", err)
	}
	smallEnv, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope small: %v", err)
	}

	tr := NewWebSocketTransport(discardConn{})

	if _, err := tr.Send(OutgoingMsg{envelope: bigEnv, seq: 1}); err != nil {
		t.Fatalf("Send large: %v", err)
	}
	capAfterLarge := cap(tr.buf) // tr.buf accessible: package server internal test
	if capAfterLarge <= initialBufCap {
		t.Fatalf("buf cap %d did not grow past initialBufCap %d after large send", capAfterLarge, initialBufCap)
	}

	if _, err := tr.Send(OutgoingMsg{envelope: smallEnv, seq: 2}); err != nil {
		t.Fatalf("Send small: %v", err)
	}
	if cap(tr.buf) != capAfterLarge {
		t.Errorf("buf cap changed after small send: got %d, want %d (monotonic growth)", cap(tr.buf), capAfterLarge)
	}
}

// TestWebSocketTransport_ZeroAllocsSend (gate) verifies envelope dispatch adds
// no allocations beyond the single gobwas/ws frame header buffer.
func TestWebSocketTransport_ZeroAllocsSend(t *testing.T) { //nolint:paralleltest // testing.AllocsPerRun is sensitive to GC pressure from concurrent goroutines; intentionally non-parallel
	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}
	msg := OutgoingMsg{envelope: env, seq: 42}

	tr := NewWebSocketTransport(discardConn{})

	// Pre-warm: allocates the reusable buffer.
	if _, err := tr.Send(msg); err != nil {
		t.Fatalf("pre-warm Send error: %v", err)
	}

	got := testing.AllocsPerRun(100, func() {
		_, _ = tr.Send(msg)
	})
	if got > 1 {
		t.Errorf("Send allocations = %v, want <= 1 (gobwas/ws header budget)", got)
	}
}
