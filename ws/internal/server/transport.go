package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
)

// TransportType identifies the transport protocol for metrics labeling.
// Low cardinality (2-3 values) per Constitution VI.
type TransportType string

const (
	// TransportWebSocket is the standard WebSocket transport.
	TransportWebSocket TransportType = "ws"

	// TransportGRPCStream is the gRPC server-streaming transport for SSE.
	TransportGRPCStream TransportType = "sse"
)

// initialBufCap is the initial capacity for each transport's reusable assembly buffer.
// 512 bytes covers typical broadcast frame sizes without over-allocating.
// Shared by both WebSocketTransport and GRPCStreamTransport.
const initialBufCap = 512

// closeFrameWriteTimeout is the maximum time allowed to write a WebSocket close frame.
// Prevents force-disconnect bulk paths from hanging on dead/slow clients.
const closeFrameWriteTimeout = 3 * time.Second

// Transport abstracts message delivery from the connection protocol.
// All transports (WebSocket, SSE/gRPC stream, future Web Push) implement this interface.
//
// The write pump calls Send/SendBatch with OutgoingMsg values — the transport
// resolves them to the wire format internally. OutgoingMsg is in the same package,
// so there is no cross-package type leak.
//
// The broadcast fan-out path (Valkey → subscription index → client.send channel)
// does NOT touch this interface. Only the per-client write pump goroutine calls
// these methods — no concurrent access to a single Transport instance.
type Transport interface {
	// Send delivers a single message and flushes. Returns bytes written.
	// WebSocket: msg.Bytes() → text frame → flush.
	// gRPC: msg.Bytes() → SubscribeResponse{Sequence, Payload} → stream.Send().
	Send(msg OutgoingMsg) (int, error)

	// SendBatch delivers multiple messages with a single flush. Returns total bytes written.
	// Preserves the batching optimization (one syscall per batch for WebSocket).
	// WebSocket: write N frames to bufio.Writer → single Flush().
	// gRPC: send N SubscribeResponses (gRPC handles its own framing).
	SendBatch(msgs []OutgoingMsg) (int, error)

	// WritePing sends a transport-level keepalive.
	// WebSocket: sends ping frame. gRPC: no-op (HTTP/2 PING handles keepalive).
	WritePing() error

	// WritePong sends a pong response with the given payload.
	// WebSocket: sends pong frame. gRPC: no-op.
	WritePong(payload []byte) error

	// Close terminates the transport connection.
	// WebSocket: sends close frame (1008 Policy Violation) then closes TCP.
	// gRPC: cancels the stream context.
	Close() error

	// CloseWithCode terminates the transport with a specific WebSocket close code and reason.
	// WebSocket: sends RFC 6455 close frame with the given code and reason, then closes TCP.
	// gRPC: cancels the stream context (code and reason are ignored — gRPC has no equivalent).
	// Call sites MUST use protocol.CloseCodeForceDisconnect / protocol.CloseReasonForceDisconnect
	// constants rather than passing literal values.
	CloseWithCode(code int, reason string) error

	// SetWriteDeadline sets the deadline for write operations.
	// WebSocket: delegates to net.Conn. gRPC: no-op (stream-level deadlines).
	SetWriteDeadline(t time.Time) error

	// Type returns the transport type for Prometheus metric labels.
	Type() TransportType

	// RemoteAddr returns the client's remote address for logging.
	RemoteAddr() string
}

// WebSocketTransport implements Transport for gorilla/gobwas WebSocket connections.
// Wraps a net.Conn with a bufio.Writer for batched frame writes.
//
// Thread safety: A single WebSocketTransport is owned by one write pump goroutine.
// No concurrent access — the write pump is the sole caller of Send/SendBatch.
type WebSocketTransport struct {
	conn   net.Conn
	writer *bufio.Writer
	buf    []byte // reusable assembly buffer; owned exclusively by the write pump goroutine
}

// NewWebSocketTransport creates a WebSocket transport wrapping the given connection.
func NewWebSocketTransport(conn net.Conn) *WebSocketTransport {
	return &WebSocketTransport{
		conn:   conn,
		writer: bufio.NewWriter(conn),
		buf:    make([]byte, 0, initialBufCap),
	}
}

// Conn returns the underlying net.Conn for WebSocket-specific operations
// (ReadLoop frame reading, read deadline). Not on the Transport interface —
// only WebSocket clients have a ReadLoop.
func (t *WebSocketTransport) Conn() net.Conn {
	return t.conn
}

// Send writes a single message as a WebSocket text frame and flushes.
// Returns the number of bytes written (for accurate metrics without double allocation).
func (t *WebSocketTransport) Send(msg OutgoingMsg) (int, error) {
	var data []byte
	if msg.IsRaw() {
		data = msg.Bytes() // zero-copy path: raw bytes sent directly
	} else {
		t.buf = msg.AppendTo(t.buf[:0]) // envelope path: reassign to store any grown slice
		data = t.buf
	}
	if data == nil {
		return 0, nil
	}
	if err := wsutil.WriteServerMessage(t.writer, ws.OpText, data); err != nil {
		return 0, fmt.Errorf("ws write message: %w", err)
	}
	if err := t.writer.Flush(); err != nil {
		return len(data), fmt.Errorf("ws flush: %w", err)
	}
	return len(data), nil
}

// SendBatch writes multiple messages as WebSocket text frames with a single flush.
// Returns the total number of bytes written across all messages.
// This preserves the batching optimization — one syscall for the entire batch.
func (t *WebSocketTransport) SendBatch(msgs []OutgoingMsg) (int, error) {
	var total int
	for _, msg := range msgs {
		var data []byte
		if msg.IsRaw() {
			data = msg.Bytes()
		} else {
			t.buf = msg.AppendTo(t.buf[:0]) // reassign — bufio.Writer.Write copies into its buffer; t.buf safe to reuse next iteration
			data = t.buf
		}
		if data == nil {
			continue
		}
		if err := wsutil.WriteServerMessage(t.writer, ws.OpText, data); err != nil {
			return total, fmt.Errorf("ws write batch message: %w", err)
		}
		total += len(data)
	}
	if err := t.writer.Flush(); err != nil {
		return total, fmt.Errorf("ws flush batch: %w", err)
	}
	return total, nil
}

// WritePing sends a WebSocket ping frame.
func (t *WebSocketTransport) WritePing() error {
	if err := wsutil.WriteServerMessage(t.conn, ws.OpPing, nil); err != nil {
		return fmt.Errorf("ws write ping: %w", err)
	}
	return nil
}

// WritePong sends a WebSocket pong frame with the given payload.
func (t *WebSocketTransport) WritePong(payload []byte) error {
	if err := wsutil.WriteServerMessage(t.conn, ws.OpPong, payload); err != nil {
		return fmt.Errorf("ws write pong: %w", err)
	}
	return nil
}

// Close sends a WebSocket close frame (1008 Policy Violation) and closes the TCP connection.
func (t *WebSocketTransport) Close() error {
	if t.conn == nil {
		return nil
	}
	closeMsg := ws.NewCloseFrameBody(ws.StatusPolicyViolation, "connection closed")
	_ = ws.WriteFrame(t.conn, ws.NewCloseFrame(closeMsg)) // best-effort close frame
	if err := t.conn.Close(); err != nil {
		return fmt.Errorf("ws close conn: %w", err)
	}
	return nil
}

// CloseWithCode sends an RFC 6455 close frame with the given application-defined close code
// and reason, then closes the TCP connection.
func (t *WebSocketTransport) CloseWithCode(code int, reason string) error {
	if t.conn == nil {
		return nil
	}
	// Set a short write deadline so a dead/slow client cannot stall the caller indefinitely.
	// This is especially important for force-disconnect bulk paths where many connections
	// are closed in parallel.
	_ = t.conn.SetWriteDeadline(time.Now().Add(closeFrameWriteTimeout))
	closeMsg := ws.NewCloseFrameBody(ws.StatusCode(code), reason) //nolint:gosec // G115: WebSocket close codes 1000-4999 fit in uint16; code is validated via protocol.CloseCode* constants
	_ = ws.WriteFrame(t.conn, ws.NewCloseFrame(closeMsg))         // best-effort close frame
	if err := t.conn.Close(); err != nil {
		return fmt.Errorf("ws close conn: %w", err)
	}
	return nil
}

// SetWriteDeadline sets the write deadline on the underlying connection.
func (t *WebSocketTransport) SetWriteDeadline(deadline time.Time) error {
	if err := t.conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("ws set write deadline: %w", err)
	}
	return nil
}

// Type returns TransportWebSocket for metrics labeling.
func (t *WebSocketTransport) Type() TransportType {
	return TransportWebSocket
}

// RemoteAddr returns the remote address of the underlying connection.
func (t *WebSocketTransport) RemoteAddr() string {
	if t.conn == nil {
		return ""
	}
	return t.conn.RemoteAddr().String()
}

// GRPCStreamTransport implements Transport for gRPC server-streaming connections.
// Used by SSE virtual clients — the gateway opens a Subscribe stream, ws-server
// creates a virtual Client with this transport, and messages flow via stream.Send().
//
// Thread safety: A single GRPCStreamTransport is owned by one write pump goroutine.
// No concurrent access — same as WebSocketTransport.
type GRPCStreamTransport struct {
	stream     serverv1.RealtimeService_SubscribeServer
	cancel     context.CancelFunc
	remoteAddr string // Stored from SubscribeRequest.remote_addr at construction
	buf        []byte // reusable assembly buffer; owned exclusively by the write pump goroutine
}

// NewGRPCStreamTransport creates a gRPC stream transport.
// remoteAddr is the original client IP from the gateway (for logging/metrics).
func NewGRPCStreamTransport(stream serverv1.RealtimeService_SubscribeServer, cancel context.CancelFunc, remoteAddr string) *GRPCStreamTransport {
	return &GRPCStreamTransport{
		stream:     stream,
		cancel:     cancel,
		remoteAddr: remoteAddr,
		buf:        make([]byte, 0, initialBufCap),
	}
}

// Send delivers a single message on the gRPC stream.
// Constructs SubscribeResponse{Sequence, Payload} from the OutgoingMsg.
func (t *GRPCStreamTransport) Send(msg OutgoingMsg) (int, error) {
	var payload []byte
	if msg.IsRaw() {
		payload = msg.Bytes() // zero-copy path
	} else {
		t.buf = msg.AppendTo(t.buf[:0]) // envelope path: reassign — proto.Marshal copies before returning
		payload = t.buf
	}
	if payload == nil {
		return 0, nil
	}
	err := t.stream.Send(&serverv1.SubscribeResponse{
		Sequence: msg.seq,
		Payload:  payload,
	})
	if err != nil {
		return 0, fmt.Errorf("grpc stream send: %w", err)
	}
	return len(payload), nil
}

// SendBatch delivers multiple messages on the gRPC stream.
// gRPC handles its own framing — each message is sent individually.
func (t *GRPCStreamTransport) SendBatch(msgs []OutgoingMsg) (int, error) {
	var total int
	for _, msg := range msgs {
		n, err := t.Send(msg)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// WritePing is a no-op for gRPC — HTTP/2 PING handles keepalive.
func (t *GRPCStreamTransport) WritePing() error { return nil }

// WritePong is a no-op for gRPC — no pong mechanism in gRPC streams.
func (t *GRPCStreamTransport) WritePong(_ []byte) error { return nil }

// Close cancels the gRPC stream context, which terminates the stream.
func (t *GRPCStreamTransport) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

// CloseWithCode cancels the gRPC stream context. Code and reason are ignored — gRPC has
// no equivalent to WebSocket application-defined close codes.
func (t *GRPCStreamTransport) CloseWithCode(_ int, _ string) error {
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

// SetWriteDeadline is a no-op for gRPC — stream-level deadlines are managed by gRPC.
func (t *GRPCStreamTransport) SetWriteDeadline(_ time.Time) error { return nil }

// Type returns TransportGRPCStream for metrics labeling.
func (t *GRPCStreamTransport) Type() TransportType {
	return TransportGRPCStream
}

// RemoteAddr returns the original client IP from the gateway.
func (t *GRPCStreamTransport) RemoteAddr() string {
	return t.remoteAddr
}
