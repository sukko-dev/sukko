package orchestration

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// ShardProxy is a WebSocket proxy that forwards connections to a backend shard.
// Capacity control is handled by the shard's ResourceGuard - the proxy simply
// forwards the connection and lets the shard decide whether to accept it.
type ShardProxy struct {
	shard      *Shard
	backendURL *url.URL
	logger     zerolog.Logger

	// Timeouts
	dialTimeout    time.Duration
	messageTimeout time.Duration
}

// NewShardProxy creates a new WebSocket proxy for a specific shard.
func NewShardProxy(shard *Shard, backendURL *url.URL, logger zerolog.Logger, dialTimeout, messageTimeout time.Duration) *ShardProxy {
	return &ShardProxy{
		shard:          shard,
		backendURL:     backendURL,
		logger:         logger.With().Str("component", "proxy").Int("shard_id", shard.ID).Logger(),
		dialTimeout:    dialTimeout,
		messageTimeout: messageTimeout,
	}
}

// buildBackendHeaders copies the gateway-set identity headers from the client
// request so they can be forwarded to the backend shard. The shard checks these
// on upgrade (defense in depth, §II): it requires the tenant header (400 "missing
// tenant ID" if absent) and constant-time validates the internal secret (401 if
// wrong). Only non-empty identity headers are copied; all other request headers
// (Authorization, cookies, WebSocket handshake headers) are intentionally
// excluded — the backend dial performs its own handshake.
func buildBackendHeaders(r *http.Request) http.Header {
	h := http.Header{}
	for _, name := range []string{
		protocol.HeaderTenantID,
		protocol.HeaderInternalSecret,
		protocol.HeaderAPIKeyID,
		protocol.HeaderUserID,
	} {
		if v := r.Header.Get(name); v != "" {
			h.Set(name, v)
		}
	}
	return h
}

// ServeHTTP handles the WebSocket proxy request.
// The proxy upgrades the client connection, then dials the backend shard.
// Capacity control is delegated to the shard's ResourceGuard which will
// return 503 if the shard is at capacity (connection limit, CPU, memory, etc.)
func (p *ShardProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	// STEP 1: Upgrade client to WebSocket using gobwas/ws
	clientConn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Str("remote_addr", r.RemoteAddr).
			Msg("Client WebSocket upgrade failed")
		return
	}
	defer func() { _ = clientConn.Close() }() // Close error non-actionable; connection may already be closed by peer

	p.logger.Debug().
		Str("remote_addr", r.RemoteAddr).
		Msg("Client upgraded, connecting to backend shard")

	// STEP 2: Connect to backend shard using gobwas/ws
	ctx, cancel := context.WithTimeout(r.Context(), p.dialTimeout)
	defer cancel()

	// DEBUG: Log before backend dial attempt
	dialStart := time.Now()
	p.logger.Debug().
		Str("backend_url", p.backendURL.String()).
		Dur("dial_timeout", p.dialTimeout).
		Str("client_remote_addr", r.RemoteAddr).
		Msg("Attempting to dial backend shard")

	// Forward the gateway-set identity headers to the backend shard, which checks
	// them on upgrade (defense in depth, §II). Without this the shard rejects the
	// upgrade with 400 "missing tenant ID" and the client never attaches to a shard.
	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(buildBackendHeaders(r))}
	backendConn, _, _, err := dialer.Dial(ctx, p.backendURL.String())
	dialDuration := time.Since(dialStart)

	if err != nil {
		// DEBUG: Enhanced backend dial failure logging
		p.logger.Error().
			Err(err).
			Str("backend_url", p.backendURL.String()).
			Dur("dial_duration_ms", dialDuration).
			Dur("elapsed_since_start_ms", time.Since(startTime)).
			Str("client_remote_addr", r.RemoteAddr).
			Int("shard_id", p.shard.ID).
			Msg("Backend dial failed")

		// Send error to client
		// Client receives: WebSocket Close Frame (code: 1011, reason: "Backend unavailable")
		// See: docs/API_REJECTION_RESPONSES.md (Scenario 6)
		closeFrame := ws.NewCloseFrameBody(ws.StatusInternalServerError, "Backend unavailable")
		_ = ws.WriteFrame(clientConn, ws.NewCloseFrame(closeFrame)) // Best-effort error frame; client may already be disconnected
		return
	}
	defer func() { _ = backendConn.Close() }() // Close error non-actionable; connection may already be closed by peer

	// DEBUG: Backend dial successful
	p.logger.Debug().
		Str("backend_url", p.backendURL.String()).
		Dur("dial_duration_ms", dialDuration).
		Int("shard_id", p.shard.ID).
		Msg("Backend dial successful")

	p.logger.Debug().
		Str("backend_url", p.backendURL.String()).
		Msg("Backend connected, starting bidirectional proxy")

	// STEP 3: Proxy messages bidirectionally
	p.proxyMessages(clientConn, backendConn)

	p.logger.Info().
		Dur("total_duration", time.Since(startTime)).
		Msg("Connection closed normally")
}

// proxyMessages forwards WebSocket messages bidirectionally between client and backend.
// Uses goroutines for concurrent forwarding in both directions.
// Returns when either connection closes or errors, after ensuring both goroutines complete.
func (p *ShardProxy) proxyMessages(client, backend net.Conn) {
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Backend (client frames are masked, need to re-mask for backend)
	go func() {
		defer logging.RecoverPanic(p.logger, "proxyClientToBackend", nil)
		defer wg.Done()
		p.copyMessages(client, backend, "client->backend", true, errChan)
	}()

	// Backend -> Client (server frames are not masked, send as-is)
	go func() {
		defer logging.RecoverPanic(p.logger, "proxyBackendToClient", nil)
		defer wg.Done()
		p.copyMessages(backend, client, "backend->client", false, errChan)
	}()

	// Wait for first error (connection close)
	err := <-errChan
	if err != nil && !errors.Is(err, io.EOF) {
		p.logger.Warn().Err(err).Msg("Connection closed with error")
	} else {
		p.logger.Debug().Msg("Connection closed normally")
	}

	// Close both connections to unblock any goroutines waiting on read/write
	_ = client.Close()  // Close error non-actionable; triggers EOF in peer goroutine
	_ = backend.Close() // Close error non-actionable; triggers EOF in peer goroutine

	// Wait for both goroutines to complete before returning
	wg.Wait()
}

// copyMessages copies WebSocket frames from src to dst.
// Handles ALL frame types transparently: text, binary, ping, pong, close.
// This ensures ping/pong frames pass through the proxy to the browser.
//
// The maskOutput parameter controls whether outgoing frames should be masked:
// - client->backend: true (client frames must be masked per WebSocket spec)
// - backend->client: false (server frames must not be masked)
func (p *ShardProxy) copyMessages(src, dst net.Conn, direction string, maskOutput bool, errChan chan error) {
	for {
		// Read frame header
		header, err := ws.ReadHeader(src)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.logger.Debug().
					Err(err).
					Str("direction", direction).
					Msg("Read header error")
			}
			errChan <- err
			return
		}

		// Read payload
		payload := make([]byte, header.Length)
		if header.Length > 0 {
			if _, err := io.ReadFull(src, payload); err != nil {
				p.logger.Debug().
					Err(err).
					Str("direction", direction).
					Msg("Read payload error")
				errChan <- err
				return
			}
		}

		// Unmask if the incoming frame was masked (client->proxy frames are masked)
		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		// Handle close frame - forward it and exit
		if header.OpCode == ws.OpClose {
			p.logger.Debug().
				Str("direction", direction).
				Msg("Received close frame, forwarding")

			outHeader := ws.Header{
				Fin:    true,
				OpCode: ws.OpClose,
				Length: int64(len(payload)),
			}
			if maskOutput {
				outHeader.Masked = true
				outHeader.Mask = ws.NewMask()
				ws.Cipher(payload, outHeader.Mask, 0)
			}
			_ = ws.WriteHeader(dst, outHeader) // Best-effort close frame forward; connection is terminating
			_, _ = dst.Write(payload)          // Best-effort close payload; connection is terminating
			errChan <- nil
			return
		}

		// Log control frames for ping/pong tracing
		switch header.OpCode {
		case ws.OpPing:
			p.logger.Debug().
				Str("direction", direction).
				Msg("Forwarded ping frame")
		case ws.OpPong:
			p.logger.Debug().
				Str("direction", direction).
				Msg("Forwarded pong frame")
		default:
			// text, binary, close, continuation — no logging, forwarded below
		}

		// Forward all other frames (text, binary, ping, pong) as-is
		outHeader := ws.Header{
			Fin:    header.Fin,
			OpCode: header.OpCode,
			Length: int64(len(payload)),
		}

		// Apply masking if needed (client->backend requires masked frames)
		if maskOutput {
			outHeader.Masked = true
			outHeader.Mask = ws.NewMask()
			ws.Cipher(payload, outHeader.Mask, 0)
		}

		if err := ws.WriteHeader(dst, outHeader); err != nil {
			p.logger.Debug().
				Err(err).
				Str("direction", direction).
				Msg("Write header error")
			errChan <- err
			return
		}
		if len(payload) > 0 {
			if _, err := dst.Write(payload); err != nil {
				p.logger.Debug().
					Err(err).
					Str("direction", direction).
					Msg("Write payload error")
				errChan <- err
				return
			}
		}
	}
}
