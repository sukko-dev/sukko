// Package server provides core WebSocket server functionality.
// This file contains the Pump struct for WebSocket read/write operations.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// PumpConfig holds timing and rate limit configuration for pump operations.
type PumpConfig struct {
	PongWait   time.Duration
	WriteWait  time.Duration
	PingPeriod time.Duration

	// Per-client message rate limit values (for log messages and error responses)
	ClientMsgBurstLimit int
	ClientMsgRatePerSec float64
}

// FallbackPingPeriodRatio is the ratio used to calculate PingPeriod from PongWait
// when an invalid configuration is provided (PingPeriod >= PongWait).
// 75% (3/4) provides a 25% buffer for network round-trip latency.
// Example: 60s PongWait * 0.75 = 45s PingPeriod = 15s buffer
const (
	FallbackPingPeriodRatio = 3
	FallbackPingPeriodDenom = 4
)

// NewPumpConfig creates a PumpConfig with the specified timeouts.
// If pingPeriod >= pongWait (invalid), logs a warning and falls back to 75% ratio.
func NewPumpConfig(pongWait, pingPeriod, writeWait time.Duration, logger zerolog.Logger) PumpConfig {
	// Validation: pingPeriod must be less than pongWait
	if pingPeriod >= pongWait {
		correctedPingPeriod := (pongWait * FallbackPingPeriodRatio) / FallbackPingPeriodDenom
		logger.Warn().
			Dur("configured_ping_period", pingPeriod).
			Dur("configured_pong_wait", pongWait).
			Dur("corrected_ping_period", correctedPingPeriod).
			Msg("PingPeriod >= PongWait is invalid, using 75% ratio")
		pingPeriod = correctedPingPeriod
	}

	return PumpConfig{
		PongWait:   pongWait,
		WriteWait:  writeWait,
		PingPeriod: pingPeriod,
	}
}

// Pump handles WebSocket read/write operations with injectable dependencies.
// This enables unit testing of pump logic without real WebSocket connections.
type Pump struct {
	Config        PumpConfig
	Logger        Logger
	ZerologLogger zerolog.Logger // For panic recovery (zerolog-specific)
	RateLimiter   RateLimiter
	AlertLogger   AlertLogger
	Stats         *stats.Stats
	Clock         Clock
}

// NewPump creates a Pump with the given dependencies.
func NewPump(config PumpConfig, logger Logger, zerologLogger zerolog.Logger, rateLimiter RateLimiter, alertLogger AlertLogger, pumpStats *stats.Stats, clock Clock) *Pump {
	return &Pump{
		Config:        config,
		Logger:        logger,
		ZerologLogger: zerologLogger,
		RateLimiter:   rateLimiter,
		AlertLogger:   alertLogger,
		Stats:         pumpStats,
		Clock:         clock,
	}
}

// recoverPanic handles panic recovery for pump operations.
// Uses the shared logging.RecoverPanic for production (zerolog),
// falls back to basic logging for tests using the Logger interface.
func (p *Pump) recoverPanic(goroutineName string, fields map[string]any) {
	if r := recover(); r != nil {
		// Try zerolog first (production) using shared logging
		if p.ZerologLogger.GetLevel() != zerolog.Disabled {
			// Re-panic and let shared logging handle it
			defer logging.RecoverPanic(p.ZerologLogger, goroutineName, fields)
			panic(r)
		}

		// Fall back to interface logger (tests)
		if p.Logger != nil {
			stack := string(debug.Stack())
			p.Logger.Error().
				Str("goroutine", goroutineName).
				Interface("panic_value", r).
				Str("stack_trace", stack).
				Msg("GOROUTINE PANIC RECOVERED")
		}
	}
}

// now returns the current time using the injected clock or real time.
func (p *Pump) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}

// newTicker creates a ticker using the injected clock or real time.
func (p *Pump) newTicker(d time.Duration) Ticker {
	if p.Clock != nil {
		return p.Clock.NewTicker(d)
	}
	return &RealTicker{ticker: time.NewTicker(d)}
}

// ReadLoop reads messages from the WebSocket connection.
// disconnectFn is called when the client disconnects.
// handleMsgFn is called for each text message received.
//
// Uses wsutil.Reader with explicit frame handling to ensure the read deadline
// is refreshed when pong frames are received. This fixes a bug where
// wsutil.ReadClientData() would handle pongs internally without returning,
// causing the read deadline to never refresh and connections to timeout.
func (p *Pump) ReadLoop(ctx context.Context, c *Client, disconnectFn func(*Client, string, string), handleMsgFn func(*Client, []byte)) {
	// Panic recovery
	defer p.recoverPanic("readPump", map[string]any{
		"client_id": c.id,
	})

	var disconnectReason string
	var initiatedBy string

	defer func() {
		if disconnectReason == "" {
			disconnectReason = pkgmetrics.DisconnectReadError
			initiatedBy = pkgmetrics.InitiatedByClient
		}
		if disconnectFn != nil {
			disconnectFn(c, disconnectReason, initiatedBy)
		}
	}()

	// Extract underlying net.Conn from WebSocketTransport.
	// ReadLoop is only called for WebSocket clients — gRPC virtual clients have no read pump.
	wsTransport, ok := c.transport.(*WebSocketTransport)
	if !ok {
		if p.Logger != nil {
			p.Logger.Error().Int64("client_id", c.id).Msg("ReadLoop called on non-WebSocket transport")
		}
		return
	}
	conn := wsTransport.Conn()

	_ = conn.SetReadDeadline(p.now().Add(p.Config.PongWait))

	// Create reader for explicit frame handling
	reader := wsutil.Reader{
		Source: conn,
		State:  ws.StateServerSide,
	}

	for {
		// Check for context cancellation (server shutdown)
		select {
		case <-ctx.Done():
			disconnectReason = pkgmetrics.DisconnectServerShutdown
			initiatedBy = pkgmetrics.InitiatedByServer
			return
		default:
		}

		// Read next frame header (BLOCKS until data arrives or conn closes)
		hdr, err := reader.NextFrame()
		if err != nil {
			// Determine if this was server shutdown or client disconnect.
			// When the server shuts down: cancel() → WriteLoop exits → closes conn →
			// NextFrame unblocks with error. The ctx check at the top of the loop
			// couldn't catch it because NextFrame was blocking. Check ctx NOW to
			// correctly attribute the disconnect.
			select {
			case <-ctx.Done():
				disconnectReason = pkgmetrics.DisconnectServerShutdown
				initiatedBy = pkgmetrics.InitiatedByServer
			default:
				disconnectReason = pkgmetrics.DisconnectReadError
				initiatedBy = pkgmetrics.InitiatedByClient
			}
			break
		}

		// Refresh deadline on ANY frame (including pong) - this is the key fix
		_ = conn.SetReadDeadline(p.now().Add(p.Config.PongWait))

		// Handle control frames
		if hdr.OpCode.IsControl() {
			// Handle ping - send pong response
			if hdr.OpCode == ws.OpPing {
				payload := make([]byte, hdr.Length)
				if hdr.Length > 0 {
					if _, err := io.ReadFull(conn, payload); err != nil {
						break
					}
					if hdr.Masked {
						ws.Cipher(payload, hdr.Mask, 0)
					}
				}
				select {
				case c.control <- payload:
					if p.Logger != nil {
						p.Logger.Debug().Int64("client_id", c.id).Msg("Received ping, queued pong")
					}
				default:
					// Control channel full — drop this pong. The client's next ping will trigger
					// another pong attempt. This is safe: WriteLoop drains control with priority,
					// so a full channel means 2 pongs are already queued and will be written shortly.
					if p.Logger != nil {
						p.Logger.Debug().Int64("client_id", c.id).Msg("Control channel full, dropped pong")
					}
				}
				continue
			}
			// Handle close
			if hdr.OpCode == ws.OpClose {
				return
			}
			// Handle pong - deadline already refreshed, discard payload
			if hdr.OpCode == ws.OpPong {
				if p.Logger != nil {
					p.Logger.Debug().Int64("client_id", c.id).Msg("Received pong from client")
				}
				_ = reader.Discard()
				continue
			}
			continue
		}

		// Read data frame payload
		msg, err := io.ReadAll(&reader)
		if err != nil {
			disconnectReason = pkgmetrics.DisconnectReadError
			initiatedBy = pkgmetrics.InitiatedByClient
			break
		}

		// Update stats
		p.Stats.MessagesReceived.Add(1)
		p.Stats.BytesReceived.Add(int64(len(msg)))
		metrics.UpdateMessageMetrics(string(TransportWebSocket), 0, 1)
		metrics.UpdateBytesMetrics(0, int64(len(msg)))

		if hdr.OpCode == ws.OpText {
			// Rate limiting check
			if p.RateLimiter != nil && !p.RateLimiter.CheckLimit(c.id) {
				p.handleRateLimitExceeded(c)
				continue
			}

			// Process message
			if handleMsgFn != nil {
				handleMsgFn(c, msg)
			}
		}
	}
}

// handleRateLimitExceeded processes a rate limit violation.
func (p *Pump) handleRateLimitExceeded(c *Client) {
	if p.Logger != nil {
		p.Logger.Warn().
			Int64("client_id", c.id).
			Int("burst_limit", p.Config.ClientMsgBurstLimit).
			Int("rate_limit_per_sec", int(p.Config.ClientMsgRatePerSec)).
			Msg("Client rate limited")
	}

	if p.AlertLogger != nil {
		p.AlertLogger.Warning("ClientRateLimited", "Client exceeded rate limit", map[string]any{
			"clientID": c.id,
			"limit":    fmt.Sprintf("%d burst, %g/sec sustained", p.Config.ClientMsgBurstLimit, p.Config.ClientMsgRatePerSec),
		})
	}

	// Send error to client
	errorMsg := CreateRateLimitErrorMessage(p.Config.ClientMsgRatePerSec)
	select {
	case c.send <- RawMsg(errorMsg):
	default:
		// Client buffer full
	}

	p.Stats.RateLimitedMessages.Add(1)
	metrics.IncrementRateLimitedMessages()
}

// WriteLoop writes messages to the client via the Transport interface.
// Transport-agnostic: works for WebSocket, gRPC stream, and future transports.
// Implements message batching (via SendBatch) and keepalive (via WritePing).
func (p *Pump) WriteLoop(ctx context.Context, c *Client) {
	// Panic recovery
	defer p.recoverPanic("writePump", map[string]any{
		"client_id": c.id,
	})

	ticker := p.newTicker(p.Config.PingPeriod)

	defer func() {
		ticker.Stop()
		c.closeOnce.Do(func() {
			if c.transport != nil {
				_ = c.transport.Close()
			}
		})
	}()

	for {
		// Priority 1: drain pong responses first (time-sensitive)
		select {
		case payload := <-c.control:
			if !p.handlePong(c, payload) {
				return
			}
			continue
		default:
		}

		// Priority 2: try primary send alone (non-blocking).
		// Provides a statistical bias toward live messages over gap notifications.
		// If c.send is ready, deliver it now without giving gapChan a chance to fire.
		//
		// Starvation bound: if c.send is continuously saturated, gapChan is not drained
		// until the burst subsides. In practice this is bounded: a persistently-saturated
		// client triggers the SlowClient circuit breaker (SlowClientMaxAttempts), which
		// disconnects the client before gapChan fills. Default gapChan buffer (8) absorbs
		// bursts; drops beyond capacity are counted via GapDropReasonBufferFull.
		select {
		case msg, ok := <-c.send:
			if !p.handleSendMsg(c, msg, ok) {
				return
			}
			continue
		default:
		}

		// Priority 3: block on all channels including gap notifications.
		// Go's pseudorandom select is acceptable here: gap notifications are rare
		// advisory events; brief reordering when both c.send and c.gapChan are
		// simultaneously ready is acceptable (statistical priority guarantee).
		select {
		case <-ctx.Done():
			return

		case payload := <-c.control:
			if !p.handlePong(c, payload) {
				return
			}

		case msg, ok := <-c.send:
			if !p.handleSendMsg(c, msg, ok) {
				return
			}

		case <-ticker.C():
			// Guard against race condition: transport may be nil if client was
			// disconnected and returned to pool while ticker was pending
			if c.transport == nil {
				return
			}
			_ = c.transport.SetWriteDeadline(p.now().Add(p.Config.WriteWait))
			if err := c.transport.WritePing(); err != nil {
				if p.Logger != nil {
					p.Logger.Debug().Err(err).Int64("client_id", c.id).Msg("Failed to send ping")
				}
				return
			}
			if p.Logger != nil {
				p.Logger.Debug().Int64("client_id", c.id).Msg("Sent ping to client")
			}

		case gap := <-c.gapChan:
			// gapChan is never closed and always non-nil for an active client.
			merged, lost := coalesceGapNotifications(gap, c.gapChan)
			if lost != nil {
				// Cross-channel item consumed during coalescing; re-enqueue non-blocking.
				// If the buffer is full, count the loss — the client can recover via
				// its own seq-gap detection without a notification.
				select {
				case c.gapChan <- *lost:
				default:
					metrics.RecordDroppedGapNotification(GapDropReasonCoalesceLoss)
				}
			}
			// gapEnvelope contains only JSON-safe types (string, int64) — Marshal cannot fail.
			data, _ := json.Marshal(gapEnvelope{
				Type:    MsgTypeGap,
				Channel: merged.Channel,
				FromSeq: merged.FromSeq,
				ToSeq:   merged.ToSeq,
				LastPos: merged.LastPos,
				Ts:      merged.Ts,
			})
			select {
			case c.send <- RawMsg(data):
			default:
				// Primary channel full — skip gap notification this cycle.
				metrics.RecordDroppedGapNotification(GapDropReasonSendFull)
			}
		}
	}
}

// handlePong writes a pong response. Returns false if the write loop must exit.
// Extracted to avoid duplicating pong write logic between Priority 1 drain and Priority 3 select.
func (p *Pump) handlePong(c *Client, payload []byte) bool {
	_ = c.transport.SetWriteDeadline(p.now().Add(p.Config.WriteWait))
	if err := c.transport.WritePong(payload); err != nil {
		if p.Logger != nil {
			p.Logger.Debug().Err(err).Int64("client_id", c.id).Msg("Failed to send pong")
		}
		return false
	}
	if p.Logger != nil {
		p.Logger.Debug().Int64("client_id", c.id).Msg("Sent pong to client")
	}
	return true
}

// handleSendMsg processes one message from c.send. Returns false if the write loop must exit.
// Extracted to avoid duplicating the batch send logic between Priority 2 and Priority 3.
func (p *Pump) handleSendMsg(c *Client, msg OutgoingMsg, ok bool) bool {
	if !ok {
		if p.Logger != nil {
			p.Logger.Debug().Int64("client_id", c.id).Msg("Send channel closed")
		}
		// Don't call Close() here — deferred closeOnce.Do handles it.
		return false
	}

	// Guard against race condition: transport may be nil if client was
	// disconnected and returned to pool while message was pending.
	if c.transport == nil {
		return false
	}
	_ = c.transport.SetWriteDeadline(p.now().Add(p.Config.WriteWait))

	// Collect batch: first message + any additional available messages.
	n := len(c.send)
	if n == 0 {
		byteCount, err := c.transport.Send(msg)
		if err != nil {
			if p.Logger != nil {
				p.Logger.Debug().Err(err).Int64("client_id", c.id).Msg("Failed to send message")
			}
			p.recordSendTimeoutDrops(c, 1)
			return false
		}
		if byteCount > 0 {
			transportLabel := string(c.transport.Type())
			p.Stats.MessagesSent.Add(1)
			p.Stats.BytesSent.Add(int64(byteCount))
			metrics.UpdateMessageMetrics(transportLabel, 1, 0)
			metrics.UpdateBytesMetrics(int64(byteCount), 0)
		}
	} else {
		batch := make([]OutgoingMsg, 1, 1+n)
		batch[0] = msg
	batchLoop:
		for range n {
			select {
			case batchMsg, batchOk := <-c.send:
				if !batchOk {
					break batchLoop
				}
				batch = append(batch, batchMsg)
			default:
				break batchLoop
			}
		}
		totalBytes, err := c.transport.SendBatch(batch)
		if err != nil {
			if p.Logger != nil {
				p.Logger.Debug().Err(err).Int64("client_id", c.id).Msg("Failed to send batch")
			}
			p.recordSendTimeoutDrops(c, len(batch))
			return false
		}
		transportLabel := string(c.transport.Type())
		batchMsgCount := int64(len(batch))
		p.Stats.MessagesSent.Add(batchMsgCount)
		p.Stats.BytesSent.Add(int64(totalBytes))
		metrics.UpdateMessageMetrics(transportLabel, batchMsgCount, 0)
		metrics.UpdateBytesMetrics(int64(totalBytes), 0)
	}
	return true
}

// recordSendTimeoutDrops records dropped messages when a write operation fails.
// failedCount is the number of messages that failed during the current write.
// It also drains and counts remaining messages in the client's send buffer.
// Uses "delivery" as channel since actual channel info is not available at write time.
func (p *Pump) recordSendTimeoutDrops(c *Client, failedCount int) {
	// Count remaining messages in send buffer
	remainingCount := len(c.send)
	totalDropped := failedCount + remainingCount

	if totalDropped > 0 {
		// Record each drop (using "delivery" as pseudo-channel since we don't have channel info)
		for range totalDropped {
			metrics.RecordDroppedBroadcastWithStats(p.Stats, "delivery", pkgmetrics.DropReasonSendTimeout)
		}
	}

	// Drain the send buffer to prevent blocking senders
	for range remainingCount {
		select {
		case <-c.send:
		default:
			return
		}
	}
}

// =============================================================================
// Pure Functions
// =============================================================================

// coalesceGapNotifications merges consecutive same-channel gap notifications from ch.
// When the first different-channel item is encountered it is dequeued and returned as
// the second return value so the caller can re-enqueue it (non-blocking) for delivery
// in the next write-pump iteration. If re-enqueue fails (buffer full), the caller
// increments GapDropReasonCoalesceLoss. Items not yet dequeued remain in ch.
func coalesceGapNotifications(first GapNotification, ch <-chan GapNotification) (merged GapNotification, overflow *GapNotification) {
	merged = first
	for {
		select {
		case gap := <-ch:
			// gapChan is never closed; the ok form is unnecessary.
			if gap.Channel != merged.Channel {
				// Different channel: return it to the caller for re-enqueue.
				return merged, &gap
			}
			if gap.FromSeq < merged.FromSeq {
				merged.FromSeq = gap.FromSeq
			}
			if gap.ToSeq > merged.ToSeq {
				merged.ToSeq = gap.ToSeq
			}
			// Keep first notification's LastPos (gapChan is FIFO; first = earliest pos).
		default:
			return merged, nil
		}
	}
}

// CreateRateLimitErrorMessage creates the error response for rate limiting.
func CreateRateLimitErrorMessage(ratePerSec float64) []byte {
	msg := map[string]any{
		"type":    "error",
		"code":    "RATE_LIMIT_EXCEEDED",
		"message": fmt.Sprintf("Too many messages, please slow down (limit: %g/sec)", ratePerSec),
	}
	// Error impossible: marshaling map[string]any with only string values
	data, _ := json.Marshal(msg)
	return data
}
