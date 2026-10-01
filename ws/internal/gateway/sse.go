package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/logging"
)

// HandleSSE handles Server-Sent Events connections for real-time message delivery.
// SSE is receive-only — publishing uses POST /api/v1/publish.
//
// Flow:
//  1. Parse channels from ?channels= query param
//  2. Authenticate via shared authenticateRequest()
//  3. Filter channels by permissions
//  4. Acquire tenant connection slot
//  5. Open gRPC Subscribe stream to ws-server
//  6. Loop: read SubscribeResponse → format as SSE event → flush
//  7. Keepalive: send `: keepalive\n\n` at SSEKeepAliveInterval
func (gw *Gateway) HandleSSE(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx := r.Context()

	// 1. Parse channels from query param
	channelsParam := r.URL.Query().Get("channels")
	if channelsParam == "" {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "channels query parameter is required")
		return
	}
	channels := strings.Split(channelsParam, ",")
	// Remove empty strings from split
	filtered := channels[:0]
	for _, ch := range channels {
		ch = strings.TrimSpace(ch)
		if ch != "" {
			filtered = append(filtered, ch)
		}
	}
	channels = filtered
	if len(channels) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "at least one channel is required")
		return
	}

	// 2. Authenticate
	authRes, authErr := gw.authenticateRequest(ctx, r)
	if authErr != nil {
		status, code := authErrorResponse(authErr)
		httputil.WriteError(w, status, code, authErr.Error())
		return
	}

	// 3. Filter channels by subscribe permissions (same as WebSocket)
	channels = gw.filterSubscribeChannels(ctx, channels, authRes.TenantSlug, authRes.Claims)
	if len(channels) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST",
			"no channels remaining after permission filtering")
		return
	}
	// Cap the channel count so the reconnect cursor token stays within request-header
	// size limits on reconnect (ADR-0030).
	if gw.config.SSEMaxChannels > 0 && len(channels) > gw.config.SSEMaxChannels {
		httputil.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST",
			fmt.Sprintf("too many channels: %d exceeds the SSE limit of %d", len(channels), gw.config.SSEMaxChannels))
		return
	}

	// 4. Acquire tenant connection slot
	if gw.connTracker != nil && authRes.TenantSlug != "" {
		if !gw.connTracker.TryAcquire(authRes.TenantSlug) {
			// LOG-010: SSE tenant connection rate limit rejection
			gw.logger.Warn().Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
				Str("remote_addr", httputil.GetClientIP(r)).
				Msg("SSE connection rejected: tenant limit")
			// Capacity limit — see the WS-path parallel in gateway.go (§XVIII).
			httputil.WriteRateLimited(w, time.Second, "TENANT_LIMIT_EXCEEDED",
				"tenant connection limit reached")
			return
		}
		defer gw.connTracker.Release(authRes.TenantSlug)
	}

	// Check that response supports flushing (required for SSE)
	flusher, ok := w.(http.Flusher)
	if !ok {
		httputil.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "streaming not supported")
		return
	}

	// 5. Open gRPC Subscribe stream to ws-server
	if gw.serverClient == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "ws-server connection not available")
		return
	}

	// Decode the Last-Event-ID reconnect cursor (ADR-0030). An empty, foreign, or
	// unparseable token yields no last_pos → live-only; the cursor's channels are
	// tenant-validated server-side before any replay (ADR-0020), so the untrusted
	// token can never replay a channel the connection does not own.
	//
	// Intersect the decoded cursor against the channels this connection is actually
	// requesting: the cursor is opaque to the SSE client, so on a resubscribe after an
	// unsubscribe it still carries the dropped channel — and the server's replay path
	// live-registers any channel it replays. Without this filter, an unsubscribe→reconnect
	// would resurrect the removed channel from the stale cursor. Only replay cursor
	// entries for channels present in the (permission-filtered) request set (§II: the
	// gateway validates its own inputs; defense in depth over the server-side check).
	var lastPos map[string]string
	if cursor, ok := decodeSSECursor(r.Header.Get("Last-Event-ID")); ok {
		lastPos = intersectCursorChannels(cursor, channels)
	}

	stream, err := gw.serverClient.Client().Subscribe(ctx, &serverv1.SubscribeRequest{
		TenantSlug: authRes.TenantSlug,
		Principal:  authRes.Principal,
		Channels:   channels,
		RemoteAddr: httputil.GetClientIP(r),
		LastPos:    lastPos,
	})
	if err != nil {
		gw.logger.Error().Err(err).
			Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
			Strs("channels", channels).
			Msg("Failed to open Subscribe stream")
		httputil.WriteError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "failed to connect to message server")
		return
	}

	// Record SSE connection
	RecordSSEConnection()
	defer func() { RecordSSEDisconnection(time.Since(startTime)) }()

	// Register SSE connection for force-disconnect on token revocation
	if gw.connectionRegistry != nil && !authRes.APIKeyOnly && authRes.Claims != nil {
		sseCtx, sseCancel := context.WithCancel(ctx)
		defer sseCancel()
		ctx = sseCtx // replace ctx so stream.Recv() is canceled on force-disconnect

		var iatUnix int64
		if authRes.Claims.IssuedAt != nil {
			iatUnix = authRes.Claims.IssuedAt.Unix()
		}
		sseConn := &sseConnection{
			cancel: sseCancel,
			sub:    authRes.Claims.Subject,
			jti:    authRes.Claims.ID,
			iat:    iatUnix,
		}
		gw.connectionRegistry.Register(sseConn, authRes.TenantSlug, authRes.Claims.Subject, authRes.Claims.ID)
		defer gw.connectionRegistry.Unregister(sseConn, authRes.TenantSlug, authRes.Claims.Subject, authRes.Claims.ID)
		// Close the fan-out registration race on the SSE path: a matching revoke that fanned out
		// before this Register would otherwise miss the stream. On a hit, sseCancel() ends it
		// (§IX).
		gw.recheckRevocationAfterRegister(sseConn, authRes.TenantSlug)
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	gw.logger.Info().
		Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
		Str("principal", authRes.Principal).
		Strs("channels", channels).
		Str("remote_addr", httputil.GetClientIP(r)).
		Msg("SSE connection established")

	// 6. Stream reader goroutine — reads from gRPC, sends to channel.
	// Single-writer pattern: all writes to ResponseWriter happen in the main
	// goroutine below. This prevents data races between message writes and
	// keepalive writes (http.ResponseWriter is NOT thread-safe).
	msgCh := make(chan *serverv1.SubscribeResponse, 1)
	go func() {
		defer logging.RecoverPanic(gw.logger, "sse_stream_reader", nil)
		defer close(msgCh)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return // Stream ended (client disconnect, server shutdown, or error)
			}
			select {
			case msgCh <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()

	// 7. Main event loop — writes messages AND keepalives from a single goroutine.
	// No concurrent writes to ResponseWriter.
	keepaliveTicker := time.NewTicker(gw.config.SSEKeepAliveInterval)
	defer keepaliveTicker.Stop()

	// SSE reconnect cursor (ADR-0030): track the latest pos per channel from the
	// delivered messages and emit it as the opaque, versioned Last-Event-ID at a
	// bounded cadence — the first pos-bearing message (so an early disconnect still
	// leaves a resume point), then at most every SSECursorEveryN messages, plus a
	// keepalive-tick flush if it advanced. Encoding the cursor therefore never sits
	// on the per-message delivery path (§VII).
	cursor := make(map[string]string, len(channels))
	sinceCursor := 0
	cursorDirty := false
	cursorSent := false

	for {
		select {
		case resp, ok := <-msgCh:
			if !ok {
				// gRPC stream ended
				goto done
			}
			payload := resp.GetPayload()
			// Every server frame is delivered as `event: message`; the client routes on the
			// envelope's `type` in `data:` — this carries delivery messages, gap notifications, and
			// the reconnect-outcome frames (no_replay / replay_truncated, ADR-0030 §XV) uniformly.
			// Only pos-bearing delivery messages advance the opaque reconnect cursor (id:).
			if ch, pos, hasPos := sseMsgPos(payload); hasPos {
				cursor[ch] = pos
				cursorDirty = true
			}
			sinceCursor++
			idLine := ""
			if cursorDirty && (!cursorSent || (gw.config.SSECursorEveryN > 0 && sinceCursor >= gw.config.SSECursorEveryN)) {
				if tok := encodeSSECursor(cursor); tok != "" {
					idLine = "id: " + tok + "\n"
					sinceCursor, cursorDirty, cursorSent = 0, false, true
				}
			}
			if _, err := fmt.Fprintf(w, "%sevent: message\ndata: %s\n\n", idLine, payload); err != nil {
				goto done
			}
			flusher.Flush()

		case <-keepaliveTicker.C:
			// Flush a bare cursor id: if it advanced since the last emission (bounds
			// staleness for low-traffic streams); otherwise a keepalive comment.
			line := ": keepalive\n\n"
			if cursorDirty {
				if tok := encodeSSECursor(cursor); tok != "" {
					line = "id: " + tok + "\n\n"
					sinceCursor, cursorDirty, cursorSent = 0, false, true
				}
			}
			if _, err := fmt.Fprint(w, line); err != nil {
				goto done
			}
			flusher.Flush()

		case <-ctx.Done():
			goto done
		}
	}
done:

	gw.logger.Info().
		Str(logging.LogKeyTenantSlug, authRes.TenantSlug).
		Str("principal", authRes.Principal).
		Dur("connection_duration", time.Since(startTime)).
		Msg("SSE connection closed")
}
