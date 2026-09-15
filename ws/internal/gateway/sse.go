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

	stream, err := gw.serverClient.Client().Subscribe(ctx, &serverv1.SubscribeRequest{
		TenantSlug: authRes.TenantSlug,
		Principal:  authRes.Principal,
		Channels:   channels,
		RemoteAddr: httputil.GetClientIP(r),
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

	for {
		select {
		case resp, ok := <-msgCh:
			if !ok {
				// gRPC stream ended
				goto done
			}
			// Format as SSE event:
			//   id: {sequence}
			//   event: message
			//   data: {payload}
			//
			// Payload is the full JSON envelope from ws-server — written as-is.
			// Sequence is used for Last-Event-ID reconnection.
			if _, err := fmt.Fprintf(w, "id: %d\nevent: message\ndata: %s\n\n", resp.GetSequence(), resp.GetPayload()); err != nil {
				goto done
			}
			flusher.Flush()

		case <-keepaliveTicker.C:
			// SSE comment — not an event, keeps connection alive
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
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
