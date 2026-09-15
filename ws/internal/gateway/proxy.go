package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// Proxy handles bidirectional WebSocket message forwarding between client and backend.
// It intercepts subscribe, publish, and auth refresh messages to:
// - Validate tenant prefix on all channels (always)
// - Filter subscribe channels based on permissions (auth only)
// - Validate publish rate limits and message size
// - Refresh JWT tokens mid-connection without disconnecting (auth only)
type Proxy struct {
	clientConn    net.Conn
	clientWriteMu sync.Mutex // protects all writes to clientConn
	backendConn   net.Conn
	closeOnce     sync.Once // guards net.Conn.Close() calls (double-close panics)
	closeReason   string    // force-close reason; written ONLY inside closeOnce.Do (first-caller wins), read only after Run returns (ForceCloseReason)
	logger        zerolog.Logger

	messageTimeout time.Duration
	maxFrameSize   int

	claims   *auth.Claims
	claimsMu sync.RWMutex // protects claims and subscribedChannels
	// filterSubscribe and canPublish are the gateway's shared authorization
	// closures (per-tenant rules) — the same implementations SSE, Web Push,
	// and REST publish use (§XVIII).
	filterSubscribe func(ctx context.Context, channels []string, claims *auth.Claims) []string
	canPublish      func(ctx context.Context, claims *auth.Claims, channel string) bool
	validator       TokenValidator

	// Auth refresh rate limiting
	authLimiter           *rate.Limiter
	authValidationTimeout time.Duration

	// Backend→client subscription tracking for forced unsubscription on auth refresh
	subscribedChannels map[string]struct{}

	// Tenant (always set — from JWT or API key)
	tenantID string

	// API key auth (only set when auth enabled + API-key-only connection)
	// apiKeyOnly is protected by claimsMu — it is auth state mutated during auth refresh
	apiKeyOnly     bool   // true when connected via API key without JWT
	apiKeyTenantID string // tenant from API key (for escalation tenant match)

	// Publish rate limiting and validation
	publishLimiter *rate.Limiter
	maxPublishSize int
}

// ProxyConfig holds configuration for creating a Proxy.
type ProxyConfig struct {
	ClientConn     net.Conn
	BackendConn    net.Conn
	Logger         zerolog.Logger
	MessageTimeout time.Duration

	Claims *auth.Claims
	// FilterSubscribe filters tenant-prefixed channels to the allowed subset
	// (gateway closure over filterSubscribeChannels — per-tenant rules).
	FilterSubscribe func(ctx context.Context, channels []string, claims *auth.Claims) []string
	// CanPublish authorizes a publish to a tenant-prefixed channel
	// (gateway closure over checkPublishAllowed — per-tenant rules).
	CanPublish func(ctx context.Context, claims *auth.Claims, channel string) bool
	Validator  TokenValidator

	// Auth refresh rate interval (minimum time between auth refreshes)
	AuthRefreshRateInterval time.Duration

	// AuthValidationTimeout is the context timeout for token validation during auth refresh.
	AuthValidationTimeout time.Duration

	// Tenant (always set — from JWT or config)
	TenantID string

	// API key auth (only set when auth enabled + API-key-only connection)
	APIKeyOnly     bool   // true when connected via API key without JWT
	APIKeyTenantID string // tenant from API key (for escalation tenant match)

	// Publish rate limiting (tokens per second, burst size)
	PublishRateLimit float64
	PublishBurst     int

	// Max publish message size in bytes.
	MaxPublishSize int

	// MaxFrameSize is the maximum allowed WebSocket frame size in bytes.
	MaxFrameSize int
}

// NewProxy creates a new proxy for a client-backend connection pair.
// All config values must be set by the caller (sourced from gateway config envDefaults).
func NewProxy(cfg ProxyConfig) *Proxy {
	return &Proxy{
		clientConn:            cfg.ClientConn,
		backendConn:           cfg.BackendConn,
		logger:                cfg.Logger,
		messageTimeout:        cfg.MessageTimeout,
		maxFrameSize:          cfg.MaxFrameSize,
		claims:                cfg.Claims,
		filterSubscribe:       cfg.FilterSubscribe,
		canPublish:            cfg.CanPublish,
		validator:             cfg.Validator,
		tenantID:              cfg.TenantID,
		apiKeyOnly:            cfg.APIKeyOnly,
		apiKeyTenantID:        cfg.APIKeyTenantID,
		publishLimiter:        rate.NewLimiter(rate.Limit(cfg.PublishRateLimit), cfg.PublishBurst),
		maxPublishSize:        cfg.MaxPublishSize,
		authLimiter:           rate.NewLimiter(rate.Every(cfg.AuthRefreshRateInterval), 1),
		authValidationTimeout: cfg.AuthValidationTimeout,
		subscribedChannels:    make(map[string]struct{}),
	}
}

// Run starts bidirectional message forwarding.
// Blocks until either connection closes, errors, or context is canceled.
//
// Note: The pump goroutines use blocking net.Conn reads which cannot be
// multiplexed with select+ctx.Done(). Shutdown is signaled by closing the
// connections (via closeOnce), which unblocks the reads. The goroutines then
// check ctx.Err() after read failure. This is the standard Go pattern for
// net.Conn-based goroutines.
func (p *Proxy) Run(ctx context.Context) {
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Backend (with message interception)
	go func() {
		defer logging.RecoverPanic(p.logger, "proxyClientToBackend", nil)
		defer wg.Done()
		p.proxyClientToBackend(ctx, errChan)
	}()

	// Backend -> Client (pass-through)
	go func() {
		defer logging.RecoverPanic(p.logger, "proxyBackendToClient", nil)
		defer wg.Done()
		p.proxyBackendToClient(ctx, errChan)
	}()

	// Wait for first error or context cancellation
	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, io.EOF) {
			p.logger.Warn().Err(err).Msg("Connection closed with error")
		} else {
			p.logger.Debug().Msg("Connection closed normally")
		}
	case <-ctx.Done():
		p.logger.Info().Msg("Context canceled, closing proxy")
	}

	// Close both connections to unblock waiting goroutines.
	// sync.Once guards against double-close: gateway.go defers Close() on both
	// connections, and this path also closes them to unblock the pump goroutines.
	p.closeOnce.Do(func() {
		_ = p.clientConn.Close()
		_ = p.backendConn.Close()
	})

	// Wait for both goroutines to complete
	wg.Wait()
}

// proxyClientToBackend forwards messages from client to backend,
// intercepting subscribe messages to filter channels.
// Handles all frame types including ping/pong transparently.
func (p *Proxy) proxyClientToBackend(ctx context.Context, errChan chan error) {
	for {
		// Set read deadline for message timeout enforcement.
		// Guard: messageTimeout is validated >= 1s at startup (Validate()), but check > 0
		// as defense-in-depth for test code that bypasses config validation.
		if p.messageTimeout > 0 {
			_ = p.clientConn.SetReadDeadline(time.Now().Add(p.messageTimeout)) // Intentional: deadline errors handled by ReadHeader
		}

		// Read frame header
		header, err := ws.ReadHeader(p.clientConn)
		if err != nil {
			// Check for context cancellation (shutdown signal)
			if ctx.Err() != nil {
				errChan <- ctx.Err()
				return
			}
			if !errors.Is(err, io.EOF) {
				RecordProxyError(ProxyErrorClientRead)
				p.logger.Debug().Err(err).Msg("Client read error")
			}
			errChan <- err
			return
		}

		// Validate frame size before allocation
		if header.Length > int64(p.maxFrameSize) {
			RecordProxyError(ProxyErrorFrameTooLarge)
			p.logger.Warn().
				Int64("frame_size", header.Length).
				Int("max_frame_size", p.maxFrameSize).
				Msg("Client frame exceeds maximum size")
			closeBody := ws.NewCloseFrameBody(ws.StatusMessageTooBig, "Frame too large")
			p.sendCloseToClient(closeBody)
			errChan <- fmt.Errorf("client frame size %d exceeds max %d", header.Length, p.maxFrameSize)
			return
		}

		// Read payload
		payload := make([]byte, header.Length)
		if header.Length > 0 {
			if _, err := io.ReadFull(p.clientConn, payload); err != nil {
				RecordProxyError(ProxyErrorClientRead)
				p.logger.Debug().Err(err).Msg("Client payload read error")
				errChan <- err
				return
			}
		}

		// Unmask client frames (client->server frames are always masked)
		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		// Handle close frame
		if header.OpCode == ws.OpClose {
			// LOG-022: Close frame with code and reason
			closeCode, closeReason := parseClosePayload(payload)
			p.logger.Debug().Int("close_code", closeCode).Str("close_reason", closeReason).
				Str("direction", "client").Msg("close frame received")
			p.forwardCloseFrame(p.backendConn, payload, true)
			errChan <- nil
			return
		}

		// For text frames, intercept and possibly modify (subscribe filtering, publish mapping)
		if header.OpCode == ws.OpText {
			// Intentional: errors from interception are handled internally by sending
			// error responses directly to the client (e.g., publish errors, auth errors).
			// A nil payload signals the message was consumed and should not be forwarded.
			payload, _ = p.interceptClientMessage(ctx, payload)
			// If payload is nil, message was handled (e.g., error sent to client)
			if payload == nil {
				continue
			}
		}

		// Record message metrics
		RecordMessage(DirectionClientToBackend, len(payload))

		// Forward frame to backend (re-mask for server)
		if err := p.forwardFrame(p.backendConn, header.OpCode, payload, header.Fin, true); err != nil {
			RecordProxyError(ProxyErrorBackendWrite)
			p.logger.Debug().Err(err).Msg("Backend write error")
			errChan <- err
			return
		}
	}
}

// proxyBackendToClient forwards messages from backend to client (pass-through).
// Handles all frame types including ping/pong transparently.
func (p *Proxy) proxyBackendToClient(ctx context.Context, errChan chan error) {
	for {
		// Set read deadline for message timeout enforcement.
		// Guard: see proxyClientToBackend — defense-in-depth for test code bypassing config validation.
		if p.messageTimeout > 0 {
			_ = p.backendConn.SetReadDeadline(time.Now().Add(p.messageTimeout)) // Intentional: deadline errors handled by ReadHeader
		}

		// Read frame header
		header, err := ws.ReadHeader(p.backendConn)
		if err != nil {
			// Check for context cancellation (shutdown signal)
			if ctx.Err() != nil {
				errChan <- ctx.Err()
				return
			}
			if !errors.Is(err, io.EOF) {
				RecordProxyError(ProxyErrorBackendRead)
				p.logger.Debug().Err(err).Msg("Backend read error")
			}
			errChan <- err
			return
		}

		// Validate frame size before allocation
		if header.Length > int64(p.maxFrameSize) {
			RecordProxyError(ProxyErrorFrameTooLarge)
			p.logger.Warn().
				Int64("frame_size", header.Length).
				Int("max_frame_size", p.maxFrameSize).
				Msg("Backend frame exceeds maximum size")
			errChan <- fmt.Errorf("backend frame size %d exceeds max %d", header.Length, p.maxFrameSize)
			return
		}

		// Read payload
		payload := make([]byte, header.Length)
		if header.Length > 0 {
			if _, err := io.ReadFull(p.backendConn, payload); err != nil {
				RecordProxyError(ProxyErrorBackendRead)
				p.logger.Debug().Err(err).Msg("Backend payload read error")
				errChan <- err
				return
			}
		}

		// Server frames are not masked, no need to unmask

		// Handle close frame
		if header.OpCode == ws.OpClose {
			// LOG-022: Close frame with code and reason
			closeCode, closeReason := parseClosePayload(payload)
			p.logger.Debug().Int("close_code", closeCode).Str("close_reason", closeReason).
				Str("direction", "backend").Msg("close frame received")
			p.sendCloseToClient(payload)
			errChan <- nil
			return
		}

		// Track subscription acks from backend (observational — always forward to client)
		if header.OpCode == ws.OpText {
			p.trackSubscriptionResponse(payload)
		}

		// Record message metrics
		RecordMessage(DirectionBackendToClient, len(payload))

		// Forward all frames to client (server->client: no masking)
		if err := p.sendToClient(header.OpCode, payload, header.Fin); err != nil {
			RecordProxyError(ProxyErrorClientWrite)
			p.logger.Debug().Err(err).Msg("Client write error")
			errChan <- err
			return
		}
	}
}

// forwardFrame forwards a WebSocket frame to the destination.
// If mask is true, the frame will be masked (required for client->server frames).
func (p *Proxy) forwardFrame(dst net.Conn, opCode ws.OpCode, payload []byte, fin, mask bool) error {
	header := ws.Header{
		Fin:    fin,
		OpCode: opCode,
		Length: int64(len(payload)),
	}

	if mask {
		header.Masked = true
		header.Mask = ws.NewMask()
		ws.Cipher(payload, header.Mask, 0)
	}

	if err := ws.WriteHeader(dst, header); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := dst.Write(payload); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}
	return nil
}

// parseClosePayload extracts the close code and reason from a WebSocket close frame payload.
// Returns (0, "") if the payload is too short to contain a status code.
func parseClosePayload(payload []byte) (code int, reason string) {
	if len(payload) >= 2 { //nolint:mnd // WebSocket close frame: 2-byte status code per RFC 6455 §5.5.1
		code = int(payload[0])<<8 | int(payload[1])
		if len(payload) > 2 {
			reason = string(payload[2:])
		}
	}
	return code, reason
}

// forwardCloseFrame forwards a close frame to the destination.
func (p *Proxy) forwardCloseFrame(dst net.Conn, payload []byte, mask bool) {
	header := ws.Header{
		Fin:    true,
		OpCode: ws.OpClose,
		Length: int64(len(payload)),
	}
	if mask {
		header.Masked = true
		header.Mask = ws.NewMask()
		ws.Cipher(payload, header.Mask, 0)
	}
	// Best-effort: connection is closing, write errors are expected and non-recoverable.
	_ = ws.WriteHeader(dst, header)
	if len(payload) > 0 {
		// Best-effort: connection is closing, write errors are expected and non-recoverable.
		_, _ = dst.Write(payload)
	}
}

// sendToClient sends a WebSocket frame to the client under the write mutex.
// This ensures atomicity of the two-phase write (header + payload) when
// multiple goroutines may write to clientConn concurrently.
func (p *Proxy) sendToClient(opCode ws.OpCode, payload []byte, fin bool) error {
	p.clientWriteMu.Lock()
	defer p.clientWriteMu.Unlock()
	return p.forwardFrame(p.clientConn, opCode, payload, fin, false)
}

// sendCloseToClient sends a close frame to the client under the write mutex.
func (p *Proxy) sendCloseToClient(payload []byte) {
	p.clientWriteMu.Lock()
	defer p.clientWriteMu.Unlock()
	p.forwardCloseFrame(p.clientConn, payload, false)
}

// trackSubscriptionResponse observes backend→client subscription ack/unsubscription ack
// messages and updates the subscribedChannels map. This is observational — messages are
// always forwarded to the client regardless.
//
// Uses a fast byte check to skip 99%+ of broadcast messages without JSON parsing:
// only messages containing "_ack" in the first 80 bytes are candidates.
func (p *Proxy) trackSubscriptionResponse(payload []byte) {
	// Fast path: skip messages that can't be subscription acks
	prefixLen := min(subscriptionAckPrefixScanLen, len(payload))
	if !bytes.Contains(payload[:prefixLen], []byte("_ack")) {
		return
	}

	// Partial parse to extract the type field
	var msg struct {
		Type       string   `json:"type"`
		Subscribed []string `json:"subscribed,omitempty"`
		// unsubscription_ack uses "unsubscribed" field
		Unsubscribed []string `json:"unsubscribed,omitempty"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "subscription_ack":
		if len(msg.Subscribed) > 0 {
			p.claimsMu.Lock()
			for _, ch := range msg.Subscribed {
				p.subscribedChannels[ch] = struct{}{}
			}
			p.claimsMu.Unlock()
		}
	case RespTypeUnsubscriptionAck:
		if len(msg.Unsubscribed) > 0 {
			p.claimsMu.Lock()
			for _, ch := range msg.Unsubscribed {
				delete(p.subscribedChannels, ch)
			}
			p.claimsMu.Unlock()
		}
	}
}

// interceptClientMessage intercepts client messages for subscribe and publish requests.
func (p *Proxy) interceptClientMessage(ctx context.Context, msg []byte) ([]byte, error) {
	// Defensive guard — tenantID is always set (from JWT or API key).
	if p.tenantID == "" {
		return msg, nil
	}

	// Fast path: skip JSON parsing for messages that can't be JSON objects
	if len(msg) == 0 || msg[0] != '{' {
		return msg, nil
	}

	var clientMsg protocol.ClientMessage
	if err := json.Unmarshal(msg, &clientMsg); err != nil {
		// Not valid JSON, pass through (intentional: non-JSON messages are passed unchanged)
		return msg, nil //nolint:nilerr // Intentional: pass through non-JSON messages
	}

	switch clientMsg.Type {
	case protocol.MsgTypeSubscribe:
		return p.interceptSubscribe(ctx, clientMsg)
	case protocol.MsgTypePublish:
		return p.interceptPublish(ctx, clientMsg)
	case MsgTypeAuth:
		return p.interceptAuthRefresh(ctx, clientMsg)
	default:
		return msg, nil
	}
}

// interceptSubscribe intercepts subscribe messages and filters the channel
// list through the gateway's shared subscribe-authorization path
// (filterSubscribeChannels — the same implementation SSE and Web Push use,
// §XVIII). The filter validates tenant prefix + format, applies the
// per-tenant rules, records all metrics/logs, and returns the allowed
// tenant-prefixed subset.
func (p *Proxy) interceptSubscribe(ctx context.Context, clientMsg protocol.ClientMessage) ([]byte, error) {
	// Parse subscribe data
	var subData protocol.SubscribeData
	if err := json.Unmarshal(clientMsg.Data, &subData); err != nil {
		p.logger.Warn().Err(err).Msg("Failed to parse subscribe data")
		data, marshalErr := json.Marshal(clientMsg)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal subscribe passthrough: %w", marshalErr)
		}
		return data, nil
	}

	// Read claims under lock — may be swapped by auth refresh
	p.claimsMu.RLock()
	currentClaims := p.claims
	p.claimsMu.RUnlock()

	channels := p.filterSubscribe(ctx, subData.Channels, currentClaims)

	// Rebuild message with validated channels
	modifiedData := protocol.SubscribeData{Channels: channels}
	dataBytes, err := json.Marshal(modifiedData)
	if err != nil {
		data, marshalErr := json.Marshal(clientMsg)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal subscribe fallback: %w", marshalErr)
		}
		return data, nil
	}

	data, marshalErr := json.Marshal(protocol.ClientMessage{
		Type: protocol.MsgTypeSubscribe,
		Data: dataBytes,
	})
	if marshalErr != nil {
		return nil, fmt.Errorf("marshal subscribe message: %w", marshalErr)
	}
	return data, nil
}

// validateChannelTenant checks that the channel's tenant prefix matches the connection's tenant.
// Delegates to shared auth.ValidateChannelTenant for the prefix check.
func (p *Proxy) validateChannelTenant(channel string) bool {
	return auth.ValidateChannelTenant(channel, p.tenantID)
}

// interceptPublish intercepts publish messages to:
// 1. Validate tenant prefix
// 2. Validate channel format ({tenant}.{suffix}, minimum 2 dot-separated parts)
// 3. Rate limit publishes
// 4. Validate message size
// 5. Enforce per-tenant publish rules (shared check with REST publish, §XVIII)
func (p *Proxy) interceptPublish(ctx context.Context, clientMsg protocol.ClientMessage) ([]byte, error) {
	start := time.Now()
	defer func() {
		RecordPublishLatency(time.Since(start).Seconds())
	}()

	// Block publish for API-key-only connections (read-only until JWT escalation)
	p.claimsMu.RLock()
	isAPIKeyOnly := p.apiKeyOnly
	p.claimsMu.RUnlock()
	if isAPIKeyOnly {
		RecordPublishResult(string(protocol.ErrCodeForbidden))
		return p.sendPublishErrorToClient(protocol.ErrCodeForbidden)
	}

	// 1. Rate limit check
	if !p.publishLimiter.Allow() {
		RecordPublishResult(string(protocol.ErrCodeRateLimited))
		return p.sendPublishErrorToClient(protocol.ErrCodeRateLimited)
	}

	// 2. Parse publish data
	var pubData protocol.PublishData
	if err := json.Unmarshal(clientMsg.Data, &pubData); err != nil {
		p.logger.Warn().Err(err).Msg("Failed to parse publish data")
		RecordPublishResult(string(protocol.ErrCodeInvalidRequest))
		return p.sendPublishErrorToClient(protocol.ErrCodeInvalidRequest)
	}

	// 3. Size check
	if len(pubData.Data) > p.maxPublishSize {
		p.logger.Warn().
			Int("size", len(pubData.Data)).
			Int("max_size", p.maxPublishSize).
			Msg("Publish message too large")
		RecordPublishResult(string(protocol.ErrCodeMessageTooLarge))
		return p.sendPublishErrorToClient(protocol.ErrCodeMessageTooLarge)
	}

	// 4. Validate channel has minimum parts ({tenant}.{suffix})
	parts := strings.Split(pubData.Channel, ".")
	if len(parts) < protocol.MinInternalChannelParts {
		p.logger.Warn().
			Str("channel", pubData.Channel).
			Msg("Invalid publish channel format")
		RecordPublishResult(string(protocol.ErrCodeInvalidChannel))
		return p.sendPublishErrorToClient(protocol.ErrCodeInvalidChannel)
	}

	// 5. Validate tenant prefix
	if !p.validateChannelTenant(pubData.Channel) {
		p.logger.Warn().
			Str("channel", pubData.Channel).
			Msg("Publish access denied: wrong tenant prefix")
		RecordPublishResult(string(protocol.ErrCodeForbidden))
		return p.sendPublishErrorToClient(protocol.ErrCodeForbidden)
	}

	// 6. Enforce per-tenant publish rules — the same shared check REST
	// publish uses (checkPublishAllowed → tenant checker). Full channel is
	// passed; the helper strips the tenant prefix internally.
	p.claimsMu.RLock()
	currentClaims := p.claims
	p.claimsMu.RUnlock()
	if !p.canPublish(ctx, currentClaims, pubData.Channel) {
		RecordPublishResult(string(protocol.ErrCodeForbidden))
		return p.sendPublishErrorToClient(protocol.ErrCodeForbidden)
	}

	p.logger.Debug().
		Str("channel", pubData.Channel).
		Msg("Publish channel validated")

	RecordPublishResult(pkgmetrics.ResultSuccess)

	// Forward original message as-is (no channel transformation)
	data, err := json.Marshal(clientMsg)
	if err != nil {
		return nil, fmt.Errorf("marshal publish message: %w", err)
	}
	return data, nil
}

// sendPublishErrorToClient sends a publish error directly to the client WebSocket.
// Returns nil bytes to signal that the message should NOT be forwarded to backend.
//
//nolint:unparam // Always returns nil bytes by design - indicates message should not be forwarded
func (p *Proxy) sendPublishErrorToClient(code protocol.ErrorCode) ([]byte, error) {
	errMsg := map[string]string{
		"type":    protocol.RespTypePublishError,
		"code":    string(code),
		"message": protocol.PublishErrorMessage(code),
	}

	errBytes, err := json.Marshal(errMsg)
	if err != nil {
		return nil, fmt.Errorf("marshal publish error: %w", err)
	}

	// Send error directly to client (mutex-protected - may race with proxyBackendToClient)
	if sendErr := p.sendToClient(ws.OpText, errBytes, true); sendErr != nil {
		p.logger.Warn().Err(sendErr).Msg("Failed to send publish error to client")
	}

	// Return nil to signal that this message should not be forwarded
	return nil, nil
}

// interceptAuthRefresh handles mid-connection JWT token refresh.
// Validates the new token, checks tenant match, swaps claims, and forces
// unsubscription from channels no longer permitted under the new token.
func (p *Proxy) interceptAuthRefresh(ctx context.Context, clientMsg protocol.ClientMessage) ([]byte, error) {
	start := time.Now()
	defer func() {
		RecordAuthRefreshLatency(time.Since(start).Seconds())
	}()

	// 1. Rate limit check
	if !p.authLimiter.Allow() {
		// LOG-016: Auth refresh denied — rate limited
		p.logger.Warn().Str(logging.LogKeyTenantSlug, p.tenantID).Str("reason", "rate_limited").
			Msg("auth refresh denied")
		RecordAuthRefresh(AuthErrRateLimited)
		return p.sendAuthErrorToClient(AuthErrRateLimited, AuthErrorMessages[AuthErrRateLimited])
	}

	// 3. Parse auth data
	var authData AuthData
	if err := json.Unmarshal(clientMsg.Data, &authData); err != nil || authData.Token == "" {
		// LOG-016: Auth refresh denied — invalid token
		p.logger.Warn().Str(logging.LogKeyTenantSlug, p.tenantID).Str("reason", "invalid_token").
			Msg("auth refresh denied")
		RecordAuthRefresh(AuthErrInvalidToken)
		return p.sendAuthErrorToClient(AuthErrInvalidToken, AuthErrorMessages[AuthErrInvalidToken])
	}

	// 4. Validate the new token
	if p.validator == nil {
		// LOG-016: Auth refresh denied — not available
		p.logger.Warn().Str(logging.LogKeyTenantSlug, p.tenantID).Str("reason", "not_available").
			Msg("auth refresh denied")
		RecordAuthRefresh(AuthErrNotAvailable)
		return p.sendAuthErrorToClient(AuthErrNotAvailable, AuthErrorMessages[AuthErrNotAvailable])
	}

	validationCtx, cancel := context.WithTimeout(ctx, p.authValidationTimeout)
	defer cancel()

	newClaims, err := p.validator.ValidateToken(validationCtx, authData.Token)
	if err != nil {
		code := AuthErrInvalidToken
		if errors.Is(err, auth.ErrTokenExpired) {
			code = AuthErrTokenExpired
		}
		if errors.Is(err, auth.ErrTenantMismatch) {
			// Cross-tenant refresh token — signed by a different tenant's key than
			// it claims. Route to the tenant-mismatch code (§XVIII consistency).
			code = AuthErrTenantMismatch
		}
		// LOG-016: Auth refresh denied — validation failed
		p.logger.Warn().Str(logging.LogKeyTenantSlug, p.tenantID).Str("reason", code).
			Msg("auth refresh denied")
		RecordAuthRefresh(code)
		return p.sendAuthErrorToClient(code, AuthErrorMessages[code])
	}

	// 5. Verify tenant match — cannot switch tenants mid-connection
	if newClaims.TenantID != p.tenantID {
		p.logger.Warn().
			Str("connection_tenant", p.tenantID).
			Str("token_tenant", newClaims.TenantID).
			Msg("Auth refresh tenant mismatch")
		RecordAuthRefresh(AuthErrTenantMismatch)
		return p.sendAuthErrorToClient(AuthErrTenantMismatch, AuthErrorMessages[AuthErrTenantMismatch])
	}

	// 5b. On an API-key-origin connection, bind the refreshed JWT to the API key's owning
	// tenant by its identity-of-record UUID (complementing step 5's slug binding). Compare
	// the token's *resolved* tenant UUID against the key's tenant UUID — never the TenantID
	// slug, which is a different representation and never matches (MatchesTenantUUID; same
	// check the connect path uses).
	if p.apiKeyTenantID != "" && !newClaims.MatchesTenantUUID(p.apiKeyTenantID) {
		p.logger.Warn().
			Str("api_key_tenant", p.apiKeyTenantID).
			Str("token_tenant_uuid", newClaims.ResolvedTenantUUID).
			Msg("Auth refresh: JWT tenant does not match API key tenant")
		RecordAuthRefresh(AuthErrTenantMismatch)
		return p.sendAuthErrorToClient(AuthErrTenantMismatch, AuthErrorMessages[AuthErrTenantMismatch])
	}

	// 6. Force unsubscribe from channels no longer permitted
	revoked := p.forceUnsubscribeRevokedChannels(ctx, newClaims)

	// 7. Swap claims and escalate from API-key-only if applicable (lock is brief — no I/O)
	p.claimsMu.Lock()
	p.claims = newClaims
	// Escalate from API-key-only to full JWT access (unblocks publish, grants non-public channels)
	if p.apiKeyOnly {
		p.apiKeyOnly = false
		p.claimsMu.Unlock()
		p.logger.Info().Msg("Escalated from API-key-only to JWT auth")
	} else {
		p.claimsMu.Unlock()
	}

	// 8. Send auth_ack to client
	var exp int64
	if newClaims.ExpiresAt != nil {
		exp = newClaims.ExpiresAt.Unix()
	}
	ackResp := AuthAckResponse{
		Type: RespTypeAuthAck,
		Data: AuthAckData{Exp: exp},
	}
	ackBytes, err := json.Marshal(ackResp)
	if err != nil {
		return nil, fmt.Errorf("marshal auth ack: %w", err)
	}
	if sendErr := p.sendToClient(ws.OpText, ackBytes, true); sendErr != nil {
		p.logger.Warn().Err(sendErr).Msg("Failed to send auth ack to client")
	}

	// 9. Record metrics and log
	RecordAuthRefresh(pkgmetrics.AuthStatusSuccess)

	p.logger.Info().
		Str("subject", newClaims.Subject).
		Int("revoked_channels", len(revoked)).
		Int64("new_exp", exp).
		Msg("Auth refresh successful")

	return nil, nil
}

// forceUnsubscribeRevokedChannels checks which currently subscribed channels are
// no longer permitted under the new claims (re-validated through the shared
// per-tenant rules filter), and sends a synthetic unsubscribe to the backend
// for those channels.
//
// Uses a three-phase pattern (Constitution VII — no mutex across I/O or
// nested provider locks): snapshot under lock → filter without lock →
// delete under lock. Client messages (subscribe, auth refresh) are processed
// sequentially on the read loop, so no subscribe can interleave between
// phases.
func (p *Proxy) forceUnsubscribeRevokedChannels(ctx context.Context, newClaims *auth.Claims) []string {
	// Phase A: snapshot subscribed channels under lock.
	p.claimsMu.Lock()
	subscribed := make([]string, 0, len(p.subscribedChannels))
	for ch := range p.subscribedChannels {
		subscribed = append(subscribed, ch)
	}
	p.claimsMu.Unlock()

	if len(subscribed) == 0 {
		return nil
	}

	// Phase B: re-validate through the shared filter (provider lookups happen
	// here, outside claimsMu).
	allowed := p.filterSubscribe(ctx, subscribed, newClaims)
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, ch := range allowed {
		allowedSet[ch] = struct{}{}
	}
	var revoked []string
	for _, ch := range subscribed {
		if _, ok := allowedSet[ch]; !ok {
			revoked = append(revoked, ch)
		}
	}

	// Phase C: remove revoked from tracking under lock.
	p.claimsMu.Lock()
	for _, ch := range revoked {
		delete(p.subscribedChannels, ch)
	}
	p.claimsMu.Unlock()

	if len(revoked) == 0 {
		return nil
	}

	// Phase B: I/O operations with lock released

	// Send synthetic unsubscribe to backend (fire-and-forget)
	unsubMsg := protocol.ClientMessage{
		Type: protocol.MsgTypeUnsubscribe,
	}
	unsubData, err := json.Marshal(protocol.SubscribeData{Channels: revoked})
	if err != nil {
		p.logger.Warn().Err(err).Msg("Failed to marshal forced unsubscribe data")
	} else {
		unsubMsg.Data = unsubData
		msgBytes, marshalErr := json.Marshal(unsubMsg)
		if marshalErr != nil {
			p.logger.Warn().Err(marshalErr).Msg("Failed to marshal forced unsubscribe message")
		} else {
			// Forward to backend — fire-and-forget, don't wait for ack
			if fwdErr := p.forwardFrame(p.backendConn, ws.OpText, msgBytes, true, true); fwdErr != nil {
				p.logger.Warn().Err(fwdErr).Msg("Failed to send forced unsubscribe to backend")
			}
		}
	}

	// Send unsubscription_ack to client
	clientAck := map[string]any{
		"type":         RespTypeUnsubscriptionAck,
		"unsubscribed": revoked,
		"forced":       true,
	}
	ackBytes, ackErr := json.Marshal(clientAck)
	if ackErr != nil {
		p.logger.Warn().Err(ackErr).Msg("Failed to marshal forced unsubscription ack")
	} else {
		if sendErr := p.sendToClient(ws.OpText, ackBytes, true); sendErr != nil {
			p.logger.Warn().Err(sendErr).Msg("Failed to send forced unsubscription ack to client")
		}
	}

	// Record metrics
	RecordForcedUnsubscription()

	p.logger.Info().
		Strs("revoked_channels", revoked).
		Msg("Forced unsubscription due to auth refresh")

	return revoked
}

// sendAuthErrorToClient sends an auth_error response to the client.
// Returns nil bytes to signal that the message should NOT be forwarded to backend.
//
//nolint:unparam // Always returns nil bytes by design
func (p *Proxy) sendAuthErrorToClient(code, message string) ([]byte, error) {
	errResp := AuthErrorResponse{
		Type: RespTypeAuthError,
		Data: AuthErrorData{
			Code:    code,
			Message: message,
		},
	}
	errBytes, err := json.Marshal(errResp)
	if err != nil {
		return nil, fmt.Errorf("marshal auth error: %w", err)
	}
	if sendErr := p.sendToClient(ws.OpText, errBytes, true); sendErr != nil {
		p.logger.Warn().Err(sendErr).Msg("Failed to send auth error to client")
	}
	return nil, nil
}

// ForceClose sends a WebSocket close frame with the given code and reason,
// then closes both client and backend connections. Used for token revocation
// force-disconnect. Safe to call concurrently (uses closeOnce).
func (p *Proxy) ForceClose(code int, reason string) {
	// Build close frame payload: 2-byte status code + reason string
	payload := make([]byte, 2+len(reason)) //nolint:mnd // WebSocket close frame: 2-byte status code per RFC 6455 §5.5.1
	payload[0] = byte(code >> 8)           //nolint:mnd,gosec // G115: WebSocket close codes are 1000-4999, fits in 2 bytes per RFC 6455 §7.4
	payload[1] = byte(code & 0xFF)         //nolint:mnd // Low byte of close code
	copy(payload[2:], reason)

	p.sendCloseToClient(payload)
	p.closeOnce.Do(func() {
		// Store the reason INSIDE the Once so the first caller wins and the write is Once-ordered:
		// the handler reads it after Run returns (Run also closes via this same closeOnce), and a
		// late fan-out that is the second Do caller never runs this closure — race-free by
		// construction (no atomic needed; cold path). See ForceCloseReason.
		p.closeReason = reason
		_ = p.clientConn.Close()
		_ = p.backendConn.Close()
	})
}

// ForceCloseReason returns the reason stored by a ForceClose, or "" if the connection was not
// force-closed. Safe to read ONLY after Run has returned: Run's teardown calls the same closeOnce,
// so sync.Once's happens-before orders any ForceClose store before Run returns, before this read.
// Never read it mid-flight (that would race the Do-closure write).
func (p *Proxy) ForceCloseReason() string {
	return p.closeReason
}

// Transport returns TransportWS for WebSocket connections.
func (p *Proxy) Transport() string { return TransportWS }

// ConnectionClaims returns the JWT claims from this connection for revocation matching.
// Implements the Connection interface.
func (p *Proxy) ConnectionClaims() (sub, jti string, iat int64) {
	p.claimsMu.RLock()
	defer p.claimsMu.RUnlock()
	if p.claims == nil {
		return "", "", 0
	}
	var iatUnix int64
	if p.claims.IssuedAt != nil {
		iatUnix = p.claims.IssuedAt.Unix()
	}
	return p.claims.Subject, p.claims.ID, iatUnix
}
