package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
)

// shutdownReadDeadline is a shorter read deadline used during shutdown
// to unblock ReadLoop quickly when the context is canceled.
const shutdownReadDeadline = 100 * time.Millisecond

// Client is a WebSocket client for the tester service.
type Client struct {
	conn      net.Conn
	rw        io.ReadWriter // reads from br (if Dial buffered data), writes to conn
	closeOnce sync.Once
	logger    zerolog.Logger
	onMsg     func(Message)
	mu        sync.Mutex // protects writeJSON only
}

// Message represents a WebSocket message received from the server.
type Message struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel,omitempty"`
	Code    string          `json:"code,omitempty"` // top-level error code on error frames (e.g. publish_error → "rate_limited"); "" on non-error frames
	Data    json.RawMessage `json:"data,omitempty"`
	Mid     string          `json:"mid,omitempty"` // stable message identity (ADR-0008); empty on frames predating the field
	// Pos is the per-channel replay cursor on kafka-backend message frames —
	// the anchor for reconnect{last_pos} and replay{from_pos}. Opaque; empty in
	// direct mode and on non-message frames.
	Pos string `json:"pos,omitempty"`
	// Subscribed lists the channels a subscription_ack actually accepted — the
	// gateway silently FILTERS channels denied by not-yet-propagated rules, so
	// callers must verify their channel appears here (#242).
	Subscribed []string `json:"subscribed,omitempty"`
	// MessagesReplayed is the TOP-LEVEL count on replay_complete/reconnect_ack
	// frames (the server's envelopes are flat — no data nesting); 0 elsewhere.
	MessagesReplayed int `json:"messages_replayed,omitempty"`
	// ErrMessage is the human-readable text on flat error envelopes
	// (history_error/reconnect_error/error) — the companion to Code.
	ErrMessage string `json:"message,omitempty"`
}

// ConnectConfig holds parameters for establishing a WebSocket connection.
type ConnectConfig struct {
	GatewayURL string
	Token      string
	APIKey     string
	Logger     zerolog.Logger
	OnMessage  func(Message)
}

// Connect dials the gateway and returns a connected Client.
func Connect(ctx context.Context, cfg ConnectConfig) (*Client, error) {
	wsURL := cfg.GatewayURL + "/ws"
	header := http.Header{}
	if cfg.Token != "" {
		header.Set("Authorization", "Bearer "+cfg.Token)
	}
	if cfg.APIKey != "" {
		header.Set("X-API-Key", cfg.APIKey)
	}

	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(header)}
	conn, br, _, err := dialer.Dial(ctx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", wsURL, err)
	}

	// br may contain bytes buffered during the handshake that haven't been
	// consumed yet. Use it as the reader so those bytes aren't lost.
	var rw io.ReadWriter = conn
	if br != nil {
		rw = struct {
			io.Reader
			io.Writer
		}{br, conn}
	}

	c := &Client{
		conn:   conn,
		rw:     rw,
		logger: cfg.Logger,
		onMsg:  cfg.OnMessage,
	}
	return c, nil
}

// Subscribe sends a subscribe request for the given channels.
func (c *Client) Subscribe(channels []string) error {
	return c.writeJSON(map[string]any{
		"type": "subscribe",
		"data": map[string]any{"channels": channels},
	})
}

// Unsubscribe sends an unsubscribe request for the given channels.
func (c *Client) Unsubscribe(channels []string) error {
	return c.writeJSON(map[string]any{
		"type": "unsubscribe",
		"data": map[string]any{"channels": channels},
	})
}

// Publish sends a message to the given channel.
func (c *Client) Publish(channel string, data json.RawMessage) error {
	return c.writeJSON(map[string]any{
		"type": "publish",
		"data": map[string]any{"channel": channel, "data": data},
	})
}

// History requests up to limit historical messages for a channel. Results
// arrive as plain "message" frames (there is no history_message type in the
// contract) terminated by history_complete, or history_error on refusal. The
// channel MUST already be subscribed (server guard).
func (c *Client) History(channel string, limit int) error {
	return c.writeJSON(map[string]any{
		"type": "history",
		"data": map[string]any{"channel": channel, "limit": limit},
	})
}

// Replay requests a live in-connection replay of a channel from an INCLUSIVE
// pos cursor (the anchor record itself replays too — unlike Reconnect's
// exclusive last_pos; deliberate server asymmetry, see handler_replay.go) — the
// "live gap recovery" leg. Replayed records arrive as replay_message frames
// terminated by a flat replay_complete{messages_replayed}. The channel MUST
// already be subscribed (server guard); rate-limited server-side (default 1 per
// 10s per channel); rejections arrive as a generic type:"error" frame.
func (c *Client) Replay(channel, fromPos string) error {
	return c.writeJSON(map[string]any{
		"type": "replay",
		"data": map[string]any{"channel": channel, "from_pos": fromPos},
	})
}

// Reconnect reopens the server's replay window after a disconnect: lastPos maps
// each subscribed channel to the last pos this client saw, and the server
// replays everything after those cursors (exclusive) as plain "message" frames
// before reconnect_ack. Stateless server-side — clientID is any client-chosen
// string, no session lookup. Channels MUST be re-subscribed BEFORE this call.
func (c *Client) Reconnect(clientID string, lastPos map[string]string) error {
	return c.writeJSON(map[string]any{
		"type": "reconnect",
		"data": map[string]any{"client_id": clientID, "last_pos": lastPos},
	})
}

// RefreshToken sends an auth refresh message to the gateway with a new JWT.
// The gateway responds asynchronously via auth_ack or auth_error messages
// which are dispatched through the ReadLoop's onMsg callback.
func (c *Client) RefreshToken(token string) error {
	return c.writeJSON(map[string]any{
		"type": "auth",
		"data": map[string]any{"token": token},
	})
}

// ReadLoop reads messages until the context is canceled or the connection closes.
// Returns the WebSocket close code if the server sent a close frame, or 0 otherwise.
func (c *Client) ReadLoop(ctx context.Context) (ws.StatusCode, error) {
	// When the context is canceled, set a short deadline to unblock any
	// in-progress read. This avoids polling with read deadlines (which can
	// corrupt WebSocket framing if a timeout hits mid-frame).
	stop := context.AfterFunc(ctx, func() {
		if tc, ok := c.conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = tc.SetReadDeadline(time.Now().Add(shutdownReadDeadline))
		}
	})
	defer stop()

	for {
		data, err := wsutil.ReadServerText(c.rw)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil
			}
			if closeErr, ok := errors.AsType[wsutil.ClosedError](err); ok {
				return closeErr.Code, nil
			}
			c.logger.Debug().Err(err).Msg("read error")
			return 0, fmt.Errorf("read loop: %w", err)
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Debug().Err(err).Msg("unmarshal error")
			continue
		}

		if c.onMsg != nil {
			c.onMsg(msg)
		}
	}
}

// Close closes the underlying WebSocket connection. Safe to call multiple times.
func (c *Client) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				closeErr = fmt.Errorf("close websocket: %w", err)
			}
		}
	})
	return closeErr
}

func (c *Client) writeJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	// Lock held across I/O: gobwas/ws requires write serialization on a single
	// net.Conn. Acceptable for tester client where concurrent writes are infrequent.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("connection closed")
	}
	if err := wsutil.WriteClientText(c.conn, data); err != nil {
		return fmt.Errorf("write ws message: %w", err)
	}
	return nil
}
