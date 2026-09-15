// Package publisher provides message publishing backends for the tester service.
// The Publisher interface abstracts direct WebSocket and Kafka publishing
// so the same test logic works against any backend.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// Publisher abstracts message publishing across backends.
// Implementations: DirectPublisher (WebSocket), ClientPublisher (ws.Client wrapper),
// KafkaPublisher (franz-go).
type Publisher interface {
	// Publish sends a message to the given channel.
	// The payload is raw JSON (caller generates it via Generator).
	Publish(ctx context.Context, channel string, payload []byte) error

	// Close releases resources. Safe to call multiple times.
	Close() error
}

// gatewayWSPath is the WebSocket endpoint path on the gateway.
const gatewayWSPath = "/ws"

// DirectPublisher publishes via a dedicated WebSocket connection to the gateway.
// Used for load/soak throughput where a separate connection for publishing is needed.
type DirectPublisher struct {
	conn      net.Conn
	mu        sync.Mutex // protects write serialization (gobwas/ws requirement)
	closeOnce sync.Once
}

// NewDirectPublisher connects to the gateway and returns a DirectPublisher.
func NewDirectPublisher(ctx context.Context, gatewayURL, token string) (*DirectPublisher, error) {
	wsURL := gatewayURL + gatewayWSPath
	header := http.Header{}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}

	dialer := ws.Dialer{Header: ws.HandshakeHeaderHTTP(header)}
	conn, _, _, err := dialer.Dial(ctx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("dial gateway for publishing: %w", err)
	}

	return &DirectPublisher{conn: conn}, nil
}

// Publish sends a message to the given channel via WebSocket.
func (p *DirectPublisher) Publish(_ context.Context, channel string, payload []byte) error {
	msg := map[string]any{
		"type": "publish",
		"data": map[string]any{
			"channel": channel,
			"data":    json.RawMessage(payload),
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// Lock held across I/O: gobwas/ws requires write serialization on a single
	// connection. Acceptable for a test tool with a single publisher goroutine.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return errors.New("publisher connection closed")
	}
	if err := wsutil.WriteClientText(p.conn, data); err != nil {
		return fmt.Errorf("write ws message: %w", err)
	}
	return nil
}

// Close closes the WebSocket connection. Safe to call multiple times.
func (p *DirectPublisher) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.conn != nil {
			if err := p.conn.Close(); err != nil {
				closeErr = fmt.Errorf("close publisher: %w", err)
			}
			p.conn = nil
		}
	})
	return closeErr
}

// Ensure DirectPublisher implements Publisher.
var _ Publisher = (*DirectPublisher)(nil)
