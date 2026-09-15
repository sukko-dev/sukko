package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// ClientPublisher wraps a ws.Client for per-user authorized publishing.
// Used by PubSubEngine for authorization + delivery testing — the gateway
// checks the connection's JWT claims before forwarding the publish.
// Close is a no-op because the caller owns the client lifecycle.
type ClientPublisher struct {
	client *testerws.Client
}

// NewClientPublisher wraps an existing WebSocket client as a Publisher.
func NewClientPublisher(client *testerws.Client) *ClientPublisher {
	return &ClientPublisher{client: client}
}

// Publish sends a message to the channel via the user's WebSocket connection.
func (p *ClientPublisher) Publish(_ context.Context, channel string, payload []byte) error {
	if err := p.client.Publish(channel, json.RawMessage(payload)); err != nil {
		return fmt.Errorf("client publish to %s: %w", channel, err)
	}
	return nil
}

// Close is a no-op — the caller owns the ws.Client lifecycle.
func (p *ClientPublisher) Close() error { return nil }

// Ensure ClientPublisher implements Publisher.
var _ Publisher = (*ClientPublisher)(nil)
