package publisher

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// Generator produces sequenced test messages.
type Generator struct {
	sequence atomic.Int64
}

// NewGenerator creates a Generator starting at sequence zero.
func NewGenerator() *Generator {
	return &Generator{}
}

// TestMessage is the payload structure for generated test messages.
type TestMessage struct {
	Sequence  int64  `json:"seq"`
	Timestamp int64  `json:"ts"`
	Payload   string `json:"payload"`
}

// Next returns the next sequenced test message as JSON.
func (g *Generator) Next(channel string) (json.RawMessage, error) {
	seq := g.sequence.Add(1)
	msg := TestMessage{
		Sequence:  seq,
		Timestamp: time.Now().UnixMicro(),
		Payload:   fmt.Sprintf("test-message-%d", seq),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal test message: %w", err)
	}
	return data, nil
}

// Reset sets the sequence counter back to zero.
func (g *Generator) Reset() {
	g.sequence.Store(0)
}
