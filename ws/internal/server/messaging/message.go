// Package messaging provides message envelope structures, sequence tracking,
// and priority handling for WebSocket messages. It implements the industry-standard
// envelope pattern used by financial trading platforms for reliable message delivery.
package messaging

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// MessagePriority defines delivery guarantees for different message types
// Trading platforms need different strategies for different data:
// - CRITICAL: Orders, fills, account updates - MUST deliver or disconnect client
// - HIGH: Price updates for watched assets - deliver or disconnect if consistently failing
// - NORMAL: General market data - can drop if client slow (prevents blocking)
//
// Industry Standard (Coinbase, Binance, Bloomberg):
// - Order updates: CRITICAL (legal requirement to deliver)
// - Price ticks: HIGH (incorrect prices = bad trades)
// - Market stats: NORMAL (nice-to-have, not critical)
type MessagePriority int

// MessagePriority constants for delivery guarantees.
const (
	PriorityCritical MessagePriority = iota // Never drop - disconnect if can't deliver
	PriorityHigh                            // Drop only as last resort
	PriorityNormal                          // Drop if client slow (prevents head-of-line blocking)
)

// MessageEnvelope wraps all WebSocket messages with delivery metadata
// This implements the industry-standard message envelope pattern used by:
// - Financial markets (FIX protocol)
// - Trading platforms (Coinbase, Kraken)
// - Real-time systems (NASDAQ, Bloomberg Terminal)
//
// Purpose:
// 1. Sequence tracking - clients detect missing messages (gap detection)
// 2. Message replay - clients can request missed messages by sequence range
// 3. Latency measurement - timestamp allows client to calculate server→client delay
// 4. Priority-based delivery - critical messages never dropped
// 5. Channel-based routing - client knows which channel the message is from
//
// Example client-side gap detection:
//
//	lastSeq = 100
//	receive seq 102
//	detect gap: 101 missing
//	request replay: {"type": "replay", "data": {"from": 101, "to": 101}}
type MessageEnvelope struct {
	// Type identifies the message type for client routing
	// Broadcast messages always have Type: "message"
	// This aligns with industry standards (Coinbase, Binance, Pusher)
	// where all messages include a type field for consistent parsing.
	//
	// Client code pattern:
	//   if (msg.type === "message") {
	//     // Broadcast data - route by msg.channel
	//   } else {
	//     // Control message - handle by msg.type
	//   }
	Type string `json:"type"`

	// Sequence number - monotonically increasing per connection
	// Starts at 1 on connection, increments for each message sent
	// Client uses this for gap detection:
	//   if (receivedSeq != expectedSeq + 1) { requestReplay(expectedSeq, receivedSeq) }
	Seq int64 `json:"seq"`

	// Server timestamp in Unix milliseconds
	// Uses:
	// 1. Client latency calculation: clientTime - serverTime = network latency
	// 2. Message ordering when replaying from buffer
	// 3. Debugging out-of-order delivery issues
	// 4. Regulatory compliance (prove when message was sent)
	Timestamp int64 `json:"ts"`

	// Channel identifies which subscription channel this message is from
	// This allows clients to route messages to the appropriate handler
	// Examples:
	//   "sukko.BTC.trade"     - Trade events for BTC
	//   "sukko.all.trade"     - All trade events (aggregate)
	//   "sukko.ETH.liquidity" - Liquidity events for ETH
	//   "sukko.BTC.balances.user123" - User-scoped balance updates
	//
	// Client code pattern:
	//   switch (message.channel) {
	//     case "sukko.BTC.trade": handleBTCTrade(message.data)
	//     case "sukko.all.trade": handleAllTrades(message.data)
	//   }
	Channel string `json:"channel"`

	// Priority level - determines delivery strategy
	// Hidden from clients (json:"-"), used internally by server
	// Server uses this in broadcast() to decide:
	// - CRITICAL: Block up to 1 second, then disconnect slow client
	// - HIGH: Block up to 100ms, then disconnect slow client
	// - NORMAL: Never block, drop message if client buffer full
	Priority MessagePriority `json:"-"`

	// Actual message payload (varies by channel)
	// Stored as json.RawMessage to avoid double-encoding
	// Server doesn't need to parse Kafka messages, just wrap and forward
	//
	// Example for "sukko.BTC.trade" channel:
	//   {"token": "BTC", "price": 45000, "amount_btc": 1500000000}
	//
	// Example for "sukko.ETH.liquidity" channel:
	//   {"token": "ETH", "poolId": "abc123", "liquidity": "1000000"}
	Data json.RawMessage `json:"data"`

	// Pos is the encoded Kafka position "(partition+1)-offset" for this message.
	// Clients use this as a durable replay cursor on reconnect.
	// Omitted when empty (omitempty) — not set for backends without pos support.
	Pos string `json:"pos,omitempty"`

	// Mid is the stable, globally unique message identity — identical on the
	// live, replayed, and history-stored copies of the same message (unlike
	// Seq, which is per-connection). Opaque to clients (≤64 chars); derived
	// from Kafka record coordinates on the kafka backend and minted as a UUID
	// on the direct backend. Identity, never a cursor — replay positioning
	// stays on Pos. Empty only for messages predating the field (omitempty).
	Mid string `json:"mid,omitempty"`
}

// SequenceGenerator creates unique, monotonically increasing sequence numbers
// Each WebSocket connection gets its own generator (sequences start at 1)
//
// Thread-safe implementation using atomic operations:
// - Multiple goroutines can call Next() concurrently
// - No mutex needed (atomic.Int64.Add is lock-free)
// - Faster than mutex-based counter under high concurrency
//
// Industry standard: Every message delivery system needs sequence numbers
// - FIX protocol: BeginSeqNo, EndSeqNo fields
// - Kafka: Partition offsets and message sequence numbers
// - WebSocket implementations: Per-connection sequences
// - Our implementation: Per-connection sequences for gap detection
type SequenceGenerator struct {
	counter atomic.Int64
}

// NewSequenceGenerator creates a sequence generator starting at 0
// First call to Next() will return 1
func NewSequenceGenerator() *SequenceGenerator {
	return &SequenceGenerator{}
}

// Next returns the next sequence number in the sequence
// Thread-safe using atomic operations
// Sequence numbers start at 1 and increment forever (no wraparound)
//
// Performance: ~10ns per call on modern CPUs (100M sequences/second)
// Memory: 8 bytes per generator (negligible)
func (s *SequenceGenerator) Next() int64 {
	return s.counter.Add(1)
}

// Current returns the current sequence number without incrementing
// Thread-safe using atomic operations
func (s *SequenceGenerator) Current() int64 {
	return s.counter.Load()
}

// Reset sets the sequence counter back to 0
// Thread-safe using atomic operations
func (s *SequenceGenerator) Reset() {
	s.counter.Store(0)
}

// WrapMessage creates an envelope for raw Kafka messages
// This is called in broadcast() before sending to clients
//
// Parameters:
//
//	data     - Raw message payload from Kafka (JSON bytes)
//	channel  - Channel this message is from ("sukko.BTC.trade", "sukko.all.trade", etc.)
//	priority - Delivery priority (CRITICAL, HIGH, NORMAL)
//	seqGen   - Per-client sequence generator
//
// Returns:
//
//	Envelope ready to serialize and send to WebSocket client
//
// Example usage:
//
//	envelope, _ := WrapMessage(kafkaData, "sukko.BTC.trade", PRIORITY_HIGH, client.seqGen)
//	jsonBytes, _ := envelope.Serialize()
//	client.send <- jsonBytes
func WrapMessage(data []byte, channel string, priority MessagePriority, seqGen *SequenceGenerator) (*MessageEnvelope, error) {
	return &MessageEnvelope{
		Type:      "message", // Standard type for broadcast messages
		Seq:       seqGen.Next(),
		Timestamp: time.Now().UnixMilli(),
		Channel:   channel,
		Priority:  priority,
		Data:      json.RawMessage(data),
	}, nil
}

// Serialize converts envelope to JSON bytes for WebSocket transmission
// This is the final step before sending over the wire
//
// Returns JSON in format:
//
//	{"type":"message","seq":1,"ts":1234567890,"channel":"sukko.BTC.trade","data":{...}}
//
// Error handling:
// - Should never fail in production (envelope fields are always valid JSON)
// - If it does fail, indicates serious bug (corrupt data structure)
func (m *MessageEnvelope) Serialize() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("serialize message envelope: %w", err)
	}
	return data, nil
}
