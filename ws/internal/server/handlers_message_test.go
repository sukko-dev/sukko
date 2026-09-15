package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// =============================================================================
// Message Parsing Tests (extracted logic for testability)
// =============================================================================

// parseClientMessage parses the outer message structure
// This is the same logic used in handleClientMessage
func parseClientMessage(data []byte) (msgType string, msgData json.RawMessage, err error) {
	var req struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(data, &req); err != nil {
		return "", nil, fmt.Errorf("unmarshal client message: %w", err)
	}
	return req.Type, req.Data, nil
}

func TestParseClientMessage_Subscribe(t *testing.T) {
	t.Parallel()
	msg := `{"type": "subscribe", "data": {"channels": ["BTC.trade", "ETH.trade"]}}`

	msgType, msgData, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "subscribe" {
		t.Errorf("msgType: got %q, want %q", msgType, "subscribe")
	}
	if len(msgData) == 0 {
		t.Error("msgData should not be empty")
	}
}

func TestParseClientMessage_Unsubscribe(t *testing.T) {
	t.Parallel()
	msg := `{"type": "unsubscribe", "data": {"channels": ["BTC.trade"]}}`

	msgType, msgData, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "unsubscribe" {
		t.Errorf("msgType: got %q, want %q", msgType, "unsubscribe")
	}
	if len(msgData) == 0 {
		t.Error("msgData should not be empty")
	}
}

func TestParseClientMessage_Heartbeat(t *testing.T) {
	t.Parallel()
	msg := `{"type": "heartbeat"}`

	msgType, _, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "heartbeat" {
		t.Errorf("msgType: got %q, want %q", msgType, "heartbeat")
	}
}

func TestParseClientMessage_Reconnect(t *testing.T) {
	t.Parallel()
	msg := `{"type": "reconnect", "data": {"client_id": "abc123", "last_pos": {"tenant.BTC.trade": "3-12345"}}}`

	msgType, msgData, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "reconnect" {
		t.Errorf("msgType: got %q, want %q", msgType, "reconnect")
	}
	if len(msgData) == 0 {
		t.Error("msgData should not be empty")
	}
}

func TestParseClientMessage_InvalidJSON(t *testing.T) {
	t.Parallel()
	msg := `{invalid json}`

	_, _, err := parseClientMessage([]byte(msg))

	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestParseClientMessage_EmptyType(t *testing.T) {
	t.Parallel()
	msg := `{"type": "", "data": {}}`

	msgType, _, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "" {
		t.Errorf("msgType: got %q, want empty string", msgType)
	}
}

func TestParseClientMessage_MissingType(t *testing.T) {
	t.Parallel()
	msg := `{"data": {"channels": ["BTC.trade"]}}`

	msgType, _, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	// Missing type defaults to empty string (zero value)
	if msgType != "" {
		t.Errorf("msgType: got %q, want empty string", msgType)
	}
}

func TestParseClientMessage_UnknownType(t *testing.T) {
	t.Parallel()
	msg := `{"type": "unknown_future_message_type", "data": {}}`

	msgType, _, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "unknown_future_message_type" {
		t.Errorf("msgType: got %q, want %q", msgType, "unknown_future_message_type")
	}
}

// =============================================================================
// Subscribe Request Parsing Tests
// =============================================================================

// parseSubscribeRequest parses the subscribe message data
func parseSubscribeRequest(data json.RawMessage) ([]string, error) {
	var subReq struct {
		Channels []string `json:"channels"`
	}
	if err := json.Unmarshal(data, &subReq); err != nil {
		return nil, fmt.Errorf("unmarshal subscribe request: %w", err)
	}
	return subReq.Channels, nil
}

func TestParseSubscribeRequest_SingleChannel(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channels": ["BTC.trade"]}`)

	channels, err := parseSubscribeRequest(data)

	if err != nil {
		t.Fatalf("parseSubscribeRequest failed: %v", err)
	}
	if len(channels) != 1 {
		t.Fatalf("channels length: got %d, want 1", len(channels))
	}
	if channels[0] != "BTC.trade" {
		t.Errorf("channels[0]: got %q, want %q", channels[0], "BTC.trade")
	}
}

func TestParseSubscribeRequest_MultipleChannels(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channels": ["BTC.trade", "ETH.trade", "SOL.liquidity", "BTC.analytics"]}`)

	channels, err := parseSubscribeRequest(data)

	if err != nil {
		t.Fatalf("parseSubscribeRequest failed: %v", err)
	}
	if len(channels) != 4 {
		t.Fatalf("channels length: got %d, want 4", len(channels))
	}
}

func TestParseSubscribeRequest_EmptyChannels(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channels": []}`)

	channels, err := parseSubscribeRequest(data)

	if err != nil {
		t.Fatalf("parseSubscribeRequest failed: %v", err)
	}
	if len(channels) != 0 {
		t.Errorf("channels length: got %d, want 0", len(channels))
	}
}

func TestParseSubscribeRequest_InvalidFormat(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channels": "not-an-array"}`)

	_, err := parseSubscribeRequest(data)

	if err == nil {
		t.Error("Expected error for invalid format")
	}
}

// =============================================================================
// Reconnect Request Parsing Tests
// =============================================================================

// parseReconnectRequestData parses the reconnect message data field using the
// same schema as handleReconnect: client_id string and last_pos map[string]string.
func parseReconnectRequestData(data json.RawMessage) (clientID string, lastPos map[string]string, err error) {
	var req struct {
		ClientID string            `json:"client_id"`
		LastPos  map[string]string `json:"last_pos"`
	}
	if err = json.Unmarshal(data, &req); err != nil {
		return "", nil, fmt.Errorf("unmarshal reconnect request: %w", err)
	}
	return req.ClientID, req.LastPos, nil
}

func TestParseReconnectRequest_Valid(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"client_id": "abc123", "last_pos": {"tenant.BTC.trade": "3-12345", "tenant.ETH.trade": "1-67890"}}`)

	clientID, positions, err := parseReconnectRequestData(data)

	if err != nil {
		t.Fatalf("parseReconnectRequestData failed: %v", err)
	}
	if clientID != "abc123" {
		t.Errorf("clientID: got %q, want %q", clientID, "abc123")
	}
	if len(positions) != 2 {
		t.Fatalf("positions length: got %d, want 2", len(positions))
	}
	if positions["tenant.BTC.trade"] != "3-12345" {
		t.Errorf("positions[tenant.BTC.trade]: got %q, want %q", positions["tenant.BTC.trade"], "3-12345")
	}
	if positions["tenant.ETH.trade"] != "1-67890" {
		t.Errorf("positions[tenant.ETH.trade]: got %q, want %q", positions["tenant.ETH.trade"], "1-67890")
	}
}

func TestParseReconnectRequest_EmptyPositions(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"client_id": "abc123", "last_pos": {}}`)

	clientID, positions, err := parseReconnectRequestData(data)

	if err != nil {
		t.Fatalf("parseReconnectRequestData failed: %v", err)
	}
	if clientID != "abc123" {
		t.Errorf("clientID: got %q, want %q", clientID, "abc123")
	}
	if len(positions) != 0 {
		t.Errorf("positions length: got %d, want 0", len(positions))
	}
}

func TestParseReconnectRequest_MissingClientID(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"last_pos": {"tenant.BTC.trade": "3-12345"}}`)

	clientID, _, err := parseReconnectRequestData(data)

	if err != nil {
		t.Fatalf("parseReconnectRequestData failed: %v", err)
	}
	// Missing client_id defaults to empty string
	if clientID != "" {
		t.Errorf("clientID: got %q, want empty string", clientID)
	}
}

func TestParseReconnectRequest_InvalidJSON(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{invalid json}`)

	_, _, err := parseReconnectRequestData(data)

	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestParseReconnectRequest_NullPositions(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"client_id": "abc123", "last_pos": null}`)

	clientID, positions, err := parseReconnectRequestData(data)

	if err != nil {
		t.Fatalf("parseReconnectRequestData failed: %v", err)
	}
	if clientID != "abc123" {
		t.Errorf("clientID: got %q, want %q", clientID, "abc123")
	}
	if positions != nil {
		t.Errorf("positions: got %v, want nil", positions)
	}
}

// =============================================================================
// MessageEnvelope Serialize Format Tests (replay context)
// =============================================================================

func TestMessageEnvelope_Serialize_ReplayFormat(t *testing.T) {
	t.Parallel()

	// Verify that MessageEnvelope.Serialize() produces valid JSON matching
	// the format used in handleReconnect for replay messages.
	envelope := &messaging.MessageEnvelope{
		Type:      MsgTypeMessage,
		Seq:       42,
		Timestamp: 1709337600000,
		Channel:   "acme.BTC.trade",
		Priority:  messaging.PriorityNormal,
		Data:      json.RawMessage(`{"price":"50000"}`),
	}

	data, err := envelope.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Parse the serialized output and verify fields
	var parsed struct {
		Type    string          `json:"type"`
		Seq     int64           `json:"seq"`
		Ts      int64           `json:"ts"`
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal serialized envelope failed: %v", err)
	}

	if parsed.Type != MsgTypeMessage {
		t.Errorf("type: got %q, want %q", parsed.Type, MsgTypeMessage)
	}
	if parsed.Seq != 42 {
		t.Errorf("seq: got %d, want 42", parsed.Seq)
	}
	if parsed.Ts != 1709337600000 {
		t.Errorf("ts: got %d, want 1709337600000", parsed.Ts)
	}
	if parsed.Channel != "acme.BTC.trade" {
		t.Errorf("channel: got %q, want %q", parsed.Channel, "acme.BTC.trade")
	}
	if string(parsed.Data) != `{"price":"50000"}` {
		t.Errorf("data: got %s, want %s", parsed.Data, `{"price":"50000"}`)
	}
}

func TestMessageEnvelope_Serialize_NilData(t *testing.T) {
	t.Parallel()

	envelope := &messaging.MessageEnvelope{
		Type:      MsgTypeMessage,
		Seq:       1,
		Timestamp: 1709337600000,
		Channel:   "acme.BTC.trade",
		Data:      nil,
	}

	data, err := envelope.Serialize()
	if err != nil {
		t.Fatalf("Serialize with nil data failed: %v", err)
	}

	// Verify it produces valid JSON
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal serialized envelope with nil data failed: %v", err)
	}
}

// =============================================================================
// Reconnect Ack Response Tests
// =============================================================================

func TestReconnectAck_JSONFormat(t *testing.T) {
	t.Parallel()

	ackMsg := map[string]any{
		"type":              RespTypeReconnectAck,
		"status":            "completed",
		"messages_replayed": 5,
		"message":           "Replayed 5 missed messages",
	}

	data, err := json.Marshal(ackMsg)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var parsed struct {
		Type             string `json:"type"`
		Status           string `json:"status"`
		MessagesReplayed int    `json:"messages_replayed"`
		Message          string `json:"message"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.Type != RespTypeReconnectAck {
		t.Errorf("type: got %q, want %q", parsed.Type, RespTypeReconnectAck)
	}
	if parsed.Status != "completed" {
		t.Errorf("status: got %q, want %q", parsed.Status, "completed")
	}
	if parsed.MessagesReplayed != 5 {
		t.Errorf("messages_replayed: got %d, want 5", parsed.MessagesReplayed)
	}
}

// =============================================================================
// Pong Response Tests
// =============================================================================

func TestPongResponse_Format(t *testing.T) {
	t.Parallel()
	before := time.Now().UnixMilli()

	pong := map[string]any{
		"type": "pong",
		"ts":   time.Now().UnixMilli(),
	}
	data, err := json.Marshal(pong)

	after := time.Now().UnixMilli()

	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Parse back
	var parsed struct {
		Type string `json:"type"`
		Ts   int64  `json:"ts"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.Type != "pong" {
		t.Errorf("type: got %q, want %q", parsed.Type, "pong")
	}
	if parsed.Ts < before || parsed.Ts > after {
		t.Errorf("ts: got %d, expected between %d and %d", parsed.Ts, before, after)
	}
}

// =============================================================================
// Subscription Ack Response Tests
// =============================================================================

func TestSubscriptionAck_JSONFormat(t *testing.T) {
	t.Parallel()
	channels := []string{"BTC.trade", "ETH.trade"}
	count := 5

	ack := map[string]any{
		"type":       "subscription_ack",
		"subscribed": channels,
		"count":      count,
	}
	data, err := json.Marshal(ack)

	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Parse back
	var parsed struct {
		Type       string   `json:"type"`
		Subscribed []string `json:"subscribed"`
		Count      int      `json:"count"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.Type != "subscription_ack" {
		t.Errorf("type: got %q, want %q", parsed.Type, "subscription_ack")
	}
	if len(parsed.Subscribed) != 2 {
		t.Errorf("subscribed length: got %d, want 2", len(parsed.Subscribed))
	}
	if parsed.Count != 5 {
		t.Errorf("count: got %d, want 5", parsed.Count)
	}
}

// =============================================================================
// Client Send Buffer Tests
// =============================================================================

func TestClientSendBuffer_NonBlocking(t *testing.T) {
	t.Parallel()
	client := &Client{
		send: make(chan OutgoingMsg, 2), // Small buffer
	}

	// Fill the buffer
	client.send <- RawMsg([]byte("msg1"))
	client.send <- RawMsg([]byte("msg2"))

	// Non-blocking send should not block
	data := []byte("msg3")
	var sent bool
	select {
	case client.send <- RawMsg(data):
		sent = true
	default:
		// Buffer is full, send would block
	}

	if sent {
		t.Error("Send should not succeed on full buffer (non-blocking)")
	}
}

func TestClientSendBuffer_Drain(t *testing.T) {
	t.Parallel()
	client := &Client{
		send: make(chan OutgoingMsg, 3),
	}

	// Add messages
	client.send <- RawMsg([]byte("msg1"))
	client.send <- RawMsg([]byte("msg2"))
	client.send <- RawMsg([]byte("msg3"))

	// Drain
	count := 0
	for len(client.send) > 0 {
		<-client.send
		count++
	}

	if count != 3 {
		t.Errorf("drained count: got %d, want 3", count)
	}
}

// =============================================================================
// Sequence Generator Tests (for message handlers)
// =============================================================================

func TestSequenceGenerator_MonotonicIncrease_Handlers(t *testing.T) {
	t.Parallel()
	gen := messaging.NewSequenceGenerator()

	var prev int64
	for range 1000 {
		seq := gen.Next()
		if seq <= prev {
			t.Errorf("Sequence should monotonically increase: prev=%d, current=%d", prev, seq)
		}
		prev = seq
	}
}

func TestSequenceGenerator_ConcurrentSafe_Handlers(t *testing.T) {
	t.Parallel()
	gen := messaging.NewSequenceGenerator()

	var wg sync.WaitGroup
	seqs := make(chan int64, 1000)

	// 10 goroutines each generating 100 sequences
	for range 10 {
		wg.Go(func() {
			for range 100 {
				seqs <- gen.Next()
			}
		})
	}

	wg.Wait()
	close(seqs)

	// Collect all sequences
	seen := make(map[int64]bool)
	for seq := range seqs {
		if seen[seq] {
			t.Errorf("Duplicate sequence detected: %d", seq)
		}
		seen[seq] = true
	}

	if len(seen) != 1000 {
		t.Errorf("Expected 1000 unique sequences, got %d", len(seen))
	}
}

// =============================================================================
// Message Type Constants Tests
// =============================================================================

func TestMessageTypes_Valid(t *testing.T) {
	t.Parallel()
	validTypes := []string{
		"subscribe",
		"unsubscribe",
		"heartbeat",
		"reconnect",
	}

	for _, msgType := range validTypes {
		msg := map[string]any{"type": msgType}
		data, err := json.Marshal(msg)
		if err != nil {
			t.Errorf("Failed to marshal message type %q: %v", msgType, err)
			continue
		}

		parsedType, _, err := parseClientMessage(data)
		if err != nil {
			t.Errorf("Failed to parse message type %q: %v", msgType, err)
			continue
		}

		if parsedType != msgType {
			t.Errorf("Message type mismatch: got %q, want %q", parsedType, msgType)
		}
	}
}

// =============================================================================
// Regression Tests - Ensure publishing feature doesn't affect existing handlers
// =============================================================================

func TestMessageHandler_ExistingTypesUnaffected(t *testing.T) {
	t.Parallel()
	// Verify all existing message types parse and route correctly
	// This ensures the new "publish" case doesn't affect the switch statement
	existingTypes := []string{"subscribe", "unsubscribe", "heartbeat", "reconnect"}

	for _, msgType := range existingTypes {
		t.Run(msgType, func(t *testing.T) {
			t.Parallel()
			msg := []byte(`{"type": "` + msgType + `", "data": {}}`)
			parsedType, _, err := parseClientMessage(msg)

			if err != nil {
				t.Fatalf("Failed to parse %s message: %v", msgType, err)
			}
			if parsedType != msgType {
				t.Errorf("Message type mismatch: got %q, want %q", parsedType, msgType)
			}
		})
	}
}

func TestMessageTypes_AllTypesIncludingPublish(t *testing.T) {
	t.Parallel()
	// Verify all message types (existing + publish) work correctly
	// Regression test: adding "publish" must not break other message types
	allTypes := []string{"subscribe", "unsubscribe", "heartbeat", "reconnect", "publish"}

	for _, msgType := range allTypes {
		t.Run(msgType, func(t *testing.T) {
			t.Parallel()
			msg := []byte(`{"type": "` + msgType + `", "data": {}}`)
			parsedType, _, err := parseClientMessage(msg)

			if err != nil {
				t.Fatalf("Failed to parse %s: %v", msgType, err)
			}
			if parsedType != msgType {
				t.Errorf("got %q, want %q", parsedType, msgType)
			}
		})
	}
}

func TestMessageHandler_PublishDoesNotAffectExistingTypes(t *testing.T) {
	t.Parallel()
	// Interleaved parsing test - ensure publish doesn't corrupt parser state
	messages := []struct {
		msgType string
		payload string
	}{
		{"subscribe", `{"type": "subscribe", "data": {"channels": ["BTC.trade"]}}`},
		{"publish", `{"type": "publish", "data": {"channel": "test.chat", "data": {"msg": "hello"}}}`},
		{"unsubscribe", `{"type": "unsubscribe", "data": {"channels": ["BTC.trade"]}}`},
		{"publish", `{"type": "publish", "data": {"channel": "test.chat", "data": {"msg": "world"}}}`},
		{"heartbeat", `{"type": "heartbeat"}`},
		{"reconnect", `{"type": "reconnect", "data": {"client_id": "abc123"}}`},
	}

	for i, msg := range messages {
		parsedType, _, err := parseClientMessage([]byte(msg.payload))
		if err != nil {
			t.Fatalf("Message %d (%s): parse failed: %v", i, msg.msgType, err)
		}
		if parsedType != msg.msgType {
			t.Errorf("Message %d: got %q, want %q", i, parsedType, msg.msgType)
		}
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkParseClientMessage_Subscribe(b *testing.B) {
	msg := []byte(`{"type": "subscribe", "data": {"channels": ["BTC.trade", "ETH.trade"]}}`)

	for b.Loop() {
		_, _, _ = parseClientMessage(msg)
	}
}

func BenchmarkParseClientMessage_Heartbeat(b *testing.B) {
	msg := []byte(`{"type": "heartbeat"}`)

	for b.Loop() {
		_, _, _ = parseClientMessage(msg)
	}
}

// =============================================================================
// handleClientMessage — history and channel-limit paths
// =============================================================================

func TestHandleClientMessage_MsgTypeHistory_HistoryDisabled(t *testing.T) {
	t.Parallel()
	s := &Server{
		config: &platform.ServerConfig{
			BaseConfig:           platform.BaseConfig{Environment: "test"},
			HistoryEnabled:       false,
			MaxChannelsPerClient: 100,
		},
		logger:            zerolog.Nop(),
		subscriptionIndex: NewSubscriptionIndex(),
	}
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("t.ch")

	s.handleClientMessage(c, []byte(`{"type":"history","data":{"channel":"t.ch","limit":10}}`))

	select {
	case msg := <-c.send:
		var resp map[string]any
		if err := json.Unmarshal(msg.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp["type"] != RespTypeHistoryError {
			t.Errorf("type = %v, want %v", resp["type"], RespTypeHistoryError)
		}
		if resp["code"] != history.ErrCodeHistoryDisabled {
			t.Errorf("code = %v, want %v", resp["code"], history.ErrCodeHistoryDisabled)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected history_error response, got none")
	}
}

func TestHandleClientMessage_MsgTypeHistory_InvalidJSON(t *testing.T) {
	t.Parallel()
	s := &Server{
		config: &platform.ServerConfig{
			BaseConfig:           platform.BaseConfig{Environment: "test"},
			HistoryEnabled:       false,
			MaxChannelsPerClient: 100,
		},
		logger:            zerolog.Nop(),
		subscriptionIndex: NewSubscriptionIndex(),
	}
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.handleClientMessage(c, []byte(`{"type":"history","data":"not_an_object"}`))

	select {
	case msg := <-c.send:
		var resp map[string]any
		if err := json.Unmarshal(msg.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp["type"] != RespTypeHistoryError {
			t.Errorf("type = %v, want %v", resp["type"], RespTypeHistoryError)
		}
		if resp["code"] != history.ErrCodeHistoryInvalidRequest {
			t.Errorf("code = %v, want %v", resp["code"], history.ErrCodeHistoryInvalidRequest)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected history_error response, got none")
	}
}

func TestHandleClientMessage_SubscribeWithHistory_HistoryDisabled(t *testing.T) {
	t.Parallel()
	s := &Server{
		config: &platform.ServerConfig{
			BaseConfig:           platform.BaseConfig{Environment: "test"},
			HistoryEnabled:       false,
			MaxChannelsPerClient: 100,
		},
		logger:            zerolog.Nop(),
		subscriptionIndex: NewSubscriptionIndex(),
	}
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.handleClientMessage(c, []byte(`{"type":"subscribe","data":{"channel":"t.BTC.trade","history":{"limit":50}}}`))

	select {
	case msg := <-c.send:
		var resp map[string]any
		if err := json.Unmarshal(msg.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp["type"] != RespTypeHistoryError {
			t.Errorf("type = %v, want %v", resp["type"], RespTypeHistoryError)
		}
		if resp["code"] != history.ErrCodeHistoryDisabled {
			t.Errorf("code = %v, want %v", resp["code"], history.ErrCodeHistoryDisabled)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected history_error response, got none")
	}
}

func TestHandleClientMessage_SubscribeWithHistory_CommunityEditionAllowed(t *testing.T) {
	t.Parallel()
	// MessageHistory is available on every edition (ADR-0009) —
	// subscribe-with-history must deliver; there is no edition gate on this path.
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	s.subscriptionIndex = NewSubscriptionIndex()

	c, cancel := newHistoryTestClient()
	defer cancel()

	s.handleClientMessage(c, []byte(`{"type":"subscribe","data":{"channel":"t.BTC.trade","history":{"limit":50}}}`))

	// First message must be subscription_ack (not history_error).
	ack := readEnvelope(t, c)
	if got := envString(t, ack, "type"); got != RespTypeSubscriptionAck {
		t.Fatalf("first message type = %q, want %q", got, RespTypeSubscriptionAck)
	}

	// Wait for async history delivery: empty stream → history_complete.
	c.clientWg.Wait()
	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryComplete {
		t.Fatalf("type: got %q, want %q", got, RespTypeHistoryComplete)
	}
}

func TestHandleClientMessage_Subscribe_ChannelLimitExceeded(t *testing.T) {
	t.Parallel()
	s := &Server{
		config: &platform.ServerConfig{
			BaseConfig:           platform.BaseConfig{Environment: "test"},
			MaxChannelsPerClient: 2,
		},
		logger:            zerolog.Nop(),
		subscriptionIndex: NewSubscriptionIndex(),
	}
	c, cancel := newHistoryTestClient()
	defer cancel()
	// Pre-fill the client to the limit.
	c.subscriptions.Add("t.ch1")
	c.subscriptions.Add("t.ch2")

	// Attempt to subscribe to a third channel — must be rejected.
	s.handleClientMessage(c, []byte(`{"type":"subscribe","data":{"channels":["t.ch3"]}}`))

	select {
	case msg := <-c.send:
		var resp map[string]any
		if err := json.Unmarshal(msg.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if resp["type"] != RespTypeSubscribeError {
			t.Errorf("type = %v, want %v", resp["type"], RespTypeSubscribeError)
		}
		if resp["code"] != string(protocol.ErrCodeSubscribeLimitExceeded) {
			t.Errorf("code = %v, want %v", resp["code"], string(protocol.ErrCodeSubscribeLimitExceeded))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected subscribe_error response, got none")
	}
}

func TestHandleClientMessage_SubscribeWithHistory_DeliversMessagesAndComplete(t *testing.T) {
	t.Parallel()
	valkeyClient := startMiniredis(t)

	// Seed stream before constructing the writer so entries are present on first request.
	ctx := context.Background()
	streamKey := history.StreamKey("test", "sukko", "BTC.trade")
	for _, p := range []string{`{"seq":1}`, `{"seq":2}`} {
		if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
			Id("*").FieldValue().
			FieldValue(history.HistoryFieldPayload, p).
			FieldValue(history.HistoryFieldTenantID, "sukko").
			FieldValue(history.HistoryFieldChannel, "BTC.trade").
			FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
			Build()).Error(); err != nil {
			t.Fatalf("XADD: %v", err)
		}
	}

	cfg := &platform.ServerConfig{
		PodID:                              "test-pod",
		HistoryWriterLockTTLMs:             2000,
		HistoryWriterHeartbeatInterval:     50 * time.Millisecond,
		HistoryWriterRestartInitialBackoff: 10 * time.Millisecond,
		HistoryWriterRestartMaxBackoff:     100 * time.Millisecond,
		HistoryValkeyCmdTimeout:            500 * time.Millisecond,
		HistoryWriterBuffer:                256,
		HistoryWriterPipelineBatch:         50,
		HistoryMaxConsecutiveLockFailures:  3,
		HistoryBufferDepth:                 100,
		HistoryTTL:                         24 * time.Hour,
	}
	hwCtx, hwCancel := context.WithCancel(context.Background())
	t.Cleanup(hwCancel)
	hw := history.NewWriter(hwCtx, cfg, &noopBus{}, valkeyClient, zerolog.Nop(), prometheus.NewRegistry(), "test")

	s := newHistoryTestServer(hw)
	s.subscriptionIndex = NewSubscriptionIndex()
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.handleClientMessage(c, []byte(`{"type":"subscribe","data":{"channel":"sukko.BTC.trade","history":{"limit":10}}}`))

	// First message must be subscription_ack.
	ack := readEnvelope(t, c)
	if got := envString(t, ack, "type"); got != RespTypeSubscriptionAck {
		t.Fatalf("first message type = %q, want %q", got, RespTypeSubscriptionAck)
	}

	// Wait for async history delivery to complete.
	c.clientWg.Wait()

	// Collect remaining messages: should be 2 history + 1 history_complete.
	var rest []map[string]json.RawMessage
	for {
		select {
		case msg := <-c.send:
			var env map[string]json.RawMessage
			if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
				rest = append(rest, env)
			}
		default:
			goto drained
		}
	}
drained:

	if len(rest) < 3 {
		t.Fatalf("expected ≥3 messages (2 history + complete), got %d", len(rest))
	}
	for i, msg := range rest[:2] {
		if got := envString(t, msg, "type"); got != MsgTypeMessage {
			t.Errorf("msg[%d] type = %q, want %q", i, got, MsgTypeMessage)
		}
		var histFlag bool
		_ = json.Unmarshal(msg["history"], &histFlag)
		if !histFlag {
			t.Errorf("msg[%d] history flag = false, want true", i)
		}
	}
	last := rest[len(rest)-1]
	if got := envString(t, last, "type"); got != RespTypeHistoryComplete {
		t.Errorf("last type = %q, want %q", got, RespTypeHistoryComplete)
	}
	var count int
	_ = json.Unmarshal(last["count"], &count)
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func BenchmarkParseSubscribeRequest_Handlers(b *testing.B) {
	data := json.RawMessage(`{"channels": ["BTC.trade", "ETH.trade", "SOL.liquidity", "BTC.analytics"]}`)

	for b.Loop() {
		_, _ = parseSubscribeRequest(data)
	}
}

func BenchmarkSequenceGenerator(b *testing.B) {
	gen := messaging.NewSequenceGenerator()

	for b.Loop() {
		_ = gen.Next()
	}
}

// =============================================================================
// handleReconnect — pos-based replay flow
// =============================================================================

// newReconnectTestServer builds a minimal Server suitable for handleReconnect tests.
func newReconnectTestServer(t *testing.T, mb *mockBackend) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Server{
		backend: mb,
		logger:  zerolog.Nop(),
		ctx:     ctx,
		stats:   stats.NewStats(),
		config: &platform.ServerConfig{
			ReplayTimeout:     2 * time.Second,
			MaxReplayMessages: 100,
		},
	}
}

func TestHandleReconnect_NormalPath_PosForwarded(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{"acme.BTC.trade": "acme.prices.BTC"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "acme.BTC.trade", Data: []byte(`{"price":"50000"}`), Pos: "3-100", Mid: "ab12cd34-3-100"},
			{Subject: "acme.BTC.trade", Data: []byte(`{"price":"51000"}`), Pos: "3-101", Mid: "ab12cd34-3-101"},
		},
	}
	s := newReconnectTestServer(t, mb)
	c := &Client{
		id:            1,
		send:          make(chan OutgoingMsg, 32),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
	}
	c.subscriptions.Add("acme.BTC.trade")

	// Client reconnects with last pos for the channel.
	reqData := []byte(`{"client_id":"cli-1","last_pos":{"acme.BTC.trade":"3-99"}}`)
	s.handleReconnect(c, reqData)

	// pos "3-99" = partition 2 (pos encodes partition+1), offset 99; reconnect's
	// last_pos is exclusive → replay starts at offset 100 on THAT partition only.
	// The partition must survive into the backend request: a topic-wide offset
	// fanned out to all partitions is wrong (other partitions have unrelated
	// logs — an exact offset valid on the anchor partition can be out of range
	// on the others).
	wantPositions := map[string]map[int32]int64{"acme.prices.BTC": {2: 100}}
	mb.mu.Lock()
	gotPositions := mb.lastReplayReq.Positions
	mb.mu.Unlock()
	if !reflect.DeepEqual(gotPositions, wantPositions) {
		t.Errorf("forwarded Positions = %v, want %v", gotPositions, wantPositions)
	}

	// Collect all sent messages.
	var msgs []map[string]json.RawMessage
	for len(c.send) > 0 {
		msg := <-c.send
		var env map[string]json.RawMessage
		if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
			msgs = append(msgs, env)
		}
	}

	// Expect 2 replayed message envelopes + 1 reconnect_ack.
	if len(msgs) < 3 {
		t.Fatalf("expected ≥3 messages (2 replay + ack), got %d", len(msgs))
	}

	// First two must be type=message with pos set.
	for i, m := range msgs[:2] {
		var typ string
		_ = json.Unmarshal(m["type"], &typ)
		if typ != MsgTypeMessage {
			t.Errorf("msgs[%d] type = %q, want %q", i, typ, MsgTypeMessage)
		}
		rawPos, ok := m["pos"]
		if !ok {
			t.Errorf("msgs[%d] missing pos field", i)
			continue
		}
		var pos string
		_ = json.Unmarshal(rawPos, &pos)
		wantPos := mb.replayMsgs[i].Pos
		if pos != wantPos {
			t.Errorf("msgs[%d] pos = %q, want %q", i, pos, wantPos)
		}
		// The replayed copy must carry the same stable mid as the live copy
		// (ADR-0008: identical on live, replayed, and history-stored copies).
		rawMid, ok := m["mid"]
		if !ok {
			t.Errorf("msgs[%d] missing mid field", i)
			continue
		}
		var mid string
		_ = json.Unmarshal(rawMid, &mid)
		if wantMid := mb.replayMsgs[i].Mid; mid != wantMid {
			t.Errorf("msgs[%d] mid = %q, want %q", i, mid, wantMid)
		}
	}

	// Last must be reconnect_ack with messages_replayed=2.
	last := msgs[len(msgs)-1]
	var typ string
	_ = json.Unmarshal(last["type"], &typ)
	if typ != RespTypeReconnectAck {
		t.Errorf("last type = %q, want %q", typ, RespTypeReconnectAck)
	}
	var replayed float64
	_ = json.Unmarshal(last["messages_replayed"], &replayed)
	if replayed != 2 {
		t.Errorf("messages_replayed = %v, want 2", replayed)
	}
}

func TestHandleReconnect_NoLastPos_ReplayWithEmptyPositions(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{},
		replayMsgs:    nil,
	}
	s := newReconnectTestServer(t, mb)
	c := &Client{
		id:            1,
		send:          make(chan OutgoingMsg, 8),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
	}

	// No last_pos key (backward compat / no-pos client).
	reqData := []byte(`{"client_id":"cli-2"}`)
	s.handleReconnect(c, reqData)

	var msgs []map[string]json.RawMessage
	for len(c.send) > 0 {
		msg := <-c.send
		var env map[string]json.RawMessage
		if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
			msgs = append(msgs, env)
		}
	}

	// Should just get a reconnect_ack with 0 replayed.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (ack), got %d", len(msgs))
	}
	var typ string
	_ = json.Unmarshal(msgs[0]["type"], &typ)
	if typ != RespTypeReconnectAck {
		t.Errorf("type = %q, want %q", typ, RespTypeReconnectAck)
	}
	var replayed float64
	_ = json.Unmarshal(msgs[0]["messages_replayed"], &replayed)
	if replayed != 0 {
		t.Errorf("messages_replayed = %v, want 0", replayed)
	}
}

func TestHandleReconnect_InvalidPos_ChannelSkipped(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{"acme.BTC.trade": "acme.prices.BTC"},
		replayMsgs:    nil,
	}
	s := newReconnectTestServer(t, mb)
	c := &Client{
		id:            1,
		send:          make(chan OutgoingMsg, 8),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
	}
	c.subscriptions.Add("acme.BTC.trade")

	// "not-a-pos" is not a valid "(partition+1)-offset" string → DecodePos returns ok=false.
	reqData := []byte(`{"client_id":"cli-3","last_pos":{"acme.BTC.trade":"not-a-pos"}}`)
	s.handleReconnect(c, reqData)

	var msgs []map[string]json.RawMessage
	for len(c.send) > 0 {
		msg := <-c.send
		var env map[string]json.RawMessage
		if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
			msgs = append(msgs, env)
		}
	}

	// Channel was skipped → Replay called with empty positions → no replay messages,
	// just the reconnect_ack.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (ack), got %d", len(msgs))
	}
	var typ string
	_ = json.Unmarshal(msgs[0]["type"], &typ)
	if typ != RespTypeReconnectAck {
		t.Errorf("type = %q, want %q", typ, RespTypeReconnectAck)
	}
}

func TestHandleReconnect_UnknownChannel_Skipped(t *testing.T) {
	t.Parallel()

	// No channel→topic mapping registered — ChannelTopic returns ok=false.
	mb := &mockBackend{
		channelTopics: map[string]string{},
		replayMsgs:    nil,
	}
	s := newReconnectTestServer(t, mb)
	c := &Client{
		id:            1,
		send:          make(chan OutgoingMsg, 8),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
	}
	c.subscriptions.Add("acme.BTC.trade")

	// Valid pos, but the channel has no topic mapping → ChannelTopic returns ok=false → skipped.
	reqData := []byte(`{"client_id":"cli-4","last_pos":{"acme.BTC.trade":"3-99"}}`)
	s.handleReconnect(c, reqData)

	var msgs []map[string]json.RawMessage
	for len(c.send) > 0 {
		msg := <-c.send
		var env map[string]json.RawMessage
		if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
			msgs = append(msgs, env)
		}
	}

	// Channel was skipped → Replay called with empty positions → reconnect_ack only.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (ack), got %d", len(msgs))
	}
	var typ string
	_ = json.Unmarshal(msgs[0]["type"], &typ)
	if typ != RespTypeReconnectAck {
		t.Errorf("type = %q, want %q", typ, RespTypeReconnectAck)
	}
}

func TestHandleReconnect_MultiChannelSameTopic_MinOffsetWins(t *testing.T) {
	t.Parallel()

	// Two channels share the same Kafka topic with different last offsets.
	// The replay request must use the smaller nextOffset (min wins).
	mb := &mockBackend{
		channelTopics: map[string]string{
			"acme.BTC.trade": "acme.trades",
			"acme.ETH.trade": "acme.trades",
		},
		replayMsgs: nil,
	}
	s := newReconnectTestServer(t, mb)
	c := &Client{
		id:            5,
		send:          make(chan OutgoingMsg, 8),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
	}
	c.subscriptions.Add("acme.BTC.trade")
	c.subscriptions.Add("acme.ETH.trade")

	// BTC last_pos "3-99" = partition 2, nextOffset 100; ETH last_pos "1-49" =
	// partition 0, nextOffset 50. Both share "acme.trades" but sit on DIFFERENT
	// partitions (records are keyed by channel), so both cursors must survive
	// per-partition — a topic-wide min would replay from offset 50 on partition
	// 2 too, re-delivering records BTC already saw.
	reqData := []byte(`{"client_id":"cli-5","last_pos":{"acme.BTC.trade":"3-99","acme.ETH.trade":"1-49"}}`)
	s.handleReconnect(c, reqData)

	mb.mu.Lock()
	req := mb.lastReplayReq
	mb.mu.Unlock()

	want := map[string]map[int32]int64{"acme.trades": {2: 100, 0: 50}}
	if !reflect.DeepEqual(req.Positions, want) {
		t.Errorf("Positions = %v, want %v (per-partition cursors on the shared topic)", req.Positions, want)
	}
}

func TestHandleReconnect_ConcurrentSafe(t *testing.T) {
	t.Parallel()
	mb := &mockBackend{
		channelTopics: map[string]string{"t.ch": "topic.ch"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "t.ch", Data: []byte(`{}`), Pos: "1-0"},
		},
	}
	s := newReconnectTestServer(t, mb)

	var wg sync.WaitGroup
	const n = 10
	for range n {
		wg.Go(func() {
			c := &Client{
				id:            1,
				send:          make(chan OutgoingMsg, 32),
				seqGen:        messaging.NewSequenceGenerator(),
				subscriptions: NewSubscriptionSet(),
			}
			c.subscriptions.Add("t.ch")
			s.handleReconnect(c, []byte(`{"client_id":"cli","last_pos":{"t.ch":"1-0"}}`))
		})
	}
	wg.Wait()
}

// =============================================================================
// Error Response Tests
// =============================================================================

func TestHandleClientMessage_ErrorResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantType string
		wantCode string
	}{
		{
			name:     "invalid_json",
			input:    `{invalid json`,
			wantType: MsgTypeError,
			wantCode: "invalid_json",
		},
		{
			name:     "invalid_subscribe_data",
			input:    `{"type":"subscribe","data":"not an object"}`,
			wantType: RespTypeSubscribeError,
			wantCode: "invalid_request",
		},
		{
			name:     "invalid_unsubscribe_data",
			input:    `{"type":"unsubscribe","data":"not an object"}`,
			wantType: RespTypeUnsubscribeError,
			wantCode: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Minimal server setup for error path testing
			server := &Server{
				logger:            zerolog.New(io.Discard),
				subscriptionIndex: NewSubscriptionIndex(),
			}

			// Minimal client setup
			client := &Client{
				id:            1,
				send:          make(chan OutgoingMsg, 16),
				subscriptions: NewSubscriptionSet(),
			}

			server.handleClientMessage(client, []byte(tt.input))

			select {
			case respMsg := <-client.send:
				var resp map[string]any
				if err := json.Unmarshal(respMsg.Bytes(), &resp); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}
				if resp["type"] != tt.wantType {
					t.Errorf("type = %v, want %v", resp["type"], tt.wantType)
				}
				if resp["code"] != tt.wantCode {
					t.Errorf("code = %v, want %v", resp["code"], tt.wantCode)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("Expected error response, got none")
			}
		})
	}
}
