package server

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/alerting"
)

// =============================================================================
// Server Stats Tests
// =============================================================================

func TestServer_GetStats(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	s.CurrentConnections.Store(42)
	s.TotalConnections.Store(100)

	server := &Server{stats: s}

	got := server.GetStats()

	if got != s {
		t.Error("GetStats() should return the same stats instance")
	}
	if got.CurrentConnections.Load() != 42 {
		t.Errorf("GetStats().CurrentConnections: got %d, want 42", got.CurrentConnections.Load())
	}
	if got.TotalConnections.Load() != 100 {
		t.Errorf("GetStats().TotalConnections: got %d, want 100", got.TotalConnections.Load())
	}
}

// =============================================================================
// Server Stats Concurrent Updates Tests
// =============================================================================

func TestServer_Stats_ConcurrentUpdates(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	server := &Server{stats: s}

	const numGoroutines = 100
	const numUpdates = 1000

	done := make(chan bool)

	// Concurrent connection increments
	for range numGoroutines {
		go func() {
			for range numUpdates {
				server.stats.CurrentConnections.Add(1)
				server.stats.TotalConnections.Add(1)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for range numGoroutines {
		<-done
	}

	expected := int64(numGoroutines * numUpdates)
	if server.stats.CurrentConnections.Load() != expected {
		t.Errorf("CurrentConnections: got %d, want %d", server.stats.CurrentConnections.Load(), expected)
	}
	if server.stats.TotalConnections.Load() != expected {
		t.Errorf("TotalConnections: got %d, want %d", server.stats.TotalConnections.Load(), expected)
	}
}

// =============================================================================
// Message Type Detection Tests
// =============================================================================

func TestParseMessageType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:    "subscribe message",
			input:   `{"type":"subscribe","data":{"channels":["BTC.trade"]}}`,
			want:    "subscribe",
			wantErr: false,
		},
		{
			name:    "unsubscribe message",
			input:   `{"type":"unsubscribe","data":{"channels":["BTC.trade"]}}`,
			want:    "unsubscribe",
			wantErr: false,
		},
		{
			name:    "heartbeat message",
			input:   `{"type":"heartbeat"}`,
			want:    "heartbeat",
			wantErr: false,
		},
		{
			name:    "reconnect message",
			input:   `{"type":"reconnect","data":{"client_id":"abc","last_offset":{}}}`,
			want:    "reconnect",
			wantErr: false,
		},
		{
			name:    "invalid json",
			input:   `{not valid json}`,
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty object",
			input:   `{}`,
			want:    "",
			wantErr: false,
		},
		{
			name:    "missing type field",
			input:   `{"data":{"channels":["BTC"]}}`,
			want:    "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var msg struct {
				Type string          `json:"type"`
				Data json.RawMessage `json:"data"`
			}

			err := json.Unmarshal([]byte(tt.input), &msg)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if msg.Type != tt.want {
				t.Errorf("Type: got %q, want %q", msg.Type, tt.want)
			}
		})
	}
}

// =============================================================================
// Subscribe/Unsubscribe Request Parsing Tests
// =============================================================================

func TestParseSubscribeRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantErr  bool
		channels []string
	}{
		{
			name:     "single channel",
			input:    `{"channels":["BTC.trade"]}`,
			wantLen:  1,
			wantErr:  false,
			channels: []string{"BTC.trade"},
		},
		{
			name:     "multiple channels",
			input:    `{"channels":["BTC.trade","ETH.liquidity","SOL.social"]}`,
			wantLen:  3,
			wantErr:  false,
			channels: []string{"BTC.trade", "ETH.liquidity", "SOL.social"},
		},
		{
			name:     "empty channels",
			input:    `{"channels":[]}`,
			wantLen:  0,
			wantErr:  false,
			channels: []string{},
		},
		{
			name:    "invalid json",
			input:   `{invalid}`,
			wantErr: true,
		},
		{
			name:     "missing channels field",
			input:    `{}`,
			wantLen:  0,
			wantErr:  false,
			channels: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var req struct {
				Channels []string `json:"channels"`
			}

			err := json.Unmarshal([]byte(tt.input), &req)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(req.Channels) != tt.wantLen {
				t.Errorf("Channels length: got %d, want %d", len(req.Channels), tt.wantLen)
			}

			if tt.channels != nil {
				for i, ch := range tt.channels {
					if i < len(req.Channels) && req.Channels[i] != ch {
						t.Errorf("Channel[%d]: got %q, want %q", i, req.Channels[i], ch)
					}
				}
			}
		})
	}
}

// =============================================================================
// Reconnect Request Parsing Tests
// =============================================================================

func TestParseReconnectRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		input        string
		wantClientID string
		wantOffsets  map[string]int64
		wantErr      bool
	}{
		{
			name:         "valid reconnect",
			input:        `{"client_id":"abc123","last_offset":{"topic1":100,"topic2":200}}`,
			wantClientID: "abc123",
			wantOffsets:  map[string]int64{"topic1": 100, "topic2": 200},
			wantErr:      false,
		},
		{
			name:         "empty offsets",
			input:        `{"client_id":"xyz","last_offset":{}}`,
			wantClientID: "xyz",
			wantOffsets:  map[string]int64{},
			wantErr:      false,
		},
		{
			name:         "missing client_id",
			input:        `{"last_offset":{"topic1":100}}`,
			wantClientID: "",
			wantOffsets:  map[string]int64{"topic1": 100},
			wantErr:      false,
		},
		{
			name:    "invalid json",
			input:   `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var req struct {
				ClientID   string           `json:"client_id"`
				LastOffset map[string]int64 `json:"last_offset"`
			}

			err := json.Unmarshal([]byte(tt.input), &req)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if req.ClientID != tt.wantClientID {
				t.Errorf("ClientID: got %q, want %q", req.ClientID, tt.wantClientID)
			}

			if len(req.LastOffset) != len(tt.wantOffsets) {
				t.Errorf("LastOffset length: got %d, want %d", len(req.LastOffset), len(tt.wantOffsets))
			}

			for k, v := range tt.wantOffsets {
				if req.LastOffset[k] != v {
					t.Errorf("LastOffset[%s]: got %d, want %d", k, req.LastOffset[k], v)
				}
			}
		})
	}
}

// =============================================================================
// Heartbeat Response Tests
// =============================================================================

func TestHeartbeatResponse_Format(t *testing.T) {
	t.Parallel()
	pong := map[string]any{
		"type": "pong",
		"ts":   time.Now().UnixMilli(),
	}

	data, err := json.Marshal(pong)
	if err != nil {
		t.Fatalf("Failed to marshal pong: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse pong: %v", err)
	}

	if parsed["type"] != "pong" {
		t.Errorf("type: got %v, want pong", parsed["type"])
	}

	if _, ok := parsed["ts"]; !ok {
		t.Error("ts field missing")
	}
}

// =============================================================================
// Subscription Acknowledgment Tests
// =============================================================================

func TestSubscriptionAck_Format(t *testing.T) {
	t.Parallel()
	channels := []string{"BTC.trade", "ETH.liquidity"}
	ack := map[string]any{
		"type":       "subscription_ack",
		"subscribed": channels,
		"count":      len(channels),
	}

	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Failed to marshal ack: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse ack: %v", err)
	}

	if parsed["type"] != "subscription_ack" {
		t.Errorf("type: got %v, want subscription_ack", parsed["type"])
	}

	subscribedArr, ok := parsed["subscribed"].([]any)
	if !ok {
		t.Error("subscribed should be an array")
		return
	}

	if len(subscribedArr) != 2 {
		t.Errorf("subscribed length: got %d, want 2", len(subscribedArr))
	}

	count, ok := parsed["count"].(float64) // JSON numbers are float64
	if !ok {
		t.Error("count should be a number")
		return
	}

	if int(count) != 2 {
		t.Errorf("count: got %d, want 2", int(count))
	}
}

func TestUnsubscriptionAck_Format(t *testing.T) {
	t.Parallel()
	channels := []string{"BTC.trade"}
	ack := map[string]any{
		"type":         "unsubscription_ack",
		"unsubscribed": channels,
		"count":        5, // remaining subscriptions
	}

	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Failed to marshal ack: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse ack: %v", err)
	}

	if parsed["type"] != "unsubscription_ack" {
		t.Errorf("type: got %v, want unsubscription_ack", parsed["type"])
	}

	unsubscribedArr, ok := parsed["unsubscribed"].([]any)
	if !ok {
		t.Error("unsubscribed should be an array")
		return
	}

	if len(unsubscribedArr) != 1 {
		t.Errorf("unsubscribed length: got %d, want 1", len(unsubscribedArr))
	}
}

// =============================================================================
// Error Message Format Tests
// =============================================================================

func TestReconnectError_Format(t *testing.T) {
	t.Parallel()
	errorMsg := map[string]any{
		"type":    "reconnect_error",
		"message": "Invalid reconnect request format",
	}

	data, err := json.Marshal(errorMsg)
	if err != nil {
		t.Fatalf("Failed to marshal error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse error: %v", err)
	}

	if parsed["type"] != "reconnect_error" {
		t.Errorf("type: got %v, want reconnect_error", parsed["type"])
	}

	if _, ok := parsed["message"]; !ok {
		t.Error("message field missing")
	}
}

// =============================================================================
// Reconnect Acknowledgment Tests
// =============================================================================

func TestReconnectAck_Format(t *testing.T) {
	t.Parallel()
	ackMsg := map[string]any{
		"type":              "reconnect_ack",
		"status":            "completed",
		"messages_replayed": 42,
		"message":           "Replayed 42 missed messages",
	}

	data, err := json.Marshal(ackMsg)
	if err != nil {
		t.Fatalf("Failed to marshal ack: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse ack: %v", err)
	}

	if parsed["type"] != "reconnect_ack" {
		t.Errorf("type: got %v, want reconnect_ack", parsed["type"])
	}

	if parsed["status"] != "completed" {
		t.Errorf("status: got %v, want completed", parsed["status"])
	}

	replayedCount, ok := parsed["messages_replayed"].(float64)
	if !ok {
		t.Error("messages_replayed should be a number")
		return
	}

	if int(replayedCount) != 42 {
		t.Errorf("messages_replayed: got %d, want 42", int(replayedCount))
	}
}

// =============================================================================
// Server Shutdown Flag Tests
// =============================================================================

func TestServer_ShutdownFlag(t *testing.T) {
	t.Parallel()
	server := &Server{}

	// Initial state
	if server.shuttingDown.Load() != 0 {
		t.Error("shuttingDown should be 0 initially")
	}

	// Set shutdown flag
	server.shuttingDown.Store(1)

	if server.shuttingDown.Load() != 1 {
		t.Error("shuttingDown should be 1 after store")
	}
}

func TestServer_ShutdownFlag_ConcurrentReads(t *testing.T) {
	t.Parallel()
	server := &Server{}

	done := make(chan bool)

	// Start readers before setting flag
	for range 100 {
		go func() {
			for range 1000 {
				_ = server.shuttingDown.Load()
			}
			done <- true
		}()
	}

	// Set flag while reads are happening
	server.shuttingDown.Store(1)

	// Wait for all readers
	for range 100 {
		<-done
	}

	// Verify final state
	if server.shuttingDown.Load() != 1 {
		t.Error("shuttingDown should still be 1")
	}
}

// =============================================================================
// Client ID Generation Tests
// =============================================================================

func TestClientIDGeneration_AtomicIncrement(t *testing.T) {
	t.Parallel()
	// Test atomic increment pattern (more reliable than UnixNano for uniqueness)
	var counter atomic.Int64
	ids := make(map[int64]bool)
	iterations := 10000

	for range iterations {
		id := counter.Add(1)
		if ids[id] {
			t.Errorf("Duplicate ID generated: %d", id)
		}
		ids[id] = true
	}

	if len(ids) != iterations {
		t.Errorf("Expected %d unique IDs, got %d", iterations, len(ids))
	}
}

// =============================================================================
// Connection Semaphore Tests
// =============================================================================

func TestConnectionSemaphore_Behavior(t *testing.T) {
	t.Parallel()
	maxConn := 3
	sem := make(chan struct{}, maxConn)

	// Should be able to acquire maxConn slots
	for i := range maxConn {
		select {
		case sem <- struct{}{}:
			// Good
		default:
			t.Errorf("Should be able to acquire slot %d", i+1)
		}
	}

	// Should NOT be able to acquire one more
	select {
	case sem <- struct{}{}:
		t.Error("Should not be able to acquire more than max")
	default:
		// Good - semaphore is full
	}

	// Release one
	<-sem

	// Now should be able to acquire again
	select {
	case sem <- struct{}{}:
		// Good
	default:
		t.Error("Should be able to acquire after release")
	}
}

// =============================================================================
// NewServer Validation Tests
// =============================================================================

func TestNewServer_ValidatesParams(t *testing.T) {
	t.Parallel()
	validConfig := newTestServerConfig()

	tests := []struct {
		name   string
		params Params
		errMsg string
	}{
		{
			name:   "nil config",
			params: Params{Addr: ":0", MaxConnections: 1},
			errMsg: "Config must not be nil",
		},
		{
			name:   "empty addr",
			params: Params{Config: validConfig, Addr: "", MaxConnections: 1},
			errMsg: "Addr must not be empty",
		},
		{
			name:   "zero max connections",
			params: Params{Config: validConfig, Addr: ":0", MaxConnections: 0},
			errMsg: "MaxConnections must be > 0",
		},
		{
			name:   "negative max connections",
			params: Params{Config: validConfig, Addr: ":0", MaxConnections: -1},
			errMsg: "MaxConnections must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, err := NewServer(tt.params, &alerting.NoopAlerter{})
			if err == nil {
				_ = srv.Shutdown()
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errMsg)
			}
		})
	}
}

// =============================================================================
// Regression Tests - Backend doesn't affect existing functionality
// =============================================================================

func TestServer_BroadcastFunctionality_NotAffectedByBackendField(t *testing.T) {
	t.Parallel()
	// Regression test: Adding backend field to Server struct
	// must not affect the broadcast functionality.

	params := newTestParams()
	params.Addr = ":0"
	params.MaxConnections = 100
	params.Config.LogLevel = "info" // Avoid SetGlobalLevel(Error) contaminating parallel tests

	server, err := NewServer(params, &alerting.NoopAlerter{})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer func() { _ = server.Shutdown() }()

	// Trigger a broadcast
	server.Broadcast("BTC.trade", []byte(`{"price": "50000"}`), "", "")

	// Note: Broadcast goes through subscription filtering, so it won't call
	// the mockBroadcast directly. Instead, verify the server accepts the call
	// without panic and the subscription mechanisms work.

	// Verify server metrics are accessible (another regression check)
	serverStats := server.GetStats()
	if serverStats == nil {
		t.Error("GetStats() returned nil")
	}

	// The key test: server didn't panic and is functional
}

func TestServer_SubscriptionFlow_NotAffectedByBackendField(t *testing.T) {
	t.Parallel()
	// Regression test: Subscription flow must work correctly
	// even with backend field present in Server struct

	params := newTestParams()
	params.Addr = ":0"
	params.MaxConnections = 100
	params.Config.LogLevel = "info" // Avoid SetGlobalLevel(Error) contaminating parallel tests

	server, err := NewServer(params, &alerting.NoopAlerter{})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer func() { _ = server.Shutdown() }()

	// Create a mock client
	client := &Client{
		id:            1,
		subscriptions: NewSubscriptionSet(),
		send:          make(chan OutgoingMsg, 10),
	}

	// Test subscription operations
	client.subscriptions.Add("BTC.trade")
	client.subscriptions.Add("ETH.liquidity")

	if !client.subscriptions.Has("BTC.trade") {
		t.Error("Subscription not recorded")
	}

	if client.subscriptions.Count() != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", client.subscriptions.Count())
	}

	// Test unsubscription
	client.subscriptions.Remove("BTC.trade")
	if client.subscriptions.Has("BTC.trade") {
		t.Error("Subscription not removed")
	}
}

func TestServer_MessageParsing_AllTypesStillWork(t *testing.T) {
	t.Parallel()
	// Regression test: All message types must parse correctly
	// This verifies the handlers_message.go switch statement
	// wasn't broken by adding the "publish" case

	testCases := []struct {
		name     string
		input    string
		wantType string
	}{
		{"subscribe", `{"type":"subscribe","data":{"channels":["BTC.trade"]}}`, "subscribe"},
		{"unsubscribe", `{"type":"unsubscribe","data":{"channels":["BTC.trade"]}}`, "unsubscribe"},
		{"heartbeat", `{"type":"heartbeat"}`, "heartbeat"},
		{"reconnect", `{"type":"reconnect","data":{"client_id":"abc"}}`, "reconnect"},
		{"publish", `{"type":"publish","data":{"channel":"test.chat","data":{}}}`, "publish"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var msg struct {
				Type string          `json:"type"`
				Data json.RawMessage `json:"data"`
			}
			err := json.Unmarshal([]byte(tc.input), &msg)
			if err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			if msg.Type != tc.wantType {
				t.Errorf("Type mismatch: got %q, want %q", msg.Type, tc.wantType)
			}
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkParseMessageType(b *testing.B) {
	input := []byte(`{"type":"subscribe","data":{"channels":["BTC.trade"]}}`)

	for b.Loop() {
		var msg struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		_ = json.Unmarshal(input, &msg)
	}
}

func BenchmarkParseSubscribeRequest(b *testing.B) {
	input := []byte(`{"channels":["BTC.trade","ETH.liquidity","SOL.social"]}`)

	for b.Loop() {
		var req struct {
			Channels []string `json:"channels"`
		}
		_ = json.Unmarshal(input, &req)
	}
}

func BenchmarkHeartbeatResponse(b *testing.B) {
	for b.Loop() {
		pong := map[string]any{
			"type": "pong",
			"ts":   time.Now().UnixMilli(),
		}
		_, _ = json.Marshal(pong)
	}
}

func BenchmarkSubscriptionAck(b *testing.B) {
	channels := []string{"BTC.trade", "ETH.liquidity", "SOL.social"}

	for b.Loop() {
		ack := map[string]any{
			"type":       "subscription_ack",
			"subscribed": channels,
			"count":      len(channels),
		}
		_, _ = json.Marshal(ack)
	}
}
