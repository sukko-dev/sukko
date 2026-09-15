package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/stats"
)

// =============================================================================
// PumpConfig Tests
// =============================================================================

// testPumpConfig returns a PumpConfig with test values matching production defaults.
// These values match the envDefault values in platform/server_config.go:
//   - PongWait: 60s, PingPeriod: 45s, WriteWait: 5s
func testPumpConfig() PumpConfig {
	return PumpConfig{
		PongWait:   60 * time.Second,
		PingPeriod: 45 * time.Second,
		WriteWait:  5 * time.Second,
	}
}

func TestPumpConfig_PingLessThanPong(t *testing.T) {
	t.Parallel()
	config := testPumpConfig()

	if config.PingPeriod >= config.PongWait {
		t.Errorf("PingPeriod (%v) should be less than PongWait (%v)", config.PingPeriod, config.PongWait)
	}
}

func TestNewPumpConfig(t *testing.T) {
	t.Parallel()

	// writeWait is the test value for WriteWait parameter
	const writeWait = 5 * time.Second

	tests := []struct {
		name           string
		pongWait       time.Duration
		pingPeriod     time.Duration
		writeWait      time.Duration
		wantPongWait   time.Duration
		wantPingPeriod time.Duration
		wantWriteWait  time.Duration
		wantWarning    bool
	}{
		{
			name:           "valid_config_60s_45s",
			pongWait:       60 * time.Second,
			pingPeriod:     45 * time.Second,
			writeWait:      writeWait,
			wantPongWait:   60 * time.Second,
			wantPingPeriod: 45 * time.Second,
			wantWriteWait:  writeWait,
			wantWarning:    false,
		},
		{
			name:           "valid_config_120s_90s",
			pongWait:       120 * time.Second,
			pingPeriod:     90 * time.Second,
			writeWait:      writeWait,
			wantPongWait:   120 * time.Second,
			wantPingPeriod: 90 * time.Second,
			wantWriteWait:  writeWait,
			wantWarning:    false,
		},
		{
			name:           "pingPeriod_equals_pongWait_falls_back_to_75_percent",
			pongWait:       60 * time.Second,
			pingPeriod:     60 * time.Second,
			writeWait:      writeWait,
			wantPongWait:   60 * time.Second,
			wantPingPeriod: 45 * time.Second, // 75% of 60s
			wantWriteWait:  writeWait,
			wantWarning:    true,
		},
		{
			name:           "pingPeriod_exceeds_pongWait_falls_back_to_75_percent",
			pongWait:       60 * time.Second,
			pingPeriod:     90 * time.Second,
			writeWait:      writeWait,
			wantPongWait:   60 * time.Second,
			wantPingPeriod: 45 * time.Second, // 75% of 60s
			wantWriteWait:  writeWait,
			wantWarning:    true,
		},
		{
			name:           "small_values_with_valid_ratio",
			pongWait:       20 * time.Second,
			pingPeriod:     10 * time.Second,
			writeWait:      writeWait,
			wantPongWait:   20 * time.Second,
			wantPingPeriod: 10 * time.Second,
			wantWriteWait:  writeWait,
			wantWarning:    false,
		},
		{
			name:           "custom_writeWait_value",
			pongWait:       60 * time.Second,
			pingPeriod:     45 * time.Second,
			writeWait:      10 * time.Second,
			wantPongWait:   60 * time.Second,
			wantPingPeriod: 45 * time.Second,
			wantWriteWait:  10 * time.Second,
			wantWarning:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use zerolog with buffer to capture warnings
			var buf bytes.Buffer
			logger := zerolog.New(&buf).Level(zerolog.WarnLevel)

			cfg := NewPumpConfig(tt.pongWait, tt.pingPeriod, tt.writeWait, logger)

			if cfg.PongWait != tt.wantPongWait {
				t.Errorf("PongWait = %v, want %v", cfg.PongWait, tt.wantPongWait)
			}
			if cfg.PingPeriod != tt.wantPingPeriod {
				t.Errorf("PingPeriod = %v, want %v", cfg.PingPeriod, tt.wantPingPeriod)
			}
			if cfg.WriteWait != tt.wantWriteWait {
				t.Errorf("WriteWait = %v, want %v", cfg.WriteWait, tt.wantWriteWait)
			}

			hasWarning := buf.Len() > 0
			if tt.wantWarning && !hasWarning {
				t.Error("Expected warning log for invalid config, got none")
			}
			if !tt.wantWarning && hasWarning {
				t.Errorf("Expected no warning log for valid config, got: %s", buf.String())
			}
		})
	}
}

// =============================================================================
// NewPump Tests
// =============================================================================

func TestNewPump_NilDependencies(t *testing.T) {
	t.Parallel()
	config := testPumpConfig()
	s := stats.NewStats()

	pump := NewPump(config, nil, zerolog.Logger{}, nil, nil, s, nil)

	if pump == nil {
		t.Fatal("NewPump returned nil")
	}
	if pump.Stats != s {
		t.Error("Stats not set correctly")
	}
}

func TestNewPump_AllDependencies(t *testing.T) {
	t.Parallel()
	config := testPumpConfig()
	mockLogger := newTestMockLogger()
	mockRateLimiter := newTestMockRateLimiter()
	mockAlertLogger := newTestMockAlertLogger()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := NewPump(
		config,
		mockLogger,
		zerolog.Logger{},
		mockRateLimiter,
		mockAlertLogger,
		s,
		mockClock,
	)

	if pump == nil {
		t.Fatal("NewPump returned nil")
	}
	if pump.Logger == nil {
		t.Error("Logger not set")
	}
	if pump.RateLimiter == nil {
		t.Error("RateLimiter not set")
	}
	if pump.AlertLogger == nil {
		t.Error("AlertLogger not set")
	}
	if pump.Clock == nil {
		t.Error("Clock not set")
	}
}

// =============================================================================
// CreateRateLimitErrorMessage Tests
// =============================================================================

func TestCreateRateLimitErrorMessage_Format(t *testing.T) {
	t.Parallel()
	msg := CreateRateLimitErrorMessage(10)

	var parsed map[string]any
	if err := json.Unmarshal(msg, &parsed); err != nil {
		t.Fatalf("Failed to parse message: %v", err)
	}

	if parsed["type"] != "error" {
		t.Errorf("type: got %v, want error", parsed["type"])
	}
	if parsed["code"] != "RATE_LIMIT_EXCEEDED" {
		t.Errorf("code: got %v, want RATE_LIMIT_EXCEEDED", parsed["code"])
	}
	if _, ok := parsed["message"]; !ok {
		t.Error("message field missing")
	}
}

func TestCreateRateLimitErrorMessage_Consistency(t *testing.T) {
	t.Parallel()
	msg1 := CreateRateLimitErrorMessage(10)
	msg2 := CreateRateLimitErrorMessage(10)

	if !bytes.Equal(msg1, msg2) {
		t.Error("CreateRateLimitErrorMessage should return consistent output")
	}
}

// =============================================================================
// Pump.handleRateLimitExceeded Tests
// =============================================================================

func TestPump_HandleRateLimitExceeded_LogsWarning(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	mockAlertLogger := newTestMockAlertLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger:      mockLogger,
		AlertLogger: mockAlertLogger,
		Stats:       s,
	}

	client := &Client{
		id:   12345,
		send: make(chan OutgoingMsg, 10),
	}

	pump.handleRateLimitExceeded(client)

	if mockLogger.messageCount() == 0 {
		t.Error("Expected log message for rate limit")
	}

	messages := mockLogger.getMessages()
	foundWarning := false
	for _, msg := range messages {
		if msg.level == "warn" {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("Expected warning level log")
	}
}

func TestPump_HandleRateLimitExceeded_AlertLog(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	mockAlertLogger := newTestMockAlertLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger:      mockLogger,
		AlertLogger: mockAlertLogger,
		Stats:       s,
	}

	client := &Client{
		id:   12345,
		send: make(chan OutgoingMsg, 10),
	}

	pump.handleRateLimitExceeded(client)

	if mockAlertLogger.eventCount() == 0 {
		t.Error("Expected alert log event")
	}

	if !mockAlertLogger.hasEvent("ClientRateLimited") {
		t.Error("Expected ClientRateLimited alert event")
	}
}

func TestPump_HandleRateLimitExceeded_SendsError(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger: mockLogger,
		Stats:  s,
	}

	client := &Client{
		id:   12345,
		send: make(chan OutgoingMsg, 10),
	}

	pump.handleRateLimitExceeded(client)

	select {
	case msg := <-client.send:
		var parsed map[string]any
		if err := json.Unmarshal(msg.Bytes(), &parsed); err != nil {
			t.Fatalf("Failed to parse sent message: %v", err)
		}
		if parsed["code"] != "RATE_LIMIT_EXCEEDED" {
			t.Errorf("Expected RATE_LIMIT_EXCEEDED, got %v", parsed["code"])
		}
	default:
		t.Error("Expected error message to be sent to client")
	}
}

func TestPump_HandleRateLimitExceeded_FullBuffer(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger: mockLogger,
		Stats:  s,
	}

	client := &Client{
		id:   12345,
		send: make(chan OutgoingMsg, 1),
	}
	client.send <- RawMsg([]byte("existing message"))

	done := make(chan bool)
	go func() {
		pump.handleRateLimitExceeded(client)
		done <- true
	}()

	select {
	case <-done:
		// Good - didn't block
	case <-time.After(100 * time.Millisecond):
		t.Error("handleRateLimitExceeded blocked on full buffer")
	}
}

func TestPump_HandleRateLimitExceeded_UpdatesStats(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger: mockLogger,
		Stats:  s,
	}

	client := &Client{
		id:   12345,
		send: make(chan OutgoingMsg, 10),
	}

	initialCount := s.RateLimitedMessages.Load()

	pump.handleRateLimitExceeded(client)

	if s.RateLimitedMessages.Load() != initialCount+1 {
		t.Errorf("RateLimitedMessages: got %d, want %d", s.RateLimitedMessages.Load(), initialCount+1)
	}
}

// =============================================================================
// Pump.now Tests
// =============================================================================

func TestPump_Now_WithMockClock(t *testing.T) {
	t.Parallel()
	fixedTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mockClock := newTestMockClock(fixedTime)

	pump := &Pump{Clock: mockClock}

	result := pump.now()

	if !result.Equal(fixedTime) {
		t.Errorf("now(): got %v, want %v", result, fixedTime)
	}
}

func TestPump_Now_WithNilClock(t *testing.T) {
	t.Parallel()
	pump := &Pump{Clock: nil}

	before := time.Now()
	result := pump.now()
	after := time.Now()

	if result.Before(before) || result.After(after) {
		t.Errorf("now() without clock should return current time")
	}
}

func TestPump_Now_ClockAdvance(t *testing.T) {
	t.Parallel()
	mockClock := newTestMockClock()
	pump := &Pump{Clock: mockClock}

	t1 := pump.now()
	mockClock.advance(5 * time.Second)
	t2 := pump.now()

	diff := t2.Sub(t1)
	if diff != 5*time.Second {
		t.Errorf("Time difference: got %v, want 5s", diff)
	}
}

// =============================================================================
// Pump.newTicker Tests
// =============================================================================

func TestPump_NewTicker_WithMockClock(t *testing.T) {
	t.Parallel()
	mockClock := newTestMockClock()
	pump := &Pump{Clock: mockClock}

	ticker := pump.newTicker(1 * time.Second)

	if ticker == nil {
		t.Fatal("newTicker returned nil")
	}

	if ticker.C() == nil {
		t.Error("Ticker channel is nil")
	}

	ticker.Stop()
}

func TestPump_NewTicker_WithNilClock(t *testing.T) {
	t.Parallel()
	pump := &Pump{Clock: nil}

	ticker := pump.newTicker(1 * time.Second)

	if ticker == nil {
		t.Fatal("newTicker returned nil")
	}

	ticker.Stop()
}

// =============================================================================
// Adapter Tests
// =============================================================================

func TestZerologAdapter_ImplementsLogger(t *testing.T) {
	t.Parallel()
	var _ Logger = (*ZerologAdapter)(nil)
}

func TestAlertLogger_ImplementsAlertLogger(t *testing.T) {
	t.Parallel()
	var _ AlertLogger = (*alertLogger)(nil)
}

func TestRateLimiterAdapter_ImplementsRateLimiter(t *testing.T) {
	t.Parallel()
	var _ RateLimiter = (*RateLimiterAdapter)(nil)
}

func TestRealClock_ImplementsClock(t *testing.T) {
	t.Parallel()
	var _ Clock = (*RealClock)(nil)
}

func TestRealTicker_ImplementsTicker(t *testing.T) {
	t.Parallel()
	var _ Ticker = (*RealTicker)(nil)
}

// =============================================================================
// RealClock Tests
// =============================================================================

func TestRealClock_Now(t *testing.T) {
	t.Parallel()
	clock := &RealClock{}

	before := time.Now()
	result := clock.Now()
	after := time.Now()

	if result.Before(before) || result.After(after) {
		t.Error("RealClock.Now() should return current time")
	}
}

func TestRealClock_NewTicker(t *testing.T) {
	t.Parallel()
	clock := &RealClock{}

	ticker := clock.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	if ticker == nil {
		t.Fatal("NewTicker returned nil")
	}

	select {
	case <-ticker.C():
		// Good
	case <-time.After(200 * time.Millisecond):
		t.Error("Ticker did not fire")
	}
}

func TestRealClock_After(t *testing.T) {
	t.Parallel()
	clock := &RealClock{}

	ch := clock.After(10 * time.Millisecond)

	select {
	case <-ch:
		// Good
	case <-time.After(100 * time.Millisecond):
		t.Error("After did not fire")
	}
}

// =============================================================================
// Concurrent Safety Tests
// =============================================================================

func TestPump_HandleRateLimitExceeded_Concurrent(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Logger: mockLogger,
		Stats:  s,
	}

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			client := &Client{
				id:   int64(id),
				send: make(chan OutgoingMsg, 10),
			}
			pump.handleRateLimitExceeded(client)
		}(i)
	}

	wg.Wait()

	if s.RateLimitedMessages.Load() != numGoroutines {
		t.Errorf("RateLimitedMessages: got %d, want %d", s.RateLimitedMessages.Load(), numGoroutines)
	}
}

// =============================================================================
// Integration-style Tests
// =============================================================================

func TestPump_RateLimitingFlow(t *testing.T) {
	t.Parallel()
	mockLogger := newTestMockLogger()
	mockRateLimiter := newTestMockRateLimiter()
	mockAlertLogger := newTestMockAlertLogger()
	s := stats.NewStats()

	mockRateLimiter.setAllowCount(5)

	pump := &Pump{
		Logger:      mockLogger,
		RateLimiter: mockRateLimiter,
		AlertLogger: mockAlertLogger,
		Stats:       s,
	}

	clientID := int64(12345)
	blockedCount := 0
	for range 10 {
		if !pump.RateLimiter.CheckLimit(clientID) {
			blockedCount++
		}
	}

	if blockedCount != 5 {
		t.Errorf("blockedCount: got %d, want 5", blockedCount)
	}

	if mockRateLimiter.getCallCount() != 10 {
		t.Errorf("Rate limiter call count: got %d, want 10", mockRateLimiter.getCallCount())
	}
}

// =============================================================================
// ZerologAdapter Tests
// =============================================================================

//nolint:paralleltest // depends on zerolog global state
func TestZerologAdapter_Methods_ReturnValidEvents(t *testing.T) {
	// Create a zerolog logger that writes to a buffer so we can verify output
	// Use Level(DebugLevel) to ensure debug messages are not filtered by global level
	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
	adapter := NewZerologAdapter(logger)

	// Test Debug level - should return a valid event that can be chained and emit output
	adapter.Debug().Str("test", "debug_value").Msg("debug message")

	// Test Info level
	adapter.Info().Str("test", "info_value").Msg("info message")

	// Test Warn level
	adapter.Warn().Str("test", "warn_value").Msg("warn message")

	// Test Error level
	adapter.Error().Str("test", "error_value").Msg("error message")

	// Verify output was written (zerolog writes JSON)
	output := buf.String()

	// Verify all levels produced output
	if !strings.Contains(output, "debug message") {
		t.Error("Debug() did not produce output")
	}
	if !strings.Contains(output, "info message") {
		t.Error("Info() did not produce output")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("Warn() did not produce output")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Error() did not produce output")
	}

	// Verify fields were included
	if !strings.Contains(output, "debug_value") {
		t.Error("Debug().Str() field not in output")
	}
}

func TestZerologEventAdapter_Chaining_ProducesOutput(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	adapter := NewZerologAdapter(logger)

	// Test method chaining with all supported methods
	adapter.Info().
		Str("key", "value").
		Int("num", 42).
		Int64("big", 1234567890).
		Interface("data", map[string]int{"a": 1}).
		Msg("test message")

	output := buf.String()

	// Verify the chained call produced output
	if output == "" {
		t.Error("Chained method call did not produce output")
	}

	// Verify the message was included
	if !strings.Contains(output, "test message") {
		t.Errorf("Output missing message: %s", output)
	}

	// Verify all fields were included in output (zerolog writes JSON)
	if !strings.Contains(output, "\"key\":\"value\"") {
		t.Errorf("Output missing Str field: %s", output)
	}
	if !strings.Contains(output, "\"num\":42") {
		t.Errorf("Output missing Int field: %s", output)
	}
	if !strings.Contains(output, "\"big\":1234567890") {
		t.Errorf("Output missing Int64 field: %s", output)
	}
	// Interface fields are JSON-encoded
	if !strings.Contains(output, "\"data\":{\"a\":1}") {
		t.Errorf("Output missing Interface field: %s", output)
	}
}

// =============================================================================
// ReadLoop Tests - WebSocket Frame Handling
// =============================================================================

// Helper to create a WebSocket frame for testing.
// Client frames are always masked per WebSocket spec.
func createWebSocketFrame(opCode ws.OpCode, payload []byte) []byte {
	var buf bytes.Buffer

	header := ws.Header{
		Fin:    true,
		OpCode: opCode,
		Length: int64(len(payload)),
		Masked: true,
		Mask:   [4]byte{0x12, 0x34, 0x56, 0x78},
	}

	_ = ws.WriteHeader(&buf, header)

	if len(payload) > 0 {
		// Create a copy to mask
		maskedPayload := make([]byte, len(payload))
		copy(maskedPayload, payload)
		ws.Cipher(maskedPayload, header.Mask, 0)
		buf.Write(maskedPayload)
	}

	return buf.Bytes()
}

func TestReadLoop_TextMessage_ProcessedCorrectly(t *testing.T) {
	t.Parallel()
	// Create test message
	textPayload := []byte(`{"type":"subscribe","data":{"channels":["BTC.trade"]}}`)
	frameData := createWebSocketFrame(ws.OpText, textPayload) // Client frames are masked

	// Add a close frame to terminate the loop
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})
	frameData = append(frameData, closeFrame...)

	mockConn := newTestMockConn()
	mockConn.setReadData(frameData)

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	var receivedMsg []byte
	handleMsgFn := func(_ *Client, msg []byte) {
		receivedMsg = msg
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, nil, handleMsgFn)

	// Verify message was received
	if receivedMsg == nil {
		t.Error("Expected text message to be processed")
	}

	if !bytes.Equal(receivedMsg, textPayload) {
		t.Errorf("Message mismatch: got %s, want %s", string(receivedMsg), string(textPayload))
	}

	// Verify stats were updated
	if s.MessagesReceived.Load() != 1 {
		t.Errorf("MessagesReceived: got %d, want 1", s.MessagesReceived.Load())
	}
}

func TestReadLoop_PingFrame_SendsPongResponse(t *testing.T) {
	t.Parallel()
	// Create ping frame with payload
	pingPayload := []byte("ping-data")
	pingFrame := createWebSocketFrame(ws.OpPing, pingPayload)

	// Add close frame to terminate
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})
	frameData := slices.Concat(pingFrame, closeFrame)

	mockConn := newTestMockConn()
	mockConn.setReadData(frameData)

	s := stats.NewStats()
	mockLogger := newTestMockLogger()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		control:       make(chan []byte, controlChannelSize),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, nil, nil)

	// Verify pong payload was queued to control channel (not written directly to conn)
	select {
	case payload := <-client.control:
		if !bytes.Equal(payload, pingPayload) {
			t.Errorf("Pong payload mismatch: got %q, want %q", string(payload), string(pingPayload))
		}
	default:
		t.Error("Expected pong payload to be queued to control channel")
	}
}

func TestReadLoop_PongFrame_RefreshesDeadline(t *testing.T) {
	t.Parallel()
	// Create pong frame (simulating client responding to server ping)
	pongPayload := []byte{}
	pongFrame := createWebSocketFrame(ws.OpPong, pongPayload)

	// Add close frame to terminate
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})
	frameData := slices.Concat(pongFrame, closeFrame)

	mockConn := newTestMockConn()
	mockConn.setReadData(frameData)

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, nil, nil)

	// Verify deadline was set multiple times (initial + after pong + after close)
	deadlineCount := mockConn.getDeadlineCount()
	if deadlineCount < 2 {
		t.Errorf("Expected at least 2 deadline sets (initial + after pong), got %d", deadlineCount)
	}
}

func TestReadLoop_CloseFrame_ExitsGracefully(t *testing.T) {
	t.Parallel()
	// Create close frame
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})

	mockConn := newTestMockConn()
	mockConn.setReadData(closeFrame)

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	done := make(chan bool)
	go func() {
		ctx := context.Background()
		pump.ReadLoop(ctx, client, nil, nil)
		done <- true
	}()

	select {
	case <-done:
		// Good - ReadLoop exited
	case <-time.After(1 * time.Second):
		t.Error("ReadLoop did not exit on close frame")
	}
}

func TestReadLoop_ContextCancellation_ExitsWithServerShutdown(t *testing.T) {
	t.Parallel()
	// Block the read until Close(), modeling the production shutdown ordering: the server cancels
	// the context, the write loop closes the connection, and only THEN does the read unblock — so
	// the disconnect is deterministically attributed to the server (not a racing client read error).
	mockConn := newTestMockConn()
	mockConn.setReadError(io.EOF)     // returned after the blocking read unblocks on Close()
	mockConn.setBlockReadUntilClose() // Read blocks (no buffered data) until Close()

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	var disconnectReason string
	var initiatedBy string
	disconnectFn := func(_ *Client, reason, by string) {
		disconnectReason = reason
		initiatedBy = by
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.ReadLoop(ctx, client, disconnectFn, nil)
		done <- true
	}()

	// Barrier: wait until the read loop is INSIDE the blocking read, then cancel (server shutdown),
	// then close the conn (as the write loop would) to unblock it. This guarantees the read unblocks
	// AFTER cancellation, so the read-error re-check attributes the disconnect to the server — the
	// exact ordering the flaky version raced.
	<-mockConn.readEnteredCh()
	cancel()
	_ = mockConn.Close()

	select {
	case <-done:
		// Verify shutdown reason
		if disconnectReason != "server_shutdown" {
			t.Errorf("Expected server_shutdown reason, got %s", disconnectReason)
		}
		if initiatedBy != "server" {
			t.Errorf("Expected server initiated, got %s", initiatedBy)
		}
	case <-time.After(1 * time.Second):
		t.Error("ReadLoop did not exit on context cancellation")
	}
}

// TestReadLoop_PreCancelledContext_ExitsWithServerShutdown covers the loop-top ctx.Done() catch
// (pump.go:177-183): a context already canceled before the loop starts exits server-attributed via
// the loop-top select, before any read. Deterministic (no blocking mode) — preserves coverage of
// that path, which the read-entry barrier in TestReadLoop_ContextCancellation_ExitsWithServerShutdown
// deliberately routes around (that test now exercises the read-error re-check path).
func TestReadLoop_PreCancelledContext_ExitsWithServerShutdown(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockConn.setReadError(io.EOF)

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	var disconnectReason string
	var initiatedBy string
	disconnectFn := func(_ *Client, reason, by string) {
		disconnectReason = reason
		initiatedBy = by
	}

	// Cancel BEFORE ReadLoop starts → the loop-top ctx.Done() catch fires deterministically,
	// before any read is attempted.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool)
	go func() {
		pump.ReadLoop(ctx, client, disconnectFn, nil)
		done <- true
	}()

	select {
	case <-done:
		if disconnectReason != "server_shutdown" {
			t.Errorf("Expected server_shutdown reason, got %s", disconnectReason)
		}
		if initiatedBy != "server" {
			t.Errorf("Expected server initiated, got %s", initiatedBy)
		}
	case <-time.After(1 * time.Second):
		t.Error("ReadLoop did not exit on pre-canceled context")
	}
}

func TestReadLoop_ReadError_CallsDisconnectFn(t *testing.T) {
	t.Parallel()
	// Create connection that returns error
	mockConn := newTestMockConn()
	mockConn.setReadError(io.ErrUnexpectedEOF)

	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	var disconnectCalled bool
	var disconnectReason string
	disconnectFn := func(_ *Client, reason, _ string) {
		disconnectCalled = true
		disconnectReason = reason
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, disconnectFn, nil)

	if !disconnectCalled {
		t.Error("Expected disconnectFn to be called")
	}

	if disconnectReason != "read_error" {
		t.Errorf("Expected read_error reason, got %s", disconnectReason)
	}
}

func TestReadLoop_RateLimiting_BlocksExcessiveMessages(t *testing.T) {
	t.Parallel()
	// Create multiple text frames
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})
	textFrame := createWebSocketFrame(ws.OpText, []byte(`{"type":"heartbeat"}`))
	frameData := make([]byte, 0, len(textFrame)*3+len(closeFrame))
	for range 3 {
		frameData = append(frameData, textFrame...)
	}
	// Add close frame
	frameData = append(frameData, closeFrame...)

	mockConn := newTestMockConn()
	mockConn.setReadData(frameData)

	mockRateLimiter := newTestMockRateLimiter()
	mockRateLimiter.setAllowCount(1) // Only allow first message

	s := stats.NewStats()
	pump := &Pump{
		Config:      testPumpConfig(),
		Stats:       s,
		RateLimiter: mockRateLimiter,
		Logger:      newTestMockLogger(),
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	processedCount := 0
	handleMsgFn := func(_ *Client, _ []byte) {
		processedCount++
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, nil, handleMsgFn)

	// Only 1 message should have been processed (first one allowed by rate limiter)
	if processedCount != 1 {
		t.Errorf("Expected 1 processed message, got %d", processedCount)
	}

	// Rate limiter should have been called 3 times
	if mockRateLimiter.getCallCount() != 3 {
		t.Errorf("Expected 3 rate limiter calls, got %d", mockRateLimiter.getCallCount())
	}
}

func TestReadLoop_PingFrame_ControlChannelFull_DropsWithLog(t *testing.T) {
	t.Parallel()
	// Create 3 ping frames (control channel cap is 2, so 3rd should be dropped)
	pingFrame := createWebSocketFrame(ws.OpPing, []byte{})
	closeFrame := createWebSocketFrame(ws.OpClose, []byte{})
	frameData := slices.Concat(pingFrame, pingFrame, pingFrame, closeFrame)

	mockConn := newTestMockConn()
	mockConn.setReadData(frameData)

	mockLogger := newTestMockLogger()
	s := stats.NewStats()
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
	}

	client := &Client{
		id:            1,
		transport:     NewWebSocketTransport(mockConn),
		send:          make(chan OutgoingMsg, 10),
		control:       make(chan []byte, controlChannelSize),
		subscriptions: NewSubscriptionSet(),
		seqGen:        messaging.NewSequenceGenerator(),
	}

	ctx := context.Background()
	pump.ReadLoop(ctx, client, nil, nil)

	// Control channel should have exactly controlChannelSize pongs (rest dropped)
	channelCount := len(client.control)
	if channelCount != controlChannelSize {
		t.Errorf("Expected %d pongs in control channel, got %d", controlChannelSize, channelCount)
	}

	// Verify drop was logged
	messages := mockLogger.getMessages()
	foundDrop := false
	for _, msg := range messages {
		if msg.message == "Control channel full, dropped pong" {
			foundDrop = true
			break
		}
	}
	if !foundDrop {
		t.Error("Expected 'Control channel full, dropped pong' log message")
	}
}

// =============================================================================
// WriteLoop Tests
// =============================================================================

func TestWriteLoop_ControlChannel_SendsPong(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
		Clock:  mockClock,
	}

	pongPayload := []byte("pong-data")
	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Pre-load pong into control channel
	client.control <- pongPayload

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Give WriteLoop time to process the pong via priority select
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// Verify pong was written to conn
	writtenData := mockConn.getWrittenData()
	if len(writtenData) == 0 {
		t.Fatal("Expected pong to be written to conn")
	}

	// Parse the frame to verify it's a pong with correct payload
	reader := bytes.NewReader(writtenData)
	header, err := ws.ReadHeader(reader)
	if err != nil {
		t.Fatalf("Failed to read written header: %v", err)
	}

	if header.OpCode != ws.OpPong {
		t.Errorf("Expected OpPong, got %v", header.OpCode)
	}

	if header.Length != int64(len(pongPayload)) {
		t.Errorf("Expected payload length %d, got %d", len(pongPayload), header.Length)
	}
}

func TestWriteLoop_ControlChannel_SetsWriteDeadline(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Pre-load pong
	client.control <- []byte("pong")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// Verify SetWriteDeadline was called before pong write
	if mockConn.getWriteDeadlineCount() == 0 {
		t.Error("Expected SetWriteDeadline to be called before pong write")
	}
}

func TestWriteLoop_ControlChannel_WriteError_Returns(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockConn.setWriteErr(io.ErrClosedPipe)
	mockClock := newTestMockClock()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Pre-load pong that will fail to write
	client.control <- []byte("pong")

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	select {
	case <-done:
		// Good - WriteLoop exited due to write error
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop should exit on pong write error")
	}

	// Verify error was logged
	messages := mockLogger.getMessages()
	foundErr := false
	for _, msg := range messages {
		if msg.message == "Failed to send pong" {
			foundErr = true
			break
		}
	}
	if !foundErr {
		t.Error("Expected 'Failed to send pong' log message")
	}
}

func TestWriteLoop_PingTimer_SendsPing(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Trigger the ping timer
	time.Sleep(20 * time.Millisecond)
	mockClock.advance(pump.Config.PingPeriod)

	// Give WriteLoop time to process the tick
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// Verify ping was written
	writtenData := mockConn.getWrittenData()
	if len(writtenData) == 0 {
		t.Fatal("Expected ping to be written to conn")
	}

	// Parse the frame to verify it's a ping
	reader := bytes.NewReader(writtenData)
	header, err := ws.ReadHeader(reader)
	if err != nil {
		t.Fatalf("Failed to read written header: %v", err)
	}

	if header.OpCode != ws.OpPing {
		t.Errorf("Expected OpPing, got %v", header.OpCode)
	}
}

func TestWriteLoop_ContextCancellation_Exits(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Cancel immediately
	cancel()

	select {
	case <-done:
		// Good - WriteLoop exited
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit on context cancellation")
	}
}

func TestWriteLoop_SendChannel_WritesMessage(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Pre-load data message
	dataMsg := []byte(`{"type":"message","channel":"sukko.BTC.trade"}`)
	client.send <- RawMsg(dataMsg)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// Verify data message was written
	writtenData := mockConn.getWrittenData()
	if len(writtenData) == 0 {
		t.Fatal("Expected data message to be written to conn")
	}

	// Verify stats were updated
	if s.MessagesSent.Load() != 1 {
		t.Errorf("Expected MessagesSent=1, got %d", s.MessagesSent.Load())
	}
}

func TestWriteLoop_SendChannelClosed_SendsClose(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	mockLogger := newTestMockLogger()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Logger: mockLogger,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 10),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	done := make(chan bool)
	go func() {
		pump.WriteLoop(context.Background(), client)
		done <- true
	}()

	// Close send channel to signal shutdown
	time.Sleep(20 * time.Millisecond)
	close(client.send)

	select {
	case <-done:
		// Good - WriteLoop exited
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit on send channel close")
	}

	// Verify close frame was sent
	writtenData := mockConn.getWrittenData()
	if len(writtenData) == 0 {
		t.Fatal("Expected close frame to be written")
	}

	reader := bytes.NewReader(writtenData)
	header, err := ws.ReadHeader(reader)
	if err != nil {
		t.Fatalf("Failed to read written header: %v", err)
	}

	if header.OpCode != ws.OpClose {
		t.Errorf("Expected OpClose, got %v", header.OpCode)
	}
}

// =============================================================================
// WriteLoop gap notification tests
// =============================================================================

func TestWriteLoop_GapNotificationLowerPriorityThanSend(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	liveMsg := RawMsg([]byte(`{"type":"message","seq":1}`))
	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 4),
		gapChan:   make(chan GapNotification, 4),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Pre-fill both channels before WriteLoop starts.
	client.send <- liveMsg
	client.gapChan <- GapNotification{
		Channel: "test.ch", FromSeq: 1, ToSeq: 1,
		LastPos: "(1)-10", Ts: 1000,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Wait for WriteLoop to drain the live message then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// The first frame written should be the live message, not the gap.
	data := mockConn.getWrittenData()
	if len(data) == 0 {
		t.Fatal("expected at least one written message")
	}
	// Decode the first WebSocket frame payload.
	reader := bytes.NewReader(data)
	hdr, err := ws.ReadHeader(reader)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	payload := make([]byte, hdr.Length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if hdr.Masked {
		ws.Cipher(payload, hdr.Mask, 0)
	}
	var msg map[string]any
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if msg["type"] != "message" {
		t.Errorf("first frame type = %v, want message (live message must be delivered before gap)", msg["type"])
	}
}

func TestWriteLoop_GapCoalescing(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 4),
		gapChan:   make(chan GapNotification, 8),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Enqueue 3 notifications for the same channel.
	client.gapChan <- GapNotification{Channel: "BTC", FromSeq: 6, ToSeq: 6, LastPos: "(1)-100", Ts: 100}
	client.gapChan <- GapNotification{Channel: "BTC", FromSeq: 7, ToSeq: 7, LastPos: "(1)-101", Ts: 101}
	client.gapChan <- GapNotification{Channel: "BTC", FromSeq: 8, ToSeq: 8, LastPos: "(1)-102", Ts: 102}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Poll for the coalesced gap frame before canceling: the gap handler runs in the
	// Priority-3 select alongside ctx.Done(), so canceling first could race the write.
	waitForGapFrames(t, mockConn, 1, 2*time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// The gap notifications should have been coalesced into a single gap message on c.send,
	// which was then written. Look for a gap frame in written data.
	data := mockConn.getWrittenData()
	// Find the gap frame by parsing all frames.
	reader := bytes.NewReader(data)
	found := false
	for reader.Len() > 0 {
		hdr, err := ws.ReadHeader(reader)
		if err != nil {
			break
		}
		payload := make([]byte, hdr.Length)
		if _, _ = io.ReadFull(reader, payload); hdr.Masked {
			ws.Cipher(payload, hdr.Mask, 0)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg["type"] == MsgTypeGap {
			fromSeq := int64(msg["from_seq"].(float64))
			toSeq := int64(msg["to_seq"].(float64))
			if fromSeq == 6 && toSeq == 8 {
				found = true
			} else {
				t.Errorf("gap coalescing: want from_seq=6 to_seq=8, got from_seq=%d to_seq=%d", fromSeq, toSeq)
			}
		}
	}
	if !found {
		t.Error("expected a coalesced gap message with from_seq=6 to_seq=8 to be written")
	}
}

func TestWriteLoop_GapNilChannel(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 4),
		gapChan:   nil, // non-WebSocket transport or uninitialized
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Cancel quickly — just verify no panic.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}
}

func TestWriteLoop_GapNotificationDifferentChannels(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	mockClock := newTestMockClock()
	s := stats.NewStats()

	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  s,
		Clock:  mockClock,
	}

	client := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 8),
		gapChan:   make(chan GapNotification, 8),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}

	// Two different channels — should produce two separate gap messages (not coalesced).
	client.gapChan <- GapNotification{Channel: "BTC", FromSeq: 1, ToSeq: 1, LastPos: "(1)-10", Ts: 100}
	client.gapChan <- GapNotification{Channel: "ETH", FromSeq: 1, ToSeq: 1, LastPos: "(1)-20", Ts: 101}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		pump.WriteLoop(ctx, client)
		done <- true
	}()

	// Poll for both channels' gap frames before canceling (gap handler races ctx.Done()).
	waitForGapFrames(t, mockConn, 2, 2*time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("WriteLoop did not exit")
	}

	// Count gap frames for each channel in written output.
	data := mockConn.getWrittenData()
	reader := bytes.NewReader(data)
	gapChannels := map[string]int{}
	for reader.Len() > 0 {
		hdr, err := ws.ReadHeader(reader)
		if err != nil {
			break
		}
		payload := make([]byte, hdr.Length)
		if _, _ = io.ReadFull(reader, payload); hdr.Masked {
			ws.Cipher(payload, hdr.Mask, 0)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg["type"] == MsgTypeGap {
			if ch, ok := msg["channel"].(string); ok {
				gapChannels[ch]++
			}
		}
	}
	// Both channels should be delivered: cross-channel item is re-enqueued by the pump
	// instead of being dropped, so a subsequent loop iteration delivers the ETH notification.
	if gapChannels["BTC"] == 0 {
		t.Error("expected at least one gap message for BTC channel")
	}
	if gapChannels["ETH"] == 0 {
		t.Error("expected at least one gap message for ETH channel (re-enqueued after cross-channel coalescing)")
	}
}

func TestCoalesceGapNotifications_CrossChannelReturnsLostItem(t *testing.T) {
	t.Parallel()

	ch := make(chan GapNotification, 2)
	ch <- GapNotification{Channel: "ETH", FromSeq: 2, ToSeq: 2, LastPos: "(1)-200", Ts: 200}
	first := GapNotification{Channel: "BTC", FromSeq: 1, ToSeq: 1, LastPos: "(1)-100", Ts: 100}

	// The function must return the cross-channel item (not drop it internally).
	// The caller is responsible for re-enqueueing and counting drops if the buffer is full.
	merged, lost := coalesceGapNotifications(first, ch)

	if merged.Channel != "BTC" {
		t.Errorf("merged.Channel = %q, want BTC", merged.Channel)
	}
	if merged.FromSeq != 1 || merged.ToSeq != 1 {
		t.Errorf("merged seq range = [%d,%d], want [1,1]", merged.FromSeq, merged.ToSeq)
	}
	if lost == nil {
		t.Fatal("expected non-nil lost item for the cross-channel ETH notification")
	}
	if lost.Channel != "ETH" {
		t.Errorf("lost.Channel = %q, want ETH", lost.Channel)
	}
	if lost.FromSeq != 2 {
		t.Errorf("lost.FromSeq = %d, want 2", lost.FromSeq)
	}
}

func TestWriteLoop_GapNotificationDroppedWhenSendFull(t *testing.T) {
	t.Parallel()

	// c.send with capacity 0: the non-blocking send inside the gap handler always fires
	// the default case, triggering GapDropReasonSendFull.
	pump := &Pump{
		Config: testPumpConfig(),
		Stats:  stats.NewStats(),
	}
	client := &Client{
		id:        1,
		transport: &noopTransport{},
		send:      make(chan OutgoingMsg), // capacity 0 → non-blocking send always drops
		gapChan:   make(chan GapNotification, 4),
		control:   make(chan []byte, controlChannelSize),
		closeOnce: sync.Once{},
	}
	client.gapChan <- GapNotification{
		Channel: "BTC.trade", FromSeq: 1, ToSeq: 1, LastPos: "(1)-100", Ts: 100,
	}

	ctx, cancel := context.WithCancel(context.Background())
	before := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonSendFull))

	done := make(chan struct{})
	go func() {
		defer close(done)
		pump.WriteLoop(ctx, client)
	}()

	// Poll for the drop metric before canceling: the gap handler runs in the Priority-3
	// select alongside ctx.Done(), so canceling first could race the drop.
	dropDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(dropDeadline) {
		if testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonSendFull))-before >= 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteLoop did not exit after context cancellation")
	}

	after := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonSendFull))
	if after-before < 1 {
		t.Error("expected GapNotificationsDropped[send_full] to increment when c.send is full")
	}
}

// =============================================================================
// handleSendMsg unit tests
// =============================================================================

func TestHandleSendMsg_ChannelClosed(t *testing.T) {
	t.Parallel()
	p := &Pump{Config: testPumpConfig(), Stats: stats.NewStats()}
	c := &Client{
		id:        1,
		transport: &noopTransport{},
		send:      make(chan OutgoingMsg, 4),
	}
	// ok=false simulates closed send channel.
	if p.handleSendMsg(c, RawMsg([]byte(`{}`)), false) {
		t.Error("handleSendMsg with ok=false must return false (exit signal)")
	}
}

func TestHandleSendMsg_NilTransport(t *testing.T) {
	t.Parallel()
	p := &Pump{Config: testPumpConfig(), Stats: stats.NewStats()}
	c := &Client{
		id:        1,
		transport: nil, // disconnected mid-flight
		send:      make(chan OutgoingMsg, 4),
	}
	if p.handleSendMsg(c, RawMsg([]byte(`{}`)), true) {
		t.Error("handleSendMsg with nil transport must return false")
	}
}

func TestHandleSendMsg_SingleSendError_RecordsDrop(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	p := &Pump{Config: testPumpConfig(), Stats: s, Clock: newTestMockClock()}
	sendErr := errors.New("write timeout")
	c := &Client{
		id:        1,
		transport: &errTransport{sendErr: sendErr},
		send:      make(chan OutgoingMsg, 4),
	}
	if p.handleSendMsg(c, RawMsg([]byte(`{}`)), true) {
		t.Error("handleSendMsg must return false on send error")
	}
	_, total := s.GetDroppedBroadcastsByChannel()
	if total == 0 {
		t.Error("expected dropped broadcast count to be incremented after send error")
	}
}

func TestHandleSendMsg_BatchSend_MultipleMessages(t *testing.T) {
	t.Parallel()
	mockConn := newTestMockConn()
	s := stats.NewStats()
	p := &Pump{Config: testPumpConfig(), Stats: s, Clock: newTestMockClock()}
	c := &Client{
		id:        1,
		transport: NewWebSocketTransport(mockConn),
		send:      make(chan OutgoingMsg, 8),
	}
	// Pre-queue 3 additional messages so len(c.send) == 3 when handleSendMsg runs.
	for range 3 {
		c.send <- RawMsg([]byte(`{"type":"message"}`))
	}
	// Call with the first message; the function should enter batch mode.
	if !p.handleSendMsg(c, RawMsg([]byte(`{"type":"message"}`)), true) {
		t.Error("handleSendMsg must return true on successful batch send")
	}
	if s.MessagesSent.Load() < 4 {
		t.Errorf("expected at least 4 messages sent (1 first + 3 batch), got %d", s.MessagesSent.Load())
	}
}

func TestHandleSendMsg_BatchSendError_RecordsDrop(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	p := &Pump{Config: testPumpConfig(), Stats: s, Clock: newTestMockClock()}
	batchErr := errors.New("batch write timeout")
	c := &Client{
		id:        1,
		transport: &errTransport{sendBatchErr: batchErr},
		send:      make(chan OutgoingMsg, 8),
	}
	// Pre-queue 2 additional messages to trigger batch mode.
	c.send <- RawMsg([]byte(`{}`))
	c.send <- RawMsg([]byte(`{}`))
	if p.handleSendMsg(c, RawMsg([]byte(`{}`)), true) {
		t.Error("handleSendMsg must return false on batch send error")
	}
	_, total := s.GetDroppedBroadcastsByChannel()
	if total == 0 {
		t.Error("expected dropped broadcast count to be incremented after batch send error")
	}
}

// gapFramesFromConn parses all gap-type frames written to the mock connection.
func gapFramesFromConn(mockConn *testMockConn) []map[string]any {
	reader := bytes.NewReader(mockConn.getWrittenData())
	var frames []map[string]any
	for reader.Len() > 0 {
		hdr, err := ws.ReadHeader(reader)
		if err != nil {
			break
		}
		payload := make([]byte, hdr.Length)
		if _, _ = io.ReadFull(reader, payload); hdr.Masked {
			ws.Cipher(payload, hdr.Mask, 0)
		}
		var msg map[string]any
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg["type"] == MsgTypeGap {
			frames = append(frames, msg)
		}
	}
	return frames
}

// waitForGapFrames polls until at least n gap frames have been written by the WriteLoop
// goroutine, or fails after timeout. Replaces fixed sleeps that race the async gap handler.
func waitForGapFrames(t *testing.T, mockConn *testMockConn, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(gapFramesFromConn(mockConn)) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected >= %d gap frame(s) within %s, got %d", n, timeout, len(gapFramesFromConn(mockConn)))
}
