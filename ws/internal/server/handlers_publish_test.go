package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/protocol"

	"github.com/rs/zerolog"
)

// =============================================================================
// Channel Validation Tests
// =============================================================================

func TestIsValidPublishChannel_ValidFormats(t *testing.T) {
	t.Parallel()
	// Channel must have at least 2 parts in internal format: {tenant_id}.{suffix}
	// These are internal (mapped) channels, not client channels
	validChannels := []string{
		"tenant.community.chat",
		"acme.group.123.message",
		"test.user.abc.notification",
		"org.app.feature.event",
		"a.b.c",                   // 3 parts (valid)
		"a.b.c.d.e.f",             // Many parts is fine
		"tenant.BTC.trade",        // Uppercase
		"acme.btc-usdt.orderbook", // Hyphen in part
		"test.user_123.settings",  // Underscore in part
		"tenant.v1.api.request",   // Version prefix
		"io.toniq.sukko.events",   // Reverse domain notation
	}

	for _, channel := range validChannels {
		t.Run(channel, func(t *testing.T) {
			t.Parallel()
			if !auth.IsValidInternalChannel(channel) {
				t.Errorf("auth.IsValidInternalChannel(%q) = false, want true", channel)
			}
		})
	}
}

func TestIsValidPublishChannel_InvalidFormats(t *testing.T) {
	t.Parallel()
	// Internal channels must have at least 2 parts: {tenant}.{suffix}
	invalidChannels := []struct {
		channel string
		reason  string
	}{
		{"", "empty string"},
		{"singletopic", "no dot separator"},
		{".chat", "empty first part"},
		{"community.", "empty last part"},
		{"community..chat", "empty middle part"},
		{"...", "all empty parts"},
		{"..", "two dots only"},
		{".", "single dot"},
		{"a.b.", "empty last part"},
		{".b.c", "empty first part"},
		{"a..c", "empty middle part"},
	}

	for _, tc := range invalidChannels {
		t.Run(tc.reason, func(t *testing.T) {
			t.Parallel()
			if auth.IsValidInternalChannel(tc.channel) {
				t.Errorf("auth.IsValidInternalChannel(%q) = true, want false (reason: %s)", tc.channel, tc.reason)
			}
		})
	}
}

func TestIsValidPublishChannel_EdgeCases(t *testing.T) {
	t.Parallel()
	// Internal channels require at least 2 parts: {tenant}.{suffix}
	testCases := []struct {
		channel string
		valid   bool
		desc    string
	}{
		{"a.b", true, "minimum 2 parts (tenant.suffix)"},
		{"a.b.c", true, "3 parts (tenant.suffix with sub-parts)"},
		{"ab.cd.ef", true, "two letter parts"},
		{"123.456.789", true, "numeric parts"},
		{"a-b.c-d.e-f", true, "hyphens allowed"},
		{"a_b.c_d.e_f", true, "underscores allowed"},
		{"A.B.C", true, "uppercase allowed"},
		{"aB.cD.eF", true, "mixed case allowed"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			result := auth.IsValidInternalChannel(tc.channel)
			if result != tc.valid {
				t.Errorf("auth.IsValidInternalChannel(%q) = %v, want %v", tc.channel, result, tc.valid)
			}
		})
	}
}

// =============================================================================
// Publish Request Parsing Tests
// =============================================================================

// parsePublishRequest parses the publish message data (mirrors handler logic)
func parsePublishRequest(data json.RawMessage) (channel string, payload json.RawMessage, err error) {
	var req struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(data, &req); err != nil {
		return "", nil, fmt.Errorf("unmarshal publish request: %w", err)
	}
	return req.Channel, req.Data, nil
}

func TestParsePublishRequest_Valid(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channel": "community.chat", "data": {"message": "Hello", "sender": "user123"}}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "community.chat" {
		t.Errorf("channel = %q, want %q", channel, "community.chat")
	}
	if len(payload) == 0 {
		t.Error("payload should not be empty")
	}
}

func TestParsePublishRequest_MinimalData(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channel": "a.b", "data": {}}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "a.b" {
		t.Errorf("channel = %q, want %q", channel, "a.b")
	}
	if string(payload) != "{}" {
		t.Errorf("payload = %q, want %q", string(payload), "{}")
	}
}

func TestParsePublishRequest_ComplexPayload(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{
		"channel": "game.events",
		"data": {
			"event": "player_action",
			"player_id": 12345,
			"action": {"type": "move", "x": 10, "y": 20},
			"timestamp": 1704067200000,
			"metadata": {"client_version": "1.2.3"}
		}
	}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "game.events" {
		t.Errorf("channel = %q, want %q", channel, "game.events")
	}

	// Verify payload can be parsed
	var payloadData map[string]any
	if err := json.Unmarshal(payload, &payloadData); err != nil {
		t.Errorf("Failed to parse payload: %v", err)
	}
	if payloadData["event"] != "player_action" {
		t.Errorf("payload.event = %v, want %q", payloadData["event"], "player_action")
	}
}

func TestParsePublishRequest_MissingChannel(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"data": {"message": "Hello"}}`)

	channel, _, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	// Missing channel defaults to empty string
	if channel != "" {
		t.Errorf("channel = %q, want empty string", channel)
	}
}

func TestParsePublishRequest_MissingData(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{"channel": "community.chat"}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "community.chat" {
		t.Errorf("channel = %q, want %q", channel, "community.chat")
	}
	// Missing data defaults to null
	if payload != nil && string(payload) != "null" && len(payload) != 0 {
		t.Errorf("payload = %q, want null or empty", string(payload))
	}
}

func TestParsePublishRequest_InvalidJSON(t *testing.T) {
	t.Parallel()
	data := json.RawMessage(`{invalid json}`)

	_, _, err := parsePublishRequest(data)

	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestParsePublishRequest_StringData(t *testing.T) {
	t.Parallel()
	// Data as string instead of object (valid JSON, client can send any JSON type)
	data := json.RawMessage(`{"channel": "test.events", "data": "simple string message"}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "test.events" {
		t.Errorf("channel = %q, want %q", channel, "test.events")
	}
	if string(payload) != `"simple string message"` {
		t.Errorf("payload = %q, want %q", string(payload), `"simple string message"`)
	}
}

func TestParsePublishRequest_ArrayData(t *testing.T) {
	t.Parallel()
	// Data as array (valid JSON)
	data := json.RawMessage(`{"channel": "batch.events", "data": [1, 2, 3, "four"]}`)

	channel, payload, err := parsePublishRequest(data)

	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "batch.events" {
		t.Errorf("channel = %q, want %q", channel, "batch.events")
	}
	if string(payload) != `[1, 2, 3, "four"]` {
		t.Errorf("payload = %q, want %q", string(payload), `[1, 2, 3, "four"]`)
	}
}

// =============================================================================
// Publish Ack Response Tests
// =============================================================================

func TestPublishAck_JSONFormat(t *testing.T) {
	t.Parallel()
	channel := "community.chat"

	ack := map[string]any{
		"type":    "publish_ack",
		"channel": channel,
		"status":  "accepted",
	}
	data, err := json.Marshal(ack)

	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Parse back
	var parsed struct {
		Type    string `json:"type"`
		Channel string `json:"channel"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.Type != "publish_ack" {
		t.Errorf("type = %q, want %q", parsed.Type, "publish_ack")
	}
	if parsed.Channel != channel {
		t.Errorf("channel = %q, want %q", parsed.Channel, channel)
	}
	if parsed.Status != "accepted" {
		t.Errorf("status = %q, want %q", parsed.Status, "accepted")
	}
}

// =============================================================================
// Publish Error Response Tests
// =============================================================================

func TestPublishError_JSONFormat(t *testing.T) {
	t.Parallel()
	code := "rate_limited"
	message := "Publish rate limit exceeded"

	errResp := map[string]any{
		"type":    "publish_error",
		"code":    code,
		"message": message,
	}
	data, err := json.Marshal(errResp)

	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Parse back
	var parsed struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.Type != "publish_error" {
		t.Errorf("type = %q, want %q", parsed.Type, "publish_error")
	}
	if parsed.Code != code {
		t.Errorf("code = %q, want %q", parsed.Code, code)
	}
	if parsed.Message != message {
		t.Errorf("message = %q, want %q", parsed.Message, message)
	}
}

func TestPublishError_ErrorCodes(t *testing.T) {
	t.Parallel()
	errorCodes := []struct {
		code    string
		message string
	}{
		{"not_available", "Publishing is not enabled on this server"},
		{"invalid_request", "Invalid publish request format"},
		{"invalid_channel", "Channel must have format: name.type (e.g., community.chat)"},
		{"message_too_large", "Message exceeds maximum size of 64KB"},
		{"rate_limited", "Publish rate limit exceeded"},
		{"publish_failed", "Failed to publish message"},
	}

	for _, tc := range errorCodes {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			errResp := map[string]any{
				"type":    "publish_error",
				"code":    tc.code,
				"message": tc.message,
			}
			data, err := json.Marshal(errResp)
			if err != nil {
				t.Fatalf("json.Marshal failed: %v", err)
			}

			var parsed struct {
				Type    string `json:"type"`
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}

			if parsed.Code != tc.code {
				t.Errorf("code = %q, want %q", parsed.Code, tc.code)
			}
		})
	}
}

// =============================================================================
// Full Publish Message Tests
// =============================================================================

func TestParseClientMessage_Publish(t *testing.T) {
	t.Parallel()
	msg := `{"type": "publish", "data": {"channel": "community.chat", "data": {"msg": "hello"}}}`

	msgType, msgData, err := parseClientMessage([]byte(msg))

	if err != nil {
		t.Fatalf("parseClientMessage failed: %v", err)
	}
	if msgType != "publish" {
		t.Errorf("msgType = %q, want %q", msgType, "publish")
	}
	if len(msgData) == 0 {
		t.Error("msgData should not be empty")
	}

	// Parse inner data
	channel, payload, err := parsePublishRequest(msgData)
	if err != nil {
		t.Fatalf("parsePublishRequest failed: %v", err)
	}
	if channel != "community.chat" {
		t.Errorf("channel = %q, want %q", channel, "community.chat")
	}
	if len(payload) == 0 {
		t.Error("payload should not be empty")
	}
}

// =============================================================================
// Message Size Tests
// =============================================================================

func TestDefaultMaxPublishSize_Constant(t *testing.T) {
	t.Parallel()
	expectedSize := 64 * 1024 // 64KB

	if protocol.DefaultMaxPublishSize != expectedSize {
		t.Errorf("protocol.DefaultMaxPublishSize = %d, want %d", protocol.DefaultMaxPublishSize, expectedSize)
	}
}

func TestMessageSize_UnderLimit(t *testing.T) {
	t.Parallel()
	// Create a message just under the limit
	smallPayload := make([]byte, 1024) // 1KB
	for i := range smallPayload {
		smallPayload[i] = 'a'
	}

	if len(smallPayload) > protocol.DefaultMaxPublishSize {
		t.Errorf("Small payload (%d bytes) exceeds max (%d bytes)", len(smallPayload), protocol.DefaultMaxPublishSize)
	}
}

func TestMessageSize_OverLimit(t *testing.T) {
	t.Parallel()
	// Create a message over the limit
	largePayload := make([]byte, protocol.DefaultMaxPublishSize+1)
	for i := range largePayload {
		largePayload[i] = 'a'
	}

	if len(largePayload) <= protocol.DefaultMaxPublishSize {
		t.Errorf("Large payload (%d bytes) should exceed max (%d bytes)", len(largePayload), protocol.DefaultMaxPublishSize)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkIsValidPublishChannel_Valid(b *testing.B) {
	channel := "tenant.group123.chat" // 3 parts - internal format

	for b.Loop() {
		_ = auth.IsValidInternalChannel(channel)
	}
}

func BenchmarkIsValidPublishChannel_Invalid(b *testing.B) {
	channel := "invalid"

	for b.Loop() {
		_ = auth.IsValidInternalChannel(channel)
	}
}

func BenchmarkParsePublishRequest(b *testing.B) {
	data := json.RawMessage(`{"channel": "community.chat", "data": {"message": "Hello world", "sender": "user123"}}`)

	for b.Loop() {
		_, _, _ = parsePublishRequest(data)
	}
}

func BenchmarkPublishAck_Marshal(b *testing.B) {
	ack := map[string]any{
		"type":    "publish_ack",
		"channel": "community.chat",
		"status":  "accepted",
	}

	for b.Loop() {
		_, _ = json.Marshal(ack)
	}
}

func BenchmarkPublishError_Marshal(b *testing.B) {
	errResp := map[string]any{
		"type":    "publish_error",
		"code":    "rate_limited",
		"message": "Publish rate limit exceeded",
	}

	for b.Loop() {
		_, _ = json.Marshal(errResp)
	}
}

// sendPublishAck must include the backend-assigned mid (ADR-0008: the
// publisher correlates/dedups against delivered copies) and omit the key for
// fan-out publishes that carry none.
func TestSendPublishAck_MidIncluded(t *testing.T) {
	t.Parallel()
	s := &Server{logger: zerolog.Nop()}
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.sendPublishAck(c, "acme.chat", "cafe42-1-7")

	select {
	case msg := <-c.send:
		var parsed map[string]any
		if err := json.Unmarshal(msg.Bytes(), &parsed); err != nil {
			t.Fatalf("unmarshal ack: %v", err)
		}
		if parsed["type"] != "publish_ack" || parsed["status"] != "accepted" {
			t.Fatalf("unexpected ack frame: %v", parsed)
		}
		if parsed["mid"] != "cafe42-1-7" {
			t.Errorf("ack mid = %v, want cafe42-1-7", parsed["mid"])
		}
	default:
		t.Fatal("no ack frame sent")
	}
}

func TestSendPublishAck_NoMidOmitted(t *testing.T) {
	t.Parallel()
	s := &Server{logger: zerolog.Nop()}
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.sendPublishAck(c, "acme.chat", "")

	select {
	case msg := <-c.send:
		var parsed map[string]any
		if err := json.Unmarshal(msg.Bytes(), &parsed); err != nil {
			t.Fatalf("unmarshal ack: %v", err)
		}
		if _, present := parsed["mid"]; present {
			t.Errorf("mid key present (%v), want omitted for fan-out publishes", parsed["mid"])
		}
	default:
		t.Fatal("no ack frame sent")
	}
}
