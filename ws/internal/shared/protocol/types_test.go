package protocol

import (
	"encoding/json"
	"testing"
)

// =============================================================================
// Message Type Constants Tests
// =============================================================================

func TestMessageTypeConstants_Values(t *testing.T) {
	t.Parallel()
	// Client→server message types (shared across gateway and server)
	if MsgTypeSubscribe != "subscribe" {
		t.Errorf("MsgTypeSubscribe = %q, want %q", MsgTypeSubscribe, "subscribe")
	}
	if MsgTypeUnsubscribe != "unsubscribe" {
		t.Errorf("MsgTypeUnsubscribe = %q, want %q", MsgTypeUnsubscribe, "unsubscribe")
	}
	if MsgTypePublish != "publish" {
		t.Errorf("MsgTypePublish = %q, want %q", MsgTypePublish, "publish")
	}
}

func TestResponseTypeConstants_Values(t *testing.T) {
	t.Parallel()
	// Only shared response types remain here; server-only types are in server/protocol.go.
	expectedTypes := map[string]string{
		"RespTypePublishAck":   RespTypePublishAck,
		"RespTypePublishError": RespTypePublishError,
	}

	expected := map[string]string{
		"RespTypePublishAck":   "publish_ack",
		"RespTypePublishError": "publish_error",
	}

	for name, actual := range expectedTypes {
		if actual != expected[name] {
			t.Errorf("%s = %q, want %q", name, actual, expected[name])
		}
	}
}

// =============================================================================
// ClientMessage Tests
// =============================================================================

func TestClientMessage_MarshalJSON(t *testing.T) {
	t.Parallel()
	msg := ClientMessage{
		Type: MsgTypeSubscribe,
		Data: json.RawMessage(`{"channels":["BTC.trade"]}`),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	expected := `{"type":"subscribe","data":{"channels":["BTC.trade"]}}`
	if string(data) != expected {
		t.Errorf("Marshal = %s, want %s", string(data), expected)
	}
}

func TestClientMessage_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(`{"type":"publish","data":{"channel":"BTC.trade","data":{"msg":"hello"}}}`)

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if msg.Type != MsgTypePublish {
		t.Errorf("Type = %q, want %q", msg.Type, MsgTypePublish)
	}
	if len(msg.Data) == 0 {
		t.Error("Data should not be empty")
	}
}

func TestClientMessage_JSONRoundTrip(t *testing.T) {
	t.Parallel()
	original := ClientMessage{
		Type: MsgTypeSubscribe,
		Data: json.RawMessage(`{"channels":["BTC.trade","ETH.orderbook"]}`),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded ClientMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if string(decoded.Data) != string(original.Data) {
		t.Errorf("Data = %s, want %s", string(decoded.Data), string(original.Data))
	}
}

func TestClientMessage_EmptyData(t *testing.T) {
	t.Parallel()
	msg := ClientMessage{
		Type: MsgTypeSubscribe,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Empty data should be omitted due to omitempty tag
	expected := `{"type":"subscribe"}`
	if string(data) != expected {
		t.Errorf("Marshal = %s, want %s", string(data), expected)
	}
}

func TestClientMessage_NullData(t *testing.T) {
	t.Parallel()
	data := []byte(`{"type":"subscribe","data":null}`)

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if msg.Type != MsgTypeSubscribe {
		t.Errorf("Type = %q, want %q", msg.Type, MsgTypeSubscribe)
	}
}

// =============================================================================
// SubscribeData Tests
// =============================================================================

func TestSubscribeData_MarshalJSON(t *testing.T) {
	t.Parallel()
	sub := SubscribeData{
		Channels: []string{"BTC.trade", "ETH.orderbook"},
	}

	data, err := json.Marshal(sub)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Parse check failed: %v", err)
	}

	channels, ok := parsed["channels"].([]any)
	if !ok {
		t.Fatal("channels field missing or wrong type")
	}
	if len(channels) != 2 {
		t.Errorf("len(channels) = %d, want 2", len(channels))
	}
}

func TestSubscribeData_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(`{"channels":["BTC.trade","ETH.orderbook","XRP.ticker"]}`)

	var sub SubscribeData
	if err := json.Unmarshal(data, &sub); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(sub.Channels) != 3 {
		t.Errorf("len(Channels) = %d, want 3", len(sub.Channels))
	}
	if sub.Channels[0] != "BTC.trade" {
		t.Errorf("Channels[0] = %q, want %q", sub.Channels[0], "BTC.trade")
	}
}

func TestSubscribeData_EmptyChannels(t *testing.T) {
	t.Parallel()
	sub := SubscribeData{
		Channels: []string{},
	}

	data, err := json.Marshal(sub)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded SubscribeData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(decoded.Channels) != 0 {
		t.Errorf("len(Channels) = %d, want 0", len(decoded.Channels))
	}
}

func TestSubscribeData_MultipleChannels(t *testing.T) {
	t.Parallel()
	channels := []string{
		"BTC.trade",
		"BTC.orderbook",
		"ETH.trade",
		"ETH.orderbook",
		"XRP.ticker",
	}
	sub := SubscribeData{Channels: channels}

	data, err := json.Marshal(sub)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded SubscribeData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(decoded.Channels) != len(channels) {
		t.Errorf("len(Channels) = %d, want %d", len(decoded.Channels), len(channels))
	}
	for i, ch := range channels {
		if decoded.Channels[i] != ch {
			t.Errorf("Channels[%d] = %q, want %q", i, decoded.Channels[i], ch)
		}
	}
}

// =============================================================================
// PublishData Tests
// =============================================================================

func TestPublishData_MarshalJSON(t *testing.T) {
	t.Parallel()
	pub := PublishData{
		Channel: "community.chat",
		Data:    json.RawMessage(`{"message":"Hello","sender":"user123"}`),
	}

	data, err := json.Marshal(pub)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Parse check failed: %v", err)
	}

	if parsed["channel"] != "community.chat" {
		t.Errorf("channel = %v, want %q", parsed["channel"], "community.chat")
	}
}

func TestPublishData_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(`{"channel":"game.events","data":{"event":"player_move","x":10,"y":20}}`)

	var pub PublishData
	if err := json.Unmarshal(data, &pub); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if pub.Channel != "game.events" {
		t.Errorf("Channel = %q, want %q", pub.Channel, "game.events")
	}
	if len(pub.Data) == 0 {
		t.Error("Data should not be empty")
	}
}

func TestPublishData_RawMessagePreserved(t *testing.T) {
	t.Parallel()
	// Test that the raw JSON is preserved exactly
	originalData := `{"nested":{"deep":{"value":123}},"array":[1,2,3]}`
	pub := PublishData{
		Channel: "test.channel",
		Data:    json.RawMessage(originalData),
	}

	data, err := json.Marshal(pub)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded PublishData
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(decoded.Data) != originalData {
		t.Errorf("Data = %s, want %s", string(decoded.Data), originalData)
	}
}

func TestPublishData_StringPayload(t *testing.T) {
	t.Parallel()
	// Test that string payloads work
	data := []byte(`{"channel":"test.events","data":"simple string message"}`)

	var pub PublishData
	if err := json.Unmarshal(data, &pub); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if pub.Channel != "test.events" {
		t.Errorf("Channel = %q, want %q", pub.Channel, "test.events")
	}
	if string(pub.Data) != `"simple string message"` {
		t.Errorf("Data = %s, want %q", string(pub.Data), `"simple string message"`)
	}
}

func TestPublishData_ArrayPayload(t *testing.T) {
	t.Parallel()
	// Test that array payloads work
	data := []byte(`{"channel":"batch.events","data":[1,2,3,"four"]}`)

	var pub PublishData
	if err := json.Unmarshal(data, &pub); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if string(pub.Data) != `[1,2,3,"four"]` {
		t.Errorf("Data = %s, want %s", string(pub.Data), `[1,2,3,"four"]`)
	}
}

func TestPublishData_NullPayload(t *testing.T) {
	t.Parallel()
	data := []byte(`{"channel":"test.channel","data":null}`)

	var pub PublishData
	if err := json.Unmarshal(data, &pub); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if pub.Channel != "test.channel" {
		t.Errorf("Channel = %q, want %q", pub.Channel, "test.channel")
	}
	// null is valid JSON
	if string(pub.Data) != "null" {
		t.Errorf("Data = %s, want %s", string(pub.Data), "null")
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkClientMessage_Marshal(b *testing.B) {
	msg := ClientMessage{
		Type: MsgTypeSubscribe,
		Data: json.RawMessage(`{"channels":["BTC.trade","ETH.orderbook"]}`),
	}

	for b.Loop() {
		_, _ = json.Marshal(msg)
	}
}

func BenchmarkClientMessage_Unmarshal(b *testing.B) {
	data := []byte(`{"type":"subscribe","data":{"channels":["BTC.trade","ETH.orderbook"]}}`)

	for b.Loop() {
		var msg ClientMessage
		_ = json.Unmarshal(data, &msg)
	}
}

func BenchmarkPublishData_Marshal(b *testing.B) {
	pub := PublishData{
		Channel: "community.chat",
		Data:    json.RawMessage(`{"message":"Hello world","sender":"user123","timestamp":1704067200}`),
	}

	for b.Loop() {
		_, _ = json.Marshal(pub)
	}
}

func BenchmarkPublishData_Unmarshal(b *testing.B) {
	data := []byte(`{"channel":"community.chat","data":{"message":"Hello world","sender":"user123","timestamp":1704067200}}`)

	for b.Loop() {
		var pub PublishData
		_ = json.Unmarshal(data, &pub)
	}
}
