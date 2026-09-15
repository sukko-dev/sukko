package broadcast

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMessage_Fields(t *testing.T) {
	t.Parallel()

	msg := &Message{
		Subject: "BTC.trade",
		Payload: []byte(`{"price":"50000.00"}`),
	}

	if msg.Subject != "BTC.trade" {
		t.Errorf("Subject: got %s, want BTC.trade", msg.Subject)
	}
	if string(msg.Payload) != `{"price":"50000.00"}` {
		t.Errorf("Payload: got %s, want {\"price\":\"50000.00\"}", msg.Payload)
	}
}

func TestMetrics_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now()
	m := Metrics{
		Type:             "valkey",
		Healthy:          true,
		ChannelPrefix:    "ws.broadcast",
		Subscribers:      3,
		PublishErrors:    5,
		MessagesReceived: 1000,
		LastPublishAgo:   1.5,
		LastPublishTime:  now,
	}

	if m.Type != "valkey" {
		t.Errorf("Type: got %s, want valkey", m.Type)
	}
	if !m.Healthy {
		t.Error("Healthy: got false, want true")
	}
	if m.ChannelPrefix != "ws.broadcast" {
		t.Errorf("Channel: got %s, want ws.broadcast", m.ChannelPrefix)
	}
	if m.Subscribers != 3 {
		t.Errorf("Subscribers: got %d, want 3", m.Subscribers)
	}
	if m.PublishErrors != 5 {
		t.Errorf("PublishErrors: got %d, want 5", m.PublishErrors)
	}
	if m.MessagesReceived != 1000 {
		t.Errorf("MessagesReceived: got %d, want 1000", m.MessagesReceived)
	}
	if m.LastPublishAgo != 1.5 {
		t.Errorf("LastPublishAgo: got %f, want 1.5", m.LastPublishAgo)
	}
	if !m.LastPublishTime.Equal(now) {
		t.Errorf("LastPublishTime: got %v, want %v", m.LastPublishTime, now)
	}
}

func TestValkeyConfig_Fields(t *testing.T) {
	t.Parallel()

	cfg := ValkeyConfig{
		Addrs:      []string{"valkey-1:6379", "valkey-2:6379"},
		MasterName: "mymaster",
		Password:   "secret",
		DB:         1,
		Channel:    "custom.channel",
	}

	if len(cfg.Addrs) != 2 {
		t.Errorf("Addrs length: got %d, want 2", len(cfg.Addrs))
	}
	if cfg.MasterName != "mymaster" {
		t.Errorf("MasterName: got %s, want mymaster", cfg.MasterName)
	}
	if cfg.Password != "secret" {
		t.Errorf("Password: got %s, want secret", cfg.Password)
	}
	if cfg.DB != 1 {
		t.Errorf("DB: got %d, want 1", cfg.DB)
	}
	if cfg.Channel != "custom.channel" {
		t.Errorf("Channel: got %s, want custom.channel", cfg.Channel)
	}
}

// =============================================================================
// Message.Pos JSON round-trip
// =============================================================================

func TestBroadcastBus_PosRoundTrip(t *testing.T) {
	t.Parallel()

	// Pos field must survive a JSON marshal/unmarshal cycle (the Valkey bus encodes
	// messages as JSON before publishing and decodes on receive).
	msg := &Message{
		Subject: "BTC.trade",
		Payload: []byte(`{"price":"100"}`),
		Pos:     "3-125",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.Pos != "3-125" {
		t.Errorf("Pos: got %q, want %q", decoded.Pos, "3-125")
	}

	// Empty Pos must be omitted from JSON (omitempty semantics).
	emptyMsg := &Message{Subject: "BTC.trade", Payload: []byte(`{}`)}
	emptyData, err := json.Marshal(emptyMsg)
	if err != nil {
		t.Fatalf("json.Marshal empty: %v", err)
	}
	if bytes.Contains(emptyData, []byte(`"pos"`)) {
		t.Errorf("empty Pos must be omitted from JSON, got: %s", emptyData)
	}
}
