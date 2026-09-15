package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
)

func TestConnect_InvalidURL(t *testing.T) {
	t.Parallel()

	_, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: "not-a-url",
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestConnect_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Connect(ctx, ConnectConfig{
		GatewayURL: "ws://localhost:59999",
		Logger:     zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestClient_CloseIdempotent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, err := wsutil.ReadClientText(conn); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("second close should be no-op, got: %v", err)
	}
}

func TestClient_Subscribe(t *testing.T) {
	t.Parallel()

	receivedCh := make(chan map[string]any, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		data, err := wsutil.ReadClientText(conn)
		if err != nil {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(data, &msg)
		receivedCh <- msg
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.Subscribe([]string{"ch.1", "ch.2"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	select {
	case received := <-receivedCh:
		if received["type"] != "subscribe" {
			t.Errorf("type = %v, want subscribe", received["type"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}
}

func TestClient_RefreshToken(t *testing.T) {
	t.Parallel()

	receivedCh := make(chan map[string]any, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		data, err := wsutil.ReadClientText(conn)
		if err != nil {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(data, &msg)
		receivedCh <- msg
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.RefreshToken("eyJnew.token.here"); err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}

	select {
	case received := <-receivedCh:
		if received["type"] != "auth" {
			t.Errorf("type = %v, want auth", received["type"])
		}
		data, ok := received["data"].(map[string]any)
		if !ok {
			t.Fatal("data is not a map")
		}
		if data["token"] != "eyJnew.token.here" {
			t.Errorf("token = %v, want eyJnew.token.here", data["token"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}
}

func TestClient_Publish(t *testing.T) {
	t.Parallel()

	receivedCh := make(chan map[string]any, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		data, err := wsutil.ReadClientText(conn)
		if err != nil {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(data, &msg)
		receivedCh <- msg
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.Publish("ch.1", json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case received := <-receivedCh:
		if received["type"] != "publish" {
			t.Errorf("type = %v, want publish", received["type"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}
}

func TestMessage_DecodeCode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		raw      string
		wantType string
		wantCode string
	}{
		{
			name:     "publish_error with rate_limited code",
			raw:      `{"type":"publish_error","code":"rate_limited","message":"too many"}`,
			wantType: "publish_error",
			wantCode: "rate_limited",
		},
		{
			name:     "publish_error with other code",
			raw:      `{"type":"publish_error","code":"invalid_channel"}`,
			wantType: "publish_error",
			wantCode: "invalid_channel",
		},
		{
			name:     "delivery envelope has no code",
			raw:      `{"type":"message","channel":"t.general.test","data":{"msg_id":"m1"}}`,
			wantType: "message",
			wantCode: "",
		},
		{
			name:     "absent code field decodes to empty string",
			raw:      `{"type":"subscribe_ack"}`,
			wantType: "subscribe_ack",
			wantCode: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var msg Message
			if err := json.Unmarshal([]byte(tc.raw), &msg); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if msg.Type != tc.wantType {
				t.Errorf("Type = %q, want %q", msg.Type, tc.wantType)
			}
			if msg.Code != tc.wantCode {
				t.Errorf("Code = %q, want %q", msg.Code, tc.wantCode)
			}
		})
	}
}

func TestClient_ReadLoop_OnMessage(t *testing.T) {
	t.Parallel()

	serverMsg := Message{
		Type:    "message",
		Channel: "test.ch",
		Data:    json.RawMessage(`{"x":1}`),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		payload, _ := json.Marshal(serverMsg)
		_ = wsutil.WriteServerText(conn, payload)

		// Keep connection open briefly to let ReadLoop process
		for {
			if _, err := wsutil.ReadClientText(conn); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	received := make(chan Message, 1)
	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
		OnMessage:  func(m Message) { received <- m },
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = client.ReadLoop(ctx) }()

	select {
	case msg := <-received:
		if msg.Type != "message" {
			t.Errorf("type = %q, want message", msg.Type)
		}
		if msg.Channel != "test.ch" {
			t.Errorf("channel = %q, want test.ch", msg.Channel)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	cancel()
	client.Close()
}

func TestClient_ReadLoop_CloseCode(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send close frame with code 1008 (Policy Violation)
		closeBody := ws.NewCloseFrameBody(ws.StatusPolicyViolation, "token revoked")
		frame := ws.NewCloseFrame(closeBody)
		if err := ws.WriteFrame(conn, frame); err != nil {
			return
		}
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	code, err := client.ReadLoop(context.Background())
	if err != nil {
		t.Fatalf("ReadLoop error: %v", err)
	}
	if code != ws.StatusPolicyViolation {
		t.Errorf("close code = %d, want %d (PolicyViolation)", code, ws.StatusPolicyViolation)
	}
}

func TestClient_ReadLoop_ContextCancel(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		// Keep connection open — never send anything
		select {}
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]
	client, err := Connect(context.Background(), ConnectConfig{
		GatewayURL: wsURL,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	var code ws.StatusCode
	var readErr error
	go func() {
		code, readErr = client.ReadLoop(ctx)
		close(done)
	}()

	// Cancel context — ReadLoop should return
	cancel()

	select {
	case <-done:
		if readErr != nil {
			t.Errorf("ReadLoop error: %v", readErr)
		}
		if code != 0 {
			t.Errorf("close code = %d, want 0 (context cancel)", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ReadLoop to return after context cancel")
	}
}

// wireShapeServer captures the first client frame as a decoded map.
func wireShapeServer(t *testing.T) (wsURL string, receivedCh chan map[string]any) {
	t.Helper()
	receivedCh = make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := ws.HTTPUpgrader{}
		conn, _, _, err := upgrader.Upgrade(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		data, err := wsutil.ReadClientText(conn)
		if err != nil {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(data, &msg)
		receivedCh <- msg
	}))
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[4:], receivedCh
}

// History must send {"type":"history","data":{"channel":...,"limit":...}} — the
// contract's client→server data-nesting (asyncapi history message).
func TestClient_History(t *testing.T) {
	t.Parallel()
	wsURL, receivedCh := wireShapeServer(t)
	client, err := Connect(context.Background(), ConnectConfig{GatewayURL: wsURL, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.History("t.chan", 25); err != nil {
		t.Fatalf("history: %v", err)
	}
	select {
	case got := <-receivedCh:
		if got["type"] != "history" {
			t.Errorf("type = %v, want history", got["type"])
		}
		data, _ := got["data"].(map[string]any)
		if data["channel"] != "t.chan" || data["limit"] != float64(25) {
			t.Errorf("data = %v, want {channel: t.chan, limit: 25}", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// Replay must send {"type":"replay","data":{"channel":...,"from_pos":...}} — the
// live gap-recovery request (asyncapi replay message).
func TestClient_Replay(t *testing.T) {
	t.Parallel()
	wsURL, receivedCh := wireShapeServer(t)
	client, err := Connect(context.Background(), ConnectConfig{GatewayURL: wsURL, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.Replay("t.chan", "3-99"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	select {
	case got := <-receivedCh:
		if got["type"] != "replay" {
			t.Errorf("type = %v, want replay", got["type"])
		}
		data, _ := got["data"].(map[string]any)
		if data["channel"] != "t.chan" || data["from_pos"] != "3-99" {
			t.Errorf("data = %v, want {channel: t.chan, from_pos: 3-99}", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// Reconnect must send {"type":"reconnect","data":{"client_id":...,"last_pos":{ch:pos}}}
// — last_pos is a PER-CHANNEL MAP (the server replays each channel from its own
// cursor; see the server's handlers_message reconnect tests), wrapped under "data"
// like every client→server frame.
func TestClient_Reconnect(t *testing.T) {
	t.Parallel()
	wsURL, receivedCh := wireShapeServer(t)
	client, err := Connect(context.Background(), ConnectConfig{GatewayURL: wsURL, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := client.Reconnect("cli-42", map[string]string{"t.chan": "3-99"}); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	select {
	case got := <-receivedCh:
		if got["type"] != "reconnect" {
			t.Errorf("type = %v, want reconnect", got["type"])
		}
		data, _ := got["data"].(map[string]any)
		if data["client_id"] != "cli-42" {
			t.Errorf("client_id = %v, want cli-42", data["client_id"])
		}
		lastPos, _ := data["last_pos"].(map[string]any)
		if lastPos["t.chan"] != "3-99" {
			t.Errorf("last_pos = %v, want {t.chan: 3-99}", lastPos)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// The pos cursor on message frames must survive decoding — it is the replay
// anchor both recovery legs depend on.
func TestMessage_PosDecodes(t *testing.T) {
	t.Parallel()
	var m Message
	if err := json.Unmarshal([]byte(`{"type":"message","channel":"t.c","pos":"3-100","mid":"m1"}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.Pos != "3-100" {
		t.Errorf("Pos = %q, want 3-100", m.Pos)
	}
}

// TestMessage_ReplayCompleteDecodes pins the server's FLAT replay_complete
// envelope (replay_protocol.go replayCompleteEnvelope): messages_replayed is a
// TOP-LEVEL field, not nested under data.
func TestMessage_ReplayCompleteDecodes(t *testing.T) {
	t.Parallel()
	var m Message
	if err := json.Unmarshal([]byte(`{"type":"replay_complete","channel":"t.c","messages_replayed":3}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.MessagesReplayed != 3 {
		t.Errorf("MessagesReplayed = %d, want 3", m.MessagesReplayed)
	}
}
