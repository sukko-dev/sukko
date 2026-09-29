package runner

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/sse"
)

// TestParseSSEEnvelope pins the fields the SSE recovery suite reads from a delivered
// envelope: top-level channel/pos/mid (broadcast_envelope.go) and the nested
// .data.msg_id. A payload without a msg_id is not a delivery envelope → ok=false.
func TestParseSSEEnvelope(t *testing.T) {
	t.Parallel()

	// A real broadcast envelope as written to the SSE data: line, carrying mid+pos.
	realEnv := `{"type":"message","seq":2,"ts":1784671852908,"channel":"t.gen","data":{"msg_id":"m2","ts":1},"pos":"1-5","mid":"MID-2"}`

	tests := []struct {
		name    string
		data    string
		wantOK  bool
		wantEnv sseParsedEnvelope
	}{
		{
			name:    "real envelope with mid+pos",
			data:    realEnv,
			wantOK:  true,
			wantEnv: sseParsedEnvelope{Channel: "t.gen", Pos: "1-5", Mid: "MID-2", MsgID: "m2"},
		},
		{
			name:   "no msg_id → not a delivery envelope",
			data:   `{"type":"message","channel":"t.gen","pos":"1-5"}`,
			wantOK: false,
		},
		{name: "malformed json", data: `not json`, wantOK: false},
		{name: "empty", data: ``, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env, ok := parseSSEEnvelope(tt.data)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && env != tt.wantEnv {
				t.Errorf("env = %+v, want %+v", env, tt.wantEnv)
			}
		})
	}
}

// sseEnvelope builds a broadcast-shaped SSE envelope for the collect test.
func sseEnvelope(channel, msgID, pos, mid string) string {
	return fmt.Sprintf(`{"type":"message","seq":1,"ts":1,"channel":%q,"data":{"msg_id":%q},"pos":%q,"mid":%q}`,
		channel, msgID, pos, mid)
}

// TestCollectSSEByMsgID proves the collect helper reads until every wanted msg_id has
// arrived, returning all envelopes seen keyed by msg_id plus arrival order — including
// an earlier record (the exclusive-cursor anchor) that arrived before the wanted ones,
// which is exactly how the suite detects a wrongly re-delivered anchor.
func TestCollectSSEByMsgID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// m1 (anchor) arrives first, then the two wanted records, then noise.
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope("t.ch", "m1", "1-4", "MID-1"))
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope("t.ch", "m2", "1-5", "MID-2"))
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope("t.ch", "m3", "1-6", "MID-3"))
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope("t.ch", "m4", "1-7", "MID-4"))
	}))
	defer srv.Close()

	client, status, err := sse.Connect(context.Background(), sse.ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"t.ch"},
		Token:      "tok",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v (status=%d)", err, status)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen, order, err := collectSSEByMsgID(ctx, client, []string{"m2", "m3"})
	if err != nil {
		t.Fatalf("collectSSEByMsgID: %v", err)
	}

	// The anchor m1 arrived before the wanted records, so it is present in `seen`
	// (the suite's "no duplicate replay" check keys off exactly this).
	if _, ok := seen["m1"]; !ok {
		t.Errorf("expected anchor m1 in seen, got %v", order)
	}
	if seen["m2"].Mid != "MID-2" || seen["m3"].Mid != "MID-3" {
		t.Errorf("mids: m2=%q m3=%q, want MID-2/MID-3", seen["m2"].Mid, seen["m3"].Mid)
	}
	// m2 must precede m3 in arrival order.
	var i2, i3 = -1, -1
	for i, id := range order {
		if id == "m2" {
			i2 = i
		}
		if id == "m3" {
			i3 = i
		}
	}
	if i2 < 0 || i3 < 0 || i2 > i3 {
		t.Errorf("order = %v, want m2 before m3", order)
	}
}

// TestCollectSSEByMsgID_TimeoutOnMissing proves the helper returns the read error (not
// a false success) when a wanted record never arrives before ctx expires.
func TestCollectSSEByMsgID_TimeoutOnMissing(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", sseEnvelope("t.ch", "m2", "1-5", "MID-2"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // deliver m2 (and headers) so Connect returns; without this Connect itself blocks
		}
		// m3 never sent; hold the stream open so the reader blocks until ctx cancel.
		<-r.Context().Done()
	}))
	defer srv.Close()

	client, _, err := sse.Connect(context.Background(), sse.ConnectConfig{
		GatewayURL: srv.URL,
		Channels:   []string{"t.ch"},
		Token:      "tok",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, _, err := collectSSEByMsgID(ctx, client, []string{"m2", "m3"}); err == nil {
		t.Fatal("expected error when a wanted msg_id never arrives, got nil")
	}
}
