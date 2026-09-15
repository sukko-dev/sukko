package publisher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestDirectPublisher_InvalidURL(t *testing.T) {
	t.Parallel()

	_, err := NewDirectPublisher(context.Background(), "not-a-url", "")
	if err == nil {
		t.Fatal("expected error for invalid gateway URL")
	}
}

func TestDirectPublisher_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewDirectPublisher(ctx, "ws://localhost:59999", "")
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestDirectPublisher_CloseIdempotent(t *testing.T) {
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
	pub, err := NewDirectPublisher(context.Background(), wsURL, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := pub.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Errorf("second close should be no-op, got: %v", err)
	}
}

func TestDirectPublisher_PublishAfterClose(t *testing.T) {
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
	pub, err := NewDirectPublisher(context.Background(), wsURL, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	_ = pub.Close()
	err = pub.Publish(context.Background(), "test.channel", []byte(`{"test":true}`))
	if err == nil {
		t.Fatal("expected error publishing after close")
	}
}

func TestDirectPublisher_Publish(t *testing.T) {
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
	pub, err := NewDirectPublisher(context.Background(), wsURL, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer pub.Close()

	err = pub.Publish(context.Background(), "test.channel", []byte(`{"seq":1}`))
	if err != nil {
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

func TestDirectPublisher_ImplementsPublisher(t *testing.T) {
	t.Parallel()
	var p Publisher = &DirectPublisher{}
	_ = p
}
