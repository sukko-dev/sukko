package server

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

func newGapTestServer() *Server {
	return &Server{
		subscriptionIndex: NewSubscriptionIndex(),
		logger:            zerolog.Nop(),
		alertLogger:       newTestMockAlertLogger(),
		stats:             stats.NewStats(),
		config: &platform.ServerConfig{
			SlowClientMaxAttempts: 3,
			GapNotifyBufferSize:   8,
		},
	}
}

func newGapTestClient(id int64, sendBuf, gapBuf int) *Client {
	c := &Client{
		id:               id,
		send:             make(chan OutgoingMsg, sendBuf),
		gapChan:          make(chan GapNotification, gapBuf),
		seqGen:           messaging.NewSequenceGenerator(),
		transport:        &noopTransport{},
		subscriptions:    NewSubscriptionSet(),
		replayInProgress: make(map[string]struct{}),
		replayLastAt:     make(map[string]time.Time),
	}
	return c
}

func TestBroadcast_GapNotificationEnqueuedOnDrop(t *testing.T) {
	t.Parallel()

	s := newGapTestServer()
	c := newGapTestClient(1, 0, 8) // send buf = 0 → always drops
	s.subscriptionIndex.Add("BTC.trade", c)

	before := testutil.ToFloat64(metrics.GapNotificationsEnqueued)
	s.Broadcast("BTC.trade", []byte(`{"p":"1"}`), "2-100", "")
	after := testutil.ToFloat64(metrics.GapNotificationsEnqueued)

	if after-before < 1 {
		t.Error("expected GapNotificationsEnqueued to increment")
	}
	if len(c.gapChan) == 0 {
		t.Fatal("expected gap notification on client.gapChan")
	}

	gap := <-c.gapChan
	if gap.Channel != "BTC.trade" {
		t.Errorf("gap.Channel = %q, want %q", gap.Channel, "BTC.trade")
	}
	if gap.LastPos != "2-100" {
		t.Errorf("gap.LastPos = %q, want %q", gap.LastPos, "2-100")
	}
	if gap.FromSeq == 0 {
		t.Error("gap.FromSeq should be set (seq > 0)")
	}
	if gap.ToSeq != gap.FromSeq {
		t.Errorf("single drop: gap.ToSeq (%d) should equal gap.FromSeq (%d)", gap.ToSeq, gap.FromSeq)
	}
	if gap.Ts == 0 {
		t.Error("gap.Ts should be set")
	}
}

func TestBroadcast_GapNotificationDroppedWhenGapChanFull(t *testing.T) {
	t.Parallel()

	s := newGapTestServer()
	c := newGapTestClient(1, 0, 1) // send buf = 0, gap buf = 1
	// Fill gapChan to capacity
	c.gapChan <- GapNotification{Channel: "BTC.trade"}

	s.subscriptionIndex.Add("BTC.trade", c)

	before := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonBufferFull))
	s.Broadcast("BTC.trade", []byte(`{"p":"1"}`), "2-101", "")
	after := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonBufferFull))

	if after-before < 1 {
		t.Error("expected GapNotificationsDropped[buffer_full] to increment")
	}
	if len(c.gapChan) != 1 {
		t.Errorf("gapChan should still be at capacity (len 1), got %d", len(c.gapChan))
	}
}

func TestBroadcast_GapNotificationNilGapChan(t *testing.T) {
	t.Parallel()

	s := newGapTestServer()
	c := newGapTestClient(1, 0, 8)
	c.gapChan = nil // non-WebSocket transport simulation

	s.subscriptionIndex.Add("BTC.trade", c)

	// Must not panic
	s.Broadcast("BTC.trade", []byte(`{"p":"1"}`), "2-102", "")
}

func TestBroadcast_GapNotificationMetrics(t *testing.T) {
	t.Parallel()

	s := newGapTestServer()

	// Case 1: enqueue succeeds
	c1 := newGapTestClient(1, 0, 8)
	s.subscriptionIndex.Add("ch.A.enq", c1)
	beforeEnq := testutil.ToFloat64(metrics.GapNotificationsEnqueued)
	s.Broadcast("ch.A.enq", []byte(`{}`), "1-1", "")
	afterEnq := testutil.ToFloat64(metrics.GapNotificationsEnqueued)
	if afterEnq-beforeEnq < 1 {
		t.Error("enqueue: expected GapNotificationsEnqueued to increment")
	}

	// Case 2: enqueue fails (gapChan full)
	c2 := newGapTestClient(2, 0, 1)
	c2.gapChan <- GapNotification{} // pre-fill to capacity
	s.subscriptionIndex.Add("ch.B.drop", c2)
	beforeDrop := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonBufferFull))
	s.Broadcast("ch.B.drop", []byte(`{}`), "1-2", "")
	afterDrop := testutil.ToFloat64(metrics.GapNotificationsDropped.WithLabelValues(GapDropReasonBufferFull))
	if afterDrop-beforeDrop < 1 {
		t.Error("drop: expected GapNotificationsDropped[buffer_full] to increment")
	}
}
