package server

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// newReplayTestServer builds a minimal Server for handleReplayRequest tests.
// Live gap recovery is available on all editions (ADR-0009) — no edition state needed.
// §XVIII: mirrors newReconnectTestServer in handlers_message_test.go.
func newReplayTestServer(t *testing.T, mb *mockBackend) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Server{
		backend:           mb,
		logger:            zerolog.Nop(),
		ctx:               ctx,
		stats:             stats.NewStats(),
		subscriptionIndex: NewSubscriptionIndex(),
		config: &platform.ServerConfig{
			MaxReplayMessages:       100,
			ReplayTimeout:           10 * time.Second,
			ReplayRateLimitInterval: 10 * time.Second,
		},
	}
}

// newReplayTestClient returns a fully-initialized Client for replay handler tests.
func newReplayTestClient(id int64) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)
	c := &Client{
		id:               id,
		send:             make(chan OutgoingMsg, 64),
		gapChan:          make(chan GapNotification, 8),
		seqGen:           messaging.NewSequenceGenerator(),
		subscriptions:    NewSubscriptionSet(),
		replayInProgress: make(map[string]struct{}),
		replayLastAt:     make(map[string]time.Time),
		clientCtx:        ctx,
		clientCancel:     cancel,
		clientWg:         wg,
	}
	return c
}

func replayJSON(t *testing.T, channel, fromPos string) []byte {
	t.Helper()
	b, _ := json.Marshal(liveReplayRequest{Channel: channel, FromPos: fromPos})
	return b
}

// drainSend collects all messages from c.send within a short timeout.
func drainReplaySend(c *Client, timeout time.Duration) []map[string]any {
	var out []map[string]any
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-c.send:
			var m map[string]any
			if err := json.Unmarshal(msg.Bytes(), &m); err == nil {
				out = append(out, m)
			}
		case <-deadline:
			return out
		}
	}
}

func TestHandleReplayRequest_ValidRequest(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{"acme.BTC.trade": "acme.BTC"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "acme.BTC.trade", Data: []byte(`{"p":"100"}`), Pos: "2-11"},
			{Subject: "acme.BTC.trade", Data: []byte(`{"p":"101"}`), Pos: "2-12"},
		},
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.subscriptions.Add("acme.BTC.trade")

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeCompleted))

	s.handleReplayRequest(c, replayJSON(t, "acme.BTC.trade", "2-10"))
	c.clientWg.Wait()

	msgs := drainReplaySend(c, 500*time.Millisecond)
	if len(msgs) < 3 {
		t.Fatalf("expected at least 3 messages (2 replay_message + 1 replay_complete), got %d", len(msgs))
	}
	// First two should be replay_message
	if msgs[0]["type"] != RespTypeReplayMessage {
		t.Errorf("msgs[0].type = %v, want %s", msgs[0]["type"], RespTypeReplayMessage)
	}
	if msgs[1]["type"] != RespTypeReplayMessage {
		t.Errorf("msgs[1].type = %v, want %s", msgs[1]["type"], RespTypeReplayMessage)
	}
	// Last should be replay_complete
	last := msgs[len(msgs)-1]
	if last["type"] != RespTypeReplayComplete {
		t.Errorf("last msg type = %v, want %s", last["type"], RespTypeReplayComplete)
	}
	if mr, ok := last["messages_replayed"].(float64); !ok || mr != 2 {
		t.Errorf("messages_replayed = %v, want 2", last["messages_replayed"])
	}
	if truncated, _ := last["truncated"].(bool); truncated {
		t.Error("replay_complete should not be truncated on success")
	}

	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeCompleted))
	if after-before < 1 {
		t.Error("expected LiveReplayOutcomeCompleted to increment")
	}
}

func TestHandleReplayRequest_NotSubscribed(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	// Do NOT subscribe

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedNotSubscribed))
	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedNotSubscribed))

	if after-before < 1 {
		t.Error("expected RejectedNotSubscribed to increment")
	}
	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "not_subscribed" {
			t.Errorf("code = %v, want not_subscribed", m["code"])
		}
	default:
		t.Fatal("expected error message on send channel")
	}
}

func TestHandleReplayRequest_InvalidFromPos(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "bad-pos-format"))

	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "invalid_request" {
			t.Errorf("code = %v, want invalid_request", m["code"])
		}
	default:
		t.Fatal("expected error message")
	}
}

func TestHandleReplayRequest_RateLimited(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")
	// Set a very recent replayLastAt so rate limit fires.
	c.replayLastAt["ch.A"] = time.Now()

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedRateLimited))
	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedRateLimited))

	if after-before < 1 {
		t.Error("expected RejectedRateLimited to increment")
	}
	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "replay_rate_limited" {
			t.Errorf("code = %v, want replay_rate_limited", m["code"])
		}
	default:
		t.Fatal("expected error message")
	}
}

func TestHandleReplayRequest_InProgress(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")
	// Mark in-progress manually.
	c.replayInProgress["ch.A"] = struct{}{}

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedInProgress))
	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedInProgress))

	if after-before < 1 {
		t.Error("expected RejectedInProgress to increment")
	}
	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "replay_in_progress" {
			t.Errorf("code = %v, want replay_in_progress", m["code"])
		}
	default:
		t.Fatal("expected error message")
	}
}

func TestHandleReplayRequest_NoBackend(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, nil)
	s.backend = nil
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))

	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "not_available" {
			t.Errorf("code = %v, want not_available", m["code"])
		}
	default:
		t.Fatal("expected error message")
	}
}

func TestHandleReplayRequest_NilOrEmptyReplay(t *testing.T) {
	t.Parallel()

	// nil, nil: covers DirectBackend no-op AND empty Kafka range
	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayMsgs:    nil,
		replayErr:     nil,
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	c.clientWg.Wait()

	msgs := drainReplaySend(c, 500*time.Millisecond)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message (replay_complete with 0), got %d", len(msgs))
	}
	if msgs[0]["type"] != RespTypeReplayComplete {
		t.Errorf("type = %v, want %s", msgs[0]["type"], RespTypeReplayComplete)
	}
	if mr, ok := msgs[0]["messages_replayed"].(float64); !ok || mr != 0 {
		t.Errorf("messages_replayed = %v, want 0", msgs[0]["messages_replayed"])
	}
}

func TestHandleReplayRequest_OffsetOutOfRange(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayErr:     kerr.OffsetOutOfRange,
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedOutOfRange))
	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	c.clientWg.Wait()
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeRejectedOutOfRange))

	if after-before < 1 {
		t.Error("expected RejectedOutOfRange to increment")
	}
	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != "offset_out_of_range" {
			t.Errorf("code = %v, want offset_out_of_range", m["code"])
		}
	default:
		t.Fatal("expected error message for OffsetOutOfRange")
	}
}

func TestHandleReplayRequest_ClientDisconnectDuringReplay(t *testing.T) {
	t.Parallel()

	// Three messages; c.send has capacity 1.
	// The goroutine sends the first message, then blocks on c.send <- msg2
	// while the client context is canceled — exercising the deliveryCtx.Done() path.
	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "ch.A", Data: []byte(`{"p":"1"}`), Pos: "1-10"},
			{Subject: "ch.A", Data: []byte(`{"p":"2"}`), Pos: "1-11"},
			{Subject: "ch.A", Data: []byte(`{"p":"3"}`), Pos: "1-12"},
		},
	}

	s := newReplayTestServer(t, mb)

	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)
	c := &Client{
		id:               1,
		send:             make(chan OutgoingMsg, 1), // capacity 1: fills after first msg, then blocks
		gapChan:          make(chan GapNotification, 8),
		seqGen:           messaging.NewSequenceGenerator(),
		subscriptions:    NewSubscriptionSet(),
		replayInProgress: make(map[string]struct{}),
		replayLastAt:     make(map[string]time.Time),
		clientCtx:        ctx,
		clientCancel:     cancel,
		clientWg:         wg,
	}
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))

	// Give the goroutine time to start and block on the second send.
	time.Sleep(20 * time.Millisecond)
	cancel() // simulate client disconnect mid-delivery

	done := make(chan struct{})
	go func() {
		c.clientWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("replay goroutine did not exit after client disconnect")
	}

	// Verify replayInProgress is cleared.
	c.historyMu.Lock()
	_, stillRunning := c.replayInProgress["ch.A"]
	c.historyMu.Unlock()
	if stillRunning {
		t.Error("replayInProgress should be cleared after goroutine exits")
	}
}

func TestHandleReplayRequest_ConcurrentRace(t *testing.T) {
	t.Parallel()
	// Tests that exactly 1 of N concurrent requests proceeds; others get replay_in_progress.
	//
	// blockCh holds the async replay goroutine inside backend.Replay until all N
	// handleReplayRequest calls have returned. Without this gate the async goroutine
	// can clear replayInProgress before peer goroutines reach the historyMu check,
	// allowing multiple requests to proceed and producing fewer than N-1 rejections.

	blockCh := make(chan struct{})
	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "ch.A", Data: []byte(`{}`), Pos: "1-11"},
		},
		blockCh: blockCh,
	}
	s := newReplayTestServer(t, mb)
	s.config.ReplayTimeout = 2 * time.Second
	s.config.ReplayRateLimitInterval = 0 // disable rate limit for this test

	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	const N = 8
	var wg sync.WaitGroup

	for range N {
		wg.Go(func() {
			s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
		})
	}
	// All N handlers have processed the replayInProgress check and returned.
	// Only now unblock the async goroutine so it cannot race the check above.
	wg.Wait()
	close(blockCh)
	c.clientWg.Wait()

	// Drain send channel and count in_progress rejections.
	rejected := 0
	for len(c.send) > 0 {
		msg := <-c.send
		var m map[string]any
		if err := json.Unmarshal(msg.Bytes(), &m); err == nil {
			if m["code"] == "replay_in_progress" {
				rejected++
			}
		}
	}

	// Exactly N-1 must be rejected: one goroutine wins the lock, the rest see the flag set.
	if rejected != N-1 {
		t.Errorf("expected exactly %d in_progress rejections, got %d", N-1, rejected)
	}
}

// =============================================================================
// sendReplayError / sendReplayComplete helper tests
// =============================================================================

func TestSendReplayError_EnvelopeFields(t *testing.T) {
	t.Parallel()

	c := newReplayTestClient(1)
	sendReplayError(c, "ch.X", protocol.ErrCodeNotSubscribed, "subscribe first")

	msg := <-c.send
	var m map[string]any
	if err := json.Unmarshal(msg.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["type"] != MsgTypeError {
		t.Errorf("type = %v, want %s", m["type"], MsgTypeError)
	}
	if m["channel"] != "ch.X" {
		t.Errorf("channel = %v, want ch.X", m["channel"])
	}
	if m["code"] != "not_subscribed" {
		t.Errorf("code = %v, want not_subscribed", m["code"])
	}
}

func TestSendReplayComplete_EnvelopeFields(t *testing.T) {
	t.Parallel()

	c := newReplayTestClient(1)
	sendReplayComplete(c, "ch.Y", 5, false)

	msg := <-c.send
	var m map[string]any
	if err := json.Unmarshal(msg.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["type"] != RespTypeReplayComplete {
		t.Errorf("type = %v, want %s", m["type"], RespTypeReplayComplete)
	}
	if m["channel"] != "ch.Y" {
		t.Errorf("channel = %v, want ch.Y", m["channel"])
	}
	if mr, _ := m["messages_replayed"].(float64); mr != 5 {
		t.Errorf("messages_replayed = %v, want 5", m["messages_replayed"])
	}
	// truncated should be absent (omitempty) when false.
	if _, present := m["truncated"]; present {
		t.Error("truncated should be omitted when false (omitempty)")
	}
}

func TestSendReplayComplete_TruncatedField(t *testing.T) {
	t.Parallel()

	c := newReplayTestClient(1)
	sendReplayComplete(c, "ch.Z", 2, true)

	msg := <-c.send
	var m map[string]any
	_ = json.Unmarshal(msg.Bytes(), &m)
	if truncated, _ := m["truncated"].(bool); !truncated {
		t.Error("truncated should be true when set")
	}
}

func TestHandleReplayRequest_CommunityEditionAllowed(t *testing.T) {
	t.Parallel()
	// LiveGapRecovery is available on every edition (ADR-0009) — replay must
	// reach the backend; there is no edition gate on this path.
	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "ch.A", Data: []byte(`{"p":"100"}`), Pos: "1-11"},
		},
	}
	s := newReplayTestServer(t, mb)

	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	c.clientWg.Wait()

	msgs := drainReplaySend(c, 500*time.Millisecond)
	if len(msgs) < 2 {
		t.Fatalf("expected replay_message + replay_complete on Community, got %d messages", len(msgs))
	}
	if msgs[0]["type"] != RespTypeReplayMessage {
		t.Errorf("msgs[0].type = %v, want %s", msgs[0]["type"], RespTypeReplayMessage)
	}
	last := msgs[len(msgs)-1]
	if last["type"] != RespTypeReplayComplete {
		t.Errorf("last msg type = %v, want %s", last["type"], RespTypeReplayComplete)
	}
}

func TestHandleReplayRequest_InvalidJSON(t *testing.T) {
	t.Parallel()

	s := newReplayTestServer(t, &mockBackend{})
	c := newReplayTestClient(1)

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed))
	s.handleReplayRequest(c, []byte("not valid json"))
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeFailed))

	if after-before < 1 {
		t.Error("expected LiveReplayOutcomeFailed to increment on invalid JSON")
	}
	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != string(protocol.ErrCodeInvalidRequest) {
			t.Errorf("code = %v, want %s", m["code"], protocol.ErrCodeInvalidRequest)
		}
	default:
		t.Fatal("expected error message on send channel")
	}
}

func TestHandleReplayRequest_ChannelTopicMiss(t *testing.T) {
	t.Parallel()

	// Backend is non-nil but has no mapping for the subscribed channel.
	mb := &mockBackend{channelTopics: map[string]string{}}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.subscriptions.Add("ch.A")

	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))

	select {
	case msg := <-c.send:
		var m map[string]any
		_ = json.Unmarshal(msg.Bytes(), &m)
		if m["code"] != string(protocol.ErrCodeNotAvailable) {
			t.Errorf("code = %v, want %s", m["code"], protocol.ErrCodeNotAvailable)
		}
	default:
		t.Fatal("expected error message for ChannelTopic miss")
	}

	c.historyMu.Lock()
	_, stillRunning := c.replayInProgress["ch.A"]
	c.historyMu.Unlock()
	if stillRunning {
		t.Error("replayInProgress should be cleared after ChannelTopic miss")
	}
}

func TestHandleReplayRequest_Timeout(t *testing.T) {
	t.Parallel()

	// send buffer of 1: first replay_message lands, second stalls until ReplayTimeout fires.
	mb := &mockBackend{
		channelTopics: map[string]string{"ch.A": "topic.A"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "ch.A", Data: []byte(`{}`), Pos: "1-10"},
			{Subject: "ch.A", Data: []byte(`{}`), Pos: "1-11"},
			{Subject: "ch.A", Data: []byte(`{}`), Pos: "1-12"},
		},
	}
	s := newReplayTestServer(t, mb)
	s.config.ReplayTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wg := new(sync.WaitGroup)
	c := &Client{
		id:               1,
		send:             make(chan OutgoingMsg, 1),
		gapChan:          make(chan GapNotification, 8),
		seqGen:           messaging.NewSequenceGenerator(),
		subscriptions:    NewSubscriptionSet(),
		replayInProgress: make(map[string]struct{}),
		replayLastAt:     make(map[string]time.Time),
		clientCtx:        ctx,
		clientCancel:     cancel,
		clientWg:         wg,
	}
	c.subscriptions.Add("ch.A")

	before := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeTimeout))
	s.handleReplayRequest(c, replayJSON(t, "ch.A", "1-10"))
	c.clientWg.Wait()
	after := testutil.ToFloat64(metrics.LiveReplayRequestsTotal.WithLabelValues(LiveReplayOutcomeTimeout))

	if after-before < 1 {
		t.Error("expected LiveReplayOutcomeTimeout to increment")
	}

	c.historyMu.Lock()
	_, stillRunning := c.replayInProgress["ch.A"]
	c.historyMu.Unlock()
	if stillRunning {
		t.Error("replayInProgress should be cleared after timeout")
	}
}

// TestHandleReplayRequest_MidCarriedThrough verifies the replayed envelope
// carries the backend-provided mid — the recomputed coordinate-derived
// identity that makes the replayed copy byte-equal in identity to the live
// copy (ADR-0008) — and that mid-less messages omit the field.
func TestHandleReplayRequest_MidCarriedThrough(t *testing.T) {
	t.Parallel()

	mb := &mockBackend{
		channelTopics: map[string]string{"acme.BTC.trade": "acme.BTC"},
		replayMsgs: []backend.ReplayMessage{
			{Subject: "acme.BTC.trade", Data: []byte(`{"p":"100"}`), Pos: "2-11", Mid: "feed42-1-10"},
			{Subject: "acme.BTC.trade", Data: []byte(`{"p":"101"}`), Pos: "2-12"},
		},
	}
	s := newReplayTestServer(t, mb)
	c := newReplayTestClient(1)
	c.subscriptions.Add("acme.BTC.trade")

	s.handleReplayRequest(c, replayJSON(t, "acme.BTC.trade", "2-10"))
	c.clientWg.Wait()

	msgs := drainReplaySend(c, 500*time.Millisecond)
	if len(msgs) < 3 {
		t.Fatalf("expected 2 replay_message + complete, got %d", len(msgs))
	}
	if got, ok := msgs[0]["mid"].(string); !ok || got != "feed42-1-10" {
		t.Errorf("msgs[0].mid = %v, want %q", msgs[0]["mid"], "feed42-1-10")
	}
	if _, present := msgs[1]["mid"]; present {
		t.Errorf("msgs[1].mid present (%v), want omitted for mid-less replay message", msgs[1]["mid"])
	}
}
