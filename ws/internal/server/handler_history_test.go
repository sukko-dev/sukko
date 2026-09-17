package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/history"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// =============================================================================
// Helpers
// =============================================================================

// newHistoryTestClient creates a minimal Client with all history-delivery fields
// properly initialized.
func newHistoryTestClient() (*Client, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		id:                1,
		send:              make(chan OutgoingMsg, 64),
		seqGen:            messaging.NewSequenceGenerator(),
		subscriptions:     NewSubscriptionSet(),
		historyInProgress: make(map[string]struct{}),
		clientCtx:         ctx,
		clientCancel:      cancel,
		clientWg:          new(sync.WaitGroup),
	}
	return c, cancel
}

// newHistoryTestServer creates a minimal Server with history config and an optional writer.
func newHistoryTestServer(hw *history.Writer) *Server {
	return &Server{
		config: &platform.ServerConfig{
			BaseConfig:             platform.BaseConfig{Environment: "test"},
			HistoryEnabled:         hw != nil,
			HistoryMaxLimit:        100,
			HistoryDeliveryTimeout: 5 * time.Second,
			MaxChannelsPerClient:   100,
		},
		historyWriter: hw,
		historyEnv:    "test",
		logger:        zerolog.Nop(),
	}
}

// noopBus is a minimal broadcast.Bus that does nothing.
// Used to satisfy HistoryWriter construction in tests that don't exercise the bus.
type noopBus struct{}

func (b *noopBus) Subscribe(_ string) (<-chan *broadcast.Message, error) {
	ch := make(chan *broadcast.Message)
	return ch, nil
}
func (b *noopBus) SubscribeAll() (<-chan *broadcast.Message, error) {
	ch := make(chan *broadcast.Message)
	return ch, nil
}
func (b *noopBus) Unsubscribe(_ string, _ <-chan *broadcast.Message) error { return nil }
func (b *noopBus) UnsubscribeAll(_ <-chan *broadcast.Message) error        { return nil }
func (b *noopBus) Publish(_ *broadcast.Message) error                      { return nil }
func (b *noopBus) Run()                                                    {}
func (b *noopBus) Shutdown()                                               {}
func (b *noopBus) ShutdownWithContext(_ context.Context)                   {}
func (b *noopBus) IsHealthy() bool                                         { return true }
func (b *noopBus) GetMetrics() broadcast.Metrics                           { return broadcast.Metrics{} }

// startMiniredis starts a miniredis instance and a valkey-go client pointing at it.
// Both are cleaned up via t.Cleanup.
func startMiniredis(t *testing.T) valkey.Client {
	t.Helper()
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("miniredis start: %v", err)
	}
	t.Cleanup(mr.Close)
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("valkey.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// newTestHistoryWriter creates a HistoryWriter backed by miniredis (not started).
func newTestHistoryWriter(t *testing.T) *history.Writer {
	t.Helper()
	client := startMiniredis(t)
	cfg := &platform.ServerConfig{
		PodID:                              "test-pod",
		HistoryWriterLockTTLMs:             2000,
		HistoryWriterHeartbeatInterval:     50 * time.Millisecond,
		HistoryWriterRestartInitialBackoff: 10 * time.Millisecond,
		HistoryWriterRestartMaxBackoff:     100 * time.Millisecond,
		HistoryValkeyCmdTimeout:            500 * time.Millisecond,
		HistoryWriterBuffer:                256,
		HistoryWriterPipelineBatch:         50,
		HistoryMaxConsecutiveLockFailures:  3,
		HistoryBufferDepth:                 100,
		HistoryTTL:                         24 * time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return history.NewWriter(ctx, cfg, &noopBus{}, client, zerolog.Nop(), prometheus.NewRegistry(), "test")
}

// newTestHistoryWriterWithClient creates a HistoryWriter using an existing valkey client.
// Use this when the test needs direct stream access via the same client for seeding.
func newTestHistoryWriterWithClient(t *testing.T, client valkey.Client) *history.Writer {
	t.Helper()
	cfg := &platform.ServerConfig{
		PodID:                              "test-pod",
		HistoryWriterLockTTLMs:             2000,
		HistoryWriterHeartbeatInterval:     50 * time.Millisecond,
		HistoryWriterRestartInitialBackoff: 10 * time.Millisecond,
		HistoryWriterRestartMaxBackoff:     100 * time.Millisecond,
		HistoryValkeyCmdTimeout:            500 * time.Millisecond,
		HistoryWriterBuffer:                256,
		HistoryWriterPipelineBatch:         50,
		HistoryMaxConsecutiveLockFailures:  3,
		HistoryBufferDepth:                 100,
		HistoryTTL:                         24 * time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return history.NewWriter(ctx, cfg, &noopBus{}, client, zerolog.Nop(), prometheus.NewRegistry(), "test")
}

// readEnvelope reads the next message from c.send within 2 s and unmarshals it.
func readEnvelope(t *testing.T, c *Client) map[string]json.RawMessage {
	t.Helper()
	select {
	case msg := <-c.send:
		data := msg.Bytes()
		var env map[string]json.RawMessage
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}
		return env
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return nil
	}
}

func envString(t *testing.T, env map[string]json.RawMessage, key string) string {
	t.Helper()
	raw, ok := env[key]
	if !ok {
		t.Fatalf("key %q not in envelope", key)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("unmarshal %q: %v", key, err)
	}
	return s
}

// =============================================================================
// sendHistoryError / sendHistoryComplete helpers
// =============================================================================

func TestSendHistoryError_EnvelopeFields(t *testing.T) {
	t.Parallel()
	c, cancel := newHistoryTestClient()
	defer cancel()

	sendHistoryError(c, "sukko.BTC.trade", history.ErrCodeHistoryDisabled, "not enabled")

	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryError {
		t.Errorf("type: got %q, want %q", got, RespTypeHistoryError)
	}
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryDisabled {
		t.Errorf("code: got %q, want %q", got, history.ErrCodeHistoryDisabled)
	}
	if got := envString(t, env, "channel"); got != "sukko.BTC.trade" {
		t.Errorf("channel: got %q, want %q", got, "sukko.BTC.trade")
	}
}

func TestSendHistoryComplete_EnvelopeFields(t *testing.T) {
	t.Parallel()
	c, cancel := newHistoryTestClient()
	defer cancel()

	sendHistoryComplete(c, "sukko.BTC.trade", 5, history.HistorySourceCache, false)

	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryComplete {
		t.Errorf("type: got %q, want %q", got, RespTypeHistoryComplete)
	}
	if got := envString(t, env, "channel"); got != "sukko.BTC.trade" {
		t.Errorf("channel: got %q, want %q", got, "sukko.BTC.trade")
	}
	if got := envString(t, env, "source"); got != history.HistorySourceCache {
		t.Errorf("source: got %q, want %q", got, history.HistorySourceCache)
	}
}

// =============================================================================
// handleHistoryRequest — validation paths (a)–(f) and in-progress guard
// =============================================================================

func TestHandleHistoryRequest_HistoryDisabled(t *testing.T) {
	t.Parallel()
	s := newHistoryTestServer(nil) // historyWriter=nil → disabled
	s.config.HistoryEnabled = false
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	env := readEnvelope(t, c)
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryDisabled {
		t.Errorf("code: got %q, want %q", got, history.ErrCodeHistoryDisabled)
	}
}

func TestHandleHistoryRequest_HistoryWriterNil(t *testing.T) {
	t.Parallel()
	// HistoryEnabled=true but historyWriter=nil (e.g., startup failed to create it).
	s := &Server{
		config: &platform.ServerConfig{
			BaseConfig:      platform.BaseConfig{Environment: "test"},
			HistoryEnabled:  true,
			HistoryMaxLimit: 100,
		},
		historyWriter: nil,
		logger:        zerolog.Nop(),
	}
	c, cancel := newHistoryTestClient()
	defer cancel()

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	env := readEnvelope(t, c)
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryDisabled {
		t.Errorf("code: got %q, want %q", got, history.ErrCodeHistoryDisabled)
	}
}

func TestHandleHistoryRequest_CommunityEditionAllowed(t *testing.T) {
	t.Parallel()
	// MessageHistory is available on every edition (ADR-0009: the data path is
	// Community; capacity caps are the tier wall) — no edition gate on this path.
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	// Wait for the async delivery goroutine.
	c.clientWg.Wait()

	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryComplete {
		t.Fatalf("type: got %q, want %q (Community must reach history delivery, not an edition gate)", got, RespTypeHistoryComplete)
	}
}

func TestHandleHistoryRequest_NotSubscribed(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	// Deliberately do NOT add the channel.

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	env := readEnvelope(t, c)
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryNotSubscribed {
		t.Errorf("code: got %q, want %q", got, history.ErrCodeHistoryNotSubscribed)
	}
}

func TestHandleHistoryRequest_InvalidLimit(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)

	tests := []struct {
		name  string
		limit int
	}{
		{"zero", 0},
		{"negative", -1},
		{"above_max", 101}, // exceeds HistoryMaxLimit of 100
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, cancel := newHistoryTestClient()
			defer cancel()
			c.subscriptions.Add("sukko.BTC.trade")

			s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: tt.limit})

			env := readEnvelope(t, c)
			if got := envString(t, env, "code"); got != history.ErrCodeHistoryInvalidLimit {
				t.Errorf("limit=%d: code: got %q, want %q", tt.limit, got, history.ErrCodeHistoryInvalidLimit)
			}
		})
	}
}

func TestHandleHistoryRequest_InvalidChannelFormat(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)

	tests := []struct {
		name    string
		channel string
	}{
		{"no_dot", "nodotchannel"},
		{"trailing_dot", "sukko."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, cancel := newHistoryTestClient()
			defer cancel()
			c.subscriptions.Add(tt.channel)

			s.handleHistoryRequest(c, HistoryRequest{Channel: tt.channel, Limit: 10})

			env := readEnvelope(t, c)
			if got := envString(t, env, "code"); got != history.ErrCodeHistoryInvalidChannel {
				t.Errorf("channel=%q: code: got %q, want %q", tt.channel, got, history.ErrCodeHistoryInvalidChannel)
			}
		})
	}
}

func TestHandleHistoryRequest_InProgress(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	// Manually mark the channel as in-progress.
	c.historyMu.Lock()
	c.historyInProgress["sukko.BTC.trade"] = struct{}{}
	c.historyMu.Unlock()

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	env := readEnvelope(t, c)
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryInProgress {
		t.Errorf("code: got %q, want %q", got, history.ErrCodeHistoryInProgress)
	}
}

// =============================================================================
// handleHistoryRequest — successful delivery (empty stream)
// =============================================================================

func TestHandleHistoryRequest_EmptyStream_DeliversCompleteWithZeroCount(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	// Wait for the async delivery goroutine.
	c.clientWg.Wait()

	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryComplete {
		t.Fatalf("type: got %q, want %q", got, RespTypeHistoryComplete)
	}
	var count int
	if err := json.Unmarshal(env["count"], &count); err != nil {
		t.Fatalf("unmarshal count: %v", err)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
}

// =============================================================================
// handleHistoryRequest — async delivery error paths
// =============================================================================

func TestHandleHistoryRequest_StorageUnavailable_SendsHistoryError(t *testing.T) {
	t.Parallel()
	hw := newTestHistoryWriter(t)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	// Close the underlying Valkey client so XREVRANGE fails.
	hw.ValkeyClient().Close()

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})
	c.clientWg.Wait()

	env := readEnvelope(t, c)
	if got := envString(t, env, "type"); got != RespTypeHistoryError {
		t.Fatalf("type = %q, want %q", got, RespTypeHistoryError)
	}
	if got := envString(t, env, "code"); got != history.ErrCodeHistoryStorageUnavailable {
		t.Errorf("code = %q, want %q", got, history.ErrCodeHistoryStorageUnavailable)
	}
}

func TestHandleHistoryRequest_PumpFull_NoHang(t *testing.T) {
	t.Parallel()
	valkeyClient := startMiniredis(t)

	// Seed 5 entries.
	ctx := context.Background()
	streamKey := history.StreamKey("test", "sukko", "BTC.trade")
	for i := range 5 {
		p := fmt.Sprintf(`{"seq":%d}`, i+1)
		if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
			Id("*").FieldValue().
			FieldValue(history.HistoryFieldPayload, p).
			FieldValue(history.HistoryFieldTenantID, "sukko").
			FieldValue(history.HistoryFieldChannel, "BTC.trade").
			FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
			Build()).Error(); err != nil {
			t.Fatalf("XADD: %v", err)
		}
	}

	hw := newTestHistoryWriterWithClient(t, valkeyClient)
	s := newHistoryTestServer(hw)

	// Pre-fill the 1-slot buffer so every non-blocking send in the delivery loop is dropped.
	cCtx, cCancel := context.WithCancel(context.Background())
	defer cCancel()
	c := &Client{
		id:                1,
		send:              make(chan OutgoingMsg, 1),
		seqGen:            messaging.NewSequenceGenerator(),
		subscriptions:     NewSubscriptionSet(),
		historyInProgress: make(map[string]struct{}),
		clientCtx:         cCtx,
		clientCancel:      cCancel,
		clientWg:          new(sync.WaitGroup),
	}
	c.send <- RawMsg([]byte(`{"type":"sentinel"}`)) // fill the slot
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 5})

	// The delivery goroutine must exit without blocking even though all sends were dropped.
	done := make(chan struct{})
	go func() { c.clientWg.Wait(); close(done) }()
	select {
	case <-done:
		// Pass: delivery completed without hanging when pump is full.
	case <-time.After(5 * time.Second):
		t.Fatal("delivery goroutine hung when pump is full")
	}
}

// =============================================================================
// handleHistoryRequest — successful delivery (cache hit)
// =============================================================================

// =============================================================================
// handleHistoryRequest — pos field in delivered messages
// =============================================================================

func TestHistoryDelivery_PosField(t *testing.T) {
	t.Parallel()

	t.Run("auto_coord_pos_absent", func(t *testing.T) {
		t.Parallel()
		valkeyClient := startMiniredis(t)
		ctx := context.Background()
		streamKey := history.StreamKey("test", "sukko", "BTC.trade")

		// Seed one entry with coord="auto" (no Kafka position).
		if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
			Id("*").FieldValue().
			FieldValue(history.HistoryFieldPayload, `{"seq":1}`).
			FieldValue(history.HistoryFieldTenantID, "sukko").
			FieldValue(history.HistoryFieldChannel, "BTC.trade").
			FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
			FieldValue(history.HistoryFieldCoord, history.HistoryCoordAuto).
			Build()).Error(); err != nil {
			t.Fatalf("XADD: %v", err)
		}

		hw := newTestHistoryWriterWithClient(t, valkeyClient)
		s := newHistoryTestServer(hw)
		c, cancel := newHistoryTestClient()
		defer cancel()
		c.subscriptions.Add("sukko.BTC.trade")

		s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})
		c.clientWg.Wait()

		var msgs []map[string]json.RawMessage
		for {
			select {
			case msg := <-c.send:
				var env map[string]json.RawMessage
				if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
					msgs = append(msgs, env)
				}
			default:
				goto donePosAbsent
			}
		}
	donePosAbsent:

		// Find the history message (type=message, history=true).
		var histMsg map[string]json.RawMessage
		for _, m := range msgs {
			var histFlag bool
			_ = json.Unmarshal(m["history"], &histFlag)
			if histFlag {
				histMsg = m
				break
			}
		}
		if histMsg == nil {
			t.Fatal("expected at least one history message")
		}
		if _, hasPosKey := histMsg["pos"]; hasPosKey {
			t.Errorf("auto-coord history message must not include pos key, got: %s", histMsg["pos"])
		}
	})

	t.Run("kafka_coord_pos_equals_entry_id", func(t *testing.T) {
		t.Parallel()
		valkeyClient := startMiniredis(t)
		ctx := context.Background()
		streamKey := history.StreamKey("test", "acme", "ETH.trade")
		kafkaPos := "3-100"

		// Seed one entry with an explicit Kafka pos as stream ID and coord.
		if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
			Id(kafkaPos).FieldValue().
			FieldValue(history.HistoryFieldPayload, `{"seq":1}`).
			FieldValue(history.HistoryFieldTenantID, "acme").
			FieldValue(history.HistoryFieldChannel, "ETH.trade").
			FieldValue(history.HistoryFieldSubject, "acme.ETH.trade").
			FieldValue(history.HistoryFieldCoord, kafkaPos).
			Build()).Error(); err != nil {
			t.Fatalf("XADD: %v", err)
		}

		hw := newTestHistoryWriterWithClient(t, valkeyClient)
		s := newHistoryTestServer(hw)
		c, cancel := newHistoryTestClient()
		defer cancel()
		c.subscriptions.Add("acme.ETH.trade")

		s.handleHistoryRequest(c, HistoryRequest{Channel: "acme.ETH.trade", Limit: 10})
		c.clientWg.Wait()

		var msgs []map[string]json.RawMessage
		for {
			select {
			case msg := <-c.send:
				var env map[string]json.RawMessage
				if err := json.Unmarshal(msg.Bytes(), &env); err == nil {
					msgs = append(msgs, env)
				}
			default:
				goto donePosPresent
			}
		}
	donePosPresent:

		var histMsg map[string]json.RawMessage
		for _, m := range msgs {
			var histFlag bool
			_ = json.Unmarshal(m["history"], &histFlag)
			if histFlag {
				histMsg = m
				break
			}
		}
		if histMsg == nil {
			t.Fatal("expected at least one history message")
		}

		rawPos, hasPosKey := histMsg["pos"]
		if !hasPosKey {
			t.Fatal("kafka-coord history message must include pos key")
		}
		var pos string
		if err := json.Unmarshal(rawPos, &pos); err != nil {
			t.Fatalf("unmarshal pos: %v", err)
		}
		if pos != kafkaPos {
			t.Errorf("pos = %q, want %q", pos, kafkaPos)
		}
	})
}

func TestHandleHistoryRequest_CacheHit_DeliversMessagesInOrder(t *testing.T) {
	t.Parallel()
	valkeyClient := startMiniredis(t)

	// Pre-seed the stream with 3 entries in order.
	ctx := context.Background()
	streamKey := history.StreamKey("test", "sukko", "BTC.trade")
	for _, payload := range []string{`{"seq":1}`, `{"seq":2}`, `{"seq":3}`} {
		if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
			Id("*").FieldValue().
			FieldValue(history.HistoryFieldPayload, payload).
			FieldValue(history.HistoryFieldTenantID, "sukko").
			FieldValue(history.HistoryFieldChannel, "BTC.trade").
			FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
			Build(),
		).Error(); err != nil {
			t.Fatalf("XADD: %v", err)
		}
	}

	hw := newTestHistoryWriterWithClient(t, valkeyClient)

	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})

	// Wait for the async delivery goroutine to finish before reading.
	c.clientWg.Wait()

	// Drain all messages without racing against the goroutine.
	var messages []map[string]json.RawMessage
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				goto done
			}
			var env map[string]json.RawMessage
			_ = json.Unmarshal(msg.Bytes(), &env)
			messages = append(messages, env)
		default:
			goto done
		}
	}
done:

	// Expect 3 history messages + 1 history_complete.
	if len(messages) < 4 {
		t.Fatalf("expected at least 4 messages (3 history + complete), got %d", len(messages))
	}

	// First 3 must be history messages with history:true.
	for i, msg := range messages[:3] {
		if got := envString(t, msg, "type"); got != MsgTypeMessage {
			t.Errorf("msg[%d] type: got %q, want %q", i, got, MsgTypeMessage)
		}
		var histFlag bool
		_ = json.Unmarshal(msg["history"], &histFlag)
		if !histFlag {
			t.Errorf("msg[%d] history flag: got false, want true", i)
		}
	}

	// Last message must be history_complete with count=3.
	last := messages[len(messages)-1]
	if got := envString(t, last, "type"); got != RespTypeHistoryComplete {
		t.Errorf("last msg type: got %q, want %q", got, RespTypeHistoryComplete)
	}
	var count int
	_ = json.Unmarshal(last["count"], &count)
	if count != 3 {
		t.Errorf("count: got %d, want 3", count)
	}
}

// TestHandleHistoryRequest_MidRoundTrip verifies the stored mid is returned in
// the history envelope (cross-copy identity, ADR-0008), and that entries
// written before the mid field existed simply omit it.
func TestHandleHistoryRequest_MidRoundTrip(t *testing.T) {
	t.Parallel()
	valkeyClient := startMiniredis(t)

	ctx := context.Background()
	streamKey := history.StreamKey("test", "sukko", "BTC.trade")
	// Entry 1: carries a mid. Entry 2: legacy entry without one.
	if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
		Id("*").FieldValue().
		FieldValue(history.HistoryFieldPayload, `{"seq":1}`).
		FieldValue(history.HistoryFieldTenantID, "sukko").
		FieldValue(history.HistoryFieldChannel, "BTC.trade").
		FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
		FieldValue(history.HistoryFieldMid, "cafe1234-2-77").
		Build(),
	).Error(); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	if err := valkeyClient.Do(ctx, valkeyClient.B().Xadd().Key(streamKey).
		Id("*").FieldValue().
		FieldValue(history.HistoryFieldPayload, `{"seq":2}`).
		FieldValue(history.HistoryFieldTenantID, "sukko").
		FieldValue(history.HistoryFieldChannel, "BTC.trade").
		FieldValue(history.HistoryFieldSubject, "sukko.BTC.trade").
		Build(),
	).Error(); err != nil {
		t.Fatalf("XADD: %v", err)
	}

	hw := newTestHistoryWriterWithClient(t, valkeyClient)
	s := newHistoryTestServer(hw)
	c, cancel := newHistoryTestClient()
	defer cancel()
	c.subscriptions.Add("sukko.BTC.trade")

	s.handleHistoryRequest(c, HistoryRequest{Channel: "sukko.BTC.trade", Limit: 10})
	c.clientWg.Wait()

	var midded, midless int
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				goto done
			}
			var env map[string]json.RawMessage
			_ = json.Unmarshal(msg.Bytes(), &env)
			if string(env["type"]) != `"message"` {
				continue
			}
			if raw, present := env["mid"]; present {
				var mid string
				_ = json.Unmarshal(raw, &mid)
				if mid != "cafe1234-2-77" {
					t.Errorf("mid = %q, want cafe1234-2-77", mid)
				}
				midded++
			} else {
				midless++
			}
		default:
			goto done
		}
	}
done:
	if midded != 1 || midless != 1 {
		t.Fatalf("midded=%d midless=%d, want 1 and 1 (stored mid returned; legacy entry omits it)", midded, midless)
	}
}
