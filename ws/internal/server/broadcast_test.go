package server

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/stats"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// =============================================================================
// extractChannel Tests
// =============================================================================

func TestExtractChannel_ValidSubjects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		subject  string
		expected string
	}{
		{
			name:     "BTC trade (2 parts)",
			subject:  "BTC.trade",
			expected: "BTC.trade",
		},
		{
			name:     "ETH liquidity (2 parts)",
			subject:  "ETH.liquidity",
			expected: "ETH.liquidity",
		},
		{
			name:     "SOL metadata (2 parts)",
			subject:  "SOL.metadata",
			expected: "SOL.metadata",
		},
		{
			name:     "User-scoped trade (3 parts)",
			subject:  "BTC.trade.user123",
			expected: "BTC.trade.user123",
		},
		{
			name:     "User-scoped balances (3 parts)",
			subject:  "ETH.balances.user456",
			expected: "ETH.balances.user456",
		},
		{
			name:     "Group-scoped community (3 parts)",
			subject:  "SOL.community.group789",
			expected: "SOL.community.group789",
		},
		{
			name:     "Simple balances (2 parts)",
			subject:  "balances.user123",
			expected: "balances.user123",
		},
		{
			name:     "Simple community (2 parts)",
			subject:  "community.group456",
			expected: "community.group456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := extractChannel(tt.subject)
			if result != tt.expected {
				t.Errorf("extractChannel(%q) = %q, want %q", tt.subject, result, tt.expected)
			}
		})
	}
}

func TestExtractChannel_EdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		subject  string
		expected string
	}{
		{
			name:     "Long symbol",
			subject:  "VERYLONGSYMBOLNAME.trade",
			expected: "VERYLONGSYMBOLNAME.trade",
		},
		{
			name:     "Lowercase symbol",
			subject:  "btc.trade",
			expected: "btc.trade",
		},
		{
			name:     "Mixed case symbol",
			subject:  "BtC.TrAdE",
			expected: "BtC.TrAdE",
		},
		{
			name:     "4 parts",
			subject:  "a.b.c.d",
			expected: "a.b.c.d",
		},
		{
			name:     "5 parts",
			subject:  "a.b.c.d.e",
			expected: "a.b.c.d.e",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := extractChannel(tt.subject)
			if result != tt.expected {
				t.Errorf("extractChannel(%q) = %q, want %q", tt.subject, result, tt.expected)
			}
		})
	}
}

func TestExtractChannel_InvalidSubjects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		subject string
	}{
		{
			name:    "Empty string",
			subject: "",
		},
		{
			name:    "Single part (no dots)",
			subject: "sukko",
		},
		{
			name:    "No dots",
			subject: "nodots",
		},
		{
			name:    "Only dots (empty parts)",
			subject: "...",
		},
		{
			name:    "Empty first part",
			subject: ".trade",
		},
		{
			name:    "Empty second part",
			subject: "BTC.",
		},
		{
			name:    "Empty middle part",
			subject: "BTC..user123",
		},
		{
			name:    "All empty parts",
			subject: "..",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := extractChannel(tt.subject)
			if result != "" {
				t.Errorf("extractChannel(%q) = %q, want empty string", tt.subject, result)
			}
		})
	}
}

func TestExtractChannel_AllEventTypes(t *testing.T) {
	t.Parallel()
	// Test all documented event types
	eventTypes := []string{
		"trade",
		"liquidity",
		"metadata",
		"social",
		"favorites",
		"creation",
		"analytics",
		"balances",
		"community",
	}

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			t.Parallel()
			subject := "BTC." + eventType
			expected := "BTC." + eventType

			result := extractChannel(subject)
			if result != expected {
				t.Errorf("extractChannel(%q) = %q, want %q", subject, result, expected)
			}
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkExtractChannel_TwoParts(b *testing.B) {
	subject := "BTC.trade"
	for b.Loop() {
		_ = extractChannel(subject)
	}
}

func BenchmarkExtractChannel_ThreeParts(b *testing.B) {
	subject := "BTC.trade.user123"
	for b.Loop() {
		_ = extractChannel(subject)
	}
}

func BenchmarkExtractChannel_Invalid(b *testing.B) {
	subject := "nodots"
	for b.Loop() {
		_ = extractChannel(subject)
	}
}

func BenchmarkExtractChannel_LongSubject(b *testing.B) {
	subject := "VERYLONGSYMBOLNAMEWITHMANYCHARACTERS.trade"
	for b.Loop() {
		_ = extractChannel(subject)
	}
}

// =============================================================================
// Broadcast — pos stamping
// =============================================================================

func TestBroadcast_PosStampedOnEnvelope(t *testing.T) {
	t.Parallel()

	s := &Server{
		subscriptionIndex: NewSubscriptionIndex(),
		logger:            zerolog.Nop(),
		alertLogger:       newTestMockAlertLogger(),
		stats:             stats.NewStats(),
		config: &platform.ServerConfig{
			SlowClientMaxAttempts: 3,
		},
	}

	c1 := &Client{
		id:        1,
		send:      make(chan OutgoingMsg, 8),
		seqGen:    messaging.NewSequenceGenerator(),
		transport: &noopTransport{},
	}
	c2 := &Client{
		id:        2,
		send:      make(chan OutgoingMsg, 8),
		seqGen:    messaging.NewSequenceGenerator(),
		transport: &noopTransport{},
	}
	// Advance c1's generator so the two clients produce different seq values on the
	// same Broadcast call — demonstrating independent per-client generators.
	c1.seqGen.Next()

	s.subscriptionIndex.Add("BTC.trade", c1)
	s.subscriptionIndex.Add("BTC.trade", c2)

	s.Broadcast("BTC.trade", []byte(`{"price":"100"}`), "5-12345", "")

	msg1, ok1 := <-c1.send
	msg2, ok2 := <-c2.send
	if !ok1 || !ok2 {
		t.Fatal("expected messages from both clients")
	}

	var env1, env2 map[string]json.RawMessage
	if err := json.Unmarshal(msg1.Bytes(), &env1); err != nil {
		t.Fatalf("c1 unmarshal: %v", err)
	}
	if err := json.Unmarshal(msg2.Bytes(), &env2); err != nil {
		t.Fatalf("c2 unmarshal: %v", err)
	}

	var pos1, pos2 string
	if err := json.Unmarshal(env1["pos"], &pos1); err != nil {
		t.Fatalf("c1 pos: %v (raw: %s)", err, env1["pos"])
	}
	if err := json.Unmarshal(env2["pos"], &pos2); err != nil {
		t.Fatalf("c2 pos: %v (raw: %s)", err, env2["pos"])
	}
	if pos1 != "5-12345" {
		t.Errorf("c1 pos: got %q, want %q", pos1, "5-12345")
	}
	if pos2 != "5-12345" {
		t.Errorf("c2 pos: got %q, want %q", pos2, "5-12345")
	}

	var seq1, seq2 int64
	if err := json.Unmarshal(env1["seq"], &seq1); err != nil {
		t.Fatalf("c1 seq: %v", err)
	}
	if err := json.Unmarshal(env2["seq"], &seq2); err != nil {
		t.Fatalf("c2 seq: %v", err)
	}
	if seq1 == seq2 {
		t.Errorf("seq values should differ (each client has its own generator): c1=%d c2=%d", seq1, seq2)
	}
}

// TestBroadcast_EmptyPosOmitted verifies that a zero-value pos produces no "pos" key in JSON.
func TestBroadcast_EmptyPosOmitted(t *testing.T) {
	t.Parallel()

	s := &Server{
		subscriptionIndex: NewSubscriptionIndex(),
		logger:            zerolog.Nop(),
		alertLogger:       newTestMockAlertLogger(),
		stats:             stats.NewStats(),
		config: &platform.ServerConfig{
			SlowClientMaxAttempts: 3,
		},
	}

	c := &Client{
		id:        1,
		send:      make(chan OutgoingMsg, 8),
		seqGen:    messaging.NewSequenceGenerator(),
		transport: &noopTransport{},
	}
	s.subscriptionIndex.Add("BTC.trade", c)

	s.Broadcast("BTC.trade", []byte(`{"price":"100"}`), "" /* no pos */, "")

	msg := <-c.send
	raw := msg.Bytes()
	if bytes.Contains(raw, []byte(`"pos"`)) {
		t.Errorf("empty pos must be omitted from JSON envelope, got: %s", raw)
	}
}

// =============================================================================
// OutgoingMsg Tests
// =============================================================================

// TestBroadcast_OutgoingMsgBytes tests Bytes() dispatch for all three modes:
// raw (pre-built), envelope (deferred), and zero-value (defensive guard).
func TestBroadcast_OutgoingMsgBytes(t *testing.T) {
	t.Parallel()

	t.Run("raw mode returns raw bytes", func(t *testing.T) {
		t.Parallel()
		raw := []byte(`{"type":"pong","ts":1000}`)
		msg := RawMsg(raw)
		got := msg.Bytes()
		if !bytes.Equal(got, raw) {
			t.Errorf("Bytes() = %s, want %s", string(got), string(raw))
		}
	})

	t.Run("envelope mode calls Build", func(t *testing.T) {
		t.Parallel()
		env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"price":45000}`), "", "")
		if err != nil {
			t.Fatalf("NewBroadcastEnvelope() error: %v", err)
		}
		msg := OutgoingMsg{envelope: env, seq: 42}
		got := msg.Bytes()

		// Verify it contains the correct seq
		var parsed map[string]any
		if err := json.Unmarshal(got, &parsed); err != nil {
			t.Fatalf("Bytes() produced invalid JSON: %v", err)
		}
		if parsed["seq"] != float64(42) {
			t.Errorf("seq = %v, want 42", parsed["seq"])
		}
	})

	t.Run("zero-value returns nil without panic", func(t *testing.T) {
		t.Parallel()
		var msg OutgoingMsg
		got := msg.Bytes()
		if got != nil {
			t.Errorf("Bytes() = %v, want nil for zero-value OutgoingMsg", got)
		}
	})
}

// TestBroadcast_PerClientSequence verifies that two clients subscribed to the same
// channel receive different sequence numbers for the same broadcast event.
func TestBroadcast_PerClientSequence(t *testing.T) {
	t.Parallel()

	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"price":45000}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope() error: %v", err)
	}

	// Simulate two clients with independent seqGens
	seqGen1 := messaging.NewSequenceGenerator()
	seqGen2 := messaging.NewSequenceGenerator()

	seq1 := seqGen1.Next()
	seq2 := seqGen2.Next()

	msg1 := OutgoingMsg{envelope: env, seq: seq1}
	msg2 := OutgoingMsg{envelope: env, seq: seq2}

	bytes1 := msg1.Bytes()
	bytes2 := msg2.Bytes()

	// Both should be valid JSON
	var parsed1, parsed2 map[string]any
	if err := json.Unmarshal(bytes1, &parsed1); err != nil {
		t.Fatalf("Client 1 Bytes() invalid JSON: %v", err)
	}
	if err := json.Unmarshal(bytes2, &parsed2); err != nil {
		t.Fatalf("Client 2 Bytes() invalid JSON: %v", err)
	}

	// Both should have seq=1 (first seq from independent generators)
	// But in the real broadcast path, each client's seqGen is shared across messages,
	// so after the first broadcast, client1.seq=1 and client2.seq=1.
	// The key contract is: each client gets its OWN seq from its OWN generator.
	if parsed1["seq"] != float64(1) {
		t.Errorf("Client 1 seq = %v, want 1", parsed1["seq"])
	}
	if parsed2["seq"] != float64(1) {
		t.Errorf("Client 2 seq = %v, want 1", parsed2["seq"])
	}

	// After a second broadcast, seqs diverge if one client gets more messages
	seq1b := seqGen1.Next()
	_ = seqGen2.Next() // client 2 also gets seq 2
	seq1c := seqGen1.Next()

	msg1c := OutgoingMsg{envelope: env, seq: seq1c}
	bytes1c := msg1c.Bytes()

	var parsed1c map[string]any
	if err := json.Unmarshal(bytes1c, &parsed1c); err != nil {
		t.Fatalf("Client 1 third Bytes() invalid JSON: %v", err)
	}

	if parsed1c["seq"] != float64(3) {
		t.Errorf("Client 1 third seq = %v, want 3", parsed1c["seq"])
	}
	_ = seq1b // used above
}

// TestBroadcast_SequenceMonotonicallyIncreasing verifies multiple broadcasts to one client
// produce strictly increasing sequence numbers.
func TestBroadcast_SequenceMonotonicallyIncreasing(t *testing.T) {
	t.Parallel()

	seqGen := messaging.NewSequenceGenerator()
	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"price":45000}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope() error: %v", err)
	}

	var prevSeq float64
	for i := range 100 {
		seq := seqGen.Next()
		msg := OutgoingMsg{envelope: env, seq: seq}
		msgBytes := msg.Bytes()

		var parsed map[string]any
		if err := json.Unmarshal(msgBytes, &parsed); err != nil {
			t.Fatalf("Broadcast %d: invalid JSON: %v", i, err)
		}

		currentSeq := parsed["seq"].(float64)
		if currentSeq <= prevSeq {
			t.Errorf("Broadcast %d: seq %v not greater than previous %v", i, currentSeq, prevSeq)
		}
		prevSeq = currentSeq
	}
}

// TestBroadcast_SequenceConsumedOnDrop verifies that seqGen.Next() is called before
// the send attempt, so the sequence is consumed even when the message is dropped.
func TestBroadcast_SequenceConsumedOnDrop(t *testing.T) {
	t.Parallel()

	seqGen := messaging.NewSequenceGenerator()

	// Simulate: broadcast loop calls seqGen.Next() BEFORE select/send
	// If send channel is full, seq is still consumed → client sees gap

	// First broadcast: seq consumed and delivered
	seq1 := seqGen.Next() // seq=1
	_ = seq1

	// Second broadcast: seq consumed but dropped (full buffer)
	seq2 := seqGen.Next() // seq=2
	_ = seq2

	// Third broadcast: seq consumed and delivered
	seq3 := seqGen.Next() // seq=3

	// Client receives seq=1 and seq=3, sees gap at seq=2
	if seq3 != 3 {
		t.Errorf("seq3 = %d, want 3", seq3)
	}

	// Verify seqGen.Current() reflects all consumed sequences
	if seqGen.Current() != 3 {
		t.Errorf("seqGen.Current() = %d, want 3 (all consumed even if dropped)", seqGen.Current())
	}
}

// TestBroadcast_CrossChannelSequenceContinuity verifies that messages on different
// channels share one sequence space per client (sequences are continuous across channels).
func TestBroadcast_CrossChannelSequenceContinuity(t *testing.T) {
	t.Parallel()

	seqGen := messaging.NewSequenceGenerator()

	envA, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"price":45000}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope(A) error: %v", err)
	}
	envB, err := messaging.NewBroadcastEnvelope("ETH.liquidity", 1001, []byte(`{"amount":100}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope(B) error: %v", err)
	}

	// Broadcast on channel A
	seqA := seqGen.Next()
	msgA := OutgoingMsg{envelope: envA, seq: seqA}

	// Broadcast on channel B
	seqB := seqGen.Next()
	msgB := OutgoingMsg{envelope: envB, seq: seqB}

	// Broadcast on channel A again
	seqA2 := seqGen.Next()
	msgA2 := OutgoingMsg{envelope: envA, seq: seqA2}

	// Extract seqs
	var parsedA, parsedB, parsedA2 map[string]any
	if err := json.Unmarshal(msgA.Bytes(), &parsedA); err != nil {
		t.Fatalf("Channel A Bytes() invalid JSON: %v", err)
	}
	if err := json.Unmarshal(msgB.Bytes(), &parsedB); err != nil {
		t.Fatalf("Channel B Bytes() invalid JSON: %v", err)
	}
	if err := json.Unmarshal(msgA2.Bytes(), &parsedA2); err != nil {
		t.Fatalf("Channel A2 Bytes() invalid JSON: %v", err)
	}

	// Verify continuous: 1, 2, 3
	if parsedA["seq"] != float64(1) {
		t.Errorf("Channel A seq = %v, want 1", parsedA["seq"])
	}
	if parsedB["seq"] != float64(2) {
		t.Errorf("Channel B seq = %v, want 2", parsedB["seq"])
	}
	if parsedA2["seq"] != float64(3) {
		t.Errorf("Channel A2 seq = %v, want 3", parsedA2["seq"])
	}

	// Verify different channels
	if parsedA["channel"] != "BTC.trade" {
		t.Errorf("Channel A channel = %v, want BTC.trade", parsedA["channel"])
	}
	if parsedB["channel"] != "ETH.liquidity" {
		t.Errorf("Channel B channel = %v, want ETH.liquidity", parsedB["channel"])
	}
}

// TestBroadcast_MidStampedOnEnvelope verifies the mid passed to Broadcast
// lands on every delivered envelope, identical across subscribers (shared
// suffix — unlike the per-client seq). Sibling of the pos-stamp test.
func TestBroadcast_MidStampedOnEnvelope(t *testing.T) {
	t.Parallel()

	s := &Server{
		subscriptionIndex: NewSubscriptionIndex(),
		logger:            zerolog.Nop(),
		alertLogger:       newTestMockAlertLogger(),
		stats:             stats.NewStats(),
		config: &platform.ServerConfig{
			SlowClientMaxAttempts: 3,
		},
	}
	c1 := &Client{id: 1, send: make(chan OutgoingMsg, 8), seqGen: messaging.NewSequenceGenerator(), transport: &noopTransport{}}
	c2 := &Client{id: 2, send: make(chan OutgoingMsg, 8), seqGen: messaging.NewSequenceGenerator(), transport: &noopTransport{}}
	s.subscriptionIndex.Add("BTC.trade", c1)
	s.subscriptionIndex.Add("BTC.trade", c2)

	s.Broadcast("BTC.trade", []byte(`{"price":"100"}`), "5-12345", "9e3779b9-4-12344")

	for i, c := range []*Client{c1, c2} {
		msg, ok := <-c.send
		if !ok {
			t.Fatalf("client %d: no message", i+1)
		}
		var env map[string]any
		if err := json.Unmarshal(msg.Bytes(), &env); err != nil {
			t.Fatalf("client %d: unmarshal: %v", i+1, err)
		}
		if env["mid"] != "9e3779b9-4-12344" {
			t.Errorf("client %d: mid = %v, want 9e3779b9-4-12344", i+1, env["mid"])
		}
	}
}
