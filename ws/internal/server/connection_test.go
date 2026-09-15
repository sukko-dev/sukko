package server

import (
	"bytes"
	"math"
	"sync"
	"testing"

	"github.com/sukko-dev/sukko/internal/server/messaging"
)

// =============================================================================
// SubscriptionSet Tests
// =============================================================================

func TestNewSubscriptionSet(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	if set == nil {
		t.Fatal("NewSubscriptionSet should return non-nil")
	}
	if set.Count() != 0 {
		t.Errorf("New set should be empty, got count %d", set.Count())
	}
}

func TestSubscriptionSet_Add(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.Add("sukko.BTC.trade")

	if !set.Has("sukko.BTC.trade") {
		t.Error("Should have sukko.BTC.trade after Add")
	}
	if set.Count() != 1 {
		t.Errorf("Count should be 1, got %d", set.Count())
	}
}

func TestSubscriptionSet_Add_Duplicate(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.Add("sukko.BTC.trade")
	set.Add("sukko.BTC.trade") // Add same channel again

	if set.Count() != 1 {
		t.Errorf("Duplicate add should not increase count, got %d", set.Count())
	}
}

func TestSubscriptionSet_AddMultiple(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	channels := []string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"}
	set.AddMultiple(channels)

	if set.Count() != 3 {
		t.Errorf("Count should be 3, got %d", set.Count())
	}

	for _, ch := range channels {
		if !set.Has(ch) {
			t.Errorf("Should have %s after AddMultiple", ch)
		}
	}
}

func TestSubscriptionSet_Remove(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.Add("sukko.BTC.trade")
	set.Add("sukko.ETH.trade")
	set.Remove("sukko.BTC.trade")

	if set.Has("sukko.BTC.trade") {
		t.Error("Should not have sukko.BTC.trade after Remove")
	}
	if !set.Has("sukko.ETH.trade") {
		t.Error("Should still have sukko.ETH.trade")
	}
	if set.Count() != 1 {
		t.Errorf("Count should be 1 after remove, got %d", set.Count())
	}
}

func TestSubscriptionSet_Remove_NonExistent(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	// Should not panic
	set.Remove("nonexistent")

	if set.Count() != 0 {
		t.Error("Count should still be 0")
	}
}

func TestSubscriptionSet_RemoveMultiple(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.AddMultiple([]string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity", "sukko.DOGE.social"})
	set.RemoveMultiple([]string{"sukko.BTC.trade", "sukko.SOL.liquidity"})

	if set.Has("sukko.BTC.trade") || set.Has("sukko.SOL.liquidity") {
		t.Error("Removed channels should not exist")
	}
	if !set.Has("sukko.ETH.trade") || !set.Has("sukko.DOGE.social") {
		t.Error("Non-removed channels should still exist")
	}
	if set.Count() != 2 {
		t.Errorf("Count should be 2, got %d", set.Count())
	}
}

func TestSubscriptionSet_Has(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	if set.Has("sukko.BTC.trade") {
		t.Error("Empty set should not have any channel")
	}

	set.Add("sukko.BTC.trade")

	if !set.Has("sukko.BTC.trade") {
		t.Error("Should have sukko.BTC.trade after Add")
	}
	if set.Has("sukko.ETH.trade") {
		t.Error("Should not have sukko.ETH.trade (not added)")
	}
}

func TestSubscriptionSet_List(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.AddMultiple([]string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"})

	list := set.List()

	if len(list) != 3 {
		t.Errorf("List should have 3 items, got %d", len(list))
	}

	// Create a map to check presence (order not guaranteed)
	listMap := make(map[string]bool)
	for _, ch := range list {
		listMap[ch] = true
	}

	for _, expected := range []string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"} {
		if !listMap[expected] {
			t.Errorf("List should contain %s", expected)
		}
	}
}

func TestSubscriptionSet_List_ReturnsCopy(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()
	set.Add("sukko.BTC.trade")

	list := set.List()
	list[0] = "MODIFIED"

	// Original should not be affected
	if !set.Has("sukko.BTC.trade") {
		t.Error("Modifying list should not affect set")
	}
}

func TestSubscriptionSet_Clear(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()

	set.AddMultiple([]string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"})
	set.Clear()

	if set.Count() != 0 {
		t.Errorf("Count should be 0 after Clear, got %d", set.Count())
	}
	if set.Has("sukko.BTC.trade") {
		t.Error("Should not have any channel after Clear")
	}
}

func TestSubscriptionSet_ThreadSafety(t *testing.T) {
	t.Parallel()
	set := NewSubscriptionSet()
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent adds
	for i := range 10 {
		wg.Go(func() {
			for range iterations {
				set.Add("channel" + string(rune('A'+i)))
			}
		})
	}

	// Concurrent reads
	for range 10 {
		wg.Go(func() {
			for range iterations {
				_ = set.Count()
				_ = set.Has("channelA")
				_ = set.List()
			}
		})
	}

	wg.Wait()
	// Test passes if no race conditions or panics
}

// =============================================================================
// SubscriptionIndex Tests
// =============================================================================

func TestNewSubscriptionIndex(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()

	if idx == nil {
		t.Fatal("NewSubscriptionIndex should return non-nil")
	}
	if idx.Count("sukko.BTC.trade") != 0 {
		t.Error("New index should have no subscribers")
	}
}

func TestSubscriptionIndex_Add(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	idx.Add("sukko.BTC.trade", client)

	if idx.Count("sukko.BTC.trade") != 1 {
		t.Errorf("Count should be 1, got %d", idx.Count("sukko.BTC.trade"))
	}

	clients := idx.Get("sukko.BTC.trade")
	if len(clients) != 1 || clients[0] != client {
		t.Error("Get should return the added client")
	}
}

func TestSubscriptionIndex_Add_Duplicate(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	idx.Add("sukko.BTC.trade", client)
	idx.Add("sukko.BTC.trade", client) // Add same client again

	if idx.Count("sukko.BTC.trade") != 1 {
		t.Errorf("Duplicate add should not increase count, got %d", idx.Count("sukko.BTC.trade"))
	}
}

func TestSubscriptionIndex_Add_MultipleClients(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client1 := &Client{id: 1}
	client2 := &Client{id: 2}
	client3 := &Client{id: 3}

	idx.Add("sukko.BTC.trade", client1)
	idx.Add("sukko.BTC.trade", client2)
	idx.Add("sukko.BTC.trade", client3)

	if idx.Count("sukko.BTC.trade") != 3 {
		t.Errorf("Count should be 3, got %d", idx.Count("sukko.BTC.trade"))
	}
}

func TestSubscriptionIndex_AddMultiple(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	channels := []string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"}
	idx.AddMultiple(channels, client)

	for _, ch := range channels {
		if idx.Count(ch) != 1 {
			t.Errorf("Count for %s should be 1", ch)
		}
	}
}

func TestSubscriptionIndex_Remove(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client1 := &Client{id: 1}
	client2 := &Client{id: 2}

	idx.Add("sukko.BTC.trade", client1)
	idx.Add("sukko.BTC.trade", client2)
	idx.Remove("sukko.BTC.trade", client1)

	if idx.Count("sukko.BTC.trade") != 1 {
		t.Errorf("Count should be 1 after remove, got %d", idx.Count("sukko.BTC.trade"))
	}

	clients := idx.Get("sukko.BTC.trade")
	if len(clients) != 1 || clients[0] != client2 {
		t.Error("Should only have client2 remaining")
	}
}

func TestSubscriptionIndex_Remove_NonExistent(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	// Should not panic
	idx.Remove("sukko.BTC.trade", client)
	idx.Remove("nonexistent", client)
}

func TestSubscriptionIndex_Remove_LastClient(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	idx.Add("sukko.BTC.trade", client)
	idx.Remove("sukko.BTC.trade", client)

	if idx.Count("sukko.BTC.trade") != 0 {
		t.Errorf("Count should be 0 after removing last client, got %d", idx.Count("sukko.BTC.trade"))
	}

	clients := idx.Get("sukko.BTC.trade")
	if clients != nil {
		t.Error("Get should return nil for empty channel")
	}
}

func TestSubscriptionIndex_RemoveMultiple(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	client := &Client{id: 1}

	idx.AddMultiple([]string{"sukko.BTC.trade", "sukko.ETH.trade", "sukko.SOL.liquidity"}, client)
	idx.RemoveMultiple([]string{"sukko.BTC.trade", "sukko.SOL.liquidity"}, client)

	if idx.Count("sukko.BTC.trade") != 0 {
		t.Error("sukko.BTC.trade should have no subscribers")
	}
	if idx.Count("sukko.ETH.trade") != 1 {
		t.Error("sukko.ETH.trade should still have subscriber")
	}
	if idx.Count("sukko.SOL.liquidity") != 0 {
		t.Error("sukko.SOL.liquidity should have no subscribers")
	}
}

func TestSubscriptionIndex_Get_NonExistent(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()

	clients := idx.Get("nonexistent")

	if clients != nil {
		t.Error("Get should return nil for non-existent channel")
	}
}

func TestSubscriptionIndex_ThreadSafety(t *testing.T) {
	t.Parallel()
	idx := NewSubscriptionIndex()
	var wg sync.WaitGroup
	iterations := 100

	// Create some clients
	clients := make([]*Client, 10)
	for i := range clients {
		clients[i] = &Client{id: int64(i)}
	}

	// Concurrent adds
	for i := range 10 {
		wg.Go(func() {
			for range iterations {
				idx.Add("channel"+string(rune('A'+i%3)), clients[i])
			}
		})
	}

	// Concurrent reads
	for range 10 {
		wg.Go(func() {
			for range iterations {
				_ = idx.Count("channelA")
				_ = idx.Get("channelB")
			}
		})
	}

	// Concurrent removes
	for i := range 5 {
		wg.Go(func() {
			for range iterations / 2 {
				idx.Remove("channelA", clients[i])
			}
		})
	}

	wg.Wait()
	// Test passes if no race conditions or panics
}

// =============================================================================
// ConnectionPool Tests
// =============================================================================

func TestNewConnectionPool(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 512, 8)
	if pool == nil {
		t.Fatal("NewConnectionPool should return non-nil")
	}

	if pool.maxSize != 100 {
		t.Errorf("maxSize should be 100, got %d", pool.maxSize)
	}
	if pool.bufferSize != 512 {
		t.Errorf("bufferSize should be 512, got %d", pool.bufferSize)
	}
}

func TestConnectionPool_Get(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()
	if client == nil {
		t.Fatal("Get should return non-nil client")
	}

	if cap(client.send) != 256 {
		t.Errorf("send channel capacity should be 256, got %d", cap(client.send))
	}
	if client.seqGen == nil {
		t.Error("seqGen should be initialized")
	}
	if client.subscriptions == nil {
		t.Error("subscriptions should be initialized")
	}
}

func TestConnectionPool_Get_SequenceReset(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()
	// Advance sequence number
	for range 10 {
		client.seqGen.Next()
	}

	// Return and get again
	pool.Put(client)
	client2 := pool.Get()

	// Sequence should be reset
	if client2.seqGen.Current() != 0 {
		t.Errorf("Sequence should be reset, got %d", client2.seqGen.Current())
	}
}

func TestConnectionPool_Get_SubscriptionsClear(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()
	client.subscriptions.Add("sukko.BTC.trade")
	client.subscriptions.Add("sukko.ETH.trade")

	// Return and get again
	pool.Put(client)
	client2 := pool.Get()

	// Subscriptions should be cleared
	if client2.subscriptions.Count() != 0 {
		t.Errorf("Subscriptions should be cleared, got %d", client2.subscriptions.Count())
	}
}

func TestConnectionPool_Get_ControlChannelInitialized(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()
	if client == nil {
		t.Fatal("Get should return non-nil client")
	}

	if client.control == nil {
		t.Error("control channel should be initialized")
	}
	if cap(client.control) != controlChannelSize {
		t.Errorf("control channel capacity should be %d, got %d", controlChannelSize, cap(client.control))
	}
}

func TestConnectionPool_Get_ControlChannelDrained(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()

	// Simulate stale pong from previous connection
	client.control <- []byte("stale-pong")

	// Return and get again
	pool.Put(client)
	client2 := pool.Get()

	// Control channel should be drained
	select {
	case <-client2.control:
		t.Error("control channel should be drained on reuse")
	default:
		// Good - channel is empty
	}
}

func TestConnectionPool_Put(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	client := pool.Get()
	client.id = 123

	pool.Put(client)

	// After Put, id should be reset
	if client.id != 0 {
		t.Errorf("id should be reset to 0, got %d", client.id)
	}
	if client.transport != nil {
		t.Error("transport should be nil after Put")
	}
	if client.server != nil {
		t.Error("server should be nil after Put")
	}
}

func TestConnectionPool_Put_Nil(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)

	// Should not panic
	pool.Put(nil)
}

func TestConnectionPool_ThreadSafety(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(100, 256, 8)
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent Get and Put
	for range 20 {
		wg.Go(func() {
			for j := range iterations {
				client := pool.Get()
				if client != nil {
					client.id = int64(j)
					pool.Put(client)
				}
			}
		})
	}

	wg.Wait()
	// Test passes if no race conditions or panics
}

// =============================================================================
// OutgoingMsg Tests
// =============================================================================

func TestOutgoingMsg_IsRaw(t *testing.T) {
	t.Parallel()

	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}

	tests := []struct {
		name string
		msg  OutgoingMsg
		want bool
	}{
		{"raw only", OutgoingMsg{raw: []byte("data")}, true},
		{"envelope only", OutgoingMsg{envelope: env, seq: 1}, false},
		{"zero value", OutgoingMsg{}, false},
		{"both raw and envelope", OutgoingMsg{raw: []byte("data"), envelope: env, seq: 1}, true},
		{"nil raw non-nil envelope", OutgoingMsg{raw: nil, envelope: env, seq: 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.msg.IsRaw(); got != tt.want {
				t.Errorf("IsRaw() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOutgoingMsg_AppendTo(t *testing.T) {
	t.Parallel()

	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}

	tests := []struct {
		name string
		msg  OutgoingMsg
		want []byte
	}{
		{
			name: "envelope seq 42",
			msg:  OutgoingMsg{envelope: env, seq: 42},
			want: []byte(`{"type":"message","seq":42,"ts":1000,"channel":"BTC.trade","data":{"x":1}}`),
		},
		{
			name: "zero value returns nil",
			msg:  OutgoingMsg{},
			want: nil,
		},
		{
			name: "max int64 seq",
			msg:  OutgoingMsg{envelope: env, seq: math.MaxInt64},
			want: []byte(`{"type":"message","seq":9223372036854775807,"ts":1000,"channel":"BTC.trade","data":{"x":1}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.msg.AppendTo(nil)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("AppendTo() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOutgoingMsg_Bytes_RawIdentity(t *testing.T) {
	t.Parallel()

	t.Run("pointer identity", func(t *testing.T) {
		t.Parallel()
		raw := []byte("test")
		msg := OutgoingMsg{raw: raw}
		got := msg.Bytes()
		raw[0] = 'X'
		if got[0] != 'X' {
			t.Error("Bytes() should return original slice (zero-copy), got a copy")
		}
	})

	t.Run("raw wins over envelope", func(t *testing.T) {
		t.Parallel()
		env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
		if err != nil {
			t.Fatalf("NewBroadcastEnvelope: %v", err)
		}
		msg := OutgoingMsg{raw: []byte("raw-data"), envelope: env, seq: 42}
		got := msg.Bytes()
		if !bytes.Equal(got, []byte("raw-data")) {
			t.Errorf("Bytes() = %q, want raw-data when both raw and envelope set", got)
		}
	})
}

func TestOutgoingMsg_ZeroAllocs(t *testing.T) { //nolint:paralleltest // testing.AllocsPerRun is sensitive to GC pressure from concurrent goroutines; intentionally non-parallel
	env, err := messaging.NewBroadcastEnvelope("BTC.trade", 1000, []byte(`{"x":1}`), "", "")
	if err != nil {
		t.Fatalf("NewBroadcastEnvelope: %v", err)
	}
	msg := OutgoingMsg{envelope: env, seq: 42}

	var buf []byte
	buf = msg.AppendTo(nil)
	if len(buf) == 0 {
		t.Fatal("pre-warm AppendTo returned empty result")
	}

	got := testing.AllocsPerRun(100, func() {
		buf = msg.AppendTo(buf[:0])
	})
	if got != 0 {
		t.Errorf("AppendTo allocations = %v, want 0", got)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkSubscriptionSet_Add(b *testing.B) {
	set := NewSubscriptionSet()
	for b.Loop() {
		set.Add("sukko.BTC.trade")
	}
}

func BenchmarkSubscriptionSet_Has(b *testing.B) {
	set := NewSubscriptionSet()
	set.Add("sukko.BTC.trade")

	for b.Loop() {
		_ = set.Has("sukko.BTC.trade")
	}
}

func BenchmarkSubscriptionIndex_Add(b *testing.B) {
	idx := NewSubscriptionIndex()
	clients := make([]*Client, 100)
	for i := range clients {
		clients[i] = &Client{id: int64(i)}
	}

	for i := 0; b.Loop(); i++ {
		idx.Add("sukko.BTC.trade", clients[i%100])
	}
}

func BenchmarkSubscriptionIndex_Get(b *testing.B) {
	idx := NewSubscriptionIndex()
	for i := range 1000 {
		idx.Add("sukko.BTC.trade", &Client{id: int64(i)})
	}

	for b.Loop() {
		_ = idx.Get("sukko.BTC.trade")
	}
}

func BenchmarkConnectionPool_GetPut(b *testing.B) {
	pool := NewConnectionPool(1000, 256, 8)

	for b.Loop() {
		client := pool.Get()
		pool.Put(client)
	}
}
