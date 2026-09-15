package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/limits"
	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/stats"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// =============================================================================
// Buffer Usage Calculation Tests
// =============================================================================

func TestBufferUsagePercent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bufferLen   int
		bufferCap   int
		wantPercent float64
	}{
		{"empty", 0, 512, 0},
		{"half", 256, 512, 50},
		{"full", 512, 512, 100},
		{"quarter", 128, 512, 25},
		{"three_quarters", 384, 512, 75},
		{"small_buffer_half", 5, 10, 50},
		{"large_buffer_half", 512, 1024, 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			percent := float64(tt.bufferLen) / float64(tt.bufferCap) * 100

			if percent != tt.wantPercent {
				t.Errorf("percent: got %f, want %f", percent, tt.wantPercent)
			}
		})
	}
}

// =============================================================================
// Connection Duration Tests
// =============================================================================

func TestConnectionDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		age    time.Duration
		minDur time.Duration
		maxDur time.Duration
	}{
		{"5_seconds", 5 * time.Second, 4 * time.Second, 6 * time.Second},
		{"1_hour", 1 * time.Hour, 59 * time.Minute, 61 * time.Minute},
		{"100_milliseconds", 100 * time.Millisecond, 90 * time.Millisecond, 200 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			connectedAt := time.Now().Add(-tt.age)
			duration := time.Since(connectedAt)

			if duration < tt.minDur || duration > tt.maxDur {
				t.Errorf("duration: got %v, expected between %v and %v", duration, tt.minDur, tt.maxDur)
			}
		})
	}
}

// =============================================================================
// Client State Tests
// =============================================================================

func TestClient_SendAttemptsAtomic(t *testing.T) {
	t.Parallel()
	client := &Client{}

	// Concurrent increments
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			client.sendAttempts.Add(1)
		})
	}
	wg.Wait()

	if client.sendAttempts.Load() != 100 {
		t.Errorf("sendAttempts: got %d, want 100", client.sendAttempts.Load())
	}
}

func TestClient_SlowClientWarnedFlag(t *testing.T) {
	t.Parallel()
	client := &Client{}

	// First warning should succeed
	if !client.slowClientWarned.CompareAndSwap(0, 1) {
		t.Error("First CAS should succeed")
	}

	// Second warning should fail (already warned)
	if client.slowClientWarned.CompareAndSwap(0, 1) {
		t.Error("Second CAS should fail (already warned)")
	}

	if client.slowClientWarned.Load() != 1 {
		t.Error("slowClientWarned should be 1")
	}
}

// =============================================================================
// Disconnect Reason Constants Tests
// =============================================================================

func TestDisconnectConstants_NotEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{"DisconnectReadError", pkgmetrics.DisconnectReadError},
		{"DisconnectWriteTimeout", pkgmetrics.DisconnectWriteTimeout},
		{"DisconnectPingTimeout", pkgmetrics.DisconnectPingTimeout},
		{"DisconnectServerShutdown", pkgmetrics.DisconnectServerShutdown},
		{"DisconnectRateLimitExceeded", pkgmetrics.DisconnectRateLimitExceeded},
		{"InitiatedByClient", pkgmetrics.InitiatedByClient},
		{"InitiatedByServer", pkgmetrics.InitiatedByServer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.value == "" {
				t.Errorf("%s constant should not be empty", tt.name)
			}
		})
	}
}

// =============================================================================
// Stats Decrement Tests
// =============================================================================

func TestStats_CurrentConnectionsDecrement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial int64
		want    int64
	}{
		{"from_10_to_9", 10, 9},
		{"from_1_to_0", 1, 0},
		{"from_100_to_99", 100, 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := stats.NewStats()
			s.CurrentConnections.Store(tt.initial)
			s.CurrentConnections.Add(-1)

			if s.CurrentConnections.Load() != tt.want {
				t.Errorf("CurrentConnections: got %d, want %d", s.CurrentConnections.Load(), tt.want)
			}
		})
	}
}

func TestStats_CurrentConnectionsDecrement_Concurrent(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	s.CurrentConnections.Store(1000)

	var wg sync.WaitGroup
	for range 1000 {
		wg.Go(func() {
			s.CurrentConnections.Add(-1)
		})
	}
	wg.Wait()

	if s.CurrentConnections.Load() != 0 {
		t.Errorf("CurrentConnections: got %d, want 0", s.CurrentConnections.Load())
	}
}

// =============================================================================
// CloseOnce Tests
// =============================================================================

func TestCloseOnce_SingleExecution(t *testing.T) {
	t.Parallel()
	var closeOnce sync.Once
	closeCount := 0

	closeOnce.Do(func() {
		closeCount++
	})

	closeOnce.Do(func() {
		closeCount++
	})

	closeOnce.Do(func() {
		closeCount++
	})

	if closeCount != 1 {
		t.Errorf("closeCount: got %d, want 1 (sync.Once should execute only once)", closeCount)
	}
}

func TestCloseOnce_ConcurrentCalls(t *testing.T) {
	t.Parallel()
	var closeOnce sync.Once
	var closeCount atomic.Int32

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			closeOnce.Do(func() {
				closeCount.Add(1)
			})
		})
	}
	wg.Wait()

	if closeCount.Load() != 1 {
		t.Errorf("closeCount: got %d, want 1", closeCount.Load())
	}
}

// =============================================================================
// Semaphore Release Tests
// =============================================================================

func TestSemaphore_Release(t *testing.T) {
	t.Parallel()
	maxConn := 10
	sem := make(chan struct{}, maxConn)

	// Fill the semaphore
	for range maxConn {
		sem <- struct{}{}
	}

	// Should be full
	select {
	case sem <- struct{}{}:
		t.Error("Semaphore should be full")
	default:
		// Expected
	}

	// Release one slot
	<-sem

	// Now should be able to acquire
	select {
	case sem <- struct{}{}:
		// Expected
	default:
		t.Error("Should be able to acquire after release")
	}
}

func TestSemaphore_ReleaseMultiple(t *testing.T) {
	t.Parallel()
	maxConn := 10
	sem := make(chan struct{}, maxConn)

	// Fill the semaphore
	for range maxConn {
		sem <- struct{}{}
	}

	// Release 5 slots
	for range 5 {
		<-sem
	}

	// Should be able to acquire 5 more
	for i := range 5 {
		select {
		case sem <- struct{}{}:
			// Expected
		default:
			t.Errorf("Should be able to acquire slot %d", i+1)
		}
	}

	// Now should be full again
	select {
	case sem <- struct{}{}:
		t.Error("Semaphore should be full again")
	default:
		// Expected
	}
}

// =============================================================================
// Integration Test: Minimal disconnectClient
// =============================================================================

func TestDisconnectClient_Integration(t *testing.T) {
	t.Parallel()
	// Create minimal server with required components
	logger := zerolog.Nop()
	s := stats.NewStats()
	s.CurrentConnections.Store(1)

	server := &Server{
		logger:            logger,
		stats:             s,
		connections:       NewConnectionPool(100, 256, 8),
		connectionsSem:    make(chan struct{}, 100),
		subscriptionIndex: NewSubscriptionIndex(),
		rateLimiter:       limits.NewRateLimiter(100, 10),
	}

	// Acquire a connection slot
	server.connectionsSem <- struct{}{}

	// Create a client
	client := &Client{
		id:            12345,
		send:          make(chan OutgoingMsg, 256),
		seqGen:        messaging.NewSequenceGenerator(),
		subscriptions: NewSubscriptionSet(),
		connectedAt:   time.Now().Add(-5 * time.Second),
		transport:     nil, // No real transport for this test
	}

	// Store client in server's map
	server.clients.Store(client, true)

	// Add some subscriptions
	client.subscriptions.Add("BTC.trade")
	server.subscriptionIndex.Add("BTC.trade", client)

	// Disconnect
	server.disconnectClient(client, pkgmetrics.DisconnectReadError, pkgmetrics.InitiatedByClient)

	// Verify stats updated
	if s.CurrentConnections.Load() != 0 {
		t.Errorf("CurrentConnections should be 0, got %d", s.CurrentConnections.Load())
	}

	// Verify client removed from subscription index
	clients := server.subscriptionIndex.Get("BTC.trade")
	if len(clients) != 0 {
		t.Errorf("Client should be removed from subscription index, got %d clients", len(clients))
	}

	// Verify semaphore released (should be able to acquire again)
	select {
	case server.connectionsSem <- struct{}{}:
		// Good - slot was released
	default:
		t.Error("Connection semaphore slot should have been released")
	}
}

func TestDisconnectClient_WaitsForHistoryGoroutines(t *testing.T) {
	t.Parallel()
	s := stats.NewStats()
	s.CurrentConnections.Store(1)

	server := &Server{
		logger:            zerolog.Nop(),
		stats:             s,
		connections:       NewConnectionPool(100, 256, 8),
		connectionsSem:    make(chan struct{}, 100),
		subscriptionIndex: NewSubscriptionIndex(),
		rateLimiter:       limits.NewRateLimiter(100, 10),
	}
	server.connectionsSem <- struct{}{}

	clientCtx, clientCancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)

	client := &Client{
		id:                1,
		send:              make(chan OutgoingMsg, 256),
		seqGen:            messaging.NewSequenceGenerator(),
		subscriptions:     NewSubscriptionSet(),
		historyInProgress: make(map[string]struct{}),
		clientCtx:         clientCtx,
		clientCancel:      clientCancel,
		clientWg:          wg,
		connectedAt:       time.Now(),
	}
	server.clients.Store(client, true)

	// Launch a goroutine that blocks until clientCtx is canceled — simulates in-flight history delivery.
	goroutineExited := make(chan struct{})
	wg.Go(func() {
		<-clientCtx.Done()
		close(goroutineExited)
	})

	done := make(chan struct{})
	go func() {
		server.disconnectClient(client, pkgmetrics.DisconnectReadError, pkgmetrics.InitiatedByClient)
		close(done)
	}()

	// disconnectClient must not return until the goroutine has exited.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnectClient blocked — did not cancel and wait for history goroutines")
	}

	select {
	case <-goroutineExited:
	default:
		t.Error("history goroutine still running after disconnectClient returned")
	}
}

// =============================================================================
// Client Fields Access Tests
// =============================================================================

func TestClient_SequenceGenerator(t *testing.T) {
	t.Parallel()
	client := &Client{
		seqGen: messaging.NewSequenceGenerator(),
	}

	// Generate some sequences
	seq1 := client.seqGen.Next()
	seq2 := client.seqGen.Next()
	seq3 := client.seqGen.Next()

	if seq1 != 1 {
		t.Errorf("First sequence should be 1, got %d", seq1)
	}
	if seq2 != 2 {
		t.Errorf("Second sequence should be 2, got %d", seq2)
	}
	if seq3 != 3 {
		t.Errorf("Third sequence should be 3, got %d", seq3)
	}

	// Current should return last generated
	if client.seqGen.Current() != 3 {
		t.Errorf("Current should be 3, got %d", client.seqGen.Current())
	}
}

func TestClient_SubscriptionsCount(t *testing.T) {
	t.Parallel()
	client := &Client{
		subscriptions: NewSubscriptionSet(),
	}

	if client.subscriptions.Count() != 0 {
		t.Errorf("Initial count should be 0, got %d", client.subscriptions.Count())
	}

	client.subscriptions.Add("BTC.trade")
	client.subscriptions.Add("ETH.trade")
	client.subscriptions.Add("SOL.liquidity")

	if client.subscriptions.Count() != 3 {
		t.Errorf("Count should be 3, got %d", client.subscriptions.Count())
	}
}

// =============================================================================
// Buffer Length/Capacity Tests
// =============================================================================

func TestClient_SendBufferMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bufferCap   int
		fillCount   int
		wantLen     int
		wantPercent float64
	}{
		{
			name:        "empty buffer",
			bufferCap:   256,
			fillCount:   0,
			wantLen:     0,
			wantPercent: 0,
		},
		{
			name:        "quarter full",
			bufferCap:   256,
			fillCount:   64,
			wantLen:     64,
			wantPercent: 25,
		},
		{
			name:        "half full",
			bufferCap:   256,
			fillCount:   128,
			wantLen:     128,
			wantPercent: 50,
		},
		{
			name:        "three quarters",
			bufferCap:   256,
			fillCount:   192,
			wantLen:     192,
			wantPercent: 75,
		},
		{
			name:        "full",
			bufferCap:   256,
			fillCount:   256,
			wantLen:     256,
			wantPercent: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := &Client{
				send: make(chan OutgoingMsg, tt.bufferCap),
			}

			// Fill buffer
			for range tt.fillCount {
				client.send <- RawMsg([]byte("msg"))
			}

			bufferLen := len(client.send)
			bufferCap := cap(client.send)
			bufferPercent := float64(bufferLen) / float64(bufferCap) * 100

			if bufferLen != tt.wantLen {
				t.Errorf("len: got %d, want %d", bufferLen, tt.wantLen)
			}
			if bufferCap != tt.bufferCap {
				t.Errorf("cap: got %d, want %d", bufferCap, tt.bufferCap)
			}
			if bufferPercent != tt.wantPercent {
				t.Errorf("percent: got %f, want %f", bufferPercent, tt.wantPercent)
			}
		})
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkBufferUsageCalculation(b *testing.B) {
	bufferLen := 256
	bufferCap := 512

	for b.Loop() {
		_ = float64(bufferLen) / float64(bufferCap) * 100
	}
}

func BenchmarkAtomicDecrement(b *testing.B) {
	var counter atomic.Int64
	counter.Store(1000000)

	for b.Loop() {
		counter.Add(-1)
		counter.Add(1) // Reset for next iteration
	}
}

func BenchmarkSyncOnce(b *testing.B) {

	for b.Loop() {
		var once sync.Once
		once.Do(func() {})
	}
}

func BenchmarkTimeSince(b *testing.B) {
	connectedAt := time.Now().Add(-5 * time.Second)

	for b.Loop() {
		_ = time.Since(connectedAt)
	}
}

// =============================================================================
// TestDisconnectClient_SubscriptionIndexCleanup
// =============================================================================

// TestDisconnectClient_SubscriptionIndexCleanup verifies that disconnectClient removes
// the client from only its subscribed channels, leaving other clients' entries intact.
func TestDisconnectClient_SubscriptionIndexCleanup(t *testing.T) {
	t.Parallel()

	newServer := func() *Server {
		s := stats.NewStats()
		s.CurrentConnections.Store(1)
		return &Server{
			logger:            zerolog.Nop(),
			stats:             s,
			connections:       NewConnectionPool(100, 256, 8),
			connectionsSem:    make(chan struct{}, 100),
			subscriptionIndex: NewSubscriptionIndex(),
			rateLimiter:       limits.NewRateLimiter(100, 10),
		}
	}

	newClient := func(id int64) *Client {
		return &Client{
			id:            id,
			send:          make(chan OutgoingMsg, 256),
			seqGen:        messaging.NewSequenceGenerator(),
			subscriptions: NewSubscriptionSet(),
			connectedAt:   time.Now(),
			transport:     nil,
		}
	}

	assertSendClosed := func(t *testing.T, c *Client) {
		t.Helper()
		select {
		case _, ok := <-c.send:
			if ok {
				t.Error("send channel should be closed, received data instead")
			}
		default:
			t.Error("send channel not closed")
		}
	}

	assertNotSubscribed := func(t *testing.T, srv *Server, prevSubs []string, c *Client) {
		t.Helper()
		for _, ch := range prevSubs {
			for _, sub := range srv.subscriptionIndex.Get(ch) {
				if sub == c {
					t.Errorf("client should not be in subscriptionIndex[%q] after disconnect", ch)
				}
			}
		}
	}

	t.Run("A has no subscriptions", func(t *testing.T) {
		t.Parallel()

		srv := newServer()
		clientA := newClient(1)
		clientB := newClient(2)

		// B subscribes to ch1 — makes the assertion non-vacuous.
		clientB.subscriptions.Add("ch1")
		srv.subscriptionIndex.Add("ch1", clientB)

		srv.clients.Store(clientA, true)
		srv.connectionsSem <- struct{}{}

		prevSubs := clientA.subscriptions.List()
		srv.disconnectClient(clientA, pkgmetrics.DisconnectReadError, pkgmetrics.InitiatedByClient)

		assertNotSubscribed(t, srv, prevSubs, clientA)

		// ch1 is unaffected — still has B.
		ch1Subs := srv.subscriptionIndex.Get("ch1")
		if len(ch1Subs) != 1 || ch1Subs[0] != clientB {
			t.Errorf("ch1 should still have only clientB, got %v", ch1Subs)
		}

		assertSendClosed(t, clientA)
	})

	t.Run("A shares channel with B", func(t *testing.T) {
		t.Parallel()

		srv := newServer()
		clientA := newClient(1)
		clientB := newClient(2)

		for _, c := range []*Client{clientA, clientB} {
			c.subscriptions.Add("ch1")
			srv.subscriptionIndex.Add("ch1", c)
		}

		srv.clients.Store(clientA, true)
		srv.connectionsSem <- struct{}{}

		prevSubs := clientA.subscriptions.List()
		srv.disconnectClient(clientA, pkgmetrics.DisconnectReadError, pkgmetrics.InitiatedByClient)

		assertNotSubscribed(t, srv, prevSubs, clientA)

		// ch1 still present with only B.
		ch1Subs := srv.subscriptionIndex.Get("ch1")
		if len(ch1Subs) != 1 {
			t.Errorf("ch1 should have 1 subscriber (B), got %d", len(ch1Subs))
		} else if ch1Subs[0] != clientB {
			t.Error("ch1's remaining subscriber should be clientB")
		}

		assertSendClosed(t, clientA)
	})

	t.Run("A owns multiple channels, C unaffected", func(t *testing.T) {
		t.Parallel()

		srv := newServer()
		clientA := newClient(1)
		clientB := newClient(2)
		clientC := newClient(3)

		// A→{ch1,ch2,ch3}, B→{ch2}, C→{ch4,ch5}
		for _, ch := range []string{"ch1", "ch2", "ch3"} {
			clientA.subscriptions.Add(ch)
			srv.subscriptionIndex.Add(ch, clientA)
		}
		clientB.subscriptions.Add("ch2")
		srv.subscriptionIndex.Add("ch2", clientB)
		for _, ch := range []string{"ch4", "ch5"} {
			clientC.subscriptions.Add(ch)
			srv.subscriptionIndex.Add(ch, clientC)
		}

		srv.clients.Store(clientA, true)
		srv.connectionsSem <- struct{}{}

		prevSubs := clientA.subscriptions.List()
		srv.disconnectClient(clientA, pkgmetrics.DisconnectReadError, pkgmetrics.InitiatedByClient)

		assertNotSubscribed(t, srv, prevSubs, clientA)

		// ch1, ch3: A was sole subscriber — key absent.
		if srv.subscriptionIndex.Get("ch1") != nil {
			t.Error("ch1 should have no subscribers (A was sole subscriber)")
		}
		if srv.subscriptionIndex.Get("ch3") != nil {
			t.Error("ch3 should have no subscribers (A was sole subscriber)")
		}

		// ch2: B remains.
		ch2Subs := srv.subscriptionIndex.Get("ch2")
		if len(ch2Subs) != 1 || ch2Subs[0] != clientB {
			t.Errorf("ch2 should have only clientB, got %v", ch2Subs)
		}

		// ch4, ch5: C unaffected.
		for _, ch := range []string{"ch4", "ch5"} {
			subs := srv.subscriptionIndex.Get(ch)
			if len(subs) != 1 || subs[0] != clientC {
				t.Errorf("%q should still have only clientC, got %v", ch, subs)
			}
		}

		assertSendClosed(t, clientA)
	})
}
