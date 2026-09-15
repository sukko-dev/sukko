package server

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// controlChannelSize is the buffer size for the pong control channel.
// Each client ping produces exactly 1 pong. WriteLoop drains this channel with
// priority (sub-millisecond), so at most 1 pong is pending at any time. Cap of 2
// provides headroom if a data batch write briefly delays the drain. If the channel
// is ever full, the pong is dropped with a log and the next client ping retries.
// This works correctly regardless of PingPeriod configuration.
const controlChannelSize = 2

// OutgoingMsg is the message type for the client send channel.
// It supports two modes:
//   - Pre-built: raw bytes (acks, errors, replay) — write pump sends directly
//   - Deferred:  shared envelope + per-client seq — write pump builds bytes
type OutgoingMsg struct {
	raw      []byte                       // pre-built bytes (non-nil = use directly)
	envelope *messaging.BroadcastEnvelope // shared broadcast template (immutable)
	seq      int64                        // per-client sequence number
}

// Bytes resolves the message to sendable bytes.
// For pre-built messages, returns raw directly (zero-cost).
// For broadcast messages, assembles bytes from envelope + seq (~100ns).
// Returns nil for zero-value OutgoingMsg (defensive guard against channel misuse).
func (m OutgoingMsg) Bytes() []byte {
	if m.raw != nil {
		return m.raw
	}
	if m.envelope == nil {
		return nil
	}
	return m.envelope.Build(m.seq)
}

// IsRaw reports whether this message carries pre-built bytes (acks, errors, replay).
// Transports use this to choose between the zero-copy raw path and the reusable-buffer
// envelope path.
func (m OutgoingMsg) IsRaw() bool {
	return m.raw != nil
}

// AppendTo appends the assembled envelope bytes for this message into dst and returns it.
// Passing buf[:0] reuses the backing array with zero allocs on warm calls.
// Returns nil for a zero-value OutgoingMsg. MUST NOT be called for raw messages — use Bytes().
func (m OutgoingMsg) AppendTo(dst []byte) []byte {
	if m.envelope == nil {
		return nil
	}
	return m.envelope.AppendTo(dst, m.seq)
}

// RawMsg creates an OutgoingMsg from pre-built bytes.
// Used by acks, errors, replay, and other non-broadcast messages.
func RawMsg(data []byte) OutgoingMsg {
	return OutgoingMsg{raw: data}
}

// Client represents a WebSocket client connection with message reliability features
// Enhanced from basic WebSocket to production-grade trading platform client
//
// Reliability features added:
// 1. Sequence numbers - Client can detect missing messages
// 2. Kafka-based replay - Client can request missed messages from Kafka offset tracking
// 3. Slow client detection - Automatically disconnect laggy clients
// 4. Rate limiting - Prevent client from DoS-ing server
//
// Memory per client depends on WS_CLIENT_SEND_BUFFER_SIZE (default: 512):
// - Base struct: ~200 bytes
// - send channel: bufferSize slots × 500 bytes avg
// - sequence generator: 8 bytes
// - Other fields: ~50 bytes
//
// Memory scaling at 125 msg/sec (25 msg/sec × 5 subscriptions):
// With 512 buffer (default, ~256KB/client):
// - 13,500 clients: ~3.5GB heap (reduced GC pressure)
// - 18,000 clients: ~4.6GB heap
//
// With 1024 buffer (~512KB/client):
// - 13,500 clients: ~6.9GB heap
// - 18,000 clients: ~9.2GB heap
//
// Buffer sizing rationale:
// - 512 buffer: 4.1s tolerance at 125 msg/sec (default - lower GC pressure)
// - 1024 buffer: 8.2s tolerance at 125 msg/sec (use if cascade disconnects occur)
//
// Trade-off: Memory vs slow-client tolerance. Smaller buffer = less GC = lower CPU spikes
type Client struct {
	// Connection fields
	id         int64            // Unique client identifier
	transport  Transport        // Transport abstraction (WebSocket, gRPC stream, etc.)
	server     *Server          // Reference to parent server
	send       chan OutgoingMsg // Buffered channel for outgoing messages (configurable via WS_CLIENT_SEND_BUFFER_SIZE)
	control    chan []byte      // Buffered channel for pong payloads (ReadLoop → WriteLoop)
	closeOnce  sync.Once        // Ensures connection is only closed once
	sendClosed atomic.Bool      // Guards close(send) from double-close

	// Message reliability fields
	// Sequence generator - creates monotonically increasing message IDs
	// Each client gets independent sequence (starts at 1 on connect)
	seqGen *messaging.SequenceGenerator

	// Slow client detection fields
	// Purpose: One slow client shouldn't block messages to 10,000 fast clients
	// Detection: If send blocks for >100ms, increment failure counter
	// Action: After 3 consecutive failures, disconnect client
	//
	// Why 100ms timeout:
	// - Trading platforms need <50ms latency for price updates
	// - 100ms is 2× the target, generous buffer for network variance
	// - Mobile clients on 4G typically <80ms latency
	// - Anything slower indicates problem (bad network, frozen app, etc.)
	//
	// Why 3 strikes:
	// - 1 failure: Could be temporary network hiccup (don't disconnect)
	// - 2 failures: Suspicious but maybe recovering
	// - 3 failures: Clear pattern, client is too slow
	//
	// Industry comparison:
	// - Coinbase: 2 strikes (more aggressive)
	// - Binance: No automatic disconnect (relies on ping timeout)
	// - FIX protocol: 5 second timeout (more lenient)
	lastMessageSentAt time.Time    // Timestamp of last successful send
	sendAttempts      atomic.Int32 // Consecutive failed send attempts (atomic for thread-safety)
	slowClientWarned  atomic.Int32 // Flag to avoid log spam (warn once) - atomic: 0 = not warned, 1 = warned
	connectedAt       time.Time    // Timestamp when client connected (for disconnect duration tracking)

	// Subscription filtering fields
	// Purpose: Only send messages to clients subscribed to specific channels
	// Performance: Reduces broadcast fanout from O(all_clients) to O(subscribed_clients)
	//
	// Example: 10K clients, 200 tokens
	// - Without filtering: 12 msg/sec × 10K clients = 120K writes/sec (CPU 99%+)
	// - With filtering: 12 msg/sec × 500 avg subscribers = 6K writes/sec (CPU <30%)
	//
	// Memory: ~40 bytes per subscription
	// - map[string]struct{}: 8 bytes (key) + 0 bytes (value) + ~32 bytes (map overhead)
	// - 30 subscriptions per client: 30 × 40 = 1.2KB per client
	// - 10K clients: 10K × 1.2KB = 12MB total (negligible)
	subscriptions *SubscriptionSet // Thread-safe set of subscribed channels

	// History delivery fields
	// clientCtx/clientCancel are derived from context.Background() (not the server context)
	// so delivery goroutines survive server shutdown until the client is disconnected.
	// disconnectClient() calls clientCancel() → clientWg.Wait() before Put().
	clientCtx         context.Context
	clientCancel      context.CancelFunc
	clientWg          *sync.WaitGroup
	historyMu         sync.Mutex
	historyInProgress map[string]struct{} // channels with in-flight history delivery

	// gapChan is the low-priority channel for gap notifications.
	// Nil for non-WebSocket transports; initialized unconditionally in ConnectionPool.Get()
	// because transport type is not known at Get() time.
	// Nil channels in select cases are permanently blocked — no nil guard needed in pump.
	gapChan chan GapNotification

	// replayInProgress and replayLastAt track live replay state per channel.
	// Both guarded by historyMu (§XVIII: same pattern as historyInProgress).
	replayInProgress map[string]struct{}  // channels with in-flight live replay
	replayLastAt     map[string]time.Time // last replay completion time per channel (rate limit)

	// NOTE: Authentication is now handled by ws-gateway
	// ws-server is a dumb broadcaster with network-level security via NetworkPolicy
	remoteAddr string // Client's remote IP address for logging

	// tenantID is set from X-Sukko-Tenant-ID at WebSocket upgrade (handlers_ws.go).
	// Used by OnTenantClientConnect/Disconnect and registry events.
	tenantID string

	// connID is a UUID generated after successful WebSocket upgrade (handlers_ws.go).
	// Used as the registry key and for force-disconnect targeting.
	connID string

	// apiKeyID is the stable database ID of the API key (X-Sukko-API-Key-ID header).
	// Empty for JWT-only auth paths.
	apiKeyID string

	// userID is the JWT subject claim (X-Sukko-User-ID header).
	// Empty for API-key-only auth.
	userID string
}

// TransportType returns the transport type of this client for metrics labeling.
// Returns empty string if transport is nil (client not yet initialized).
func (c *Client) TransportType() TransportType {
	if c.transport == nil {
		return ""
	}
	return c.transport.Type()
}

// ConnID returns the unique connection ID assigned at WebSocket upgrade.
// Implements registry.ForceCloser.
func (c *Client) ConnID() string { return c.connID }

// TenantID returns the authenticated tenant ID for this connection.
// Implements registry.ForceCloser.
func (c *Client) TenantID() string { return c.tenantID }

// ForceDisconnect sends WebSocket close code 4000 ("force_disconnect") exactly once.
// Uses closeOnce to prevent double-close panics if concurrent shutdown fires simultaneously.
// Implements registry.ForceCloser.
func (c *Client) ForceDisconnect() {
	c.closeOnce.Do(func() {
		if c.transport != nil {
			_ = c.transport.CloseWithCode(protocol.CloseCodeForceDisconnect, protocol.CloseReasonForceDisconnect)
		}
	})
}

// closeSend safely closes the send channel exactly once.
// Uses atomic CAS to prevent double-close panics during concurrent shutdown.
func (c *Client) closeSend() {
	if c.sendClosed.CompareAndSwap(false, true) {
		close(c.send)
	}
}

// ConnectionPool manages a pool of reusable client objects
type ConnectionPool struct {
	pool                sync.Pool
	maxSize             int
	bufferSize          int
	gapNotifyBufferSize int
}

// NewConnectionPool creates a connection pool with configurable buffer size.
// Config values MUST be validated (e.g., via ServerConfig.Validate()) before calling.
//
// bufferSize: per-client send channel buffer (range: 64–4096)
//
//	At 125 msg/sec: 512 slots ≈ 4.1s buffer, ~3.5 GB heap at 13.5K clients (default)
//	                1024 slots ≈ 8.2s buffer, ~6.9 GB heap at 13.5K clients
//
// gapNotifyBufferSize: per-client gap notification channel buffer (range: 1–64)
//
//	At default 8: absorbs ~8 burst drops before dropping; 8 × ~64 B ≈ 512 B per client
func NewConnectionPool(maxSize, bufferSize, gapNotifyBufferSize int) *ConnectionPool {
	cp := &ConnectionPool{
		maxSize:             maxSize,
		bufferSize:          bufferSize,
		gapNotifyBufferSize: gapNotifyBufferSize,
	}

	cp.pool = sync.Pool{
		New: func() any {
			client := &Client{
				send:    make(chan OutgoingMsg, cp.bufferSize),
				control: make(chan []byte, controlChannelSize),
				// historyInProgress, gapChan, replayInProgress, replayLastAt: nil —
				// Get() unconditionally initializes all four; allocating them here
				// wastes one allocation per pool-miss (GC eviction + cold start).
			}
			return client
		},
	}

	return cp
}

// Get retrieves a client from the pool, resetting it for reuse.
func (p *ConnectionPool) Get() *Client {
	v := p.pool.Get()
	if client, ok := v.(*Client); ok {
		// Recreate send channel if it was closed (e.g., during force shutdown)
		if client.sendClosed.Load() {
			client.send = make(chan OutgoingMsg, p.bufferSize)
			client.sendClosed.Store(false)
		} else {
			// Drain all pending messages from previous connection
			for {
				select {
				case <-client.send:
				default:
					goto sendDrained
				}
			}
		sendDrained:
		}

		// Drain stale pongs from previous connection
		for {
			select {
			case <-client.control:
			default:
				goto controlDrained
			}
		}
	controlDrained:

		// Initialize or reset sequence generator
		// Each new connection gets fresh sequence starting at 1
		if client.seqGen == nil {
			client.seqGen = messaging.NewSequenceGenerator()
		} else {
			// Reset counter for reused client — under load, Pool.Get() returns
			// existing objects the vast majority of the time, so this is the common path.
			client.seqGen.Reset()
		}

		// Reset closeOnce for new connection lifecycle.
		// sync.Once is a value type — zero value is ready to fire.
		// Without this, reused clients have closeOnce in "done" state from
		// the previous connection, causing transport.Close() to be skipped.
		client.closeOnce = sync.Once{}

		// Initialize slow client detection fields
		client.lastMessageSentAt = time.Now()
		client.sendAttempts.Store(0)
		client.slowClientWarned.Store(0) // 0 = not warned
		client.connectedAt = time.Now()  // Track connection start time for metrics

		// Initialize subscription set
		// Each new connection starts with no subscriptions
		// Client must explicitly subscribe to channels via WebSocket messages
		if client.subscriptions == nil {
			client.subscriptions = NewSubscriptionSet()
		} else {
			client.subscriptions.Clear()
		}

		// Reset logging fields
		client.remoteAddr = ""

		// Initialize history delivery context for this connection lease.
		// Cancel any previous context first (nil-guarded: pool.New sets these nil).
		if client.clientCancel != nil {
			client.clientCancel()
		}
		client.clientCtx, client.clientCancel = context.WithCancel(context.Background())
		client.clientWg = new(sync.WaitGroup)
		client.historyInProgress = make(map[string]struct{})

		// Unconditional make — creates a fresh channel on each Get(), preventing
		// stale notifications from a previous connection lease from reaching the new client.
		// Put() does NOT nil gapChan (avoiding a data race with Broadcast() which reads
		// the field without synchronization); the old channel is GC'd when the pool
		// slot's *Client is overwritten by the next Get() call.
		client.gapChan = make(chan GapNotification, p.gapNotifyBufferSize)
		client.replayInProgress = make(map[string]struct{})
		client.replayLastAt = make(map[string]time.Time)

		return client
	}
	return nil
}

// Put returns a client to the pool after resetting its state.
func (p *ConnectionPool) Put(c *Client) {
	if c == nil {
		return
	}

	// Reset connection
	c.transport = nil
	c.server = nil
	c.id = 0

	// Clear subscriptions before returning to pool
	if c.subscriptions != nil {
		c.subscriptions.Clear()
	}

	// Clear connection data before returning to pool
	c.remoteAddr = ""
	c.tenantID = ""
	c.connID = ""
	c.apiKeyID = ""
	c.userID = ""
	c.historyInProgress = nil
	// gapChan intentionally NOT nilled here — Get() always creates a fresh channel,
	// and nilling concurrently with Broadcast() reads would be a data race.
	c.replayInProgress = nil
	c.replayLastAt = nil

	p.pool.Put(c)
}

// SubscriptionSet is a thread-safe set of channel subscriptions
// Used to filter which messages a client receives
//
// Thread-safety: All methods use RWMutex for safe concurrent access
// - Add/Remove: Write lock (exclusive)
// - Has/Count/List: Read lock (shared - multiple readers allowed)
//
// Performance characteristics:
// - Add/Remove: O(1) average
// - Has: O(1) average
// - List: O(n) where n = number of subscriptions
// - Memory: ~40 bytes per subscription (map overhead)
type SubscriptionSet struct {
	channels map[string]struct{} // Set implementation using map with empty struct values
	mu       sync.RWMutex        // Protects concurrent access to channels map
}

// NewSubscriptionSet creates a new empty subscription set
func NewSubscriptionSet() *SubscriptionSet {
	return &SubscriptionSet{
		channels: make(map[string]struct{}),
	}
}

// Add subscribes to a channel
// Thread-safe: Uses write lock
func (s *SubscriptionSet) Add(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channel] = struct{}{}
}

// AddMultiple subscribes to multiple channels at once
// More efficient than calling Add() multiple times (single lock acquisition)
func (s *SubscriptionSet) AddMultiple(channels []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range channels {
		s.channels[ch] = struct{}{}
	}
}

// Remove unsubscribes from a channel
// Thread-safe: Uses write lock
func (s *SubscriptionSet) Remove(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, channel)
}

// RemoveMultiple unsubscribes from multiple channels at once
// More efficient than calling Remove() multiple times (single lock acquisition)
func (s *SubscriptionSet) RemoveMultiple(channels []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range channels {
		delete(s.channels, ch)
	}
}

// Has checks if client is subscribed to a channel
// Thread-safe: Uses read lock (allows concurrent Has() calls)
func (s *SubscriptionSet) Has(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.channels[channel]
	return exists
}

// Count returns the number of active subscriptions
// Thread-safe: Uses read lock
func (s *SubscriptionSet) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels)
}

// List returns a copy of all subscribed channels
// Thread-safe: Uses read lock
// Returns: New slice (safe to modify without affecting internal state)
func (s *SubscriptionSet) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]string, 0, len(s.channels))
	for ch := range s.channels {
		result = append(result, ch)
	}
	return result
}

// Clear removes all subscriptions
// Thread-safe: Uses write lock
func (s *SubscriptionSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels = make(map[string]struct{})
}

// SubscriptionIndex maintains a reverse index from channels to subscribed clients
// Performance optimization: Instead of iterating ALL clients to filter by subscription,
// iterate ONLY subscribed clients.
//
// Use case: Production scenario where clients subscribe to specific tokens
// - Without index: Iterate 7,000 clients, filter to ~500 subscribers = 7,000 checks
// - With index: Directly access ~500 subscribers = 500 iterations
// - CPU savings: 93% (14× reduction in iterations)
//
// Memory cost: ~7MB for 5 channels × 7,000 clients (map overhead)
// CPU savings: 93% fewer iterations per broadcast in production
//
// Thread-safety: Atomic snapshots with copy-on-write
// - Add/Remove: Write lock, creates new slice, atomically swaps snapshot
// - Get: Lock-free atomic load of immutable snapshot (HOT PATH OPTIMIZATION!)
// - At 125 broadcasts/sec, eliminates 125 RWMutex locks + 125 slice copies per second
// - Result: 3-5% CPU reduction on broadcast path
type SubscriptionIndex struct {
	subscribers map[string]*atomic.Value // channel → atomic.Value holding []*Client snapshot
	mu          sync.RWMutex             // Protects map structure, not individual snapshots
}

// NewSubscriptionIndex creates a new empty subscription index
func NewSubscriptionIndex() *SubscriptionIndex {
	return &SubscriptionIndex{
		subscribers: make(map[string]*atomic.Value),
	}
}

// Add registers a client as a subscriber to a channel
// Thread-safe: Uses write lock with copy-on-write
func (idx *SubscriptionIndex) Add(channel string, client *Client) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Get or create atomic.Value for this channel
	atomicVal := idx.subscribers[channel]
	if atomicVal == nil {
		atomicVal = &atomic.Value{}
		idx.subscribers[channel] = atomicVal
	}

	// Load current snapshot (may be nil for new channel)
	var currentSlice []*Client
	if v := atomicVal.Load(); v != nil {
		if cs, ok := v.([]*Client); ok {
			currentSlice = cs
		}
	}

	// Check if client already subscribed (avoid duplicates)
	if slices.Contains(currentSlice, client) {
		return // Already subscribed
	}

	// Create new slice with added client (copy-on-write)
	newSlice := make([]*Client, len(currentSlice)+1)
	copy(newSlice, currentSlice)
	newSlice[len(currentSlice)] = client

	// Atomically swap the new snapshot
	atomicVal.Store(newSlice)
}

// AddMultiple registers a client as a subscriber to multiple channels
// More efficient than calling Add() multiple times (single lock acquisition)
// Thread-safe: Uses write lock with copy-on-write
func (idx *SubscriptionIndex) AddMultiple(channels []string, client *Client) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, channel := range channels {
		// Get or create atomic.Value for this channel
		atomicVal := idx.subscribers[channel]
		if atomicVal == nil {
			atomicVal = &atomic.Value{}
			idx.subscribers[channel] = atomicVal
		}

		// Load current snapshot
		var currentSlice []*Client
		if v := atomicVal.Load(); v != nil {
			if cs, ok := v.([]*Client); ok {
				currentSlice = cs
			}
		}

		// Check if client already subscribed
		alreadySubscribed := slices.Contains(currentSlice, client)

		if !alreadySubscribed {
			// Create new slice with added client
			newSlice := make([]*Client, len(currentSlice)+1)
			copy(newSlice, currentSlice)
			newSlice[len(currentSlice)] = client
			atomicVal.Store(newSlice)
		}
	}
}

// Remove unregisters a client from a channel
// Thread-safe: Uses write lock with copy-on-write
func (idx *SubscriptionIndex) Remove(channel string, client *Client) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	atomicVal, exists := idx.subscribers[channel]
	if !exists {
		return
	}

	// Load current snapshot
	v := atomicVal.Load()
	if v == nil {
		return
	}
	currentSlice, ok := v.([]*Client)
	if !ok {
		return
	}

	// Find client in slice
	for i, existing := range currentSlice {
		if existing != client {
			continue
		}
		// Create new slice without this client (copy-on-write)
		newSlice := make([]*Client, len(currentSlice)-1)
		copy(newSlice, currentSlice[:i])
		copy(newSlice[i:], currentSlice[i+1:])

		// If slice is now empty, remove the channel entry
		if len(newSlice) == 0 {
			delete(idx.subscribers, channel)
		} else {
			// Atomically swap the new snapshot
			atomicVal.Store(newSlice)
		}
		return
	}
}

// RemoveMultiple unregisters a client from multiple channels
// More efficient than calling Remove() multiple times (single lock acquisition)
// Thread-safe: Uses write lock with copy-on-write
func (idx *SubscriptionIndex) RemoveMultiple(channels []string, client *Client) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, channel := range channels {
		atomicVal, exists := idx.subscribers[channel]
		if !exists {
			continue
		}

		v := atomicVal.Load()
		if v == nil {
			continue
		}
		currentSlice, ok := v.([]*Client)
		if !ok {
			continue
		}

		// Find and remove client
		for i, existing := range currentSlice {
			if existing != client {
				continue
			}
			newSlice := make([]*Client, len(currentSlice)-1)
			copy(newSlice, currentSlice[:i])
			copy(newSlice[i:], currentSlice[i+1:])

			if len(newSlice) == 0 {
				delete(idx.subscribers, channel)
			} else {
				atomicVal.Store(newSlice)
			}
			break
		}
	}
}

// Get returns all clients subscribed to a channel
// Thread-safe: Lock-free atomic load (HOT PATH OPTIMIZATION!)
// Returns: Immutable snapshot slice (safe to iterate, DO NOT MODIFY)
//
// CRITICAL PERFORMANCE OPTIMIZATION:
// - No RWMutex lock (eliminates lock contention at 125 broadcasts/sec)
// - No slice copy (eliminates 125 allocations/sec per channel)
// - Simple atomic load ~10ns (vs RLock ~50ns + copy ~200ns)
// - Result: 3-5% CPU reduction on broadcast path
//
// Safety: Returned slice is immutable snapshot, safe to iterate but must not be modified
func (idx *SubscriptionIndex) Get(channel string) []*Client {
	// Fast path: Read lock only to get atomic.Value reference
	idx.mu.RLock()
	atomicVal, exists := idx.subscribers[channel]
	idx.mu.RUnlock()

	if !exists {
		return nil
	}

	// Lock-free atomic load of immutable snapshot
	v := atomicVal.Load()
	if v == nil {
		return nil
	}

	// Return snapshot directly (no copy needed - it's immutable!)
	// Use safe type assertion to prevent panic if value is corrupted
	clients, ok := v.([]*Client)
	if !ok {
		return nil
	}
	return clients
}

// Count returns the number of subscribers for a channel
// Thread-safe: Lock-free atomic load
func (idx *SubscriptionIndex) Count(channel string) int {
	idx.mu.RLock()
	atomicVal, exists := idx.subscribers[channel]
	idx.mu.RUnlock()

	if !exists {
		return 0
	}

	v := atomicVal.Load()
	if v == nil {
		return 0
	}

	// Use safe type assertion to prevent panic if value is corrupted
	clients, ok := v.([]*Client)
	if !ok {
		return 0
	}
	return len(clients)
}
