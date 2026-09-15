package server

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sukko-dev/sukko/internal/server/backend"
)

// =============================================================================
// Minimal Transport mock
// =============================================================================

// noopTransport satisfies the Transport interface by discarding all writes.
// Used to make Client.transport non-nil without requiring a real connection.
type noopTransport struct{}

func (t *noopTransport) Send(_ OutgoingMsg) (int, error)        { return 0, nil }
func (t *noopTransport) SendBatch(_ []OutgoingMsg) (int, error) { return 0, nil }
func (t *noopTransport) WritePing() error                       { return nil }
func (t *noopTransport) WritePong(_ []byte) error               { return nil }
func (t *noopTransport) Close() error                           { return nil }
func (t *noopTransport) CloseWithCode(_ int, _ string) error    { return nil }
func (t *noopTransport) SetWriteDeadline(_ time.Time) error     { return nil }
func (t *noopTransport) Type() TransportType                    { return "noop" }
func (t *noopTransport) RemoteAddr() string                     { return "127.0.0.1:0" }

// errTransport returns a configurable error on Send and/or SendBatch.
// Used to exercise the error paths in handleSendMsg.
type errTransport struct {
	sendErr      error
	sendBatchErr error
}

func (t *errTransport) Send(_ OutgoingMsg) (int, error)        { return 0, t.sendErr }
func (t *errTransport) SendBatch(_ []OutgoingMsg) (int, error) { return 0, t.sendBatchErr }
func (t *errTransport) WritePing() error                       { return nil }
func (t *errTransport) WritePong(_ []byte) error               { return nil }
func (t *errTransport) Close() error                           { return nil }
func (t *errTransport) CloseWithCode(_ int, _ string) error    { return nil }
func (t *errTransport) SetWriteDeadline(_ time.Time) error     { return nil }
func (t *errTransport) Type() TransportType                    { return "err" }
func (t *errTransport) RemoteAddr() string                     { return "127.0.0.1:0" }

// =============================================================================
// Configurable backend mock
// =============================================================================

// mockBackend implements backend.MessageBackend for handler tests.
// channelTopics maps channel names to Kafka topic names.
// replayMsgs and replayErr configure what Replay returns.
// lastReplayReq captures the most recent ReplayRequest for assertion in tests.
// blockCh, when non-nil, causes Replay to block until the channel is closed
// (or the context is canceled). Used by TestHandleReplayRequest_ConcurrentRace
// to hold the async goroutine in Replay while all N handlers process the
// replayInProgress check.
type mockBackend struct {
	mu            sync.Mutex
	channelTopics map[string]string
	replayMsgs    []backend.ReplayMessage
	replayErr     error
	lastReplayReq backend.ReplayRequest
	blockCh       chan struct{}
	notReady      bool // when true, Ready() reports false (readiness gate tests); default = ready
}

func (m *mockBackend) Start(_ context.Context) error { return nil }
func (m *mockBackend) Publish(_ context.Context, _ int64, _, _ string, _ []byte) (string, error) {
	return "mock-mid", nil
}
func (m *mockBackend) Replay(ctx context.Context, req backend.ReplayRequest) ([]backend.ReplayMessage, error) {
	m.mu.Lock()
	m.lastReplayReq = req
	blockCh := m.blockCh
	msgs := m.replayMsgs
	err := m.replayErr
	m.mu.Unlock()

	if blockCh != nil {
		select {
		case <-blockCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return msgs, err
}
func (m *mockBackend) IsHealthy() bool { return true }
func (m *mockBackend) Ready() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.notReady
}
func (m *mockBackend) Shutdown(_ context.Context) error { return nil }
func (m *mockBackend) ChannelTopic(channel string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	topic, ok := m.channelTopics[channel]
	return topic, ok
}

// =============================================================================
// Test Mocks (internal to shared package tests)
// =============================================================================

// testMockLogger captures log messages for testing.
type testMockLogger struct {
	mu       sync.Mutex
	messages []testLogMessage
}

type testLogMessage struct {
	level   string
	message string
	fields  map[string]any
	err     error
}

func newTestMockLogger() *testMockLogger {
	return &testMockLogger{messages: make([]testLogMessage, 0)}
}

func (m *testMockLogger) Debug() LogEvent           { return &testMockLogEvent{logger: m, level: "debug"} }
func (m *testMockLogger) Info() LogEvent            { return &testMockLogEvent{logger: m, level: "info"} }
func (m *testMockLogger) Warn() LogEvent            { return &testMockLogEvent{logger: m, level: "warn"} }
func (m *testMockLogger) Error() LogEvent           { return &testMockLogEvent{logger: m, level: "error"} }
func (m *testMockLogger) Printf(_ string, _ ...any) {}

func (m *testMockLogger) messageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func (m *testMockLogger) getMessages() []testLogMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]testLogMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

// testMockLogEvent implements LogEvent for testing.
type testMockLogEvent struct {
	logger *testMockLogger
	level  string
	fields map[string]any
	err    error
}

func (e *testMockLogEvent) Err(err error) LogEvent {
	e.err = err
	return e
}

func (e *testMockLogEvent) Int64(key string, val int64) LogEvent {
	if e.fields == nil {
		e.fields = make(map[string]any)
	}
	e.fields[key] = val
	return e
}

func (e *testMockLogEvent) Int(key string, val int) LogEvent {
	if e.fields == nil {
		e.fields = make(map[string]any)
	}
	e.fields[key] = val
	return e
}

func (e *testMockLogEvent) Str(key, val string) LogEvent {
	if e.fields == nil {
		e.fields = make(map[string]any)
	}
	e.fields[key] = val
	return e
}

func (e *testMockLogEvent) Interface(key string, val any) LogEvent {
	if e.fields == nil {
		e.fields = make(map[string]any)
	}
	e.fields[key] = val
	return e
}

func (e *testMockLogEvent) Msg(msg string) {
	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()
	e.logger.messages = append(e.logger.messages, testLogMessage{
		level:   e.level,
		message: msg,
		fields:  e.fields,
		err:     e.err,
	})
}

// testMockClock provides controllable time for testing.
type testMockClock struct {
	mu         sync.Mutex
	now        time.Time
	tickers    []*testMockTicker
	afterChans []chan time.Time
}

func newTestMockClock(t ...time.Time) *testMockClock {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if len(t) > 0 && !t[0].IsZero() {
		now = t[0]
	}
	return &testMockClock{
		now:        now,
		tickers:    make([]*testMockTicker, 0),
		afterChans: make([]chan time.Time, 0),
	}
}

func (c *testMockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testMockClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	ticker := &testMockTicker{
		c:        make(chan time.Time, 1),
		duration: d,
		stopped:  false,
	}
	c.tickers = append(c.tickers, ticker)
	return ticker
}

func (c *testMockClock) After(_ time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.afterChans = append(c.afterChans, ch)
	return ch
}

func (c *testMockClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	tickers := c.tickers
	afterChans := c.afterChans
	c.afterChans = make([]chan time.Time, 0)
	c.mu.Unlock()

	for _, t := range tickers {
		if !t.stopped {
			select {
			case t.c <- now:
			default:
			}
		}
	}

	for _, ch := range afterChans {
		select {
		case ch <- now:
		default:
		}
	}
}

// testMockTicker implements Ticker for testing.
type testMockTicker struct {
	c        chan time.Time
	duration time.Duration
	stopped  bool
}

func (t *testMockTicker) C() <-chan time.Time {
	return t.c
}

func (t *testMockTicker) Stop() {
	t.stopped = true
}

// testMockRateLimiter provides controllable rate limiting for testing.
type testMockRateLimiter struct {
	mu          sync.Mutex
	allowAll    bool
	allowCount  int
	blockedIDs  map[int64]bool
	callHistory []int64
}

func newTestMockRateLimiter() *testMockRateLimiter {
	return &testMockRateLimiter{
		allowAll:    true,
		blockedIDs:  make(map[int64]bool),
		callHistory: make([]int64, 0),
	}
}

func (m *testMockRateLimiter) CheckLimit(clientID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callHistory = append(m.callHistory, clientID)

	if m.blockedIDs[clientID] {
		return false
	}

	if m.allowAll {
		return true
	}

	if m.allowCount > 0 {
		m.allowCount--
		return true
	}

	return false
}

func (m *testMockRateLimiter) setAllowCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allowAll = false
	m.allowCount = count
}

func (m *testMockRateLimiter) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.callHistory)
}

// testMockAlertLogger captures alert events for testing.
type testMockAlertLogger struct {
	mu     sync.Mutex
	events []testAlertEvent
}

type testAlertEvent struct {
	level    string
	event    string
	message  string
	metadata map[string]any
}

func newTestMockAlertLogger() *testMockAlertLogger {
	return &testMockAlertLogger{events: make([]testAlertEvent, 0)}
}

func (m *testMockAlertLogger) Info(event, message string, metadata map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, testAlertEvent{
		level:    "info",
		event:    event,
		message:  message,
		metadata: metadata,
	})
}

func (m *testMockAlertLogger) Warning(event, message string, metadata map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, testAlertEvent{
		level:    "warning",
		event:    event,
		message:  message,
		metadata: metadata,
	})
}

func (m *testMockAlertLogger) Error(event, message string, metadata map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, testAlertEvent{
		level:    "error",
		event:    event,
		message:  message,
		metadata: metadata,
	})
}

func (m *testMockAlertLogger) Critical(event, message string, metadata map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, testAlertEvent{
		level:    "critical",
		event:    event,
		message:  message,
		metadata: metadata,
	})
}

func (m *testMockAlertLogger) eventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *testMockAlertLogger) hasEvent(eventName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if e.event == eventName {
			return true
		}
	}
	return false
}

// =============================================================================
// Mock Connection for WebSocket Testing
// =============================================================================

// testMockConn implements net.Conn for testing WebSocket frame handling.
type testMockConn struct {
	mu             sync.Mutex
	readBuf        []byte
	readPos        int
	readErr        error
	writeBuf       []byte
	writeErr       error
	closed         bool
	deadlines      []time.Time
	readDeadline   time.Time
	writeDeadlines []time.Time

	// Opt-in "block read until Close()" mode (default off — zero impact on all other callers).
	// Models the production shutdown ordering (cancel → write loop → conn close → read unblocks)
	// so ReadLoop shutdown-attribution tests are deterministic instead of racing an instant EOF.
	blockRead       bool
	readUnblock     chan struct{} // Close() closes this (once) to unblock a blocked Read
	readEntered     chan struct{} // closed (once) when a blocked Read is entered — test barrier
	readEnteredOnce sync.Once
	closeOnce       sync.Once
}

func newTestMockConn() *testMockConn {
	return &testMockConn{
		readBuf:   make([]byte, 0),
		writeBuf:  make([]byte, 0),
		deadlines: make([]time.Time, 0),
	}
}

func (c *testMockConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()

	// Opt-in blocking mode: with no buffered data, block until Close(). Checked BEFORE the
	// readErr fast path so a conn configured with both blocking mode AND a read error blocks
	// first, then returns the error on unblock. Snapshot state under the lock, release the lock
	// BEFORE receiving on the unblock channel (never hold c.mu across the block — Close() needs
	// it). Mirrors the mockBackend.Replay snapshot-then-block discipline.
	if c.blockRead && c.readPos >= len(c.readBuf) {
		unblock := c.readUnblock
		rerr := c.readErr
		c.readEnteredOnce.Do(func() { close(c.readEntered) })
		c.mu.Unlock()
		<-unblock
		if rerr == nil {
			rerr = io.EOF // guarantee a non-nil error so wsutil.Reader.NextFrame does not spin
		}
		return 0, rerr
	}

	if c.readErr != nil {
		c.mu.Unlock()
		return 0, c.readErr
	}

	if c.readPos >= len(c.readBuf) {
		c.mu.Unlock()
		return 0, c.readErr
	}

	n = copy(b, c.readBuf[c.readPos:])
	c.readPos += n
	c.mu.Unlock()
	return n, nil
}

func (c *testMockConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.writeErr != nil {
		return 0, c.writeErr
	}

	c.writeBuf = append(c.writeBuf, b...)
	return len(b), nil
}

func (c *testMockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	// Unblock any Read blocked in the opt-in blocking mode. Guarded so repeated Close() is a
	// safe no-op (a second close of a closed channel would panic — §VII).
	if c.readUnblock != nil {
		c.closeOnce.Do(func() { close(c.readUnblock) })
	}
	return nil
}

func (c *testMockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}
}

func (c *testMockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (c *testMockConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, t)
	return nil
}

func (c *testMockConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = t
	c.deadlines = append(c.deadlines, t)
	return nil
}

func (c *testMockConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadlines = append(c.writeDeadlines, t)
	return nil
}

// Helper methods for testing

func (c *testMockConn) setReadData(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readBuf = data
	c.readPos = 0
}

func (c *testMockConn) setReadError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readErr = err
}

// setBlockReadUntilClose enables the opt-in blocking mode: a Read with no buffered data blocks
// until Close() is called, then returns the configured readErr (default io.EOF). Models the
// production shutdown ordering (cancel → write loop → conn close → read unblocks) so the ReadLoop
// shutdown-attribution test is deterministic instead of racing an instant EOF.
func (c *testMockConn) setBlockReadUntilClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blockRead = true
	c.readUnblock = make(chan struct{})
	c.readEntered = make(chan struct{})
}

// readEnteredCh returns a channel closed when a blocked Read has been entered — a deterministic
// barrier so a test can guarantee the read loop is inside the blocking read before canceling.
func (c *testMockConn) readEnteredCh() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readEntered
}

func (c *testMockConn) getWrittenData() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]byte, len(c.writeBuf))
	copy(result, c.writeBuf)
	return result
}

func (c *testMockConn) getDeadlineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deadlines)
}

func (c *testMockConn) setWriteErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeErr = err
}

func (c *testMockConn) getWriteDeadlineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writeDeadlines)
}
