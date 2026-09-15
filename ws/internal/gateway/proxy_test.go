package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// testClaims creates auth.Claims with the given subject for testing.
func testClaims(subject string) *auth.Claims {
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: subject,
		},
		TenantID: "test-tenant",
		Groups:   []string{},
	}
	return claims
}

// testClaimsWithGroups creates auth.Claims with subject and groups for testing.
func testClaimsWithGroups(subject string, groups []string) *auth.Claims {
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: subject,
		},
		TenantID: "test-tenant",
		Groups:   groups,
	}
	return claims
}

// newTestProxy creates a Proxy for testing interceptClientMessage, wired with
// rules-backed authorization closures (provisioning-only model). The legacy
// pattern-list signature is preserved by mapping onto per-tenant rules:
// public → Public, user-scoped ({principal}) → Default, and group patterns
// ({group_id}) → GroupMappings expanded from the claims' groups.
func newTestProxy(claims *auth.Claims, publicPatterns, userPatterns, groupPatterns []string) *Proxy {
	tenantID := ""
	if claims != nil {
		tenantID = claims.TenantID
	}

	// Publish was not rules-checked pre-provisioning-only; legacy fixtures
	// treat public/user patterns as publishable to preserve test intent.
	rules := &types.ChannelRules{
		Public:         publicPatterns,
		Default:        userPatterns,
		PublishPublic:  publicPatterns,
		PublishDefault: userPatterns,
	}
	if len(groupPatterns) > 0 && claims != nil {
		rules.GroupMappings = make(map[string][]string, len(claims.Groups))
		for _, g := range claims.Groups {
			for _, p := range groupPatterns {
				rules.GroupMappings[g] = append(rules.GroupMappings[g],
					strings.ReplaceAll(p, "{group_id}", g))
			}
		}
	}
	filterFn, canPublishFn, _ := testProxyAuthz(tenantID, rules)

	return &Proxy{
		clientConn:  nil, // Not needed for interception tests
		backendConn: nil, // Not needed for interception tests

		claims:             claims,
		tenantID:           tenantID,
		filterSubscribe:    filterFn,
		canPublish:         canPublishFn,
		logger:             zerolog.Nop(),
		messageTimeout:     60 * time.Second,
		maxFrameSize:       protocol.DefaultMaxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100), // 10/sec, 100 burst
		maxPublishSize:     64 * 1024,                // 64KB
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}
}

func TestProxy_InterceptClientMessage_EmptyTenantBypass(t *testing.T) {
	t.Parallel()
	// Empty tenantID should bypass all interception (defensive guard)
	proxy := &Proxy{
		tenantID:       "",
		logger:         zerolog.Nop(),
		publishLimiter: rate.NewLimiter(10, 100),
		maxPublishSize: 64 * 1024,
	}

	input := `{"type":"subscribe","data":{"channels":["secret.channel"]}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))

	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	if string(result) != input {
		t.Errorf("Empty tenantID should pass through unchanged.\nGot:  %s\nWant: %s", result, input)
	}
}

func TestProxy_InterceptClientMessage_NonJSON(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user456"), []string{"*.trade"}, nil, nil)

	// Non-JSON messages should pass through
	inputs := []string{
		"not json at all",
		"{invalid json",
		"",
		"12345",
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			result, _ := proxy.interceptClientMessage(context.Background(), []byte(input))
			if string(result) != input {
				t.Errorf("Non-JSON should pass through unchanged.\nGot:  %s\nWant: %s", result, input)
			}
		})
	}
}

func TestProxy_InterceptClientMessage_NonSubscribeNonPublish(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)

	// Non-subscribe/non-publish messages should pass through unchanged
	messages := []string{
		`{"type":"ping"}`,
		`{"type":"pong"}`,
		`{"type":"unsubscribe","data":{"channels":["BTC.trade"]}}`,
	}

	for _, input := range messages {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			result, _ := proxy.interceptClientMessage(context.Background(), []byte(input))
			if string(result) != input {
				t.Errorf("Non-subscribe/non-publish should pass through unchanged.\nGot:  %s\nWant: %s", result, input)
			}
		})
	}
}

func TestProxy_InterceptClientMessage_PublishValidatesChannel(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)

	// Publish with explicit tenant-prefixed channel
	input := `{"type":"publish","data":{"channel":"test-tenant.BTC.trade","data":{"msg":"test"}}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))

	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// Result should have channel as-is (no mapping, just validation)
	var msg protocol.ClientMessage
	if err := json.Unmarshal(result, &msg); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}
	if msg.Type != "publish" {
		t.Errorf("Type should be publish, got %s", msg.Type)
	}

	var pubData protocol.PublishData
	if err := json.Unmarshal(msg.Data, &pubData); err != nil {
		t.Fatalf("Failed to parse publish data: %v", err)
	}
	// Channel should be unchanged (explicit tenant prefix validated)
	expectedChannel := "test-tenant.BTC.trade"
	if pubData.Channel != expectedChannel {
		t.Errorf("Channel should be %s, got %s", expectedChannel, pubData.Channel)
	}
}

func TestProxy_InterceptClientMessage_AllAllowed(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade", "*.liquidity"}, nil, nil)

	// Explicit channels: clients include tenant prefix
	input := `{"type":"subscribe","data":{"channels":["test-tenant.BTC.trade","test-tenant.ETH.liquidity"]}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))

	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// All channels pass tenant validation and permission filtering
	var msg protocol.ClientMessage
	if err := json.Unmarshal(result, &msg); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}
	var data protocol.SubscribeData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatalf("Failed to parse data: %v", err)
	}

	expected := []string{"test-tenant.BTC.trade", "test-tenant.ETH.liquidity"}
	if len(data.Channels) != len(expected) {
		t.Errorf("Expected %d channels, got %d: %v", len(expected), len(data.Channels), data.Channels)
	}
	for i, ch := range expected {
		if i < len(data.Channels) && data.Channels[i] != ch {
			t.Errorf("Channel[%d] = %q, want %q", i, data.Channels[i], ch)
		}
	}
}

func TestProxy_InterceptClientMessage_SomeFiltered(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)

	// Explicit channels: some allowed by permissions, some not
	// "secret.channel" doesn't match *.trade pattern, so it's denied by permissions
	input := `{"type":"subscribe","data":{"channels":["test-tenant.BTC.trade","test-tenant.secret.channel","test-tenant.ETH.trade"]}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))

	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	var msg protocol.ClientMessage
	if err := json.Unmarshal(result, &msg); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	var data protocol.SubscribeData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatalf("Failed to parse data: %v", err)
	}

	// Should have BTC.trade and ETH.trade (not secret.channel — denied by permissions)
	expectedChannels := []string{"test-tenant.BTC.trade", "test-tenant.ETH.trade"}
	if len(data.Channels) != len(expectedChannels) {
		t.Errorf("Expected %d channels, got %d: %v", len(expectedChannels), len(data.Channels), data.Channels)
	}

	for i, ch := range expectedChannels {
		if i >= len(data.Channels) || data.Channels[i] != ch {
			t.Errorf("Channel[%d] = %q, want %q", i, data.Channels[i], ch)
		}
	}
}

func TestProxy_InterceptClientMessage_AllFiltered(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)

	// Explicit channels with correct tenant prefix but not matching *.trade pattern
	input := `{"type":"subscribe","data":{"channels":["test-tenant.secret.channel","test-tenant.forbidden.data"]}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))

	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// Parse result
	var msg protocol.ClientMessage
	if err := json.Unmarshal(result, &msg); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	var data protocol.SubscribeData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatalf("Failed to parse data: %v", err)
	}

	// Should have empty channels list
	if len(data.Channels) != 0 {
		t.Errorf("Expected 0 channels when all filtered, got %d: %v", len(data.Channels), data.Channels)
	}
}

func TestProxy_InterceptClientMessage_UserScoped(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(
		testClaims("user123"),
		[]string{"*.trade"},
		[]string{"balances.{principal}"},
		nil,
	)

	tests := []struct {
		name            string
		channels        []string
		expectedAllowed []string
	}{
		{
			name:            "own balance allowed",
			channels:        []string{"test-tenant.balances.user123"},
			expectedAllowed: []string{"test-tenant.balances.user123"},
		},
		{
			name:            "other user balance denied",
			channels:        []string{"test-tenant.balances.user456"},
			expectedAllowed: []string{},
		},
		{
			name:            "mixed - own allowed, other denied",
			channels:        []string{"test-tenant.balances.user123", "test-tenant.balances.user456"},
			expectedAllowed: []string{"test-tenant.balances.user123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inputData := protocol.SubscribeData{Channels: tt.channels}
			dataBytes, _ := json.Marshal(inputData)
			inputMsg := protocol.ClientMessage{Type: "subscribe", Data: dataBytes}
			input, _ := json.Marshal(inputMsg)

			result, err := proxy.interceptClientMessage(context.Background(), input)
			if err != nil {
				t.Fatalf("interceptClientMessage() error = %v", err)
			}

			var msg protocol.ClientMessage
			if err := json.Unmarshal(result, &msg); err != nil {
				t.Fatalf("Failed to parse result: %v", err)
			}

			var data protocol.SubscribeData
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				t.Fatalf("Failed to parse data: %v", err)
			}

			if len(data.Channels) != len(tt.expectedAllowed) {
				t.Errorf("Expected %d channels, got %d: %v", len(tt.expectedAllowed), len(data.Channels), data.Channels)
			}
		})
	}
}

func TestProxy_InterceptClientMessage_GroupScoped(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(
		testClaimsWithGroups("user123", []string{"vip", "traders"}),
		[]string{"*.trade"},
		nil,
		[]string{"community.{group_id}"},
	)

	tests := []struct {
		name            string
		channels        []string
		expectedAllowed []string
	}{
		{
			name:            "member group allowed",
			channels:        []string{"test-tenant.community.vip"},
			expectedAllowed: []string{"test-tenant.community.vip"},
		},
		{
			name:            "non-member group denied",
			channels:        []string{"test-tenant.community.whales"},
			expectedAllowed: []string{},
		},
		{
			name:            "mixed member and non-member",
			channels:        []string{"test-tenant.community.vip", "test-tenant.community.whales", "test-tenant.community.traders"},
			expectedAllowed: []string{"test-tenant.community.vip", "test-tenant.community.traders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inputData := protocol.SubscribeData{Channels: tt.channels}
			dataBytes, _ := json.Marshal(inputData)
			inputMsg := protocol.ClientMessage{Type: "subscribe", Data: dataBytes}
			input, _ := json.Marshal(inputMsg)

			result, err := proxy.interceptClientMessage(context.Background(), input)
			if err != nil {
				t.Fatalf("interceptClientMessage() error = %v", err)
			}

			var msg protocol.ClientMessage
			if err := json.Unmarshal(result, &msg); err != nil {
				t.Fatalf("Failed to parse result: %v", err)
			}

			var data protocol.SubscribeData
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				t.Fatalf("Failed to parse data: %v", err)
			}

			if len(data.Channels) != len(tt.expectedAllowed) {
				t.Errorf("Expected %d channels, got %d: %v", len(tt.expectedAllowed), len(data.Channels), data.Channels)
			}
		})
	}
}

func TestProxy_InterceptClientMessage_MalformedSubscribeData(t *testing.T) {
	t.Parallel()
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)

	// Valid subscribe type but malformed data
	input := `{"type":"subscribe","data":"not-an-object"}`
	result, _ := proxy.interceptClientMessage(context.Background(), []byte(input))

	// Should pass through unchanged on parse error
	if string(result) != input {
		t.Errorf("Malformed subscribe data should pass through.\nGot:  %s\nWant: %s", result, input)
	}
}

// Benchmark for interceptClientMessage to ensure performance
func BenchmarkInterceptClientMessage_PassThrough(b *testing.B) {
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)
	input := []byte(`{"type":"subscribe","data":{"channels":["test-tenant.BTC.trade","test-tenant.ETH.trade"]}}`)

	for b.Loop() {
		_, _ = proxy.interceptClientMessage(context.Background(), input)
	}
}

func BenchmarkInterceptClientMessage_Filtered(b *testing.B) {
	proxy := newTestProxy(testClaims("user123"), []string{"*.trade"}, nil, nil)
	input := []byte(`{"type":"subscribe","data":{"channels":["test-tenant.BTC.trade","test-tenant.secret.channel","test-tenant.ETH.trade"]}}`)

	for b.Loop() {
		_, _ = proxy.interceptClientMessage(context.Background(), input)
	}
}

// writeWSFrame writes a WebSocket frame header + payload to conn.
// mask=true for client→server frames, false for server→client.
func writeWSFrame(conn net.Conn, opCode ws.OpCode, payload []byte, mask bool) error {
	header := ws.Header{
		Fin:    true,
		OpCode: opCode,
		Length: int64(len(payload)),
	}
	if mask {
		header.Masked = true
		header.Mask = ws.NewMask()
		ws.Cipher(payload, header.Mask, 0)
	}
	if err := ws.WriteHeader(conn, header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

// writeOversizedFrameHeader writes a WebSocket frame header with a declared length
// exceeding maxFrameSize but does NOT write the payload (simulates OOM attack vector).
func writeOversizedFrameHeader(conn net.Conn, declaredLength int64, mask bool) error {
	header := ws.Header{
		Fin:    true,
		OpCode: ws.OpText,
		Length: declaredLength,
	}
	if mask {
		header.Masked = true
		header.Mask = ws.NewMask()
	}
	if err := ws.WriteHeader(conn, header); err != nil {
		return fmt.Errorf("write oversized frame header: %w", err)
	}
	return nil
}

// newRunTestProxy creates a Proxy with net.Pipe connections for Run() tests.
func newRunTestProxy(maxFrameSize int, messageTimeout time.Duration) (proxy *Proxy, client, backend net.Conn) {
	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()

	proxy = &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		tenantID:           "test-tenant",
		logger:             zerolog.Nop(),
		messageTimeout:     messageTimeout,
		maxFrameSize:       maxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}

	return proxy, clientRemote, backendRemote
}

func TestProxy_FrameSizeValidation_ClientExceedsMax(t *testing.T) {
	t.Parallel()
	maxSize := 1024 // 1KB for testing
	proxy, clientRemote, backendRemote := newRunTestProxy(maxSize, 0)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// Drain backend side to prevent blocking
	go drainConn(backendRemote)

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(context.Background())
	}()

	// Write oversized header from client, then drain close frame response.
	// net.Pipe is full-duplex: writes and reads on the same conn use separate streams.
	go func() {
		_ = writeOversizedFrameHeader(clientRemote, int64(maxSize+1), true)
		drainConn(clientRemote) // drain close frame sent back by proxy
	}()

	select {
	case <-done:
		// Proxy exited — expected
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after oversized client frame")
	}
}

func TestProxy_FrameSizeValidation_BackendExceedsMax(t *testing.T) {
	t.Parallel()
	maxSize := 1024
	proxy, clientRemote, backendRemote := newRunTestProxy(maxSize, 0)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// Drain client side to prevent blocking
	go drainConn(clientRemote)

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(context.Background())
	}()

	// Send oversized frame from backend side (no masking — server→client)
	err := writeOversizedFrameHeader(backendRemote, int64(maxSize+1), false)
	if err != nil {
		t.Fatalf("Failed to write oversized header: %v", err)
	}

	select {
	case <-done:
		// Proxy exited — expected
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after oversized backend frame")
	}
}

func TestProxy_FrameSizeValidation_NormalFrameAllowed(t *testing.T) {
	t.Parallel()
	maxSize := 1024
	proxy, clientRemote, backendRemote := newRunTestProxy(maxSize, 0)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// Drain proxy responses to client (close frames, etc.)
	// net.Pipe is full-duplex: reads here don't interfere with writes below.
	go drainConn(clientRemote)

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(context.Background())
	}()

	// Send a normal-sized text frame from client (masked)
	payload := []byte(`{"type":"ping"}`)
	if err := writeWSFrame(clientRemote, ws.OpText, payload, true); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Read the forwarded frame on backend side
	header, err := ws.ReadHeader(backendRemote)
	if err != nil {
		t.Fatalf("Failed to read forwarded header: %v", err)
	}
	if header.Length == 0 {
		t.Fatal("Expected non-zero forwarded payload")
	}

	// Read payload
	buf := make([]byte, header.Length)
	if _, err := backendRemote.Read(buf); err != nil {
		t.Fatalf("Failed to read forwarded payload: %v", err)
	}

	// Unmask if needed
	if header.Masked {
		ws.Cipher(buf, header.Mask, 0)
	}

	// Verify payload content
	if !strings.Contains(string(buf), "ping") {
		t.Errorf("Expected forwarded payload to contain 'ping', got %q", string(buf))
	}

	// Close client to end proxy
	closeBody := make([]byte, 2)
	binary.BigEndian.PutUint16(closeBody, uint16(ws.StatusNormalClosure))
	if err := writeWSFrame(clientRemote, ws.OpClose, closeBody, true); err != nil {
		t.Fatalf("Failed to send close: %v", err)
	}

	// Drain remaining backend data (forwarded close frame)
	go drainConn(backendRemote)

	select {
	case <-done:
		// Proxy exited normally
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after close frame")
	}
}

func TestProxy_MessageTimeout_ClientStall(t *testing.T) {
	t.Parallel()
	// Very short timeout to trigger quickly in test
	timeout := 100 * time.Millisecond
	proxy, clientRemote, backendRemote := newRunTestProxy(protocol.DefaultMaxFrameSize, timeout)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// Drain both sides to prevent blocking
	go drainConn(backendRemote)

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(context.Background())
	}()

	// Don't send anything from client — let it stall
	select {
	case <-done:
		// Proxy exited due to timeout — expected
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after client message timeout")
	}
}

func TestProxy_MessageTimeout_BackendStall(t *testing.T) {
	t.Parallel()
	timeout := 100 * time.Millisecond
	proxy, clientRemote, backendRemote := newRunTestProxy(protocol.DefaultMaxFrameSize, timeout)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// Drain client side
	go drainConn(clientRemote)

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(context.Background())
	}()

	// Don't send anything from backend — let it stall
	select {
	case <-done:
		// Proxy exited due to timeout — expected
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after backend message timeout")
	}
}

func TestProxy_ContextCancellation(t *testing.T) {
	t.Parallel()
	proxy, clientRemote, backendRemote := newRunTestProxy(protocol.DefaultMaxFrameSize, 0)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.Run(ctx)
	}()

	// Cancel context to simulate shutdown
	cancel()

	select {
	case <-done:
		// Proxy exited due to context cancellation — expected
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy did not exit after context cancellation")
	}

	_ = clientRemote.Close()
	_ = backendRemote.Close()
}

// testAllowTradeFilter / testAllowTradePublish are small conveniences for
// inline Proxy literals that just need *.trade rules for the given tenant.
func testAllowTradeFilter(tenantID string) func(context.Context, []string, *auth.Claims) []string {
	f, _, _ := testProxyAuthz(tenantID, &types.ChannelRules{Public: []string{"*.trade"}})
	return f
}

func testAllowTradePublish(tenantID string) func(context.Context, *auth.Claims, string) bool {
	_, p, _ := testProxyAuthz(tenantID, &types.ChannelRules{Public: []string{"*.trade"}})
	return p
}

// newTestProxyAPIKeyOnly creates a Proxy for API-key-only connection testing.
// Auth is enabled, claims are nil, apiKeyOnly=true. Rules-backed: nil claims
// are checked against the tenant's public rules only.
func newTestProxyAPIKeyOnly(tenantID string, publicPatterns []string) *Proxy {
	filterFn, canPublishFn, _ := testProxyAuthz(tenantID, &types.ChannelRules{
		Public:        publicPatterns,
		PublishPublic: publicPatterns, // legacy fixtures: public ⇒ publishable
	})
	return &Proxy{
		clientConn:  nil,
		backendConn: nil,

		claims:             nil,
		tenantID:           tenantID,
		apiKeyOnly:         true,
		apiKeyTenantID:     tenantID,
		filterSubscribe:    filterFn,
		canPublish:         canPublishFn,
		logger:             zerolog.Nop(),
		messageTimeout:     60 * time.Second,
		maxFrameSize:       protocol.DefaultMaxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}
}

func TestProxy_APIKeyOnly_PublishRejected(t *testing.T) {
	t.Parallel()

	// Need net.Pipe because publish rejection sends error to client
	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims:             nil,
		tenantID:           "sukko",
		apiKeyOnly:         true,
		apiKeyTenantID:     "sukko",
		logger:             zerolog.Nop(),
		filterSubscribe:    testAllowTradeFilter("sukko"),
		canPublish:         testAllowTradePublish("sukko"),
		messageTimeout:     60 * time.Second,
		maxFrameSize:       protocol.DefaultMaxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}

	// API-key-only connections cannot publish
	input := `{"type":"publish","data":{"channel":"sukko.BTC.trade","data":"price=42000"}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// Result should be nil (message consumed, error sent to client)
	if result != nil {
		t.Errorf("expected nil result for API-key-only publish, got %q", string(result))
	}
}

func TestProxy_APIKeyOnly_SubscribePublicOnly(t *testing.T) {
	t.Parallel()
	proxy := newTestProxyAPIKeyOnly("sukko", []string{"*.trade"})

	// Subscribe to mix of public and non-public channels
	input := `{"type":"subscribe","data":{"channels":["sukko.BTC.trade","sukko.balances.user123","sukko.ETH.trade"]}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	var msg protocol.ClientMessage
	if err := json.Unmarshal(result, &msg); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}
	var data protocol.SubscribeData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		t.Fatalf("Failed to parse data: %v", err)
	}

	// Only public channels should pass through (nil claims = public only)
	expected := []string{"sukko.BTC.trade", "sukko.ETH.trade"}
	if len(data.Channels) != len(expected) {
		t.Fatalf("expected %d channels, got %d: %v", len(expected), len(data.Channels), data.Channels)
	}
	for i, ch := range expected {
		if data.Channels[i] != ch {
			t.Errorf("channel[%d] = %q, want %q", i, data.Channels[i], ch)
		}
	}
}

func TestProxy_APIKeyOnly_AuthEscalation(t *testing.T) {
	t.Parallel()

	// Verify initial state
	proxy := newTestProxyAPIKeyOnly("test-tenant", []string{"*.trade"})
	if !proxy.apiKeyOnly {
		t.Fatal("expected apiKeyOnly=true initially")
	}

	// After escalation (simulated by setting claims and clearing apiKeyOnly)
	newClaims := testClaims("user123")
	proxy.claimsMu.Lock()
	proxy.claims = newClaims
	proxy.apiKeyOnly = false
	proxy.claimsMu.Unlock()

	// Now publish should work
	input := `{"type":"publish","data":{"channel":"test-tenant.BTC.trade","data":"price=42000"}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// Result should not be nil (message forwarded)
	if result == nil {
		t.Error("expected non-nil result after escalation, publish should be allowed")
	}
}

func TestProxy_APIKeyOnly_AuthRefreshTenantMismatch(t *testing.T) {
	t.Parallel()

	// Need net.Pipe because auth refresh sends error response to client
	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		// Genuine cross-tenant: connection is tenant-a; the refresh JWT is for tenant-b, so
		// step 5 (slug) rejects it before step 5b is reached. apiKeyTenantID is the key's
		// owning tenant UUID, matching how gateway.go wires it.
		claims:                nil,
		tenantID:              "tenant-a",
		apiKeyOnly:            true,
		apiKeyTenantID:        "aaaaaaaa-0000-0000-0000-00000000000a",
		logger:                zerolog.Nop(),
		filterSubscribe:       testAllowTradeFilter("tenant-a"),
		canPublish:            testAllowTradePublish("tenant-a"),
		messageTimeout:        60 * time.Second,
		maxFrameSize:          protocol.DefaultMaxFrameSize,
		publishLimiter:        rate.NewLimiter(10, 100),
		maxPublishSize:        64 * 1024,
		authLimiter:           rate.NewLimiter(rate.Every(30*time.Second), 1),
		authValidationTimeout: 5 * time.Second,
		subscribedChannels:    make(map[string]struct{}),
		validator: &mockTokenValidator{
			claims: &auth.Claims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "user123",
				},
				TenantID: "tenant-b", // mismatch with apiKeyTenantID
			},
		},
	}

	// Send auth refresh with a token
	input := `{"type":"auth","data":{"token":"eyJ.mock.token"}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}

	// Result should be nil (error sent to client, not forwarded)
	if result != nil {
		t.Errorf("expected nil result for tenant mismatch, got %q", string(result))
	}

	// apiKeyOnly should still be true (escalation failed)
	if !proxy.apiKeyOnly {
		t.Error("apiKeyOnly should still be true after failed escalation")
	}
}

// TestProxy_InterceptPublish_RulesDenied verifies WS publish enforces the
// per-tenant publish-side rules: a tenant with subscribe-only rules is
// denied publish with the FORBIDDEN error frame.
func TestProxy_InterceptPublish_RulesDenied(t *testing.T) {
	t.Parallel()

	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	// Subscribe-side only — publish side empty ⇒ deny all publishes.
	filterFn, canPublishFn, _ := testProxyAuthz("test-tenant", &types.ChannelRules{
		Public: []string{"*.trade"},
	})

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims:             testClaims("user123"),
		tenantID:           "test-tenant",
		filterSubscribe:    filterFn,
		canPublish:         canPublishFn,
		logger:             zerolog.Nop(),
		messageTimeout:     60 * time.Second,
		maxFrameSize:       protocol.DefaultMaxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}

	input := `{"type":"publish","data":{"channel":"test-tenant.BTC.trade","data":"price=42000"}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}
	// nil result = message consumed (error frame sent to client, not forwarded)
	if result != nil {
		t.Errorf("expected nil result for rules-denied publish, got %q", string(result))
	}
}

// TestProxy_InterceptPublish_RulesAllowed verifies the frame forwards when the
// publish-side rules allow the channel.
func TestProxy_InterceptPublish_RulesAllowed(t *testing.T) {
	t.Parallel()

	filterFn, canPublishFn, _ := testProxyAuthz("test-tenant", &types.ChannelRules{
		Public:        []string{"*.trade"},
		PublishPublic: []string{"*.trade"},
	})

	proxy := &Proxy{
		clientConn:  nil,
		backendConn: nil,

		claims:             testClaims("user123"),
		tenantID:           "test-tenant",
		filterSubscribe:    filterFn,
		canPublish:         canPublishFn,
		logger:             zerolog.Nop(),
		messageTimeout:     60 * time.Second,
		maxFrameSize:       protocol.DefaultMaxFrameSize,
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		subscribedChannels: make(map[string]struct{}),
	}

	input := `{"type":"publish","data":{"channel":"test-tenant.BTC.trade","data":"price=42000"}}`
	result, err := proxy.interceptClientMessage(context.Background(), []byte(input))
	if err != nil {
		t.Fatalf("interceptClientMessage() error = %v", err)
	}
	if result == nil {
		t.Error("expected forwarded frame for rules-allowed publish, got nil")
	}
}
