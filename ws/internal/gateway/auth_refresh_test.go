package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
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

// drainConn reads and discards all data from a connection until it closes.
func drainConn(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// mockTokenValidator is a test double for TokenValidator.
type mockTokenValidator struct {
	claims *auth.Claims
	err    error
}

func (m *mockTokenValidator) ValidateToken(_ context.Context, _ string) (*auth.Claims, error) {
	return m.claims, m.err
}

// newAuthTestProxy creates a Proxy configured for auth refresh tests.
// Uses net.Pipe for client/backend connections so sendToClient works.
func newAuthTestProxy(validator TokenValidator, claims *auth.Claims) (proxy *Proxy, client, backend net.Conn) {
	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()

	tenantID := ""
	if claims != nil {
		tenantID = claims.TenantID
	}

	filterFn, canPublishFn, _ := testProxyAuthz(tenantID, &types.ChannelRules{
		Public: []string{"*.trade", "*.liquidity"},
	})

	proxy = &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims:                claims,
		tenantID:              tenantID,
		filterSubscribe:       filterFn,
		canPublish:            canPublishFn,
		validator:             validator,
		logger:                zerolog.Nop(),
		messageTimeout:        60 * time.Second,
		maxFrameSize:          protocol.DefaultMaxFrameSize,
		publishLimiter:        rate.NewLimiter(10, 100),
		maxPublishSize:        64 * 1024,
		authLimiter:           rate.NewLimiter(rate.Every(100*time.Millisecond), 1), // Fast for tests
		authValidationTimeout: 5 * time.Second,
		subscribedChannels:    make(map[string]struct{}),
	}

	return proxy, clientRemote, backendRemote
}

func TestInterceptAuthRefresh_ValidToken(t *testing.T) {
	t.Parallel()
	oldClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "test-tenant",
	}
	validator := &mockTokenValidator{claims: newClaims}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, oldClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"valid-jwt-token"}`),
	}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)

	if err != nil {
		t.Fatalf("interceptAuthRefresh error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result (message handled, not forwarded)")
	}

	// Verify claims were swapped
	proxy.claimsMu.RLock()
	if proxy.claims != newClaims {
		t.Error("Claims should have been swapped to new claims")
	}
	proxy.claimsMu.RUnlock()
}

func TestInterceptAuthRefresh_ExpiredToken(t *testing.T) {
	t.Parallel()
	oldClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{err: auth.ErrTokenExpired}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, oldClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"expired-token"}`),
	}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)

	if err != nil {
		t.Fatalf("interceptAuthRefresh error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result")
	}

	// Claims should NOT have changed
	proxy.claimsMu.RLock()
	if proxy.claims != oldClaims {
		t.Error("Claims should not have changed on expired token")
	}
	proxy.claimsMu.RUnlock()
}

func TestInterceptAuthRefresh_InvalidToken(t *testing.T) {
	t.Parallel()
	oldClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{err: auth.ErrInvalidToken}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, oldClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"bad-signature"}`),
	}
	result, _ := proxy.interceptAuthRefresh(context.Background(), msg)

	if result != nil {
		t.Error("Expected nil result")
	}
}

// TestInterceptAuthRefresh_BindingTenantMismatch verifies the tenant-binding
// rejection path on auth refresh (#158): when ValidateToken rejects a refresh
// token because it is signed by a key whose owning tenant does not match the
// claimed tenant (auth.ErrTenantMismatch), the refresh is denied with the
// DISTINCT AuthErrTenantMismatch code — not the generic invalid-token code — and
// the connection's claims are unchanged. Asserting the code is what exercises the
// errors.Is(err, auth.ErrTenantMismatch) branch (a bare nil-result check would
// pass even without it, since any error rejects the refresh).
func TestInterceptAuthRefresh_BindingTenantMismatch(t *testing.T) {
	t.Parallel()
	oldClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{err: auth.ErrTenantMismatch}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, oldClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// net.Pipe is synchronous — read the error frame concurrently so the proxy's
	// send does not block.
	type frameResult struct {
		payload []byte
		err     error
	}
	frameCh := make(chan frameResult, 1)
	go func() {
		frame, rerr := ws.ReadFrame(clientRemote)
		frameCh <- frameResult{payload: frame.Payload, err: rerr}
	}()

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"cross-tenant-signed"}`),
	}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)
	if err != nil {
		t.Fatalf("interceptAuthRefresh error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result (refresh rejected), got %q", string(result))
	}

	proxy.claimsMu.RLock()
	if proxy.claims != oldClaims {
		t.Error("claims should not change on tenant-binding rejection")
	}
	proxy.claimsMu.RUnlock()

	fr := <-frameCh
	if fr.err != nil {
		t.Fatalf("read client error frame: %v", fr.err)
	}
	var resp AuthErrorResponse
	if uerr := json.Unmarshal(fr.payload, &resp); uerr != nil {
		t.Fatalf("unmarshal auth error frame: %v", uerr)
	}
	if resp.Data.Code != AuthErrTenantMismatch {
		t.Errorf("auth error code = %q, want %q (distinct tenant-mismatch code)", resp.Data.Code, AuthErrTenantMismatch)
	}
}

func TestInterceptAuthRefresh_TenantMismatch(t *testing.T) {
	t.Parallel()
	oldClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "tenant-a",
	}
	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "tenant-b",
	}
	validator := &mockTokenValidator{claims: newClaims}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, oldClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"different-tenant-token"}`),
	}
	result, _ := proxy.interceptAuthRefresh(context.Background(), msg)

	if result != nil {
		t.Error("Expected nil result")
	}

	// Claims should NOT have changed
	proxy.claimsMu.RLock()
	if proxy.claims != oldClaims {
		t.Error("Claims should not have changed on tenant mismatch")
	}
	proxy.claimsMu.RUnlock()
}

// newAPIKeyOriginRefreshProxy builds a production-shaped API-key-origin proxy: claims nil,
// apiKeyOnly=true, the connection tenant carried as the slug, and the API key's owning
// tenant carried as the UUID (matching how gateway.go wires it). The validator returns the
// supplied refreshed claims.
func newAPIKeyOriginRefreshProxy(connSlug, apiKeyUUID string, newClaims *auth.Claims) (proxy *Proxy, client, backend net.Conn) {
	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	return &Proxy{
		clientConn:            clientConn,
		backendConn:           backendConn,
		claims:                nil,
		tenantID:              connSlug,
		apiKeyOnly:            true,
		apiKeyTenantID:        apiKeyUUID,
		filterSubscribe:       testAllowTradeFilter(connSlug),
		canPublish:            testAllowTradePublish(connSlug),
		validator:             &mockTokenValidator{claims: newClaims},
		logger:                zerolog.Nop(),
		messageTimeout:        60 * time.Second,
		maxFrameSize:          protocol.DefaultMaxFrameSize,
		publishLimiter:        rate.NewLimiter(10, 100),
		maxPublishSize:        64 * 1024,
		authLimiter:           rate.NewLimiter(rate.Every(100*time.Millisecond), 1),
		authValidationTimeout: 5 * time.Second,
		subscribedChannels:    make(map[string]struct{}),
	}, clientRemote, backendRemote
}

// TestInterceptAuthRefresh_APIKeyOriginEscalation covers the previously-UNSATISFIABLE flow:
// an API-key-origin connection upgrades with a same-tenant JWT. Step 5 passes (slug matches)
// and step 5b passes (the JWT's resolved tenant UUID matches the API key's tenant UUID). The
// connection escalates and an auth_ack is sent. Before the fix, step 5b compared the JWT's
// slug against the API key's UUID and rejected every such upgrade.
func TestInterceptAuthRefresh_APIKeyOriginEscalation(t *testing.T) {
	t.Parallel()
	const (
		connSlug   = "acme"
		tenantUUID = "3700aa59-5546-4dc0-97dd-fca1f9e303a2"
	)
	newClaims := &auth.Claims{
		RegisteredClaims:   jwt.RegisteredClaims{Subject: "user1", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		TenantID:           connSlug,   // matches the connection slug → step 5 passes
		ResolvedTenantUUID: tenantUUID, // matches the API key's tenant UUID → step 5b passes
	}
	proxy, clientRemote, backendRemote := newAPIKeyOriginRefreshProxy(connSlug, tenantUUID, newClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// net.Pipe is unbuffered — CONTINUOUSLY drain the client conn (so the ack write never
	// blocks) and signal when an auth_ack frame appears. Asserting the ack (not a bare
	// drain) closes the allowed-side vacuous-pass gap.
	ackSeen := make(chan struct{}, 1)
	go func() {
		tmp := make([]byte, 4096)
		var acc []byte
		for {
			n, err := clientRemote.Read(tmp)
			if n > 0 {
				acc = append(acc, tmp[:n]...)
				if bytes.Contains(acc, []byte(RespTypeAuthAck)) {
					select {
					case ackSeen <- struct{}{}:
					default:
					}
				}
			}
			if err != nil {
				return
			}
		}
	}()

	msg := protocol.ClientMessage{Type: MsgTypeAuth, Data: json.RawMessage(`{"token":"same-tenant-jwt"}`)}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)
	if err != nil {
		t.Fatalf("interceptAuthRefresh error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result (handled, not forwarded)")
	}

	proxy.claimsMu.RLock()
	swapped := proxy.claims == newClaims
	stillAPIKeyOnly := proxy.apiKeyOnly
	proxy.claimsMu.RUnlock()
	if !swapped {
		t.Error("Claims should have been swapped to the new JWT claims")
	}
	if stillAPIKeyOnly {
		t.Error("apiKeyOnly should have flipped to false after successful escalation")
	}

	select {
	case <-ackSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("no auth_ack frame sent to client")
	}
}

// TestInterceptAuthRefresh_APIKeyOriginSlugCollisionRejected proves step 5b's independent
// value (why it's kept, not deleted): the refresh JWT's slug matches the connection (step 5
// passes) but it resolves to a DIFFERENT tenant UUID than the API key — a slug reused/
// reassigned across tenants (#170/#171). Step 5b (resolved UUID) rejects; step 5 (slug)
// alone would have wrongly escalated.
func TestInterceptAuthRefresh_APIKeyOriginSlugCollisionRejected(t *testing.T) {
	t.Parallel()
	const (
		connSlug   = "acme"
		apiKeyUUID = "11111111-1111-1111-1111-111111111111"
		otherUUID  = "22222222-2222-2222-2222-222222222222"
	)
	newClaims := &auth.Claims{
		RegisteredClaims:   jwt.RegisteredClaims{Subject: "user1", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		TenantID:           connSlug,  // slug matches the connection → step 5 PASSES
		ResolvedTenantUUID: otherUUID, // but resolves to a different tenant → step 5b REJECTS
	}
	proxy, clientRemote, backendRemote := newAPIKeyOriginRefreshProxy(connSlug, apiKeyUUID, newClaims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{Type: MsgTypeAuth, Data: json.RawMessage(`{"token":"slug-collision-jwt"}`)}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)
	if err != nil {
		t.Fatalf("interceptAuthRefresh error: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result (error sent to client, not forwarded)")
	}

	proxy.claimsMu.RLock()
	stillAPIKeyOnly := proxy.apiKeyOnly
	claimsUnchanged := proxy.claims == nil
	proxy.claimsMu.RUnlock()
	if !stillAPIKeyOnly {
		t.Error("apiKeyOnly must stay true — a slug-collision upgrade must NOT escalate")
	}
	if !claimsUnchanged {
		t.Error("Claims must not swap when step 5b rejects the upgrade")
	}
}

func TestInterceptAuthRefresh_RateLimited(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "test-tenant",
	}
	validator := &mockTokenValidator{claims: newClaims}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, claims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	// Use a very slow rate limiter
	proxy.authLimiter = rate.NewLimiter(rate.Every(time.Hour), 1)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"valid-token"}`),
	}

	// First call succeeds (consumes the burst token)
	_, _ = proxy.interceptAuthRefresh(context.Background(), msg)

	// Second call should be rate limited
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for rate limited request")
	}
}

func TestInterceptAuthRefresh_EmptyToken(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{claims: claims}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, claims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":""}`),
	}
	result, _ := proxy.interceptAuthRefresh(context.Background(), msg)

	if result != nil {
		t.Error("Expected nil result for empty token")
	}
}

func TestInterceptAuthRefresh_MissingData(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{claims: claims}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, claims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{}`),
	}
	result, _ := proxy.interceptAuthRefresh(context.Background(), msg)

	if result != nil {
		t.Error("Expected nil result for missing data")
	}
}

func TestInterceptAuthRefresh_NilValidator(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	proxy, clientRemote, backendRemote := newAuthTestProxy(nil, claims)
	proxy.validator = nil
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"valid-token"}`),
	}
	result, err := proxy.interceptAuthRefresh(context.Background(), msg)

	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for nil validator")
	}
}

func TestInterceptAuthRefresh_ValidatorError(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	validator := &mockTokenValidator{err: errors.New("unexpected error")}
	proxy, clientRemote, backendRemote := newAuthTestProxy(validator, claims)
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()
	go drainConn(clientRemote)

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"some-token"}`),
	}
	result, _ := proxy.interceptAuthRefresh(context.Background(), msg)

	if result != nil {
		t.Error("Expected nil result for validator error")
	}
}

// =============================================================================
// Forced Unsubscription Tests
// =============================================================================

func TestForceUnsubscribeRevokedChannels_NoRevocation(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	filterFn, canPublishFn, _ := testProxyAuthz("test-tenant", &types.ChannelRules{
		Public: []string{"*.trade", "*.liquidity"},
	})

	proxy := &Proxy{

		claims:             claims,
		tenantID:           "test-tenant",
		filterSubscribe:    filterFn,
		canPublish:         canPublishFn,
		logger:             zerolog.Nop(),
		subscribedChannels: map[string]struct{}{"test-tenant.BTC.trade": {}, "test-tenant.ETH.liquidity": {}},
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
	}

	// New claims have same permissions — no revocation
	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}

	revoked := proxy.forceUnsubscribeRevokedChannels(context.Background(), newClaims)

	if len(revoked) != 0 {
		t.Errorf("Expected 0 revoked channels, got %d: %v", len(revoked), revoked)
	}
	if len(proxy.subscribedChannels) != 2 {
		t.Errorf("Expected 2 subscribed channels, got %d", len(proxy.subscribedChannels))
	}
}

func TestForceUnsubscribeRevokedChannels_PartialRevocation(t *testing.T) {
	t.Parallel()
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}
	// Initial rules: trade + liquidity
	filterFn, canPublishFn, registry := testProxyAuthz("test-tenant", &types.ChannelRules{
		Public: []string{"*.trade", "*.liquidity"},
	})

	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims:          claims,
		tenantID:        "test-tenant",
		filterSubscribe: filterFn,
		canPublish:      canPublishFn,
		logger:          zerolog.Nop(),
		subscribedChannels: map[string]struct{}{
			"test-tenant.BTC.trade":     {},
			"test-tenant.ETH.liquidity": {},
		},
		authLimiter:    rate.NewLimiter(rate.Every(30*time.Second), 1),
		publishLimiter: rate.NewLimiter(10, 100),
		maxPublishSize: 64 * 1024,
	}

	// Rules tightened: only trade remains (liquidity revoked) — the natural
	// per-tenant flow, an operator updates the tenant's rules.
	registry.setRules("test-tenant", &types.ChannelRules{Public: []string{"*.trade"}})

	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}

	// Drain client and backend reads in background to prevent blocking
	go drainConn(clientRemote)
	go drainConn(backendRemote)

	revoked := proxy.forceUnsubscribeRevokedChannels(context.Background(), newClaims)

	if len(revoked) != 1 {
		t.Fatalf("Expected 1 revoked channel, got %d: %v", len(revoked), revoked)
	}
	if revoked[0] != "test-tenant.ETH.liquidity" {
		t.Errorf("Expected revoked channel test-tenant.ETH.liquidity, got %q", revoked[0])
	}
	// BTC.trade should still be tracked
	if _, ok := proxy.subscribedChannels["test-tenant.BTC.trade"]; !ok {
		t.Error("test-tenant.BTC.trade should still be in subscribedChannels")
	}
}

func TestForceUnsubscribeRevokedChannels_AllRevoked(t *testing.T) {
	t.Parallel()

	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	// No rules for the tenant → deny-all (everything revoked)
	filterFn, canPublishFn, _ := testProxyAuthz("test-tenant", nil)

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims: &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
			TenantID:         "test-tenant",
		},
		tenantID:        "test-tenant",
		filterSubscribe: filterFn,
		canPublish:      canPublishFn,
		logger:          zerolog.Nop(),
		subscribedChannels: map[string]struct{}{
			"test-tenant.BTC.trade":     {},
			"test-tenant.ETH.liquidity": {},
		},
		authLimiter:    rate.NewLimiter(rate.Every(30*time.Second), 1),
		publishLimiter: rate.NewLimiter(10, 100),
		maxPublishSize: 64 * 1024,
	}

	// Drain connections
	go drainConn(clientRemote)
	go drainConn(backendRemote)

	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}

	revoked := proxy.forceUnsubscribeRevokedChannels(context.Background(), newClaims)

	if len(revoked) != 2 {
		t.Fatalf("Expected 2 revoked channels, got %d: %v", len(revoked), revoked)
	}
	if len(proxy.subscribedChannels) != 0 {
		t.Errorf("Expected 0 subscribed channels, got %d", len(proxy.subscribedChannels))
	}
}

func TestForceUnsubscribeRevokedChannels_NoSubscriptions(t *testing.T) {
	t.Parallel()

	proxy := &Proxy{

		claims: &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
			TenantID:         "test-tenant",
		},
		tenantID:           "test-tenant",
		logger:             zerolog.Nop(),
		subscribedChannels: make(map[string]struct{}),
		authLimiter:        rate.NewLimiter(rate.Every(30*time.Second), 1),
		publishLimiter:     rate.NewLimiter(10, 100),
		maxPublishSize:     64 * 1024,
	}
	proxy.filterSubscribe, proxy.canPublish, _ = testProxyAuthz("test-tenant", &types.ChannelRules{
		Public: []string{"*.trade"},
	})

	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"},
		TenantID:         "test-tenant",
	}

	revoked := proxy.forceUnsubscribeRevokedChannels(context.Background(), newClaims)

	if revoked != nil {
		t.Errorf("Expected nil revoked, got %v", revoked)
	}
}

// =============================================================================
// Benchmark
// =============================================================================

func BenchmarkInterceptAuthRefresh(b *testing.B) {
	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		TenantID: "test-tenant",
	}
	newClaims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(2 * time.Hour)),
		},
		TenantID: "test-tenant",
	}
	validator := &mockTokenValidator{claims: newClaims}

	clientConn, clientRemote := net.Pipe()
	backendConn, backendRemote := net.Pipe()
	defer func() { _ = clientRemote.Close() }()
	defer func() { _ = backendRemote.Close() }()

	proxy := &Proxy{
		clientConn:  clientConn,
		backendConn: backendConn,

		claims:                claims,
		tenantID:              "test-tenant",
		validator:             validator,
		logger:                zerolog.Nop(),
		publishLimiter:        rate.NewLimiter(10, 100),
		maxPublishSize:        64 * 1024,
		authLimiter:           rate.NewLimiter(rate.Inf, 1), // No rate limit for benchmarks
		authValidationTimeout: 5 * time.Second,
		subscribedChannels:    make(map[string]struct{}),
	}
	proxy.filterSubscribe, proxy.canPublish, _ = testProxyAuthz("test-tenant", &types.ChannelRules{
		Public: []string{"*.trade"},
	})

	msg := protocol.ClientMessage{
		Type: MsgTypeAuth,
		Data: json.RawMessage(`{"token":"valid-jwt-token"}`),
	}

	// Drain client reads
	go drainConn(clientRemote)

	for b.Loop() {
		_, _ = proxy.interceptAuthRefresh(context.Background(), msg)
	}
}
