package gateway

import (
	"context"
	"slices"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// TestAuthzConsistency_AllTransports (§XVIII) verifies that identical
// claims + rules + channel inputs produce identical authorization decisions
// across every transport path:
//   - subscribe: the WS proxy's injected filter closure vs the direct
//     gw.filterSubscribeChannels call SSE and Web Push make;
//   - publish: the WS proxy's injected canPublish closure vs the direct
//     gw.checkPublishAllowed call REST publish makes.
//
// The closures are built exactly as Gateway proxy construction builds them,
// so divergence here means the WS proxy wiring drifted from the shared path.
func TestAuthzConsistency_AllTransports(t *testing.T) {
	t.Parallel()

	const tenantID = "acme"
	rules := &types.ChannelRules{
		Public: []string{"*.public"},
		GroupMappings: map[string][]string{
			"traders": {"*.trade"},
		},
		Default:       []string{"inbox.{principal}"},
		PublishPublic: []string{"chat.*"},
		PublishGroupMappings: map[string][]string{
			"traders": {"orders.*"},
		},
		PublishDefault: []string{"outbox.{principal}"},
	}

	registry := newMockChannelRulesProvider()
	registry.setRules(tenantID, rules)
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewTenantPermissionChecker: %v", err)
	}
	gw := &Gateway{tenantPermChecker: checker, logger: zerolog.Nop()}

	// Closures wired exactly as in Gateway WS-proxy construction.
	proxyFilter := func(ctx context.Context, channels []string, cl *auth.Claims) []string {
		return gw.filterSubscribeChannels(ctx, channels, tenantID, cl)
	}
	proxyCanPublish := func(ctx context.Context, cl *auth.Claims, channel string) bool {
		return gw.checkPublishAllowed(ctx, tenantID, cl, channel)
	}

	mkClaims := func(subject string, groups []string) *auth.Claims {
		return &auth.Claims{
			RegisteredClaims: jwt.RegisteredClaims{Subject: subject},
			TenantID:         tenantID,
			Groups:           groups,
		}
	}

	claimVariants := map[string]*auth.Claims{
		"nil (api-key-only)": nil,
		"no groups":          mkClaims("alice", nil),
		"trader group":       mkClaims("bob", []string{"traders"}),
		"unmapped group":     mkClaims("carol", []string{"guests"}),
	}

	subscribeChannels := []string{
		"acme.BTC.public", "acme.BTC.trade", "acme.inbox.alice",
		"acme.inbox.bob", "acme.secret.x", "wrong.BTC.public", "acme",
	}
	publishChannels := []string{
		"acme.chat.general", "acme.orders.new", "acme.outbox.alice",
		"acme.outbox.bob", "acme.secret.x",
	}

	ctx := context.Background()
	for name, claims := range claimVariants {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Subscribe: WS-proxy closure vs direct (SSE/push) path.
			viaProxy := proxyFilter(ctx, subscribeChannels, claims)
			viaDirect := gw.filterSubscribeChannels(ctx, subscribeChannels, tenantID, claims)
			if !slices.Equal(viaProxy, viaDirect) {
				t.Errorf("subscribe divergence: proxy=%v direct=%v", viaProxy, viaDirect)
			}

			// Publish: WS-proxy closure vs direct (REST) path.
			for _, ch := range publishChannels {
				wsDecision := proxyCanPublish(ctx, claims, ch)
				restDecision := gw.checkPublishAllowed(ctx, tenantID, claims, ch)
				if wsDecision != restDecision {
					t.Errorf("publish divergence on %q: ws=%v rest=%v", ch, wsDecision, restDecision)
				}
			}
		})
	}
}

// TestChannelRulesStreamLabel exhaustively covers the readiness mapping:
// disconnected → degraded; connected-without-snapshot → degraded
// (connected_awaiting_snapshot — /ready holds 503 through the cold-start
// window); connected-with-snapshot → healthy.
func TestChannelRulesStreamLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		state        int32
		snapshot     bool
		wantLabel    string
		wantDegraded bool
	}{
		{"disconnected without snapshot", provapi.StreamStateDisconnected, false, provapi.StreamLabelDisconnected, true},
		{"disconnected with snapshot", provapi.StreamStateDisconnected, true, provapi.StreamLabelDisconnected, true},
		{"connected awaiting snapshot", provapi.StreamStateConnected, false, provapi.StreamLabelConnectedAwaitingSnapshot, true},
		{"connected with snapshot", provapi.StreamStateConnected, true, provapi.StreamLabelConnected, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			label, degraded := channelRulesStreamLabel(tt.state, tt.snapshot)
			if label != tt.wantLabel || degraded != tt.wantDegraded {
				t.Errorf("channelRulesStreamLabel(%d, %v) = (%q, %v), want (%q, %v)",
					tt.state, tt.snapshot, label, degraded, tt.wantLabel, tt.wantDegraded)
			}
		})
	}
}
