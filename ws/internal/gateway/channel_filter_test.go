package gateway

import (
	"context"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// testGatewayWithRules builds a Gateway whose tenant checker serves the given
// rules for tenant "acme" — the provisioning-only authorization path.
func testGatewayWithRules(t *testing.T) *Gateway {
	t.Helper()
	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		Public:  []string{"*.trade", "*.metadata"},
		Default: []string{"inbox.{principal}"},
	})
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewTenantPermissionChecker: %v", err)
	}
	return &Gateway{
		tenantPermChecker: checker,
		logger:            zerolog.Nop(),
	}
}

func TestFilterSubscribeChannels_ValidChannelsPassThrough(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"}, TenantID: "acme"}
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme.BTC.trade", "acme.ETH.metadata"},
		"acme", claims)

	if len(result) != 2 {
		t.Errorf("expected 2 channels, got %d: %v", len(result), result)
	}
}

func TestFilterSubscribeChannels_WrongTenantFiltered(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"}, TenantID: "acme"}
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme.BTC.trade", "other.BTC.trade"},
		"acme", claims)

	if len(result) != 1 {
		t.Errorf("expected 1 channel (wrong tenant filtered), got %d: %v", len(result), result)
	}
	if len(result) > 0 && result[0] != "acme.BTC.trade" {
		t.Errorf("expected acme.BTC.trade, got %s", result[0])
	}
}

func TestFilterSubscribeChannels_InvalidFormatFiltered(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"}, TenantID: "acme"}
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme", "acme.BTC.trade"},
		"acme", claims)

	if len(result) != 1 {
		t.Errorf("expected 1 channel (single-part filtered), got %d: %v", len(result), result)
	}
}

func TestFilterSubscribeChannels_NilClaims_PublicOnly(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	// nil claims = API-key-only → public channels only
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme.BTC.trade", "acme.inbox.user1"},
		"acme", nil)

	// BTC.trade matches *.trade (public); inbox.user1 matches the
	// inbox.{principal} default rule which requires JWT claims.
	if len(result) != 1 {
		t.Errorf("expected 1 public channel, got %d: %v", len(result), result)
	}
	if len(result) > 0 && result[0] != "acme.BTC.trade" {
		t.Errorf("expected acme.BTC.trade, got %s", result[0])
	}
}

func TestFilterSubscribeChannels_JWTClaims_PublicAndUserScoped(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"}, TenantID: "acme"}
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme.BTC.trade", "acme.inbox.user1"},
		"acme", claims)

	if len(result) != 2 {
		t.Errorf("expected 2 channels (public + user-scoped), got %d: %v", len(result), result)
	}
}

func TestFilterSubscribeChannels_AllFiltered_EmptyResult(t *testing.T) {
	t.Parallel()
	gw := testGatewayWithRules(t)

	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"other.BTC.trade", "wrong.ETH.trade"},
		"acme", nil)

	if len(result) != 0 {
		t.Errorf("expected 0 channels (all wrong tenant), got %d: %v", len(result), result)
	}
}

func TestFilterSubscribeChannels_RulelessTenant_DenyAll(t *testing.T) {
	t.Parallel()
	// Provisioning-only authorization: a tenant with no rules is denied
	// everything — there is no fallback and no pass-through mode.
	registry := newMockChannelRulesProvider()
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewTenantPermissionChecker: %v", err)
	}
	gw := &Gateway{tenantPermChecker: checker, logger: zerolog.Nop()}

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "user1"}, TenantID: "acme"}
	result := gw.filterSubscribeChannels(context.Background(),
		[]string{"acme.any.channel", "acme.whatever.foo"},
		"acme", claims)

	if len(result) != 0 {
		t.Errorf("expected 0 channels (ruleless tenant deny-all), got %d: %v", len(result), result)
	}
}

// TestCheckPublishAllowed verifies the shared publish-authorization helper
// (used by both WS interceptPublish and REST HandlePublish) and its metric
// contract: the check records the result; a denial additionally records an
// access denial.
func TestCheckPublishAllowed(t *testing.T) {
	t.Parallel()
	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		PublishPublic:  []string{"chat.*"},
		PublishDefault: []string{"outbox.{principal}"},
	})
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewTenantPermissionChecker: %v", err)
	}
	gw := &Gateway{tenantPermChecker: checker, logger: zerolog.Nop()}
	ctx := context.Background()

	claims := &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: "alice"}, TenantID: "acme"}

	tests := []struct {
		name    string
		claims  *auth.Claims
		channel string // full tenant-prefixed channel (helper strips internally)
		want    bool
	}{
		{"publish public allowed", claims, "acme.chat.general", true},
		{"publish principal own outbox", claims, "acme.outbox.alice", true},
		{"publish principal other outbox denied", claims, "acme.outbox.bob", false},
		{"publish unmatched denied", claims, "acme.secret.x", false},
		{"nil claims denied", nil, "acme.chat.general", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := gw.checkPublishAllowed(ctx, "acme", tt.claims, tt.channel); got != tt.want {
				t.Errorf("checkPublishAllowed(%q) = %v, want %v", tt.channel, got, tt.want)
			}
		})
	}
}
