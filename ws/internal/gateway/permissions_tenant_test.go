package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

func mustNewTenantPermissionChecker(t *testing.T, provider ChannelRulesProvider) *TenantPermissionChecker {
	t.Helper()
	checker, err := NewTenantPermissionChecker(provider, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewTenantPermissionChecker: %v", err)
	}
	return checker
}

// testTenantChecker builds a rules-backed tenant checker for the tenant.
// nil rules = tenant absent (deny-all).
func testTenantChecker(tenantID string, rules *types.ChannelRules) *TenantPermissionChecker {
	registry := newMockChannelRulesProvider()
	if rules != nil {
		registry.setRules(tenantID, rules)
	}
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		panic(err) // test helper: provider is never nil
	}
	return checker
}

// testProxyAuthz returns proxy authorization closures backed by a minimal
// real Gateway + tenant checker, so direct-Proxy tests exercise the
// production filter/publish path (§XVIII — same code as SSE/push/REST).
// The returned registry allows mid-test rules mutation (revocation scenarios).
func testProxyAuthz(tenantID string, rules *types.ChannelRules) (
	filter func(ctx context.Context, channels []string, claims *auth.Claims) []string,
	canPublish func(ctx context.Context, claims *auth.Claims, channel string) bool,
	registry *mockChannelRulesProvider,
) {
	registry = newMockChannelRulesProvider()
	if rules != nil {
		registry.setRules(tenantID, rules)
	}
	checker, err := NewTenantPermissionChecker(registry, zerolog.Nop())
	if err != nil {
		panic(err) // test helper: provider is never nil
	}
	gw := &Gateway{tenantPermChecker: checker, logger: zerolog.Nop()}
	filter = func(ctx context.Context, channels []string, claims *auth.Claims) []string {
		return gw.filterSubscribeChannels(ctx, channels, tenantID, claims)
	}
	canPublish = func(ctx context.Context, claims *auth.Claims, channel string) bool {
		return gw.checkPublishAllowed(ctx, tenantID, claims, channel)
	}
	return filter, canPublish, registry
}

func TestNewTenantPermissionChecker_NilProvider(t *testing.T) {
	t.Parallel()
	if _, err := NewTenantPermissionChecker(nil, zerolog.Nop()); err == nil {
		t.Fatal("expected error for nil provider (§I constructor validation)")
	}
}

// TestTenantPermissionChecker_CanSubscribe is the subscribe-side authorization
// matrix: public / group mapping / default / {principal} / deny variants.
func TestTenantPermissionChecker_CanSubscribe(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		Public: []string{"*.public", "news.*"},
		GroupMappings: map[string][]string{
			"traders": {"*.trade"},
		},
		Default: []string{"alerts.*", "inbox.{principal}"},
	})
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()

	jwtClaims := &auth.Claims{TenantID: "acme", Groups: []string{"traders"}}
	jwtClaims.Subject = "user123"
	noGroupClaims := &auth.Claims{TenantID: "acme"}
	noGroupClaims.Subject = "user456"

	tests := []struct {
		name    string
		claims  *auth.Claims
		channel string
		want    bool
	}{
		{"public pattern allowed", jwtClaims, "BTC.public", true},
		{"public prefix pattern allowed", jwtClaims, "news.breaking", true},
		{"group mapping allowed for member", jwtClaims, "BTC.trade", true},
		{"group mapping denied for non-member", noGroupClaims, "BTC.trade", false},
		// Default is the no-group-matched fallback tier (channel_rules.go
		// ComputeAllowedPatterns): users whose groups match a mapping skip it.
		{"default tier applies when no group matched", noGroupClaims, "alerts.system", true},
		{"default tier skipped for group-matched user", jwtClaims, "alerts.system", false},
		{"principal substitution allows own inbox (default tier)", noGroupClaims, "inbox.user456", true},
		{"principal substitution denies other inbox", noGroupClaims, "inbox.user123", false},
		{"no matching pattern denied", jwtClaims, "secret.channel", false},
		{"nil claims allowed on public only", nil, "BTC.public", true},
		{"nil claims denied on group channel", nil, "BTC.trade", false},
		{"nil claims denied on default channel", nil, "alerts.system", false},
		{"nil claims denied on principal channel", nil, "inbox.user123", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := checker.CanSubscribe(ctx, "acme", tt.claims, tt.channel); got != tt.want {
				t.Errorf("CanSubscribe(%q, claims=%v) = %v, want %v", tt.channel, tt.claims, got, tt.want)
			}
		})
	}
}

// TestTenantPermissionChecker_CanPublish is the publish-side matrix —
// side-specific rules, {principal}, and the JWT-required contract.
func TestTenantPermissionChecker_CanPublish(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		// Subscribe side deliberately different from publish side (asymmetry).
		Public:        []string{"*.public"},
		PublishPublic: []string{"chat.*"},
		PublishGroupMappings: map[string][]string{
			"writers": {"docs.*"},
		},
		PublishDefault: []string{"outbox.{principal}"},
	})
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()

	writer := &auth.Claims{TenantID: "acme", Groups: []string{"writers"}}
	writer.Subject = "alice"
	reader := &auth.Claims{TenantID: "acme"}
	reader.Subject = "bob"

	tests := []struct {
		name    string
		claims  *auth.Claims
		channel string
		want    bool
	}{
		{"publish public allowed", reader, "chat.general", true},
		{"publish group mapping allowed", writer, "docs.spec", true},
		{"publish group mapping denied for non-member", reader, "docs.spec", false},
		// PublishDefault is the no-group-matched fallback tier — writer's
		// group match skips it; reader (no matching groups) gets it.
		{"publish principal own outbox (default tier)", reader, "outbox.bob", true},
		{"publish principal other outbox denied", reader, "outbox.alice", false},
		{"group-matched user skips publish default tier", writer, "outbox.alice", false},
		{"subscribe-side pattern does NOT grant publish", reader, "BTC.public", false},
		{"nil claims always denied (publish requires JWT)", nil, "chat.general", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := checker.CanPublish(ctx, "acme", tt.claims, tt.channel); got != tt.want {
				t.Errorf("CanPublish(%q) = %v, want %v", tt.channel, got, tt.want)
			}
		})
	}
}

// TestTenantPermissionChecker_DenyAll covers no rules → deny everything;
// an empty rule side → deny that side.
func TestTenantPermissionChecker_DenyAll(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider() // snapshot done, no tenants
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()
	claims := &auth.Claims{TenantID: "ruleless"}
	claims.Subject = "u1"

	if checker.CanSubscribe(ctx, "ruleless", claims, "any.channel") {
		t.Error("ruleless tenant must be denied subscribe (deny-all, no fallback)")
	}
	if checker.CanPublish(ctx, "ruleless", claims, "any.channel") {
		t.Error("ruleless tenant must be denied publish (deny-all, no fallback)")
	}
	if got := checker.FilterChannels(ctx, "ruleless", claims, []string{"a.b", "c.d"}); len(got) != 0 {
		t.Errorf("FilterChannels for ruleless tenant = %v, want empty", got)
	}

	// Empty publish side with populated subscribe side: publish denied.
	registry.setRules("subonly", &types.ChannelRules{Public: []string{"*"}})
	if !checker.CanSubscribe(ctx, "subonly", claims, "any.channel") {
		t.Error("subscribe should be allowed by public wildcard")
	}
	if checker.CanPublish(ctx, "subonly", claims, "any.channel") {
		t.Error("empty publish side must deny all publishes (side-specific deny)")
	}
}

// TestTenantPermissionChecker_FailClosed covers the unknown-rules states:
// cold start (no snapshot) and stream-down-uncached both deny; cached tenants
// keep working when the stream drops (stale-serve).
func TestTenantPermissionChecker_FailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	claims := &auth.Claims{TenantID: "acme"}
	claims.Subject = "u1"

	t.Run("cold start denies uncached tenant", func(t *testing.T) {
		t.Parallel()
		registry := newMockChannelRulesProvider()
		registry.snapshotDone.Store(false) // initial snapshot not applied
		checker := mustNewTenantPermissionChecker(t, registry)
		if checker.CanSubscribe(ctx, "acme", claims, "a.b") {
			t.Error("must fail closed before the initial snapshot")
		}
	})

	t.Run("stream down denies uncached tenant", func(t *testing.T) {
		t.Parallel()
		registry := newMockChannelRulesProvider()
		registry.disconnected.Store(true)
		checker := mustNewTenantPermissionChecker(t, registry)
		if checker.CanSubscribe(ctx, "uncached", claims, "a.b") {
			t.Error("must fail closed for uncached tenant while stream is down")
		}
	})

	t.Run("stream down serves cached tenant stale", func(t *testing.T) {
		t.Parallel()
		registry := newMockChannelRulesProvider()
		registry.setRules("acme", &types.ChannelRules{Public: []string{"*"}})
		registry.disconnected.Store(true)
		checker := mustNewTenantPermissionChecker(t, registry)
		if !checker.CanSubscribe(ctx, "acme", claims, "a.b") {
			t.Error("cached rules must keep serving while the stream is down (stale-serve)")
		}
	})
}

func TestTenantPermissionChecker_FilterChannels(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		Public:  []string{"*.public"},
		Default: []string{"inbox.{principal}"},
	})
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()
	claims := &auth.Claims{TenantID: "acme"}
	claims.Subject = "user1"

	got := checker.FilterChannels(ctx, "acme", claims,
		[]string{"BTC.public", "inbox.user1", "inbox.user2", "secret.x"})
	want := []string{"BTC.public", "inbox.user1"}
	if len(got) != len(want) {
		t.Fatalf("FilterChannels = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FilterChannels = %v, want %v", got, want)
		}
	}

	// Nil claims: public only.
	gotNil := checker.FilterChannels(ctx, "acme", nil,
		[]string{"BTC.public", "inbox.user1"})
	if len(gotNil) != 1 || gotNil[0] != "BTC.public" {
		t.Fatalf("FilterChannels(nil claims) = %v, want [BTC.public]", gotNil)
	}
}

// TestTenantPermissionChecker_ConcurrentReadsAndWrites exercises the checker
// under concurrent authorization reads while the provider's rules mutate —
// the -race run is the §VII/§VIII enforcement.
func TestTenantPermissionChecker_ConcurrentReadsAndWrites(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{Public: []string{"*.public"}})
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()
	claims := &auth.Claims{TenantID: "acme", Groups: []string{"g1"}}
	claims.Subject = "u1"

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = checker.CanSubscribe(ctx, "acme", claims, "BTC.public")
				_ = checker.CanPublish(ctx, "acme", claims, "chat.x")
				_ = checker.FilterChannels(ctx, "acme", claims, []string{"BTC.public", "x.y"})
			}
		})
	}
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			registry.setRules("acme", &types.ChannelRules{
				Public:        []string{"*.public"},
				PublishPublic: []string{"chat.*"},
				GroupMappings: map[string][]string{"g1": {"*.trade"}},
			})
			if i%10 == 0 {
				registry.snapshotDone.Store(true)
			}
		}
	})

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestTenantPermissionChecker_AllBuiltinPlaceholders verifies every
// placeholder the platform validates (auth.DefaultPlaceholders) is resolved
// at match time — provisioning accepts these tokens in rules, so the gateway
// must resolve all of them, not just {principal}. Patterns left with an
// unresolved token are dropped (fail closed).
func TestTenantPermissionChecker_AllBuiltinPlaceholders(t *testing.T) {
	t.Parallel()

	registry := newMockChannelRulesProvider()
	registry.setRules("acme", &types.ChannelRules{
		Default: []string{
			"inbox.{principal}",
			"mail.{sub}",
			"files.{user_id}",
			"scope.{tenant_id}.alerts",
			"org.{tenant}.news",
			"app.{app_id}.events",
		},
	})
	checker := mustNewTenantPermissionChecker(t, registry)
	ctx := context.Background()

	claims := &auth.Claims{TenantID: "acme"}
	claims.Subject = "user1"

	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{"principal resolves to subject", "inbox.user1", true},
		{"sub resolves to subject", "mail.user1", true},
		{"user_id resolves to subject", "files.user1", true},
		{"tenant_id resolves to tenant", "scope.acme.alerts", true},
		{"tenant resolves to tenant", "org.acme.news", true},
		{"app_id resolves to subject", "app.user1.events", true},
		{"other subject denied", "inbox.user2", false},
		{"other tenant denied", "scope.other.alerts", false},
		{"literal placeholder channel denied", "inbox.{principal}", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := checker.CanSubscribe(ctx, "acme", claims, tt.channel); got != tt.want {
				t.Errorf("CanSubscribe(%q) = %v, want %v", tt.channel, got, tt.want)
			}
		})
	}

	// Nil claims: every placeholder pattern is dropped — nothing matches,
	// including the literal token channel (fail closed).
	if checker.CanSubscribe(ctx, "acme", nil, "inbox.{principal}") {
		t.Error("nil claims must not match a literal placeholder channel")
	}
}
