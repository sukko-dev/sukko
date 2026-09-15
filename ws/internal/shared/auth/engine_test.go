package auth

import (
	"context"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// mustNewPolicyEngine is a test helper that creates a PolicyEngine or fails the test.
func mustNewPolicyEngine(t *testing.T, config PolicyEngineConfig, opts ...PolicyEngineOption) *PolicyEngine {
	t.Helper()
	engine, err := NewPolicyEngine(config, opts...)
	if err != nil {
		t.Fatalf("NewPolicyEngine: %v", err)
	}
	return engine
}

func TestNewPolicyEngine(t *testing.T) {
	t.Parallel()
	t.Run("with default config", func(t *testing.T) {
		t.Parallel()
		engine := mustNewPolicyEngine(t, DefaultPolicyEngineConfig())
		if engine == nil {
			t.Fatal("expected non-nil engine")
		}
		if engine.defaultEffect != EffectDeny {
			t.Errorf("expected default effect deny, got %v", engine.defaultEffect)
		}
	})

	t.Run("with custom default effect", func(t *testing.T) {
		t.Parallel()
		engine := mustNewPolicyEngine(t, PolicyEngineConfig{
			DefaultEffect: EffectAllow,
		})
		if engine.defaultEffect != EffectAllow {
			t.Errorf("expected default effect allow, got %v", engine.defaultEffect)
		}
	})

	t.Run("with rules", func(t *testing.T) {
		t.Parallel()
		engine := mustNewPolicyEngine(t, PolicyEngineConfig{
			Rules: []*PermissionRule{
				{ID: "rule1", Priority: 10},
				{ID: "rule2", Priority: 20},
			},
		})
		// Should be sorted by priority (highest first)
		if engine.rules[0].ID != "rule2" {
			t.Error("expected rules to be sorted by priority")
		}
	})

	t.Run("invalid pattern returns error", func(t *testing.T) {
		t.Parallel()
		_, err := NewPolicyEngine(PolicyEngineConfig{
			Rules: []*PermissionRule{
				{
					ID:    "bad-pattern",
					Match: RuleMatch{ChannelPattern: "[invalid"},
				},
			},
		})
		if err == nil {
			t.Error("expected error for invalid regex pattern")
		}
	})
}

func TestPolicyEngine_Authorize_NoRules(t *testing.T) {
	t.Parallel()
	t.Run("default deny", func(t *testing.T) {
		t.Parallel()
		engine := mustNewPolicyEngine(t, PolicyEngineConfig{
			DefaultEffect: EffectDeny,
		})

		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "acme.trade",
		})

		if result.Allowed {
			t.Error("expected default deny to reject")
		}
	})

	t.Run("default allow", func(t *testing.T) {
		t.Parallel()
		engine := mustNewPolicyEngine(t, PolicyEngineConfig{
			DefaultEffect: EffectAllow,
		})

		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "acme.trade",
		})

		if !result.Allowed {
			t.Error("expected default allow to permit")
		}
	})
}

func TestPolicyEngine_Authorize_ExactMatch(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "exact-match",
				Match:   RuleMatch{ChannelExact: "public.announcements"},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
		},
	})

	tests := []struct {
		channel string
		allowed bool
	}{
		{"public.announcements", true},
		{"public.other", false},
		{"private.announcements", false},
	}

	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			t.Parallel()
			result := engine.Authorize(&AuthzRequest{
				Claims:  &Claims{TenantID: "acme"},
				Action:  RuleSubscribe,
				Channel: tt.channel,
			})

			if result.Allowed != tt.allowed {
				t.Errorf("channel %q: expected %v, got %v", tt.channel, tt.allowed, result.Allowed)
			}
		})
	}
}

func TestPolicyEngine_Authorize_PrefixMatch(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "prefix-match",
				Match:   RuleMatch{ChannelPrefix: "public."},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
		},
	})

	tests := []struct {
		channel string
		allowed bool
	}{
		{"public.announcements", true},
		{"public.events", true},
		{"private.events", false},
	}

	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			t.Parallel()
			result := engine.Authorize(&AuthzRequest{
				Claims:  &Claims{TenantID: "acme"},
				Action:  RuleSubscribe,
				Channel: tt.channel,
			})

			if result.Allowed != tt.allowed {
				t.Errorf("channel %q: expected %v, got %v", tt.channel, tt.allowed, result.Allowed)
			}
		})
	}
}

func TestPolicyEngine_Authorize_PatternMatch(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "pattern-match",
				Match:   RuleMatch{ChannelPattern: "{tenant_id}.*.trade"},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
		},
	})

	tests := []struct {
		channel string
		allowed bool
	}{
		{"acme.BTC.trade", true},
		{"acme.ETH.trade", true},
		{"globex.BTC.trade", true},
		{"acme.BTC.liquidity", false},
		{"acme.trade", false},
	}

	for _, tt := range tests {
		t.Run(tt.channel, func(t *testing.T) {
			t.Parallel()
			result := engine.Authorize(&AuthzRequest{
				Claims:  &Claims{TenantID: "acme"},
				Action:  RuleSubscribe,
				Channel: tt.channel,
			})

			if result.Allowed != tt.allowed {
				t.Errorf("channel %q: expected %v, got %v (reason: %s)",
					tt.channel, tt.allowed, result.Allowed, result.Reason)
			}
		})
	}
}

func TestPolicyEngine_Authorize_PlaceholderResolution(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "user-channel",
				Match:   RuleMatch{ChannelExact: "{tenant_id}.{user_id}.balances"},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
		},
	})

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user123"},
		TenantID:         "acme",
	}

	t.Run("matches resolved channel", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  claims,
			Action:  RuleSubscribe,
			Channel: "acme.user123.balances",
		})

		if !result.Allowed {
			t.Errorf("expected allowed, got denied: %s", result.Reason)
		}
	})

	t.Run("rejects wrong user", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  claims,
			Action:  RuleSubscribe,
			Channel: "acme.other.balances",
		})

		if result.Allowed {
			t.Error("expected denied for wrong user")
		}
	})
}

func TestPolicyEngine_Authorize_ActionFilter(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "subscribe-only",
				Match:   RuleMatch{ChannelPrefix: "public."},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
		},
	})

	t.Run("subscribe allowed", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "public.events",
		})
		if !result.Allowed {
			t.Error("expected subscribe to be allowed")
		}
	})

	t.Run("publish denied", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RulePublish,
			Channel: "public.events",
		})
		if result.Allowed {
			t.Error("expected publish to be denied")
		}
	})
}

func TestPolicyEngine_Authorize_Conditions(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "admin-only",
				Match:   RuleMatch{ChannelPrefix: "admin."},
				Actions: []Action{RuleSubscribe},
				Conditions: []Condition{
					{
						Type:  ConditionTypeClaim,
						Field: "roles",
						Op:    OpContains,
						Value: "admin",
					},
				},
				Effect: EffectAllow,
			},
		},
	})

	t.Run("admin role allowed", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme", Roles: []string{"admin"}},
			Action:  RuleSubscribe,
			Channel: "admin.dashboard",
		})
		if !result.Allowed {
			t.Error("expected admin to be allowed")
		}
	})

	t.Run("non-admin denied", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme", Roles: []string{"user"}},
			Action:  RuleSubscribe,
			Channel: "admin.dashboard",
		})
		if result.Allowed {
			t.Error("expected non-admin to be denied")
		}
	})
}

func TestPolicyEngine_Authorize_Priority(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:       "allow-all",
				Priority: 10,
				Match:    RuleMatch{ChannelPrefix: "public."},
				Effect:   EffectAllow,
			},
			{
				ID:       "deny-secret",
				Priority: 20, // Higher priority
				Match:    RuleMatch{ChannelExact: "public.secret"},
				Effect:   EffectDeny,
			},
		},
	})

	t.Run("higher priority rule wins", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "public.secret",
		})
		if result.Allowed {
			t.Error("expected higher priority deny to win")
		}
		if result.MatchedRule.ID != "deny-secret" {
			t.Errorf("expected deny-secret rule, got %s", result.MatchedRule.ID)
		}
	})

	t.Run("lower priority allows other channels", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "public.events",
		})
		if !result.Allowed {
			t.Error("expected allow rule to permit")
		}
	})
}

func TestPolicyEngine_Authorize_TenantRules(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:     "global-allow",
				Match:  RuleMatch{ChannelPrefix: "public."},
				Effect: EffectAllow,
			},
		},
		TenantRules: map[string][]*PermissionRule{
			"premium": {
				{
					ID:       "premium-extra",
					Priority: 100,
					Match:    RuleMatch{ChannelPrefix: "premium."},
					Effect:   EffectAllow,
				},
			},
		},
	})

	t.Run("premium tenant gets extra access", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "premium"},
			Action:  RuleSubscribe,
			Channel: "premium.vip",
		})
		if !result.Allowed {
			t.Error("expected premium tenant to access premium channels")
		}
	})

	t.Run("regular tenant denied premium", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "basic"},
			Action:  RuleSubscribe,
			Channel: "premium.vip",
		})
		if result.Allowed {
			t.Error("expected basic tenant to be denied premium channels")
		}
	})
}

func TestPolicyEngine_Authorize_Captures(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:     "capture-test",
				Match:  RuleMatch{ChannelPattern: "{tenant}.{symbol}.{event}"},
				Effect: EffectAllow,
			},
		},
	})

	result := engine.Authorize(&AuthzRequest{
		Claims:  &Claims{TenantID: "acme"},
		Action:  RuleSubscribe,
		Channel: "acme.BTC.trade",
	})

	if !result.Allowed {
		t.Fatal("expected allowed")
	}

	if result.Captures["tenant"] != "acme" {
		t.Errorf("expected tenant=acme, got %q", result.Captures["tenant"])
	}
	if result.Captures["symbol"] != "BTC" {
		t.Errorf("expected symbol=BTC, got %q", result.Captures["symbol"])
	}
	if result.Captures["event"] != "trade" {
		t.Errorf("expected event=trade, got %q", result.Captures["event"])
	}
}

func TestPolicyEngine_Authorize_NegateCondition(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:    "not-banned",
				Match: RuleMatch{ChannelPrefix: "public."},
				Conditions: []Condition{
					{
						Type:   ConditionTypeClaim,
						Field:  "roles",
						Op:     OpContains,
						Value:  "banned",
						Negate: true, // NOT banned
					},
				},
				Effect: EffectAllow,
			},
		},
	})

	t.Run("non-banned allowed", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme", Roles: []string{"user"}},
			Action:  RuleSubscribe,
			Channel: "public.events",
		})
		if !result.Allowed {
			t.Error("expected non-banned user to be allowed")
		}
	})

	t.Run("banned denied", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme", Roles: []string{"banned"}},
			Action:  RuleSubscribe,
			Channel: "public.events",
		})
		if result.Allowed {
			t.Error("expected banned user to be denied")
		}
	})
}

func TestPolicyEngine_CompareValues(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, DefaultPolicyEngineConfig())

	tests := []struct {
		name       string
		fieldValue any
		op         Operator
		condValue  any
		expected   bool
	}{
		// Exists
		{"exists_true", "value", OpExists, nil, true},
		{"exists_false", "", OpExists, nil, false},
		{"exists_nil", nil, OpExists, nil, false},

		// Equals
		{"eq_string", "hello", OpEquals, "hello", true},
		{"eq_string_fail", "hello", OpEquals, "world", false},
		{"eq_int", 42, OpEquals, "42", true},

		// NotEquals
		{"neq_true", "hello", OpNotEquals, "world", true},
		{"neq_false", "hello", OpNotEquals, "hello", false},

		// Contains (slice)
		{"contains_slice_true", []string{"a", "b", "c"}, OpContains, "b", true},
		{"contains_slice_false", []string{"a", "b", "c"}, OpContains, "d", false},

		// Contains (string)
		{"contains_string_true", "hello world", OpContains, "world", true},
		{"contains_string_false", "hello world", OpContains, "foo", false},

		// In
		{"in_true", "b", OpIn, []string{"a", "b", "c"}, true},
		{"in_false", "d", OpIn, []string{"a", "b", "c"}, false},

		// Matches
		{"matches_true", "user123", OpMatches, "^user\\d+$", true},
		{"matches_false", "admin", OpMatches, "^user\\d+$", false},

		// StartsWith
		{"starts_with_true", "hello world", OpStartsWith, "hello", true},
		{"starts_with_false", "hello world", OpStartsWith, "world", false},

		// EndsWith
		{"ends_with_true", "hello world", OpEndsWith, "world", true},
		{"ends_with_false", "hello world", OpEndsWith, "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := engine.compareValues(tt.fieldValue, tt.op, tt.condValue)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v, %v) = %v, want %v",
					tt.fieldValue, tt.op, tt.condValue, result, tt.expected)
			}
		})
	}
}

func TestPolicyEngine_AddRemoveRule(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, DefaultPolicyEngineConfig())

	// Add rule
	rule := &PermissionRule{
		ID:     "test-rule",
		Match:  RuleMatch{ChannelPrefix: "test."},
		Effect: EffectAllow,
	}
	if err := engine.AddRule(rule); err != nil {
		t.Fatalf("AddRule: %v", err)
	}

	// Verify added
	if got := engine.GetRule("test-rule"); got == nil {
		t.Fatal("expected rule to be added")
	}

	// List rules
	rules := engine.ListRules()
	if len(rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(rules))
	}

	// Remove rule
	if !engine.RemoveRule("test-rule") {
		t.Error("expected removal to succeed")
	}

	// Verify removed
	if got := engine.GetRule("test-rule"); got != nil {
		t.Error("expected rule to be removed")
	}

	// Remove non-existent
	if engine.RemoveRule("non-existent") {
		t.Error("expected removal of non-existent to fail")
	}

	// AddRule with invalid pattern returns error
	badRule := &PermissionRule{
		ID:    "bad-rule",
		Match: RuleMatch{ChannelPattern: "[invalid"},
	}
	if err := engine.AddRule(badRule); err == nil {
		t.Error("expected AddRule to return error for invalid pattern")
	}
}

func TestPolicyEngine_CanSubscribe_CanPublish(t *testing.T) {
	t.Parallel()
	engine := mustNewPolicyEngine(t, PolicyEngineConfig{
		DefaultEffect: EffectDeny,
		Rules: []*PermissionRule{
			{
				ID:      "subscribe-allow",
				Match:   RuleMatch{ChannelPrefix: "public."},
				Actions: []Action{RuleSubscribe},
				Effect:  EffectAllow,
			},
			{
				ID:      "publish-allow",
				Match:   RuleMatch{ChannelPrefix: "user."},
				Actions: []Action{RulePublish},
				Effect:  EffectAllow,
			},
		},
	})

	ctx := context.Background()
	claims := &Claims{TenantID: "acme"}

	// CanSubscribe
	if !engine.CanSubscribe(ctx, claims, "public.events") {
		t.Error("expected CanSubscribe to return true for public channel")
	}
	if engine.CanSubscribe(ctx, claims, "private.events") {
		t.Error("expected CanSubscribe to return false for private channel")
	}

	// CanPublish
	if !engine.CanPublish(ctx, claims, "user.events") {
		t.Error("expected CanPublish to return true for user channel")
	}
	if engine.CanPublish(ctx, claims, "public.events") {
		t.Error("expected CanPublish to return false for public channel")
	}
}

func TestPolicyEngine_WithTenantIsolator(t *testing.T) {
	t.Parallel()
	isolator, err := NewTenantIsolator(TenantIsolationConfig{Environment: "local"})
	if err != nil {
		t.Fatalf("NewTenantIsolator() unexpected error: %v", err)
	}

	engine := mustNewPolicyEngine(t,
		PolicyEngineConfig{
			DefaultEffect: EffectAllow,
		},
		WithPolicyTenantIsolator(isolator),
	)

	t.Run("same tenant allowed", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "acme.trade",
		})
		if !result.Allowed {
			t.Errorf("expected same tenant to be allowed: %s", result.Reason)
		}
	})

	t.Run("different tenant denied", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme"},
			Action:  RuleSubscribe,
			Channel: "globex.trade",
		})
		if result.Allowed {
			t.Error("expected different tenant to be denied")
		}
	})

	t.Run("admin cross-tenant denied (cross-tenant roles removed)", func(t *testing.T) {
		t.Parallel()
		result := engine.Authorize(&AuthzRequest{
			Claims:  &Claims{TenantID: "acme", Roles: []string{"admin"}},
			Action:  RuleSubscribe,
			Channel: "globex.trade",
		})
		if result.Allowed {
			t.Error("expected admin cross-tenant to be denied (cross-tenant roles removed)")
		}
	})
}

func TestDefaultPolicyEngineConfig(t *testing.T) {
	t.Parallel()
	config := DefaultPolicyEngineConfig()

	if config.DefaultEffect != EffectDeny {
		t.Errorf("expected default effect deny, got %v", config.DefaultEffect)
	}
	if len(config.Rules) != 0 {
		t.Errorf("expected no rules, got %d", len(config.Rules))
	}
	if config.TenantRules == nil {
		t.Error("expected TenantRules to be initialized")
	}
}
