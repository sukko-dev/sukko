package auth

import (
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestNewPlaceholderResolver(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()

	if r == nil {
		t.Fatal("expected non-nil resolver")
	}
	if r.builtins == nil {
		t.Error("expected builtins to be initialized")
	}
	if r.custom == nil {
		t.Error("expected custom to be initialized")
	}
}

func TestPlaceholderResolver_Resolve(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()

	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user123",
		},
		TenantID: "acme",
		Attributes: map[string]string{
			"tier":   "premium",
			"region": "us-east",
		},
	}

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "tenant_id placeholder",
			pattern:  "{tenant_id}.BTC.trade",
			expected: "acme.BTC.trade",
		},
		{
			name:     "user_id placeholder",
			pattern:  "{tenant_id}.{user_id}.balances",
			expected: "acme.user123.balances",
		},
		{
			name:     "sub placeholder",
			pattern:  "{sub}.notifications",
			expected: "user123.notifications",
		},
		{
			name:     "tenant alias",
			pattern:  "{tenant}.events",
			expected: "acme.events",
		},
		{
			name:     "attribute from attrs",
			pattern:  "{tier}.channels",
			expected: "premium.channels",
		},
		{
			name:     "multiple placeholders",
			pattern:  "{tenant_id}.{user_id}.{tier}",
			expected: "acme.user123.premium",
		},
		{
			name:     "no placeholders",
			pattern:  "all.trade",
			expected: "all.trade",
		},
		{
			name:     "unknown placeholder kept",
			pattern:  "{unknown}.channel",
			expected: "{unknown}.channel",
		},
		{
			name:     "empty pattern",
			pattern:  "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := r.Resolve(tt.pattern, claims)
			if result != tt.expected {
				t.Errorf("Resolve(%q) = %q, want %q", tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestPlaceholderResolver_Resolve_NilClaims(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()

	result := r.Resolve("{tenant_id}.trade", nil)
	if result != "{tenant_id}.trade" {
		t.Errorf("expected pattern unchanged with nil claims, got %q", result)
	}
}

func TestPlaceholderResolver_RegisterCustom(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()

	// Register custom placeholder
	r.RegisterCustom("org", func(_ *Claims) string {
		return "custom-org"
	})

	claims := &Claims{
		TenantID: "acme",
	}

	result := r.Resolve("{org}.{tenant_id}.channel", claims)
	if result != "custom-org.acme.channel" {
		t.Errorf("expected custom placeholder to work, got %q", result)
	}
}

func TestPlaceholderResolver_RegisterCustom_OverridesBuiltin(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()

	// Override built-in tenant_id
	r.RegisterCustom("tenant_id", func(_ *Claims) string {
		return "custom-tenant"
	})

	claims := &Claims{
		TenantID: "acme",
	}

	result := r.Resolve("{tenant_id}.channel", claims)
	if result != "custom-tenant.channel" {
		t.Errorf("expected custom to override builtin, got %q", result)
	}
}

func TestPlaceholderResolver_RegisterAttribute(t *testing.T) {
	t.Parallel()
	r := NewPlaceholderResolver()
	r.RegisterAttribute("tier")

	claims := &Claims{
		Attributes: map[string]string{
			"tier": "gold",
		},
	}

	result := r.Resolve("{tier}.vip.channel", claims)
	if result != "gold.vip.channel" {
		t.Errorf("expected attribute placeholder to work, got %q", result)
	}
}

func TestHasPlaceholders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern  string
		expected bool
	}{
		{"{tenant_id}.trade", true},
		{"{a}.{b}.{c}", true},
		{"all.trade", false},
		{"", false},
		{"{}", false},    // Invalid placeholder format
		{"{123}", false}, // Placeholders must start with letter/underscore
		{"{_valid}", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()
			result := HasPlaceholders(tt.pattern)
			if result != tt.expected {
				t.Errorf("HasPlaceholders(%q) = %v, want %v", tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestExtractPlaceholderNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern  string
		expected []string
	}{
		{"{tenant_id}.trade", []string{"tenant_id"}},
		{"{tenant_id}.{user_id}.balances", []string{"tenant_id", "user_id"}},
		{"{a}.{b}.{a}", []string{"a", "b"}}, // Duplicates removed
		{"all.trade", []string{}},
		{"", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()
			result := ExtractPlaceholderNames(tt.pattern)
			if len(result) != len(tt.expected) {
				t.Errorf("ExtractPlaceholderNames(%q) = %v, want %v", tt.pattern, result, tt.expected)
				return
			}
			for i, name := range result {
				if name != tt.expected[i] {
					t.Errorf("ExtractPlaceholderNames(%q)[%d] = %q, want %q", tt.pattern, i, name, tt.expected[i])
				}
			}
		})
	}
}

func TestMatchPattern(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		channel  string
		matched  bool
		captures map[string]string
	}{
		{
			name:     "simple tenant match",
			pattern:  "{tenant_id}.BTC.trade",
			channel:  "acme.BTC.trade",
			matched:  true,
			captures: map[string]string{"tenant_id": "acme"},
		},
		{
			name:     "multiple captures",
			pattern:  "{tenant_id}.{symbol}.trade",
			channel:  "acme.BTC.trade",
			matched:  true,
			captures: map[string]string{"tenant_id": "acme", "symbol": "BTC"},
		},
		{
			name:     "user scoped channel",
			pattern:  "{tenant_id}.{user_id}.balances",
			channel:  "acme.user123.balances",
			matched:  true,
			captures: map[string]string{"tenant_id": "acme", "user_id": "user123"},
		},
		{
			name:     "no match - different literal",
			pattern:  "{tenant_id}.BTC.trade",
			channel:  "acme.BTC.liquidity",
			matched:  false,
			captures: map[string]string{},
		},
		{
			name:     "no match - too few parts",
			pattern:  "{tenant_id}.{symbol}.trade",
			channel:  "acme.trade",
			matched:  false,
			captures: map[string]string{},
		},
		{
			name:     "no match - too many parts",
			pattern:  "{tenant_id}.trade",
			channel:  "acme.BTC.trade",
			matched:  false,
			captures: map[string]string{},
		},
		{
			name:     "literal pattern match",
			pattern:  "system.broadcast",
			channel:  "system.broadcast",
			matched:  true,
			captures: map[string]string{},
		},
		{
			name:     "literal pattern no match",
			pattern:  "system.broadcast",
			channel:  "system.other",
			matched:  false,
			captures: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := MatchPattern(tt.pattern, tt.channel)

			if result.Matched != tt.matched {
				t.Errorf("MatchPattern(%q, %q).Matched = %v, want %v",
					tt.pattern, tt.channel, result.Matched, tt.matched)
			}

			if tt.matched {
				for key, expected := range tt.captures {
					if got := result.Captures[key]; got != expected {
						t.Errorf("MatchPattern(%q, %q).Captures[%q] = %q, want %q",
							tt.pattern, tt.channel, key, got, expected)
					}
				}
			}
		})
	}
}

func TestMatchPattern_InvalidRegex(t *testing.T) {
	t.Parallel()

	// Pattern that produces an invalid regex (unmatched bracket after escaping)
	result := MatchPattern("[invalid", "test")
	if result.Matched {
		t.Error("expected no match for invalid pattern")
	}
	if result.Error == nil {
		t.Error("expected Error to be set for invalid pattern")
	}
}

func TestBuildPattern(t *testing.T) {
	t.Parallel()
	tests := []struct {
		parts    []string
		expected string
	}{
		{[]string{"{tenant_id}", "trade"}, "{tenant_id}.trade"},
		{[]string{"{tenant_id}", "{user_id}", "balances"}, "{tenant_id}.{user_id}.balances"},
		{[]string{"system", "broadcast"}, "system.broadcast"},
		{[]string{"single"}, "single"},
		{[]string{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			result := BuildPattern(tt.parts...)
			if result != tt.expected {
				t.Errorf("BuildPattern(%v) = %q, want %q", tt.parts, result, tt.expected)
			}
		})
	}
}

func TestDefaultPlaceholders(t *testing.T) {
	t.Parallel()
	claims := &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user123",
		},
		TenantID: "acme",
	}

	tests := []struct {
		name     string
		expected string
	}{
		{"user_id", "user123"},
		{"tenant_id", "acme"},
		{"tenant", "acme"},
		{"sub", "user123"},
		{"app_id", "user123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fn, ok := DefaultPlaceholders[tt.name]
			if !ok {
				t.Fatalf("DefaultPlaceholders[%q] not found", tt.name)
			}
			result := fn(claims)
			if result != tt.expected {
				t.Errorf("DefaultPlaceholders[%q](claims) = %q, want %q", tt.name, result, tt.expected)
			}
		})
	}
}
