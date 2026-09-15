package auth

import (
	"testing"
)

func mustNewTopicIsolator(t *testing.T, config TopicIsolationConfig) *TopicIsolator {
	t.Helper()
	iso, err := NewTopicIsolator(config)
	if err != nil {
		t.Fatalf("NewTopicIsolator() unexpected error: %v", err)
	}
	return iso
}

func TestNewTopicIsolator(t *testing.T) {
	t.Parallel()
	t.Run("with default config", func(t *testing.T) {
		t.Parallel()
		iso := mustNewTopicIsolator(t, DefaultTopicIsolationConfig())
		if iso == nil {
			t.Fatal("expected non-nil isolator")
		}
		if iso.config.Environment != "local" {
			t.Errorf("expected environment 'local', got %q", iso.config.Environment)
		}
		if iso.config.Separator != "." {
			t.Errorf("expected separator '.', got %q", iso.config.Separator)
		}
	})

	t.Run("with empty separator defaults to dot", func(t *testing.T) {
		t.Parallel()
		iso := mustNewTopicIsolator(t, TopicIsolationConfig{Environment: "local", Separator: ""})
		if iso.config.Separator != "." {
			t.Errorf("expected separator '.', got %q", iso.config.Separator)
		}
	})

	t.Run("with empty environment returns error", func(t *testing.T) {
		t.Parallel()
		_, err := NewTopicIsolator(TopicIsolationConfig{Environment: ""})
		if err == nil {
			t.Fatal("expected error for empty environment")
		}
	})
}

func TestTopicIsolator_CheckTopicAccess(t *testing.T) {
	t.Parallel()
	iso := mustNewTopicIsolator(t, TopicIsolationConfig{
		Environment:         "prod",
		TenantPosition:      1,
		Separator:           ".",
		SharedTopicPatterns: []string{"prod.shared.*"},
	})

	tests := []struct {
		name          string
		claims        *Claims
		topic         string
		action        TopicAction
		expectAllowed bool
		expectReason  string
	}{
		{
			name:          "same tenant allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.acme.trade",
			action:        TopicActionConsume,
			expectAllowed: true,
			expectReason:  "tenant match",
		},
		{
			name:          "different tenant denied",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.globex.trade",
			action:        TopicActionConsume,
			expectAllowed: false,
		},
		{
			name:          "nil claims allowed (auth disabled)",
			claims:        nil,
			topic:         "prod.acme.trade",
			action:        TopicActionConsume,
			expectAllowed: true,
		},
		{
			name:          "empty tenant allowed (auth disabled)",
			claims:        &Claims{TenantID: ""},
			topic:         "prod.acme.trade",
			action:        TopicActionConsume,
			expectAllowed: true,
		},
		{
			name:          "admin role cross-tenant denied (cross-tenant roles removed)",
			claims:        &Claims{TenantID: "acme", Roles: []string{"admin"}},
			topic:         "prod.globex.trade",
			action:        TopicActionConsume,
			expectAllowed: false,
		},
		{
			name:          "system role cross-tenant denied (cross-tenant roles removed)",
			claims:        &Claims{TenantID: "acme", Roles: []string{"system"}},
			topic:         "prod.globex.trade",
			action:        TopicActionPublish,
			expectAllowed: false,
		},
		{
			name:          "shared topic allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.shared.broadcast",
			action:        TopicActionConsume,
			expectAllowed: true,
		},
		{
			name:          "rejects topic without tenant segment",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.trade", // Missing tenant - not in shared patterns
			action:        TopicActionConsume,
			expectAllowed: false,
		},
		{
			name:          "publish same tenant allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.acme.trade",
			action:        TopicActionPublish,
			expectAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := iso.CheckTopicAccess(tt.claims, tt.topic, tt.action)

			if result.Allowed != tt.expectAllowed {
				t.Errorf("CheckTopicAccess() Allowed = %v, want %v (reason: %s)",
					result.Allowed, tt.expectAllowed, result.Reason)
			}

			if tt.expectReason != "" && result.Reason != tt.expectReason {
				t.Errorf("CheckTopicAccess() Reason = %q, want %q",
					result.Reason, tt.expectReason)
			}
		})
	}
}

func TestTopicIsolator_CheckTopicAccess_Flags(t *testing.T) {
	t.Parallel()
	iso := mustNewTopicIsolator(t, TopicIsolationConfig{
		Environment:         "prod",
		TenantPosition:      1,
		Separator:           ".",
		SharedTopicPatterns: []string{"prod.shared.*"},
	})

	t.Run("shared topic flag set", func(t *testing.T) {
		t.Parallel()
		claims := &Claims{TenantID: "acme"}
		result := iso.CheckTopicAccess(claims, "prod.shared.broadcast", TopicActionConsume)

		if !result.IsSharedTopic {
			t.Error("expected IsSharedTopic to be true for shared topic")
		}
	})
}

func TestTopicIsolator_ExtractTenantFromTopic(t *testing.T) {
	t.Parallel()
	iso := mustNewTopicIsolator(t, DefaultTopicIsolationConfig())

	tests := []struct {
		topic    string
		expected string
	}{
		{"prod.acme.trade", "acme"},
		{"prod.globex.liquidity", "globex"},
		{"dev.startup.trade", "startup"},
		{"prod.trade", "trade"}, // Position 1 exists, returns "trade"
		{"trade", ""},           // Not enough parts (position 1 doesn't exist)
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			t.Parallel()
			result := iso.ExtractTenantFromTopic(tt.topic)
			if result != tt.expected {
				t.Errorf("ExtractTenantFromTopic(%q) = %q, want %q",
					tt.topic, result, tt.expected)
			}
		})
	}
}

func TestTopicIsolator_CanPublish_CanConsume(t *testing.T) {
	t.Parallel()
	iso := mustNewTopicIsolator(t, DefaultTopicIsolationConfig())

	claims := &Claims{TenantID: "acme"}

	// Same tenant
	if !iso.CanPublish(claims, "prod.acme.trade") {
		t.Error("expected CanPublish to return true for same tenant")
	}
	if !iso.CanConsume(claims, "prod.acme.trade") {
		t.Error("expected CanConsume to return true for same tenant")
	}

	// Different tenant
	if iso.CanPublish(claims, "prod.globex.trade") {
		t.Error("expected CanPublish to return false for different tenant")
	}
	if iso.CanConsume(claims, "prod.globex.trade") {
		t.Error("expected CanConsume to return false for different tenant")
	}
}

func TestMatchTopicPattern(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern  string
		topic    string
		expected bool
	}{
		// Exact match
		{"prod.acme.trade", "prod.acme.trade", true},
		{"prod.acme.trade", "prod.acme.liquidity", false},

		// Wildcard in tenant position
		{"prod.*.trade", "prod.acme.trade", true},
		{"prod.*.trade", "prod.globex.trade", true},
		{"prod.*.trade", "prod.acme.liquidity", false},

		// Wildcard in suffix position
		{"prod.shared.*", "prod.shared.broadcast", true},
		{"prod.shared.*", "prod.shared.alerts", true},
		{"prod.shared.*", "prod.acme.trade", false},

		// Multiple wildcards
		{"*.*.*", "prod.acme.trade", true},
		{"*.*.*", "dev.globex.liquidity", true},

		// Length mismatch
		{"prod.*.trade", "prod.acme", false},
		{"prod.*", "prod.acme.trade", false},
	}

	for _, tt := range tests {
		name := tt.pattern + "_vs_" + tt.topic
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result := matchTopicPattern(tt.pattern, tt.topic, ".")
			if result != tt.expected {
				t.Errorf("matchTopicPattern(%q, %q) = %v, want %v",
					tt.pattern, tt.topic, result, tt.expected)
			}
		})
	}
}

func TestDefaultTopicIsolationConfig(t *testing.T) {
	t.Parallel()
	config := DefaultTopicIsolationConfig()

	if config.Environment != "local" {
		t.Errorf("expected environment 'local', got %q", config.Environment)
	}
	if config.TenantPosition != 1 {
		t.Errorf("expected tenant position 1, got %d", config.TenantPosition)
	}
	if config.Separator != "." {
		t.Errorf("expected separator '.', got %q", config.Separator)
	}
}

func TestTopicIsolator_FailSecure(t *testing.T) {
	t.Parallel()
	iso := mustNewTopicIsolator(t, TopicIsolationConfig{
		Environment:         "prod",
		TenantPosition:      1,
		Separator:           ".",
		SharedTopicPatterns: []string{"prod.shared.*"},
	})

	claims := &Claims{TenantID: "acme"}

	t.Run("topic with matching tenant allowed", func(t *testing.T) {
		t.Parallel()
		result := iso.CheckTopicAccess(claims, "prod.acme.trade", TopicActionConsume)
		if !result.Allowed {
			t.Errorf("expected topic with matching tenant to be allowed, got: %s", result.Reason)
		}
	})

	t.Run("topic without tenant segment denied", func(t *testing.T) {
		t.Parallel()
		// Single part topic has no tenant - must be rejected (fail-secure)
		result := iso.CheckTopicAccess(claims, "broadcast", TopicActionConsume)
		if result.Allowed {
			t.Error("expected topic without tenant segment to be denied (fail-secure)")
		}
	})

	t.Run("shared topic allowed", func(t *testing.T) {
		t.Parallel()
		// Explicitly configured shared topic
		result := iso.CheckTopicAccess(claims, "prod.shared.broadcast", TopicActionConsume)
		if !result.Allowed {
			t.Errorf("expected shared topic to be allowed, got: %s", result.Reason)
		}
	})

	t.Run("topic with different tenant denied", func(t *testing.T) {
		t.Parallel()
		result := iso.CheckTopicAccess(claims, "prod.globex.trade", TopicActionConsume)
		if result.Allowed {
			t.Error("expected topic with different tenant to be denied")
		}
	})
}
