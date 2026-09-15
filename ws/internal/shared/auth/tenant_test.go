package auth

import (
	"context"
	"testing"
)

func mustNewTenantIsolator(t *testing.T, config TenantIsolationConfig, opts ...TenantIsolatorOption) *TenantIsolator {
	t.Helper()
	iso, err := NewTenantIsolator(config, opts...)
	if err != nil {
		t.Fatalf("NewTenantIsolator() unexpected error: %v", err)
	}
	return iso
}

func TestNewTenantIsolator(t *testing.T) {
	t.Parallel()
	t.Run("with default config", func(t *testing.T) {
		t.Parallel()
		iso := mustNewTenantIsolator(t, DefaultTenantIsolationConfig())
		if iso == nil {
			t.Fatal("expected non-nil isolator")
		}
		if iso.topicIsolator == nil {
			t.Error("expected topic isolator to be initialized")
		}
	})

	t.Run("with custom audit logger", func(t *testing.T) {
		t.Parallel()
		logger := &testAuditLogger{}
		iso := mustNewTenantIsolator(t,
			DefaultTenantIsolationConfig(),
			WithAuditLogger(logger),
		)
		if iso.auditLogger != logger {
			t.Error("expected custom audit logger to be used")
		}
	})
}

func TestNewTenantIsolator_Errors(t *testing.T) {
	t.Parallel()

	t.Run("empty environment without custom TopicIsolator returns error", func(t *testing.T) {
		t.Parallel()
		_, err := NewTenantIsolator(TenantIsolationConfig{Environment: ""})
		if err == nil {
			t.Fatal("expected error for empty environment")
		}
	})

	t.Run("with custom TopicIsolator skips environment requirement", func(t *testing.T) {
		t.Parallel()
		customIso := mustNewTopicIsolator(t, TopicIsolationConfig{Environment: "prod"})
		iso, err := NewTenantIsolator(
			TenantIsolationConfig{},
			WithTopicIsolator(customIso),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if iso.topicIsolator != customIso {
			t.Error("expected custom topic isolator to be used")
		}
	})

	t.Run("WithTopicIsolator(nil) still requires environment", func(t *testing.T) {
		t.Parallel()
		_, err := NewTenantIsolator(
			TenantIsolationConfig{},
			WithTopicIsolator(nil),
		)
		if err == nil {
			t.Fatal("expected error when nil TopicIsolator and no environment")
		}
	})
}

func TestTenantIsolator_CheckChannelAccess(t *testing.T) {
	t.Parallel()
	iso := mustNewTenantIsolator(t, TenantIsolationConfig{
		Environment:           "local",
		SharedChannelPatterns: []string{"system.*", "broadcast.*"},
		AuditDenials:          true,
	})

	ctx := context.Background()

	tests := []struct {
		name          string
		claims        *Claims
		channel       string
		action        AccessAction
		expectAllowed bool
	}{
		{
			name:          "same tenant allowed",
			claims:        &Claims{TenantID: "acme"},
			channel:       "acme.BTC.trade",
			action:        ActionSubscribe,
			expectAllowed: true,
		},
		{
			name:          "different tenant denied",
			claims:        &Claims{TenantID: "acme"},
			channel:       "globex.BTC.trade",
			action:        ActionSubscribe,
			expectAllowed: false,
		},
		{
			name:          "nil claims allowed",
			claims:        nil,
			channel:       "acme.BTC.trade",
			action:        ActionSubscribe,
			expectAllowed: true,
		},
		{
			name:          "empty tenant allowed",
			claims:        &Claims{TenantID: ""},
			channel:       "acme.BTC.trade",
			action:        ActionSubscribe,
			expectAllowed: true,
		},
		{
			name:          "shared channel allowed",
			claims:        &Claims{TenantID: "acme"},
			channel:       "system.notifications",
			action:        ActionSubscribe,
			expectAllowed: true,
		},
		{
			name:          "broadcast shared channel allowed",
			claims:        &Claims{TenantID: "acme"},
			channel:       "broadcast.all",
			action:        ActionSubscribe,
			expectAllowed: true,
		},
		{
			name:          "admin role cross-tenant denied (cross-tenant roles removed)",
			claims:        &Claims{TenantID: "acme", Roles: []string{"admin"}},
			channel:       "globex.BTC.trade",
			action:        ActionSubscribe,
			expectAllowed: false,
		},
		{
			name:          "publish same tenant allowed",
			claims:        &Claims{TenantID: "acme"},
			channel:       "acme.events",
			action:        ActionPublish,
			expectAllowed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := iso.CheckChannelAccess(ctx, tt.claims, tt.channel, tt.action)

			if result.Allowed != tt.expectAllowed {
				t.Errorf("CheckChannelAccess() Allowed = %v, want %v (reason: %s)",
					result.Allowed, tt.expectAllowed, result.Reason)
			}

			if result.ResourceType != "channel" {
				t.Errorf("expected ResourceType 'channel', got %q", result.ResourceType)
			}

			if result.Resource != tt.channel {
				t.Errorf("expected Resource %q, got %q", tt.channel, result.Resource)
			}
		})
	}
}

func TestTenantIsolator_CheckTopicAccess(t *testing.T) {
	t.Parallel()
	iso := mustNewTenantIsolator(t, TenantIsolationConfig{
		Environment:         "local",
		SharedTopicPatterns: []string{"prod.shared.*"},
	})

	ctx := context.Background()

	tests := []struct {
		name          string
		claims        *Claims
		topic         string
		action        AccessAction
		expectAllowed bool
	}{
		{
			name:          "same tenant consume allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.acme.trade",
			action:        ActionConsume,
			expectAllowed: true,
		},
		{
			name:          "same tenant publish allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.acme.trade",
			action:        ActionPublish,
			expectAllowed: true,
		},
		{
			name:          "different tenant denied",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.globex.trade",
			action:        ActionConsume,
			expectAllowed: false,
		},
		{
			name:          "shared topic allowed",
			claims:        &Claims{TenantID: "acme"},
			topic:         "prod.shared.broadcast",
			action:        ActionConsume,
			expectAllowed: true,
		},
		{
			name:          "admin cross-tenant denied (cross-tenant roles removed)",
			claims:        &Claims{TenantID: "acme", Roles: []string{"admin"}},
			topic:         "prod.globex.trade",
			action:        ActionConsume,
			expectAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := iso.CheckTopicAccess(ctx, tt.claims, tt.topic, tt.action)

			if result.Allowed != tt.expectAllowed {
				t.Errorf("CheckTopicAccess() Allowed = %v, want %v (reason: %s)",
					result.Allowed, tt.expectAllowed, result.Reason)
			}

			if result.ResourceType != "topic" {
				t.Errorf("expected ResourceType 'topic', got %q", result.ResourceType)
			}
		})
	}
}

func TestTenantIsolator_CanAccessResource(t *testing.T) {
	t.Parallel()
	iso := mustNewTenantIsolator(t, DefaultTenantIsolationConfig())

	claims := &Claims{TenantID: "acme"}

	// Channel access
	if !iso.CanAccessResource(claims, "acme.BTC.trade", "channel", ActionSubscribe) {
		t.Error("expected channel access to be allowed for same tenant")
	}

	if iso.CanAccessResource(claims, "globex.BTC.trade", "channel", ActionSubscribe) {
		t.Error("expected channel access to be denied for different tenant")
	}

	// Topic access
	if !iso.CanAccessResource(claims, "prod.acme.trade", "topic", ActionConsume) {
		t.Error("expected topic access to be allowed for same tenant")
	}

	// Unknown resource type
	if iso.CanAccessResource(claims, "something", "unknown", ActionSubscribe) {
		t.Error("expected unknown resource type to be denied")
	}
}

func TestTenantIsolator_AuditLogging(t *testing.T) {
	t.Parallel()
	logger := &testAuditLogger{}

	iso := mustNewTenantIsolator(t,
		TenantIsolationConfig{
			Environment:    "local",
			AuditDenials:   true,
			AuditAllAccess: true,
		},
		WithAuditLogger(logger),
	)

	ctx := context.Background()
	claims := &Claims{TenantID: "acme"}

	// Access same tenant (allowed) - should be logged with AuditAllAccess
	iso.CheckChannelAccess(ctx, claims, "acme.trade", ActionSubscribe)
	if logger.allowedCount != 1 {
		t.Errorf("expected 1 allowed log, got %d", logger.allowedCount)
	}

	// Access different tenant (denied) - should be logged
	iso.CheckChannelAccess(ctx, claims, "globex.trade", ActionSubscribe)
	if logger.deniedCount != 1 {
		t.Errorf("expected 1 denied log, got %d", logger.deniedCount)
	}
}

func TestTenantIsolator_ResultFlags(t *testing.T) {
	t.Parallel()
	iso := mustNewTenantIsolator(t, TenantIsolationConfig{
		Environment:           "local",
		SharedChannelPatterns: []string{"system.*"},
	})

	ctx := context.Background()

	t.Run("shared flag set", func(t *testing.T) {
		t.Parallel()
		claims := &Claims{TenantID: "acme"}
		result := iso.CheckChannelAccess(ctx, claims, "system.broadcast", ActionSubscribe)

		if !result.IsShared {
			t.Error("expected IsShared to be true")
		}
	})
}

func TestExtractTenantContext(t *testing.T) {
	t.Parallel()
	t.Run("with claims", func(t *testing.T) {
		t.Parallel()
		claims := &Claims{
			TenantID: "acme",
			Roles:    []string{"admin", "user"},
			Groups:   []string{"vip"},
		}
		claims.Subject = "user123"

		ctx := ExtractTenantContext(claims)

		if ctx.TenantID != "acme" {
			t.Errorf("expected TenantID 'acme', got %q", ctx.TenantID)
		}
		if ctx.UserID != "user123" {
			t.Errorf("expected UserID 'user123', got %q", ctx.UserID)
		}
		if len(ctx.Roles) != 2 {
			t.Errorf("expected 2 roles, got %d", len(ctx.Roles))
		}
		if len(ctx.Groups) != 1 {
			t.Errorf("expected 1 group, got %d", len(ctx.Groups))
		}
	})

	t.Run("with nil claims", func(t *testing.T) {
		t.Parallel()
		ctx := ExtractTenantContext(nil)

		if ctx.TenantID != "" {
			t.Errorf("expected empty TenantID, got %q", ctx.TenantID)
		}
	})
}

func TestDefaultTenantIsolationConfig(t *testing.T) {
	t.Parallel()
	config := DefaultTenantIsolationConfig()

	if !config.AuditDenials {
		t.Error("expected AuditDenials to be true")
	}

	if config.AuditAllAccess {
		t.Error("expected AuditAllAccess to be false by default")
	}
}

// testAuditLogger is a test implementation of AuditLogger.
type testAuditLogger struct {
	deniedCount  int
	allowedCount int
	entries      []*AuditEntry
}

func (l *testAuditLogger) LogDenied(_ context.Context, entry *AuditEntry) {
	l.deniedCount++
	l.entries = append(l.entries, entry)
}

func (l *testAuditLogger) LogAllowed(_ context.Context, entry *AuditEntry) {
	l.allowedCount++
	l.entries = append(l.entries, entry)
}
