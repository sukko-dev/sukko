package platform_test

import (
	"context"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

func TestAnalyticsConfig_Validate(t *testing.T) {
	t.Parallel()

	// Default is Enabled:false — set to true explicitly for tests that check enabled-path validation.
	valid := platform.AnalyticsConfig{
		Enabled:               true,
		DBURL:                 "postgresql://localhost:5432/analytics",
		DBMaxConns:            5,
		FlushInterval:         60 * time.Second,
		BufferSize:            10000,
		RawRetention:          time.Hour,
		DowngradePollInterval: 5 * time.Minute,
	}

	tests := []struct {
		name    string
		cfg     platform.AnalyticsConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid defaults",
			cfg:     valid,
			wantErr: false,
		},
		{
			name:    "disabled with empty URL — no error",
			cfg:     platform.AnalyticsConfig{Enabled: false},
			wantErr: false,
		},
		{
			name:    "disabled with DBMaxConns=0 — no error (bounds skip when disabled)",
			cfg:     platform.AnalyticsConfig{Enabled: false, DBMaxConns: 0},
			wantErr: false,
		},
		{
			name:    "enabled with empty URL — error",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.DBURL = ""; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_DB_URL",
		},
		{
			name:    "DBMaxConns below min",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.DBMaxConns = 0; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_DB_MAX_CONNS",
		},
		{
			name:    "DBMaxConns above max",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.DBMaxConns = 51; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_DB_MAX_CONNS",
		},
		{
			name:    "FlushInterval below min",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.FlushInterval = time.Second; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_FLUSH_INTERVAL",
		},
		{
			name:    "FlushInterval above max",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.FlushInterval = 2 * time.Hour; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_FLUSH_INTERVAL",
		},
		{
			name:    "BufferSize below min",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.BufferSize = 0; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_BUFFER_SIZE",
		},
		{
			name:    "BufferSize above max",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.BufferSize = 2_000_000; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_BUFFER_SIZE",
		},
		{
			name:    "RawRetention below min",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.RawRetention = time.Minute; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_RAW_RETENTION",
		},
		{
			name:    "DowngradePollInterval above max",
			cfg:     func() platform.AnalyticsConfig { c := valid; c.DowngradePollInterval = 2 * time.Hour; return c }(),
			wantErr: true,
			errMsg:  "ANALYTICS_DOWNGRADE_POLL_INTERVAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() expected error containing %q, got nil", tt.errMsg)
					return
				}
				if tt.errMsg != "" && !containsStr(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %q, want it to contain %q", err.Error(), tt.errMsg)
				}
			} else if err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

func TestOpenAnalyticsPool_Disabled(t *testing.T) {
	t.Parallel()
	cfg := platform.AnalyticsConfig{Enabled: false}
	pool, err := platform.OpenAnalyticsPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("OpenAnalyticsPool(disabled) unexpected error: %v", err)
	}
	if pool != nil {
		pool.Close()
		t.Error("OpenAnalyticsPool(disabled) expected nil pool, got non-nil")
	}
}

func TestOpenAnalyticsPool_MalformedURL(t *testing.T) {
	t.Parallel()
	cfg := platform.AnalyticsConfig{
		Enabled:    true,
		DBURL:      "not-a-valid-url://!!!",
		DBMaxConns: 5,
	}
	pool, err := platform.OpenAnalyticsPool(context.Background(), cfg)
	if pool != nil {
		pool.Close()
	}
	if err == nil {
		t.Error("OpenAnalyticsPool(malformed URL) expected error, got nil")
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || substr == "" ||
		func() bool {
			for i := 0; i+len(substr) <= len(s); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
