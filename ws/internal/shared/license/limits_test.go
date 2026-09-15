package license

import (
	"errors"
	"strings"
	"testing"
)

func TestDefaultLimits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		edition    Edition
		tenants    int
		conns      int
		shards     int
		topics     int
		rules      int
		wantEdName Edition
	}{
		{Community, 3, 500, 1, 10, 10, Community},
		{Pro, 50, 10000, 8, 50, 100, Pro},
		{Enterprise, 0, 0, 0, 0, 0, Enterprise},
		{"", 3, 500, 1, 10, 10, Community},                 // empty → Community
		{Edition("unknown"), 3, 500, 1, 10, 10, Community}, // unknown → Community
	}
	for _, tt := range tests {
		t.Run(tt.edition.String(), func(t *testing.T) {
			t.Parallel()
			l := DefaultLimits(tt.edition)
			if l.Edition != tt.wantEdName {
				t.Errorf("Edition = %q, want %q", l.Edition, tt.wantEdName)
			}
			if l.MaxTenants != tt.tenants {
				t.Errorf("MaxTenants = %d, want %d", l.MaxTenants, tt.tenants)
			}
			if l.MaxTotalConnections != tt.conns {
				t.Errorf("MaxTotalConnections = %d, want %d", l.MaxTotalConnections, tt.conns)
			}
			if l.MaxShards != tt.shards {
				t.Errorf("MaxShards = %d, want %d", l.MaxShards, tt.shards)
			}
			if l.MaxTopicsPerTenant != tt.topics {
				t.Errorf("MaxTopicsPerTenant = %d, want %d", l.MaxTopicsPerTenant, tt.topics)
			}
			if l.MaxRoutingRulesPerTenant != tt.rules {
				t.Errorf("MaxRoutingRulesPerTenant = %d, want %d", l.MaxRoutingRulesPerTenant, tt.rules)
			}
		})
	}
}

func TestLimits_CheckTenants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		limits  Limits
		current int
		wantErr bool
	}{
		{"under limit", DefaultLimits(Community), 2, false},
		{"at limit", DefaultLimits(Community), 3, true},
		{"over limit", DefaultLimits(Community), 5, true},
		{"zero current", DefaultLimits(Community), 0, false},
		{"unlimited (enterprise)", DefaultLimits(Enterprise), 1000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.limits.CheckTenants(tt.current)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckTenants(%d) error = %v, wantErr %v", tt.current, err, tt.wantErr)
			}
			if err != nil {
				limitErr, ok := errors.AsType[*EditionLimitError](err)
				if !ok {
					t.Fatalf("expected EditionLimitError, got %T", err)
				}
				if limitErr.Dimension != "tenants" {
					t.Errorf("Dimension = %q, want %q", limitErr.Dimension, "tenants")
				}
				if limitErr.Current != tt.current {
					t.Errorf("Current = %d, want %d", limitErr.Current, tt.current)
				}
			}
		})
	}
}

func TestLimits_CheckTotalConnections(t *testing.T) {
	t.Parallel()
	community := DefaultLimits(Community)
	// CheckTotalConnections uses checkValue (>) — configured capacity
	if err := community.CheckTotalConnections(500); err != nil {
		t.Errorf("500 connections should be allowed for community (max 500): %v", err)
	}
	if err := community.CheckTotalConnections(501); err == nil {
		t.Error("501 connections should fail for community (max 500)")
	}

	enterprise := DefaultLimits(Enterprise)
	if err := enterprise.CheckTotalConnections(999999); err != nil {
		t.Errorf("enterprise should be unlimited: %v", err)
	}
}

func TestLimits_CheckShards(t *testing.T) {
	t.Parallel()
	community := DefaultLimits(Community)
	// CheckShards uses checkValue (>) not checkCount (>=)
	// NumShards=1 with MaxShards=1 is allowed (1 is not > 1)
	if err := community.CheckShards(1); err != nil {
		t.Errorf("1 shard should be allowed for community (max 1): %v", err)
	}
	if err := community.CheckShards(2); err == nil {
		t.Error("2 shards should fail for community (max 1)")
	}

	pro := DefaultLimits(Pro)
	if err := pro.CheckShards(8); err != nil {
		t.Errorf("8 shards should be allowed for pro (max 8): %v", err)
	}
	if err := pro.CheckShards(9); err == nil {
		t.Error("9 shards should fail for pro (max 8)")
	}
}

func TestLimits_CheckTopicsPerTenant(t *testing.T) {
	t.Parallel()
	community := DefaultLimits(Community)
	if err := community.CheckTopicsPerTenant(9); err != nil {
		t.Errorf("9 < 10 should pass: %v", err)
	}
	if err := community.CheckTopicsPerTenant(10); err == nil {
		t.Error("10 >= 10 should fail")
	}
}

func TestLimits_CheckRoutingRulesPerTenant(t *testing.T) {
	t.Parallel()
	community := DefaultLimits(Community)
	// CheckRoutingRulesPerTenant uses checkValue (>) — len(rules) is the requested value
	if err := community.CheckRoutingRulesPerTenant(10); err != nil {
		t.Errorf("10 rules should be allowed for community (max 10): %v", err)
	}
	if err := community.CheckRoutingRulesPerTenant(11); err == nil {
		t.Error("11 rules should fail for community (max 10)")
	}
}

func TestIsUnlimited(t *testing.T) {
	t.Parallel()
	if !IsUnlimited(0) {
		t.Error("0 should be unlimited")
	}
	if IsUnlimited(1) {
		t.Error("1 should not be unlimited")
	}
	if IsUnlimited(-1) {
		t.Error("-1 should not be unlimited")
	}
}

func TestLimits_CheckError_Format(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits(Community)
	err := limits.CheckTenants(3)
	if err == nil {
		t.Fatal("expected error")
	}

	limitErr, ok := errors.AsType[*EditionLimitError](err)
	if !ok {
		t.Fatalf("expected EditionLimitError, got %T", err)
	}

	// Verify error message contains key info
	msg := limitErr.Error()
	for _, substr := range []string{"tenants", "3/3", "community", UpgradeURL} {
		if !strings.Contains(msg, substr) {
			t.Errorf("error message %q missing %q", msg, substr)
		}
	}

	// Verify code format
	if code := limitErr.Code(); code != "EDITION_LIMIT_TENANTS" {
		t.Errorf("Code() = %q, want EDITION_LIMIT_TENANTS", code)
	}
}
