package platform

import "testing"

func TestUseValkeySentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		addrs      []string
		masterName string
		want       bool
	}{
		// Regression for #140: a single instance must never use Sentinel even with the
		// default master name "mymaster" — otherwise the client speaks the Sentinel
		// protocol and FATALs against a standalone Valkey.
		{"single addr, default master name", []string{"valkey:6379"}, "mymaster", false},
		{"single addr, empty master name", []string{"valkey:6379"}, "", false},
		{"multi addr, master name set (sentinel)", []string{"s1:26379", "s2:26379", "s3:26379"}, "mymaster", true},
		{"multi addr, empty master name", []string{"s1:26379", "s2:26379"}, "", false},
		{"no addrs", nil, "mymaster", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := UseValkeySentinel(tt.addrs, tt.masterName); got != tt.want {
				t.Errorf("UseValkeySentinel(%v, %q) = %v, want %v", tt.addrs, tt.masterName, got, tt.want)
			}
		})
	}
}
