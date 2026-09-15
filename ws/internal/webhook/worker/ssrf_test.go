package worker

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// mockResolver is an injectable Resolver for SSRF tests.
type mockResolver struct {
	ips []string
	err error
}

func (m *mockResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	return m.ips, m.err
}

func TestSSRFDialer_PrivateIPBlocked(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		ips     []string
		wantErr bool
		errPfx  string
	}{
		{"private RFC1918 10.x", []string{"10.0.0.1"}, true, "ssrf_blocked"},
		{"private RFC1918 172.16.x", []string{"172.16.5.1"}, true, "ssrf_blocked"},
		{"private RFC1918 192.168.x", []string{"192.168.1.1"}, true, "ssrf_blocked"},
		{"loopback 127.0.0.1", []string{"127.0.0.1"}, true, "ssrf_blocked"},
		{"link-local 169.254.x (DNS rebinding)", []string{"169.254.169.254"}, true, "ssrf_blocked"},
		{"IPv6 loopback ::1", []string{"::1"}, true, "ssrf_blocked"},
		{"public IP allowed", []string{"93.184.216.34"}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dialer := NewSSRFDialer(&mockResolver{ips: tt.ips}, 5*time.Second, false)
			_, err := dialer.DialContext(context.Background(), "tcp", "example.com:443")
			if tt.wantErr {
				if err == nil {
					t.Error("expected ssrf_blocked error, got nil")
					return
				}
				if tt.errPfx != "" {
					if !strings.Contains(err.Error(), tt.errPfx) {
						t.Errorf("error %q should contain %q", err.Error(), tt.errPfx)
					}
				}
			} else if err != nil {
				// For public IPs, the dial will fail (no real server), but the error
				// must NOT be ssrf_blocked — it should be a connection error.
				if strings.HasPrefix(err.Error(), ssrfBlockedPrefix) {
					t.Errorf("public IP should not be blocked: %v", err)
				}
				// connection refused / no route is expected — that's fine
			}
		})
	}
}

// TestSSRFDialer_IPLiteral checks that IP literals in the URL are also validated
// without going through DNS resolution.
func TestSSRFDialer_IPLiteral(t *testing.T) {
	t.Parallel()
	// Private IP literal in addr — resolver is never called.
	dialer := NewSSRFDialer(&mockResolver{err: errShouldNotBeCalled}, 5*time.Second, false)
	_, err := dialer.DialContext(context.Background(), "tcp", "10.0.0.1:80")
	if err == nil || !strings.Contains(err.Error(), ssrfBlockedPrefix) {
		t.Errorf("private IP literal should be blocked with %q prefix, got: %v", ssrfBlockedPrefix, err)
	}
}

var errShouldNotBeCalled = &net.DNSError{Err: "resolver should not be called for IP literals"}

// TestSSRFDialer_AllowPrivate verifies that allowPrivate=true bypasses all private-IP checks.
func TestSSRFDialer_AllowPrivate(t *testing.T) {
	t.Parallel()

	t.Run("private IP via DNS allowed when allowPrivate=true", func(t *testing.T) {
		t.Parallel()
		dialer := NewSSRFDialer(&mockResolver{ips: []string{"10.0.0.1"}}, 5*time.Second, true)
		_, err := dialer.DialContext(context.Background(), "tcp", "tester.sukko.internal:8080")
		// The connection will fail (no server at 10.0.0.1:8080), but the error must NOT
		// be an ssrf_blocked error — it should be a network/connection error.
		if err != nil && strings.HasPrefix(err.Error(), ssrfBlockedPrefix) {
			t.Errorf("private IP should not be blocked when allowPrivate=true: %v", err)
		}
	})

	t.Run("private IP literal allowed when allowPrivate=true", func(t *testing.T) {
		t.Parallel()
		dialer := NewSSRFDialer(&mockResolver{err: errShouldNotBeCalled}, 5*time.Second, true)
		_, err := dialer.DialContext(context.Background(), "tcp", "192.168.1.50:8080")
		if err != nil && strings.HasPrefix(err.Error(), ssrfBlockedPrefix) {
			t.Errorf("private IP literal should not be blocked when allowPrivate=true: %v", err)
		}
	})
}
