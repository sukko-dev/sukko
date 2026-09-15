package worker

import (
	"context"
	"fmt"
	"net"
	"time"
)

// privateRanges holds CIDR ranges the SSRF dialer rejects.
// Covers RFC 1918 (private), RFC 5735 (loopback), RFC 3927 (link-local),
// IPv6 ULA (fc00::/7), IPv6 link-local (fe80::/10), and IPv6 loopback.
var privateRanges []*net.IPNet

func init() {
	cidrs := []string{
		"10.0.0.0/8",     // RFC 1918 Class A
		"172.16.0.0/12",  // RFC 1918 Class B
		"192.168.0.0/16", // RFC 1918 Class C
		"127.0.0.0/8",    // IPv4 loopback
		"169.254.0.0/16", // IPv4 link-local
		"::1/128",        // IPv6 loopback
		"fc00::/7",       // IPv6 ULA
		"fe80::/10",      // IPv6 link-local
	}
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Sprintf("ssrf: bad cidr %s: %v", cidr, err))
		}
		privateRanges = append(privateRanges, network)
	}
}

// isPrivate returns true if ip falls in any blocked range.
func isPrivate(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, r := range privateRanges {
		if r.Contains(ip) {
			return true
		}
	}
	return false
}

// ssrfBlockedPrefix is the error string prefix returned when a connection is SSRF-blocked.
// Named constant so delivery.go and tests reference the same string (§I: magic strings forbidden).
const ssrfBlockedPrefix = "ssrf_blocked"

// SSRFDialer wraps a Resolver to perform dial-time IP validation before connecting.
// Rejects connections to private/loopback/link-local IPs.
// Resolver is injectable via the Resolver interface for DNS-rebinding tests.
// When allowPrivate is true, private-IP checks are skipped — use only in CI/staging.
type SSRFDialer struct {
	resolver     Resolver
	dialer       net.Dialer // pre-allocated; Timeout is set once at construction
	allowPrivate bool
}

// NewSSRFDialer creates an SSRFDialer.
// resolver should be &net.Resolver{} in production; an injectable mock for tests.
// allowPrivate disables SSRF protection for private IPs; set to cfg.WebhookAllowPrivateIPs.
func NewSSRFDialer(resolver Resolver, timeout time.Duration, allowPrivate bool) *SSRFDialer {
	return &SSRFDialer{
		resolver:     resolver,
		dialer:       net.Dialer{Timeout: timeout},
		allowPrivate: allowPrivate,
	}
}

// DialContext resolves host, rejects private IPs, and dials using the first public IP.
// Returns an error with prefix "ssrf_blocked:" when the resolved IP is private.
// Private-IP checks are skipped when allowPrivate was set at construction.
func (d *SSRFDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("ssrf dialer: split host/port %q: %w", addr, err)
	}

	// If addr is already an IP literal, skip DNS resolution.
	if ip := net.ParseIP(host); ip != nil {
		if !d.allowPrivate && isPrivate(ip) {
			return nil, fmt.Errorf("%s: %s is a private IP address", ssrfBlockedPrefix, host)
		}
		conn, dialErr := d.dialer.DialContext(ctx, network, addr)
		if dialErr != nil {
			return nil, fmt.Errorf("ssrf dialer: dial %s: %w", addr, dialErr)
		}
		return conn, nil
	}

	// DNS resolution — subject to rebinding attacks; check each resolved IP.
	ips, err := d.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("ssrf dialer: dns lookup %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("ssrf dialer: no IPs for %s", host)
	}

	// Check ALL resolved IPs — reject if any is private (DNS rebinding defense).
	if !d.allowPrivate {
		for _, ipStr := range ips {
			ip := net.ParseIP(ipStr)
			if ip == nil {
				continue
			}
			if isPrivate(ip) {
				return nil, fmt.Errorf("%s: %s resolves to private IP %s", ssrfBlockedPrefix, host, ipStr)
			}
		}
	}

	conn, dialErr := d.dialer.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
	if dialErr != nil {
		return nil, fmt.Errorf("ssrf dialer: dial %s: %w", host, dialErr)
	}
	return conn, nil
}
