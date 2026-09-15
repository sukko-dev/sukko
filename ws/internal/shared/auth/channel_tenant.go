package auth

import "strings"

// ValidateChannelTenant checks that a channel belongs to the given tenant.
// Returns true if channel starts with "{tenantID}." — the dot separator ensures
// "acme" does not match "acmex.alerts", only "acme.alerts".
//
// Examples:
//
//	ValidateChannelTenant("acme.alerts.BTC", "acme") → true
//	ValidateChannelTenant("other.alerts.BTC", "acme") → false
//	ValidateChannelTenant("acmex.alerts", "acme") → false (no dot boundary)
//	ValidateChannelTenant("acme", "acme") → false (no dot separator)
func ValidateChannelTenant(channel, tenantID string) bool {
	if channel == "" || tenantID == "" {
		return false
	}
	return strings.HasPrefix(channel, tenantID+".")
}
