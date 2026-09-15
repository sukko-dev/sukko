package license

import (
	"context"
	"net/http"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
)

// EditionResponse is the JSON response for the GET /edition endpoint.
type EditionResponse struct {
	Edition   string        `json:"edition"`
	Org       string        `json:"org,omitzero"`
	ExpiresAt *time.Time    `json:"expires_at,omitzero"`
	Expired   bool          `json:"expired"`
	Limits    EditionLimits `json:"limits"`
	Usage     *EditionUsage `json:"usage,omitzero"`
}

// EditionLimits is the limits section of the edition response.
// Zero values mean unlimited.
type EditionLimits struct {
	MaxTenants               int `json:"max_tenants"`
	MaxTotalConnections      int `json:"max_total_connections"`
	MaxShards                int `json:"max_shards"`
	MaxTopicsPerTenant       int `json:"max_topics_per_tenant"`
	MaxRoutingRulesPerTenant int `json:"max_routing_rules_per_tenant"`
}

// EditionUsage holds live resource counts. Services populate the fields they own.
type EditionUsage struct {
	Tenants     *int `json:"tenants,omitzero"`
	Connections *int `json:"connections,omitzero"`
	Shards      *int `json:"shards,omitzero"`
}

// UsageFunc is called by the edition handler to get live usage counts.
// Receives the request context for database queries.
// Returns nil if no usage data is available.
type UsageFunc func(ctx context.Context) *EditionUsage

// EditionHandler returns an http.HandlerFunc for the GET /edition endpoint.
// Uses CurrentEdition()/CurrentLimits() for expiry-aware response.
// usageFn may be nil if the service has no usage data to report.
func EditionHandler(mgr *Manager, usageFn UsageFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if mgr == nil {
			_ = httputil.WriteJSON(w, http.StatusOK, EditionResponse{ // WriteJSON error = broken client connection; nothing actionable
				Edition: Community.String(),
				Limits:  limitsToResponse(DefaultLimits(Community)),
			})
			return
		}

		edition := mgr.CurrentEdition()
		limits := mgr.CurrentLimits()

		resp := EditionResponse{
			Edition: edition.String(),
			Org:     mgr.Org(),
			Limits:  limitsToResponse(limits),
		}

		// Include expiry info if there's a license (even expired)
		if claims := mgr.Claims(); claims != nil {
			expiresAt := time.Unix(claims.Exp, 0).UTC()
			resp.ExpiresAt = &expiresAt
			resp.Expired = claims.IsExpired()
		}

		// Include usage if the service provides it
		if usageFn != nil {
			resp.Usage = usageFn(r.Context())
		}

		_ = httputil.WriteJSON(w, http.StatusOK, resp) // WriteJSON error = broken client connection; nothing actionable
	}
}

// limitsToResponse converts Limits to the response struct.
func limitsToResponse(l Limits) EditionLimits {
	return EditionLimits{
		MaxTenants:               l.MaxTenants,
		MaxTotalConnections:      l.MaxTotalConnections,
		MaxShards:                l.MaxShards,
		MaxTopicsPerTenant:       l.MaxTopicsPerTenant,
		MaxRoutingRulesPerTenant: l.MaxRoutingRulesPerTenant,
	}
}
