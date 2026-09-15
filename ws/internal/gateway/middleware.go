package gateway

import (
	"fmt"
	"net/http"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// RequireFeature returns middleware that gates a handler behind an edition feature check.
// Editions below the feature's required edition get 403 EDITION_LIMIT.
// Used on http.ServeMux — no chi migration required.
func RequireFeature(mgr *license.Manager, feature license.Feature) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if mgr != nil && !mgr.HasFeature(feature) {
				httputil.WriteError(w, http.StatusForbidden, "EDITION_LIMIT",
					fmt.Sprintf("Feature %q requires %s edition or higher", feature, license.RequiredEdition(feature)))
				return
			}
			next(w, r)
		}
	}
}
