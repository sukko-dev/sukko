package api

import (
	"fmt"
	"net/http"

	"github.com/sukko-dev/sukko/internal/shared/httputil"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// RequireFeature returns middleware that rejects requests when the current
// edition does not include the specified feature. Returns 403 EDITION_LIMIT
// with the required edition name in the error message.
func RequireFeature(mgr *license.Manager, feature license.Feature) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !mgr.HasFeature(feature) {
				required := license.RequiredEdition(feature)
				httputil.WriteError(w, http.StatusForbidden, errCodeEditionLimit,
					fmt.Sprintf("This feature requires %s edition or higher", required))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
