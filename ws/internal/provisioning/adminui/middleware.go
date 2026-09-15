package adminui

import (
	"net/http"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

// SessionMiddleware validates the admin_session cookie.
// On missing or invalid session it redirects to /admin/login.
// For HTMX partial requests it returns 401 + HX-Redirect so HTMX navigates
// the full page rather than injecting the login form into a target div.
func SessionMiddleware(auth AdminAuthProvider, _ zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := r.Cookie(AdminSessionCookieName)
			if err != nil || c.Value == "" {
				redirectToLogin(w, r)
				return
			}
			if err := auth.ValidateSession(r.Context(), c.Value); err != nil {
				redirectToLogin(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// redirectToLogin sends a full-page redirect for normal requests and an
// HX-Redirect header for HTMX partial requests.
func redirectToLogin(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/admin/login")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

// Gate enforces the AdminUI edition feature gate.
// Community operators receive a 200 upgrade notice page (not a redirect or 403)
// so they can see what the admin UI offers before upgrading. Routes that need
// both session auth and a feature gate (Connections, Analytics, etc.) apply
// httputil.WriteError for JSON partial responses after SessionMiddleware.
func Gate(edMgr *license.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if edMgr == nil || license.EditionHasFeature(edMgr.Edition(), license.AdminUI) {
				next.ServeHTTP(w, r)
				return
			}
			// Community edition: serve the upgrade notice page (200).
			// Auth is NOT required — Community operators should see this without logging in.
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			// Upgrade page HTML is rendered inline here to avoid template dependency in middleware.
			// The real upgrade page is rendered by AdminUIHandler on its own routes.
			_, _ = w.Write(upgradePageHTML())
		})
	}
}

// upgradePageHTML returns a minimal HTML upgrade notice for Community operators.
// The full-featured upgrade template (templates/upgrade.html) is used by AdminUIHandler
// when it is instantiated; this fallback is used only when the handler has not been wired.
func upgradePageHTML() []byte {
	return []byte(`<!DOCTYPE html><html><head><title>Upgrade Required</title></head><body>
<h1>Admin UI — Pro Edition Feature</h1>
<p>The Sukko Admin UI requires a Pro or Enterprise license.
Visit <a href="https://sukko.io/pricing">sukko.io/pricing</a> to upgrade.</p>
</body></html>`)
}
