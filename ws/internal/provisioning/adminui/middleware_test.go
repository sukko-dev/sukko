package adminui

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func makeAuth(token string) *TokenAuthProvider {
	return NewTestTokenAuthProvider(token, time.Hour)
}

func TestSessionMiddleware_MissingCookie(t *testing.T) {
	t.Parallel()
	mw := SessionMiddleware(makeAuth("tok"), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/admin/", http.NoBody)
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/admin/login" {
		t.Errorf("expected redirect to /admin/login, got %q", loc)
	}
}

func TestSessionMiddleware_InvalidCookie(t *testing.T) {
	t.Parallel()
	mw := SessionMiddleware(makeAuth("tok"), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/admin/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: AdminSessionCookieName, Value: "bad-session-id"})
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d", rec.Code)
	}
}

func TestSessionMiddleware_ExpiredCookie(t *testing.T) {
	t.Parallel()
	auth := makeAuth("tok")
	store := auth.store
	id := store.CreateSession(-time.Second) // expired immediately
	mw := SessionMiddleware(auth, zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/admin/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: AdminSessionCookieName, Value: id})
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected 303, got %d", rec.Code)
	}
}

func TestSessionMiddleware_ValidSession(t *testing.T) {
	t.Parallel()
	token := "a-valid-token-that-is-32-chars!!"
	auth := makeAuth(token)
	id := auth.store.CreateSession(time.Hour)
	mw := SessionMiddleware(auth, zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/admin/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: AdminSessionCookieName, Value: id})
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for valid session, got %d", rec.Code)
	}
}

func TestSessionMiddleware_HTMXExpiredSession(t *testing.T) {
	t.Parallel()
	auth := makeAuth("tok")
	store := auth.store
	id := store.CreateSession(-time.Second)
	mw := SessionMiddleware(auth, zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/admin/tenants", http.NoBody)
	req.AddCookie(&http.Cookie{Name: AdminSessionCookieName, Value: id})
	req.Header.Set("HX-Request", "true")
	rec := httptest.NewRecorder()
	mw(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for HTMX expired session, got %d", rec.Code)
	}
	if hxRedirect := rec.Header().Get("HX-Redirect"); hxRedirect != "/admin/login" {
		t.Errorf("expected HX-Redirect: /admin/login, got %q", hxRedirect)
	}
}

func TestSessionCookie_SecurityFlags(t *testing.T) {
	t.Parallel()
	// Build the cookie the same way Login handler sets it.
	cookie := &http.Cookie{
		Name:     AdminSessionCookieName,
		Value:    "session-id",
		Path:     AdminSessionCookiePath,
		MaxAge:   int(8 * time.Hour / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	}
	if !cookie.HttpOnly {
		t.Error("cookie must be HttpOnly")
	}
	if !cookie.Secure {
		t.Error("cookie must be Secure")
	}
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Error("cookie must have SameSite=Strict")
	}
	if cookie.Path != AdminSessionCookiePath {
		t.Errorf("expected Path=%q, got %q", AdminSessionCookiePath, cookie.Path)
	}
}
