package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSMiddleware(t *testing.T) {
	t.Parallel()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name            string
		allowedOrigins  []string
		origin          string
		method          string
		wantStatus      int
		wantCORSOrigin  string // expected Access-Control-Allow-Origin header ("" = absent)
		wantCORSHeaders string // expected Access-Control-Allow-Headers header ("" = absent)
	}{
		{
			name:           "no origin header — passthrough",
			allowedOrigins: []string{"*"},
			origin:         "",
			method:         "GET",
			wantStatus:     http.StatusOK,
			wantCORSOrigin: "",
		},
		{
			name:            "wildcard allows any origin",
			allowedOrigins:  []string{"*"},
			origin:          "https://example.com",
			method:          "GET",
			wantStatus:      http.StatusOK,
			wantCORSOrigin:  "https://example.com",
			wantCORSHeaders: "Authorization, X-API-Key, Content-Type",
		},
		{
			name:            "exact match allowed",
			allowedOrigins:  []string{"https://myapp.com", "https://admin.myapp.com"},
			origin:          "https://myapp.com",
			method:          "GET",
			wantStatus:      http.StatusOK,
			wantCORSOrigin:  "https://myapp.com",
			wantCORSHeaders: "Authorization, X-API-Key, Content-Type",
		},
		{
			name:           "disallowed origin — no CORS headers",
			allowedOrigins: []string{"https://myapp.com"},
			origin:         "https://evil.com",
			method:         "GET",
			wantStatus:     http.StatusOK,
			wantCORSOrigin: "",
		},
		{
			name:            "preflight OPTIONS returns 204",
			allowedOrigins:  []string{"*"},
			origin:          "https://example.com",
			method:          "OPTIONS",
			wantStatus:      http.StatusNoContent,
			wantCORSOrigin:  "https://example.com",
			wantCORSHeaders: "Authorization, X-API-Key, Content-Type",
		},
		{
			name:           "preflight with disallowed origin — 204 but no CORS headers",
			allowedOrigins: []string{"https://myapp.com"},
			origin:         "https://evil.com",
			method:         "OPTIONS",
			wantStatus:     http.StatusNoContent,
			wantCORSOrigin: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := CORSMiddleware(tt.allowedOrigins)(inner)

			req := httptest.NewRequest(tt.method, "/test", http.NoBody)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}

			gotOrigin := rec.Header().Get("Access-Control-Allow-Origin")
			if gotOrigin != tt.wantCORSOrigin {
				t.Errorf("CORS origin = %q, want %q", gotOrigin, tt.wantCORSOrigin)
			}

			gotHeaders := rec.Header().Get("Access-Control-Allow-Headers")
			if tt.wantCORSHeaders != "" && gotHeaders != tt.wantCORSHeaders {
				t.Errorf("CORS headers = %q, want %q", gotHeaders, tt.wantCORSHeaders)
			}
			if tt.wantCORSHeaders == "" && gotHeaders != "" {
				t.Errorf("CORS headers should be absent, got %q", gotHeaders)
			}
		})
	}
}

func TestIsOriginAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		origin  string
		allowed []string
		want    bool
	}{
		{"wildcard", "https://any.com", []string{"*"}, true},
		{"exact match", "https://myapp.com", []string{"https://myapp.com"}, true},
		{"no match", "https://evil.com", []string{"https://myapp.com"}, false},
		{"empty list", "https://any.com", []string{}, false},
		{"multiple with match", "https://b.com", []string{"https://a.com", "https://b.com"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isOriginAllowed(tt.origin, tt.allowed); got != tt.want {
				t.Errorf("isOriginAllowed(%q, %v) = %v, want %v", tt.origin, tt.allowed, got, tt.want)
			}
		})
	}
}
