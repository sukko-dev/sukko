package httputil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		query     string
		header    string
		wantToken string
	}{
		{
			name:      "token in query parameter",
			query:     "token=abc123",
			header:    "",
			wantToken: "abc123",
		},
		{
			name:      "token in Authorization header",
			query:     "",
			header:    "Bearer xyz789",
			wantToken: "xyz789",
		},
		{
			name:      "query parameter takes precedence",
			query:     "token=query-token",
			header:    "Bearer header-token",
			wantToken: "query-token",
		},
		{
			name:      "no token provided",
			query:     "",
			header:    "",
			wantToken: "",
		},
		{
			name:      "invalid Authorization header (no Bearer)",
			query:     "",
			header:    "Basic abc123",
			wantToken: "",
		},
		{
			name:      "Authorization header with extra spaces",
			query:     "",
			header:    "Bearer   token-with-spaces",
			wantToken: "  token-with-spaces",
		},
		{
			name:      "empty query token parameter",
			query:     "token=",
			header:    "Bearer fallback",
			wantToken: "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			url := "http://example.com"
			if tt.query != "" {
				url += "?" + tt.query
			}
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			got := ExtractBearerToken(req)
			if got != tt.wantToken {
				t.Errorf("ExtractBearerToken() = %q, want %q", got, tt.wantToken)
			}
		})
	}
}

func TestExtractAPIKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		query   string
		header  string
		wantKey string
	}{
		{
			name:    "api_key in query parameter",
			query:   "api_key=pk_live_abc123",
			header:  "",
			wantKey: "pk_live_abc123",
		},
		{
			name:    "api key in X-API-Key header",
			query:   "",
			header:  "pk_live_xyz789",
			wantKey: "pk_live_xyz789",
		},
		{
			name:    "query parameter takes precedence over header",
			query:   "api_key=pk_live_query",
			header:  "pk_live_header",
			wantKey: "pk_live_query",
		},
		{
			name:    "no api key provided",
			query:   "",
			header:  "",
			wantKey: "",
		},
		{
			name:    "empty query parameter falls back to header",
			query:   "api_key=",
			header:  "pk_live_fallback",
			wantKey: "pk_live_fallback",
		},
		{
			name:    "empty header returns empty",
			query:   "",
			header:  "",
			wantKey: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			url := "http://example.com"
			if tt.query != "" {
				url += "?" + tt.query
			}
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
			if tt.header != "" {
				req.Header.Set("X-API-Key", tt.header)
			}

			got := ExtractAPIKey(req)
			if got != tt.wantKey {
				t.Errorf("ExtractAPIKey() = %q, want %q", got, tt.wantKey)
			}
		})
	}
}

func TestGetClientIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		xff        string
		remoteAddr string
		wantIP     string
	}{
		{
			name:       "single IP in X-Forwarded-For",
			xff:        "192.168.1.100",
			remoteAddr: "10.0.0.1:8080",
			wantIP:     "192.168.1.100",
		},
		{
			name:       "multiple IPs in X-Forwarded-For (takes first)",
			xff:        "192.168.1.100, 10.0.0.1, 172.16.0.1",
			remoteAddr: "127.0.0.1:8080",
			wantIP:     "192.168.1.100",
		},
		{
			name:       "X-Forwarded-For with spaces",
			xff:        "  192.168.1.100  ",
			remoteAddr: "10.0.0.1:8080",
			wantIP:     "192.168.1.100",
		},
		{
			name:       "no X-Forwarded-For, use RemoteAddr",
			xff:        "",
			remoteAddr: "10.0.0.1:8080",
			wantIP:     "10.0.0.1",
		},
		{
			name:       "RemoteAddr without port",
			xff:        "",
			remoteAddr: "10.0.0.1",
			wantIP:     "10.0.0.1",
		},
		{
			name:       "IPv6 address in brackets with port",
			xff:        "",
			remoteAddr: "[::1]:8080",
			wantIP:     "::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.com", nil)
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			req.RemoteAddr = tt.remoteAddr

			got := GetClientIP(req)
			if got != tt.wantIP {
				t.Errorf("GetClientIP() = %q, want %q", got, tt.wantIP)
			}
		})
	}
}
