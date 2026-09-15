// Package httputil provides shared HTTP request/response utilities.
package httputil

import (
	"net/http"
	"strings"
)

// ExtractBearerToken extracts JWT from query param "token" or Authorization header.
// Checks query parameter first (for WebSocket connections), then Authorization header.
func ExtractBearerToken(r *http.Request) string {
	// Check query parameter first (common for WebSocket connections)
	if token := r.URL.Query().Get("token"); token != "" {
		return token
	}

	// Check Authorization header
	auth := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(auth, "Bearer "); ok {
		return after
	}

	return ""
}

// ExtractAPIKey extracts the API key from the query parameter "api_key" or X-API-Key header.
// Checks query parameter first (for WebSocket connections), then header.
func ExtractAPIKey(r *http.Request) string {
	if key := r.URL.Query().Get("api_key"); key != "" {
		return key
	}
	return r.Header.Get("X-API-Key")
}

// GetClientIP extracts the client IP from the request.
// Checks X-Forwarded-For header first (for load balancers/proxies),
// then falls back to RemoteAddr.
func GetClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (standard for load balancers)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Use first IP in the chain (client IP)
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Fall back to RemoteAddr, stripping port if present
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx > 0 {
		// Check if this looks like an IPv6 address in brackets
		if strings.Contains(addr, "[") {
			// IPv6: [::1]:8080 -> [::1] or just ::1
			if bracketEnd := strings.Index(addr, "]"); bracketEnd > 0 {
				return addr[1:bracketEnd] // Strip brackets and port
			}
		}
		return addr[:idx]
	}

	return addr
}
