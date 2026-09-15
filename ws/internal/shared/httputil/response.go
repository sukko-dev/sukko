package httputil

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		return fmt.Errorf("encode JSON response: %w", err)
	}
	return nil
}

// ErrorResponse represents a standard API error response.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WriteError writes a JSON error response with the given status code.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Code: code, Message: message}) // best-effort: client may have disconnected
}

// WriteRateLimited writes a 429 rate-limit rejection WITH the Retry-After header the
// constitution requires (§IX: "Rate limit responses MUST use HTTP 429 with Retry-After
// header"). The header tells a well-behaved client how long to back off; without it,
// clients retry immediately into a still-empty token bucket. Sub-second durations round
// UP so the header never reads "0" ("retry now" — the opposite of its purpose).
func WriteRateLimited(w http.ResponseWriter, retryAfter time.Duration, code, message string) {
	w.Header().Set("Retry-After", strconv.Itoa(RetryAfterSeconds(retryAfter)))
	WriteError(w, http.StatusTooManyRequests, code, message)
}

// RetryAfterSeconds converts a backoff duration to the whole seconds RFC 9110 requires for
// Retry-After, rounding UP and flooring at 1. Exported for the non-JSON rejection paths (a
// WebSocket upgrade keeps a plain-text body) so every 429 in the platform rounds identically.
func RetryAfterSeconds(retryAfter time.Duration) int {
	return max(int(math.Ceil(retryAfter.Seconds())), 1)
}

// WriteHealthOK writes a standard health check response.
func WriteHealthOK(w http.ResponseWriter, serviceName string) {
	_ = WriteJSON(w, http.StatusOK, map[string]string{ // best-effort: client may have disconnected
		"status":  "ok",
		"service": serviceName,
	})
}
