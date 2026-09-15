package httputil

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriteJSON_ErrorPath(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	err := WriteJSON(w, http.StatusOK, make(chan int))
	if err == nil {
		t.Fatal("WriteJSON() with unencodable data should return error")
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		data       any
		wantStatus int
		wantBody   string
	}{
		{
			name:       "simple object",
			status:     http.StatusOK,
			data:       map[string]string{"key": "value"},
			wantStatus: http.StatusOK,
			wantBody:   `{"key":"value"}`,
		},
		{
			name:       "status created",
			status:     http.StatusCreated,
			data:       map[string]int{"id": 123},
			wantStatus: http.StatusCreated,
			wantBody:   `{"id":123}`,
		},
		{
			name:       "array data",
			status:     http.StatusOK,
			data:       []string{"a", "b", "c"},
			wantStatus: http.StatusOK,
			wantBody:   `["a","b","c"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			err := WriteJSON(w, tt.status, tt.data)
			if err != nil {
				t.Fatalf("WriteJSON() error = %v", err)
			}

			if w.Code != tt.wantStatus {
				t.Errorf("WriteJSON() status = %d, want %d", w.Code, tt.wantStatus)
			}

			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("WriteJSON() Content-Type = %q, want %q", ct, "application/json")
			}

			// Parse both as JSON to compare (handles whitespace differences)
			var got, want any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("Failed to parse response: %v", err)
			}
			if err := json.Unmarshal([]byte(tt.wantBody), &want); err != nil {
				t.Fatalf("Failed to parse want: %v", err)
			}

			gotBytes, _ := json.Marshal(got)
			wantBytes, _ := json.Marshal(want)
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Errorf("WriteJSON() body = %s, want %s", gotBytes, wantBytes)
			}
		})
	}
}

func TestWriteError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      int
		code        string
		message     string
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{
			name:        "bad request error",
			status:      http.StatusBadRequest,
			code:        "INVALID_INPUT",
			message:     "Invalid input provided",
			wantStatus:  http.StatusBadRequest,
			wantCode:    "INVALID_INPUT",
			wantMessage: "Invalid input provided",
		},
		{
			name:        "not found error",
			status:      http.StatusNotFound,
			code:        "NOT_FOUND",
			message:     "Resource not found",
			wantStatus:  http.StatusNotFound,
			wantCode:    "NOT_FOUND",
			wantMessage: "Resource not found",
		},
		{
			name:        "internal server error",
			status:      http.StatusInternalServerError,
			code:        "INTERNAL_ERROR",
			message:     "Something went wrong",
			wantStatus:  http.StatusInternalServerError,
			wantCode:    "INTERNAL_ERROR",
			wantMessage: "Something went wrong",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			WriteError(w, tt.status, tt.code, tt.message)

			if w.Code != tt.wantStatus {
				t.Errorf("WriteError() status = %d, want %d", w.Code, tt.wantStatus)
			}

			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("WriteError() Content-Type = %q, want %q", ct, "application/json")
			}

			var resp ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("Failed to parse response: %v", err)
			}

			if resp.Code != tt.wantCode {
				t.Errorf("WriteError() code = %q, want %q", resp.Code, tt.wantCode)
			}
			if resp.Message != tt.wantMessage {
				t.Errorf("WriteError() message = %q, want %q", resp.Message, tt.wantMessage)
			}
		})
	}
}

func TestWriteHealthOK(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "gateway service",
			serviceName: "ws-gateway",
		},
		{
			name:        "server service",
			serviceName: "ws-server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			WriteHealthOK(w, tt.serviceName)

			if w.Code != http.StatusOK {
				t.Errorf("WriteHealthOK() status = %d, want %d", w.Code, http.StatusOK)
			}

			var resp map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("Failed to parse response: %v", err)
			}

			if resp["status"] != "ok" {
				t.Errorf("WriteHealthOK() status = %q, want %q", resp["status"], "ok")
			}
			if resp["service"] != tt.serviceName {
				t.Errorf("WriteHealthOK() service = %q, want %q", resp["service"], tt.serviceName)
			}
		})
	}
}

// TestWriteRateLimited pins the §IX contract for rate-limit responses: HTTP 429 WITH a
// Retry-After header. The header is what lets a well-behaved client back off for the
// right duration instead of hammering an empty token bucket; omitting it turns every
// polite client into an accidental retry storm.
func TestWriteRateLimited(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	WriteRateLimited(rec, 2*time.Second, "RATE_LIMITED", "publish rate limit exceeded")

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want %q (whole seconds, per RFC 9110)", got, "2")
	}
	var body ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Code != "RATE_LIMITED" || body.Message != "publish rate limit exceeded" {
		t.Errorf("body = %+v, want the code and message passed through", body)
	}
}

// TestWriteRateLimited_SubSecondRoundsUp: a sub-second hint must not truncate to
// "Retry-After: 0", which reads as "retry immediately" — the exact behavior the
// header exists to prevent.
func TestWriteRateLimited_SubSecondRoundsUp(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	WriteRateLimited(rec, 100*time.Millisecond, "RATE_LIMITED", "slow down")

	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want %q (sub-second durations round UP)", got, "1")
	}
}
