package adminui

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// stubTransport is a hermetic http.RoundTripper returning a canned status or error
// with no network I/O. probeHealth's classification (status/error → status string) is
// pure; routing it through real kernel TCP (httptest.NewServer + a loopback dial) made
// the test hostage to transient socket errors on loaded CI runners (§VIII determinism).
type stubTransport struct {
	status int
	err    error
}

func (s stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &http.Response{StatusCode: s.status, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
}

func TestWorstStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		statuses []string
		want     string
	}{
		{"all ok", []string{"ok", "ok", "ok"}, "ok"},
		{"one unconfigured", []string{"ok", "unconfigured", "ok"}, "unconfigured"},
		{"one degraded", []string{"ok", "degraded", "ok"}, "degraded"},
		{"one unknown", []string{"ok", "unknown", "ok"}, "unknown"},
		{"unknown beats degraded", []string{"degraded", "unknown"}, "unknown"},
		{"unknown beats unconfigured", []string{"unconfigured", "unknown"}, "unknown"},
		{"degraded beats unconfigured", []string{"unconfigured", "degraded"}, "degraded"},
		{"single ok", []string{"ok"}, "ok"},
		{"single unknown", []string{"unknown"}, "unknown"},
		{"empty", []string{}, "ok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := worstStatus(tt.statuses...)
			if got != tt.want {
				t.Errorf("worstStatus(%v) = %q, want %q", tt.statuses, got, tt.want)
			}
		})
	}
}

func TestProbeHealth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		transport stubTransport
		url       string
		want      string
	}{
		{"2xx → ok", stubTransport{status: http.StatusOK}, "http://ws-server/health", "ok"},
		{"503 → degraded", stubTransport{status: http.StatusServiceUnavailable}, "http://ws-server/health", "degraded"},
		{"404 → degraded", stubTransport{status: http.StatusNotFound}, "http://ws-server/health", "degraded"},
		// Transport error (unreachable upstream) → unknown. Folds in the former
		// TestProbeHealth_Unreachable without a real closed-socket dial (which itself flaked:
		// a concurrent test could rebind the freed port, flipping "unknown" to "ok").
		{"transport error → unknown", stubTransport{err: errors.New("connect: connection refused")}, "http://ws-server/health", "unknown"},
		// Malformed URL → NewRequestWithContext fails → unknown (previously-untested branch).
		{"invalid url → unknown", stubTransport{status: http.StatusOK}, "://bad-url", "unknown"},
		{"empty url → unconfigured", stubTransport{}, "", "unconfigured"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: tt.transport}
			got := probeHealth(t.Context(), zerolog.Nop(), client, tt.url)
			if got != tt.want {
				t.Errorf("probeHealth(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

// TestProbeHealth_LogsOnError asserts the error paths are no longer silent: a Debug line with the
// url + err is emitted (§III), while a healthy probe stays quiet.
func TestProbeHealth_LogsOnError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		transport stubTransport
		url       string
		wantLog   bool
		wantMsg   string
	}{
		{"transport error logs", stubTransport{err: errors.New("connection refused")}, "http://ws-server/health", true, "transport error"},
		{"invalid url logs", stubTransport{status: http.StatusOK}, "://bad-url", true, "request build failed"},
		{"2xx does not log", stubTransport{status: http.StatusOK}, "http://ws-server/health", false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
			client := &http.Client{Transport: tt.transport}
			_ = probeHealth(t.Context(), logger, client, tt.url)
			out := buf.String()
			if tt.wantLog {
				if !strings.Contains(out, tt.wantMsg) {
					t.Errorf("log missing %q; got %s", tt.wantMsg, out)
				}
				if !strings.Contains(out, `"url":`) || !strings.Contains(out, `"error":`) {
					t.Errorf("log missing url/error field; got %s", out)
				}
			} else if out != "" {
				t.Errorf("expected no log on success, got %s", out)
			}
		})
	}
}
