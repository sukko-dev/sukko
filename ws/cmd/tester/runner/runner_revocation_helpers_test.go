package runner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRevokeTokenWithClient covers all HTTP response shapes (§VIII table-driven).
func TestRevokeTokenWithClient(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		statusCode int
		body       string
		wantCode   int
		wantErr    bool
	}{
		{name: "200 OK", statusCode: 200, wantCode: 200},
		{name: "400 bad request", statusCode: 400, wantCode: 400},
		{name: "403 forbidden", statusCode: 403, wantCode: 403},
		{name: "429 too many requests", statusCode: 429, wantCode: 429},
		{name: "500 internal error", statusCode: 500, wantCode: 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()

			code, err := revokeTokenWithClient(context.Background(), srv.Client(),
				srv.URL, "tok", "tenant1", revokeRequest{Sub: "user"})
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if code != tc.wantCode {
				t.Errorf("code: got %d, want %d", code, tc.wantCode)
			}
		})
	}

	t.Run("conn refused", func(t *testing.T) {
		t.Parallel()
		_, err := revokeTokenWithClient(context.Background(), &http.Client{Timeout: time.Second},
			"http://127.0.0.1:1", "tok", "tenant1", revokeRequest{Sub: "user"})
		if err == nil {
			t.Fatal("expected error for refused connection")
		}
	})

	t.Run("ctx cancel", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already canceled
		_, err := revokeTokenWithClient(ctx, srv.Client(), srv.URL, "tok", "tenant1", revokeRequest{Sub: "user"})
		if err == nil {
			t.Fatal("expected error on canceled context")
		}
	})
}

// TestParseRetryAfter covers header parsing, defaulting, and clamping (§VIII).
func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"empty → default", "", defaultRevokeRetryAfter},
		{"garbage → default", "soon", defaultRevokeRetryAfter},
		{"zero → default", "0", defaultRevokeRetryAfter},
		{"parses seconds", "3", 3 * time.Second},
		{"clamps to cap", "9999", maxRevokeRetryAfter},
		{"whitespace trimmed", " 1 ", 1 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := parseRetryAfter(tc.header); got != tc.want {
				t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}

// TestRevokeToken_RetriesOn429 verifies the validate-suite revoke honors Retry-After and returns the
// eventual non-429 status (defense-in-depth). The retry path does not fire in the green e2e run
// (the e2e stack raises the revoke limit), so it is covered deterministically here (§VIII).
func TestRevokeToken_RetriesOn429(t *testing.T) {
	t.Parallel()

	t.Run("429 then 200 → 200 after retry", func(t *testing.T) {
		t.Parallel()
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		code, err := revokeToken(context.Background(), srv.URL, "tok", "tenant1", revokeRequest{JTI: "j"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if code != http.StatusOK {
			t.Errorf("code = %d, want 200 (should retry past the 429)", code)
		}
		if got := hits.Load(); got != 2 {
			t.Errorf("server hits = %d, want 2 (one 429 + one 200)", got)
		}
	})

	t.Run("non-429 returns immediately without retry", func(t *testing.T) {
		t.Parallel()
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusForbidden)
		}))
		defer srv.Close()

		code, err := revokeToken(context.Background(), srv.URL, "tok", "tenant1", revokeRequest{JTI: "j"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if code != http.StatusForbidden {
			t.Errorf("code = %d, want 403", code)
		}
		if got := hits.Load(); got != 1 {
			t.Errorf("server hits = %d, want 1 (no retry on non-429)", got)
		}
	})

	t.Run("ctx canceled during retry wait → returns last 429 + error", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		code, err := revokeToken(ctx, srv.URL, "tok", "tenant1", revokeRequest{JTI: "j"})
		if err == nil {
			t.Fatal("expected error when context is canceled during the retry wait")
		}
		if code != http.StatusTooManyRequests {
			t.Errorf("code = %d, want 429 (last observed status on ctx cancel)", code)
		}
	})
}

// TestIsErrorRateExceeded validates the strict greater-than semantics.
func TestIsErrorRateExceeded(t *testing.T) {
	t.Parallel()
	cases := []struct {
		errors, total int
		threshold     float64
		want          bool
	}{
		{0, 0, 0.05, false},    // zero denominator → false
		{0, 100, 0.05, false},  // 0% < 5%
		{5, 100, 0.05, false},  // exactly 5% → NOT exceeded (strict >)
		{6, 100, 0.05, true},   // 6% > 5%
		{100, 100, 0.05, true}, // 100% > 5%
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d/%d@%.0f%%", tc.errors, tc.total, 100*tc.threshold), func(t *testing.T) {
			t.Parallel()
			got := isErrorRateExceeded(tc.errors, tc.total, tc.threshold)
			if got != tc.want {
				t.Errorf("isErrorRateExceeded(%d,%d,%.2f) = %v, want %v", tc.errors, tc.total, tc.threshold, got, tc.want)
			}
		})
	}
}

// TestPollGaugesUntil covers immediate match, poll-until match, timeout, scrape error, and ctx cancel.
func TestPollGaugesUntil(t *testing.T) {
	t.Parallel()

	t.Run("immediate", func(t *testing.T) {
		t.Parallel()
		v, err := pollGaugesUntil(context.Background(),
			func(_ context.Context) (map[string]float64, error) { return map[string]float64{"m": 42}, nil },
			"m", func(x float64) bool { return x == 42 },
			1*time.Millisecond, 100*time.Millisecond,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != 42 {
			t.Errorf("got %v, want 42", v)
		}
	})

	t.Run("after N polls", func(t *testing.T) {
		t.Parallel()
		count := 0
		v, err := pollGaugesUntil(context.Background(),
			func(_ context.Context) (map[string]float64, error) {
				count++
				return map[string]float64{"m": float64(count)}, nil
			},
			"m", func(x float64) bool { return x >= 3 },
			1*time.Millisecond, 100*time.Millisecond,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v < 3 {
			t.Errorf("got %v, want ≥ 3", v)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		_, err := pollGaugesUntil(context.Background(),
			func(_ context.Context) (map[string]float64, error) { return map[string]float64{"m": 0}, nil },
			"m", func(x float64) bool { return x > 999 },
			1*time.Millisecond, 5*time.Millisecond,
		)
		if err == nil {
			t.Fatal("expected timeout error")
		}
	})

	t.Run("scrape error retries then timeout", func(t *testing.T) {
		t.Parallel()
		_, err := pollGaugesUntil(context.Background(),
			func(_ context.Context) (map[string]float64, error) { return nil, errors.New("scrape failed") },
			"m", func(x float64) bool { return x > 0 },
			1*time.Millisecond, 5*time.Millisecond,
		)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "scrape failed") {
			t.Errorf("expected last scrape error in message, got: %v", err)
		}
	})

	t.Run("ctx cancel", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pollGaugesUntil(ctx,
			func(_ context.Context) (map[string]float64, error) { return map[string]float64{"m": 0}, nil },
			"m", func(x float64) bool { return x > 999 },
			1*time.Millisecond, 100*time.Millisecond,
		)
		if err == nil {
			t.Fatal("expected error on canceled context")
		}
	})
}

// TestScrapeGatewayMetrics verifies counter/gauge summing and non-200 error handling.
func TestScrapeGatewayMetrics(t *testing.T) {
	t.Parallel()

	t.Run("sums counter labels", func(t *testing.T) {
		t.Parallel()
		body := `# HELP gateway_token_force_disconnects_total Total force disconnects.
# TYPE gateway_token_force_disconnects_total counter
gateway_token_force_disconnects_total{type="jwt",transport="ws"} 7
gateway_token_force_disconnects_total{type="apikey",transport="ws"} 3
`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprint(w, body)
		}))
		defer srv.Close()

		gauges, err := scrapeGatewayMetrics(context.Background(), srv.URL)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got := gauges["gateway_token_force_disconnects_total"]
		if got != 10 {
			t.Errorf("sum = %v, want 10", got)
		}
	})

	t.Run("non-200 returns error", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		_, err := scrapeGatewayMetrics(context.Background(), srv.URL)
		if err == nil {
			t.Fatal("expected error for non-200 response")
		}
	})
}
