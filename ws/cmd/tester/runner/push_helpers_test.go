package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// pushServiceAvailable must discriminate "push service not deployed" (gateway
// 502/503/504 from a dead upstream, or transport error) from "deployed but
// answering with an application status" (200, 403 edition gate, etc.).
func TestPushServiceAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{name: "200 OK → deployed", status: http.StatusOK, want: true},
		{name: "403 edition gate → deployed (service answered)", status: http.StatusForbidden, want: true},
		{name: "404 → not deployed (push surface disabled at the gateway, GATEWAY_PUSH_ENABLED=false)", status: http.StatusNotFound, want: false},
		{name: "502 bad gateway → not deployed", status: http.StatusBadGateway, want: false},
		{name: "503 unavailable → not deployed", status: http.StatusServiceUnavailable, want: false},
		{name: "504 gateway timeout → not deployed", status: http.StatusGatewayTimeout, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			if got := pushServiceAvailable(context.Background(), srv.URL, "test-token"); got != tc.want {
				t.Errorf("pushServiceAvailable(status=%d) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestPushServiceAvailable_TransportError(t *testing.T) {
	t.Parallel()
	// Nothing listening on this address → transport error → not deployed.
	if pushServiceAvailable(context.Background(), "http://127.0.0.1:1", "test-token") {
		t.Error("pushServiceAvailable(unreachable) = true, want false")
	}
}
