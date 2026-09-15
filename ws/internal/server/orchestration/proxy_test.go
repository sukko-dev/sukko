package orchestration

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// =============================================================================
// ShardProxy Creation Tests
// =============================================================================

func TestShardProxy_NewShardProxy(t *testing.T) {
	t.Parallel()

	shard := &Shard{ID: 1}
	backendURL, _ := url.Parse("ws://localhost:3005/ws") // constant URL in test
	logger := zerolog.Nop()

	proxy := NewShardProxy(shard, backendURL, logger, 10*time.Second, 60*time.Second)

	if proxy == nil {
		t.Fatal("NewShardProxy should return non-nil proxy")
	}
	if proxy.shard != shard {
		t.Error("Proxy should reference the provided shard")
	}
	if proxy.backendURL != backendURL {
		t.Error("Proxy should reference the provided backendURL")
	}
}

func TestShardProxy_ExplicitTimeouts(t *testing.T) {
	t.Parallel()

	shard := &Shard{ID: 1}
	backendURL, _ := url.Parse("ws://localhost:3005/ws") // constant URL in test
	logger := zerolog.Nop()

	proxy := NewShardProxy(shard, backendURL, logger, 15*time.Second, 90*time.Second)

	// Check explicit timeouts
	if proxy.dialTimeout != 15*time.Second {
		t.Errorf("dialTimeout: got %v, want 15s", proxy.dialTimeout)
	}
	if proxy.messageTimeout != 90*time.Second {
		t.Errorf("messageTimeout: got %v, want 90s", proxy.messageTimeout)
	}
}

// =============================================================================
// buildBackendHeaders Tests
// =============================================================================

func TestBuildBackendHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers map[string]string
		want    map[string]string
	}{
		{
			name: "all identity headers forwarded",
			headers: map[string]string{
				protocol.HeaderTenantID:       "tenant-1",
				protocol.HeaderInternalSecret: "secret-abc",
				protocol.HeaderAPIKeyID:       "key-42",
				protocol.HeaderUserID:         "user-7",
			},
			want: map[string]string{
				protocol.HeaderTenantID:       "tenant-1",
				protocol.HeaderInternalSecret: "secret-abc",
				protocol.HeaderAPIKeyID:       "key-42",
				protocol.HeaderUserID:         "user-7",
			},
		},
		{
			name: "empty identity headers are skipped",
			headers: map[string]string{
				protocol.HeaderTenantID: "tenant-1",
				protocol.HeaderUserID:   "",
			},
			want: map[string]string{
				protocol.HeaderTenantID: "tenant-1",
			},
		},
		{
			name: "non-identity headers are excluded",
			headers: map[string]string{
				protocol.HeaderTenantID: "tenant-1",
				"Authorization":         "Bearer token",
				"Cookie":                "session=xyz",
				"Sec-WebSocket-Key":     "abc123",
			},
			want: map[string]string{
				protocol.HeaderTenantID: "tenant-1",
			},
		},
		{
			name:    "no identity headers yields empty result",
			headers: map[string]string{"Authorization": "Bearer token"},
			want:    map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := http.NewRequest(http.MethodGet, "ws://localhost:3005/ws", http.NoBody)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}

			got := buildBackendHeaders(r)

			if len(got) != len(tt.want) {
				t.Errorf("header count: got %d (%v), want %d (%v)", len(got), got, len(tt.want), tt.want)
			}
			for k, v := range tt.want {
				if got.Get(k) != v {
					t.Errorf("header %q: got %q, want %q", k, got.Get(k), v)
				}
			}
			// Ensure no excluded headers leaked through.
			for k := range tt.headers {
				if _, wanted := tt.want[k]; !wanted && got.Get(k) != "" {
					t.Errorf("header %q leaked through: got %q, want it excluded", k, got.Get(k))
				}
			}
		})
	}
}
