package runner

import "testing"

func TestHTTPURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ws to http", "ws://localhost:3000", "http://localhost:3000"},
		{"wss to https", "wss://gateway.example.com", "https://gateway.example.com"},
		{"http passthrough", "http://localhost:8080", "http://localhost:8080"},
		{"https passthrough", "https://example.com", "https://example.com"},
		{"empty string", "", ""},
		{"short string", "ws", "ws"},
		{"ws:// only", "ws://", "ws://"},    // len("ws://") == 5, not > 5
		{"wss:// only", "wss://", "wss://"}, // len("wss://") == 6, not > 6
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := httpURL(tt.input)
			if got != tt.want {
				t.Errorf("httpURL(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
