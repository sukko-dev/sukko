package runner

import "testing"

// TestSSEEventIDError pins the reconnect-cursor validation for a received SSE `id:` (ADR-0030):
// absent is valid (direct backend, or between cadence emissions on Kafka); a present id must be the
// opaque `v1:` cursor. This is the check that replaced the dead pre-ADR-0030 per-message-sequence
// "every event has an id" assertion, which failed on direct cells.
func TestSSEEventIDError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		id   string
		want string
	}{
		{"absent is valid (direct backend / between cadence)", "", ""},
		{"v1 cursor is valid", "v1:eyJzdWtrby5CVEMudHJhZGUiOiIyLTUifQ", ""},
		{"bare sequence is rejected (the removed per-message contract)", "1234", `event id "1234" is not a v1 reconnect cursor`},
		{"foreign token is rejected", "garbage", `event id "garbage" is not a v1 reconnect cursor`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sseEventIDError(tc.id); got != tc.want {
				t.Errorf("sseEventIDError(%q) = %q, want %q", tc.id, got, tc.want)
			}
		})
	}
}
