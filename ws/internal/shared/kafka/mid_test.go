package kafka

import (
	"hash/fnv"
	"strconv"
	"strings"
	"testing"
)

func TestMessageID_Deterministic(t *testing.T) {
	t.Parallel()
	a := MessageID("prod.acme.odds", 3, 1048576)
	b := MessageID("prod.acme.odds", 3, 1048576)
	if a != b {
		t.Errorf("MessageID not deterministic: %q vs %q", a, b)
	}
}

func TestMessageID_DistinctPerCoordinate(t *testing.T) {
	t.Parallel()
	base := MessageID("prod.acme.odds", 3, 1048576)
	tests := []struct {
		name string
		mid  string
	}{
		{"different topic", MessageID("prod.acme.trades", 3, 1048576)},
		{"different partition", MessageID("prod.acme.odds", 4, 1048576)},
		{"different offset", MessageID("prod.acme.odds", 3, 1048577)},
	}
	for _, tt := range tests {
		if tt.mid == base {
			t.Errorf("%s: MessageID collided with base %q", tt.name, base)
		}
	}
}

// Golden format: hex(fnv1a64(topic)) + "-" + partition + "-" + offset.
// The topic component is hashed (internal topic names carry infra naming and
// must not leak to clients); partition/offset stay readable for ops
// correlation. Clients treat the whole mid as opaque.
func TestMessageID_GoldenFormat(t *testing.T) {
	t.Parallel()
	h := fnv.New64a()
	_, _ = h.Write([]byte("prod.acme.odds"))
	want := strconv.FormatUint(h.Sum64(), 16) + "-3-1048576"

	if got := MessageID("prod.acme.odds", 3, 1048576); got != want {
		t.Errorf("MessageID = %q, want %q", got, want)
	}
}

func TestMessageID_BoundedLength(t *testing.T) {
	t.Parallel()
	// Documented client contract: opaque string ≤ 64 chars.
	mid := MessageID(strings.Repeat("t", 300), 2147483647, 9223372036854775807)
	if len(mid) > 64 {
		t.Errorf("MessageID length = %d, want ≤ 64 (%q)", len(mid), mid)
	}
}
