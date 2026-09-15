package routing

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizePattern(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "bare star becomes double star", input: "*", want: "**"},
		{name: "double star unchanged", input: "**", want: "**"},
		{name: "bare star segment in prefix", input: "a.*.b", want: "a.**.b"},
		{name: "double star segment in prefix unchanged", input: "a.**.b", want: "a.**.b"},
		{name: "mixed bare star and literal", input: "a.*.c.*.d", want: "a.**.c.**.d"},
		{name: "mixed bare and double star", input: "*.b.**", want: "**.b.**"},
		{name: "all literal no wildcards", input: "a.b.c", want: "a.b.c"},
		{name: "empty string unchanged", input: "", want: ""},
		{name: "idempotent on already-normalised", input: "a.**", want: "a.**"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizePattern(tc.input)
			if got != tc.want {
				t.Errorf("NormalizePattern(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestMatchRoutingPattern(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		pattern string
		channel string
		want    bool
		wantErr error
	}{
		// spec cases — single-segment wildcard
		{name: "a.* matches a.x", pattern: "a.*", channel: "a.x", want: true},
		{name: "a.* does not match a.x.y", pattern: "a.*", channel: "a.x.y", want: false},
		{name: "a.* does not match a", pattern: "a.*", channel: "a", want: false},

		// spec cases — multi-segment wildcard
		{name: "a.** matches a.b", pattern: "a.**", channel: "a.b", want: true},
		{name: "a.** matches a.b.c", pattern: "a.**", channel: "a.b.c", want: true},
		{name: "a.** does not match a alone", pattern: "a.**", channel: "a", want: false},

		// spec cases — prefix wildcard
		{name: "**.suffix matches a.suffix", pattern: "**.suffix", channel: "a.suffix", want: true},
		{name: "**.suffix matches a.b.suffix", pattern: "**.suffix", channel: "a.b.suffix", want: true},
		{name: "**.suffix does not match suffix alone", pattern: "**.suffix", channel: "suffix", want: false},

		// spec cases — bare double star
		{name: "bare ** matches a", pattern: "**", channel: "a", want: true},
		{name: "bare ** matches a.b.c", pattern: "**", channel: "a.b.c", want: true},

		// post-migration rewritten patterns (same as a.**)
		{name: "a.** matches a.x.y post-migration", pattern: "a.**", channel: "a.x.y", want: true},

		// literal segment must match exactly
		{name: "literal mismatch", pattern: "a.b.c", channel: "a.b.d", want: false},
		{name: "literal exact match", pattern: "a.b.c", channel: "a.b.c", want: true},

		// mixed wildcards
		{name: "a.*.c matches a.x.c", pattern: "a.*.c", channel: "a.x.c", want: true},
		{name: "a.*.c does not match a.x.y.c", pattern: "a.*.c", channel: "a.x.y.c", want: false},
		{name: "a.**.c matches a.b.c", pattern: "a.**.c", channel: "a.b.c", want: true},
		{name: "a.**.c matches a.b.d.c", pattern: "a.**.c", channel: "a.b.d.c", want: true},
		{name: "a.**.c does not match a.c", pattern: "a.**.c", channel: "a.c", want: false},

		// error cases
		{name: "multi double-star returns error", pattern: "a.**.**.b", channel: "a.x.y.b", want: false, wantErr: ErrMultipleDoubleWildcard},
		{name: "multiple bare ** returns error after normalize", pattern: "**.b.**", channel: "a.b.c", want: false, wantErr: ErrMultipleDoubleWildcard},

		// empty channel
		{name: "empty channel returns false", pattern: "a.*", channel: "", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := MatchRoutingPattern(tc.pattern, tc.channel)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("MatchRoutingPattern(%q, %q) error = %v, want %v", tc.pattern, tc.channel, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("MatchRoutingPattern(%q, %q) unexpected error: %v", tc.pattern, tc.channel, err)
			}
			if got != tc.want {
				t.Errorf("MatchRoutingPattern(%q, %q) = %v, want %v", tc.pattern, tc.channel, got, tc.want)
			}
		})
	}
}

func BenchmarkMatchRoutingPattern(b *testing.B) {
	patterns := make([]string, 100)
	for i := range patterns {
		switch i % 5 {
		case 0:
			patterns[i] = "tenant.**.price"
		case 1:
			patterns[i] = "tenant.*"
		case 2:
			patterns[i] = "tenant.BTC.trade"
		case 3:
			patterns[i] = "**.price"
		case 4:
			patterns[i] = "tenant.**"
		}
	}
	channel := "tenant.BTC.price"

	b.ResetTimer()
	for b.Loop() {
		for _, p := range patterns {
			_, _ = MatchRoutingPattern(p, channel)
		}
	}

	// Assert ≤1ms per individual call.
	if b.N > 0 {
		perCall := b.Elapsed() / time.Duration(b.N) / time.Duration(len(patterns))
		if perCall > time.Millisecond {
			b.Fatalf("MatchRoutingPattern too slow: %v per call (want ≤1ms)", perCall)
		}
	}
}
