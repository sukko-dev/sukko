package history

import (
	"fmt"
	"testing"
)

func TestEncodePos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		partition int32
		offset    int64
		want      string
	}{
		{0, 0, "1-0"},
		{0, 42, "1-42"},
		{5, 1234567890, "6-1234567890"},
		{999998, 9999999999, "999999-9999999999"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("p%d_o%d", tt.partition, tt.offset), func(t *testing.T) {
			t.Parallel()
			got := EncodePos(tt.partition, tt.offset)
			if got != tt.want {
				t.Errorf("EncodePos(%d, %d) = %q, want %q", tt.partition, tt.offset, got, tt.want)
			}
		})
	}
}

func TestDecodePos_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := [][2]int64{
		{0, 0},
		{0, 42},
		{5, 1234567890},
		{999998, 9999999999},
		{999998, 0},
	}

	for _, c := range cases {
		partition, offset := int32(c[0]), c[1]
		t.Run(fmt.Sprintf("p%d_o%d", partition, offset), func(t *testing.T) {
			t.Parallel()
			encoded := EncodePos(partition, offset)
			gotP, gotO, ok := DecodePos(encoded)
			if !ok {
				t.Fatalf("DecodePos(%q) ok=false, want true", encoded)
			}
			if gotP != partition || gotO != offset {
				t.Errorf("DecodePos(%q) = (%d, %d), want (%d, %d)", encoded, gotP, gotO, partition, offset)
			}
		})
	}
}

func TestDecodePos_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		s    string
	}{
		{"empty", ""},
		{"no separator", "123"},
		{"ms zero", "0-5"},
		{"ms negative", "-1-5"},
		{"ms at limit", "1000000-5"},
		{"ms above limit (valkey auto-id)", "1748613600000-0"},
		{"ms non-integer", "abc-5"},
		{"seq non-integer", "1-abc"},
		{"both garbage", "foo-bar"},
		{"only separator", "-"},
		{"ms empty", "-5"},
		{"seq empty", "1-"},
		{"negative offset", "1--1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, ok := DecodePos(tt.s)
			if ok {
				t.Errorf("DecodePos(%q) ok=true, want false", tt.s)
			}
		})
	}
}

func TestDecodePos_ValkeyAutoIDDiscriminator(t *testing.T) {
	t.Parallel()

	// Valkey auto-IDs use Unix milliseconds (~1.7×10¹²), always ≥ posMaxPartitionPlusOne.
	valkeyIDs := []string{
		"1748613600000-0",
		"1748613600001-123",
		"9999999999999-0",
	}
	for _, id := range valkeyIDs {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			_, _, ok := DecodePos(id)
			if ok {
				t.Errorf("DecodePos(%q) ok=true — Valkey auto-ID should not decode as pos", id)
			}
		})
	}
}

func BenchmarkEncodePos(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = EncodePos(5, 1234567890)
	}
}

func BenchmarkDecodePos(b *testing.B) {
	s := EncodePos(5, 1234567890)
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = DecodePos(s)
	}
}
