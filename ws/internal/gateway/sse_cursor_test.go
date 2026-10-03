package gateway

import "testing"

func TestSSECursor_RoundTrip(t *testing.T) {
	t.Parallel()
	in := map[string]string{"acme.md-1": "2-5", "acme.md-2": "3-100"}
	tok := encodeSSECursor(in)
	if tok == "" || tok[:3] != "v1:" {
		t.Fatalf("token = %q, want a v1: prefix", tok)
	}
	out, ok := decodeSSECursor(tok)
	if !ok {
		t.Fatal("decode failed for a token we just encoded")
	}
	if len(out) != len(in) {
		t.Fatalf("decoded %d channels, want %d", len(out), len(in))
	}
	for k, v := range in {
		if out[k] != v {
			t.Errorf("channel %q pos = %q, want %q", k, out[k], v)
		}
	}
}

func TestSSECursor_RejectsForeignAndEmpty(t *testing.T) {
	t.Parallel()
	// An old per-connection integer id (no v1: prefix) → live-only.
	if _, ok := decodeSSECursor("12345"); ok {
		t.Error("plain integer id must not decode as a cursor")
	}
	if _, ok := decodeSSECursor(""); ok {
		t.Error("empty token must not decode")
	}
	if _, ok := decodeSSECursor("v1:@@not-base64@@"); ok {
		t.Error("unparseable base64 must not decode")
	}
	if _, ok := decodeSSECursor("v2:eyJhIjoiYiJ9"); ok {
		t.Error("foreign version must not decode")
	}
	if encodeSSECursor(nil) != "" || encodeSSECursor(map[string]string{}) != "" {
		t.Error("empty cursor must encode to empty string (no id: line)")
	}
}

func TestSSEMsgPos(t *testing.T) {
	t.Parallel()
	ch, pos, ok := sseMsgPos([]byte(`{"type":"message","channel":"acme.md-1","pos":"2-6","mid":"m1"}`))
	if !ok || ch != "acme.md-1" || pos != "2-6" {
		t.Fatalf("got (%q,%q,%v), want (acme.md-1,2-6,true)", ch, pos, ok)
	}
	if _, _, ok := sseMsgPos([]byte(`{"channel":"c"}`)); ok {
		t.Error("payload without pos must return ok=false")
	}
	if _, _, ok := sseMsgPos([]byte(`not json`)); ok {
		t.Error("non-JSON payload must return ok=false")
	}
}

func TestIntersectCursorChannels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cursor   map[string]string
		channels []string
		want     map[string]string
	}{
		{
			name:     "keeps only requested channels (unsubscribe: b dropped from request)",
			cursor:   map[string]string{"t.a": "1-5", "t.b": "1-9"},
			channels: []string{"t.a"},
			want:     map[string]string{"t.a": "1-5"},
		},
		{
			name:     "full overlap keeps all",
			cursor:   map[string]string{"t.a": "1-5", "t.b": "1-9"},
			channels: []string{"t.a", "t.b"},
			want:     map[string]string{"t.a": "1-5", "t.b": "1-9"},
		},
		{
			name:     "no overlap → nil (live-only, no replay)",
			cursor:   map[string]string{"t.b": "1-9"},
			channels: []string{"t.a"},
			want:     nil,
		},
		{name: "empty cursor → nil", cursor: map[string]string{}, channels: []string{"t.a"}, want: nil},
		{name: "nil cursor → nil", cursor: nil, channels: []string{"t.a"}, want: nil},
		{
			name:     "requested channel absent from cursor is not invented",
			cursor:   map[string]string{"t.a": "1-5"},
			channels: []string{"t.a", "t.c"},
			want:     map[string]string{"t.a": "1-5"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := intersectCursorChannels(tt.cursor, tt.channels)
			if len(got) != len(tt.want) {
				t.Fatalf("intersectCursorChannels() = %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("key %q = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestSeedCursor_PreservesQuietChannelOnAdvance is the PR1 regression for Last-Event-ID erosion:
// when a reconnect's inbound cursor covers A and B and only B advances this connection (A stays
// quiet), the emitted cursor must still carry A's baseline. Before the seed, the cursor was rebuilt
// from delivered messages alone, so A silently dropped out of the next token.
func TestSeedCursor_PreservesQuietChannelOnAdvance(t *testing.T) {
	t.Parallel()
	inbound := map[string]string{"acme.md-1": "2-5", "acme.md-2": "3-9"}
	cursor := seedCursor(inbound)

	// Mutating the seed must not alias the caller's inbound map.
	cursor["acme.md-2"] = "3-20" // B advances; A (md-1) stays quiet
	if inbound["acme.md-2"] != "3-9" {
		t.Fatalf("seedCursor aliased the inbound map: inbound md-2 = %q, want 3-9", inbound["acme.md-2"])
	}

	out, ok := decodeSSECursor(encodeSSECursor(cursor))
	if !ok {
		t.Fatal("re-decode of the emitted cursor failed")
	}
	if out["acme.md-1"] != "2-5" {
		t.Errorf("quiet channel eroded: md-1 = %q, want 2-5 (preserved baseline)", out["acme.md-1"])
	}
	if out["acme.md-2"] != "3-20" {
		t.Errorf("advanced channel md-2 = %q, want 3-20", out["acme.md-2"])
	}
}

// TestSeedCursor_NilInboundIsEmptyMutable guards the first-connect path: a nil inbound cursor must
// yield an empty, non-nil map the delivery loop can still write into (maps.Clone(nil) would return
// nil and the later cursor[ch]=pos would panic).
func TestSeedCursor_NilInboundIsEmptyMutable(t *testing.T) {
	t.Parallel()
	cursor := seedCursor(nil)
	if cursor == nil {
		t.Fatal("seedCursor(nil) returned a nil map; the delivery loop would panic writing to it")
	}
	if len(cursor) != 0 {
		t.Fatalf("seedCursor(nil) = %v, want empty", cursor)
	}
	cursor["acme.md-1"] = "1-1" // must not panic
}
