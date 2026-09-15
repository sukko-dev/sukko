package runner

import "testing"

// sseEventMsgID extracts the delivered payload's msg_id from a raw SSE `data:` line. The SSE
// transport delivers the FULL broadcast envelope ({"type":"message",...,"data":{"msg_id":...}}),
// so msg_id lives at .data.msg_id — NOT top-level. Parsing top-level (as the WS callback may,
// where the client library has already unwrapped .data) yields "" and makes every SSE delivery
// check time out. This test pins the nested extraction and proves the old top-level parse fails.
func TestSSEEventMsgID(t *testing.T) {
	t.Parallel()
	// A real broadcast envelope as written by the gateway's `data:` line (captured from a
	// live pro/kafka repro). msg_id is nested under .data.
	realEnvelope := `{"type":"message","seq":1,"ts":1784671852908,"channel":"tester-82b67ba7.general.validate","data":{"msg_id":"988254b8-a09b-4bd1-b6e5-844a9944f5bc","ts":1784671852886},"pos":"1-4"}`

	tests := []struct {
		name string
		data string
		want string
	}{
		{name: "real broadcast envelope (nested)", data: realEnvelope, want: "988254b8-a09b-4bd1-b6e5-844a9944f5bc"},
		{name: "minimal nested", data: `{"data":{"msg_id":"abc"}}`, want: "abc"},
		// The bug: msg_id at top level (not under .data) MUST NOT be treated as the payload id —
		// the envelope never puts msg_id there, so top-level-only yields "".
		{name: "top-level msg_id only (bug shape)", data: `{"msg_id":"abc"}`, want: ""},
		{name: "nested empty msg_id", data: `{"data":{"msg_id":""}}`, want: ""},
		{name: "no data field", data: `{"type":"message"}`, want: ""},
		{name: "malformed json", data: `not json`, want: ""},
		{name: "empty string", data: ``, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sseEventMsgID(tt.data); got != tt.want {
				t.Errorf("sseEventMsgID(%q) = %q, want %q", tt.data, got, tt.want)
			}
		})
	}
}
