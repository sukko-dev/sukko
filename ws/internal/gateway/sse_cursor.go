package gateway

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// sseCursorPrefix versions the opaque Last-Event-ID cursor so a future format is
// distinguishable and an old per-connection integer id is rejected as foreign.
const sseCursorPrefix = "v1:"

// encodeSSECursor serializes a per-channel {channel: pos} cursor into an opaque,
// versioned Last-Event-ID token (ADR-0030): sseCursorPrefix + base64url(JSON).
// An empty cursor yields the empty string, so no id: line is emitted.
func encodeSSECursor(cursor map[string]string) string {
	if len(cursor) == 0 {
		return ""
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return sseCursorPrefix + base64.RawURLEncoding.EncodeToString(b)
}

// decodeSSECursor parses a Last-Event-ID token back into the {channel: pos}
// cursor. It returns ok=false for an empty, unprefixed (a foreign or pre-ADR-0030
// per-connection id), or unparseable token — the caller then serves live-only
// (ADR-0030 §5). The token is untrusted input; every channel it names is still
// tenant-validated server-side before any replay (ADR-0020).
func decodeSSECursor(token string) (map[string]string, bool) {
	token = strings.TrimSpace(token)
	if !strings.HasPrefix(token, sseCursorPrefix) {
		return nil, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, sseCursorPrefix))
	if err != nil {
		return nil, false
	}
	var cursor map[string]string
	if err := json.Unmarshal(raw, &cursor); err != nil || len(cursor) == 0 {
		return nil, false
	}
	return cursor, true
}

// sseMsgPos extracts channel and pos from a delivered SSE payload envelope so the
// gateway can maintain the reconnect cursor without the server carrying them as
// separate stream fields. Returns ok=false for a payload without a pos (e.g. a
// direct-backend message), which simply does not advance that channel's cursor.
func sseMsgPos(payload []byte) (channel, pos string, ok bool) {
	var e struct {
		Channel string `json:"channel"`
		Pos     string `json:"pos"`
	}
	if err := json.Unmarshal(payload, &e); err != nil || e.Pos == "" {
		return "", "", false
	}
	return e.Channel, e.Pos, true
}

// intersectCursorChannels returns the subset of the decoded reconnect cursor whose channels
// are in the (permission-filtered) requested set, or nil when there is no overlap. The SSE
// Last-Event-ID cursor is opaque to the client, so after an unsubscribe it still carries the
// dropped channel; the server's replay path live-registers whatever it replays, so replaying
// a stale cursor entry would resurrect a channel the client no longer wants. Restricting the
// cursor to the channels this connection actually requests closes that (§II: the gateway
// validates its own inputs — defense in depth over the server-side tenant check).
func intersectCursorChannels(cursor map[string]string, channels []string) map[string]string {
	if len(cursor) == 0 {
		return nil
	}
	requested := make(map[string]struct{}, len(channels))
	for _, ch := range channels {
		requested[ch] = struct{}{}
	}
	out := make(map[string]string, len(cursor))
	for ch, pos := range cursor {
		if _, ok := requested[ch]; ok {
			out[ch] = pos
		}
	}
	if len(out) == 0 {
		return nil // no overlap → live-only, no replay
	}
	return out
}
