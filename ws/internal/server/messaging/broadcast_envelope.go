package messaging

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// broadcastPrefix is the constant JSON prefix shared by all broadcast messages.
// Field order matches json.Marshal(MessageEnvelope{}): type, seq, ts, channel, data.
// Effectively constant — MUST NOT be modified after package init.
var broadcastPrefix = []byte(`{"type":"message","seq":`)

// maxInt64Digits is the maximum number of digits in a base-10 int64 representation.
// Used to size the sharedSuffix allocation in NewBroadcastEnvelope.
const maxInt64Digits = 20

// Suffix fragment sizes (byte lengths of JSON structural tokens).
const (
	suffixTsKey      = 6  // ,"ts":
	suffixChannelKey = 11 // ,"channel":
	suffixDataKey    = 8  // ,"data":
	suffixPosKey     = 7  // ,"pos":
	suffixMidKey     = 7  // ,"mid":
	suffixClose      = 1  // }
)

// BroadcastEnvelope holds pre-computed shared byte fragments for efficient
// per-client message assembly. Immutable after creation — safe for concurrent
// reads by multiple write pump goroutines.
//
// Architecture: The broadcast loop creates one BroadcastEnvelope per event.
// Each write pump goroutine calls AppendTo(buf[:0], seq) to assemble per-client
// bytes into a caller-owned buffer. This defers the ~100ns byte assembly to
// write pumps (parallelized across cores) while keeping the broadcast loop at
// ~25ns/client (atomic + channel send).
type BroadcastEnvelope struct {
	prefix       []byte // {"type":"message","seq":
	sharedSuffix []byte // ,"ts":...,"channel":"...","data":...[,"pos":"..."][,"mid":"..."]}
}

// NewBroadcastEnvelope pre-computes shared byte fragments from broadcast parameters.
// The channel is JSON-escaped via json.Marshal to match json.Marshal(MessageEnvelope{}) output.
// A nil payload is normalized to the JSON literal "null" to produce valid JSON output.
// pos is the encoded Kafka position "(partition+1)-offset"; when non-empty it is appended as
// `,"pos":"<pos>"` in the suffix. mid is the stable message identity (kafka.MessageID or a
// direct-backend UUID); when non-empty it is appended as `,"mid":"<mid>"` after pos. Both use
// omitempty semantics when empty. Both are appended RAW (no JSON escaping) — valid only
// because every producer of these values emits JSON-safe characters (hex/digits/dashes/UUID);
// a new mid or pos source MUST preserve that property or escape here. Per-message values belong here in the shared suffix (built
// once per event, zero per-client cost); only the per-receiver seq is assembled in AppendTo.
// Called once per broadcast event — O(1), not per-client.
func NewBroadcastEnvelope(channel string, timestamp int64, payload []byte, pos, mid string) (*BroadcastEnvelope, error) {
	// Normalize nil payload to JSON null (produces valid "data":null)
	if payload == nil {
		payload = []byte("null")
	}

	// JSON-escape the channel name to match json.Marshal behavior.
	// This handles quotes, backslashes, control characters, and Unicode.
	channelJSON, err := json.Marshal(channel)
	if err != nil {
		return nil, fmt.Errorf("json-escape channel %q: %w", channel, err)
	}

	// Build sharedSuffix: ,"ts":<ts>,"channel":<channelJSON>,"data":<payload>[,"pos":"<pos>"][,"mid":"<mid>"]
	posExtra := 0
	if pos != "" {
		// suffixPosKey + 2 quotes + len(pos)
		posExtra = suffixPosKey + 2 + len(pos)
	}
	midExtra := 0
	if mid != "" {
		// suffixMidKey + 2 quotes + len(mid)
		midExtra = suffixMidKey + 2 + len(mid)
	}
	suffixCap := suffixTsKey + maxInt64Digits + suffixChannelKey + len(channelJSON) + suffixDataKey + len(payload) + posExtra + midExtra + suffixClose
	suffix := make([]byte, 0, suffixCap)
	suffix = append(suffix, `,"ts":`...)
	suffix = strconv.AppendInt(suffix, timestamp, 10)
	suffix = append(suffix, `,"channel":`...)
	suffix = append(suffix, channelJSON...)
	suffix = append(suffix, `,"data":`...)
	suffix = append(suffix, payload...)
	if pos != "" {
		suffix = append(suffix, `,"pos":"`...)
		suffix = append(suffix, pos...)
		suffix = append(suffix, '"')
	}
	if mid != "" {
		suffix = append(suffix, `,"mid":"`...)
		suffix = append(suffix, mid...)
		suffix = append(suffix, '"')
	}
	suffix = append(suffix, '}')

	return &BroadcastEnvelope{
		prefix:       broadcastPrefix,
		sharedSuffix: suffix,
	}, nil
}

// AppendTo appends the assembled message bytes for seq into dst and returns the result.
// Passing dst[:0] reuses the backing array with zero allocs on warm calls.
// Passing nil allocates on the first call; use the returned slice for subsequent calls.
func (e *BroadcastEnvelope) AppendTo(dst []byte, seq int64) []byte {
	dst = append(dst, e.prefix...)
	dst = strconv.AppendInt(dst, seq, 10)
	dst = append(dst, e.sharedSuffix...)
	return dst
}

// Build assembles the final message bytes for seq.
// Equivalent to AppendTo(nil, seq); retained for callers that do not hold a reuse buffer.
func (e *BroadcastEnvelope) Build(seq int64) []byte {
	return e.AppendTo(nil, seq)
}
