package history

import (
	"strconv"
	"strings"
)

// posMaxPartitionPlusOne discriminates encoded Kafka positions from Valkey auto-IDs.
// Kafka partition+1 values are bounded by real cluster sizes (≪ 1_000_000).
// Valkey auto-IDs use Unix milliseconds (~1.7×10¹²) — always above this threshold.
const posMaxPartitionPlusOne int64 = 1_000_000

// EncodePos encodes a Kafka partition and offset as a Valkey Streams-compatible ID:
// "(partition+1)-offset". This format is accepted by XADD as an explicit message ID,
// making the Kafka position the durable cursor for history replay.
//
// Zero allocations: uses a stack-allocated [32]byte scratch buffer with strconv.AppendInt.
func EncodePos(partition int32, offset int64) string {
	var buf [32]byte
	b := strconv.AppendInt(buf[:0], int64(partition)+1, 10)
	b = append(b, '-')
	b = strconv.AppendInt(b, offset, 10)
	return string(b)
}

// DecodePos parses a pos string back to partition and offset.
// Returns ok=false for any of these conditions:
//  1. no "-" separator found
//  2. left field is not a valid integer
//  3. left field <= 0 (partition+1 must be ≥ 1)
//  4. left field ≥ posMaxPartitionPlusOne (discriminates Valkey auto-IDs)
//  5. right field is not a valid integer
//  6. right field < 0 (Kafka offsets are non-negative)
func DecodePos(s string) (partition int32, offset int64, ok bool) {
	left, right, found := strings.Cut(s, "-")
	if !found {
		return 0, 0, false
	}
	ms, err := strconv.ParseInt(left, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if ms <= 0 || ms >= posMaxPartitionPlusOne {
		return 0, 0, false
	}
	seq, err := strconv.ParseInt(right, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if seq < 0 {
		return 0, 0, false
	}
	return int32(ms - 1), seq, true
}
