// This file defines the stable message identity (mid) derivation for the
// Kafka backend — the single source of truth for the mid format.
package kafka

import (
	"hash/fnv"
	"strconv"
)

// MessageID derives the stable, globally unique message identity (mid) for a
// Kafka record from its immutable coordinates. The same record always yields
// the same mid — on live delivery, on replay, and in history — which is what
// makes the mid usable as a deduplication and correlation key. External
// producers need no cooperation: identity is assigned at the ingestion
// boundary from coordinates Kafka itself assigns.
//
// Format: hex(fnv1a64(topic)) + "-" + partition + "-" + offset. The topic is
// hashed because internal topic names ({namespace}.{tenant}.{suffix}) carry
// infrastructure naming that must not leak to clients; partition and offset
// stay readable for operator correlation. Clients MUST treat the mid as an
// opaque string (≤ 64 chars) — the format may differ per backend (the direct
// backend mints UUIDs) and may change between releases.
//
// mid is identity, never a cursor: replay positioning remains the pos
// ("(partition+1)-offset") encoding, and the replay API will never accept a
// mid.
func MessageID(topic string, partition int32, offset int64) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(topic)) // fnv Write never returns an error
	return strconv.FormatUint(h.Sum64(), 16) +
		"-" + strconv.FormatInt(int64(partition), 10) +
		"-" + strconv.FormatInt(offset, 10)
}
