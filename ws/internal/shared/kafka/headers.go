package kafka

// Kafka record header keys that form the wire contract between message producers
// (ws-server's producer and the integration tester's direct-to-Kafka publisher)
// and ws-server's consumer. These MUST be identical on both sides — a record
// missing HeaderChannel is dropped by the consumer for editions that require
// channel-header routing.
const (
	// HeaderChannel carries the tenant-qualified channel the record belongs to.
	// The consumer reads it to derive the broadcast subject (Pro/Enterprise).
	HeaderChannel = "x-sukko-channel"
	// HeaderSource identifies the producer that emitted the record (provenance).
	HeaderSource = "source"
	// HeaderTimestamp is the producer-side emit time in Unix milliseconds.
	HeaderTimestamp = "timestamp"
)
