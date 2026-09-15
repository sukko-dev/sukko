package server

// GapNotification is enqueued by Broadcast() when a message is dropped for a client.
// Delivered by the write pump as a low-priority "gap" protocol message.
// Internal type — never marshaled directly; wire format is gapEnvelope.
type GapNotification struct {
	Channel string
	FromSeq int64
	ToSeq   int64
	LastPos string // Kafka pos of the DROPPED message (conservative replay anchor)
	Ts      int64  // Unix milliseconds
}

// gapEnvelope is the server→client wire format for gap notifications.
// Parallel to HistoryMessageEnvelope in history_protocol.go.
type gapEnvelope struct {
	Type    string `json:"type"` // always MsgTypeGap
	Channel string `json:"channel"`
	FromSeq int64  `json:"from_seq"`
	ToSeq   int64  `json:"to_seq"`
	LastPos string `json:"last_pos"`
	Ts      int64  `json:"ts"`
}

// GapDropReason label values for GapNotificationsDropped counter (§XV: distinct causes).
const (
	GapDropReasonBufferFull   = "buffer_full"   // client.gapChan was full (client too slow)
	GapDropReasonCoalesceLoss = "coalesce_loss" // cross-channel item could not be re-enqueued during coalescing
	GapDropReasonSendFull     = "send_full"     // c.send was full when write pump tried to forward gap notification
)
