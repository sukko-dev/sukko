package server

import "encoding/json"

// liveReplayRequest is the client→server payload for a live gap recovery replay request.
// Named liveReplayRequest (unexported) to avoid shadowing backend.ReplayRequest.
type liveReplayRequest struct {
	Channel string `json:"channel"`
	FromPos string `json:"from_pos"`
}

// replayMessageEnvelope is the server→client envelope for a single replayed message.
// Parallel to HistoryMessageEnvelope in history_protocol.go.
type replayMessageEnvelope struct {
	Type    string          `json:"type"` // always RespTypeReplayMessage
	Seq     int64           `json:"seq"`
	Channel string          `json:"channel"`
	Ts      int64           `json:"ts"`
	Data    json.RawMessage `json:"data"`
	Pos     string          `json:"pos,omitempty"`
	// Mid is the stable message identity, identical to the live copy's
	// (recomputed from record coordinates; ADR-0008).
	Mid string `json:"mid,omitempty"`
}

// replayCompleteEnvelope is the server→client signal that replay is done.
// Parallel to HistoryCompleteEnvelope in history_protocol.go.
// Truncated is true when delivery was cut short by context cancellation or timeout.
// §XV: no Source field — live replay always fetches from Kafka; including "kafka" as a
// constant in every response adds noise without client value (KISS).
type replayCompleteEnvelope struct {
	Type             string `json:"type"` // always RespTypeReplayComplete
	Channel          string `json:"channel"`
	MessagesReplayed int    `json:"messages_replayed"`   // spec-defined name (cf. history_complete "count")
	Truncated        bool   `json:"truncated,omitempty"` // true = delivery cut short; absent when false
}

// replayErrorEnvelope is the server→client error response from sendReplayError.
// Parallel to HistoryErrorEnvelope in history_protocol.go.
// Uses MsgTypeError ("error") per acceptance criteria.
type replayErrorEnvelope struct {
	Type    string `json:"type"` // always MsgTypeError ("error")
	Channel string `json:"channel"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ReplayResponseKind label values for LiveReplayResponseDropped counter (§I: no magic strings).
const (
	ReplayResponseKindComplete = "complete"
	ReplayResponseKindError    = "error"
)

// LiveReplayOutcome label values for LiveReplayRequestsTotal counter (§I: no magic strings).
const (
	LiveReplayOutcomeStarted               = "started"
	LiveReplayOutcomeCompleted             = "completed"
	LiveReplayOutcomeFailed                = "failed"
	LiveReplayOutcomeTimeout               = "timeout" // deliveryCtx canceled
	LiveReplayOutcomeRejectedNotSubscribed = "rejected_not_subscribed"
	LiveReplayOutcomeRejectedInProgress    = "rejected_in_progress"
	LiveReplayOutcomeRejectedOutOfRange    = "rejected_out_of_range"
	LiveReplayOutcomeRejectedRateLimited   = "rejected_rate_limited"
)
