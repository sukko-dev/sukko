package server

import "encoding/json"

// HistoryRequest is the client-sent payload for a history or subscribe-with-history request.
type HistoryRequest struct {
	Channel string `json:"channel"`
	Limit   int    `json:"limit"`
	// FromTs and ToTs (time-range queries) will be added when that feature is implemented.
}

// HistoryMessageEnvelope wraps a historical message delivered to a client.
type HistoryMessageEnvelope struct {
	Type    string          `json:"type"`
	Seq     int64           `json:"seq"`
	Ts      int64           `json:"ts"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	History bool            `json:"history"`
	Pos     string          `json:"pos,omitempty"`
	// Mid is the stable message identity — identical to the live copy's mid
	// (ADR-0008). Absent for entries stored before the field existed.
	Mid string `json:"mid,omitempty"`
}

// HistoryCompleteEnvelope signals that history delivery is finished.
type HistoryCompleteEnvelope struct {
	Type      string `json:"type"`
	Channel   string `json:"channel"`
	Count     int    `json:"count"`
	Source    string `json:"source"`
	Truncated bool   `json:"truncated,omitempty"` // true when delivery timed out before all entries were sent
}

// HistoryErrorEnvelope signals a history delivery failure.
type HistoryErrorEnvelope struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Channel string `json:"channel"`
	Message string `json:"message"`
}
