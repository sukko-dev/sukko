// Package server implements the core WebSocket server with sharded connections,
// Kafka consumption, and Valkey broadcast.
//
// Protocol constants and types in this file are used exclusively by the
// ws-server and MUST NOT be imported by the gateway. Shared types (used by
// both gateway and server) remain in shared/protocol. All ErrorCode constants
// (including server-originated ones) are defined in shared/protocol/errors.go.
package server

// Message type constants for client→server messages handled only by the server.
const (
	MsgTypeReconnect = "reconnect"
	MsgTypeHeartbeat = "heartbeat"
	MsgTypeHistory   = "history"
	MsgTypeReplay    = "replay" // client→server: request live gap recovery replay
)

// Message type constants for server→client messages produced only by the server.
const (
	MsgTypeMessage = "message"
	MsgTypePong    = "pong"
	MsgTypeError   = "error"
	MsgTypeGap     = "gap" // server→client: gap notification (message drop detected)
)

// Replay response type constants (server→client gap recovery protocol).
const (
	RespTypeReplayMessage  = "replay_message"  // single replayed message envelope
	RespTypeReplayComplete = "replay_complete" // signals end of replay stream
)

// Response type constants for server-only acknowledgments and errors.
const (
	RespTypeSubscriptionAck   = "subscription_ack"
	RespTypeUnsubscriptionAck = "unsubscription_ack"
	RespTypeReconnectAck      = "reconnect_ack"
	RespTypeReconnectError    = "reconnect_error"
	RespTypeSubscribeError    = "subscribe_error"
	RespTypeUnsubscribeError  = "unsubscribe_error"
	RespTypeHistoryComplete   = "history_complete"
	RespTypeHistoryError      = "history_error"
)

// UnsubscribeData is the payload for unsubscribe messages, used only by the server.
type UnsubscribeData struct {
	Channels []string `json:"channels"`
}
