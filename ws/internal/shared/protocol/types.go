// Package protocol provides shared WebSocket protocol types, constants, and error codes
// used by both gateway and server components.
//
// Server-only types (MsgTypeReconnect, MsgTypeHeartbeat, MsgTypeMessage, MsgTypePong,
// MsgTypeError, RespTypeSubscriptionAck, RespTypeUnsubscriptionAck, RespTypeReconnectAck,
// RespTypeReconnectError, RespTypeSubscribeError, RespTypeUnsubscribeError, UnsubscribeData)
// live in ws/internal/server/protocol.go.
package protocol

import "encoding/json"

// Message type constants for client→server messages used by both gateway and server.
const (
	MsgTypeSubscribe   = "subscribe"
	MsgTypeUnsubscribe = "unsubscribe"
	MsgTypePublish     = "publish"
)

// Response type constants used by both gateway and server.
const (
	RespTypePublishAck   = "publish_ack"
	RespTypePublishError = "publish_error"
)

// ClientMessage is the standard envelope for client→server messages.
type ClientMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// SubscribeHistoryOptions is the inline history options within a subscribe-with-history message.
type SubscribeHistoryOptions struct {
	Limit int `json:"limit"`
}

// SubscribeData is the payload for subscribe messages.
// For a multi-channel subscribe, populate Channels.
// For a single-channel subscribe-with-history, populate Channel and History.
type SubscribeData struct {
	Channels []string                 `json:"channels,omitempty"`
	Channel  string                   `json:"channel,omitempty"`
	History  *SubscribeHistoryOptions `json:"history,omitempty"`
}

// PublishData is the client-facing payload for publish messages.
// This struct is deserialized from raw client WebSocket frames.
// It MUST NOT contain internal routing fields — use InternalPublishData for gateway→server.
type PublishData struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// InternalPublishData is the gateway→server publish payload with resolved routing.
// The gateway parses the client's PublishData, resolves routing rules, and constructs
// this struct for forwarding to the server. The server MUST only read TopicSuffix from
// InternalPublishData, never from raw client messages.
type InternalPublishData struct {
	Channel     string          `json:"channel"`
	Data        json.RawMessage `json:"data"`
	TopicSuffix string          `json:"topic_suffix"` // Resolved from tenant routing rules
}
