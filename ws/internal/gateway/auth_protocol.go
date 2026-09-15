// Package gateway implements the WebSocket reverse proxy with JWT auth,
// tenant isolation, rate limiting, and connection tracking.
//
// Auth refresh protocol constants and types in this file are used exclusively
// by the ws-gateway for mid-connection JWT refresh and MUST NOT be imported
// by the server.
package gateway

// Auth message type constants.
const (
	// MsgTypeAuth is the client→gateway message type for token refresh.
	MsgTypeAuth = "auth"

	// RespTypeAuthAck is sent to the client on successful auth refresh.
	RespTypeAuthAck = "auth_ack"

	// RespTypeAuthError is sent to the client on failed auth refresh.
	RespTypeAuthError = "auth_error"

	// RespTypeUnsubscriptionAck is the backend→client unsubscription acknowledgment type.
	RespTypeUnsubscriptionAck = "unsubscription_ack"
)

// subscriptionAckPrefixScanLen is the number of bytes to scan for "_ack" in
// backend messages to skip JSON parsing for non-subscription-ack messages.
const subscriptionAckPrefixScanLen = 80

// Auth-specific error code constants.
const (
	AuthErrInvalidToken   = "invalid_token"
	AuthErrTokenExpired   = "token_expired"
	AuthErrTenantMismatch = "tenant_mismatch"
	AuthErrRateLimited    = "rate_limited"
	AuthErrNotAvailable   = "not_available"
)

// AuthErrorMessages provides human-readable messages for auth error codes.
var AuthErrorMessages = map[string]string{
	AuthErrInvalidToken:   "Token validation failed",
	AuthErrTokenExpired:   "Token has expired",
	AuthErrTenantMismatch: "Token tenant does not match connection tenant",
	AuthErrRateLimited:    "Auth refresh rate limit exceeded",
	AuthErrNotAvailable:   "Auth refresh is not available",
}

// AuthData is the payload for auth refresh messages.
// Client sends: {"type": "auth", "data": {"token": "eyJ..."}}
type AuthData struct {
	Token string `json:"token"`
}

// AuthAckResponse is sent to the client on successful auth refresh.
type AuthAckResponse struct {
	Type string      `json:"type"`
	Data AuthAckData `json:"data"`
}

// AuthAckData contains the auth refresh success payload.
type AuthAckData struct {
	Exp int64 `json:"exp"`
}

// AuthErrorResponse is sent to the client on failed auth refresh.
type AuthErrorResponse struct {
	Type string        `json:"type"`
	Data AuthErrorData `json:"data"`
}

// AuthErrorData contains the auth refresh error payload.
type AuthErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
