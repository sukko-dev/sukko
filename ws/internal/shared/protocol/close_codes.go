package protocol

// Full close code registry
//
// WebSocket close codes in the 4000-4999 range are application-defined (RFC 6455 §7.4.2).
// Sukko allocates from the bottom of the range. Add new codes sequentially below.
//
// 4000 — force_disconnect: operator-initiated force-disconnect via Connections API.
const (
	// CloseCodeForceDisconnect is the WebSocket close code sent when an operator
	// force-disconnects a client via the Connections Management API (DELETE /connections).
	// All call sites MUST use this constant — do NOT use the literal 4000.
	CloseCodeForceDisconnect = 4000

	// CloseReasonForceDisconnect is the close reason string paired with CloseCodeForceDisconnect.
	// Clients SHOULD display or log this reason to help users understand the disconnect.
	CloseReasonForceDisconnect = "force_disconnect"
)
