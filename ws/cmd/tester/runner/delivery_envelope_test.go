package runner

import "testing"

// acceptsDeliveryEnvelope decides which WS message types count as a delivered payload for a
// delivery check. It MUST accept both "message" (the server's BroadcastEnvelope) and "publish"
// (peer-forwarded) — accepting only one silently drops delivered messages and every delivery
// check reports "missing" despite successful delivery (the exact rest-publish/pubsub bug).
func TestAcceptsDeliveryEnvelope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		msgType string
		want    bool
	}{
		{name: "broadcast envelope", msgType: "message", want: true},
		{name: "peer-forwarded publish", msgType: "publish", want: true},
		{name: "control frame", msgType: "subscribed", want: false},
		{name: "error frame", msgType: "error", want: false},
		{name: "empty", msgType: "", want: false},
		{name: "unknown", msgType: "other", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := acceptsDeliveryEnvelope(tt.msgType); got != tt.want {
				t.Errorf("acceptsDeliveryEnvelope(%q) = %v, want %v", tt.msgType, got, tt.want)
			}
		})
	}
}
