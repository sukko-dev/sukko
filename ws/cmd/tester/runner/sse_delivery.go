package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sukko-dev/sukko/cmd/tester/sse"
)

// sseEventMsgID extracts the delivered payload's msg_id from a raw SSE `data:` line.
//
// The SSE transport delivers the FULL broadcast envelope as the event data:
//
//	{"type":"message","seq":1,...,"channel":"...","data":{"msg_id":"...","ts":...},"pos":"1-4"}
//
// so msg_id lives at .data.msg_id — NOT top-level. The WS delivery callbacks parse a top-level
// msg_id because the tester's WS client (cmd/tester/ws) has already unwrapped the envelope's
// inner .data into Message.Data; the SSE reader receives the raw envelope, so it MUST dig one
// level deeper. A top-level parse on an SSE event yields "" and every SSE delivery check times
// out despite correct end-to-end delivery. Returns "" when the payload is not a delivery
// envelope carrying a string msg_id.
func sseEventMsgID(eventData string) string {
	var env struct {
		Data struct {
			MsgID string `json:"msg_id"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(eventData), &env) != nil {
		return ""
	}
	return env.Data.MsgID
}

// readSSEUntilMsgID reads SSE events until one whose .data.msg_id equals wantMsgID arrives, or
// ctx's deadline elapses (ReadEvent returns the error). Returns the matching event so callers can
// assert its format. Filtering by the specific published msg_id keeps SSE delivery checks
// non-vacuous — unrelated traffic (the routing probe, a warmup, or an earlier suite's message)
// cannot satisfy the check.
func readSSEUntilMsgID(ctx context.Context, c *sse.Client, wantMsgID string) (*sse.Event, error) {
	for {
		event, err := c.ReadEvent(ctx)
		if err != nil {
			return nil, fmt.Errorf("sse read: %w", err)
		}
		if sseEventMsgID(event.Data) == wantMsgID {
			return event, nil
		}
	}
}
