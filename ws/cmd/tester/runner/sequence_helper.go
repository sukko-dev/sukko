package runner

import (
	"encoding/json"
	"sync"

	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// channelTrackers manages per-channel SequenceTrackers for multi-channel tests.
// Each channel gets its own tracker instance (lazy-initialized on first message).
// Thread-safe: mutex guards the map; each tracker has its own internal mutex.
type channelTrackers struct {
	mu       sync.RWMutex
	trackers map[string]*publisher.SequenceTracker
}

func newChannelTrackers() *channelTrackers {
	return &channelTrackers{trackers: make(map[string]*publisher.SequenceTracker)}
}

// track parses a received message and routes it to the appropriate per-channel tracker.
// Skips non-"message" type messages (control messages like auth_ack).
// Skips messages that don't contain a valid TestMessage with Sequence > 0.
func (ct *channelTrackers) track(msg testerws.Message) {
	if msg.Type != "message" {
		return
	}

	var testMsg publisher.TestMessage
	if err := json.Unmarshal(msg.Data, &testMsg); err != nil || testMsg.Sequence <= 0 {
		return // non-tester messages or control frames — silently skip
	}

	ct.mu.Lock()
	tracker, ok := ct.trackers[msg.Channel]
	if !ok {
		tracker = &publisher.SequenceTracker{}
		ct.trackers[msg.Channel] = tracker
	}
	ct.mu.Unlock()

	tracker.Track(testMsg.Sequence)
}

// aggregateStats returns the sum of Gaps and Duplicates across all channel trackers.
func (ct *channelTrackers) aggregateStats() publisher.SequenceStats {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	var total publisher.SequenceStats
	for _, t := range ct.trackers {
		s := t.Stats()
		total.Received += s.Received
		total.Gaps += s.Gaps
		total.Duplicates += s.Duplicates
		if s.HighestSeen > total.HighestSeen {
			total.HighestSeen = s.HighestSeen
		}
	}
	return total
}
