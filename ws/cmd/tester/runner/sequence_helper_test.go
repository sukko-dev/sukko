package runner

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/sukko-dev/sukko/cmd/tester/publisher"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

func TestChannelTrackers_BasicTrackAndAggregate(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// Simulate receiving 3 sequential messages on one channel
	for seq := int64(1); seq <= 3; seq++ {
		ct.track(makeTestMessage("ch1", seq))
	}

	stats := ct.aggregateStats()
	if stats.Received != 3 {
		t.Errorf("received = %d, want 3", stats.Received)
	}
	if stats.Gaps != 0 {
		t.Errorf("gaps = %d, want 0", stats.Gaps)
	}
	if stats.Duplicates != 0 {
		t.Errorf("duplicates = %d, want 0", stats.Duplicates)
	}
}

func TestChannelTrackers_DetectsGaps(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// Send seq 1, then skip to 5 — 3 gaps (2, 3, 4)
	ct.track(makeTestMessage("ch1", 1))
	ct.track(makeTestMessage("ch1", 5))

	stats := ct.aggregateStats()
	if stats.Gaps != 3 {
		t.Errorf("gaps = %d, want 3", stats.Gaps)
	}
}

func TestChannelTrackers_DetectsDuplicates(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	ct.track(makeTestMessage("ch1", 1))
	ct.track(makeTestMessage("ch1", 2))
	ct.track(makeTestMessage("ch1", 1)) // duplicate

	stats := ct.aggregateStats()
	if stats.Duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", stats.Duplicates)
	}
}

func TestChannelTrackers_MultiChannel(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// Channel A: seq 1, 2, 3 (clean)
	for seq := int64(1); seq <= 3; seq++ {
		ct.track(makeTestMessage("ch-a", seq))
	}

	// Channel B: seq 1, 5 (3 gaps)
	ct.track(makeTestMessage("ch-b", 1))
	ct.track(makeTestMessage("ch-b", 5))

	stats := ct.aggregateStats()
	if stats.Received != 5 {
		t.Errorf("received = %d, want 5", stats.Received)
	}
	if stats.Gaps != 3 {
		t.Errorf("gaps = %d, want 3 (from ch-b)", stats.Gaps)
	}
}

func TestChannelTrackers_SkipsNonMessageType(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// auth_ack should be skipped
	ct.track(testerws.Message{Type: "auth_ack", Channel: "ch1"})

	// subscribe_ack should be skipped
	ct.track(testerws.Message{Type: "subscribe_ack", Channel: "ch1"})

	stats := ct.aggregateStats()
	if stats.Received != 0 {
		t.Errorf("received = %d, want 0 (non-message types should be skipped)", stats.Received)
	}
}

func TestChannelTrackers_SkipsInvalidJSON(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// Invalid JSON in data
	ct.track(testerws.Message{
		Type:    "message",
		Channel: "ch1",
		Data:    json.RawMessage(`{invalid json`),
	})

	stats := ct.aggregateStats()
	if stats.Received != 0 {
		t.Errorf("received = %d, want 0 (invalid JSON should be skipped)", stats.Received)
	}
}

func TestChannelTrackers_SkipsZeroSequence(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	// Sequence 0 should be skipped
	ct.track(makeTestMessage("ch1", 0))

	// Negative sequence should be skipped
	ct.track(makeTestMessage("ch1", -1))

	stats := ct.aggregateStats()
	if stats.Received != 0 {
		t.Errorf("received = %d, want 0 (seq <= 0 should be skipped)", stats.Received)
	}
}

func TestChannelTrackers_ConcurrentTrackAndAggregate(t *testing.T) {
	t.Parallel()
	ct := newChannelTrackers()

	var wg sync.WaitGroup
	// 10 goroutines each tracking 100 messages on different channels
	for i := range 10 {
		ch := i
		wg.Go(func() {
			channel := "ch-" + string(rune('a'+ch))
			for seq := int64(1); seq <= 100; seq++ {
				ct.track(makeTestMessage(channel, seq))
			}
		})
	}
	wg.Wait()

	stats := ct.aggregateStats()
	if stats.Received != 1000 {
		t.Errorf("received = %d, want 1000", stats.Received)
	}
	if stats.Gaps != 0 {
		t.Errorf("gaps = %d, want 0 (sequential per channel)", stats.Gaps)
	}
	if stats.Duplicates != 0 {
		t.Errorf("duplicates = %d, want 0", stats.Duplicates)
	}
}

// makeTestMessage creates a testerws.Message with a TestMessage payload.
func makeTestMessage(channel string, seq int64) testerws.Message {
	data, _ := json.Marshal(publisher.TestMessage{
		Sequence:  seq,
		Timestamp: 1000000 + seq,
		Payload:   "test",
	})
	return testerws.Message{
		Type:    "message",
		Channel: channel,
		Data:    data,
	}
}
