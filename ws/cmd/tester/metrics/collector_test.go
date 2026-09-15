package metrics

import (
	"testing"
	"time"
)

func TestNewCollector(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	if c == nil {
		t.Fatal("expected non-nil collector")
	}
	snap := c.Snapshot()
	if snap.ConnectionsActive != 0 || snap.MessagesSent != 0 || snap.ErrorsTotal != 0 {
		t.Errorf("expected zero initial metrics, got %+v", snap)
	}
}

func TestCollector_AtomicCounters(t *testing.T) {
	t.Parallel()

	c := NewCollector()

	c.ConnectionsActive.Add(5)
	c.ConnectionsTotal.Add(10)
	c.ConnectionsFailed.Add(2)
	c.MessagesSent.Add(100)
	c.MessagesReceived.Add(95)
	c.MessagesDropped.Add(3)
	c.ErrorsTotal.Add(1)

	snap := c.Snapshot()
	if snap.ConnectionsActive != 5 {
		t.Errorf("ConnectionsActive = %d, want 5", snap.ConnectionsActive)
	}
	if snap.ConnectionsTotal != 10 {
		t.Errorf("ConnectionsTotal = %d, want 10", snap.ConnectionsTotal)
	}
	if snap.ConnectionsFailed != 2 {
		t.Errorf("ConnectionsFailed = %d, want 2", snap.ConnectionsFailed)
	}
	if snap.MessagesSent != 100 {
		t.Errorf("MessagesSent = %d, want 100", snap.MessagesSent)
	}
	if snap.MessagesReceived != 95 {
		t.Errorf("MessagesReceived = %d, want 95", snap.MessagesReceived)
	}
	if snap.MessagesDropped != 3 {
		t.Errorf("MessagesDropped = %d, want 3", snap.MessagesDropped)
	}
	if snap.ErrorsTotal != 1 {
		t.Errorf("ErrorsTotal = %d, want 1", snap.ErrorsTotal)
	}
}

func TestCollector_AuthCounters(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.AuthRefreshTotal.Add(10)
	c.AuthRefreshFailed.Add(2)
	c.AuthErrors.Add(3)

	snap := c.Snapshot()
	if snap.AuthRefreshTotal != 10 {
		t.Errorf("AuthRefreshTotal = %d, want 10", snap.AuthRefreshTotal)
	}
	if snap.AuthRefreshFailed != 2 {
		t.Errorf("AuthRefreshFailed = %d, want 2", snap.AuthRefreshFailed)
	}
	if snap.AuthErrors != 3 {
		t.Errorf("AuthErrors = %d, want 3", snap.AuthErrors)
	}

	c.Reset()
	snap = c.Snapshot()
	if snap.AuthRefreshTotal != 0 || snap.AuthRefreshFailed != 0 || snap.AuthErrors != 0 {
		t.Error("auth counters not zeroed after reset")
	}
}

func TestCollector_Latency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.Latency.Record(10 * time.Millisecond)
	c.Latency.Record(20 * time.Millisecond)

	snap := c.Snapshot()
	if snap.Latency.Count != 2 {
		t.Errorf("expected latency count=2, got %d", snap.Latency.Count)
	}
}

func TestCollector_Elapsed(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	// Snapshot should have a non-empty elapsed string
	time.Sleep(5 * time.Millisecond)
	snap := c.Snapshot()
	if snap.Elapsed == "" {
		t.Error("expected non-empty elapsed")
	}
}

func TestCollector_Reset(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.ConnectionsActive.Add(5)
	c.MessagesSent.Add(100)
	c.Latency.Record(10 * time.Millisecond)

	c.Reset()
	snap := c.Snapshot()
	if snap.ConnectionsActive != 0 {
		t.Errorf("expected ConnectionsActive=0 after reset, got %d", snap.ConnectionsActive)
	}
	if snap.MessagesSent != 0 {
		t.Errorf("expected MessagesSent=0 after reset, got %d", snap.MessagesSent)
	}
	if snap.Latency.Count != 0 {
		t.Errorf("expected latency count=0 after reset, got %d", snap.Latency.Count)
	}
}

func TestCollector_SnapshotTimestamp(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	before := time.Now()
	snap := c.Snapshot()
	after := time.Now()

	if snap.Timestamp.Before(before) || snap.Timestamp.After(after) {
		t.Errorf("timestamp %v not between %v and %v", snap.Timestamp, before, after)
	}
}

func TestCollector_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	done := make(chan struct{})

	go func() {
		for range 1000 {
			c.ConnectionsActive.Add(1)
			c.MessagesSent.Add(1)
			// Auth mode fields
			c.ConnectionsAPIKey.Add(1)
			c.ConnectionsJWT.Add(1)
			c.ConnectionsUpgrade.Add(1)
			c.AuthUpgradeTotal.Add(1)
			c.AuthUpgradeFailed.Add(1)
		}
		close(done)
	}()

	// Read snapshots concurrently
	for range 100 {
		_ = c.Snapshot()
	}

	<-done
	snap := c.Snapshot()
	if snap.ConnectionsActive != 1000 {
		t.Errorf("expected ConnectionsActive=1000, got %d", snap.ConnectionsActive)
	}
	if snap.ConnectionsAPIKey != 1000 {
		t.Errorf("expected ConnectionsAPIKey=1000, got %d", snap.ConnectionsAPIKey)
	}
	if snap.ConnectionsJWT != 1000 {
		t.Errorf("expected ConnectionsJWT=1000, got %d", snap.ConnectionsJWT)
	}
	if snap.ConnectionsUpgrade != 1000 {
		t.Errorf("expected ConnectionsUpgrade=1000, got %d", snap.ConnectionsUpgrade)
	}
	if snap.AuthUpgradeTotal != 1000 {
		t.Errorf("expected AuthUpgradeTotal=1000, got %d", snap.AuthUpgradeTotal)
	}
	if snap.AuthUpgradeFailed != 1000 {
		t.Errorf("expected AuthUpgradeFailed=1000, got %d", snap.AuthUpgradeFailed)
	}
}

func TestCollector_SSEAndRESTCounters(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.SSEMessagesReceived.Add(15)
	c.RESTPublishSuccess.Add(8)
	c.RESTPublishErrors.Add(2)

	snap := c.Snapshot()
	if snap.SSEMessagesReceived != 15 {
		t.Errorf("SSEMessagesReceived = %d, want 15", snap.SSEMessagesReceived)
	}
	if snap.RESTPublishSuccess != 8 {
		t.Errorf("RESTPublishSuccess = %d, want 8", snap.RESTPublishSuccess)
	}
	if snap.RESTPublishErrors != 2 {
		t.Errorf("RESTPublishErrors = %d, want 2", snap.RESTPublishErrors)
	}

	c.Reset()
	snap = c.Snapshot()
	if snap.SSEMessagesReceived != 0 || snap.RESTPublishSuccess != 0 || snap.RESTPublishErrors != 0 {
		t.Error("SSE/REST counters not zeroed after reset")
	}
}

func TestCollector_ChannelModeCounters(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.PublicSent.Add(10)
	c.PublicReceived.Add(20)
	c.UserScopedSent.Add(30)
	c.UserScopedReceived.Add(40)
	c.GroupScopedSent.Add(50)
	c.GroupScopedReceived.Add(60)
	c.Misrouted.Add(1)

	snap := c.Snapshot()
	if snap.PublicSent != 10 {
		t.Errorf("PublicSent = %d, want 10", snap.PublicSent)
	}
	if snap.PublicReceived != 20 {
		t.Errorf("PublicReceived = %d, want 20", snap.PublicReceived)
	}
	if snap.UserScopedSent != 30 {
		t.Errorf("UserScopedSent = %d, want 30", snap.UserScopedSent)
	}
	if snap.UserScopedReceived != 40 {
		t.Errorf("UserScopedReceived = %d, want 40", snap.UserScopedReceived)
	}
	if snap.GroupScopedSent != 50 {
		t.Errorf("GroupScopedSent = %d, want 50", snap.GroupScopedSent)
	}
	if snap.GroupScopedReceived != 60 {
		t.Errorf("GroupScopedReceived = %d, want 60", snap.GroupScopedReceived)
	}
	if snap.Misrouted != 1 {
		t.Errorf("Misrouted = %d, want 1", snap.Misrouted)
	}

	c.Reset()
	snap = c.Snapshot()
	if snap.PublicSent != 0 || snap.PublicReceived != 0 ||
		snap.UserScopedSent != 0 || snap.UserScopedReceived != 0 ||
		snap.GroupScopedSent != 0 || snap.GroupScopedReceived != 0 ||
		snap.Misrouted != 0 {
		t.Error("channel-mode counters not zeroed after reset")
	}
}

// TestCollector_APIKeyAuthCounters verifies the 5 new auth-mode metrics fields
// introduced with connection type tracking + upgrade flow counters.
func TestCollector_APIKeyAuthCounters(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.ConnectionsAPIKey.Add(7)
	c.ConnectionsJWT.Add(11)
	c.ConnectionsUpgrade.Add(3)
	c.AuthUpgradeTotal.Add(20)
	c.AuthUpgradeFailed.Add(2)

	snap := c.Snapshot()
	if snap.ConnectionsAPIKey != 7 {
		t.Errorf("ConnectionsAPIKey = %d, want 7", snap.ConnectionsAPIKey)
	}
	if snap.ConnectionsJWT != 11 {
		t.Errorf("ConnectionsJWT = %d, want 11", snap.ConnectionsJWT)
	}
	if snap.ConnectionsUpgrade != 3 {
		t.Errorf("ConnectionsUpgrade = %d, want 3", snap.ConnectionsUpgrade)
	}
	if snap.AuthUpgradeTotal != 20 {
		t.Errorf("AuthUpgradeTotal = %d, want 20", snap.AuthUpgradeTotal)
	}
	if snap.AuthUpgradeFailed != 2 {
		t.Errorf("AuthUpgradeFailed = %d, want 2", snap.AuthUpgradeFailed)
	}

	c.Reset()
	snap = c.Snapshot()
	if snap.ConnectionsAPIKey != 0 || snap.ConnectionsJWT != 0 ||
		snap.ConnectionsUpgrade != 0 || snap.AuthUpgradeTotal != 0 ||
		snap.AuthUpgradeFailed != 0 {
		t.Error("auth-mode counters not zeroed after reset")
	}
}
