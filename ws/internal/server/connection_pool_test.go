package server

import (
	"testing"
)

// TestConnectionPool_GapChannelResetOnReuse verifies that a GapNotification enqueued
// during a client's session is NOT visible to the next client that reuses the same pool slot.
// gapChan is NOT nilled in Put() (to avoid a data race with Broadcast() reads); Get() always
// creates a fresh channel, so stale notifications from the previous lease cannot reach the new client.
func TestConnectionPool_GapChannelResetOnReuse(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(10, 512, 8)

	// Get a client and enqueue a stale gap notification.
	c := pool.Get()
	if c == nil {
		t.Fatal("expected non-nil client from pool")
	}
	if c.gapChan == nil {
		t.Fatal("expected non-nil gapChan after Get()")
	}

	c.gapChan <- GapNotification{
		Channel: "test.chan",
		FromSeq: 1,
		ToSeq:   1,
		LastPos: "(1)-100",
		Ts:      123456789,
	}

	// Return to pool. replayInProgress and replayLastAt are nilled; gapChan is NOT
	// nilled (by design — Get() creates a fresh channel, avoiding the data race).
	pool.Put(c)

	if c.replayInProgress != nil {
		t.Error("Put() must nil replayInProgress")
	}
	if c.replayLastAt != nil {
		t.Error("Put() must nil replayLastAt")
	}

	// Get a new client (may be the same underlying object from sync.Pool).
	// Get() must always create a fresh, empty gapChan regardless of old content.
	c2 := pool.Get()
	if c2 == nil {
		t.Fatal("expected non-nil client from second Get()")
	}
	if c2.gapChan == nil {
		t.Fatal("expected non-nil gapChan on reused client")
	}
	if len(c2.gapChan) != 0 {
		t.Fatalf("expected empty gapChan on reused client (Get() must create a fresh channel), got len=%d", len(c2.gapChan))
	}
	if c2.replayInProgress == nil {
		t.Fatal("expected non-nil replayInProgress on reused client")
	}
	if c2.replayLastAt == nil {
		t.Fatal("expected non-nil replayLastAt on reused client")
	}
}

// TestConnectionPool_Put_ClearsConnID verifies that Pool.Put clears connID so a recycled client
// cannot emit registry events for the previous connection's ID (§IX — stale identity on recycled
// pool object causes cross-connection registry pollution).
func TestConnectionPool_Put_ClearsConnID(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(10, 512, 8)
	c := pool.Get()
	if c == nil {
		t.Fatal("expected non-nil client")
	}

	// Simulate post-upgrade identity assignment.
	c.connID = "test-conn-id"
	c.apiKeyID = "test-api-key-id"
	c.userID = "test-user-id"
	c.tenantID = "test-tenant-id"

	pool.Put(c)

	// Retrieve the client (may be same object).
	c2 := pool.Get()
	if c2 == nil {
		t.Fatal("expected non-nil client from second Get()")
	}

	// All identity fields must be cleared by Put.
	if c2.connID != "" {
		t.Errorf("connID not cleared on Put: got %q", c2.connID)
	}
	if c2.apiKeyID != "" {
		t.Errorf("apiKeyID not cleared on Put: got %q", c2.apiKeyID)
	}
	if c2.userID != "" {
		t.Errorf("userID not cleared on Put: got %q", c2.userID)
	}
	if c2.tenantID != "" {
		t.Errorf("tenantID not cleared on Put: got %q", c2.tenantID)
	}
}
