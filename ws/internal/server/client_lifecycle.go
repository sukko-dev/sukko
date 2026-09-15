package server

import (
	"time"

	"github.com/sukko-dev/sukko/internal/server/metrics"
)

// Client connection lifecycle management
func (s *Server) disconnectClient(c *Client, reason, initiatedBy string) {
	// Calculate connection duration for metrics
	duration := time.Since(c.connectedAt)

	// Record disconnect metrics (both Prometheus and Stats)
	metrics.RecordDisconnectWithStats(s.stats, string(c.TransportType()), reason, initiatedBy, duration)

	// Log disconnect with enhanced context (Phase 4: Structured logging)
	bufferLen := len(c.send)
	bufferCap := cap(c.send)
	bufferUsagePercent := float64(bufferLen) / float64(bufferCap) * 100

	s.logger.Info().
		Int64("client_id", c.id).
		Str("reason", reason).
		Str("initiated_by", initiatedBy).
		Dur("connection_duration", duration).
		Int("subscriptions_count", c.subscriptions.Count()).
		Int64("sequence_number", c.seqGen.Current()).
		Int("send_buffer_len", bufferLen).
		Int("send_buffer_cap", bufferCap).
		Float64("send_buffer_usage_pct", bufferUsagePercent).
		Int32("send_attempts", c.sendAttempts.Load()).
		Time("connected_at", c.connectedAt).
		Msg("Client disconnected")

	// Use sync.Once to ensure transport is only closed once
	// Prevents race condition with multiple goroutines trying to close
	c.closeOnce.Do(func() {
		if c.transport != nil {
			_ = c.transport.Close()
		}
	})

	// Push registry disconnect event (non-blocking, before client is returned to pool).
	if s.config != nil && s.config.ConnectionsRegistryEnabled && s.registryWriter != nil && c.connID != "" {
		s.registryWriter.PushDisconnect(c.connID, c.tenantID, c.apiKeyID)
	}

	// Notify shard to release per-tenant broadcast channel if this was the last client
	// for this tenant on this shard. Guard: tenantID empty in direct/local mode.
	if s.tenantHooks != nil && c.tenantID != "" {
		s.tenantHooks.OnTenantClientDisconnect(c.tenantID)
	}

	// Cleanup resources
	s.clients.Delete(c)
	s.stats.CurrentConnections.Add(-1)

	// Remove client from subscription index (prevent memory leak)
	s.subscriptionIndex.RemoveMultiple(c.subscriptions.List(), c)

	// Cancel history delivery context and wait for in-flight delivery goroutines.
	// Must complete before Put() so the pool allocator sees no lingering goroutines.
	if c.clientCancel != nil {
		c.clientCancel()
	}
	if c.clientWg != nil {
		c.clientWg.Wait()
	}
	// Close the send channel so the WriteLoop (tracked by s.wg, not c.clientWg) exits
	// before the client is returned to the pool. Without this, the old WriteLoop goroutine
	// can still be reading c.send after Get() assigns the same struct to a new connection.
	c.closeSend()

	// Return client to pool for reuse
	s.connections.Put(c)
	<-s.connectionsSem // Release connection slot

	// Clean up rate limiter state
	s.rateLimiter.RemoveClient(c.id)
}
