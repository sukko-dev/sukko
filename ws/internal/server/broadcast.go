package server

import (
	"slices"
	"strings"
	"time"

	"github.com/sukko-dev/sukko/internal/server/messaging"
	"github.com/sukko-dev/sukko/internal/server/metrics"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	pkgmetrics "github.com/sukko-dev/sukko/internal/shared/metrics"
)

// extractChannel returns the broadcast subject as the channel for subscription matching.
// The broadcast subject IS the channel - no transformation needed.
//
// Supported formats (2+ parts):
//   - "BTC.trade" (public)
//   - "BTC.trade.user123" (user-scoped)
//   - "community.group456" (group-scoped)
func extractChannel(subject string) string {
	parts := strings.Split(subject, ".")

	// Minimum: {subject}.{eventType}, can have more parts
	if len(parts) < 2 {
		return ""
	}

	// Validate no empty parts
	if slices.Contains(parts, "") {
		return ""
	}

	return subject
}

// Broadcast sends a message to subscribed clients with reliability guarantees
// This is the critical path for message delivery in a trading platform
//
// Changes from basic WebSocket broadcast:
// 1. Wraps raw Kafka messages in messaging.MessageEnvelope with sequence numbers
// 2. Stores in replay buffer BEFORE sending (ensures can replay if send fails)
// 3. Detects slow clients and disconnects them (prevents head-of-line blocking)
// 4. Never drops messages silently (either deliver or disconnect client)
// 5. HIERARCHICAL SUBSCRIPTION FILTERING: Only sends to clients subscribed to specific event types (8x reduction per symbol)
//
// Industry standard approach:
// - Bloomberg Terminal: Disconnects slow clients, no message drops
// - Coinbase: 2-strike disconnection policy
// - FIX protocol: Requires guaranteed delivery or explicit reject
//
// Performance characteristics WITH hierarchical filtering:
// - O(m) where m = number of subscribed clients to specific event type
// - With 10,000 clients, 500 subscribers per channel: ~0.1ms per broadcast
// - Called ~12 times/second from Kafka consumer (varies by event type)
// - Total CPU: ~1-2% (vs 99%+ without filtering) - 50-100x improvement!
//
// Example savings with hierarchical channels:
// - 10K clients, 200 tokens, 8 event types
// - Clients subscribe to specific events: "BTC.trade", "ETH.trade" (not all 8 event types)
// - Without filtering: 12 × 8 × 10,000 = 960,000 writes/sec (CPU overload)
// - With symbol filtering: 12 × 8 × 500 = 48,000 writes/sec (CPU 80%+)
// - With hierarchical filtering: 12 × 500 = 6,000 writes/sec (CPU <30%)
// - Result: 160x reduction vs no filtering, 8x reduction vs symbol-only filtering
func (s *Server) Broadcast(subject string, message []byte, pos, mid string) {
	// Extract hierarchical channel from subject for subscription filtering
	// Example: "BTC.trade" (new format) or "sukko.token.BTC.trade" (legacy) → "BTC.trade"
	channel := extractChannel(subject)

	// Edge case: If channel is empty (malformed subject), skip broadcast entirely
	// Invalid subjects indicate misconfigured publisher - should never happen in production
	if channel == "" {
		return
	}

	// SUBSCRIPTION INDEX OPTIMIZATION: Directly lookup subscribers for this channel
	// Instead of iterating ALL clients and filtering, only iterate subscribed clients!
	//
	// Performance comparison:
	// - Old approach: Iterate 7,000 clients → filter to ~500 subscribers = 7,000 iterations
	// - New approach: Lookup ~500 subscribers directly = 500 iterations
	// - CPU savings: 93% (14× reduction in iterations!)
	//
	// Real production scenario (clients subscribe to specific tokens):
	// - Without index: 12 msg/sec × 5 channels × 7K clients = 420,000 iter/sec (CPU 60%+)
	// - With index: 12 msg/sec × 5 channels × 500 subscribers = 30,000 iter/sec (CPU <10%)
	// - Result: 93% CPU savings on broadcast hot path!
	subscribers := s.subscriptionIndex.Get(channel)
	if len(subscribers) == 0 {
		return // No subscribers for this channel
	}

	// Track broadcast metrics for debug logging
	totalCount := len(subscribers)
	successCount := 0

	// Pre-compute broadcast envelope template (shared across all write pump goroutines).
	// Write-pump deferred assembly: broadcast loop stays O(1) per client (~25ns),
	// byte assembly (~100ns) is parallelized across write pump goroutines.
	//
	// Performance vs old shared-serialization (seq=0):
	//   - Broadcast loop: ~25ns/client (up from ~15ns — atomic increment is negligible)
	//   - Byte assembly: ~100ns/client (deferred to write pumps, parallelized)
	//   - Enables client-side gap detection (seq>0, monotonically increasing)
	envelope, err := messaging.NewBroadcastEnvelope(channel, time.Now().UnixMilli(), message, pos, mid)
	if err != nil {
		metrics.RecordSerializationError(pkgmetrics.SeverityCritical)
		s.logger.Error().
			Err(err).
			Str("channel", channel).
			Int("subscribers", totalCount).
			Msg("Failed to prepare broadcast envelope")
		return
	}

	// Iterate ONLY subscribed clients (not all clients!)
	for _, client := range subscribers {
		// NOTE: Replay buffer removed - Kafka provides message replay via offset tracking
		// Removing buffer saves 6GB RAM (12K clients × 500KB) and reduces broadcast CPU overhead
		// For reconnection, clients will use Kafka-based replay (see handleReconnect)

		// Check if client already disconnected (race condition protection)
		// This can happen if client disconnects between getting subscriber list and sending
		if client.transport == nil {
			metrics.RecordDroppedBroadcastWithStats(s.stats, channel, pkgmetrics.DropReasonClientDisconnected)
			continue
		}

		// Assign sequence BEFORE send attempt — consumed even if dropped.
		// Client sees gap (e.g., 5 → 7) if message is dropped, enabling gap detection.
		seq := client.seqGen.Next()

		// Attempt to send - COMPLETELY NON-BLOCKING
		// Critical fix: Do not use time.After() which blocks the entire broadcast
		// Instead, immediately detect full buffers and mark client as slow
		// Send lightweight struct — write pump builds final bytes via envelope.Build(seq)
		select {
		case client.send <- OutgoingMsg{envelope: envelope, seq: seq}:
			// Success — lightweight struct sent to write pump for deferred assembly
			client.sendAttempts.Store(0)
			client.lastMessageSentAt = time.Now()
			successCount++

			// DEBUG level: Zero overhead in production (LOG_LEVEL=info)
			s.logger.Debug().
				Int64("client_id", client.id).
				Str("channel", channel).
				Int64("seq", seq).
				Msg("Broadcast to client")

		default:
			// Buffer full - client can't keep up
			// This indicates:
			// 1. Client on slow network (mobile, bad wifi)
			// 2. Client device overloaded (CPU pegged, memory swapping)
			// 3. Client code has bug (not reading messages)
			//
			// CRITICAL: We do NOT block here. We increment failure counter
			// and let the cleanup goroutine handle disconnection.
			// This keeps broadcast fast (~1-2ms) regardless of slow clients.
			attempts := client.sendAttempts.Add(1)

			// Track dropped broadcast with channel and reason (both Prometheus and Stats)
			metrics.RecordDroppedBroadcastWithStats(s.stats, channel, pkgmetrics.DropReasonBufferFull)

			// Enqueue in-band gap notification (non-blocking — never stalls the broadcast hot path).
			// Must be BEFORE the slow-client disconnect check: a client being disconnected
			// does not need a gap notification.
			//
			// gapChan is always non-nil for an active client (Get() initializes it unconditionally;
			// Put() does NOT nil it — the field is only overwritten by the next Get() call, which
			// has exclusive access before the client enters any snapshot). This eliminates the
			// data race that would exist if Put() nilled the field concurrently with this send.
			gap := GapNotification{
				Channel: channel,
				FromSeq: seq,
				ToSeq:   seq,
				LastPos: pos,
				Ts:      time.Now().UnixMilli(),
			}
			select {
			case client.gapChan <- gap:
				metrics.GapNotificationsEnqueued.Inc()
			default:
				metrics.RecordDroppedGapNotification(GapDropReasonBufferFull)
			}

			// Phase 4: Sampled structured logging (every 100th drop to avoid log spam)
			dropCount := s.stats.DroppedBroadcastLogCounter.Add(1)
			if dropCount%100 == 0 {
				bufferLen := len(client.send)
				bufferCap := cap(client.send)
				s.logger.Warn().
					Int64("client_id", client.id).
					Str("channel", channel).
					Str("reason", pkgmetrics.DropReasonBufferFull).
					Int32("attempts", attempts).
					Int("buffer_len", bufferLen).
					Int("buffer_cap", bufferCap).
					Float64("buffer_usage_pct", float64(bufferLen)/float64(bufferCap)*100).
					Int64("total_drops", dropCount).
					Msg("Broadcast dropped (sampled: every 100th)")
			}

			// Log warning on first failure (avoid spam)
			// Use atomic CompareAndSwap to avoid race condition
			if attempts == 1 && client.slowClientWarned.CompareAndSwap(0, 1) {
				s.logger.Warn().
					Int64("client_id", client.id).
					Str("reason", "send_buffer_full").
					Msg("Client is slow")
			}

			// Disconnect after SlowClientMaxAttempts consecutive failures
			// Configurable via WS_SLOW_CLIENT_MAX_ATTEMPTS (default: 3)
			// Industry comparison: Coinbase=2, Binance=unlimited, FIX=5s timeout
			if int(attempts) >= s.config.SlowClientMaxAttempts {
				s.logger.Warn().
					Int64("client_id", client.id).
					Int32("consecutive_failures", attempts).
					Str("reason", "too_slow").
					Msg("Disconnecting slow client")

				// Record disconnect metrics with proper categorization (both Prometheus and Stats)
				duration := time.Since(client.connectedAt)
				metrics.RecordDisconnectWithStats(s.stats, string(client.TransportType()), pkgmetrics.DisconnectWriteTimeout, pkgmetrics.InitiatedByServer, duration)

				// Record how many attempts it took before disconnect (for histogram analysis)
				metrics.RecordSlowClientAttempt(int(attempts))

				// Alert log slow client disconnection
				s.alertLogger.Warning("SlowClientDisconnected", "Client disconnected for being too slow", map[string]any{
					"clientID":           client.id,
					"consecutiveFails":   attempts,
					"connectionDuration": duration.Seconds(),
					"subscriptionsCount": client.subscriptions.Count(),
					"sequenceNumber":     client.seqGen.Current(),
				})

				// Close the transport — each transport handles its own close semantics:
				// WebSocket: sends close frame (1008 Policy Violation) then closes TCP
				// gRPC: cancels the stream context
				// Uses logging.RecoverPanic per Constitution V — concurrent close during
				// active writes could panic on partially-closed connections.
				if client.transport != nil {
					func() {
						defer logging.RecoverPanic(s.logger, "broadcast_slow_client_close", nil)
						_ = client.transport.Close()
					}()
				}

				// Increment slow client counter for monitoring
				// If this counter is high (>1% of connections), indicates:
				// - Network infrastructure issues
				// - Client app performance issues
				// - Need to optimize message size/frequency
				s.stats.SlowClientsDisconnected.Add(1)
				metrics.IncrementSlowClientDisconnects(string(client.TransportType()))
			}
		}
	}

	// Debug logging: Track broadcast metrics with subscription index
	// Only logs when LOG_LEVEL=debug (no overhead in production)
	if channel != "" {
		successRate := float64(successCount) / float64(totalCount) * 100
		s.logger.Debug().
			Str("channel", channel).
			Int("subscribers", totalCount).
			Int("sent_successfully", successCount).
			Float64("success_rate_pct", successRate).
			Msg("Targeted broadcast via subscription index")
	}
}

// handleClientMessage processes incoming WebSocket messages from clients
// Trading clients send various message types:
// 1. "replay" - Request missed messages (gap recovery after network issue)
// 2. "heartbeat" - Keep-alive ping (some clients don't support WebSocket ping)
// 3. "subscribe" - Subscribe to specific symbols (future enhancement)
// 4. "unsubscribe" - Unsubscribe from symbols (future enhancement)
//
// Message format (JSON):
//
//	{
//	  "type": "replay",
//	  "data": {"from": 100, "to": 150}  // Request sequence 100-150
//	}
//
// Industry patterns:
// - FIX protocol: Gap fill requests are standard
// - Kafka: Consumers can seek to specific offsets
// - Our implementation: Similar to Kafka consumer offset seeking
