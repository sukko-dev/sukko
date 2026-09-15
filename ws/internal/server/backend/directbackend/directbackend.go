// Package directbackend provides the zero-dependency implementation of the
// MessageBackend interface. It routes client-published messages directly to
// the broadcast bus with no persistence and no replay capability.
package directbackend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/backend"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/server/metrics"
)

// backendName identifies this backend in metrics labels and logging.
const backendName = "direct"

// DirectBackend routes client-published messages directly to the broadcast bus
// with zero external dependencies. No persistence, no replay.
type DirectBackend struct {
	bus    broadcast.Bus
	logger zerolog.Logger
}

// New creates a new direct backend with the given broadcast bus.
// Returns an error if bus is nil.
func New(bus broadcast.Bus, logger zerolog.Logger) (*DirectBackend, error) {
	if bus == nil {
		return nil, errors.New("direct backend: broadcast bus is required")
	}

	return &DirectBackend{
		bus:    bus,
		logger: logger.With().Str("component", "direct-backend").Logger(),
	}, nil
}

// Start is a no-op for direct mode (no consumption loop needed).
func (db *DirectBackend) Start(_ context.Context) error {
	db.logger.Info().Msg("Direct backend started (no external dependencies)")
	metrics.SetBackendHealthy(backendName, true)
	return nil
}

// Publish sends a message directly to the broadcast bus.
// tenantID must be non-empty; the bus rejects messages with an empty tenant ID.
// The mid is minted here (UUIDv7 — time-ordered; no durable coordinates exist
// on the direct backend) and carried on the bus message so the live and
// history copies share the identity returned in the publish ack.
func (db *DirectBackend) Publish(_ context.Context, _ int64, tenantID, channel string, data []byte) (string, error) {
	if channel == "" {
		return "", fmt.Errorf("%w: channel is required", backend.ErrPublishFailed)
	}
	if tenantID == "" {
		return "", fmt.Errorf("%w: tenant ID is required", backend.ErrPublishFailed)
	}
	mid := mintMid()
	start := time.Now()
	db.bus.Publish(&broadcast.Message{
		Subject:  channel,
		Payload:  data,
		TenantID: tenantID,
		Mid:      mid,
	})
	metrics.RecordBackendPublishLatency(backendName, time.Since(start).Seconds())
	metrics.RecordBackendPublish(backendName)
	return mid, nil
}

// mintMid returns a new UUIDv7 message identity — time-ordered, which keeps
// mids roughly sortable for debugging. uuid.Must is safe here: on Go 1.24+
// crypto/rand.Read cannot fail, so NewV7's only error path is unreachable
// (and a fallback branch would be dead code).
func mintMid() string {
	return uuid.Must(uuid.NewV7()).String()
}

// Replay returns nil, nil — direct mode has no persistence and cannot replay.
func (db *DirectBackend) Replay(_ context.Context, _ backend.ReplayRequest) ([]backend.ReplayMessage, error) {
	metrics.RecordBackendReplayRequest(backendName)
	return nil, nil
}

// IsHealthy always returns true — direct mode has no external dependencies.
func (db *DirectBackend) IsHealthy() bool {
	return true
}

// Ready is always true for direct mode: there is no routing snapshot to wait for (#179 P3).
func (db *DirectBackend) Ready() bool {
	return true
}

// Shutdown is a no-op for direct mode.
func (db *DirectBackend) Shutdown(_ context.Context) error {
	metrics.SetBackendHealthy(backendName, false)
	db.logger.Info().Msg("Direct backend shut down")
	return nil
}

// ChannelTopic returns ok=false — direct mode has no topic mapping.
func (db *DirectBackend) ChannelTopic(_ string) (string, bool) { return "", false }

// Compile-time interface check.
var _ backend.MessageBackend = (*DirectBackend)(nil)
