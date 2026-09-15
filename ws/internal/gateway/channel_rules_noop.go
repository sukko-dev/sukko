package gateway

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// NoopChannelRulesProvider provides fallback when database is unavailable.
// Used for graceful degradation — tenant-signed JWTs only.
type NoopChannelRulesProvider struct {
	logger zerolog.Logger
}

// NewNoopChannelRulesProvider creates a noop provider that always returns not-found errors.
// This enables graceful degradation when the database is unavailable.
func NewNoopChannelRulesProvider(logger zerolog.Logger) *NoopChannelRulesProvider {
	logger.Warn().Msg("Using NoopChannelRulesProvider - per-tenant channel rules disabled")
	return &NoopChannelRulesProvider{logger: logger}
}

// GetChannelRules always returns ErrChannelRulesNotFound.
// This forces fallback to default/global channel rules.
func (n *NoopChannelRulesProvider) GetChannelRules(_ context.Context, tenantID string) (*types.ChannelRules, error) {
	n.logger.Debug().
		Str(logging.LogKeyTenantSlug, tenantID).
		Msg("NoopChannelRulesProvider: channel rules lookup returning not found")
	return nil, types.ErrChannelRulesNotFound
}

// SnapshotReceived always reports true: the noop provider's answer is final
// (deny-all via not-found), never "unknown" — reporting false would hold
// readiness degraded forever.
func (n *NoopChannelRulesProvider) SnapshotReceived() bool {
	return true
}

// State always reports connected — there is no stream to degrade.
func (n *NoopChannelRulesProvider) State() int32 {
	return provapi.StreamStateConnected
}

// Close is a no-op for NoopChannelRulesProvider.
func (n *NoopChannelRulesProvider) Close() error {
	return nil
}

// Compile-time check that NoopChannelRulesProvider implements ChannelRulesProvider.
var _ ChannelRulesProvider = (*NoopChannelRulesProvider)(nil)
