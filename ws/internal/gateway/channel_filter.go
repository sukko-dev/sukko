package gateway

import (
	"context"
	"slices"
	"strings"

	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/protocol"
)

// filterSubscribeChannels validates and filters channels for subscribe-like operations.
// The single subscribe-authorization path for ALL transports (WS proxy, SSE,
// Web Push) — §XVIII consistency by construction.
//
// Steps:
//  1. Filter out channels failing tenant prefix validation
//  2. Filter out channels with fewer than MinInternalChannelParts
//  3. Strip tenant prefix from surviving channels
//  4. Apply per-tenant rules filtering (tenant checker — the sole source)
//  5. Re-prefix allowed channels with tenantID
func (gw *Gateway) filterSubscribeChannels(
	ctx context.Context,
	channels []string,
	tenantID string,
	claims *auth.Claims,
) []string {
	// 1 + 2. Tenant prefix and channel format validation
	valid := make([]string, 0, len(channels))
	for _, ch := range channels {
		switch {
		case !auth.ValidateChannelTenant(ch, tenantID):
			RecordChannelCheck(ChannelCheckDenied)
			RecordAccessDenial(AccessDenialResourceChannel, AccessDenialReasonWrongTenant)
			gw.logger.Warn().
				Str("channel", ch).
				Str(logging.LogKeyTenantSlug, tenantID).
				Str("reason", "wrong_tenant_prefix").
				Msg("channel denied")
		case strings.Count(ch, ".")+1 < protocol.MinInternalChannelParts:
			RecordChannelCheck(ChannelCheckDenied)
			RecordAccessDenial(AccessDenialResourceChannel, AccessDenialReasonInvalidFormat)
			gw.logger.Warn().
				Str("channel", ch).
				Str(logging.LogKeyTenantSlug, tenantID).
				Str("reason", "invalid_format").
				Msg("channel denied")
		default:
			valid = append(valid, ch)
		}
	}

	// 3. Strip tenant prefix for pattern matching
	tenantPrefix := tenantID + "."
	stripped := make([]string, len(valid))
	for i, ch := range valid {
		stripped[i] = strings.TrimPrefix(ch, tenantPrefix)
	}

	// 4. Per-tenant rules filtering — the sole authorization source.
	// Defensive (§II): the checker is constructed unconditionally at startup,
	// so nil here should be impossible — fail closed rather than panic.
	if gw.tenantPermChecker == nil {
		gw.logger.Error().Str(logging.LogKeyTenantSlug, tenantID).
			Msg("tenant permission checker not initialized; denying all channels")
		return nil
	}
	allowedStripped := gw.tenantPermChecker.FilterChannels(ctx, tenantID, claims, stripped)

	// 5. Re-prefix allowed channels and record metrics
	allowed := make([]string, 0, len(allowedStripped))
	for _, s := range allowedStripped {
		allowed = append(allowed, tenantPrefix+s)
	}

	// Record metrics for permission-denied channels
	for _, ch := range valid {
		if !slices.Contains(allowed, ch) {
			RecordChannelCheck(ChannelCheckDenied)
			RecordAccessDenial(AccessDenialResourceChannel, AccessDenialReasonUnauthorized)
			gw.logger.Warn().
				Str("channel", ch).
				Str(logging.LogKeyTenantSlug, tenantID).
				Str("reason", "permission_denied").
				Msg("channel denied")
		} else {
			RecordChannelCheck(ChannelCheckAllowed)
		}
	}

	return allowed
}

// checkPublishAllowed is the single publish-authorization path for ALL
// transports (WS proxy interceptPublish + REST HandlePublish) — §XVIII.
// It takes the FULL (tenant-prefixed) channel and strips the prefix
// internally; rules are stored bare. Callers have already validated the
// tenant prefix, but stripping tolerates a mismatch (no-op → deny).
func (gw *Gateway) checkPublishAllowed(ctx context.Context, tenantID string, claims *auth.Claims, channel string) bool {
	// Defensive (§II): fail closed if the checker is missing (see above).
	if gw.tenantPermChecker == nil {
		gw.logger.Error().Str(logging.LogKeyTenantSlug, tenantID).
			Msg("tenant permission checker not initialized; denying publish")
		return false
	}
	stripped := strings.TrimPrefix(channel, tenantID+".")
	if gw.tenantPermChecker.CanPublish(ctx, tenantID, claims, stripped) {
		RecordChannelCheck(ChannelCheckAllowed)
		return true
	}
	RecordChannelCheck(ChannelCheckDenied)
	RecordAccessDenial(AccessDenialResourceChannel, AccessDenialReasonUnauthorized)
	gw.logger.Warn().
		Str("channel", channel).
		Str(logging.LogKeyTenantSlug, tenantID).
		Str("reason", "publish_denied").
		Msg("channel publish denied")
	return false
}
