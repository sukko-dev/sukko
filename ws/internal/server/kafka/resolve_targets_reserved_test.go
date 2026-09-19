package kafka

import (
	"slices"
	"testing"

	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/types"
)

// TestResolveTargets_SkipsReservedEgressSuffixes pins the §II produce-time
// backstop for ADR-0018's core invariant.
//
// Write-time validation rejects 'default' and 'dead-letter' as egress topics,
// and migration 003 strips them from legacy rows — but neither guards a
// snapshot that reaches the producer by some other route (a skewed peer, a
// hand-edited row, a future write path). Both would be delivery bugs, not
// merely wasted writes:
//
//   - 'default' is ALWAYS consumed, so an egress copy there is delivered to
//     subscribers a second time, carrying a different mid (identity hashes the
//     topic), so clients cannot deduplicate it.
//   - 'dead-letter' is never consumed and must not receive live traffic.
func TestResolveTargets_SkipsReservedEgressSuffixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		egress     []string
		wantEgress []string
	}{
		{
			name:       "default suffix is skipped — it is always consumed",
			egress:     []string{"default", "audit"},
			wantEgress: []string{"prod.acme.audit"},
		},
		{
			name:       "dead-letter suffix is skipped — it is never consumed",
			egress:     []string{"dead-letter", "audit"},
			wantEgress: []string{"prod.acme.audit"},
		},
		{
			name:       "both reserved suffixes skipped, leaving no egress",
			egress:     []string{"default", "dead-letter"},
			wantEgress: []string{},
		},
		{
			name:       "non-reserved egress is untouched",
			egress:     []string{"audit", "analytics"},
			wantEgress: []string{"prod.acme.audit", "prod.acme.analytics"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			prov := &stubRulesProvider{
				ok:     true,
				synced: true,
				snap: provapi.TenantRoutingSnapshot{
					Rules: []types.RoutingRule{{
						Pattern:      "**",
						IngressTopic: "trades",
						EgressTopics: tt.egress,
						Priority:     1,
					}},
				},
			}

			plan, err := resolveTargetsProducer(prov).resolveTargets("acme.BTC.trade", "acme")
			if err != nil {
				t.Fatalf("resolveTargets: %v", err)
			}
			if plan.ingressTopic != "prod.acme.trades" {
				t.Errorf("ingressTopic = %q, want prod.acme.trades", plan.ingressTopic)
			}
			if !slices.Equal(plan.egressTopics, tt.wantEgress) {
				t.Errorf("egressTopics = %v, want %v (reserved suffixes must never be egress targets)",
					plan.egressTopics, tt.wantEgress)
			}
		})
	}
}
