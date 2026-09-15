package license

import "testing"

func TestRequiredEdition(t *testing.T) {
	t.Parallel()

	// Community: the full data path (ingestion + history + recovery) is free so
	// the public benchmark is reproducible; capacity caps are the tier wall
	// (ADR-0009). Channel rules are ungated (sole channel-authorization
	// mechanism); REST publish is the no-Kafka ingestion on-ramp. Routing rules
	// are the sole channel→topic mapping, so publishing into the free Kafka
	// backend requires them — MaxRoutingRulesPerTenant is the wall (ADR-0014).
	communityFeatures := []Feature{
		PerTenantChannelRules, KafkaBackend, MessageHistory, LiveGapRecovery,
		RESTPublish, ChannelTopicRouting,
	}
	for _, f := range communityFeatures {
		if got := RequiredEdition(f); got != Community {
			t.Errorf("RequiredEdition(%q) = %q, want Community", f, got)
		}
	}

	proFeatures := []Feature{
		SSETransport, WebPush, AnalyticsPush,
		PerTenantConnectionLimits, PerTenantConfigurableQuotas,
		TenantLifecycleManager, Alerting, Analytics, ConnectionTracing, AdminUI,
		TokenRevocation, Webhooks, ConnectionsAPI,
		ChannelPatternsCEL, DeltaCompression,
	}
	for _, f := range proFeatures {
		if got := RequiredEdition(f); got != Pro {
			t.Errorf("RequiredEdition(%q) = %q, want Pro", f, got)
		}
	}

	enterpriseFeatures := []Feature{
		AuditLogging, MobilePush, IPAllowlisting,
		PriorityRouting, CustomQuotaPolicies,
	}
	for _, f := range enterpriseFeatures {
		if got := RequiredEdition(f); got != Enterprise {
			t.Errorf("RequiredEdition(%q) = %q, want Enterprise", f, got)
		}
	}

	// Unknown feature → Community (not gated)
	if got := RequiredEdition(Feature("nonexistent")); got != Community {
		t.Errorf("RequiredEdition(unknown) = %q, want Community", got)
	}
}

func TestEditionHasFeature(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		edition Edition
		feature Feature
		want    bool
	}{
		// Community has the full data path (ADR-0009: benchmark reproducible free)
		{"community+kafka", Community, KafkaBackend, true},
		{"community+history", Community, MessageHistory, true},
		{"community+gap-recovery", Community, LiveGapRecovery, true},
		{"community+rest-publish", Community, RESTPublish, true},

		// Community cannot use Pro features
		{"community+sse", Community, SSETransport, false},
		{"community+alerting", Community, Alerting, false},
		{"community+webpush", Community, WebPush, false},

		// Pro can use Pro features, not Enterprise
		{"pro+kafka", Pro, KafkaBackend, true},
		{"pro+alerting", Pro, Alerting, true},
		{"pro+webpush", Pro, WebPush, true},
		{"pro+analytics-push", Pro, AnalyticsPush, true},
		{"pro+mobilepush", Pro, MobilePush, false},
		{"pro+audit", Pro, AuditLogging, false},

		// Enterprise can use everything
		{"enterprise+kafka", Enterprise, KafkaBackend, true},
		{"enterprise+webpush", Enterprise, WebPush, true},
		{"enterprise+mobilepush", Enterprise, MobilePush, true},
		{"enterprise+audit", Enterprise, AuditLogging, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := EditionHasFeature(tt.edition, tt.feature); got != tt.want {
				t.Errorf("EditionHasFeature(%q, %q) = %v, want %v", tt.edition, tt.feature, got, tt.want)
			}
		})
	}
}
