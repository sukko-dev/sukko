package api

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/internal/server/registry"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// newTestReader returns a registryReader with a nil Valkey client.
// Safe for deriveStatus and fieldsToDetail which do not touch the client.
func newTestReader() *registryReader {
	return &registryReader{
		client: nil,
		env:    "test",
		logger: zerolog.Nop(),
	}
}

// staleThreshold is the computed threshold that deriveStatus uses.
// Equal to platform.RegistryStalenessDivisor × platform.DefaultConnectionsRegistryHeartbeatInterval.
var staleThreshold = time.Duration(platform.RegistryStalenessDivisor) * platform.DefaultConnectionsRegistryHeartbeatInterval

// TestDeriveStatus covers all branches of deriveStatus — the function that determines
// per-connection registry_status from the pod's health data.
func TestDeriveStatus(t *testing.T) {
	t.Parallel()

	// Heartbeat timestamps are computed per subtest, not once for the whole
	// table. The freshness window is RegistryStalenessDivisor × the heartbeat
	// interval (60s), and these subtests run in parallel with the rest of the
	// package — so a timestamp captured at table-construction time can age past
	// the window before it is evaluated, turning a correct "current" result into
	// a spurious "possibly_stale" failure.
	freshTS := func() string { return time.Now().UTC().Format(time.RFC3339) }
	staleTS := func() string {
		return time.Now().Add(-(staleThreshold + time.Minute)).UTC().Format(time.RFC3339)
	}

	tests := []struct {
		name       string
		podID      string
		healthMap  func() map[string]map[string]string
		wantStatus string
	}{
		{
			name:  "current — healthy pod with zero drops, fresh heartbeat, admin healthy",
			podID: "pod-1",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-1": {
						registry.HealthFieldDrops:               "0",
						registry.HealthFieldLastHeartbeat:       freshTS(),
						registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
					},
				}
			},
			wantStatus: registry.RegistryStatusCurrent,
		},
		{
			name:       "possibly_stale — pod not in health map",
			podID:      "pod-missing",
			healthMap:  func() map[string]map[string]string { return map[string]map[string]string{} },
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:       "possibly_stale — health map has pod with empty fields",
			podID:      "pod-empty",
			healthMap:  func() map[string]map[string]string { return map[string]map[string]string{"pod-empty": {}} },
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "possibly_stale — drops > 0",
			podID: "pod-drops",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-drops": {
						registry.HealthFieldDrops:               "5",
						registry.HealthFieldLastHeartbeat:       freshTS(),
						registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
					},
				}
			},
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "possibly_stale — stale heartbeat",
			podID: "pod-stale",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-stale": {
						registry.HealthFieldDrops:               "0",
						registry.HealthFieldLastHeartbeat:       staleTS(),
						registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
					},
				}
			},
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "possibly_stale — missing heartbeat field",
			podID: "pod-nohb",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-nohb": {
						registry.HealthFieldDrops:               "0",
						registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
						// HealthFieldLastHeartbeat absent
					},
				}
			},
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "possibly_stale — admin channel not healthy",
			podID: "pod-noadmin",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-noadmin": {
						registry.HealthFieldDrops:               "0",
						registry.HealthFieldLastHeartbeat:       freshTS(),
						registry.HealthFieldAdminChannelHealthy: "false",
					},
				}
			},
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "possibly_stale — admin channel field empty string",
			podID: "pod-emptyadmin",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-emptyadmin": {
						registry.HealthFieldDrops:               "0",
						registry.HealthFieldLastHeartbeat:       freshTS(),
						registry.HealthFieldAdminChannelHealthy: "",
					},
				}
			},
			wantStatus: registry.RegistryStatusPossiblyStale,
		},
		{
			name:  "current — drops field empty string (parses as 0)",
			podID: "pod-nodrops",
			healthMap: func() map[string]map[string]string {
				return map[string]map[string]string{
					"pod-nodrops": {
						registry.HealthFieldDrops:               "",
						registry.HealthFieldLastHeartbeat:       freshTS(),
						registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
					},
				}
			},
			wantStatus: registry.RegistryStatusCurrent,
		},
	}

	r := newTestReader()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := r.deriveStatus(tt.podID, tt.healthMap())
			if got != tt.wantStatus {
				t.Errorf("deriveStatus() = %q, want %q", got, tt.wantStatus)
			}
		})
	}
}

// TestFieldsToDetail_ChannelsParsed verifies that a JSON channels array in the
// FieldChannels hash field is correctly parsed into ConnectionDetail.Channels.
func TestFieldsToDetail_ChannelsParsed(t *testing.T) {
	t.Parallel()

	r := newTestReader()
	fields := map[string]string{
		registry.FieldTenantID:       "acme",
		registry.FieldAPIKeyID:       "key-1",
		registry.FieldUserID:         "user-1",
		registry.FieldPodID:          "pod-1",
		registry.FieldShardID:        "0",
		registry.FieldRemoteIP:       "1.2.3.4",
		registry.FieldTransport:      "ws",
		registry.FieldConnectedAt:    "2024-01-01T00:00:00Z",
		registry.FieldChannels:       `["acme.prices","acme.trades"]`,
		registry.FieldChannelsCapped: "0",
	}
	healthMap := map[string]map[string]string{}

	detail := r.fieldsToDetail("conn-abc", fields, healthMap)

	if detail.ConnectionID != "conn-abc" {
		t.Errorf("ConnectionID = %q, want %q", detail.ConnectionID, "conn-abc")
	}
	if detail.TenantID != "acme" {
		t.Errorf("TenantID = %q, want %q", detail.TenantID, "acme")
	}
	if len(detail.Channels) != 2 {
		t.Fatalf("len(Channels) = %d, want 2; got %v", len(detail.Channels), detail.Channels)
	}
	if detail.Channels[0] != "acme.prices" {
		t.Errorf("Channels[0] = %q, want %q", detail.Channels[0], "acme.prices")
	}
	if detail.Channels[1] != "acme.trades" {
		t.Errorf("Channels[1] = %q, want %q", detail.Channels[1], "acme.trades")
	}
	if detail.ChannelsCapped {
		t.Error("ChannelsCapped = true, want false for '0' value")
	}
}

// TestFieldsToDetail_EmptyChannels verifies that a missing channels field produces an
// empty (non-nil) slice — JSON marshaling must output "channels":[] not "channels":null.
func TestFieldsToDetail_EmptyChannels(t *testing.T) {
	t.Parallel()

	r := newTestReader()
	fields := map[string]string{
		registry.FieldTenantID:  "acme",
		registry.FieldPodID:     "pod-1",
		registry.FieldTransport: "ws",
		// FieldChannels intentionally absent
	}

	detail := r.fieldsToDetail("conn-1", fields, map[string]map[string]string{})

	if detail.Channels == nil {
		t.Error("Channels is nil — must be empty slice for correct JSON output")
	}
	if len(detail.Channels) != 0 {
		t.Errorf("len(Channels) = %d, want 0", len(detail.Channels))
	}
}

// TestFieldsToDetail_ChannelsCapped verifies that channels_capped="1" sets
// ConnectionDetail.ChannelsCapped to true.
func TestFieldsToDetail_ChannelsCapped(t *testing.T) {
	t.Parallel()

	r := newTestReader()
	fields := map[string]string{
		registry.FieldTenantID:       "acme",
		registry.FieldPodID:          "pod-1",
		registry.FieldChannels:       `["ch1","ch2"]`,
		registry.FieldChannelsCapped: "1",
	}

	detail := r.fieldsToDetail("conn-capped", fields, map[string]map[string]string{})

	if !detail.ChannelsCapped {
		t.Error("ChannelsCapped = false, want true for '1' value")
	}
}

// TestFieldsToDetail_StatusDerived verifies that fieldsToDetail calls deriveStatus using
// the pod_id from the hash fields and the provided healthMap.
func TestFieldsToDetail_StatusDerived(t *testing.T) {
	t.Parallel()

	// Same reason as TestDeriveStatus: evaluate the heartbeat at use time so a
	// captured timestamp cannot age past the 60s freshness window first.
	freshTS := func() string { return time.Now().UTC().Format(time.RFC3339) }
	r := newTestReader()

	healthMap := map[string]map[string]string{
		"pod-abc": {
			registry.HealthFieldDrops:               "0",
			registry.HealthFieldLastHeartbeat:       freshTS(),
			registry.HealthFieldAdminChannelHealthy: registry.HealthValueTrue,
		},
	}
	fields := map[string]string{
		registry.FieldPodID: "pod-abc",
	}

	detail := r.fieldsToDetail("conn-1", fields, healthMap)

	if detail.RegistryStatus != registry.RegistryStatusCurrent {
		t.Errorf("RegistryStatus = %q, want %q", detail.RegistryStatus, registry.RegistryStatusCurrent)
	}
}

// TestFieldsToDetail_InvalidChannelsJSON verifies that malformed JSON in the channels field
// is handled gracefully — result is an empty (non-nil) slice, not a panic.
func TestFieldsToDetail_InvalidChannelsJSON(t *testing.T) {
	t.Parallel()

	r := newTestReader()
	fields := map[string]string{
		registry.FieldChannels: `not-valid-json`,
	}

	detail := r.fieldsToDetail("conn-1", fields, map[string]map[string]string{})

	if detail.Channels == nil {
		t.Error("Channels is nil after bad JSON — must be empty slice")
	}
}
