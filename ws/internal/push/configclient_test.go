package push

import (
	"encoding/json"
	"testing"
	"time"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
)

// ---------------------------------------------------------------------------
// Tests: Snapshot application via applySnapshot + public getters
// ---------------------------------------------------------------------------

// newTestConfigClient creates a ConfigClient with only the pushConfig
// atomic.Value initialized (no gRPC connection). Suitable for testing
// cache logic without network dependencies.
func newTestConfigClient() *ConfigClient {
	c := &ConfigClient{}
	c.pushConfig.Store(emptySnapshot())
	return c
}

func TestApplySnapshot_PatternsAndCredentials(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	resp := &provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{
				TenantSlug:     "acme",
				Provider:       "vapid",
				CredentialData: `{"public_key":"abc","private_key":"xyz"}`,
			},
			{
				TenantSlug:     "acme",
				Provider:       "fcm",
				CredentialData: `{"api_key":"fcm-key-123"}`,
			},
			{
				TenantSlug:     "beta",
				Provider:       "vapid",
				CredentialData: `{"public_key":"beta-pub","private_key":"beta-priv"}`,
			},
		},
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{
				TenantSlug: "acme",
				Patterns:   []string{"acme.alerts.*", "acme.market.*"},
			},
			{
				TenantSlug: "beta",
				Patterns:   []string{"beta.notifications.*"},
			},
		},
	}

	c.applySnapshot(resp)

	// Verify GetPushPatterns.
	acmePatterns := c.GetPushPatterns("acme")
	if len(acmePatterns) != 2 {
		t.Fatalf("acme patterns count = %d, want 2", len(acmePatterns))
	}
	if acmePatterns[0] != "acme.alerts.*" || acmePatterns[1] != "acme.market.*" {
		t.Errorf("acme patterns = %v, want [acme.alerts.* acme.market.*]", acmePatterns)
	}

	betaPatterns := c.GetPushPatterns("beta")
	if len(betaPatterns) != 1 || betaPatterns[0] != "beta.notifications.*" {
		t.Errorf("beta patterns = %v, want [beta.notifications.*]", betaPatterns)
	}

	// Unknown tenant returns nil.
	if got := c.GetPushPatterns("unknown"); got != nil {
		t.Errorf("unknown tenant patterns = %v, want nil", got)
	}

	// Verify GetCredential.
	vapidCred, err := c.GetCredential("acme", "vapid")
	if err != nil {
		t.Fatalf("GetCredential(acme, vapid) error = %v", err)
	}
	var parsed struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(vapidCred, &parsed); err != nil {
		t.Fatalf("unmarshal vapid credential: %v", err)
	}
	if parsed.PublicKey != "abc" {
		t.Errorf("vapid public_key = %q, want %q", parsed.PublicKey, "abc")
	}

	fcmCred, err := c.GetCredential("acme", "fcm")
	if err != nil {
		t.Fatalf("GetCredential(acme, fcm) error = %v", err)
	}
	if fcmCred == nil {
		t.Fatal("expected non-nil FCM credential")
	}

	// Unknown provider returns nil, nil.
	unknownCred, err := c.GetCredential("acme", "apns")
	if err != nil {
		t.Fatalf("GetCredential(acme, apns) error = %v", err)
	}
	if unknownCred != nil {
		t.Errorf("expected nil credential for unknown provider, got %s", unknownCred)
	}

	// Unknown tenant returns nil, nil.
	unknownTenantCred, err := c.GetCredential("unknown", "vapid")
	if err != nil {
		t.Fatalf("GetCredential(unknown, vapid) error = %v", err)
	}
	if unknownTenantCred != nil {
		t.Errorf("expected nil credential for unknown tenant, got %s", unknownTenantCred)
	}
}

func TestApplySnapshot_ReplacesExisting(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	// Apply first snapshot.
	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "acme", Patterns: []string{"acme.old.*"}},
		},
	})

	if got := c.GetPushPatterns("acme"); len(got) != 1 || got[0] != "acme.old.*" {
		t.Fatalf("initial patterns = %v, want [acme.old.*]", got)
	}

	// Apply second snapshot — should fully replace the first.
	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "beta", Patterns: []string{"beta.new.*"}},
		},
	})

	// Old tenant should be gone.
	if got := c.GetPushPatterns("acme"); got != nil {
		t.Errorf("acme patterns after new snapshot = %v, want nil", got)
	}
	if got := c.GetPushPatterns("beta"); len(got) != 1 || got[0] != "beta.new.*" {
		t.Errorf("beta patterns = %v, want [beta.new.*]", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: Delta application
// ---------------------------------------------------------------------------

func TestApplyDelta_Upsert(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	// Start with a snapshot.
	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{TenantSlug: "acme", Provider: "vapid", CredentialData: `{"public_key":"old"}`},
		},
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "acme", Patterns: []string{"acme.alerts.*"}},
		},
	})

	// Apply delta that updates acme's credential and adds beta.
	c.applyDelta(&provisioningv1.WatchPushConfigResponse{
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{TenantSlug: "acme", Provider: "vapid", CredentialData: `{"public_key":"new"}`},
			{TenantSlug: "beta", Provider: "vapid", CredentialData: `{"public_key":"beta-key"}`},
		},
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "beta", Patterns: []string{"beta.market.*"}},
		},
	})

	// acme credential should be updated.
	cred, err := c.GetCredential("acme", "vapid")
	if err != nil {
		t.Fatalf("GetCredential error = %v", err)
	}
	var parsed struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(cred, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.PublicKey != "new" {
		t.Errorf("acme vapid public_key = %q, want %q", parsed.PublicKey, "new")
	}

	// acme patterns should still exist (delta didn't remove them).
	if got := c.GetPushPatterns("acme"); len(got) != 1 || got[0] != "acme.alerts.*" {
		t.Errorf("acme patterns after delta = %v, want [acme.alerts.*]", got)
	}

	// beta should be added.
	if got := c.GetPushPatterns("beta"); len(got) != 1 || got[0] != "beta.market.*" {
		t.Errorf("beta patterns = %v, want [beta.market.*]", got)
	}
}

func TestApplyDelta_RemoveTenant(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	// Start with a snapshot containing two tenants.
	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{TenantSlug: "acme", Provider: "vapid", CredentialData: `{"public_key":"acme-key"}`},
			{TenantSlug: "beta", Provider: "vapid", CredentialData: `{"public_key":"beta-key"}`},
		},
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "acme", Patterns: []string{"acme.alerts.*"}},
			{TenantSlug: "beta", Patterns: []string{"beta.alerts.*"}},
		},
	})

	// Delta removes acme entirely.
	c.applyDelta(&provisioningv1.WatchPushConfigResponse{
		RemovedConfigTenantSlugs: []string{"acme"},
	})

	// acme should be gone.
	if got := c.GetPushPatterns("acme"); got != nil {
		t.Errorf("acme patterns after removal = %v, want nil", got)
	}
	cred, _ := c.GetCredential("acme", "vapid")
	if cred != nil {
		t.Errorf("acme credential after removal = %s, want nil", cred)
	}

	// beta should be unaffected.
	if got := c.GetPushPatterns("beta"); len(got) != 1 {
		t.Errorf("beta patterns = %v, want [beta.alerts.*]", got)
	}
}

func TestApplyDelta_RemoveCredential(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{TenantSlug: "acme", Provider: "vapid", CredentialData: `{"public_key":"v"}`},
			{TenantSlug: "acme", Provider: "fcm", CredentialData: `{"api_key":"f"}`},
		},
	})

	// Remove only the FCM credential via "tenantID:provider" format.
	c.applyDelta(&provisioningv1.WatchPushConfigResponse{
		RemovedCredentialIds: []string{"acme:fcm"},
	})

	// VAPID should still exist.
	vapid, _ := c.GetCredential("acme", "vapid")
	if vapid == nil {
		t.Error("expected VAPID credential to survive FCM removal")
	}

	// FCM should be gone.
	fcm, _ := c.GetCredential("acme", "fcm")
	if fcm != nil {
		t.Errorf("expected nil FCM credential after removal, got %s", fcm)
	}
}

// ---------------------------------------------------------------------------
// Tests: GetPushPatterns returns a copy (mutation safety)
// ---------------------------------------------------------------------------

func TestGetPushPatterns_ReturnsCopy(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushChannelConfigs: []*provisioningv1.PushChannelConfig{
			{TenantSlug: "acme", Patterns: []string{"acme.alerts.*"}},
		},
	})

	patterns := c.GetPushPatterns("acme")
	patterns[0] = "MUTATED"

	// Original should be unaffected.
	original := c.GetPushPatterns("acme")
	if original[0] != "acme.alerts.*" {
		t.Errorf("GetPushPatterns returned a reference instead of a copy: %v", original)
	}
}

// ---------------------------------------------------------------------------
// Tests: GetCredential returns a copy (mutation safety)
// ---------------------------------------------------------------------------

func TestGetCredential_ReturnsCopy(t *testing.T) {
	t.Parallel()

	c := newTestConfigClient()

	c.applySnapshot(&provisioningv1.WatchPushConfigResponse{
		IsSnapshot: true,
		PushCredentials: []*provisioningv1.PushCredentialInfo{
			{TenantSlug: "acme", Provider: "vapid", CredentialData: `{"public_key":"original"}`},
		},
	})

	cred, _ := c.GetCredential("acme", "vapid")
	cred[0] = 'X' // mutate the returned slice

	// Original should be unaffected.
	original, _ := c.GetCredential("acme", "vapid")
	var parsed struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(original, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.PublicKey != "original" {
		t.Errorf("GetCredential returned a reference instead of a copy: %s", original)
	}
}

// ---------------------------------------------------------------------------
// Tests: parseCredentialID
// ---------------------------------------------------------------------------

func TestParseCredentialID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		id           string
		wantTenantID string
		wantProvider string
	}{
		{"valid", "acme:vapid", "acme", "vapid"},
		{"valid with dots", "my.tenant:fcm", "my.tenant", "fcm"},
		{"empty", "", "", ""},
		{"no colon", "acmevapid", "", ""},
		{"colon at start", ":vapid", "", "vapid"},
		{"colon at end", "acme:", "acme", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotTenant, gotProvider := parseCredentialID(tt.id)
			if gotTenant != tt.wantTenantID {
				t.Errorf("tenantID = %q, want %q", gotTenant, tt.wantTenantID)
			}
			if gotProvider != tt.wantProvider {
				t.Errorf("provider = %q, want %q", gotProvider, tt.wantProvider)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: pushConfigBackoff
// ---------------------------------------------------------------------------

func TestPushConfigBackoff(t *testing.T) {
	t.Parallel()

	base := 1 * time.Second
	maxDelay := 30 * time.Second

	// First backoff from base should approximately double (with jitter 75%-100%).
	next := pushConfigBackoff(base, maxDelay)
	// 2s * 0.75 = 1.5s minimum, 2s * 1.0 = 2.0s maximum
	if next < 1500*time.Millisecond || next > 2*time.Second {
		t.Errorf("first backoff = %v, want between 1.5s and 2.0s", next)
	}

	// Should cap at maxDelay.
	huge := pushConfigBackoff(20*time.Second, maxDelay)
	// 40s capped to 30s, then jitter: 30s * 0.75 = 22.5s min, 30s * 1.0 = 30s max
	if huge > maxDelay {
		t.Errorf("backoff exceeded max: %v > %v", huge, maxDelay)
	}
}

// ---------------------------------------------------------------------------
// Tests: emptySnapshot initialization
// ---------------------------------------------------------------------------

func TestEmptySnapshot(t *testing.T) {
	t.Parallel()

	snap := emptySnapshot()
	if snap.patterns == nil {
		t.Error("patterns map should be initialized")
	}
	if snap.credentials == nil {
		t.Error("credentials map should be initialized")
	}
	if snap.pushEnabledTenants == nil {
		t.Error("pushEnabledTenants map should be initialized")
	}
}
