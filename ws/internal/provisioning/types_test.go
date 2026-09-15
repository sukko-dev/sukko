package provisioning

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestTenant_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		tenant  *Tenant
		wantErr bool
	}{
		{
			name: "valid tenant",
			tenant: &Tenant{
				Slug:         "acme-corp",
				Name:         "Acme Corporation",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: false,
		},
		{
			name: "valid tenant with numbers",
			tenant: &Tenant{
				Slug:         "tenant-123",
				Name:         "Tenant 123",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: false,
		},
		{
			name: "valid tenant - minimum length",
			tenant: &Tenant{
				Slug:         "abc",
				Name:         "ABC",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: false,
		},
		{
			name: "invalid - uppercase letters",
			tenant: &Tenant{
				Slug:         "ACME-CORP",
				Name:         "Acme",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - starts with number",
			tenant: &Tenant{
				Slug:         "123-tenant",
				Name:         "Tenant",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - starts with hyphen",
			tenant: &Tenant{
				Slug:         "-tenant",
				Name:         "Tenant",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - too short",
			tenant: &Tenant{
				Slug:         "ab",
				Name:         "AB",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - too long",
			tenant: &Tenant{
				Slug:         "this-is-a-very-long-tenant-id-that-exceeds-the-maximum-allowed-length-limit",
				Name:         "Long",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - special characters",
			tenant: &Tenant{
				Slug:         "tenant_id",
				Name:         "Tenant",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
		{
			name: "invalid - empty name",
			tenant: &Tenant{
				Slug:         "valid-id",
				Name:         "",
				Status:       StatusActive,
				ConsumerType: ConsumerShared,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.tenant.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Tenant.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTenantKey_Validate(t *testing.T) {
	t.Parallel()
	samplePEM := `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEn6jKqjRy/2aBT3c5H8QT2CnLMz7O
nUwZ9KeOJoL8G5FmH6u0L9Pt5TXpR1LW9YXhNO3WL9YqKYL7qfqB5i0b6Q==
-----END PUBLIC KEY-----`

	tests := []struct {
		name    string
		key     *TenantKey
		wantErr bool
	}{
		{
			name: "valid ES256 key",
			key: &TenantKey{
				KeyID:     "key-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmES256,
				PublicKey: samplePEM,
			},
			wantErr: false,
		},
		{
			name: "valid RS256 key",
			key: &TenantKey{
				KeyID:     "rsa-key-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmRS256,
				PublicKey: samplePEM,
			},
			wantErr: false,
		},
		{
			name: "valid EdDSA key",
			key: &TenantKey{
				KeyID:     "ed-key-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmEdDSA,
				PublicKey: samplePEM,
			},
			wantErr: false,
		},
		{
			name: "invalid key ID - uppercase",
			key: &TenantKey{
				KeyID:     "KEY-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmES256,
				PublicKey: samplePEM,
			},
			wantErr: true,
		},
		{
			name: "invalid key ID - too short",
			key: &TenantKey{
				KeyID:     "ab",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmES256,
				PublicKey: samplePEM,
			},
			wantErr: true,
		},
		{
			name: "invalid algorithm",
			key: &TenantKey{
				KeyID:     "key-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: "HS256",
				PublicKey: samplePEM,
			},
			wantErr: true,
		},
		{
			name: "empty public key",
			key: &TenantKey{
				KeyID:     "key-1",
				TenantID:  "12345678-1234-1234-1234-123456789abc",
				Algorithm: AlgorithmES256,
				PublicKey: "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.key.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("TenantKey.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTenantKey_IsExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	tests := []struct {
		name    string
		key     *TenantKey
		expired bool
	}{
		{
			name: "no expiry",
			key: &TenantKey{
				ExpiresAt: nil,
			},
			expired: false,
		},
		{
			name: "future expiry",
			key: &TenantKey{
				ExpiresAt: &future,
			},
			expired: false,
		},
		{
			name: "past expiry",
			key: &TenantKey{
				ExpiresAt: &past,
			},
			expired: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.key.IsExpired(); got != tt.expired {
				t.Errorf("TenantKey.IsExpired() = %v, want %v", got, tt.expired)
			}
		})
	}
}

func TestTenantStatus_IsValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status TenantStatus
		valid  bool
	}{
		{StatusActive, true},
		{StatusSuspended, true},
		{StatusDeprovisioning, true},
		{StatusDeleted, true},
		{"invalid", false},
		{"", false},
		{"ACTIVE", false}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			t.Parallel()
			if got := tt.status.IsValid(); got != tt.valid {
				t.Errorf("TenantStatus(%q).IsValid() = %v, want %v", tt.status, got, tt.valid)
			}
		})
	}
}

func TestConsumerType_IsValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		ctype ConsumerType
		valid bool
	}{
		{ConsumerShared, true},
		{ConsumerDedicated, true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.ctype), func(t *testing.T) {
			t.Parallel()
			if got := tt.ctype.IsValid(); got != tt.valid {
				t.Errorf("ConsumerType(%q).IsValid() = %v, want %v", tt.ctype, got, tt.valid)
			}
		})
	}
}

func TestAlgorithm_IsValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		alg   Algorithm
		valid bool
	}{
		{AlgorithmES256, true},
		{AlgorithmRS256, true},
		{AlgorithmEdDSA, true},
		{"HS256", false}, // symmetric not allowed
		{"", false},
		{"es256", false}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(string(tt.alg), func(t *testing.T) {
			t.Parallel()
			if got := tt.alg.IsValid(); got != tt.valid {
				t.Errorf("Algorithm(%q).IsValid() = %v, want %v", tt.alg, got, tt.valid)
			}
		})
	}
}

func TestTenant_IsActive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status TenantStatus
		active bool
	}{
		{StatusActive, true},
		{StatusSuspended, false},
		{StatusDeprovisioning, false},
		{StatusDeleted, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			t.Parallel()
			tenant := &Tenant{Status: tt.status}
			if got := tenant.IsActive(); got != tt.active {
				t.Errorf("Tenant{Status: %q}.IsActive() = %v, want %v", tt.status, got, tt.active)
			}
		})
	}
}

func TestFormatPrincipal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tenantID string
		expected string
	}{
		{"acme", "User:acme"},
		{"tenant-123", "User:tenant-123"},
		{"a", "User:a"},
		{"", "User:"},
	}

	for _, tt := range tests {
		t.Run(tt.tenantID, func(t *testing.T) {
			t.Parallel()
			got := FormatPrincipal(tt.tenantID)
			if got != tt.expected {
				t.Errorf("FormatPrincipal(%q) = %q, want %q", tt.tenantID, got, tt.expected)
			}
		})
	}
}

func TestParsePrincipal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		principal string
		expected  string
	}{
		{"User:acme", "acme"},
		{"User:tenant-123", "tenant-123"},
		{"User:a", "a"},
		{"User:", ""},
		{"acme", ""},       // missing prefix
		{"user:acme", ""},  // wrong case
		{"Group:acme", ""}, // wrong type
		{"", ""},           // empty
		{"User", ""},       // no colon
	}

	for _, tt := range tests {
		t.Run(tt.principal, func(t *testing.T) {
			t.Parallel()
			got := ParsePrincipal(tt.principal)
			if got != tt.expected {
				t.Errorf("ParsePrincipal(%q) = %q, want %q", tt.principal, got, tt.expected)
			}
		})
	}
}

func TestValidatePrincipal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		principal string
		valid     bool
	}{
		{"User:acme", true},
		{"User:tenant-123", true},
		{"User:a", true},
		{"User:", false},      // empty tenant ID
		{"acme", false},       // missing prefix
		{"user:acme", false},  // wrong case
		{"Group:acme", false}, // wrong type
		{"", false},           // empty
		{"User", false},       // no colon
		{"Use:", false},       // too short prefix
	}

	for _, tt := range tests {
		t.Run(tt.principal, func(t *testing.T) {
			t.Parallel()
			got := ValidatePrincipal(tt.principal)
			if got != tt.valid {
				t.Errorf("ValidatePrincipal(%q) = %v, want %v", tt.principal, got, tt.valid)
			}
		})
	}
}

func TestACLConstants(t *testing.T) {
	t.Parallel()
	// Verify ACL constants have expected values
	if ACLResourceTopic != "TOPIC" {
		t.Errorf("ACLResourceTopic = %q, want %q", ACLResourceTopic, "TOPIC")
	}
	if ACLResourceGroup != "GROUP" {
		t.Errorf("ACLResourceGroup = %q, want %q", ACLResourceGroup, "GROUP")
	}
	if ACLPatternLiteral != "LITERAL" {
		t.Errorf("ACLPatternLiteral = %q, want %q", ACLPatternLiteral, "LITERAL")
	}
	if ACLPatternPrefixed != "PREFIXED" {
		t.Errorf("ACLPatternPrefixed = %q, want %q", ACLPatternPrefixed, "PREFIXED")
	}
	if ACLOpAll != "ALL" {
		t.Errorf("ACLOpAll = %q, want %q", ACLOpAll, "ALL")
	}
	if ACLPermissionAllow != "ALLOW" {
		t.Errorf("ACLPermissionAllow = %q, want %q", ACLPermissionAllow, "ALLOW")
	}
	if ACLPermissionDeny != "DENY" {
		t.Errorf("ACLPermissionDeny = %q, want %q", ACLPermissionDeny, "DENY")
	}
}

func TestGenerateAPIKeyID(t *testing.T) {
	t.Parallel()

	t.Run("has pk_live_ prefix", func(t *testing.T) {
		t.Parallel()
		key, err := GenerateAPIKeyID()
		if err != nil {
			t.Fatalf("GenerateAPIKeyID() error = %v", err)
		}
		if !strings.HasPrefix(key, "pk_live_") {
			t.Errorf("GenerateAPIKeyID() = %q, want pk_live_ prefix", key)
		}
	})

	t.Run("sufficient length", func(t *testing.T) {
		t.Parallel()
		key, err := GenerateAPIKeyID()
		if err != nil {
			t.Fatalf("GenerateAPIKeyID() error = %v", err)
		}
		// pk_live_ (8 chars) + base64url of 32 bytes (43 chars) = 51 chars
		if len(key) < 50 {
			t.Errorf("GenerateAPIKeyID() length = %d, want >= 50", len(key))
		}
	})

	t.Run("uniqueness", func(t *testing.T) {
		t.Parallel()
		key1, err := GenerateAPIKeyID()
		if err != nil {
			t.Fatalf("GenerateAPIKeyID() key1 error = %v", err)
		}
		key2, err := GenerateAPIKeyID()
		if err != nil {
			t.Fatalf("GenerateAPIKeyID() key2 error = %v", err)
		}
		if key1 == key2 {
			t.Errorf("GenerateAPIKeyID() produced duplicate keys: %q", key1)
		}
	})

	t.Run("entropy - 32 bytes decoded", func(t *testing.T) {
		t.Parallel()
		key, err := GenerateAPIKeyID()
		if err != nil {
			t.Fatalf("GenerateAPIKeyID() error = %v", err)
		}
		encoded := strings.TrimPrefix(key, "pk_live_")
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("base64 decode error = %v", err)
		}
		if len(decoded) != 32 {
			t.Errorf("decoded entropy length = %d bytes, want 32", len(decoded))
		}
	})
}
