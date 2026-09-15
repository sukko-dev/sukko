package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sukko-dev/sukko/internal/shared/license"
)

func TestParseEdition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    license.Edition
		wantErr bool
	}{
		{"community", "community", license.Community, false},
		{"pro", "pro", license.Pro, false},
		{"enterprise", "enterprise", license.Enterprise, false},
		{"invalid", "platinum", "", true},
		{"empty", "", "", true},
		{"capitalized", "Pro", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseEdition(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseEdition(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseEdition(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseExpiry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		check   func(time.Time) bool
		wantErr bool
	}{
		{
			name:  "absolute date",
			input: "2027-01-01",
			check: func(t time.Time) bool {
				return t.Year() == 2027 && t.Month() == 1 && t.Day() == 1 && t.Hour() == 23
			},
		},
		{
			name:  "relative +30d",
			input: "+30d",
			check: func(t time.Time) bool {
				expected := time.Now().UTC().AddDate(0, 0, 30)
				diff := t.Sub(expected)
				return diff > -time.Minute && diff < time.Minute
			},
		},
		{
			name:  "relative +1y",
			input: "+1y",
			check: func(t time.Time) bool {
				expected := time.Now().UTC().AddDate(1, 0, 0)
				diff := t.Sub(expected)
				return diff > -time.Minute && diff < time.Minute
			},
		},
		{
			name:  "relative -1d (past)",
			input: "-1d",
			check: func(t time.Time) bool {
				return t.Before(time.Now())
			},
		},
		{
			name:    "invalid format",
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:    "invalid relative unit",
			input:   "+30m",
			wantErr: true,
		},
		{
			name:    "invalid relative number",
			input:   "+abcd",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseExpiry(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExpiry(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.check != nil && !tt.check(got) {
				t.Errorf("parseExpiry(%q) = %v, failed check", tt.input, got)
			}
		})
	}
}

func TestBuildClaims(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		edition string
		org     string
		limits  license.Limits
		wantErr bool
		check   func(license.Claims) bool
	}{
		{
			name:    "valid pro",
			edition: "pro",
			org:     "Test Corp",
			limits:  license.Limits{},
			check: func(c license.Claims) bool {
				return c.Edition == license.Pro && c.Org == "Test Corp" && c.Limits.MaxTenants == 0
			},
		},
		{
			name:    "with limit overrides",
			edition: "pro",
			org:     "Test",
			limits:  license.Limits{MaxTenants: 5, MaxTotalConnections: 1000},
			check: func(c license.Claims) bool {
				return c.Limits.MaxTenants == 5 && c.Limits.MaxTotalConnections == 1000
			},
		},
		{
			name:    "zero overrides stay zero",
			edition: "enterprise",
			org:     "Test",
			limits:  license.Limits{},
			check: func(c license.Claims) bool {
				return c.Limits.MaxTenants == 0 && c.Limits.MaxShards == 0
			},
		},
		{
			name:    "invalid edition",
			edition: "platinum",
			org:     "Test",
			wantErr: true,
		},
	}

	exp := time.Now().Add(24 * time.Hour)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildClaims(tt.edition, tt.org, exp, time.Now().Unix(), 0, tt.limits)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildClaims() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.check != nil && !tt.check(got) {
				t.Errorf("buildClaims() = %+v, failed check", got)
			}
		})
	}
}

func TestValidateRequired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		key     string
		edition string
		org     string
		expires string
		wantErr bool
	}{
		{"all set", "key.pem", "pro", "Test", "+1y", false},
		{"missing key", "", "pro", "Test", "+1y", true},
		{"missing edition", "key.pem", "", "Test", "+1y", true},
		{"missing org", "key.pem", "pro", "", "+1y", true},
		{"missing expires", "key.pem", "pro", "Test", "", true},
		{"all missing", "", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateRequired(tt.key, tt.edition, tt.org, tt.expires)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRequired() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestLoadPrivateKey pins loadPrivateKey accepts a P-256 PKCS#8 PEM and
// rejects — with a clear error, never a panic — a well-formed PKCS#8 PEM of the wrong
// type (Ed25519, P-384) or a non-PEM file, so gentoken never signs with a wrong curve.
func TestLoadPrivateKey(t *testing.T) {
	t.Parallel()

	writePEM := func(t *testing.T, typ string, der []byte) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "key.pem")
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	pkcs8 := func(t *testing.T, key any) []byte {
		t.Helper()
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}

	p256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid P-256", writePEM(t, "PRIVATE KEY", pkcs8(t, p256)), false},
		{"P-384 wrong curve", writePEM(t, "PRIVATE KEY", pkcs8(t, p384)), true},
		{"Ed25519 wrong type", writePEM(t, "PRIVATE KEY", pkcs8(t, edPriv)), true},
		{"not PEM", func() string {
			p := filepath.Join(t.TempDir(), "x.pem")
			_ = os.WriteFile(p, []byte("not a pem"), 0o600)
			return p
		}(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := loadPrivateKey(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadPrivateKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
