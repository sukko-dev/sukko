package kafka

import (
	"os"
	"strings"
	"testing"
)

func TestBuildKgoOpts(t *testing.T) {
	t.Parallel()

	brokers := []string{"localhost:9092"}

	tests := []struct {
		name    string
		sasl    *SASLConfig
		tls     *TLSConfig
		wantErr bool
	}{
		{
			name:    "no SASL, no TLS — success",
			sasl:    nil,
			tls:     nil,
			wantErr: false,
		},
		{
			name: "SCRAM-SHA-256 — success",
			sasl: &SASLConfig{Mechanism: MechanismSCRAMSHA256, Username: "user", Password: "pass"},
			tls:  nil,
		},
		{
			name: "SCRAM-SHA-512 — success",
			sasl: &SASLConfig{Mechanism: MechanismSCRAMSHA512, Username: "user", Password: "pass"},
			tls:  nil,
		},
		{
			name: "PLAIN — success",
			sasl: &SASLConfig{Mechanism: MechanismPLAIN, Username: "user", Password: "pass"},
			tls:  nil,
		},
		{
			name:    "invalid SASL mechanism — error",
			sasl:    &SASLConfig{Mechanism: "oauthbearer"},
			wantErr: true,
		},
		{
			name: "TLS enabled, no CA path — success — uses system roots",
			tls:  &TLSConfig{Enabled: true},
		},
		{
			name: "TLS disabled — success — TLS config ignored",
			tls:  &TLSConfig{Enabled: false, InsecureSkipVerify: true},
		},
		{
			name:    "TLS enabled, CA path missing — error",
			tls:     &TLSConfig{Enabled: true, CAPath: "/nonexistent/ca.pem"},
			wantErr: true,
		},
		{
			name: "TLS enabled, InsecureSkipVerify — success",
			tls:  &TLSConfig{Enabled: true, InsecureSkipVerify: true},
		},
		{
			name: "SASL and TLS together — success",
			sasl: &SASLConfig{Mechanism: MechanismSCRAMSHA256, Username: "u", Password: "p"},
			tls:  &TLSConfig{Enabled: true, InsecureSkipVerify: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts, err := BuildKgoOpts(brokers, tc.sasl, tc.tls)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) == 0 {
				t.Fatal("expected at least one opt (SeedBrokers), got none")
			}
		})
	}
}

func TestBuildTLSConfig_InvalidPEM(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp(t.TempDir(), "ca-*.pem")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("not valid PEM data"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = buildTLSConfig(&TLSConfig{Enabled: true, CAPath: f.Name()})
	if err == nil {
		t.Fatal("expected error for invalid PEM, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse CA certificate from") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
