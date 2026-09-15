package platform

import (
	"strings"
	"testing"
	"time"
)

func TestBaseConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  BaseConfig
		wantErr bool
	}{
		{
			name:    "valid defaults",
			config:  BaseConfig{LogLevel: "info", LogFormat: "json", Environment: "local"},
			wantErr: false,
		},
		{
			name:    "valid dev environment",
			config:  BaseConfig{LogLevel: "debug", LogFormat: "pretty", Environment: "dev"},
			wantErr: false,
		},
		{
			name:    "valid prod environment",
			config:  BaseConfig{LogLevel: "warn", LogFormat: "json", Environment: "prod"},
			wantErr: false,
		},
		{
			name:    "valid fatal log level",
			config:  BaseConfig{LogLevel: "fatal", LogFormat: "json", Environment: "local"},
			wantErr: false,
		},
		{
			name:    "valid custom environment",
			config:  BaseConfig{LogLevel: "error", LogFormat: "json", Environment: "custom-env"},
			wantErr: false,
		},
		{
			name:    "invalid log format text",
			config:  BaseConfig{LogLevel: "info", LogFormat: "text", Environment: "local"},
			wantErr: true,
		},
		{
			name:    "invalid log level",
			config:  BaseConfig{LogLevel: "invalid", LogFormat: "json", Environment: "local"},
			wantErr: true,
		},
		{
			name:    "invalid log format",
			config:  BaseConfig{LogLevel: "info", LogFormat: "xml", Environment: "local"},
			wantErr: true,
		},
		{
			name:    "empty environment",
			config:  BaseConfig{LogLevel: "info", LogFormat: "json", Environment: ""},
			wantErr: true,
		},
		{
			name:    "whitespace-only environment",
			config:  BaseConfig{LogLevel: "info", LogFormat: "json", Environment: "   "},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("BaseConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProvisioningClientConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        ProvisioningClientConfig
		wantErr       bool
		errorContains string
	}{
		{
			name: "valid defaults",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
			},
			wantErr: false,
		},
		{
			name: "empty addr",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
			},
			wantErr:       true,
			errorContains: "PROVISIONING_GRPC_ADDR",
		},
		{
			name: "delay too small",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 50 * time.Millisecond, GRPCReconnectMaxDelay: 30 * time.Second},
			},
			wantErr:       true,
			errorContains: "PROVISIONING_GRPC_RECONNECT_DELAY",
		},
		{
			name: "delay exactly 100ms",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 100 * time.Millisecond, GRPCReconnectMaxDelay: 30 * time.Second},
			},
			wantErr: false,
		},
		{
			name: "max delay less than delay",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 5 * time.Second, GRPCReconnectMaxDelay: time.Second},
			},
			wantErr:       true,
			errorContains: "PROVISIONING_GRPC_RECONNECT_MAX_DELAY",
		},
		{
			name: "equal delay values",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 5 * time.Second, GRPCReconnectMaxDelay: 5 * time.Second},
			},
			wantErr: false,
		},
		{
			name: "max delay above 5m ceiling",
			config: ProvisioningClientConfig{
				ProvisioningGRPCAddr: "localhost:9090",
				GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: time.Second, GRPCReconnectMaxDelay: 6 * time.Minute},
			},
			wantErr:       true,
			errorContains: "PROVISIONING_GRPC_RECONNECT_MAX_DELAY must be <= 5m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ProvisioningClientConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errorContains != "" && err != nil && !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error should contain %q, got: %v", tt.errorContains, err)
			}
		})
	}
}

func TestHTTPTimeoutConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        HTTPTimeoutConfig
		wantErr       bool
		errorContains string
	}{
		{
			name:    "valid defaults",
			config:  HTTPTimeoutConfig{HTTPReadTimeout: 15 * time.Second, HTTPWriteTimeout: 15 * time.Second, HTTPIdleTimeout: 60 * time.Second},
			wantErr: false,
		},
		{
			name:          "read timeout zero",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 0, HTTPWriteTimeout: 15 * time.Second, HTTPIdleTimeout: 60 * time.Second},
			wantErr:       true,
			errorContains: "HTTP_READ_TIMEOUT",
		},
		{
			name:          "read timeout too large",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 121 * time.Second, HTTPWriteTimeout: 15 * time.Second, HTTPIdleTimeout: 60 * time.Second},
			wantErr:       true,
			errorContains: "HTTP_READ_TIMEOUT",
		},
		{
			name:          "write timeout zero",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 15 * time.Second, HTTPWriteTimeout: 0, HTTPIdleTimeout: 60 * time.Second},
			wantErr:       true,
			errorContains: "HTTP_WRITE_TIMEOUT",
		},
		{
			name:          "write timeout too large",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 15 * time.Second, HTTPWriteTimeout: 121 * time.Second, HTTPIdleTimeout: 60 * time.Second},
			wantErr:       true,
			errorContains: "HTTP_WRITE_TIMEOUT",
		},
		{
			name:          "idle timeout zero",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 15 * time.Second, HTTPWriteTimeout: 15 * time.Second, HTTPIdleTimeout: 0},
			wantErr:       true,
			errorContains: "HTTP_IDLE_TIMEOUT",
		},
		{
			name:          "idle timeout too large",
			config:        HTTPTimeoutConfig{HTTPReadTimeout: 15 * time.Second, HTTPWriteTimeout: 15 * time.Second, HTTPIdleTimeout: 301 * time.Second},
			wantErr:       true,
			errorContains: "HTTP_IDLE_TIMEOUT",
		},
		{
			name:    "boundary min values",
			config:  HTTPTimeoutConfig{HTTPReadTimeout: time.Second, HTTPWriteTimeout: time.Second, HTTPIdleTimeout: time.Second},
			wantErr: false,
		},
		{
			name:    "boundary max values",
			config:  HTTPTimeoutConfig{HTTPReadTimeout: 120 * time.Second, HTTPWriteTimeout: 120 * time.Second, HTTPIdleTimeout: 300 * time.Second},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPTimeoutConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errorContains != "" && err != nil && !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error should contain %q, got: %v", tt.errorContains, err)
			}
		})
	}
}

func TestKafkaNamespaceConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        KafkaNamespaceConfig
		wantErr       bool
		errorContains string
	}{
		{
			name:    "empty namespace passes allowlist (non-empty enforced per service)",
			config:  KafkaNamespaceConfig{ValidNamespaces: "local,dev,stag,prod"},
			wantErr: false,
		},
		{
			name:    "valid namespace in allowlist",
			config:  KafkaNamespaceConfig{ValidNamespaces: "local,dev,stag,prod", KafkaTopicNamespace: "dev"},
			wantErr: false,
		},
		{
			name:          "empty valid namespaces",
			config:        KafkaNamespaceConfig{ValidNamespaces: ""},
			wantErr:       true,
			errorContains: "VALID_NAMESPACES",
		},
		{
			name:          "namespace not in valid set",
			config:        KafkaNamespaceConfig{ValidNamespaces: "local,dev", KafkaTopicNamespace: "prod"},
			wantErr:       true,
			errorContains: "KAFKA_TOPIC_NAMESPACE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("KafkaNamespaceConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errorContains != "" && err != nil && !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error should contain %q, got: %v", tt.errorContains, err)
			}
		})
	}
}

// TestKafkaNamespaceConfig_Normalize covers the trim+lowercase canonicalization that replaces
// the deleted kafka.ResolveNamespace normalizer (migrated cases).
func TestKafkaNamespaceConfig_Normalize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"already canonical", "prod", "prod"},
		{"uppercase", "PROD", "prod"},
		{"whitespace", "  dev  ", "dev"},
		{"mixed", " StaG ", "stag"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := KafkaNamespaceConfig{KafkaTopicNamespace: tt.input}
			c.Normalize()
			if c.KafkaTopicNamespace != tt.want {
				t.Errorf("Normalize(%q) = %q, want %q", tt.input, c.KafkaTopicNamespace, tt.want)
			}
		})
	}
}

func TestMessageBackendConfig_Validate_SASL(t *testing.T) {
	t.Parallel()

	mkCfg := func(conn KafkaConnectionConfig) MessageBackendConfig {
		conn.KafkaBrokers = "localhost:19092"
		return MessageBackendConfig{MessageBackend: MessageBackendKafka, KafkaConnectionConfig: conn}
	}

	tests := []struct {
		name          string
		cfg           MessageBackendConfig
		wantErr       bool
		errorContains string
	}{
		{
			name: "invalid mechanism",
			cfg: mkCfg(KafkaConnectionConfig{
				KafkaSASLEnabled:   true,
				KafkaSASLMechanism: "oauthbearer", // plain is now valid; use a still-invalid mechanism
				KafkaSASLUsername:  "user",
				KafkaSASLPassword:  "pass",
			}),
			wantErr:       true,
			errorContains: "KAFKA_SASL_MECHANISM",
		},
		{
			name: "missing username",
			cfg: mkCfg(KafkaConnectionConfig{
				KafkaSASLEnabled:   true,
				KafkaSASLMechanism: "scram-sha-256",
				KafkaSASLUsername:  "",
				KafkaSASLPassword:  "pass",
			}),
			wantErr:       true,
			errorContains: "KAFKA_SASL_USERNAME",
		},
		{
			name: "missing password",
			cfg: mkCfg(KafkaConnectionConfig{
				KafkaSASLEnabled:   true,
				KafkaSASLMechanism: "scram-sha-256",
				KafkaSASLUsername:  "user",
				KafkaSASLPassword:  "",
			}),
			wantErr:       true,
			errorContains: "KAFKA_SASL_PASSWORD",
		},
		{
			name: "all valid (plain)",
			cfg: mkCfg(KafkaConnectionConfig{
				KafkaSASLEnabled:   true,
				KafkaSASLMechanism: "plain",
				KafkaSASLUsername:  "user",
				KafkaSASLPassword:  "pass",
			}),
			wantErr: false,
		},
		{
			name: "all valid (scram-512)",
			cfg: mkCfg(KafkaConnectionConfig{
				KafkaSASLEnabled:   true,
				KafkaSASLMechanism: "scram-sha-512",
				KafkaSASLUsername:  "user",
				KafkaSASLPassword:  "pass",
			}),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && tt.errorContains != "" && err != nil && !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error must contain %q, got: %v", tt.errorContains, err)
			}
		})
	}
}

func TestMessageBackendConfig_Validate_TLSCAPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		caPath    string
		wantErr   bool
		wantSubst string
	}{
		{
			name:    "TLS enabled, no CA path — no error",
			caPath:  "",
			wantErr: false,
		},
		{
			name:      "TLS enabled, nonexistent CA path — startup failure",
			caPath:    "/nonexistent/ca.pem",
			wantErr:   true,
			wantSubst: "KAFKA_TLS_CA_PATH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := MessageBackendConfig{
				MessageBackend: MessageBackendKafka,
				KafkaConnectionConfig: KafkaConnectionConfig{
					KafkaBrokers:    "localhost:19092",
					KafkaTLSEnabled: true,
					KafkaTLSCAPath:  tt.caPath,
				},
			}
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.wantSubst) {
				t.Errorf("error must contain %q, got: %v", tt.wantSubst, err)
			}
		})
	}
}

func TestMessageBackendConfig_Validate_NATSRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		backend string
	}{
		{"nats rejected", "nats"},
		{"unknown rejected", "unknown_value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := MessageBackendConfig{MessageBackend: tt.backend, KafkaConnectionConfig: KafkaConnectionConfig{KafkaBrokers: "localhost:19092"}}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for MESSAGE_BACKEND=%q, got nil", tt.backend)
			}
			if !strings.Contains(err.Error(), "(valid: direct, kafka)") {
				t.Errorf("error must list valid backends as '(valid: direct, kafka)', got: %v", err)
			}
		})
	}
}
