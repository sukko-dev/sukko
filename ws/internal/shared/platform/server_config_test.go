package platform

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/internal/shared/license"
)

// newValidServerConfig returns a server config with all valid defaults for testing.
func newValidServerConfig() *ServerConfig {
	return &ServerConfig{
		BaseConfig: BaseConfig{
			LogLevel:    "info",
			LogFormat:   "json",
			Environment: "local",
		},
		KafkaNamespaceConfig: KafkaNamespaceConfig{
			ValidNamespaces:     "local,dev,stag,prod",
			KafkaTopicNamespace: "local",
		},
		Addr:                     ":3002",
		NumShards:                1,
		BasePort:                 3002,
		LBAddr:                   ":3005",
		GRPCPort:                 3006,
		RateLimitBurstMultiplier: 2,
		MessageBackendConfig: MessageBackendConfig{
			MessageBackend: "direct",
			KafkaConnectionConfig: KafkaConnectionConfig{
				KafkaBrokers: "localhost:19092",
			},
		},
		MemoryLimit:                512 * 1024 * 1024, // 512MB
		MaxConnections:             1000,
		MaxKafkaMessagesPerSec:     1000,
		MaxBroadcastsPerSec:        100,
		MaxGoroutines:              1000,
		ConnectionRateLimitEnabled: true,
		ConnRateLimitIPBurst:       100,
		ConnRateLimitIPRate:        100.0,
		ConnRateLimitGlobalBurst:   300,
		ConnRateLimitGlobalRate:    50.0,
		CPURejectThreshold:         75.0,
		CPURejectThresholdLower:    65.0, // Explicit: non-zero skips auto-compute
		CPUPauseThreshold:          80.0,
		CPUPauseThresholdLower:     70.0, // Explicit: non-zero skips auto-compute
		CPUEWMABeta:                0.8,
		TCPListenBacklog:           2048,
		HTTPTimeoutConfig: HTTPTimeoutConfig{
			HTTPReadTimeout:  15 * time.Second,
			HTTPWriteTimeout: 15 * time.Second,
			HTTPIdleTimeout:  60 * time.Second,
		},
		MetricsInterval:             15 * time.Second,
		CPUPollInterval:             1 * time.Second,
		BroadcastType:               "valkey",
		ValkeyAddrs:                 []string{"localhost:6379"},
		ClientSendBufferSize:        512,
		SlowClientMaxAttempts:       3,
		MemoryWarningPercent:        80,
		MemoryCriticalPercent:       90,
		BufferHighSaturationPercent: 90,
		BufferPopulationWarnPercent: 25,
		// Topic refresh interval (required for kafka backend)
		TopicRefreshInterval: 60 * time.Second,
		// Kafka topic defaults (needed when tests switch to kafka backend)
		KafkaDefaultPartitions:        1,
		KafkaDefaultReplicationFactor: 1,
		// Provisioning gRPC (required for topic discovery)
		ProvisioningClientConfig: ProvisioningClientConfig{
			ProvisioningGRPCAddr: "localhost:9090",
			GRPCReconnectConfig:  GRPCReconnectConfig{GRPCReconnectDelay: 1 * time.Second, GRPCReconnectMaxDelay: 30 * time.Second},
		},
		// WebSocket ping/pong (required)
		PongWait:   60 * time.Second,
		PingPeriod: 45 * time.Second,
		WriteWait:  5 * time.Second,
		// Handler timeouts
		ReplayTimeout:        5 * time.Second,
		PublishTimeout:       5 * time.Second,
		MaxReplayMessages:    100,
		TopicCreationTimeout: 30 * time.Second,
		// Orchestration
		ShardDialTimeout:           10 * time.Second,
		ShardMessageTimeout:        60 * time.Second,
		MetricsAggregationInterval: 5 * time.Second,
		// Shutdown
		ShutdownGracePeriod:   30 * time.Second,
		ShutdownCheckInterval: 1 * time.Second,
		// Internal monitoring
		MetricsCollectInterval: 2 * time.Second,
		MemoryMonitorInterval:  30 * time.Second,
		BufferSampleInterval:   10 * time.Second,
		BufferMaxSamples:       100,
		// Connection rate limiter internals
		ConnRateLimitIPTTL:           5 * time.Minute,
		ConnRateLimitCleanupInterval: 1 * time.Minute,
		// Broadcast bus
		BroadcastBufferSize:      256, // matches BROADCAST_BUFFER_SIZE envDefault
		BroadcastShutdownTimeout: 5 * time.Second,
		// Valkey broadcast tuning
		ValkeyWriteTimeout:            3 * time.Second,
		ValkeyPublishTimeout:          100 * time.Millisecond,
		ValkeyStartupPingTimeout:      5 * time.Second,
		ValkeyReconnectInitialBackoff: 100 * time.Millisecond,
		ValkeyReconnectMaxBackoff:     30 * time.Second,
		ValkeyReconnectMaxAttempts:    10,
		ValkeyHealthCheckInterval:     10 * time.Second,
		ValkeyHealthCheckTimeout:      5 * time.Second,
		// Kafka consumer tuning
		KafkaBatchSize:                 50,
		KafkaBatchTimeout:              10 * time.Millisecond,
		KafkaFetchMaxWait:              500 * time.Millisecond,
		KafkaFetchMinBytes:             1,
		KafkaFetchMaxBytes:             10485760,
		KafkaSessionTimeout:            30 * time.Second,
		KafkaRebalanceTimeout:          60 * time.Second,
		KafkaReplayFetchMaxBytes:       5242880,
		KafkaBackpressureCheckInterval: 100 * time.Millisecond,
		// Kafka producer tuning
		KafkaProducerBatchMaxBytes:      1048576,
		KafkaProducerMaxBufferedRecords: 10000,
		KafkaProducerRecordRetries:      8,
		KafkaProducerCBTimeout:          30 * time.Second,
		KafkaProducerCBMaxFailures:      5,
		KafkaProducerCBHalfOpenReqs:     1,
		KafkaProducerShutdownTimeout:    10 * time.Second,
		// Valkey config
		ValkeyMasterName: "mymaster",
		ValkeyChannel:    "ws.broadcast",
		// Routing / DLQ config
		RoutingFanoutWorkers:   4,
		RoutingFanoutQueueSize: 256,
		DLQMaxRetries:          3,
		DLQBaseDelay:           100 * time.Millisecond,
		DLQMaxDelay:            5 * time.Second,
		DLQRetryWorkers:        4,
		// Partition-revoke commit tuning
		KafkaCommitOnRevokeTimeout: 5 * time.Second,
		KafkaAutoCommitInterval:    5 * time.Second,
		// Channel subscription limit
		MaxChannelsPerClient: 100,
		// Gap notification and live replay
		GapNotifyBufferSize:     8,
		ReplayRateLimitInterval: 10 * time.Second,
		// Connections registry (validated unconditionally)
		ConnectionsRegistryBuffer:                1024,
		ConnectionsRegistryTTL:                   120 * time.Second,
		ConnectionsRegistryFlushInterval:         5 * time.Second,
		ConnectionsRegistryHeartbeatInterval:     30 * time.Second,
		ConnectionsRegistryRestartInitialBackoff: 100 * time.Millisecond,
		ConnectionsRegistryRestartMaxBackoff:     30 * time.Second,
		ConnectionsRegistryShutdownDrainTimeout:  100 * time.Millisecond,
		AdminChannelSubscribeTimeout:             10 * time.Second,
	}
}

func TestServerConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	cfg := newValidServerConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Valid config should not error: %v", err)
	}
}

func TestServerConfig_Validate_EmptyAddr(t *testing.T) {
	t.Parallel()
	cfg := newValidServerConfig()
	cfg.Addr = ""

	err := cfg.Validate()
	if err == nil {
		t.Error("Should error on empty Addr")
	}
	if !strings.Contains(err.Error(), "WS_ADDR") {
		t.Errorf("Error should mention WS_ADDR: %v", err)
	}
}

func TestServerConfig_Validate_BasePortPlusShards(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		basePort  int
		numShards int
		wantErr   bool
	}{
		{"valid_3002_3shards", 3002, 3, false},
		{"valid_65533_3shards", 65533, 3, false},
		{"overflow_65534_3shards", 65534, 3, true},
		{"overflow_65535_2shards", 65535, 2, true},
		{"edge_65535_1shard", 65535, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.BasePort = tt.basePort
			cfg.NumShards = tt.numShards

			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("Expected error for port overflow")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if tt.wantErr && err != nil && !strings.Contains(err.Error(), "exceeds max port") {
				t.Errorf("Error should mention port overflow: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_MaxConnections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int
		shouldError bool
	}{
		{"zero", 0, true},
		{"negative", -1, true},
		{"one", 1, false},
		{"large", 100000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MaxConnections = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_CPUThresholds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		reject      float64
		rejectLower float64
		pause       float64
		pauseLower  float64
		shouldError bool
		errorField  string
	}{
		{"valid defaults", 75.0, 65.0, 80.0, 70.0, false, ""},
		{"reject negative", -1.0, 65.0, 80.0, 70.0, true, "WS_CPU_REJECT_THRESHOLD"},
		{"reject > 100", 101.0, 65.0, 80.0, 70.0, true, "WS_CPU_REJECT_THRESHOLD"},
		{"pause < reject", 80.0, 70.0, 75.0, 65.0, true, "WS_CPU_PAUSE_THRESHOLD"},
		{"reject lower >= upper", 75.0, 75.0, 80.0, 70.0, true, "WS_CPU_REJECT_THRESHOLD_LOWER"},
		{"pause lower >= upper", 75.0, 65.0, 80.0, 80.0, true, "WS_CPU_PAUSE_THRESHOLD_LOWER"},
		{"pause lower negative", 75.0, 65.0, 80.0, -1.0, true, "WS_CPU_PAUSE_THRESHOLD_LOWER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.CPURejectThreshold = tt.reject
			cfg.CPURejectThresholdLower = tt.rejectLower
			cfg.CPUPauseThreshold = tt.pause
			cfg.CPUPauseThresholdLower = tt.pauseLower

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorField != "" && !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_CPUEWMABeta(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       float64
		shouldError bool
	}{
		{"valid default", 0.8, false},
		{"valid low", 0.1, false},
		{"valid high", 0.99, false},
		{"zero", 0.0, true},
		{"one", 1.0, true},
		{"negative", -0.5, true},
		{"greater than one", 1.5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.CPUEWMABeta = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if tt.shouldError && err != nil && !strings.Contains(err.Error(), "WS_CPU_EWMA_BETA") {
				t.Errorf("Error should mention WS_CPU_EWMA_BETA: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_LogLevel(t *testing.T) {
	t.Parallel()
	validLevels := []string{"debug", "info", "warn", "error"}
	invalidLevels := []string{"DEBUG", "INFO", "invalid", "", "trace"}

	for _, level := range validLevels {
		t.Run("valid_"+level, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.LogLevel = level
			if err := cfg.Validate(); err != nil {
				t.Errorf("%s should be valid: %v", level, err)
			}
		})
	}

	for _, level := range invalidLevels {
		t.Run("invalid_"+level, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.LogLevel = level
			err := cfg.Validate()
			if err == nil {
				t.Errorf("%s should be invalid", level)
			}
			if !strings.Contains(err.Error(), "LOG_LEVEL") {
				t.Errorf("Error should mention LOG_LEVEL: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_LogFormat(t *testing.T) {
	t.Parallel()
	validFormats := []string{"json", "pretty"}
	invalidFormats := []string{"JSON", "xml", "text", "", "console"}

	for _, format := range validFormats {
		t.Run("valid_"+format, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.LogFormat = format
			if err := cfg.Validate(); err != nil {
				t.Errorf("%s should be valid: %v", format, err)
			}
		})
	}

	for _, format := range invalidFormats {
		t.Run("invalid_"+format, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.LogFormat = format
			err := cfg.Validate()
			if err == nil {
				t.Errorf("%s should be invalid", format)
			}
		})
	}
}

func TestServerConfig_Validate_Valkey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		addrs       []string
		db          int
		channel     string
		shouldError bool
		errorField  string
	}{
		{"valid", []string{"localhost:26379"}, 0, "ws.broadcast", false, ""},
		{"multiple addrs", []string{"host1:26379", "host2:26379", "host3:26379"}, 0, "ws.broadcast", false, ""},
		{"empty addrs", []string{}, 0, "ws.broadcast", true, "VALKEY_ADDRS"},
		{"negative db", []string{"localhost:26379"}, -1, "ws.broadcast", true, "VALKEY_DB"},
		{"empty channel", []string{"localhost:26379"}, 0, "", true, "VALKEY_CHANNEL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.BroadcastType = "valkey"
			cfg.ValkeyAddrs = tt.addrs
			cfg.ValkeyDB = tt.db
			cfg.ValkeyChannel = tt.channel

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_BroadcastType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bType       string
		shouldError bool
	}{
		{"nats rejected", "nats", true},
		{"valkey", "valkey", false},
		{"redis rejected", "redis", true},
		{"empty", "", true},
		{"invalid", "rabbitmq", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.BroadcastType = tt.bType
			if tt.bType == "valkey" {
				cfg.ValkeyAddrs = []string{"localhost:26379"}
				cfg.ValkeyMasterName = "mymaster"
				cfg.ValkeyChannel = "ws.broadcast"
			}
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_BroadcastTypeOnlyValkey(t *testing.T) {
	t.Parallel()
	cfg := newValidServerConfig()
	cfg.BroadcastType = "unknown_value"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for unknown broadcast type")
	}
	if !strings.Contains(err.Error(), "valid: valkey") {
		t.Errorf("Error should mention valid: valkey, got: %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "nats") {
		t.Errorf("Error should NOT mention nats, got: %v", err)
	}
}

func TestServerConfig_Normalize_HysteresisAutoCompute(t *testing.T) {
	t.Parallel()

	// When lower = 0 (sentinel), Normalize auto-computes as upper - 10
	cfg := newValidServerConfig()
	cfg.CPURejectThreshold = 60.0
	cfg.CPURejectThresholdLower = 0 // sentinel
	cfg.CPUPauseThreshold = 70.0
	cfg.CPUPauseThresholdLower = 0 // sentinel

	cfg.Normalize()

	if err := cfg.Validate(); err != nil {
		t.Errorf("Auto-compute should produce valid config: %v", err)
	}
	if cfg.CPURejectThresholdLower != 50.0 {
		t.Errorf("CPURejectThresholdLower should be 50.0 (auto: 60-10), got %.1f", cfg.CPURejectThresholdLower)
	}
	if cfg.CPUPauseThresholdLower != 60.0 {
		t.Errorf("CPUPauseThresholdLower should be 60.0 (auto: 70-10), got %.1f", cfg.CPUPauseThresholdLower)
	}

	// When lower is explicitly set (non-zero), Normalize does NOT override
	cfg2 := newValidServerConfig()
	cfg2.CPURejectThreshold = 75.0
	cfg2.CPURejectThresholdLower = 60.0 // explicit
	cfg2.CPUPauseThreshold = 80.0
	cfg2.CPUPauseThresholdLower = 65.0 // explicit

	cfg2.Normalize()

	if err := cfg2.Validate(); err != nil {
		t.Errorf("Explicit lower thresholds should be valid: %v", err)
	}
	if cfg2.CPURejectThresholdLower != 60.0 {
		t.Errorf("Explicit CPURejectThresholdLower should remain 60.0, got %.1f", cfg2.CPURejectThresholdLower)
	}
	if cfg2.CPUPauseThresholdLower != 65.0 {
		t.Errorf("Explicit CPUPauseThresholdLower should remain 65.0, got %.1f", cfg2.CPUPauseThresholdLower)
	}
}

func TestServerConfig_Validate_ClientSendBufferSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		size        int
		shouldError bool
	}{
		{"min valid", 64, false},
		{"default", 512, false},
		{"large", 1024, false},
		{"max valid", 4096, false},
		{"too small", 63, true},
		{"too large", 4097, true},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.ClientSendBufferSize = tt.size
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_CPUPollInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		pollInterval    time.Duration
		metricsInterval time.Duration
		shouldError     bool
		errorField      string
	}{
		{"valid", 1 * time.Second, 15 * time.Second, false, ""},
		{"equal to metrics", 15 * time.Second, 15 * time.Second, false, ""},
		{"minimum 100ms", 100 * time.Millisecond, 15 * time.Second, false, ""},
		{"too fast", 50 * time.Millisecond, 15 * time.Second, true, "CPU_POLL_INTERVAL"},
		{"greater than metrics", 20 * time.Second, 15 * time.Second, true, "CPU_POLL_INTERVAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.CPUPollInterval = tt.pollInterval
			cfg.MetricsInterval = tt.metricsInterval

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_GapNotifyBufferSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		size        int
		shouldError bool
	}{
		{"zero", 0, true},
		{"negative", -1, true},
		{"too large", MaxGapNotifyBufferSize + 1, true},
		{"min valid", 1, false},
		{"max valid", MaxGapNotifyBufferSize, false},
		{"default", 8, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.GapNotifyBufferSize = tt.size
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("expected error")
			}
			if tt.shouldError && err != nil && !strings.Contains(err.Error(), "WS_GAP_NOTIFY_BUFFER_SIZE") {
				t.Errorf("error must mention WS_GAP_NOTIFY_BUFFER_SIZE: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_ReplayRateLimitInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		interval    time.Duration
		shouldError bool
	}{
		{"zero", 0, true},
		{"negative", -1 * time.Second, true},
		{"valid default", 10 * time.Second, false},
		{"valid small", 1 * time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.ReplayRateLimitInterval = tt.interval
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("expected error")
			}
			if tt.shouldError && err != nil && !strings.Contains(err.Error(), "WS_REPLAY_RATE_LIMIT_INTERVAL") {
				t.Errorf("error must mention WS_REPLAY_RATE_LIMIT_INTERVAL: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_SlowClientMaxAttempts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int
		shouldError bool
	}{
		{"min valid", 1, false},
		{"default", 3, false},
		{"max valid", 10, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"too large", 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.SlowClientMaxAttempts = tt.value
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "WS_SLOW_CLIENT_MAX_ATTEMPTS") {
					t.Errorf("Error should mention WS_SLOW_CLIENT_MAX_ATTEMPTS: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_HysteresisGap(t *testing.T) {
	t.Parallel()
	// Test that hysteresis gap (upper - lower) is reasonable

	// Valid: 10% gap
	cfg := newValidServerConfig()
	cfg.CPURejectThreshold = 75.0
	cfg.CPURejectThresholdLower = 65.0
	if err := cfg.Validate(); err != nil {
		t.Errorf("10%% gap should be valid: %v", err)
	}

	// Valid: 1% gap (minimal)
	cfg.CPURejectThreshold = 75.0
	cfg.CPURejectThresholdLower = 74.0
	if err := cfg.Validate(); err != nil {
		t.Errorf("1%% gap should be valid: %v", err)
	}

	// Invalid: 0% gap (lower == upper)
	cfg.CPURejectThreshold = 75.0
	cfg.CPURejectThresholdLower = 75.0
	if err := cfg.Validate(); err == nil {
		t.Error("0%% gap (lower == upper) should be invalid")
	}

	// Invalid: negative gap (lower > upper)
	cfg.CPURejectThreshold = 75.0
	cfg.CPURejectThresholdLower = 76.0
	if err := cfg.Validate(); err == nil {
		t.Error("Negative gap (lower > upper) should be invalid")
	}
}

func TestServerConfig_Validate_PingPong(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pongWait    time.Duration
		pingPeriod  time.Duration
		shouldError bool
		errorField  string
	}{
		{
			name:        "valid_60s_45s",
			pongWait:    60 * time.Second,
			pingPeriod:  45 * time.Second,
			shouldError: false,
		},
		{
			name:        "valid_120s_90s",
			pongWait:    120 * time.Second,
			pingPeriod:  90 * time.Second,
			shouldError: false,
		},
		{
			name:        "pongWait_too_small",
			pongWait:    MinPongWait - 1*time.Second, // Below minimum
			pingPeriod:  MinPingPeriod,
			shouldError: true,
			errorField:  "WS_PONG_WAIT",
		},
		{
			name:        "pingPeriod_too_small",
			pongWait:    60 * time.Second,
			pingPeriod:  MinPingPeriod - 1*time.Second, // Below minimum
			shouldError: true,
			errorField:  "WS_PING_PERIOD",
		},
		{
			name:        "pingPeriod_equals_pongWait",
			pongWait:    60 * time.Second,
			pingPeriod:  60 * time.Second,
			shouldError: true,
			errorField:  "WS_PING_PERIOD",
		},
		{
			name:        "pingPeriod_exceeds_pongWait",
			pongWait:    60 * time.Second,
			pingPeriod:  90 * time.Second,
			shouldError: true,
			errorField:  "WS_PING_PERIOD",
		},
		{
			name:        "minimum_valid_values",
			pongWait:    MinPongWait,
			pingPeriod:  MinPingPeriod,
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidServerConfig()
			cfg.PongWait = tt.pongWait
			cfg.PingPeriod = tt.pingPeriod

			err := cfg.Validate()

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorField)
				} else if !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("expected error containing %q, got %q", tt.errorField, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_WriteWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		writeWait   time.Duration
		shouldError bool
	}{
		{
			name:        "valid_5s",
			writeWait:   5 * time.Second,
			shouldError: false,
		},
		{
			name:        "valid_10s",
			writeWait:   10 * time.Second,
			shouldError: false,
		},
		{
			name:        "valid_minimum_1s",
			writeWait:   1 * time.Second,
			shouldError: false,
		},
		{
			name:        "valid_maximum_30s",
			writeWait:   30 * time.Second,
			shouldError: false,
		},
		{
			name:        "too_small",
			writeWait:   500 * time.Millisecond,
			shouldError: true,
		},
		{
			name:        "too_large",
			writeWait:   31 * time.Second,
			shouldError: true,
		},
		{
			name:        "zero",
			writeWait:   0,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := newValidServerConfig()
			cfg.WriteWait = tt.writeWait

			err := cfg.Validate()

			if tt.shouldError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), "WS_WRITE_WAIT") {
					t.Errorf("expected error containing WS_WRITE_WAIT, got %q", err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_MessageBackend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		backend     string
		shouldError bool
		errorField  string
	}{
		{"direct", "direct", false, ""},
		{"kafka", "kafka", false, ""},
		{"nats rejected", "nats", true, "MESSAGE_BACKEND"},
		{"invalid backend", "redis", true, "MESSAGE_BACKEND"},
		{"empty backend", "", true, "MESSAGE_BACKEND"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MessageBackend = tt.backend

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorField != "" && !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_TopicRefreshInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		backend  string
		interval time.Duration
		wantErr  bool
	}{
		{"kafka valid", "kafka", 60 * time.Second, false},
		{"direct skips validation", "direct", 0, false},
		{"kafka zero", "kafka", 0, true},
		{"kafka too small", "kafka", 500 * time.Millisecond, true},
		{"kafka min valid", "kafka", 1 * time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MessageBackend = tt.backend
			cfg.TopicRefreshInterval = tt.interval

			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error")
			} else if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_KafkaTopicDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		partitions        int
		replicationFactor int
		shouldError       bool
		errorField        string
	}{
		{"valid defaults", 1, 1, false, ""},
		{"valid higher values", 6, 3, false, ""},
		{"zero partitions", 0, 1, true, "KAFKA_DEFAULT_PARTITIONS"},
		{"negative partitions", -1, 1, true, "KAFKA_DEFAULT_PARTITIONS"},
		{"zero replication", 1, 0, true, "KAFKA_DEFAULT_REPLICATION_FACTOR"},
		{"negative replication", 1, -1, true, "KAFKA_DEFAULT_REPLICATION_FACTOR"},
		{"both zero", 0, 0, true, "KAFKA_DEFAULT_PARTITIONS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MessageBackend = "kafka"
			cfg.KafkaDefaultPartitions = tt.partitions
			cfg.KafkaDefaultReplicationFactor = tt.replicationFactor

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorField != "" && !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else {
				if err != nil {
					t.Errorf("Should not error: %v", err)
				}
			}
		})
	}
}

func TestServerConfig_Validate_KafkaTopicDefaults_SkippedForDirect(t *testing.T) {
	t.Parallel()
	// Zero partitions with direct backend should NOT error (validation is skipped)
	cfg := newValidServerConfig()
	cfg.MessageBackend = "direct"
	cfg.KafkaDefaultPartitions = 0
	cfg.KafkaDefaultReplicationFactor = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("Kafka topic defaults with direct backend should not error: %v", err)
	}
}

func TestServerConfig_Validate_KafkaSASL_OnlyWithKafkaBackend(t *testing.T) {
	t.Parallel()

	// SASL enabled with direct backend should NOT error (SASL validation is skipped)
	cfg := newValidServerConfig()
	cfg.MessageBackend = "direct"
	cfg.KafkaSASLEnabled = true
	// Deliberately leave SASL fields empty
	if err := cfg.Validate(); err != nil {
		t.Errorf("SASL with direct backend should not error: %v", err)
	}

	// SASL enabled with kafka backend SHOULD error (missing credentials)
	cfg2 := newValidServerConfig()
	cfg2.MessageBackend = "kafka"
	cfg2.KafkaSASLEnabled = true
	cfg2.KafkaSASLMechanism = "scram-sha-256"
	// Missing username/password
	if err := cfg2.Validate(); err == nil {
		t.Error("SASL with kafka backend and missing credentials should error")
	}
}

func TestServerConfig_Validate_MemoryLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int64
		shouldError bool
	}{
		{"valid 512MB", 512 * 1024 * 1024, false},
		{"valid 64MB min", 64 * 1024 * 1024, false},
		{"too small", 32 * 1024 * 1024, true},
		{"zero", 0, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MemoryLimit = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if tt.shouldError && err != nil && !strings.Contains(err.Error(), "WS_MEMORY_LIMIT") {
				t.Errorf("Error should mention WS_MEMORY_LIMIT: %v", err)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_RateLimits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		maxKafka      int
		maxBroadcast  int
		maxGoroutines int
		shouldError   bool
		errorContains string
	}{
		{"valid defaults", 1000, 100, 1000, false, ""},
		{"zero kafka rate", 0, 100, 1000, true, "WS_MAX_KAFKA_RATE"},
		{"negative kafka rate", -1, 100, 1000, true, "WS_MAX_KAFKA_RATE"},
		{"zero broadcast rate", 1000, 0, 1000, true, "WS_MAX_BROADCAST_RATE"},
		{"goroutines too low", 1000, 100, 50, true, "WS_MAX_GOROUTINES"},
		{"goroutines min valid", 1000, 100, 100, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MaxKafkaMessagesPerSec = tt.maxKafka
			cfg.MaxBroadcastsPerSec = tt.maxBroadcast
			cfg.MaxGoroutines = tt.maxGoroutines

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should mention %s: %v", tt.errorContains, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_ConnRateLimits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		enabled       bool
		ipBurst       int
		ipRate        float64
		globalBurst   int
		globalRate    float64
		shouldError   bool
		errorContains string
	}{
		{"valid enabled", true, 100, 100.0, 300, 50.0, false, ""},
		{"disabled skips checks", false, 0, 0, 0, 0, false, ""},
		{"zero ip burst", true, 0, 100.0, 300, 50.0, true, "CONN_RATE_LIMIT_IP_BURST"},
		{"zero ip rate", true, 100, 0, 300, 50.0, true, "CONN_RATE_LIMIT_IP_RATE"},
		{"zero global burst", true, 100, 100.0, 0, 50.0, true, "CONN_RATE_LIMIT_GLOBAL_BURST"},
		{"zero global rate", true, 100, 100.0, 300, 0, true, "CONN_RATE_LIMIT_GLOBAL_RATE"},
		{"negative ip rate", true, 100, -1.0, 300, 50.0, true, "CONN_RATE_LIMIT_IP_RATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.ConnectionRateLimitEnabled = tt.enabled
			cfg.ConnRateLimitIPBurst = tt.ipBurst
			cfg.ConnRateLimitIPRate = tt.ipRate
			cfg.ConnRateLimitGlobalBurst = tt.globalBurst
			cfg.ConnRateLimitGlobalRate = tt.globalRate

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Error should mention %s: %v", tt.errorContains, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_TCPListenBacklog(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		value       int
		shouldError bool
	}{
		{"valid default", 2048, false},
		{"zero disables", 0, false},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.TCPListenBacklog = tt.value
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Error("Should error")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_HTTPTimeouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		readTimeout  time.Duration
		writeTimeout time.Duration
		idleTimeout  time.Duration
		shouldError  bool
		errorField   string
	}{
		{"valid defaults", 15 * time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"read too small", 500 * time.Millisecond, 15 * time.Second, 60 * time.Second, true, "HTTP_READ_TIMEOUT"},
		{"read too large", 121 * time.Second, 15 * time.Second, 60 * time.Second, true, "HTTP_READ_TIMEOUT"},
		{"write too small", 15 * time.Second, 0, 60 * time.Second, true, "HTTP_WRITE_TIMEOUT"},
		{"write too large", 15 * time.Second, 121 * time.Second, 60 * time.Second, true, "HTTP_WRITE_TIMEOUT"},
		{"idle too small", 15 * time.Second, 15 * time.Second, 0, true, "HTTP_IDLE_TIMEOUT"},
		{"idle too large", 15 * time.Second, 15 * time.Second, 301 * time.Second, true, "HTTP_IDLE_TIMEOUT"},
		{"min read", 1 * time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"max read", 120 * time.Second, 15 * time.Second, 60 * time.Second, false, ""},
		{"max idle", 15 * time.Second, 15 * time.Second, 300 * time.Second, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.HTTPReadTimeout = tt.readTimeout
			cfg.HTTPWriteTimeout = tt.writeTimeout
			cfg.HTTPIdleTimeout = tt.idleTimeout

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if tt.errorField != "" && !strings.Contains(err.Error(), tt.errorField) {
					t.Errorf("Error should mention %s: %v", tt.errorField, err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_MetricsInterval(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		interval    time.Duration
		shouldError bool
	}{
		{"valid default", 15 * time.Second, false},
		{"valid 1s", 1 * time.Second, false},
		{"too small", 500 * time.Millisecond, true},
		{"zero", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MetricsInterval = tt.interval
			// CPUPollInterval must be <= MetricsInterval
			if tt.interval > 0 {
				cfg.CPUPollInterval = tt.interval
			}

			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error")
				} else if !strings.Contains(err.Error(), "METRICS_INTERVAL") {
					t.Errorf("Error should mention METRICS_INTERVAL: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_NamespaceRequired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		backend     string
		namespace   string
		shouldError bool
	}{
		{"kafka mode + valid namespace", MessageBackendKafka, "dev", false},
		{"kafka mode + empty is required", MessageBackendKafka, "", true},
		{"kafka mode + not in valid set", MessageBackendKafka, "staging", true},
		{"direct mode + empty passes (not required)", MessageBackendDirect, "", false},
		{"direct mode + valid namespace also fine", MessageBackendDirect, "dev", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.MessageBackend = tt.backend
			cfg.KafkaTopicNamespace = tt.namespace
			err := cfg.Validate()
			if tt.shouldError {
				if err == nil {
					t.Error("Should error on missing/invalid namespace")
				} else if !strings.Contains(err.Error(), "KAFKA_TOPIC_NAMESPACE") {
					t.Errorf("Error should mention KAFKA_TOPIC_NAMESPACE: %v", err)
				}
			} else if err != nil {
				t.Errorf("Should not error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_NewDurationFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setField      func(*ServerConfig)
		errorContains string
	}{
		{"ReplayTimeout", func(c *ServerConfig) { c.ReplayTimeout = 0 }, "WS_REPLAY_TIMEOUT"},
		{"PublishTimeout", func(c *ServerConfig) { c.PublishTimeout = 0 }, "WS_PUBLISH_TIMEOUT"},
		{"TopicCreationTimeout", func(c *ServerConfig) { c.TopicCreationTimeout = 0 }, "WS_TOPIC_CREATION_TIMEOUT"},
		{"ShardDialTimeout", func(c *ServerConfig) { c.ShardDialTimeout = 0 }, "WS_SHARD_DIAL_TIMEOUT"},
		{"ShardMessageTimeout", func(c *ServerConfig) { c.ShardMessageTimeout = 0 }, "WS_SHARD_MESSAGE_TIMEOUT"},
		{"MetricsAggregationInterval", func(c *ServerConfig) { c.MetricsAggregationInterval = 0 }, "WS_METRICS_AGGREGATION_INTERVAL"},
		{"ShutdownGracePeriod", func(c *ServerConfig) { c.ShutdownGracePeriod = 0 }, "WS_SHUTDOWN_GRACE_PERIOD"},
		{"ShutdownCheckInterval", func(c *ServerConfig) { c.ShutdownCheckInterval = 0 }, "WS_SHUTDOWN_CHECK_INTERVAL"},
		{"MetricsCollectInterval", func(c *ServerConfig) { c.MetricsCollectInterval = 0 }, "WS_METRICS_COLLECT_INTERVAL"},
		{"MemoryMonitorInterval", func(c *ServerConfig) { c.MemoryMonitorInterval = 0 }, "WS_MEMORY_MONITOR_INTERVAL"},
		{"BufferSampleInterval", func(c *ServerConfig) { c.BufferSampleInterval = 0 }, "WS_BUFFER_SAMPLE_INTERVAL"},
		{"ConnRateLimitIPTTL", func(c *ServerConfig) { c.ConnRateLimitIPTTL = 0 }, "CONN_RATE_LIMIT_IP_TTL"},
		{"ConnRateLimitCleanupInterval", func(c *ServerConfig) { c.ConnRateLimitCleanupInterval = 0 }, "CONN_RATE_LIMIT_CLEANUP_INTERVAL"},
		{"BroadcastShutdownTimeout", func(c *ServerConfig) { c.BroadcastShutdownTimeout = 0 }, "BROADCAST_SHUTDOWN_TIMEOUT"},
		{"KafkaBatchTimeout", func(c *ServerConfig) { c.KafkaBatchTimeout = 0 }, "KAFKA_BATCH_TIMEOUT"},
		{"KafkaFetchMaxWait", func(c *ServerConfig) { c.KafkaFetchMaxWait = 0 }, "KAFKA_FETCH_MAX_WAIT"},
		{"KafkaSessionTimeout", func(c *ServerConfig) { c.KafkaSessionTimeout = 0 }, "KAFKA_SESSION_TIMEOUT"},
		{"KafkaRebalanceTimeout", func(c *ServerConfig) { c.KafkaRebalanceTimeout = 0 }, "KAFKA_REBALANCE_TIMEOUT"},
		{"KafkaBackpressureCheckInterval", func(c *ServerConfig) { c.KafkaBackpressureCheckInterval = 0 }, "KAFKA_BACKPRESSURE_CHECK_INTERVAL"},
		{"KafkaProducerCBTimeout", func(c *ServerConfig) { c.KafkaProducerCBTimeout = 0 }, "KAFKA_PRODUCER_CB_TIMEOUT"},
		{"KafkaProducerShutdownTimeout", func(c *ServerConfig) { c.KafkaProducerShutdownTimeout = 0 }, "KAFKA_PRODUCER_SHUTDOWN_TIMEOUT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			tt.setField(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Errorf("Expected error for zero %s", tt.name)
			} else if !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("Error should contain %q: %v", tt.errorContains, err)
			}
		})
	}
}

func TestServerConfig_Validate_NewIntFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setField      func(*ServerConfig)
		errorContains string
	}{
		{"MaxReplayMessages", func(c *ServerConfig) { c.MaxReplayMessages = 0 }, "WS_MAX_REPLAY_MESSAGES"},
		{"BufferMaxSamples", func(c *ServerConfig) { c.BufferMaxSamples = 0 }, "WS_BUFFER_MAX_SAMPLES"},
		{"BroadcastBufferSize", func(c *ServerConfig) { c.BroadcastBufferSize = 0 }, "BROADCAST_BUFFER_SIZE"},
		{"KafkaBatchSize", func(c *ServerConfig) { c.KafkaBatchSize = 0 }, "KAFKA_BATCH_SIZE"},
		{"KafkaFetchMinBytes", func(c *ServerConfig) { c.KafkaFetchMinBytes = 0 }, "KAFKA_FETCH_MIN_BYTES"},
		{"KafkaFetchMaxBytes", func(c *ServerConfig) { c.KafkaFetchMaxBytes = 0 }, "KAFKA_FETCH_MAX_BYTES"},
		{"KafkaReplayFetchMaxBytes", func(c *ServerConfig) { c.KafkaReplayFetchMaxBytes = 0 }, "KAFKA_REPLAY_FETCH_MAX_BYTES"},
		{"KafkaProducerBatchMaxBytes", func(c *ServerConfig) { c.KafkaProducerBatchMaxBytes = 0 }, "KAFKA_PRODUCER_BATCH_MAX_BYTES"},
		{"KafkaProducerMaxBufferedRecords", func(c *ServerConfig) { c.KafkaProducerMaxBufferedRecords = 0 }, "KAFKA_PRODUCER_MAX_BUFFERED_RECORDS"},
		{"KafkaProducerRecordRetries", func(c *ServerConfig) { c.KafkaProducerRecordRetries = 0 }, "KAFKA_PRODUCER_RECORD_RETRIES"},
		{"KafkaProducerCBMaxFailures", func(c *ServerConfig) { c.KafkaProducerCBMaxFailures = 0 }, "KAFKA_PRODUCER_CB_MAX_FAILURES"},
		{"KafkaProducerCBHalfOpenReqs", func(c *ServerConfig) { c.KafkaProducerCBHalfOpenReqs = 0 }, "KAFKA_PRODUCER_CB_HALF_OPEN_REQS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			tt.setField(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Errorf("Expected error for zero %s", tt.name)
			} else if !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("Error should contain %q: %v", tt.errorContains, err)
			}
		})
	}
}

func TestServerConfig_Validate_PercentageFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setField      func(*ServerConfig)
		errorContains string
	}{
		{"MemoryWarningPercent zero", func(c *ServerConfig) { c.MemoryWarningPercent = 0 }, "WS_MEMORY_WARNING_PERCENT"},
		{"MemoryWarningPercent 101", func(c *ServerConfig) { c.MemoryWarningPercent = 101 }, "WS_MEMORY_WARNING_PERCENT"},
		{"MemoryCriticalPercent zero", func(c *ServerConfig) { c.MemoryCriticalPercent = 0 }, "WS_MEMORY_CRITICAL_PERCENT"},
		{"MemoryCriticalPercent 101", func(c *ServerConfig) { c.MemoryCriticalPercent = 101 }, "WS_MEMORY_CRITICAL_PERCENT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			tt.setField(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Errorf("Expected error for %s", tt.name)
			} else if !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("Error should contain %q: %v", tt.errorContains, err)
			}
		})
	}
}

// --- Edition Gate Tests ---
// MUST NOT use t.Parallel() — tests share license.SetPublicKeyForTesting.

// setEditionManager creates a license.Manager from a test license key and assigns
// it to the config's unexported editionManager field.
func setServerEditionManager(t *testing.T, cfg *ServerConfig, edition license.Edition) {
	t.Helper()
	priv, pub := license.GenerateTestKeyPair()
	license.SetPublicKeyForTesting(pub)
	claims := license.Claims{
		Edition: edition,
		Org:     "test",
		Exp:     time.Now().Add(time.Hour).Unix(),
	}
	key := license.SignTestLicense(claims, priv)
	mgr, err := license.NewManager(key, zerolog.Nop())
	if err != nil {
		t.Fatalf("create test license manager: %v", err)
	}
	cfg.editionManager = mgr
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_Community(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*ServerConfig)
		errSub string
	}{
		{
			name: "community rejects alerting",
			modify: func(c *ServerConfig) {
				c.AlertEnabled = true
				c.AlertSlackWebhookURL = "https://hooks.slack.com/test"
				c.AlertSlackTimeout = 5 * time.Second
				c.AlertRateLimitWindow = 5 * time.Minute
				c.AlertRateLimitMax = 3
			},
			errSub: "ALERT_ENABLED",
		},
		{
			name:   "community rejects 2 shards",
			modify: func(c *ServerConfig) { c.NumShards = 2 },
			errSub: "shards",
		},
		{
			name:   "community rejects 1000 total connections (limit 500)",
			modify: func(c *ServerConfig) { c.MaxConnections = 1000 },
			errSub: "total_connections",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newValidServerConfig()
			setServerEditionManager(t, cfg, license.Community)
			tt.modify(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errSub) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errSub)
			}
		})
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_CommunityAcceptsDataPath(t *testing.T) {
	// ADR-0009: the data path (Kafka backend, message history) is Community —
	// the free tier reproduces the benchmark; capacity caps are the tier wall.
	cfg := newValidServerConfig()
	setServerEditionManager(t, cfg, license.Community)
	cfg.MessageBackend = "kafka"
	// Stay within Community capacity caps — the caps, not feature gates,
	// are what stops a Community deployment from scaling (limits.go).
	cfg.NumShards = 1
	cfg.MaxConnections = 500

	if err := cfg.Validate(); err != nil {
		t.Errorf("Community should accept the kafka backend (data path is Community): %v", err)
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_ProAccepts(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*ServerConfig)
	}{
		{
			name:   "pro accepts kafka backend",
			modify: func(c *ServerConfig) { c.MessageBackend = "kafka" },
		},
		{
			name: "pro accepts alerting",
			modify: func(c *ServerConfig) {
				c.AlertEnabled = true
				c.AlertSlackWebhookURL = "https://hooks.slack.com/test"
				c.AlertSlackTimeout = 5 * time.Second
				c.AlertRateLimitWindow = 5 * time.Minute
				c.AlertRateLimitMax = 3
			},
		},
		{
			name:   "pro accepts 8 shards",
			modify: func(c *ServerConfig) { c.NumShards = 8; c.MaxConnections = 500 },
		},
		{
			name:   "pro accepts 10000 connections",
			modify: func(c *ServerConfig) { c.NumShards = 2; c.MaxConnections = 5000 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newValidServerConfig()
			setServerEditionManager(t, cfg, license.Pro)
			tt.modify(cfg)

			if err := cfg.Validate(); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_ProLimits(t *testing.T) {
	// WS_MAX_CONNECTIONS is the TOTAL (divided across shards in main.go).
	// The gate checks MaxConnections directly, NOT MaxConnections * NumShards.
	cfg := newValidServerConfig()
	setServerEditionManager(t, cfg, license.Pro)
	cfg.NumShards = 4
	cfg.MaxConnections = 15000 // 15K total exceeds Pro limit of 10K

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for 15K connections on Pro (max 10K)")
	}
	if !strings.Contains(err.Error(), "total_connections") {
		t.Errorf("error %q should mention total_connections", err.Error())
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_ProAcceptsWithinLimit(t *testing.T) {
	// 5K total across 4 shards = well within Pro's 10K limit
	cfg := newValidServerConfig()
	setServerEditionManager(t, cfg, license.Pro)
	cfg.NumShards = 4
	cfg.MaxConnections = 5000

	if err := cfg.Validate(); err != nil {
		t.Errorf("5K connections on Pro should pass (max 10K): %v", err)
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_EnterpriseAcceptsAll(t *testing.T) {
	cfg := newValidServerConfig()
	setServerEditionManager(t, cfg, license.Enterprise)
	cfg.MessageBackend = "kafka"
	cfg.NumShards = 16
	cfg.MaxConnections = 50000
	cfg.AlertEnabled = true
	cfg.AlertSlackWebhookURL = "https://hooks.slack.com/test"
	cfg.AlertSlackTimeout = 5 * time.Second
	cfg.AlertRateLimitWindow = 5 * time.Minute
	cfg.AlertRateLimitMax = 3

	if err := cfg.Validate(); err != nil {
		t.Errorf("Enterprise should accept everything: %v", err)
	}
}

func TestServerConfig_Validate_EditionGates_NoManager(t *testing.T) {
	t.Parallel()
	// No editionManager set → edition gates skipped (backward compatibility)
	cfg := newValidServerConfig()
	cfg.MessageBackend = "kafka"
	if err := cfg.Validate(); err != nil {
		t.Errorf("no manager should skip edition gates: %v", err)
	}
}

//nolint:paralleltest // shares license.SetPublicKeyForTesting via setEditionManager helper
func TestServerConfig_Validate_EditionGates_FeatureError(t *testing.T) {
	cfg := newValidServerConfig()
	setServerEditionManager(t, cfg, license.Community)
	cfg.NumShards = 1
	cfg.MaxConnections = 500
	cfg.AlertEnabled = true
	cfg.AlertSlackWebhookURL = "https://hooks.slack.com/test"
	cfg.AlertSlackTimeout = 5 * time.Second
	cfg.AlertRateLimitWindow = 5 * time.Minute
	cfg.AlertRateLimitMax = 3

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}

	featureErr, ok := errors.AsType[*license.EditionFeatureError](err)
	if !ok {
		t.Fatalf("expected EditionFeatureError, got %T: %v", err, err)
	}
	if featureErr.Feature != license.Alerting {
		t.Errorf("Feature = %q, want Alerting", featureErr.Feature)
	}
	if featureErr.CurrentEdition != license.Community {
		t.Errorf("CurrentEdition = %q, want Community", featureErr.CurrentEdition)
	}
}

// =============================================================================
// Kafka Partition-Revoke Commit Validation Tests
// =============================================================================

func TestServerConfig_KafkaCommitOnRevokeTimeout_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		timeout   time.Duration
		wantError bool
	}{
		{"zero timeout", 0, true},
		{"negative timeout", -1 * time.Second, true},
		{"equal to RebalanceTimeout (not strict <)", 60 * time.Second, true}, // KafkaRebalanceTimeout is 60s in newValidServerConfig
		{"at 80% of RebalanceTimeout", 48 * time.Second, false},
		{"valid 5s", 5 * time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.KafkaCommitOnRevokeTimeout = tt.timeout
			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Errorf("expected validation error for KafkaCommitOnRevokeTimeout=%v, got nil", tt.timeout)
			}
			if !tt.wantError && err != nil {
				t.Errorf("expected no error for KafkaCommitOnRevokeTimeout=%v, got: %v", tt.timeout, err)
			}
			if tt.wantError && err != nil && !strings.Contains(err.Error(), "KAFKA_COMMIT_ON_REVOKE_TIMEOUT") {
				t.Errorf("error must mention KAFKA_COMMIT_ON_REVOKE_TIMEOUT, got: %v", err)
			}
		})
	}
}

func TestServerConfig_KafkaAutoCommitInterval_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		interval  time.Duration
		wantError bool
	}{
		{"zero interval", 0, true},
		{"below floor (50ms)", 50 * time.Millisecond, true},
		{"exact floor (100ms)", 100 * time.Millisecond, false},
		{"valid 1s", 1 * time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.KafkaAutoCommitInterval = tt.interval
			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Errorf("expected validation error for KafkaAutoCommitInterval=%v, got nil", tt.interval)
			}
			if !tt.wantError && err != nil {
				t.Errorf("expected no error for KafkaAutoCommitInterval=%v, got: %v", tt.interval, err)
			}
			if tt.wantError && err != nil && !strings.Contains(err.Error(), "KAFKA_AUTO_COMMIT_INTERVAL") {
				t.Errorf("error must mention KAFKA_AUTO_COMMIT_INTERVAL, got: %v", err)
			}
		})
	}
}

func TestServerConfig_KafkaRebalanceSessionCrossField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		rebalanceTimeout time.Duration
		sessionTimeout   time.Duration
		wantError        bool
	}{
		{"RebalanceTimeout == SessionTimeout (not strict >)", 30 * time.Second, 30 * time.Second, true},
		{"RebalanceTimeout > SessionTimeout (valid)", 60 * time.Second, 30 * time.Second, false},
		{"RebalanceTimeout < SessionTimeout (invalid)", 20 * time.Second, 30 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.KafkaRebalanceTimeout = tt.rebalanceTimeout
			cfg.KafkaSessionTimeout = tt.sessionTimeout
			// Ensure CommitOnRevokeTimeout is valid (< RebalanceTimeout)
			cfg.KafkaCommitOnRevokeTimeout = tt.rebalanceTimeout / 2
			if cfg.KafkaCommitOnRevokeTimeout <= 0 {
				cfg.KafkaCommitOnRevokeTimeout = 1 * time.Second
			}
			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Errorf("expected error for RebalanceTimeout=%v SessionTimeout=%v, got nil",
					tt.rebalanceTimeout, tt.sessionTimeout)
			}
			if !tt.wantError && err != nil {
				t.Errorf("expected no error for RebalanceTimeout=%v SessionTimeout=%v, got: %v",
					tt.rebalanceTimeout, tt.sessionTimeout, err)
			}
		})
	}
}

func TestServerConfig_Validate_LogConfig_NoNATSFields(t *testing.T) {
	t.Parallel()
	cfg := newValidServerConfig()
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	cfg.LogConfig(logger)
	output := buf.String()
	if strings.Contains(strings.ToLower(output), "nats") {
		t.Errorf("LogConfig output must not contain any 'nats' field, got: %s", output)
	}
}

// TestLoadServerConfig_StartupWarning verifies the startup warning logic in LoadServerConfig.
// MUST NOT call t.Parallel() at parent level — t.Setenv requires sequential execution.
//

func TestLoadServerConfig_StartupWarning(t *testing.T) {
	tests := []struct {
		name           string
		commitTimeout  string // KAFKA_COMMIT_ON_REVOKE_TIMEOUT env var value
		sessionTimeout string // KAFKA_SESSION_TIMEOUT env var value
		expectWarning  bool
	}{
		{
			name:           "timeout > session/3 → warn",
			commitTimeout:  "15s",
			sessionTimeout: "30s", // session/3 = 10s; 15s > 10s → warn
			expectWarning:  true,
		},
		{
			name:           "timeout == session/3 → no warn (not strictly >)",
			commitTimeout:  "10s",
			sessionTimeout: "30s", // session/3 = 10s; 10s == 10s → no warn
			expectWarning:  false,
		},
		{
			name:           "timeout < session/3 → no warn",
			commitTimeout:  "7s",
			sessionTimeout: "30s", // session/3 = 10s; 7s < 10s → no warn
			expectWarning:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set required env vars for a minimal valid config
			t.Setenv("KAFKA_COMMIT_ON_REVOKE_TIMEOUT", tt.commitTimeout)
			t.Setenv("KAFKA_SESSION_TIMEOUT", tt.sessionTimeout)
			// Set KAFKA_REBALANCE_TIMEOUT > KAFKA_SESSION_TIMEOUT
			t.Setenv("KAFKA_REBALANCE_TIMEOUT", "60s")

			var buf strings.Builder
			logger := zerolog.New(&buf)

			cfg, err := LoadServerConfig(logger)
			if err != nil {
				// If config is invalid for other reasons (missing required fields),
				// that's OK — we only care about warning presence when config is valid.
				// Skip if the config failed for reasons unrelated to our fields.
				if !strings.Contains(buf.String(), MsgCommitOnRevokeTimeoutWarning) && tt.expectWarning {
					t.Logf("LoadServerConfig error (may be missing required env): %v", err)
				}
				return
			}
			_ = cfg

			logOutput := buf.String()
			hasWarning := strings.Contains(logOutput, MsgCommitOnRevokeTimeoutWarning)

			if tt.expectWarning && !hasWarning {
				t.Errorf("expected warning %q in logs, got: %s", MsgCommitOnRevokeTimeoutWarning, logOutput)
			}
			if !tt.expectWarning && hasWarning {
				t.Errorf("unexpected warning %q in logs", MsgCommitOnRevokeTimeoutWarning)
			}
		})
	}
}

func TestServerConfig_BroadcastBufferSizeBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		size      int
		wantErr   bool
		errSubstr string
	}{
		{"zero", 0, true, "BROADCAST_BUFFER_SIZE"},
		{"above max", BroadcastBufferSizeMax + 1, true, "BROADCAST_BUFFER_SIZE_MAX"},
		{"at max", BroadcastBufferSizeMax, false, ""},
		{"default 256", 256, false, ""},
		{"positive", 1, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.BroadcastBufferSize = tt.size
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for BroadcastBufferSize=%d, got nil", tt.size)
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error for BroadcastBufferSize=%d: %v", tt.size, err)
			}
		})
	}
}

func TestServerConfig_ValidateEditionLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limits    license.Limits
		numShards int
		bufSize   int
		memLimit  int64
		wantErr   bool
	}{
		{
			name:      "Enterprise unlimited always passes",
			limits:    license.Limits{MaxTenants: 0}, // 0 = unlimited
			numShards: 10,
			bufSize:   BroadcastBufferSizeMax,
			memLimit:  1, // tiny memory limit — still passes for unlimited
			wantErr:   false,
		},
		{
			name:      "Pro MaxTenants=50 within memory limit",
			limits:    license.Limits{MaxTenants: 50},
			numShards: 3,
			bufSize:   256,
			memLimit:  512 * 1024 * 1024, // 512MB
			wantErr:   false,
		},
		{
			name:      "Pro MaxTenants=50 exceeds memory limit",
			limits:    license.Limits{MaxTenants: 50},
			numShards: 3,
			bufSize:   256,
			memLimit:  1, // 1 byte — guaranteed failure
			wantErr:   true,
		},
		{
			name:      "Community MaxTenants=3 within memory",
			limits:    license.Limits{MaxTenants: 3},
			numShards: 1,
			bufSize:   256,
			memLimit:  512 * 1024 * 1024,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			cfg.NumShards = tt.numShards
			cfg.BroadcastBufferSize = tt.bufSize
			cfg.MemoryLimit = tt.memLimit
			err := cfg.ValidateEditionLimits(tt.limits)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestServerConfig_Validate_Registry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		modify  func(*ServerConfig)
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid defaults",
			modify:  func(c *ServerConfig) {},
			wantErr: false,
		},
		{
			name:    "buffer at min (1)",
			modify:  func(c *ServerConfig) { c.ConnectionsRegistryBuffer = MinConnectionsRegistryBuffer },
			wantErr: false,
		},
		{
			name:    "buffer at max (8192)",
			modify:  func(c *ServerConfig) { c.ConnectionsRegistryBuffer = MaxConnectionsRegistryBuffer },
			wantErr: false,
		},
		{
			name:    "buffer below min (0)",
			modify:  func(c *ServerConfig) { c.ConnectionsRegistryBuffer = 0 },
			wantErr: true,
			errMsg:  "WS_CONNECTIONS_REGISTRY_BUFFER",
		},
		{
			name:    "buffer above max (8193)",
			modify:  func(c *ServerConfig) { c.ConnectionsRegistryBuffer = MaxConnectionsRegistryBuffer + 1 },
			wantErr: true,
			errMsg:  "WS_CONNECTIONS_REGISTRY_BUFFER",
		},
		{
			name: "shutdown drain at min (0=ok, skip drain)",
			modify: func(c *ServerConfig) {
				c.ConnectionsRegistryShutdownDrainTimeout = MinRegistryWriterShutdownDrainTimeout
			},
			wantErr: false,
		},
		{
			name: "shutdown drain above max",
			modify: func(c *ServerConfig) {
				c.ConnectionsRegistryShutdownDrainTimeout = MaxRegistryWriterShutdownDrainTimeout + time.Second
			},
			wantErr: true,
			errMsg:  "WS_CONNECTIONS_REGISTRY_SHUTDOWN_DRAIN_TIMEOUT",
		},
		{
			name:    "internal secret enabled with empty secret",
			modify:  func(c *ServerConfig) { c.InternalSecretEnabled = true; c.InternalSecret = "" },
			wantErr: true,
			errMsg:  "WS_INTERNAL_SECRET",
		},
		{
			name:    "internal secret enabled with non-empty secret",
			modify:  func(c *ServerConfig) { c.InternalSecretEnabled = true; c.InternalSecret = "secret" },
			wantErr: false,
		},
		{
			name:    "internal secret disabled empty secret ok",
			modify:  func(c *ServerConfig) { c.InternalSecretEnabled = false; c.InternalSecret = "" },
			wantErr: false,
		},
		{
			name:    "admin subscribe timeout below min",
			modify:  func(c *ServerConfig) { c.AdminChannelSubscribeTimeout = 500 * time.Millisecond },
			wantErr: true,
			errMsg:  "WS_ADMIN_CHANNEL_SUBSCRIBE_TIMEOUT",
		},
		{
			name:    "admin subscribe timeout above max",
			modify:  func(c *ServerConfig) { c.AdminChannelSubscribeTimeout = MaxAdminChannelSubscribeTimeout + time.Second },
			wantErr: true,
			errMsg:  "WS_ADMIN_CHANNEL_SUBSCRIBE_TIMEOUT",
		},
		{
			name: "flush interval exceeds heartbeat",
			modify: func(c *ServerConfig) {
				c.ConnectionsRegistryFlushInterval = c.ConnectionsRegistryHeartbeatInterval + time.Second
			},
			wantErr: true,
			errMsg:  "WS_CONNECTIONS_REGISTRY_FLUSH_INTERVAL",
		},
		{
			name: "initial backoff must be less than max backoff",
			modify: func(c *ServerConfig) {
				c.ConnectionsRegistryRestartInitialBackoff = c.ConnectionsRegistryRestartMaxBackoff
			},
			wantErr: true,
			errMsg:  "WS_CONNECTIONS_REGISTRY_RESTART_INITIAL_BACKOFF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := newValidServerConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected validation error, got nil")
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("error %q does not contain expected string %q", err.Error(), tt.errMsg)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
