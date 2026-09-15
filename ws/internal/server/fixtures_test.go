package server

import (
	"time"

	"github.com/sukko-dev/sukko/internal/shared/platform"
)

// newTestServerConfig creates a *platform.ServerConfig with safe defaults for testing.
// All limits are set high to avoid triggering protection mechanisms unless explicitly tested.
func newTestServerConfig() *platform.ServerConfig {
	return &platform.ServerConfig{
		BaseConfig: platform.BaseConfig{
			LogLevel:    "error",
			LogFormat:   "json",
			Environment: "test",
		},

		Addr:           "localhost:0", // Random available port
		MaxConnections: 1000,

		// Resource limits (generous for testing)
		MemoryLimit:   4 * 1024 * 1024 * 1024, // 4GB
		MaxGoroutines: 10000,

		// Shard configuration
		NumShards: 1,
		BasePort:  3002,
		LBAddr:    ":3005",

		// Rate limiting (high limits for testing)
		MaxKafkaMessagesPerSec:   10000,
		MaxBroadcastsPerSec:      10000,
		RateLimitBurstMultiplier: 2,

		// Per-client rate limiting (generous for testing)
		ClientMsgBurstLimit: 1000,
		ClientMsgRatePerSec: 100.0,

		// Connection rate limiting (disabled by default for tests)
		ConnectionRateLimitEnabled:   false,
		ConnRateLimitIPBurst:         100,
		ConnRateLimitIPRate:          10.0,
		ConnRateLimitGlobalBurst:     1000,
		ConnRateLimitGlobalRate:      100.0,
		ConnRateLimitIPTTL:           5 * time.Minute,
		ConnRateLimitCleanupInterval: 1 * time.Minute,

		// CPU thresholds (high to avoid triggering in normal tests)
		CPURejectThreshold:      95.0,
		CPURejectThresholdLower: 90.0,
		CPUPauseThreshold:       98.0,
		CPUPauseThresholdLower:  95.0,

		// Slow client detection
		SlowClientMaxAttempts: 3,

		// WebSocket ping/pong
		PongWait:   60 * time.Second,
		PingPeriod: 45 * time.Second,
		WriteWait:  5 * time.Second,

		// Client buffer
		ClientSendBufferSize: 512,

		// Timeouts
		HTTPTimeoutConfig: platform.HTTPTimeoutConfig{
			HTTPReadTimeout:  30 * time.Second,
			HTTPWriteTimeout: 30 * time.Second,
			HTTPIdleTimeout:  120 * time.Second,
		},

		// Monitoring
		MetricsInterval: 15 * time.Second,
		CPUPollInterval: 1 * time.Second,

		// Handler timeouts
		ReplayTimeout:     5 * time.Second,
		PublishTimeout:    5 * time.Second,
		MaxReplayMessages: 100,

		// Shutdown
		ShutdownGracePeriod:   30 * time.Second,
		ShutdownCheckInterval: 1 * time.Second,

		// Internal monitoring
		MetricsCollectInterval:      2 * time.Second,
		MemoryMonitorInterval:       30 * time.Second,
		MemoryWarningPercent:        80,
		MemoryCriticalPercent:       90,
		BufferSampleInterval:        10 * time.Second,
		BufferMaxSamples:            100,
		BufferHighSaturationPercent: 90,
		BufferPopulationWarnPercent: 25,
	}
}

// newTestParams creates Params with test defaults.
func newTestParams() Params {
	cfg := newTestServerConfig()
	return Params{
		Config:         cfg,
		Addr:           cfg.Addr,
		MaxConnections: cfg.MaxConnections,
	}
}
