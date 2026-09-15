// Push notification service entry point.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	pushv1 "github.com/sukko-dev/sukko/gen/proto/sukko/push/v1"
	"github.com/sukko-dev/sukko/internal/push"
	"github.com/sukko-dev/sukko/internal/push/consumer"
	pushgrpc "github.com/sukko-dev/sukko/internal/push/grpcserver"
	"github.com/sukko-dev/sukko/internal/push/provider"
	"github.com/sukko-dev/sukko/internal/push/repository"
	"github.com/sukko-dev/sukko/internal/push/worker"
	"github.com/sukko-dev/sukko/internal/shared/analytics"
	"github.com/sukko-dev/sukko/internal/shared/httputil"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/profiling"
	"github.com/sukko-dev/sukko/internal/shared/provapi"
	"github.com/sukko-dev/sukko/internal/shared/tracing"
)

const serviceName = "push"

func main() {
	// Bootstrap logger for pre-config startup (zerolog without config dependency)
	bootLogger := logging.BootstrapLogger(serviceName)

	// Load configuration first (env vars + envDefaults)
	cfg, err := push.LoadConfig(bootLogger)
	if err != nil {
		bootLogger.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// CLI flags use env var config as defaults (CLI overrides env overrides envDefault)
	var (
		debug          = flag.Bool("debug", cfg.LogLevel == "debug", "enable debug logging (overrides LOG_LEVEL)")
		validateConfig = flag.Bool("validate-config", false, "validate configuration and exit")
	)
	flag.Parse()

	// Override debug mode if flag set
	if *debug {
		cfg.LogLevel = "debug"
		bootLogger.Info().Msg("Debug mode enabled via flag")
	}

	// --validate-config: validate and exit
	if *validateConfig {
		bootLogger.Info().Msg("Configuration is valid")
		os.Exit(0)
	}

	// Initialize structured logger
	structuredLogger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevel(cfg.LogLevel),
		Format:      logging.LogFormat(cfg.LogFormat),
		ServiceName: serviceName,
	})

	structuredLogger.Info().Int("gomaxprocs", runtime.GOMAXPROCS(0)).Msg("GOMAXPROCS set by Go runtime (container-aware)")

	// Initialize tracing (cold-path only, noop when disabled)
	tracingShutdown, err := tracing.Init(context.Background(), tracing.Config{
		Enabled:      cfg.OTELTracingEnabled,
		ExporterType: cfg.OTELExporterType,
		Endpoint:     cfg.OTELExporterEndpoint,
		ServiceName:  serviceName,
		Environment:  cfg.Environment,
	}, structuredLogger)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to initialize tracing")
	}
	defer func() { _ = tracingShutdown(context.Background()) }()

	// Initialize profiling (Pyroscope continuous profiling, noop when disabled)
	pyroscopeStop, err := profiling.InitPyroscope(profiling.PyroscopeConfig{
		Enabled:     cfg.PyroscopeEnabled,
		Addr:        cfg.PyroscopeAddr,
		ServiceName: serviceName,
		Environment: cfg.Environment,
	}, structuredLogger)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to initialize Pyroscope")
	}
	defer pyroscopeStop()

	// Open push database (PostgreSQL via pgxpool)
	pool, err := repository.OpenDatabase(context.Background(), cfg.DatabaseURL)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to open database")
	}
	defer pool.Close() // Close is non-blocking for pgxpool
	structuredLogger.Info().Msg("Database pool opened")

	// Create subscription repository
	subRepo := repository.NewSubscriptionRepository(pool)

	// Explicit topic namespace (validated + normalized at config load).
	topicNamespace := cfg.KafkaTopicNamespace
	structuredLogger.Info().Str("namespace", topicNamespace).Str("environment", cfg.Environment).Msg("Topic namespace set")

	// Create ConfigClient — connects to provisioning gRPC for WatchPushConfig + WatchTopics
	configClient, err := push.NewConfigClient(push.ConfigClientConfig{
		ProvisioningAddr:  cfg.ProvisioningGRPCAddr,
		Namespace:         topicNamespace,
		ReconnectDelay:    cfg.GRPCReconnectDelay,
		ReconnectMaxDelay: cfg.GRPCReconnectMaxDelay,
		Logger:            structuredLogger,
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create config client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start ConfigClient (launches WatchPushConfig stream goroutine)
	configClient.Start(ctx)

	push.SetDryRunMode(cfg.DryRun)

	// Create providers based on dry-run mode
	providers := make(map[string]provider.Provider)
	if cfg.DryRun {
		dryRun := provider.NewDryRunProvider(structuredLogger)
		providers["web"] = dryRun
		providers["android"] = dryRun
		providers["ios"] = dryRun
		structuredLogger.Info().Msg("Dry-run mode enabled — all push notifications will be logged only")
	} else {
		// Web Push provider — credential lookup scoped to "vapid"
		webPushProv, err := provider.NewWebPushProvider(structuredLogger, func(tenantID string) (json.RawMessage, error) {
			return configClient.GetCredential(tenantID, "vapid")
		})
		if err != nil {
			structuredLogger.Fatal().Err(err).Msg("Failed to create Web Push provider")
		}
		providers["web"] = webPushProv

		// FCM provider — credential lookup scoped to "fcm"
		fcmProv, err := provider.NewFCMProvider(structuredLogger, func(tenantID string) (json.RawMessage, error) {
			return configClient.GetCredential(tenantID, "fcm")
		})
		if err != nil {
			structuredLogger.Fatal().Err(err).Msg("Failed to create FCM provider")
		}
		providers["android"] = fcmProv

		// APNs provider — credential lookup scoped to "apns"
		apnsProv, err := provider.NewAPNsProvider(structuredLogger, func(tenantID string) (json.RawMessage, error) {
			return configClient.GetCredential(tenantID, "apns")
		})
		if err != nil {
			structuredLogger.Fatal().Err(err).Msg("Failed to create APNs provider")
		}
		providers["ios"] = apnsProv

		structuredLogger.Info().Int("provider_count", len(providers)).Msg("Push providers initialized")
	}

	// Build consumer pool config from message backend settings
	var consumerSASL *kafkashared.SASLConfig
	if cfg.KafkaSASLEnabled {
		consumerSASL = &kafkashared.SASLConfig{
			Mechanism: cfg.KafkaSASLMechanism,
			Username:  cfg.KafkaSASLUsername,
			Password:  cfg.KafkaSASLPassword,
		}
	}

	var consumerTLS *kafkashared.TLSConfig
	if cfg.KafkaTLSEnabled {
		consumerTLS = &kafkashared.TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: cfg.KafkaTLSInsecure,
			CAPath:             cfg.KafkaTLSCAPath,
		}
	}

	// Split comma-separated brokers
	brokers := splitBrokers(cfg.KafkaBrokers)

	// Create push service (orchestrates consumer pool + worker pool)
	svc, err := push.NewService(push.ServiceConfig{
		ConsumerPool: consumer.PoolConfig{
			Brokers:       brokers,
			Namespace:     topicNamespace,
			ConsumerGroup: "push-service",
			Logger:        structuredLogger,
			SASL:          consumerSASL,
			TLS:           consumerTLS,
		},
		WorkerPool: worker.PoolConfig{
			WorkerCount: cfg.WorkerPoolSize,
			QueueSize:   cfg.JobQueueSize,
			Providers:   providers,
			Repo:        subRepo,
			MaxRetries:  cfg.MaxRetries,
			Logger:      structuredLogger,
		},
		WorkerCount:    cfg.WorkerPoolSize,
		Repo:           subRepo,
		Cache:          configClient,
		DefaultTTL:     cfg.DefaultTTL,
		DefaultUrgency: cfg.DefaultUrgency,
		Logger:         structuredLogger,
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create push service")
	}

	// Start push service (consumer pool + worker pool)
	if err := svc.Start(ctx); err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to start push service")
	}

	// Token revocation — subscribe to WatchTokenRevocations from provisioning
	revocationRegistry, revErr := provapi.NewStreamRevocationRegistry(provapi.StreamRevocationRegistryConfig{
		GRPCAddr:          cfg.ProvisioningGRPCAddr,
		ReconnectDelay:    cfg.GRPCReconnectDelay,
		ReconnectMaxDelay: cfg.GRPCReconnectMaxDelay,
		MetricPrefix:      serviceName,
		Logger:            structuredLogger,
		OnRevocation: func(entry provapi.RevocationEntry) {
			svc.HandleRevocation(entry)
		},
	})
	if revErr != nil {
		structuredLogger.Fatal().Err(revErr).Msg("Failed to create revocation registry")
	}

	// License watcher — subscribe to WatchLicense gRPC stream for runtime edition updates
	licenseWatcher, licErr := provapi.NewStreamLicenseWatcher(provapi.StreamLicenseWatcherConfig{
		GRPCAddr:          cfg.ProvisioningGRPCAddr,
		ReconnectDelay:    cfg.GRPCReconnectDelay,
		ReconnectMaxDelay: cfg.GRPCReconnectMaxDelay,
		MetricPrefix:      serviceName,
		Manager:           cfg.EditionManager(),
		Logger:            structuredLogger,
		OnDowngrade:       provapi.LicenseDowngradeShutdown(structuredLogger, cancel),
	})
	if licErr != nil {
		structuredLogger.Fatal().Err(licErr).Msg("Failed to create license watcher")
	}

	// Create gRPC server with interceptors for device registration
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	grpcListener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", grpcAddr)
	if err != nil {
		structuredLogger.Fatal().Err(err).Int("port", cfg.GRPCPort).Msg("Failed to listen on gRPC port")
	}

	grpcSrv := grpc.NewServer(
		tracing.StatsHandler(),
		grpc.ChainUnaryInterceptor(
			pushgrpc.RecoveryUnaryInterceptor(structuredLogger),
			pushgrpc.LoggingUnaryInterceptor(structuredLogger),
			pushgrpc.MetricsUnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			pushgrpc.RecoveryStreamInterceptor(structuredLogger),
			pushgrpc.LoggingStreamInterceptor(structuredLogger),
			pushgrpc.MetricsStreamInterceptor(),
		),
	)

	pushGRPCServer, err := pushgrpc.NewServer(pushgrpc.ServerConfig{
		Repo:        subRepo,
		ConfigCache: configClient,
		ProvClient:  configClient.ProvisioningClient(),
		Logger:      structuredLogger,
		Manager:     cfg.EditionManager(),
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create push gRPC server")
	}
	pushv1.RegisterPushServiceServer(grpcSrv, pushGRPCServer)

	// Set up HTTP server for health + metrics
	httpMux := newHTTPMux(licenseWatcher, cfg.EditionManager())

	httpAddr := fmt.Sprintf(":%d", cfg.HTTPPort)
	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           httpMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Open analytics pool (returns nil when ANALYTICS_ENABLED=false → NoopCollector).
	analyticsPool, err := platform.OpenAnalyticsPool(ctx, cfg.AnalyticsConfig)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to open analytics pool")
	}
	if analyticsPool != nil {
		defer analyticsPool.Close()
	}
	if cfg.WSPodID != "" && cfg.SukkoPodID == "" {
		structuredLogger.Warn().Msg("WS_POD_ID is deprecated; set SUKKO_POD_ID via Kubernetes Downward API")
	}
	analyticsCollector := analytics.NewCollector(analytics.CollectorConfig{
		ServicePrefix:  "push",
		PodID:          cfg.PodID(),
		BufferSize:     cfg.BufferSize,
		FlushInterval:  cfg.FlushInterval,
		DowngradePoll:  cfg.DowngradePollInterval,
		EditionManager: cfg.EditionManager(),
		Feature:        license.AnalyticsPush,
	}, analyticsPool, structuredLogger)
	// pass analyticsCollector to push worker for RecordPushDelivery calls.

	// Start gRPC server in goroutine
	var wg sync.WaitGroup

	// Start analytics collector — registers its goroutines directly on wg.
	if err := analyticsCollector.Start(ctx, &wg); err != nil {
		structuredLogger.Error().Err(err).Msg("Analytics collector start failed")
	}
	wg.Go(func() {
		defer logging.RecoverPanic(structuredLogger, "grpc.Serve", nil)
		structuredLogger.Info().Str("addr", grpcAddr).Msg("Starting push gRPC server")
		if err := grpcSrv.Serve(grpcListener); err != nil {
			structuredLogger.Error().Err(err).Msg("Push gRPC server error")
			cancel()
		}
	})

	// Start HTTP server in goroutine
	wg.Go(func() {
		defer logging.RecoverPanic(structuredLogger, "http.ListenAndServe", nil)
		structuredLogger.Info().Str("addr", httpAddr).Msg("Starting push HTTP server (health + metrics)")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			structuredLogger.Error().Err(err).Msg("Push HTTP server error")
			cancel()
		}
	})

	structuredLogger.Info().
		Int("grpc_port", cfg.GRPCPort).
		Int("http_port", cfg.HTTPPort).
		Bool("dry_run", cfg.DryRun).
		Int("workers", cfg.WorkerPoolSize).
		Msg("Push notification service started")

	// Wait for interrupt signal or context cancellation (from server error)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}

	structuredLogger.Info().Msg("Shutting down push notification service")

	// Cancel context to signal all goroutines
	cancel()

	// Stop push service (stops consumer pool + worker pool)
	svc.Stop()

	// Graceful shutdown gRPC server
	grpcSrv.GracefulStop()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		structuredLogger.Error().Err(err).Msg("Error during HTTP server shutdown")
	}

	// Stop revocation registry (closes gRPC stream)
	if err := revocationRegistry.Close(); err != nil {
		structuredLogger.Error().Err(err).Msg("Error stopping revocation registry")
	}

	// Stop license watcher (closes gRPC stream)
	if err := licenseWatcher.Close(); err != nil {
		structuredLogger.Warn().Err(err).Msg("License watcher close failed")
	}

	// Stop config client (closes gRPC streams + connections)
	if err := configClient.Stop(); err != nil {
		structuredLogger.Error().Err(err).Msg("Error stopping config client")
	}

	// Close providers
	for name, prov := range providers {
		if err := prov.Close(); err != nil {
			structuredLogger.Error().Err(err).Str("provider", name).Msg("Error closing provider")
		}
	}

	// Wait for background goroutines (gRPC serve, HTTP serve)
	wg.Wait()

	structuredLogger.Info().Msg("Push notification service gracefully shut down")
}

// licenseStateGetter is satisfied by *provapi.StreamLicenseWatcher.
// Extracted as an interface to allow HTTP handler unit testing.
type licenseStateGetter interface {
	State() int32
}

// newHTTPMux builds the HTTP mux for the push service.
// /health — liveness probe: always 200; reports license stream connectivity.
// /ready  — readiness probe: 503 until Enterprise license active, then 200.
// /metrics — Prometheus metrics.
func newHTTPMux(watcher licenseStateGetter, mgr *license.Manager) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		streamState := provapi.StreamLabelConnected
		if watcher.State() == provapi.StreamStateDisconnected {
			streamState = provapi.StreamLabelDegraded
		}
		_ = httputil.WriteJSON(w, http.StatusOK, map[string]string{ // write error non-actionable for liveness endpoint
			"status":         "ok",
			"service":        serviceName,
			"license_stream": streamState,
		})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !mgr.HasFeature(license.WebPush) {
			_ = httputil.WriteJSON(w, http.StatusServiceUnavailable, map[string]any{ // write error non-actionable for readiness endpoint
				"ready":   false,
				"edition": mgr.CurrentEdition().String(),
			})
			return
		}
		_ = httputil.WriteJSON(w, http.StatusOK, map[string]any{ // write error non-actionable for readiness endpoint
			"ready":   true,
			"edition": mgr.CurrentEdition().String(),
		})
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// splitBrokers splits a comma-separated broker list into individual addresses.
func splitBrokers(brokers string) []string {
	var result []string
	for b := range strings.SplitSeq(brokers, ",") {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
