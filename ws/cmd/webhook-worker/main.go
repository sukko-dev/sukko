// Package main is the entry point for the webhook-worker service.
// The worker subscribes to the Valkey broadcast bus, matches events against
// cached webhook registrations, and delivers signed HTTP POSTs with retry logic.
package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	valkey "github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcmetadata "google.golang.org/grpc/metadata"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/grpcserver"
	"github.com/sukko-dev/sukko/internal/server/broadcast"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/webhook/worker"
)

const (
	serviceName = "webhook-worker"

	// invalidationChannelBuffer is the number of tenant IDs that can be queued in the
	// invalidation channel before signals are dropped and deferred to the next TTL tick.
	// Sized to absorb a burst of concurrent webhook writes across tenants (§I: no magic numbers).
	invalidationChannelBuffer = 256
)

func main() {
	bootLogger := logging.BootstrapLogger(serviceName)

	cfg, err := platform.LoadWebhookWorkerConfig()
	if err != nil {
		bootLogger.Fatal().Err(err).Msg("Failed to load configuration")
	}

	logger := logging.NewLogger(buildLoggerConfig(cfg))
	// NewLogger already injects "service" as a base field; do not re-add it here.
	logger.Info().Str("environment", cfg.Environment).Msg("Starting")
	if cfg.WebhookAllowPrivateIPs {
		logger.Warn().Msg("WEBHOOK_ALLOW_PRIVATE_IPS=true: SSRF protection disabled — ensure this is not a production deployment")
	}
	if cfg.WebhookAllowHTTP {
		logger.Warn().Msg("WEBHOOK_ALLOW_HTTP=true: plain-HTTP delivery enabled — webhook payloads and signatures transmitted without TLS")
	}

	// Parse AES-256 encryption key.
	encKey, err := crypto.ParseEncryptionKey(cfg.CredentialsEncryptionKey)
	if err != nil {
		logger.Fatal().Err(err).Msg("Invalid CREDENTIALS_ENCRYPTION_KEY")
	}

	// --- Outbound: worker → provisioning gRPC (WebhookWorkerService) ---
	// Interceptor adds WEBHOOK_INTERNAL_TOKEN to outgoing metadata.
	// NOTE: This is the CLIENT-SIDE (outbound) interceptor — not WebhookWorkerAuthUnaryInterceptor,
	// which is the SERVER-SIDE inbound interceptor for the worker's own gRPC server.
	internalToken := cfg.WebhookInternalToken
	outboundInterceptor := grpc.WithUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = grpcmetadata.AppendToOutgoingContext(ctx,
				platform.GRPCInternalTokenMetadataKey, internalToken)
			return invoker(ctx, method, req, reply, cc, opts...)
		},
	)
	provConn, err := grpc.NewClient(cfg.WebhookWorkerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		outboundInterceptor,
	)
	if err != nil {
		logger.Fatal().Err(err).Str("addr", cfg.WebhookWorkerGRPCAddr).
			Msg("Failed to dial provisioning gRPC")
	}
	defer func() { _ = provConn.Close() }()
	provClient := worker.NewGRPCProvisioningClient(provConn)

	// --- Broadcast bus (SubscribeAll for fresh events) ---
	bus, err := broadcast.NewBus(buildBroadcastConfig(cfg), logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create broadcast bus")
	}
	broadcastCh, err := bus.SubscribeAll()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to subscribe to broadcast bus")
	}
	sub := worker.NewBusSubscriber(bus, broadcastCh)

	// Create ctx and wg before starting any goroutines or building the runner,
	// so that startInvalidationSubscriber can be tracked by wg and use the real ctx.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	var wg sync.WaitGroup

	// --- Valkey invalidation PSUBSCRIBE — started before runner construction so runner
	// receives the live channel (not a nil placeholder).
	invalidationCh := startInvalidationSubscriber(ctx, &wg, cfg, logger)

	// --- Build cache and deliverer ---
	cache := worker.NewWebhookCache(provClient, logger)

	ssrfDialer := worker.NewSSRFDialer(&net.Resolver{}, cfg.DeliveryTimeout, cfg.WebhookAllowPrivateIPs)
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: ssrfDialer.DialContext,
		},
		Timeout: cfg.DeliveryTimeout,
	}
	deliverer := worker.NewDeliverer(cache, httpClient, encKey, logger)

	// --- Build runner ---
	r, err := worker.NewRunner(worker.Config{
		WorkerConcurrency:  cfg.WorkerConcurrency,
		RetryQueueSize:     cfg.RetryQueueSize,
		DeliveryTimeout:    cfg.DeliveryTimeout,
		CacheTTL:           cfg.CacheTTL,
		ProvisioningClient: provClient,
		Subscriber:         sub,
		InvalidationCh:     invalidationCh, // now non-nil; live channel from startInvalidationSubscriber
		Cache:              cache,
		Deliverer:          deliverer,
		Logger:             logger,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create runner")
	}

	// --- Internal gRPC server (inbound: provisioning → worker TestDeliver) ---
	internalSrv, err := worker.NewInternalServer(worker.InternalServerConfig{
		Cache: cache,

		TestDeliverFn: func(ctx context.Context, webhookID, tenantID string) worker.DeliveryResult {
			return deliverer.TestDeliver(ctx, webhookID, tenantID)
		},
		Logger: logger,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create internal gRPC server")
	}

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcserver.RecoveryUnaryInterceptor(logger),
			grpcserver.LoggingUnaryInterceptor(logger),
			worker.WebhookMetricsUnaryInterceptor(),
			grpcserver.WebhookWorkerAuthUnaryInterceptor(cfg.WebhookInternalToken),
		),
	)
	provisioningv1.RegisterWebhookWorkerInternalServiceServer(grpcSrv, internalSrv)

	grpcListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", portStr(cfg.InternalGRPCPort))
	if err != nil {
		logger.Fatal().Err(err).Int("port", cfg.InternalGRPCPort).
			Msg("Failed to listen on internal gRPC port")
	}

	// --- HTTP server (health + metrics) ---
	httpMux := http.NewServeMux()
	httpMux.Handle("/metrics", promhttp.Handler())
	httpMux.HandleFunc("/health", worker.HealthHandler(r, logger))
	httpServer := &http.Server{
		Addr:              portStr(cfg.Port),
		Handler:           httpMux,
		ReadHeaderTimeout: 10 * time.Second, // mitigate Slowloris (gosec G112)
	}

	// --- Start everything ---

	// §VII: wg.Go() is the only permitted goroutine launch pattern in Go 1.26+.
	// It calls Add(1) before launch and Done() after return — do NOT add defer wg.Done() inside.
	// RecoverPanic must be the first defer in each function body.
	wg.Go(func() {
		defer logging.RecoverPanic(logger, "broadcast.bus.Run", nil)
		bus.Run() // blocks until ShutdownWithContext is called
	})

	wg.Go(func() {
		defer logging.RecoverPanic(logger, "grpc.Serve", nil)
		logger.Info().Int("port", cfg.InternalGRPCPort).Msg("Starting internal gRPC server")
		if err := grpcSrv.Serve(grpcListener); err != nil && ctx.Err() == nil {
			logger.Error().Err(err).Msg("Internal gRPC server error")
		}
	})

	wg.Go(func() {
		defer logging.RecoverPanic(logger, "http.Serve", nil)
		logger.Info().Int("port", cfg.Port).Msg("Starting HTTP server (health + metrics)")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server error")
		}
	})

	wg.Go(func() {
		defer logging.RecoverPanic(logger, "runner.Run", nil)
		logger.Info().Msg("Starting webhook delivery runner")
		if err := r.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error().Err(err).Msg("Runner exited unexpectedly")
		}
	})

	// Block until SIGTERM or SIGINT.
	<-ctx.Done()
	logger.Info().Msg("Shutdown signal received; stopping webhook-worker")

	// Shutdown ordering: cancel context → wg.Wait → close connections.
	r.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(),
		cfg.DeliveryTimeout+time.Second)

	grpcSrv.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
	// Bus drain is bounded by its own WEBHOOK_WORKER_BROADCAST_SHUTDOWN_TIMEOUT (via
	// Config.ShutdownTimeout → b.shutdownTimeout), matching ws-server's broadcastBus.Shutdown() (§XVIII).
	bus.Shutdown()

	// Wait for all goroutines to exit with a timeout guard (§VII: wg.Wait SHOULD have
	// a timeout mechanism to detect stuck goroutines rather than hanging indefinitely).
	// shutdownCtx covers gRPC+HTTP stop and the wg.Wait below; the bus drain is bounded
	// separately by BroadcastShutdownTimeout above.
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
		shutdownCancel()
		stop()
		logger.Info().Msg("All goroutines exited; webhook-worker stopped")
	case <-shutdownCtx.Done():
		shutdownCancel()
		stop()
		logger.Error().Msg("Shutdown timed out waiting for goroutines; forcing exit")
		os.Exit(1) //nolint:gocritic // forced exit on timeout — deferred conn.Close() skipped intentionally (OS cleanup handles it)
	}
}

// portStr converts a port number to a listener address string.
func portStr(port int) string {
	return ":" + strconv.Itoa(port)
}

// buildLoggerConfig assembles the structured-logger config, including the required
// ServiceName (omitting it panics in logging.NewLogger). Side-effect-free and
// test-callable so the boot smoke test can assert the service identity is set.
func buildLoggerConfig(cfg *platform.WebhookWorkerConfig) logging.LoggerConfig {
	return logging.LoggerConfig{
		Level:       logging.LogLevel(cfg.LogLevel),
		Format:      logging.LogFormat(cfg.LogFormat),
		ServiceName: serviceName,
	}
}

// buildBroadcastConfig assembles the broadcast bus config for this subscribe-only worker,
// mirroring ws-server's config (§XVIII). Side-effect-free (does NOT dial Valkey) so the boot
// smoke test can assert every bus-consumed field is populated without a live Valkey.
//
// Fields intentionally left at their zero value — the worker only calls SubscribeAll() and
// never publishes, so these have no effect here (§XV: no dead knobs; enumerated per spec):
// BufferSize/Limits (SubscribeAll sizes its buffer to the max when Limits is unset), DB
// (0 = database 0, same as ws-server), PublishTimeout/PublishStalenessThreshold/
// ZeroSubscriberWindow (publisher-side; a future publishing use of the bus from this service
// must expose them first — ZeroSubscriberWindow at 0 correctly disables the ADR-0019 gate on a
// publish-free bus), and the Reconnect* knobs (the bus never consumes them — reconnection uses
// internal backoff constants).
//
// NOTE (§X, §XVIII documented deviation): the broadcast package lives under internal/server/;
// with two services consuming it, §X would place it in internal/shared/. Relocation is a tracked
// follow-up — the import location is unchanged here.
func buildBroadcastConfig(cfg *platform.WebhookWorkerConfig) broadcast.Config {
	return broadcast.Config{
		Type:            platform.BroadcastTypeValkey,
		ShutdownTimeout: cfg.BroadcastShutdownTimeout,
		Valkey: broadcast.ValkeyConfig{
			Addrs:               cfg.ValkeyConfig.Addrs,
			Password:            cfg.ValkeyConfig.Password,
			MasterName:          cfg.ValkeyConfig.MasterName,
			Channel:             cfg.ValkeyChannel,
			WriteTimeout:        cfg.ValkeyWriteTimeout,
			StartupPingTimeout:  cfg.ValkeyStartupPingTimeout,
			HealthCheckInterval: cfg.ValkeyHealthCheckInterval,
			HealthCheckTimeout:  cfg.ValkeyHealthCheckTimeout,
			// TLS settings honored (previously silently dropped).
			TLSEnabled:  cfg.ValkeyConfig.TLSEnabled,
			TLSInsecure: cfg.ValkeyConfig.TLSInsecure,
			TLSCAPath:   cfg.ValkeyConfig.TLSCAPath,
		},
	}
}

// startInvalidationSubscriber creates a Valkey PSUBSCRIBE goroutine tracked by wg,
// and returns a channel that feeds tenant IDs to the runner's invalidationConsumer.
// Non-fatal: falls back to TTL-only refresh if Valkey is unavailable.
// ctx is the application shutdown context — client.Receive exits when ctx is canceled.
func startInvalidationSubscriber(ctx context.Context, wg *sync.WaitGroup, cfg *platform.WebhookWorkerConfig, logger zerolog.Logger) <-chan string {
	ch := make(chan string, invalidationChannelBuffer)
	wg.Go(func() {
		defer logging.RecoverPanic(logger, "invalidation.psubscribe", nil)
		opt := valkey.ClientOption{
			InitAddress: cfg.ValkeyConfig.Addrs,
			Password:    cfg.ValkeyConfig.Password,
		}
		if cfg.ValkeyConfig.MasterName != "" {
			opt.Sentinel = valkey.SentinelOption{MasterSet: cfg.ValkeyConfig.MasterName}
		}
		client, err := valkey.NewClient(opt)
		if err != nil {
			logger.Warn().Err(err).
				Msg("Invalidation Valkey client unavailable; cache will use TTL-only refresh")
			return
		}
		defer client.Close()

		pattern := provisioning.WebhookInvalidationSubjectPrefix + "*"
		// Pass application ctx so client.Receive exits promptly on SIGTERM (§VII).
		err = client.Receive(ctx,
			client.B().Psubscribe().Pattern(pattern).Build(),
			func(msg valkey.PubSubMessage) {
				tenantID := strings.TrimPrefix(msg.Channel, provisioning.WebhookInvalidationSubjectPrefix)
				if tenantID == "" {
					return
				}
				select {
				case ch <- tenantID:
				default:
					logger.Warn().Str(logging.LogKeyTenantUUID, tenantID).
						Msg("Invalidation channel full; refresh delayed to next TTL tick")
				}
			},
		)
		if err != nil && ctx.Err() == nil {
			logger.Warn().Err(err).Msg("Invalidation PSUBSCRIBE error; falling back to TTL refresh")
		}
	})
	return ch
}
