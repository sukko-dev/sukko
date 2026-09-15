// Provisioning service entry point for tenant management.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	grpcmetadata "google.golang.org/grpc/metadata"

	provisioningv1 "github.com/sukko-dev/sukko/gen/proto/sukko/provisioning/v1"
	"github.com/sukko-dev/sukko/internal/provisioning"
	"github.com/sukko-dev/sukko/internal/provisioning/adminui"
	"github.com/sukko-dev/sukko/internal/provisioning/api"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/provisioning/eventbus"
	"github.com/sukko-dev/sukko/internal/provisioning/grpcserver"
	"github.com/sukko-dev/sukko/internal/provisioning/repository"
	"github.com/sukko-dev/sukko/internal/provisioning/revocation"
	"github.com/sukko-dev/sukko/internal/shared/analytics"
	"github.com/sukko-dev/sukko/internal/shared/auth"
	"github.com/sukko-dev/sukko/internal/shared/crypto"
	"github.com/sukko-dev/sukko/internal/shared/database"
	"github.com/sukko-dev/sukko/internal/shared/license"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/migrations"
	"github.com/sukko-dev/sukko/internal/shared/platform"
	"github.com/sukko-dev/sukko/internal/shared/profiling"
	"github.com/sukko-dev/sukko/internal/shared/tracing"
	webhookconst "github.com/sukko-dev/sukko/internal/webhook"
)

const serviceName = "provisioning"

// grpcKeepaliveMinTime is the minimum interval the gRPC server tolerates between client
// keepalive pings. It MUST be <= the provapi stream clients' keepalive interval (20s) or the
// server sends GOAWAY "too_many_pings" and tears down long-lived streams.
const grpcKeepaliveMinTime = 10 * time.Second

// base64 encoding variants for bootstrap key decoding.
var (
	base64Std = base64.StdEncoding
	base64Raw = base64.RawStdEncoding
)

func main() {
	// Bootstrap logger for pre-config startup (zerolog without config dependency)
	bootLogger := logging.BootstrapLogger(serviceName)

	// Load configuration first (env vars + envDefaults)
	cfg, err := platform.LoadProvisioningConfig(bootLogger)
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

	// Print configuration
	cfg.Print()

	// --validate-config: validate and exit
	if *validateConfig {
		bootLogger.Info().Msg("Configuration is valid")
		os.Exit(0)
	}

	// Explicit topic namespace (validated + normalized at config load).
	topicNamespace := cfg.KafkaTopicNamespace
	bootLogger.Info().Str("namespace", topicNamespace).Str("environment", cfg.Environment).Msg("Topic namespace set")

	// Initialize structured logger
	structuredLogger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevel(cfg.LogLevel),
		Format:      logging.LogFormat(cfg.LogFormat),
		ServiceName: serviceName,
	})

	structuredLogger.Info().Int("gomaxprocs", runtime.GOMAXPROCS(0)).Msg("GOMAXPROCS set by Go runtime (container-aware)")

	// Log edition
	structuredLogger.Info().
		Str("edition", cfg.EditionManager().Edition().String()).
		Str("org", cfg.EditionManager().Org()).
		Msg("Sukko edition resolved")
	if cfg.WebhookAllowHTTP {
		structuredLogger.Warn().Msg("WEBHOOK_ALLOW_HTTP=true: plain-HTTP delivery enabled — webhook payloads and signatures transmitted without TLS")
	}

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
	// pprof endpoints are registered on the HTTP server mux via the router
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

	// Create cancellable context for service lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// Create event bus for gRPC streaming notifications
	bus := eventbus.New(structuredLogger)

	// Run database migrations (embedded SQL files)
	if err := database.RunMigrations(ctx, cfg.DatabaseURL, migrations.Postgres, structuredLogger); err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to run database migrations")
	}

	// Open analytics pool early so the SSE handler can be passed to NewRouter.
	// Returns (nil, nil) when ANALYTICS_ENABLED=false — that's the default.
	analyticsPool, err := platform.OpenAnalyticsPool(ctx, cfg.AnalyticsConfig)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to open analytics pool")
	}
	if analyticsPool != nil {
		defer analyticsPool.Close()
	}

	// Open database connection pool (PostgreSQL via pgxpool)
	pool, err := repository.OpenDatabase(ctx, cfg.DatabaseURL)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to open database")
	}
	defer pool.Close()
	structuredLogger.Info().Msg("Database pool opened")

	// Initialize repositories
	tenantRepo := repository.NewTenantRepository(pool)
	keyRepo := repository.NewKeyRepository(pool)
	apiKeyRepo := repository.NewAPIKeyStore(pool)
	routingRulesRepo := repository.NewRoutingRulesRepository(pool, structuredLogger, "provisioning")
	quotaRepo := repository.NewQuotaRepository(pool)
	auditRepo := repository.NewAuditRepository(pool)
	channelRulesRepo := repository.NewChannelRulesRepository(pool)
	pushCredentialsRepo, err := repository.NewCredentialsRepository(pool, cfg.CredentialsEncryptionKey)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create push credentials repository")
	}
	pushChannelConfigRepo := repository.NewChannelConfigRepository(pool)

	// Parse encryption key for license state persistence (same key as push credentials)
	encryptionKey, err := crypto.ParseEncryptionKey(cfg.CredentialsEncryptionKey)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to parse CREDENTIALS_ENCRYPTION_KEY")
	}
	licenseStateRepo := repository.NewLicenseStateRepository(pool, encryptionKey)

	// Load license from DB (takes precedence over env var).
	// The Manager was already created from SUKKO_LICENSE_KEY during config loading.
	// If a newer key exists in DB (from a previous webhook reload), reload the Manager.
	var currentLicenseKey string
	if dbKey, err := licenseStateRepo.Load(ctx); err != nil {
		structuredLogger.Warn().Err(err).Msg("failed to load license from DB, using env var")
	} else if dbKey != "" {
		if err := cfg.EditionManager().Reload(dbKey); err != nil {
			structuredLogger.Warn().Err(err).Msg("DB license key failed reload, using env var")
		} else {
			currentLicenseKey = dbKey
			structuredLogger.Info().
				Str("edition", cfg.EditionManager().Edition().String()).
				Msg("license loaded from DB (takes precedence over SUKKO_LICENSE_KEY)")
		}
	}
	if currentLicenseKey == "" {
		currentLicenseKey = cfg.LicenseKey // fall back to env var
	}

	// Kafka admin disabled — topic creation is handled by ws-server's KafkaBackend
	kafkaAdmin := provisioning.NewNoopKafkaAdmin()
	structuredLogger.Info().Msg("Kafka admin disabled (topic creation moved to ws-server)")

	webhookRepo := repository.NewWebhookRepository(pool, structuredLogger)

	// Webhook invalidation publisher — built before NewService() so it can be wired into ServiceConfig.
	// Uses the same PROVISIONING_VALKEY_ADDRS as the connections client; a separate client is
	// created so shutdown ordering is independent.
	var invalidationPublisher provisioning.WebhookCacheInvalidator
	if mgr := cfg.EditionManager(); mgr != nil && license.EditionHasFeature(mgr.Edition(), license.Webhooks) {
		invalidValkeyClient, ivErr := api.BuildConnectionsValkeyClient(*cfg)
		if ivErr != nil {
			structuredLogger.Fatal().Err(ivErr).Msg("Failed to create webhook invalidation Valkey client")
		}
		defer invalidValkeyClient.Close()
		invalidationPublisher = provisioning.NewInvalidationPublisher(invalidValkeyClient, structuredLogger)
	}

	svc, err := provisioning.NewService(provisioning.ServiceConfig{
		TenantStore:                 tenantRepo,
		KeyStore:                    keyRepo,
		APIKeyStore:                 apiKeyRepo,
		RoutingRulesStore:           routingRulesRepo,
		QuotaStore:                  quotaRepo,
		AuditStore:                  auditRepo,
		ChannelRulesStore:           channelRulesRepo,
		KafkaAdmin:                  kafkaAdmin,
		EventBus:                    bus,
		TopicNamespace:              topicNamespace,
		DefaultPartitions:           cfg.DefaultPartitions,
		DefaultRetentionMs:          cfg.DefaultRetentionMs,
		MaxTopicsPerTenant:          cfg.MaxTopicsPerTenant,
		MaxStorageBytes:             cfg.MaxStorageBytes,
		DefaultProducerRate:         cfg.ProducerByteRate,
		DefaultConsumerRate:         cfg.ConsumerByteRate,
		DeprovisionGraceDays:        cfg.DeprovisionGraceDays,
		MaxRoutingRulesPerTenant:    cfg.MaxRoutingRulesPerTenant,
		MaxTopicsPerRule:            cfg.MaxTopicsPerRule,
		DeadLetterTopicPartitions:   cfg.DeadLetterTopicPartitions,
		DeadLetterTopicRetentionMs:  cfg.DeadLetterTopicRetentionMs,
		InfraTopicReplicationFactor: cfg.InfraTopicReplicationFactor,
		SlugRenameTopicHoldPeriod:   cfg.SlugRenameTopicHoldPeriod,
		EditionManager:              cfg.EditionManager(),
		Logger:                      structuredLogger,
		WebhookStore:                webhookRepo,
		EncryptionKey:               encryptionKey,
		MaxWebhooksPerTenant:        cfg.MaxWebhooksPerTenant,
		WebhookAllowHTTP:            cfg.WebhookAllowHTTP,
		InvalidationPublisher:       invalidationPublisher,
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create provisioning service")
	}

	// Run startup scan for pending slug renames (informational — failure must not block startup).
	if err := svc.Start(ctx); err != nil {
		structuredLogger.Warn().Err(err).Msg("Startup scan for pending slug renames failed (non-fatal)")
	}

	// Set up authentication
	// Create key registry from existing database connection
	keyRegistry, err := auth.NewKeyRegistry(auth.KeyRegistryConfig{
		Pool:            pool,
		RefreshInterval: cfg.KeyRegistryRefreshInterval,
		QueryTimeout:    cfg.KeyRegistryQueryTimeout,
		Logger:          structuredLogger.With().Str("component", "key_registry").Logger(),
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create key registry")
	}
	defer func() { _ = keyRegistry.Close() }() // Close error non-actionable during shutdown

	// Build validator config. The tenant resolver binds the JWT tenant_id claim
	// to the signing key's owning tenant UUID (grace-aware, so old-slug tokens
	// still resolve during a rename hold window).
	tenantResolver := provauth.NewGraceTenantResolver(tenantRepo.GetIDBySlugWithGrace, cfg.SlugRenameTopicHoldPeriod)
	validatorCfg := auth.MultiTenantValidatorConfig{
		KeyRegistry:     keyRegistry,
		RequireTenantID: true,
		TenantResolver:  tenantResolver,
	}

	// Create multi-tenant validator
	var validator *auth.MultiTenantValidator
	validator, err = auth.NewMultiTenantValidator(validatorCfg)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create validator")
	}

	structuredLogger.Info().Msg("Authentication enabled")

	// Initialize admin key repository and registry
	adminKeyRepo := repository.NewAdminKeyRepository(pool)
	adminKeyRegistry := provauth.NewAdminKeyRegistry()

	// Bootstrap: auto-register admin key from env var if no keys exist
	if cfg.AdminBootstrapKey != "" {
		if err := bootstrapAdminKey(ctx, cfg.AdminBootstrapKey, adminKeyRepo, adminKeyRegistry, structuredLogger); err != nil {
			structuredLogger.Warn().Err(err).Msg("admin key bootstrap failed")
		}
	} else {
		// Load existing keys into cache
		loadAdminKeyCache(ctx, adminKeyRepo, adminKeyRegistry, structuredLogger)
	}

	adminValidator := provauth.NewAdminValidator(adminKeyRegistry)

	// Initialize license handler for hot-reload endpoint
	licenseHandler := api.NewLicenseHandler(cfg.EditionManager(), licenseStateRepo, bus, structuredLogger)
	licenseHandler.SetCurrentKey(currentLicenseKey)

	// Initialize token revocation store (background prune goroutine; Close on shutdown)
	revStore := revocation.New(structuredLogger)
	defer revStore.Close()
	revHandler := api.NewRevocationHandler(revStore, bus, cfg.TokenRevocationMaxLifetime, cfg.TokenRevocationRateLimitPerMinute, cfg.TokenRevocationRateLimitBurst, structuredLogger)

	// Initialize webhook handler (Pro edition — always wired; RequireFeature gate in router).
	webhookHandler, err := api.NewWebhookHandler(svc, structuredLogger)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create webhook handler")
	}

	// Initialize webhook test handler — dials the worker's internal gRPC server (Pro+ only).
	var webhookTestHandler *api.WebhookTestHandler
	if mgr := cfg.EditionManager(); mgr != nil && license.EditionHasFeature(mgr.Edition(), license.Webhooks) &&
		cfg.WebhookWorkerGRPCAddr != "" {
		// Outbound client interceptor: adds WEBHOOK_INTERNAL_TOKEN to outgoing metadata.
		internalToken := cfg.WebhookInternalToken // capture for closure
		tokenInterceptor := func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = grpcmetadata.AppendToOutgoingContext(ctx, platform.GRPCInternalTokenMetadataKey, internalToken)
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		workerConn, wcErr := grpc.NewClient(cfg.WebhookWorkerGRPCAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(tokenInterceptor),
		)
		if wcErr != nil {
			structuredLogger.Fatal().Err(wcErr).Str("addr", cfg.WebhookWorkerGRPCAddr).
				Msg("Failed to dial webhook worker internal gRPC")
		}
		defer func() { _ = workerConn.Close() }()
		workerInternalClient := provisioningv1.NewWebhookWorkerInternalServiceClient(workerConn)

		// Two Valkey-backed rate limiters for the /test endpoint:
		// one enforcing the per-webhook limit (10/min) and one the per-tenant limit (30/min).
		if invalidValkeyClientForRL, rlErr := api.BuildConnectionsValkeyClient(*cfg); rlErr == nil {
			defer invalidValkeyClientForRL.Close()
			rlWebhook := api.NewValkeyRateLimiter(invalidValkeyClientForRL,
				webhookconst.WebhookTestRateLimitPerWebhook, webhookconst.WebhookTestRateLimitWindow, structuredLogger)
			rlTenant := api.NewValkeyRateLimiter(invalidValkeyClientForRL,
				webhookconst.WebhookTestRateLimitPerTenant, webhookconst.WebhookTestRateLimitWindow, structuredLogger)
			wth, wthErr := api.NewWebhookTestHandler(workerInternalClient, rlWebhook, rlTenant, structuredLogger)
			if wthErr != nil {
				structuredLogger.Fatal().Err(wthErr).Msg("Failed to create webhook test handler")
			}
			webhookTestHandler = wth
		} else {
			structuredLogger.Warn().Err(rlErr).Msg("Webhook test rate limiter Valkey unavailable; test endpoint will fail-open")
			wth, wthErr := api.NewWebhookTestHandler(workerInternalClient,
				&permitAllRateLimiter{}, &permitAllRateLimiter{}, structuredLogger)
			if wthErr != nil {
				structuredLogger.Fatal().Err(wthErr).Msg("Failed to create webhook test handler")
			}
			webhookTestHandler = wth
		}
	}

	// Initialize connections registry Valkey client and handler (Pro+ only).
	var connectionsHandler *api.ConnectionsHandler
	if mgr := cfg.EditionManager(); mgr != nil && license.EditionHasFeature(mgr.Edition(), license.ConnectionsAPI) {
		connectionsValkeyClient, cvcErr := api.BuildConnectionsValkeyClient(*cfg)
		if cvcErr != nil {
			structuredLogger.Fatal().Err(cvcErr).Msg("Failed to create connections registry Valkey client")
		}
		defer connectionsValkeyClient.Close()
		connectionsHandler = api.NewConnectionsHandler(api.ConnectionsHandlerParams{
			Client:         connectionsValkeyClient,
			Cfg:            *cfg,
			EditionManager: mgr,
			Logger:         structuredLogger,
			WG:             &wg,
			ServiceCtx:     ctx,
			Service:        svc,
		})
	}

	// Create the SSE handler once — shared by both the REST API and Admin UI routes.
	// Creating it twice would produce two independent semaphores and query goroutines.
	var sharedAnalyticsSSEHandler *api.AnalyticsSSEHandler
	if analyticsPool != nil {
		sharedAnalyticsSSEHandler = api.NewAnalyticsSSEHandler(
			analyticsPool,
			cfg.SSEMaxConns,
			cfg.FlushInterval,
			cfg.EditionManager(),
			structuredLogger,
		)
	}

	// Initialize Admin UI handler (returns nil when ADMIN_UI_ENABLED=false — router nil-guards).
	adminUIHandler, err := adminui.NewHandler(cfg.AdminUIConfig, svc, sharedAnalyticsSSEHandler, cfg.EditionManager(), structuredLogger)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create Admin UI handler")
	}

	// Initialize push handlers (Enterprise — RequireFeature gate in the router). Wired here so
	// the REST push routes are actually registered; both resolve the client tenant slug → UUID
	// via svc.GetTenantBySlug before writing the UUID-typed columns.
	pushCredentialHandler, err := api.NewPushCredentialHandler(pushCredentialsRepo, svc.GetTenantBySlug, bus, structuredLogger, cfg.EditionManager())
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create push credential handler")
	}
	pushChannelHandler, err := api.NewPushChannelHandler(pushChannelConfigRepo, svc.GetTenantBySlug, bus, structuredLogger)
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create push channel handler")
	}

	// Initialize HTTP router
	router, err := api.NewRouter(api.RouterConfig{
		Service:               svc,
		ProvisioningConfig:    *cfg,
		Logger:                structuredLogger,
		RateLimit:             cfg.APIRateLimitPerMinute,
		Validator:             validator,
		AdminValidator:        adminValidator,
		AdminKeyRegistry:      adminKeyRegistry,
		AdminKeyRepo:          adminKeyRepo,
		EventBus:              bus,
		LicenseHandler:        licenseHandler,
		CORSAllowedOrigins:    cfg.CORSAllowedOrigins,
		CORSMaxAge:            cfg.CORSMaxAge,
		ConfigHandler:         platform.ConfigHandler(cfg),
		EditionManager:        cfg.EditionManager(),
		PprofEnabled:          cfg.PprofEnabled,
		RevocationHandler:     revHandler,
		ConnectionsHandler:    connectionsHandler,
		WebhookHandler:        webhookHandler,
		WebhookTestHandler:    webhookTestHandler,
		AnalyticsSSEHandler:   sharedAnalyticsSSEHandler,
		AdminUIHandler:        adminUIHandler,
		PushCredentialHandler: pushCredentialHandler,
		PushChannelHandler:    pushChannelHandler,
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to initialize HTTP router")
	}

	// Wrap router with tracing middleware (noop when tracing disabled)
	tracedHandler := tracing.HTTPMiddleware(router)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:         cfg.Addr,
		Handler:      tracedHandler,
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	}

	// Create gRPC server with interceptors
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	grpcListener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", grpcAddr)
	if err != nil {
		structuredLogger.Fatal().Err(err).Int("port", cfg.GRPCPort).Msg("Failed to listen on gRPC port")
	}

	// Primary gRPC server — ProvisioningInternalService (ws-server, ws-gateway).
	// Internal pod-to-pod; no auth interceptor needed.
	grpcSrv := grpc.NewServer(
		tracing.StatsHandler(),
		// Tolerate the provapi stream clients' keepalive (gateway/ws-server ping every 20s with
		// PermitWithoutStream — see provapi.provAPIKeepaliveTime). Without a matching enforcement
		// policy the server uses gRPC's default (MinTime 5m, PermitWithoutStream false) and kills
		// every long-lived stream with GOAWAY "too_many_pings" — which drops tenant-config/key
		// propagation to the gateway and makes it 401 all connections. MinTime MUST be <= the
		// client keepalive interval.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             grpcKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.ChainStreamInterceptor(
			grpcserver.RecoveryStreamInterceptor(structuredLogger),
			grpcserver.LoggingStreamInterceptor(structuredLogger),
			grpcserver.MetricsStreamInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			grpcserver.RecoveryUnaryInterceptor(structuredLogger),
			grpcserver.LoggingUnaryInterceptor(structuredLogger),
			grpcserver.MetricsUnaryInterceptor(),
		),
	)
	grpcStreamServer, err := grpcserver.NewServer(svc, bus, structuredLogger, grpcserver.ServerConfig{
		MaxTenantsFetchLimit:  cfg.MaxTenantsFetchLimit,
		RevocationStore:       revStore,
		PushCredentialsRepo:   pushCredentialsRepo,
		PushChannelConfigRepo: pushChannelConfigRepo,
		CurrentLicenseKey:     licenseHandler.CurrentKey,
		SlugRenameHoldPeriod:  cfg.SlugRenameTopicHoldPeriod,
	})
	if err != nil {
		structuredLogger.Fatal().Err(err).Msg("Failed to create gRPC stream server")
	}
	provisioningv1.RegisterProvisioningInternalServiceServer(grpcSrv, grpcStreamServer)

	// Webhook-worker gRPC server — WebhookWorkerService on a dedicated port.
	// Separate from the primary gRPC server so auth is enforced at the listener
	// level rather than via FullMethod filtering inside a shared interceptor.
	// Only started for Pro/Enterprise editions (§IV: optional deps use nil-guarded gates).
	var webhookWorkerGRPCSrv *grpc.Server
	var webhookWorkerGRPCAddr string
	if mgr := cfg.EditionManager(); mgr != nil && license.EditionHasFeature(mgr.Edition(), license.Webhooks) {
		webhookWorkerGRPCAddr = fmt.Sprintf(":%d", cfg.WebhookWorkerGRPCPort)
		webhookWorkerGRPCListener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", webhookWorkerGRPCAddr)
		if err != nil {
			structuredLogger.Fatal().Err(err).Int("port", cfg.WebhookWorkerGRPCPort).Msg("Failed to listen on webhook-worker gRPC port")
		}
		webhookWorkerGRPCSrv = grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpcserver.RecoveryUnaryInterceptor(structuredLogger),
				grpcserver.LoggingUnaryInterceptor(structuredLogger),
				grpcserver.MetricsUnaryInterceptor(),
				grpcserver.WebhookWorkerAuthUnaryInterceptor(cfg.WebhookInternalToken),
			),
		)
		webhookWorkerSrv, err := grpcserver.NewWebhookWorkerServer(webhookRepo, invalidationPublisher, structuredLogger)
		if err != nil {
			structuredLogger.Fatal().Err(err).Msg("Failed to create webhook worker gRPC server")
		}
		provisioningv1.RegisterWebhookWorkerServiceServer(webhookWorkerGRPCSrv, webhookWorkerSrv)
		wg.Go(func() {
			defer logging.RecoverPanic(structuredLogger, "webhookWorkerGRPC.Serve", nil)
			structuredLogger.Info().Str("addr", webhookWorkerGRPCAddr).Msg("Starting webhook-worker gRPC server")
			if err := webhookWorkerGRPCSrv.Serve(webhookWorkerGRPCListener); err != nil {
				structuredLogger.Error().Err(err).Msg("Webhook-worker gRPC server error")
				cancel()
			}
		})
	}

	// Start lifecycle manager (background job for tenant cleanup)
	var lifecycleManager *provisioning.LifecycleManager
	if cfg.LifecycleManagerEnabled {
		lm, lmErr := provisioning.NewLifecycleManager(provisioning.LifecycleManagerConfig{
			Service:         svc,
			Interval:        cfg.LifecycleCheckInterval,
			DeletionTimeout: cfg.DeletionTimeout,
			Logger:          structuredLogger,
		})
		if lmErr != nil {
			structuredLogger.Fatal().Err(lmErr).Msg("Failed to create lifecycle manager")
		}
		lifecycleManager = lm
		lifecycleManager.Start()
		defer lifecycleManager.Stop()
		structuredLogger.Info().Dur("interval", cfg.LifecycleCheckInterval).Msg("Lifecycle manager started")
	}

	// Start AnalyticsManager (rollup + partition maintenance) when analytics is enabled.
	if analyticsPool != nil {
		pm := analytics.NewPartitionManager(analyticsPool, structuredLogger)
		rm := analytics.NewRollupManager(analyticsPool, structuredLogger)
		analyticsManager := provisioning.NewAnalyticsManager(provisioning.AnalyticsManagerConfig{
			PartitionManager: pm,
			RollupManager:    rm,
			PartmanInterval:  cfg.PartmanInterval,
			Logger:           structuredLogger,
		})
		analyticsManager.Start(ctx)
		defer analyticsManager.Stop()
	}
	if cfg.WSPodID != "" && cfg.SukkoPodID == "" {
		structuredLogger.Warn().Msg("WS_POD_ID is deprecated; set SUKKO_POD_ID via Kubernetes Downward API")
	}

	// Start primary gRPC server in goroutine
	wg.Go(func() {
		defer logging.RecoverPanic(structuredLogger, "grpc.Serve", nil)
		structuredLogger.Info().Str("addr", grpcAddr).Msg("Starting gRPC server")
		if err := grpcSrv.Serve(grpcListener); err != nil {
			structuredLogger.Error().Err(err).Msg("gRPC server error")
			cancel()
		}
	})

	// Start HTTP server in goroutine
	wg.Go(func() {
		defer logging.RecoverPanic(structuredLogger, "http.ListenAndServe", nil)
		structuredLogger.Info().Str("addr", cfg.Addr).Msg("Starting provisioning HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			structuredLogger.Error().Err(err).Msg("HTTP server error")
			cancel()
		}
	})

	// Wait for interrupt signal or context cancellation (from server error)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}

	structuredLogger.Info().Msg("Shutting down provisioning service")

	// Cancel context to stop background goroutines
	cancel()

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		structuredLogger.Error().Err(err).Msg("Error during HTTP server shutdown")
	}

	// Graceful stop gRPC server(s)
	grpcSrv.GracefulStop()
	if webhookWorkerGRPCSrv != nil {
		webhookWorkerGRPCSrv.GracefulStop()
	}

	// Wait for background goroutines
	wg.Wait()

	structuredLogger.Info().Msg("Provisioning service gracefully shut down")
}

// bootstrapAdminKey auto-registers the bootstrap public key if no admin keys exist.
// One-time only — ignored if keys already exist in the database.
func bootstrapAdminKey(ctx context.Context, bootstrapKeyBase64 string, repo *repository.AdminKeyRepository, registry *provauth.AdminKeyRegistry, logger zerolog.Logger) error {
	count, err := repo.CountActive(ctx)
	if err != nil {
		return fmt.Errorf("count active admin keys: %w", err)
	}
	if count > 0 {
		logger.Debug().Int("existing_keys", count).Msg("admin keys exist, skipping bootstrap")
		loadAdminKeyCache(ctx, repo, registry, logger)
		return nil
	}

	// Decode base64 → Ed25519 public key → PEM for storage
	decoded, err := decodeBootstrapKey(bootstrapKeyBase64)
	if err != nil {
		return fmt.Errorf("decode bootstrap key: %w", err)
	}

	pemKey, err := marshalPublicKeyPEM(decoded)
	if err != nil {
		return fmt.Errorf("marshal bootstrap key to PEM: %w", err)
	}

	key := &repository.AdminKey{
		KeyID:        provauth.BootstrapAdminKeyID,
		Name:         "bootstrap",
		Algorithm:    "EdDSA",
		PublicKey:    pemKey,
		RegisteredBy: "system",
	}
	if err := repo.Create(ctx, key); err != nil {
		return fmt.Errorf("register bootstrap key: %w", err)
	}

	logger.Info().Str("key_id", provauth.BootstrapAdminKeyID).Msg("admin key auto-registered from bootstrap")

	// Load into cache
	loadAdminKeyCache(ctx, repo, registry, logger)
	return nil
}

// loadAdminKeyCache loads all active admin keys from DB into the in-memory registry.
func loadAdminKeyCache(ctx context.Context, repo *repository.AdminKeyRepository, registry *provauth.AdminKeyRegistry, logger zerolog.Logger) {
	keys, err := repo.ListActive(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load admin keys into cache")
		return
	}

	keyInfos := make([]*auth.KeyInfo, 0, len(keys))
	for _, k := range keys {
		pubKey, err := provauth.ParsePublicKeyPEM(k.PublicKey)
		if err != nil {
			logger.Warn().Err(err).Str("key_id", k.KeyID).Msg("skipping admin key with invalid key material")
			continue
		}
		keyInfos = append(keyInfos, provauth.AdminKeyToKeyInfo(k.KeyID, k.Name, k.Algorithm, pubKey))
	}

	registry.Refresh(keyInfos)
	logger.Info().Int("keys", len(keyInfos)).Msg("admin key cache loaded")
}

// decodeBootstrapKey decodes a base64-encoded Ed25519 public key (padded or unpadded).
func decodeBootstrapKey(b64 string) ([]byte, error) {
	decoded, err := base64Std.DecodeString(b64)
	if err != nil {
		decoded, err = base64Raw.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("invalid base64: %w", err)
		}
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf("expected 32 bytes for Ed25519 public key, got %d", len(decoded))
	}
	return decoded, nil
}

// permitAllRateLimiter is a fallback RateLimiter that always allows requests.
// Used when the Valkey client fails to initialize; the handler logs a warning
// and processes the request without enforcing rate limits (fail-open).
type permitAllRateLimiter struct{}

func (p *permitAllRateLimiter) Allow(_ context.Context, _ string) (bool, time.Duration, error) {
	return true, 0, nil
}

// marshalPublicKeyPEM encodes an Ed25519 public key as PEM.
func marshalPublicKeyPEM(pubKeyBytes []byte) (string, error) {
	pubKey := ed25519.PublicKey(pubKeyBytes)
	der, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("marshal PKIX: %w", err)
	}
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	return string(pemBlock), nil
}
