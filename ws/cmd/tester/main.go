package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/sukko-dev/sukko/cmd/tester/api"
	"github.com/sukko-dev/sukko/cmd/tester/runner"
	kafkashared "github.com/sukko-dev/sukko/internal/shared/kafka"
	"github.com/sukko-dev/sukko/internal/shared/logging"
	"github.com/sukko-dev/sukko/internal/shared/platform"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
	shutdownTimeout       = 10 * time.Second
)

func main() {
	// Bootstrap logger for config parsing errors; replaced after config is loaded.
	bootLogger := logging.BootstrapLogger("sukko-tester")

	if err := run(); err != nil {
		bootLogger.Fatal().Err(err).Msg("fatal error")
	}
}

func run() error {
	var cfg TesterConfig
	if err := env.Parse(&cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// Canonicalize the topic namespace before validation (the tester parses config inline here,
	// with no platform loader — see the other services' loaders).
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	logger := logging.NewLogger(logging.LoggerConfig{
		Level:       logging.LogLevel(cfg.LogLevel),
		Format:      logging.LogFormat(cfg.LogFormat),
		ServiceName: "sukko-tester",
	})

	r := runner.New(buildRunnerConfig(cfg), logger)

	handler := api.NewRouter(r, cfg.AuthToken, cfg.AdminKeyID, logger)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Go(func() {
		defer logging.RecoverPanic(logger, "http-server", nil)
		logger.Info().Int("port", cfg.Port).Msg("tester API listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	})

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info().Str("signal", sig.String()).Msg("shutting down")
	case err := <-errCh:
		return err
	}

	// Shutdown ordering: cancel tests → stop HTTP → wait for goroutines
	r.StopAll() // cancel running test contexts first

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	r.Wait()  // wait for test goroutines to finish
	wg.Wait() // wait for HTTP server goroutine
	logger.Info().Msg("shutdown complete")
	return nil
}

func buildSecurityConfig(cfg platform.KafkaConnectionConfig) (*kafkashared.SASLConfig, *kafkashared.TLSConfig) {
	var sasl *kafkashared.SASLConfig
	if cfg.KafkaSASLEnabled {
		sasl = &kafkashared.SASLConfig{
			Mechanism: cfg.KafkaSASLMechanism,
			Username:  cfg.KafkaSASLUsername,
			Password:  cfg.KafkaSASLPassword,
		}
	}
	var tlsCfg *kafkashared.TLSConfig
	if cfg.KafkaTLSEnabled {
		tlsCfg = &kafkashared.TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: cfg.KafkaTLSInsecure,
			CAPath:             cfg.KafkaTLSCAPath,
		}
	}
	return sasl, tlsCfg
}

func buildRunnerConfig(cfg TesterConfig) runner.Config {
	sasl, tls := buildSecurityConfig(cfg.KafkaConnectionConfig)
	return runner.Config{
		GatewayURL:             cfg.GatewayURL,
		ProvisioningURL:        cfg.ProvisioningURL,
		Environment:            cfg.Environment,
		KafkaTopicNamespace:    cfg.KafkaTopicNamespace,
		KafkaBrokers:           cfg.KafkaBrokers,
		KafkaSASL:              sasl,
		KafkaTLS:               tls,
		JWTLifetime:            cfg.JWTLifetime,
		JWTRefreshBefore:       cfg.JWTRefreshBefore,
		KeyExpiry:              cfg.KeyExpiry,
		SigningKeyFile:         cfg.SigningKeyFile,
		AdminKeyFile:           cfg.AdminKeyFile,
		PushReceiverHost:       cfg.PushReceiverHost,
		PushReceiverPort:       cfg.PushReceiverPort,
		AdminKeyID:             cfg.AdminKeyID,
		AuthMode:               cfg.AuthMode,
		APIKey:                 cfg.APIKey,
		AuthMixRatio:           cfg.AuthMixRatio,
		AuthUpgradeTimeout:     cfg.AuthUpgradeTimeout,
		GatewayMetricsURL:      cfg.GatewayMetricsURL,
		GatewayMetricsInterval: cfg.GatewayMetricsInterval,
		WebhookBaseURL:         cfg.WebhookBaseURL,
		WebhookDeliveryTimeout: cfg.WebhookDeliveryTimeout,
		WebhookRetryTimeout:    cfg.WebhookRetryTimeout,
	}
}
