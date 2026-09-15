// Package tracing provides OpenTelemetry distributed tracing initialization.
// Cold-path only — per-message hot paths are not instrumented.
// When disabled (default), a noop TracerProvider is registered with zero overhead.
package tracing

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config holds tracing configuration parsed from environment variables.
type Config struct {
	Enabled      bool
	ExporterType string // otlp-grpc, otlp-http, stdout
	Endpoint     string
	ServiceName  string
	Environment  string
}

// Init initializes OpenTelemetry tracing. When disabled, registers a noop
// TracerProvider and returns a no-op shutdown function (zero overhead).
// When enabled, creates an exporter, batch span processor, and sets the
// global TracerProvider. The returned shutdown function must be called
// during service shutdown to flush pending spans.
func Init(ctx context.Context, cfg Config, logger zerolog.Logger) (shutdown func(context.Context) error, err error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		logger.Info().Msg("Tracing disabled (noop provider)")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := createExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.DeploymentEnvironmentKey.String(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	logger.Info().
		Str("exporter", cfg.ExporterType).
		Str("endpoint", cfg.Endpoint).
		Str("service", cfg.ServiceName).
		Msg("Tracing enabled")

	return tp.Shutdown, nil
}

// createExporter creates a span exporter based on the configured type.
func createExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	var (
		exporter sdktrace.SpanExporter
		err      error
	)
	switch cfg.ExporterType {
	case "otlp-grpc":
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithInsecure(),
		)
	case "otlp-http":
		exporter, err = otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(cfg.Endpoint),
			otlptracehttp.WithInsecure(),
		)
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	default:
		return nil, fmt.Errorf("unknown exporter type: %s", cfg.ExporterType)
	}
	if err != nil {
		return nil, fmt.Errorf("create %s exporter: %w", cfg.ExporterType, err)
	}
	return exporter, nil
}
