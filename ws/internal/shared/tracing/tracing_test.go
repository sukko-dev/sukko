package tracing

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestInit_Disabled(t *testing.T) { //nolint:paralleltest // sets global otel.TracerProvider — Constitution VIII
	cfg := Config{Enabled: false}
	logger := zerolog.Nop()

	shutdown, err := Init(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("Init(disabled) error: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	// Verify noop provider is set
	tp := otel.GetTracerProvider()
	if _, ok := tp.(noop.TracerProvider); !ok {
		t.Errorf("expected noop.TracerProvider, got %T", tp)
	}
}

func TestInit_Stdout(t *testing.T) { //nolint:paralleltest // sets global otel.TracerProvider — Constitution VIII
	cfg := Config{
		Enabled:      true,
		ExporterType: "stdout",
		ServiceName:  "test-service",
		Environment:  "test",
	}
	logger := zerolog.Nop()

	shutdown, err := Init(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("Init(stdout) error: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	// Verify a real (non-noop) provider is set
	tp := otel.GetTracerProvider()
	if _, ok := tp.(noop.TracerProvider); ok {
		t.Error("expected real TracerProvider, got noop")
	}
}

func TestInit_InvalidExporter(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Enabled:      true,
		ExporterType: "invalid",
		ServiceName:  "test-service",
		Environment:  "test",
	}
	logger := zerolog.Nop()

	_, err := Init(context.Background(), cfg, logger)
	if err == nil {
		t.Fatal("expected error for invalid exporter type")
	}
}

func TestCreateExporter_UnknownType(t *testing.T) {
	t.Parallel()
	cfg := Config{ExporterType: "unknown"}
	_, err := createExporter(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unknown exporter type")
	}
}
