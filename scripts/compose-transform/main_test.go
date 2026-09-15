package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestTransform_BuildReplacedWithImage(t *testing.T) {
	t.Parallel()
	input := `services:
  ws-server:
    build:
      context: ./ws
      dockerfile: build/server/Dockerfile
    ports:
      - "3005:3005"
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if strings.Contains(result, "build:") {
		t.Error("output still contains build:")
	}
	if !strings.Contains(result, "image: ghcr.io/sukko-dev/sukko-server:latest") {
		t.Error("output missing image reference")
	}
	if !strings.Contains(result, `ports:`) {
		t.Error("ports field missing from output")
	}
}

func TestTransform_ServiceWithoutBuildUnchanged(t *testing.T) {
	t.Parallel()
	input := `services:
  nats:
    image: nats:2.10-alpine
    ports:
      - "4222:4222"
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if !strings.Contains(result, "image: nats:2.10-alpine") {
		t.Error("nats image should be unchanged")
	}
	if !strings.Contains(result, `"4222:4222"`) {
		t.Error("nats ports should be unchanged")
	}
}

func TestTransform_UnknownServiceBuildRemoved(t *testing.T) {
	t.Parallel()
	input := `services:
  unknown-svc:
    build:
      context: ./unknown
    ports:
      - "9999:9999"
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if strings.Contains(result, "build:") {
		t.Error("build: should be removed for unknown service")
	}
	if strings.Contains(result, "image:") {
		t.Error("no image: should be added for unknown service")
	}
	if !strings.Contains(result, `"9999:9999"`) {
		t.Error("ports should be preserved")
	}

	warnings := errOut.String()
	if !strings.Contains(warnings, "unknown-svc") {
		t.Error("expected warning about unknown service")
	}
}

func TestTransform_CommentsPreserved(t *testing.T) {
	t.Parallel()
	input := `# Top-level comment
services:
  # Service comment
  ws-server:
    build:
      context: ./ws
      dockerfile: build/server/Dockerfile
    # Port comment
    ports:
      - "3005:3005"
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if !strings.Contains(result, "# Top-level comment") {
		t.Error("top-level comment not preserved")
	}
	if !strings.Contains(result, "# Service comment") {
		t.Error("service comment not preserved")
	}
	if !strings.Contains(result, "# Port comment") {
		t.Error("port comment not preserved")
	}
}

func TestTransform_ServiceOrderPreserved(t *testing.T) {
	t.Parallel()
	input := `services:
  nats:
    image: nats:2.10-alpine
  provisioning:
    build:
      context: ./ws
      dockerfile: build/provisioning/Dockerfile
  ws-server:
    build:
      context: ./ws
      dockerfile: build/server/Dockerfile
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	natsIdx := strings.Index(result, "nats:")
	provIdx := strings.Index(result, "provisioning:")
	serverIdx := strings.Index(result, "ws-server:")

	if natsIdx >= provIdx || provIdx >= serverIdx {
		t.Errorf("service order not preserved: nats=%d, provisioning=%d, ws-server=%d", natsIdx, provIdx, serverIdx)
	}
}

func TestTransform_ConfigsBlockUnchanged(t *testing.T) {
	t.Parallel()
	input := `services:
  nats:
    image: nats:2.10-alpine
configs:
  my-config:
    content: |
      some config data
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if !strings.Contains(result, "configs:") {
		t.Error("configs block missing")
	}
	if !strings.Contains(result, "some config data") {
		t.Error("config content missing")
	}
}

func TestTransform_ProfilesPreserved(t *testing.T) {
	t.Parallel()
	input := `services:
  redis:
    profiles: ["cache"]
    image: redis:7-alpine
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if !strings.Contains(result, "profiles:") {
		t.Error("profiles field missing")
	}
}

func TestTransform_Idempotent(t *testing.T) {
	t.Parallel()
	input := `services:
  ws-server:
    build:
      context: ./ws
      dockerfile: build/server/Dockerfile
    ports:
      - "3005:3005"
  nats:
    image: nats:2.10-alpine
`
	// First transform
	var out1, errOut1 bytes.Buffer
	if err := run(strings.NewReader(input), &out1, &errOut1); err != nil {
		t.Fatalf("first transform error: %v", err)
	}

	// Second transform (on already-transformed output)
	var out2, errOut2 bytes.Buffer
	if err := run(strings.NewReader(out1.String()), &out2, &errOut2); err != nil {
		t.Fatalf("second transform error: %v", err)
	}

	if out1.String() != out2.String() {
		t.Error("transform is not idempotent — second run produced different output")
	}
}

func TestTransform_EmptyInput(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	err := run(strings.NewReader(""), &out, &errOut)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestTransform_InvalidYAML(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	err := run(strings.NewReader("{{invalid yaml"), &out, &errOut)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestTransform_PlatformStrippedWithBuild(t *testing.T) {
	t.Parallel()
	input := `services:
  postgres:
    platform: linux/amd64
    build:
      context: ./docker/postgres-dev
    ports:
      - "15432:5432"
`
	var out, errOut bytes.Buffer
	if err := run(strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("transform error: %v", err)
	}

	result := out.String()
	if strings.Contains(result, "platform:") {
		t.Error("platform: should be stripped — it is a dev-only build constraint")
	}
	if strings.Contains(result, "build:") {
		t.Error("build: should be removed")
	}
	if !strings.Contains(result, "image: postgres:16-alpine") {
		t.Error("image: should be injected for postgres")
	}
}

// TestServiceImageMap_CoversAllBuildServices guards against drift between
// docker-compose.yml and serviceImageMap. If a service with a build: block
// is missing from the map, the sync-compose CI will produce an invalid compose
// file for end users (no image: and no build: → docker compose config fails).
func TestServiceImageMap_CoversAllBuildServices(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Skipf("docker-compose.yml not found at ../../docker-compose.yml: %v", err)
	}

	var out, errOut bytes.Buffer
	if err := run(bytes.NewReader(data), &out, &errOut); err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if warnings := errOut.String(); warnings != "" {
		t.Errorf("transform emitted warnings for build: services not in serviceImageMap:\n%s\nAdd these services to serviceImageMap in main.go", warnings)
	}
}
