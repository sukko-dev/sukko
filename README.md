# Sukko

Multi-tenant WebSocket infrastructure platform for real-time data distribution. Built for trading and market data — delivers messages from Kafka/Redpanda through WebSocket, SSE, and push notification channels with JWT authentication, per-tenant isolation, and edition-based feature gating.

## Architecture

```
Client SDKs ──┐
              ├──▶ ws-gateway ──▶ ws-server shards
              │      (auth,        (Kafka consumer,
              │       proxy,        Valkey broadcast,
              │       rate limit)   per-client delivery)
              │
              └──▶ provisioning
                    (tenants, keys,
                     rules, license)
```

**Services:**
- **ws-gateway** — WebSocket/SSE/REST reverse proxy with JWT auth, tenant isolation, rate limiting, connection tracking, token revocation
- **ws-server** — Core WebSocket server with sharded connections, Kafka/Redpanda consumption, Valkey broadcast
- **provisioning** — Multi-tenant management API (tenants, signing keys, API keys, routing rules, channel rules, license, token revocation)
- **push** — Push notification service (Web Push, FCM, APNs) with Kafka-driven delivery

**Transports:**
- WebSocket (`/ws`) — bidirectional, real-time
- SSE (`/sse`) — server-sent events, read-only stream
- REST Publish (`POST /api/v1/publish`) — HTTP message injection
- Push Notifications — offline delivery via Web Push, FCM, APNs

## Quick Start

### Prerequisites

- [Go 1.26+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/)
- [Task](https://taskfile.dev/installation/) (optional — for remote K8s operations)

### Local Development

```bash
# Install the CLI (manages the local Docker Compose environment)
brew install sukko-dev/tap/sukko
# or: go install github.com/sukko-dev/cli@latest   (installs as `cli`, not `sukko`)

# Initialize and start the platform
sukko init --defaults
sukko up

# Create a tenant and generate credentials
sukko tenant create --slug my-app --name "My App"
sukko keys create --tenant my-app --generate

# Generate a JWT and connect
sukko token generate --tenant my-app --sub user1
sukko subscribe orders.new --token <jwt>
```

**Local URLs:**

| Service | URL | Description |
|---------|-----|-------------|
| Gateway | `ws://localhost:13000/ws` | WebSocket endpoint |
| Gateway | `http://localhost:13000/sse` | SSE endpoint |
| Gateway | `http://localhost:13000/api/v1/publish` | REST publish |
| Provisioning | `http://localhost:18080` | Admin API |

### Observability (optional)

Select the observability stack when initialising the project, then start it:

```bash
sukko init            # choose observability when prompted
sukko up
```

| Service | URL |
|---------|-----|
| Grafana | `http://localhost:3030` |
| Prometheus | `http://localhost:19091` |

## Development

```bash
# Run tests
cd ws && go test -race ./...

# Build binaries
cd ws && go build ./cmd/server
cd ws && go build ./cmd/gateway
cd ws && go build ./cmd/push

# Lint
cd ws && golangci-lint run ./...

# Proto codegen
cd ws && buf generate
```

## Remote Kubernetes

```bash
# Deploy to GKE
task k8s:setup PLATFORM=gke ENV=demo

# Operations
task k8s:status ENV=demo
task k8s:logs ENV=demo
task k8s:deploy ENV=demo      # Helm upgrade
task k8s:reload ENV=demo      # Restart pods
```

After deployment, provision tenants via sukko-cli:
```bash
sukko tenant create --id <tenant> --name "<name>"
sukko key create --tenant <tenant> --generate
sukko rules routing set --tenant <tenant> --file routing.json
sukko rules channels set --tenant <tenant> --public "*"
```

## Editions

| Feature | Community | Pro | Enterprise |
|---------|-----------|-----|------------|
| WebSocket transport | Yes | Yes | Yes |
| JWT + API key auth | Yes | Yes | Yes |
| Tenant isolation | Yes | Yes | Yes |
| Tenants | 3 | 50 | Unlimited |
| Connections | 500 | 10,000 | Unlimited |
| SSE transport | - | Yes | Yes |
| REST publish | Yes | Yes | Yes |
| Kafka/Redpanda backend | Yes | Yes | Yes |
| Channel-topic routing | Yes | Yes | Yes |
| Message history & gap recovery | Yes | Yes | Yes |
| Token revocation | - | Yes | Yes |
| Web Push | - | Yes | Yes |
| Mobile push (FCM/APNs) | - | - | Yes |

## Key Technologies

- **Go 1.26+** with modern features
- **franz-go** for Kafka/Redpanda consumption
- **Valkey** for the inter-pod broadcast bus
- **gRPC + protobuf** for internal service communication
- **gorilla/websocket** for WebSocket connections
- **zerolog** for structured logging
- **Prometheus** for metrics
- **Helm 3** for Kubernetes deployments
- **Terraform** for cloud infrastructure (GKE)

## Documentation

- [Engineering principles](docs/engineering-principles.md) — the rules this
  codebase is built and reviewed against. Code comments cite them by section:
  `// per §VII` refers to section VII of that document.
- [Architecture decision records](docs/adr/) — durable decisions with context
  and rejected alternatives; comments cite them as `ADR-NNNN`.
- [sukko-cli README](https://github.com/sukko-dev/cli) — CLI reference
- [Contributing](CONTRIBUTING.md) — how to propose a change, and what a
  mergeable one looks like
- [Security policy](SECURITY.md) — how to report a vulnerability privately

## Licence

Sukko is released under the [Fair Core License, Version 1.0, ALv2 Future
License](LICENSE) (`FCL-1.0-ALv2`).

In short: you may read, use, modify and redistribute the source for any purpose
that does not compete with our business, and each version additionally becomes
available under the Apache License 2.0 on the second anniversary of its release.
Licence-key functionality must not be circumvented. The [LICENSE](LICENSE) file
is the authoritative text — this summary is not a substitute for it.

Copyright 2025-2026 Sukko Pty Ltd.
