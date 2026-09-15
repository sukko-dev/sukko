# Stage 1: Build the application
FROM golang:1.26.1-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source tree
# Structure: cmd/, internal/ with organized packages
COPY . .

# Build-time version arguments
ARG VERSION=dev
ARG COMMIT_HASH=unknown
ARG BUILD_TIME=unknown

# Build the provisioning service from cmd/provisioning
# -ldflags: -s -w strips debug info, -X injects version variables
# CGO_ENABLED=0 ensures a static binary without C dependencies
ARG TARGETOS
ARG TARGETARCH
# GO_BUILD_TAGS selects Go build tags (e.g. sukko_e2e, which embeds the dev license
# public key so the e2e harness can validate keys it mints at runtime).
# Empty by default = production build embedding the committed keys/sukko.pub.
ARG GO_BUILD_TAGS=""

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -tags "${GO_BUILD_TAGS}" \
    -ldflags="-s -w \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.Version=${VERSION}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.CommitHash=${COMMIT_HASH}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.BuildTime=${BUILD_TIME}'" \
    -o /sukko-provisioning ./cmd/provisioning

# Stage 2: Create the final, minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

# Create data directory for SQLite
RUN mkdir -p /data && chown sukko:sukko /data

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /sukko-provisioning .

# Ensure binary is executable
RUN chmod +x /app/sukko-provisioning && chown sukko:sukko /app/sukko-provisioning

# Switch to non-root user
USER sukko

# Expose the provisioning API port and gRPC port
EXPOSE 8080
EXPOSE 9090

# SQLite data volume
VOLUME ["/data"]

# Command to run the application
# Configuration via environment variables:
# - PROVISIONING_ADDR: Address to listen on (default: :8080)
# - GRPC_PORT: gRPC server port (default: 9090)
# - PROVISIONING_MODE: "api" (database) or "config" (YAML file)
# - DATABASE_DRIVER: "sqlite" (default) or "postgres"
# - DATABASE_PATH: SQLite file path (default: /data/sukko.db)
# - DATABASE_URL: PostgreSQL connection URL (required when driver=postgres)
# - AUTO_MIGRATE: Auto-apply migrations on startup (default: true)
# - AUTH_ENABLED: Enable JWT authentication (default: true)
# - PROVISIONING_ADMIN_TOKEN: Admin bearer token for API auth
# - LOG_LEVEL: debug, info, warn, error, fatal
# - LOG_FORMAT: json, pretty
CMD ["./sukko-provisioning"]
