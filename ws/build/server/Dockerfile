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

# Build the server from cmd/server
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
    -o /sukko-server ./cmd/server

# Stage 2: Create the final, minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS connections (Kafka/Redpanda TLS support)
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /sukko-server .

# Ensure binary is executable
RUN chmod +x /app/sukko-server && chown sukko:sukko /app/sukko-server

# Switch to non-root user
USER sukko

# Expose the load balancer port (default 3005)
EXPOSE 3005

# Command to run the application
# All configuration is driven by environment variables (see platform.ServerConfig).
# Override via env vars: WS_NUM_SHARDS, WS_BASE_PORT, WS_LB_ADDR, etc.
CMD ["./sukko-server"]
