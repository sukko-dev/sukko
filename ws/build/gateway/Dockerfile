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

# Build the gateway from cmd/gateway
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
    -o /sukko-gateway ./cmd/gateway

# Stage 2: Create the final, minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /sukko-gateway .

# Ensure binary is executable
RUN chmod +x /app/sukko-gateway && chown sukko:sukko /app/sukko-gateway

# Switch to non-root user
USER sukko

# Expose the gateway port (default 3000)
EXPOSE 3000

# Command to run the application
# Configuration via environment variables:
# - GATEWAY_PORT: Port to listen on (default: 3000)
# - GATEWAY_BACKEND_URL: ws-server WebSocket URL
# - LOG_LEVEL: debug, info, warn, error, fatal
# - LOG_FORMAT: json, pretty
CMD ["./sukko-gateway"]
