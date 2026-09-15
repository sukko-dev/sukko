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

# Build the push service from cmd/push
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
    -o /sukko-push ./cmd/push

# Stage 2: Create the final, minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS connections (push provider TLS support)
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /sukko-push .

# Ensure binary is executable
RUN chmod +x /app/sukko-push && chown sukko:sukko /app/sukko-push

# Switch to non-root user
USER sukko

# Expose the gRPC port (default 3008) and HTTP port (default 3009)
EXPOSE 3008 3009

# Command to run the application
# All configuration is driven by environment variables (see push.PushConfig).
# Override via env vars: PUSH_GRPC_PORT, PUSH_HTTP_PORT, PUSH_DRY_RUN, etc.
CMD ["./sukko-push"]
