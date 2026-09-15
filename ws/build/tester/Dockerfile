# Stage 1: Build the application
FROM golang:1.26.1-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source tree
COPY . .

# Build-time version arguments
ARG VERSION=dev
ARG COMMIT_HASH=unknown
ARG BUILD_TIME=unknown

# Build the tester from cmd/tester
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.Version=${VERSION}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.CommitHash=${COMMIT_HASH}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.BuildTime=${BUILD_TIME}'" \
    -o /sukko-tester ./cmd/tester

# Stage 2: Create the final, minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

WORKDIR /app

# Copy the static binary from the builder stage
COPY --from=builder /sukko-tester .

# Ensure binary is executable
RUN chmod +x /app/sukko-tester && chown sukko:sukko /app/sukko-tester

# Switch to non-root user
USER sukko

# Expose the tester API port (default 8090)
EXPOSE 8090

CMD ["./sukko-tester"]
