# Stage 1: Build
FROM golang:1.26.1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT_HASH=unknown
ARG BUILD_TIME=unknown
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.Version=${VERSION}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.CommitHash=${COMMIT_HASH}' \
    -X 'github.com/sukko-dev/sukko/internal/shared/version.BuildTime=${BUILD_TIME}'" \
    -o /webhook-worker ./cmd/webhook-worker

# Stage 2: Minimal runtime
FROM alpine:3.21

RUN apk --no-cache add ca-certificates

RUN addgroup -S sukko && \
    adduser -u 1000 -S sukko -G sukko

WORKDIR /app

COPY --from=builder /webhook-worker .

RUN chmod +x /app/webhook-worker && chown sukko:sukko /app/webhook-worker

USER sukko

EXPOSE 8083 9095

ENTRYPOINT ["/app/webhook-worker"]
