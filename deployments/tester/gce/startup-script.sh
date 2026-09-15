#!/bin/bash
set -euo pipefail

echo "=== sukko-tester startup ==="

# Verify Docker is installed (expected on DO Docker marketplace image)
if ! command -v docker &>/dev/null; then
  echo "ERROR: Docker is not installed. Use a Docker marketplace image."
  exit 1
fi

# Wait for Docker daemon to be ready
echo "Waiting for Docker daemon..."
for i in $(seq 1 30); do
  if docker info &>/dev/null; then
    echo "Docker daemon ready"
    break
  fi
  sleep 2
done

if ! docker info &>/dev/null; then
  echo "ERROR: Docker daemon did not start within 60 seconds"
  exit 1
fi

# Authenticate to GHCR
echo "Authenticating to GHCR..."
echo "${GHCR_TOKEN}" | docker login ghcr.io -u oauth --password-stdin

# Pull the tester image
echo "Pulling image: ${GHCR_IMAGE}"
docker pull "${GHCR_IMAGE}"

# Run the tester container
echo "Starting sukko-tester container..."
docker run -d \
  --name sukko-tester \
  --restart=unless-stopped \
  -p "${TESTER_PORT}:${TESTER_PORT}" \
  -e GATEWAY_URL="${GATEWAY_URL}" \
  -e PROVISIONING_URL="${PROVISIONING_URL}" \
  -e TESTER_AUTH_TOKEN="${TESTER_AUTH_TOKEN}" \
  -e TESTER_PORT="${TESTER_PORT}" \
  -e KAFKA_BROKERS="${KAFKA_BROKERS:-}" \
  "${GHCR_IMAGE}"

echo "=== sukko-tester started on port ${TESTER_PORT} ==="
