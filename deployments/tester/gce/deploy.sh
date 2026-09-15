#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$SCRIPT_DIR/config.env}"

# =============================================================================
# Load Configuration
# =============================================================================
if [ ! -f "$CONFIG_FILE" ]; then
  echo "ERROR: Config file not found: $CONFIG_FILE"
  echo ""
  echo "Create it from the example:"
  echo "  cp $SCRIPT_DIR/config.env.example $SCRIPT_DIR/config.env"
  echo "  # Edit config.env with your values"
  exit 1
fi

# shellcheck source=/dev/null
source "$CONFIG_FILE"

# =============================================================================
# Validate Required Config
# =============================================================================
ERRORS=()

[ -z "${ENVIRONMENT:-}" ] && ERRORS+=("ENVIRONMENT is required")
[ -z "${GCP_PROJECT:-}" ] && ERRORS+=("GCP_PROJECT is required")
[ -z "${GCP_ZONE:-}" ] && ERRORS+=("GCP_ZONE is required")
[ -z "${VPC_NETWORK:-}" ] && ERRORS+=("VPC_NETWORK is required (foundation VPC name)")
[ -z "${VPC_SUBNET:-}" ] && ERRORS+=("VPC_SUBNET is required (foundation subnet name)")
[ -z "${GHCR_TOKEN:-}" ] && ERRORS+=("GHCR_TOKEN is required (GitHub PAT with read:packages scope)")
[ -z "${GHCR_IMAGE:-}" ] && ERRORS+=("GHCR_IMAGE is required (e.g., ghcr.io/sukko-dev/sukko-tester:latest)")

# GATEWAY_URL validation: reject empty, placeholder, or localhost
if [ -z "${GATEWAY_URL:-}" ]; then
  ERRORS+=("GATEWAY_URL is required (get via: task k8s:external-ips PLATFORM=gke ENV=demo)")
elif [[ "$GATEWAY_URL" == *"<"* ]]; then
  ERRORS+=("GATEWAY_URL contains a placeholder — replace <gateway-ip> with the actual IP")
elif [[ "$GATEWAY_URL" =~ ^wss?://localhost ]]; then
  ERRORS+=("GATEWAY_URL starts with localhost — the GCE instance cannot reach your local machine")
fi

if [ ${#ERRORS[@]} -gt 0 ]; then
  echo "ERROR: Missing or invalid configuration:"
  for err in "${ERRORS[@]}"; do
    echo "  - $err"
  done
  exit 1
fi

# Defaults
INSTANCE_SIZE="${INSTANCE_SIZE:-e2-medium}"
TESTER_PORT="${TESTER_PORT:-8090}"
PROVISIONING_URL="${PROVISIONING_URL:-}"
TESTER_AUTH_TOKEN="${TESTER_AUTH_TOKEN:-}"
KAFKA_BROKERS="${KAFKA_BROKERS:-}"

INSTANCE_NAME="sukko-tester-${ENVIRONMENT}"

# =============================================================================
# Safety Checks
# =============================================================================

# Check for existing instance (idempotency)
if gcloud compute instances describe "$INSTANCE_NAME" --zone="$GCP_ZONE" --project="$GCP_PROJECT" &>/dev/null; then
  echo "ERROR: Instance '${INSTANCE_NAME}' already exists"
  echo "  Destroy it first: task tester:gce:destroy"
  echo "  Or SSH into it:   task tester:gce:ssh"
  exit 1
fi

# Verify image exists in GHCR before creating instance
echo "Verifying image exists: ${GHCR_IMAGE}"
if ! docker manifest inspect "${GHCR_IMAGE}" &>/dev/null; then
  echo "ERROR: Image not found in registry: ${GHCR_IMAGE}"
  echo "  Push it first: task local:build:push:tester"
  echo ""
  echo "  Make sure you are logged into GHCR:"
  echo "    echo \$GHCR_TOKEN | docker login ghcr.io -u oauth --password-stdin"
  exit 1
fi

# =============================================================================
# Build startup script with embedded env vars
# =============================================================================
STARTUP_SCRIPT=$(cat <<SCRIPT
#!/bin/bash
set -euo pipefail

# Export env vars for startup script
export GHCR_TOKEN="${GHCR_TOKEN}"
export GHCR_IMAGE="${GHCR_IMAGE}"
export GATEWAY_URL="${GATEWAY_URL}"
export PROVISIONING_URL="${PROVISIONING_URL}"
export TESTER_AUTH_TOKEN="${TESTER_AUTH_TOKEN}"
export TESTER_PORT="${TESTER_PORT}"
export KAFKA_BROKERS="${KAFKA_BROKERS}"

$(tail -n +3 "$SCRIPT_DIR/startup-script.sh")
SCRIPT
)

# =============================================================================
# Create Instance
# =============================================================================
echo ""
echo "=== Creating sukko-tester GCE Instance ==="
echo "  Name:        $INSTANCE_NAME"
echo "  Project:     $GCP_PROJECT"
echo "  Zone:        $GCP_ZONE"
echo "  Size:        $INSTANCE_SIZE"
echo "  VPC:         $VPC_NETWORK / $VPC_SUBNET"
echo "  Gateway:     $GATEWAY_URL"
echo "  Tester Port: $TESTER_PORT"
echo "  Public IP:   none (IAP SSH only)"
echo ""

gcloud compute instances create "$INSTANCE_NAME" \
  --project="$GCP_PROJECT" \
  --zone="$GCP_ZONE" \
  --machine-type="$INSTANCE_SIZE" \
  --network="$VPC_NETWORK" \
  --subnet="$VPC_SUBNET" \
  --no-address \
  --image-family=cos-stable \
  --image-project=cos-cloud \
  --metadata=startup-script="$STARTUP_SCRIPT" \
  --tags=sukko-tester \
  --scopes=cloud-platform

# =============================================================================
# Wait for Health Check (via IAP tunnel)
# =============================================================================
echo ""
echo "Waiting for health check via IAP tunnel..."
TIMEOUT=300  # 5 minutes
INTERVAL=15
ELAPSED=0

while [ $ELAPSED -lt $TIMEOUT ]; do
  if gcloud compute ssh "$INSTANCE_NAME" \
    --project="$GCP_PROJECT" \
    --zone="$GCP_ZONE" \
    --tunnel-through-iap \
    --command="curl -sf http://localhost:${TESTER_PORT}/health" &>/dev/null; then
    echo ""
    echo "=== sukko-tester is ready ==="
    echo "  Instance:    $INSTANCE_NAME"
    echo "  Tester URL:  http://localhost:${TESTER_PORT} (via IAP tunnel)"
    echo ""
    echo "Usage (via sukko CLI with IAP port-forward):"
    echo "  gcloud compute ssh $INSTANCE_NAME --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap -- -L ${TESTER_PORT}:localhost:${TESTER_PORT}"
    echo "  sukko test smoke --tester-url http://localhost:${TESTER_PORT}"
    echo ""
    echo "Management:"
    echo "  task tester:gce:logs       View container logs"
    echo "  task tester:gce:ssh        SSH into instance"
    echo "  task tester:gce:destroy    Destroy instance"
    exit 0
  fi
  echo "  ... waiting (${ELAPSED}s / ${TIMEOUT}s)"
  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
done

echo ""
echo "ERROR: Health check timed out after ${TIMEOUT} seconds"
echo "  The instance was created but the tester service may not be running."
echo "  Debug with: task tester:gce:ssh"
echo "  Then run:   docker logs sukko-tester"
exit 1
