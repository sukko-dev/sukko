#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$SCRIPT_DIR/config.env}"

# Load config if exists
if [ -f "$CONFIG_FILE" ]; then
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
fi

ENVIRONMENT="${ENVIRONMENT:-demo}"
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-central1-a}"
INSTANCE_NAME="sukko-tester-${ENVIRONMENT}"

if [ -z "$GCP_PROJECT" ]; then
  echo "ERROR: GCP_PROJECT is required"
  exit 1
fi

echo "Connecting to ${INSTANCE_NAME} (via IAP tunnel)..."
gcloud compute ssh "$INSTANCE_NAME" \
  --project="$GCP_PROJECT" \
  --zone="$GCP_ZONE" \
  --tunnel-through-iap
