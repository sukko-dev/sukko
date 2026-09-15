#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$SCRIPT_DIR/config.env}"

# Load config if exists, otherwise use defaults
if [ -f "$CONFIG_FILE" ]; then
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
fi

ENVIRONMENT="${ENVIRONMENT:-demo}"
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-central1-a}"
INSTANCE_NAME="sukko-tester-${ENVIRONMENT}"

# =============================================================================
# Safety Checks
# =============================================================================

# CRITICAL: Only delete instances with sukko-tester prefix
if [[ ! "$INSTANCE_NAME" =~ ^sukko-tester- ]]; then
  echo "ERROR: Refusing to delete instance without 'sukko-tester-' prefix"
  echo "  Instance Name: $INSTANCE_NAME"
  echo "  This is a safety measure to prevent accidental deletion"
  exit 1
fi

if [ -z "$GCP_PROJECT" ]; then
  echo "ERROR: GCP_PROJECT is required"
  exit 1
fi

# Check if instance exists
if ! gcloud compute instances describe "$INSTANCE_NAME" --zone="$GCP_ZONE" --project="$GCP_PROJECT" &>/dev/null; then
  echo "Instance '${INSTANCE_NAME}' does not exist. Nothing to delete."
  exit 0
fi

# =============================================================================
# Delete Instance
# =============================================================================
echo "Deleting instance: $INSTANCE_NAME"

gcloud compute instances delete "$INSTANCE_NAME" \
  --zone="$GCP_ZONE" \
  --project="$GCP_PROJECT" \
  --quiet

echo ""
echo "=== Instance Deleted Successfully ==="
