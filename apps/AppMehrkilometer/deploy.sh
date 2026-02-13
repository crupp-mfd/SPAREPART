#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
APP_DIR_NAME="AppMehrkilometer"
APP_NAME="${APP_NAME:-appmehrkilometer-api}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-mfd-automation}"
ACR_NAME="${ACR_NAME:-acrmfdauto10028}"
IMAGE_REPO="${IMAGE_REPO:-appmehrkilometer}"
TAG="${TAG:-$(date +%Y%m%d-%H%M%S)}"
BUILD_CONTEXT_DIR="${BUILD_CONTEXT_DIR:-$(mktemp -d)}"
CLEANUP_CONTEXT="${CLEANUP_CONTEXT:-1}"

cleanup() {
  if [[ "$CLEANUP_CONTEXT" == "1" ]]; then
    rm -rf "$BUILD_CONTEXT_DIR"
  fi
}
trap cleanup EXIT

"$ROOT_DIR/scripts/create-app-build-context.sh" "$APP_DIR_NAME" "$BUILD_CONTEXT_DIR"

az acr build   --registry "$ACR_NAME"   --image "$IMAGE_REPO:$TAG"   --file "$BUILD_CONTEXT_DIR/apps/$APP_DIR_NAME/Dockerfile"   "$BUILD_CONTEXT_DIR"

az containerapp update   -n "$APP_NAME"   -g "$RESOURCE_GROUP"   --image "$ACR_NAME.azurecr.io/$IMAGE_REPO:$TAG"   --revision-suffix "r$(date +%H%M%S)"

echo "Deployed $APP_NAME with image $ACR_NAME.azurecr.io/$IMAGE_REPO:$TAG"
