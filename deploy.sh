#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
APP_DIR="${APP_DIR:-AppMFD}"
TARGET_SCRIPT="$ROOT_DIR/apps/$APP_DIR/deploy.sh"

if [[ ! -x "$TARGET_SCRIPT" ]]; then
  echo "Deploy-Skript nicht gefunden oder nicht ausführbar: $TARGET_SCRIPT" >&2
  exit 1
fi

"$TARGET_SCRIPT" "$@"
