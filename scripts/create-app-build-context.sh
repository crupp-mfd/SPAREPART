#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR_NAME="${1:-}"
TARGET_DIR="${2:-}"

if [[ -z "$APP_DIR_NAME" || -z "$TARGET_DIR" ]]; then
  echo "Usage: $(basename "$0") <AppDirName> <TargetDir>" >&2
  exit 2
fi

SRC_APP_DIR="$ROOT_DIR/apps/$APP_DIR_NAME"
if [[ ! -d "$SRC_APP_DIR" ]]; then
  echo "App directory not found: $SRC_APP_DIR" >&2
  exit 2
fi

mkdir -p "$TARGET_DIR/apps" "$TARGET_DIR/packages"

copy_tree() {
  local src="$1"
  local dst="$2"
  shift 2
  local -a excludes=("$@")
  mkdir -p "$dst"
  if command -v rsync >/dev/null 2>&1; then
    local -a rsync_args=(
      "-a"
      "--delete"
      "--exclude=__pycache__/"
      "--exclude=.pytest_cache/"
      "--exclude=*.pyc"
      "--exclude=.DS_Store"
    )
    for ex in "${excludes[@]-}"; do
      rsync_args+=("--exclude=$ex")
    done
    rsync "${rsync_args[@]}" "$src/" "$dst/"
  else
    rm -rf "$dst"
    mkdir -p "$dst"
    cp -R "$src/." "$dst/"
    for ex in "${excludes[@]-}"; do
      rm -rf "$dst/$ex"
    done
  fi
}

copy_tree "$ROOT_DIR/python" "$TARGET_DIR/python"
copy_tree "$ROOT_DIR/packages/sparepart-shared" "$TARGET_DIR/packages/sparepart-shared"
copy_tree "$SRC_APP_DIR" "$TARGET_DIR/apps/$APP_DIR_NAME" "runtime" "credentials" "legacy_source/Output" "legacy_source/Quellen"

# Keep a lean context by design; credentials are mounted at runtime.
echo "Prepared build context for $APP_DIR_NAME at $TARGET_DIR"
