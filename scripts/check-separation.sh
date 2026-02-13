#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

fail() {
  echo "[separation-check] $*" >&2
  exit 1
}

# Root frontend must only contain index.html
extra_frontend_files="$(find frontend -type f ! -name 'index.html' -print)"
if [[ -n "${extra_frontend_files:-}" ]]; then
  echo "$extra_frontend_files" >&2
  fail "frontend/ darf nur index.html enthalten."
fi

# No shared SQL directory at repo root
if [[ -d sql ]]; then
  fail "Root-Verzeichnis sql/ ist nicht erlaubt. Nutze apps/<App>/sql/."
fi

echo "[separation-check] OK"
