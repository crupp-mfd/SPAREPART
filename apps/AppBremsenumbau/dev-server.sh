#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
"$ROOT_DIR/scripts/bootstrap.sh"
source "$ROOT_DIR/.venv/bin/activate"

export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/apps/AppBremsenumbau/src:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_FRONTEND_DIR="$ROOT_DIR/apps/AppBremsenumbau/frontend"
export MFDAPPS_SERVICE="bremsenumbau"
exec uvicorn app_bremsenumbau.main:app --reload "$@"
