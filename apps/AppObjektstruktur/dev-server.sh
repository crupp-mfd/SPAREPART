#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
"$ROOT_DIR/scripts/bootstrap.sh"
source "$ROOT_DIR/.venv/bin/activate"

export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/apps/AppObjektstruktur/src:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_FRONTEND_DIR="$ROOT_DIR/apps/AppObjektstruktur/frontend"
export MFDAPPS_SERVICE="objektstruktur"
exec uvicorn app_objektstruktur.main:app --reload "$@"
