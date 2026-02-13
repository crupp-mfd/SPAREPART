#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
VENV_DIR="$ROOT_DIR/.venv"
ENV_FILE="$ROOT_DIR/.env"
SERVICE="${SERVICE:-AppMFD}"

case "${SERVICE}" in
  AppMFD) APP_MODULE_DEFAULT="app_mfd.main:app"; APP_SRC_DIR="apps/AppMFD/src"; APP_FRONTEND_DIR="apps/AppMFD/frontend"; APP_SERVICE_ID="appmfd" ;;
  AppObjektstruktur) APP_MODULE_DEFAULT="app_objektstruktur.main:app"; APP_SRC_DIR="apps/AppObjektstruktur/src"; APP_FRONTEND_DIR="apps/AppObjektstruktur/frontend"; APP_SERVICE_ID="objektstruktur" ;;
  AppBremsenumbau) APP_MODULE_DEFAULT="app_bremsenumbau.main:app"; APP_SRC_DIR="apps/AppBremsenumbau/src"; APP_FRONTEND_DIR="apps/AppBremsenumbau/frontend"; APP_SERVICE_ID="bremsenumbau" ;;
  AppTeilenummer) APP_MODULE_DEFAULT="app_teilenummer.main:app"; APP_SRC_DIR="apps/AppTeilenummer/src"; APP_FRONTEND_DIR="apps/AppTeilenummer/frontend"; APP_SERVICE_ID="teilenummer" ;;
  AppWagensuche) APP_MODULE_DEFAULT="app_wagensuche.main:app"; APP_SRC_DIR="apps/AppWagensuche/src"; APP_FRONTEND_DIR="apps/AppWagensuche/frontend"; APP_SERVICE_ID="wagensuche" ;;
  AppRSRD) APP_MODULE_DEFAULT="app_rsrd.main:app"; APP_SRC_DIR="apps/AppRSRD/src"; APP_FRONTEND_DIR="apps/AppRSRD/frontend"; APP_SERVICE_ID="rsrd" ;;
  AppGoldenView) APP_MODULE_DEFAULT="app_goldenview.main:app"; APP_SRC_DIR="apps/AppGoldenView/src"; APP_FRONTEND_DIR="apps/AppGoldenView/frontend"; APP_SERVICE_ID="goldenview" ;;
  AppSQL-API) APP_MODULE_DEFAULT="app_sql_api.main:app"; APP_SRC_DIR="apps/AppSQL-API/src"; APP_FRONTEND_DIR="apps/AppSQL-API/frontend"; APP_SERVICE_ID="sql_api" ;;
  AppMehrkilometer) APP_MODULE_DEFAULT="app_mehrkilometer.main:app"; APP_SRC_DIR="apps/AppMehrkilometer/src"; APP_FRONTEND_DIR="apps/AppMehrkilometer/frontend"; APP_SERVICE_ID="mehrkilometer" ;;
  *) echo "Unbekannter SERVICE: $SERVICE" >&2; exit 1 ;;
esac

APP_MODULE="${APP_MODULE:-$APP_MODULE_DEFAULT}"

"$ROOT_DIR/scripts/bootstrap.sh"

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Bootstrap konnte kein virtuelles Environment erstellen." >&2
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

source "$VENV_DIR/bin/activate"
export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/$APP_SRC_DIR:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_FRONTEND_DIR="$ROOT_DIR/$APP_FRONTEND_DIR"
export MFDAPPS_SERVICE="$APP_SERVICE_ID"

# Sandbox environments can block file watchers; allow disabling reload.
if [[ "${NO_RELOAD:-}" == "1" ]]; then
  exec uvicorn "$APP_MODULE" "$@"
else
  exec uvicorn "$APP_MODULE" --reload "$@"
fi
