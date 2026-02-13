#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
SERVICE="${SERVICE:-AppMFD}"
PORT="${PORT:-8000}"

case "${SERVICE}" in
  AppMFD) APP_MODULE_DEFAULT="app_mfd.main:app"; APP_SRC_DIR="apps/AppMFD/src"; APP_FRONTEND_DIR="apps/AppMFD/frontend" ;;
  AppObjektstruktur) APP_MODULE_DEFAULT="app_objektstruktur.main:app"; APP_SRC_DIR="apps/AppObjektstruktur/src"; APP_FRONTEND_DIR="apps/AppObjektstruktur/frontend" ;;
  AppBremsenumbau) APP_MODULE_DEFAULT="app_bremsenumbau.main:app"; APP_SRC_DIR="apps/AppBremsenumbau/src"; APP_FRONTEND_DIR="apps/AppBremsenumbau/frontend" ;;
  AppTeilenummer) APP_MODULE_DEFAULT="app_teilenummer.main:app"; APP_SRC_DIR="apps/AppTeilenummer/src"; APP_FRONTEND_DIR="apps/AppTeilenummer/frontend" ;;
  AppWagensuche) APP_MODULE_DEFAULT="app_wagensuche.main:app"; APP_SRC_DIR="apps/AppWagensuche/src"; APP_FRONTEND_DIR="apps/AppWagensuche/frontend" ;;
  AppRSRD) APP_MODULE_DEFAULT="app_rsrd.main:app"; APP_SRC_DIR="apps/AppRSRD/src"; APP_FRONTEND_DIR="apps/AppRSRD/frontend" ;;
  AppGoldenView) APP_MODULE_DEFAULT="app_goldenview.main:app"; APP_SRC_DIR="apps/AppGoldenView/src"; APP_FRONTEND_DIR="apps/AppGoldenView/frontend" ;;
  AppSQL-API) APP_MODULE_DEFAULT="app_sql_api.main:app"; APP_SRC_DIR="apps/AppSQL-API/src"; APP_FRONTEND_DIR="apps/AppSQL-API/frontend" ;;
  AppMehrkilometer) APP_MODULE_DEFAULT="app_mehrkilometer.main:app"; APP_SRC_DIR="apps/AppMehrkilometer/src"; APP_FRONTEND_DIR="apps/AppMehrkilometer/frontend" ;;
  *) echo "ERROR: Unbekannter SERVICE '${SERVICE}'." >&2; exit 1 ;;
esac

APP_MODULE="${APP_MODULE:-$APP_MODULE_DEFAULT}"

cd "$ROOT_DIR"
echo "INFO: Host=$(hostname)  User=$(whoami)  PWD=$ROOT_DIR  Service=$SERVICE  Module=$APP_MODULE  Port=$PORT"

pkill -f "uvicorn ${APP_MODULE}" 2>/dev/null || true

source .venv/bin/activate
export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/packages/sparepart-shared/src:$ROOT_DIR/$APP_SRC_DIR:${PYTHONPATH:-}"
export MFDAPPS_HOME="$ROOT_DIR"
export MFDAPPS_FRONTEND_DIR="$ROOT_DIR/$APP_FRONTEND_DIR"

start_uvicorn() {
  local port="$1"
  nohup uvicorn "$APP_MODULE" --host 127.0.0.1 --port "$port" > /tmp/uvicorn.log 2>&1 &
}

selected_port="$PORT"

if command -v lsof >/dev/null 2>&1; then
  existing_pid=$(lsof -tiTCP:"${selected_port}" -sTCP:LISTEN || true)
  if [ -n "${existing_pid}" ]; then
    echo "INFO: Beende bestehenden Prozess auf Port ${selected_port} (PID ${existing_pid}) ..."
    kill "${existing_pid}" 2>/dev/null || true
    sleep 1
    if lsof -iTCP:"${selected_port}" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "WARN: Prozess läuft noch, versuche kill -9 ..."
      kill -9 "${existing_pid}" 2>/dev/null || true
      sleep 1
    fi
  fi
fi

start_uvicorn "$selected_port"

sleep 1

if command -v rg >/dev/null 2>&1 && rg -q "error while attempting to bind" /tmp/uvicorn.log; then
  echo "ERROR: Uvicorn konnte nicht binden. Logauszug:"
  tail -n 20 /tmp/uvicorn.log
  exit 1
fi

tail -n 20 /tmp/uvicorn.log

echo "INFO: Health check (HEAD) ..."
curl -I "http://127.0.0.1:${selected_port}/" 2>/dev/null || true

if command -v lsof >/dev/null 2>&1; then
  if lsof -iTCP:"${selected_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "INFO: Uvicorn hört auf Port ${selected_port}."
  else
    echo "WARN: Kein Listener auf Port ${selected_port}. Prüfe /tmp/uvicorn.log."
  fi
else
  echo "WARN: lsof nicht verfügbar. Listener-Check übersprungen."
fi
