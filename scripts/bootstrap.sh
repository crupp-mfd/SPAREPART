#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
TEMPLATE_FILE="$ROOT_DIR/.env.template"
VENV_DIR="$ROOT_DIR/.venv"

echo "[bootstrap] Repository: $ROOT_DIR"

if [[ ! -f "$ENV_FILE" && -f "$TEMPLATE_FILE" ]]; then
  cp "$TEMPLATE_FILE" "$ENV_FILE"
  echo "[bootstrap] .env aus Template erzeugt. Werte bei Bedarf anpassen."
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python-Interpreter '$PYTHON_BIN' nicht gefunden." >&2
  exit 1
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[bootstrap] Erstelle virtuelles Environment ..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r "$ROOT_DIR/python/requirements.txt"

mkdir -p "$ROOT_DIR/data"
touch "$ROOT_DIR/data/cache.db"

set +e
"$VENV_DIR/bin/python" "$ROOT_DIR/python/load_erp_wagons.py"
LOAD_STATUS=$?
set -e

if [[ $LOAD_STATUS -ne 0 ]]; then
  echo "[bootstrap] Hinweis: load_erp_wagons.py lieferte Status $LOAD_STATUS."
  echo "            Prüfe Compass-Zugänge (.ionapi) und JDBC-JAR."
else
  echo "[bootstrap] ERP-Wagennummern erfolgreich geladen."
fi

echo "[bootstrap] Fertig."
