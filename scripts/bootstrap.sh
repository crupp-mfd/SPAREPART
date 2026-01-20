#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
TEMPLATE_FILE="$ROOT_DIR/.env.template"
VENV_DIR="$ROOT_DIR/.venv"
REQ_FILE="$ROOT_DIR/python/requirements.txt"
REQ_HASH_FILE="$VENV_DIR/.requirements.hash"
DB_PATH="$ROOT_DIR/data/cache.db"

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

calc_req_hash() {
  "$PYTHON_BIN" - "$1" <<'PY'
from pathlib import Path
import hashlib
import sys

path = Path(sys.argv[1])
data = path.read_bytes() if path.exists() else b""
print(hashlib.sha256(data).hexdigest())
PY
}

NEED_INSTALL="0"
if [[ ! -d "$VENV_DIR" ]]; then
  echo "[bootstrap] Erstelle virtuelles Environment ..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
  NEED_INSTALL="1"
elif [[ -f "$REQ_FILE" ]]; then
  REQ_HASH="$(calc_req_hash "$REQ_FILE")"
  if [[ ! -f "$REQ_HASH_FILE" ]] || [[ "$(cat "$REQ_HASH_FILE")" != "$REQ_HASH" ]]; then
    NEED_INSTALL="1"
  fi
fi

source "$VENV_DIR/bin/activate"
if [[ "$NEED_INSTALL" == "1" ]]; then
  pip install --upgrade pip
  if [[ -f "$REQ_FILE" ]]; then
    pip install -r "$REQ_FILE"
    REQ_HASH="$(calc_req_hash "$REQ_FILE")"
    echo "$REQ_HASH" > "$REQ_HASH_FILE"
  fi
fi

mkdir -p "$ROOT_DIR/data"
touch "$DB_PATH"

LOAD_ERP="${BOOTSTRAP_LOAD_ERP:-auto}"
NEED_LOAD_ERP="1"
if [[ "$LOAD_ERP" == "never" ]]; then
  NEED_LOAD_ERP="0"
elif [[ "$LOAD_ERP" == "auto" ]]; then
  NEED_LOAD_ERP="1"
  if [[ -f "$DB_PATH" ]]; then
    SPAREPART_ENV="${SPAREPART_ENV:-}"
    CHECK_STATUS=1
    PYTHONPATH="$ROOT_DIR" "$VENV_DIR/bin/python" - "$DB_PATH" <<'PY'
import os
import sqlite3
from pathlib import Path
import sys

try:
    from python.env_loader import load_project_dotenv
except Exception:
    load_project_dotenv = None

if load_project_dotenv:
    load_project_dotenv()

aliases = {"live": "prd", "prod": "prd", "prd": "prd", "test": "tst", "tst": "tst"}
env = os.getenv("SPAREPART_ENV", "prd").lower()
normalized = aliases.get(env)
suffix = "PRD" if normalized == "prd" else "TST"
table = f"RSRD_ERP_WAGONNO_{suffix}"
db_path = Path(sys.argv[1])
if not db_path.exists():
    raise SystemExit(1)

conn = sqlite3.connect(db_path)
try:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not row:
        raise SystemExit(1)
    count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] or 0
    raise SystemExit(0 if count > 0 else 1)
finally:
    conn.close()
PY
    CHECK_STATUS=$?
    if [[ $CHECK_STATUS -eq 0 ]]; then
      NEED_LOAD_ERP="0"
    fi
  fi
elif [[ "$LOAD_ERP" == "always" ]]; then
  NEED_LOAD_ERP="1"
fi

if [[ "$NEED_LOAD_ERP" == "1" ]]; then
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
fi

echo "[bootstrap] Fertig."
