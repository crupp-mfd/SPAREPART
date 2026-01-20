#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
ENV_FILE="$ROOT_DIR/.env"

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
exec uvicorn python.web_server:app --reload "$@"
