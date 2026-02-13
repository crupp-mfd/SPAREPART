#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/workspace_guard.sh"
enforce_onedrive_workspace "$ROOT_DIR"
DEFAULT_AUTOMATE_ROOT="$HOME/Library/CloudStorage/OneDrive-FreigegebeneBibliotheken–MFDRailGmbH/ICT - Dokumente/AUTOMATE"
AUTOMATE_ROOT="${AUTOMATE_ROOT:-$DEFAULT_AUTOMATE_ROOT}"
WORKSPACE_USER="${WORKSPACE_USER:-$(whoami)}"
SOURCE_REPO="${SOURCE_REPO:-}"
WRITE_ROOT_README="${WRITE_ROOT_README:-1}"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --user <name>             Workspace user folder under workspaces/<name>
  --automate-root <path>    OneDrive AUTOMATE root path
  --source <path-or-url>    git clone source (default: origin URL, fallback current repo)
  --no-root-readme          Do not create/update AUTOMATE/README_WORKSPACES.md
  -h, --help                Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      WORKSPACE_USER="$2"
      shift 2
      ;;
    --automate-root)
      AUTOMATE_ROOT="$2"
      shift 2
      ;;
    --source)
      SOURCE_REPO="$2"
      shift 2
      ;;
    --no-root-readme)
      WRITE_ROOT_README="0"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${SOURCE_REPO}" ]]; then
  SOURCE_REPO="$(git -C "$ROOT_DIR" config --get remote.origin.url || true)"
fi
if [[ -z "${SOURCE_REPO}" ]]; then
  SOURCE_REPO="$ROOT_DIR"
fi

TARGET_DIR="$AUTOMATE_ROOT/workspaces/$WORKSPACE_USER/MFDApps"
TARGET_PARENT="$(dirname "$TARGET_DIR")"

mkdir -p "$TARGET_PARENT"

if [[ -d "$TARGET_DIR/.git" ]]; then
  echo "Workspace already exists: $TARGET_DIR"
else
  echo "Cloning to $TARGET_DIR"
  git clone "$SOURCE_REPO" "$TARGET_DIR"
fi

# If cloned from a local path, prefer the same upstream origin URL as source repo.
if [[ -d "$SOURCE_REPO/.git" ]]; then
  SOURCE_ORIGIN_URL="$(git -C "$SOURCE_REPO" config --get remote.origin.url || true)"
  if [[ -n "$SOURCE_ORIGIN_URL" ]]; then
    git -C "$TARGET_DIR" remote set-url origin "$SOURCE_ORIGIN_URL"
  fi
fi

if [[ "$WRITE_ROOT_README" == "1" ]]; then
  README_SOURCE="$ROOT_DIR/docs/README_WORKSPACES.md"
  README_TARGET="$AUTOMATE_ROOT/README_WORKSPACES.md"
  if [[ -f "$README_SOURCE" ]]; then
    cp "$README_SOURCE" "$README_TARGET"
    echo "Updated workspace README: $README_TARGET"
  fi
fi

cat <<INFO
Done.
Workspace: $TARGET_DIR

Next steps:
1) cd "$TARGET_DIR"
2) ./scripts/bootstrap.sh
3) git checkout -b feature/<app>/<topic>
INFO
