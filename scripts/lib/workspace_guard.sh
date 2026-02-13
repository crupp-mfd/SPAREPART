#!/usr/bin/env bash
set -euo pipefail

is_ci_runtime() {
  [[ "${CI:-}" == "true" ]] || [[ "${GITHUB_ACTIONS:-}" == "true" ]]
}

is_onedrive_workspace_path() {
  local path="$1"
  case "$path" in
    *"/Library/CloudStorage/OneDrive-FreigegebeneBibliotheken–MFDRailGmbH/ICT - Dokumente/AUTOMATE/workspaces/"*"/MFDApps"* )
      return 0
      ;;
    * )
      return 1
      ;;
  esac
}

enforce_onedrive_workspace() {
  local root_dir="$1"

  if is_ci_runtime; then
    return 0
  fi

  local enforce="${MFDAPPS_ENFORCE_ONEDRIVE:-1}"
  local enforce_norm
  enforce_norm="$(printf '%s' "$enforce" | tr '[:upper:]' '[:lower:]')"
  case "$enforce_norm" in
    0|false|no|off)
      return 0
      ;;
  esac

  local resolved
  resolved="$(cd "$root_dir" && pwd -P)"

  if is_onedrive_workspace_path "$resolved"; then
    return 0
  fi

  echo "ERROR: Dieses Workspace ist nicht im erlaubten OneDrive-Pfad." >&2
  echo "       Aktuell: $resolved" >&2
  echo "       Erlaubt: /Users/<user>/Library/CloudStorage/OneDrive-FreigegebeneBibliotheken–MFDRailGmbH/ICT - Dokumente/AUTOMATE/workspaces/<user>/MFDApps" >&2
  echo "" >&2
  echo "Bitte in den OneDrive-Clone wechseln und dort ausfuehren." >&2
  echo "Optional (nur Ausnahmefall): MFDAPPS_ENFORCE_ONEDRIVE=0 setzen." >&2
  return 1
}
