# MFDApps OneDrive Workspaces

## Ziel

Mehrere Entwickler arbeiten parallel in getrennten Git-Clones, nicht im gleichen Working Directory.

## Standardpfad

`/Users/<user>/Library/CloudStorage/OneDrive-FreigegebeneBibliotheken–MFDRailGmbH/ICT - Dokumente/AUTOMATE/workspaces/<user>/MFDApps`

## Regeln

1. Kein geteilter Clone fuer mehrere Personen.
2. Jede Aenderung geht per Push nach GitHub (kein dauerhaft nur lokaler OneDrive-Stand).
3. Branch-Schema: `feature/<app>/<topic>`.
4. Eine App pro Pull Request.
5. Deploy nur aus GitHub Actions.
6. Start-/Deploy-Skripte sind auf OneDrive-Workspaces erzwungen (`MFDAPPS_ENFORCE_ONEDRIVE=1`).

## App-Ordner

- `apps/AppMFD`
- `apps/AppBremsenumbau`
- `apps/AppGoldenView`
- `apps/AppMehrkilometer`
- `apps/AppObjektstruktur`
- `apps/AppRSRD`
- `apps/AppSQL-API`
- `apps/AppTeilenummer`
- `apps/AppWagensuche`

## Credentials

- Credentials liegen lokal unter `apps/AppMFD/credentials` (nicht in Git).
- Alternative ueber `MFDAPPS_CREDENTIALS_DIR`.

## Setup (pro User)

```bash
cd /path/to/current/MFDApps-clone
./scripts/setup-onedrive-workspace.sh --user <user>
```
