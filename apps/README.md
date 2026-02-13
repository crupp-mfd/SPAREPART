# MFDApps Modules

Alle Programme liegen getrennt unter `apps/`:

- `AppMFD` (Portal/Einsprung + gemeinsame Credentials-Position)
- `AppBremsenumbau`
- `AppGoldenView`
- `AppMehrkilometer`
- `AppObjektstruktur`
- `AppRSRD`
- `AppSQL-API`
- `AppTeilenummer`
- `AppWagensuche`

Jede App hat eigene Dateien fuer:

- `src/` (Backend-Einstieg)
- `frontend/` (statische Dateien)
- `sql/` (app-spezifische SQL-Abfragen)
- `runtime/` (lokale Laufzeitdaten, nicht in Git)
- `tests/`
- `Dockerfile`, `deploy.sh`, `dev-server.sh`

Gemeinsame Helper liegen in `packages/sparepart-shared`.
