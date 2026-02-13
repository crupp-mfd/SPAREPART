# MFDApps Workflow

## Workspace Rule

- OneDrive Root: `.../ICT - Dokumente/AUTOMATE`
- Pro Entwickler eigener Clone unter `.../workspaces/<user>/MFDApps`
- Kein gemeinsames `.git`-Working-Directory fuer mehrere Nutzer

## Branch Rule

- Schema: `feature/<app>/<topic>`
- Beispiel: `feature/apprsrd/compare-fix`

## Pull Request Rule

- Eine App pro PR
- Squash-Merge auf `main`
- Review ueber `CODEOWNERS`

## Deployment Rule

- Deploy nur aus GitHub Actions
- Path-Filter triggert nur betroffene App
- Jede App hat eigenes Image und eigenes Rollback

## Tags

- `appmfd-vX.Y.Z`
- `appbremsenumbau-vX.Y.Z`
- `appgoldenview-vX.Y.Z`
- `appmehrkilometer-vX.Y.Z`
- `appobjektstruktur-vX.Y.Z`
- `apprsrd-vX.Y.Z`
- `appsqlapi-vX.Y.Z`
- `appteilenummer-vX.Y.Z`
- `appwagensuche-vX.Y.Z`
