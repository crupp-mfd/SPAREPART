# MFDApps

Monorepo fuer die getrennten MFD-Anwendungen mit OneDrive-Workspace-Modell und app-spezifischem Deploy.

## OneDrive Workspace Standard

- Basis: `.../ICT - Dokumente/AUTOMATE`
- Pro Entwickler eigener Clone: `.../workspaces/<user>/MFDApps`
- Kein gemeinsam genutztes `.git`-Arbeitsverzeichnis fuer mehrere Personen

Workspace-Anlage (pro User):

```bash
./scripts/setup-onedrive-workspace.sh --user <user>
```

## App-Struktur

Alle Programme liegen unter `apps/`:

1. `AppMFD` (Einstieg/Portal + Credentials-Standardposition)
2. `AppBremsenumbau`
3. `AppGoldenView`
4. `AppMehrkilometer`
5. `AppObjektstruktur`
6. `AppRSRD`
7. `AppSQL-API`
8. `AppTeilenummer`
9. `AppWagensuche`

Gemeinsame Bibliothek: `packages/sparepart-shared`.

### Root-Regel (Trennung)

- Unter `frontend/` im Repo-Root liegt nur `index.html` als Einstieg.
- App-spezifische Frontend-Dateien liegen ausschließlich in `apps/<AppName>/frontend/`.
- App-spezifische SQL-Dateien liegen ausschließlich in `apps/<AppName>/sql/`.
- Gemeinsame Infra im Root bleibt erlaubt (`python/` Connectoren, `scripts/` Dev/Deploy, Credentials-Pfade via `MFDAPPS_CREDENTIALS_DIR`).

## Runtime und Secrets

- `MFDAPPS_HOME`: optionales Basisverzeichnis (Default: Repo-Root)
- `MFDAPPS_ENFORCE_ONEDRIVE`: sperrt lokale Starts außerhalb OneDrive-Workspace (Default `1`)
- `MFDAPPS_RUNTIME_ROOT`: Runtime-Dateien (`cache.db`, Logs, Exporte), Default `data/`
- `MFDAPPS_CREDENTIALS_DIR`: Secrets-Verzeichnis, Default `apps/AppMFD/credentials` mit Legacy-Fallback `credentials/`

Secrets gehoeren nicht ins Repo.

## Lokaler Start

```bash
# AppMFD (Default)
./scripts/dev-server.sh

# Eine konkrete App
SERVICE=AppRSRD ./scripts/dev-server.sh --port 8001
```

Pro App existieren ausserdem eigene Dev-Skripte:

```bash
./apps/AppMFD/dev-server.sh --port 8000
./apps/AppRSRD/dev-server.sh --port 8001
./apps/AppGoldenView/dev-server.sh --port 8002
```

## Deploy (Azure Container Apps)

Jede App hat ein eigenes Deploy-Skript:

```bash
./apps/AppMFD/deploy.sh
./apps/AppBremsenumbau/deploy.sh
./apps/AppGoldenView/deploy.sh
./apps/AppMehrkilometer/deploy.sh
./apps/AppObjektstruktur/deploy.sh
./apps/AppRSRD/deploy.sh
./apps/AppSQL-API/deploy.sh
./apps/AppTeilenummer/deploy.sh
./apps/AppWagensuche/deploy.sh
```

GitHub Actions Workflow: `.github/workflows/deploy-split.yml` (path-filter je App).
Azure deployt nur aus GitHub Actions.

## Team-Git-Regeln

- Branch-Schema: `feature/<app>/<topic>`
- Eine App pro PR
- Squash-Merge auf `main`
- CODEOWNERS trennt Review-Verantwortung pro `apps/App*/`

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/shared -q
```

## Nutzung

### Direkt per Python

```bash
python3 python/m3_api_call.py \\
  --program MOS256MI \\
  --transaction LstAsBuild \\
  --params-json '{\"MTRL\":\"EXAMPLE\",\"SERN\":\"00 00 0000 000-0\"}' \\
  --use-example
```

Ohne weitere Argumente sucht das Skript automatisch nach einer `.ionapi` in `${MFDAPPS_CREDENTIALS_DIR}/ionapi` (Default: `apps/AppMFD/credentials/ionapi`).

### Über Node.js (z.B. für UI-Integration)

```bash
npm install
npm run m3:example
```

Eigene Parameter:

```bash
node scripts/run-m3-call.js \\
  --program MOS256MI \\
  --transaction LstAsBuild \\
  --params-json '{\"MTRL\":\"EXAMPLE\",\"SERN\":\"00 00 0000 000-0\"}'
```

Die Node-Brücke startet intern `python/m3_api_call.py`, liest dessen JSON-Ausgabe und gibt die rohe MI-Antwort auf stdout aus. So kann das Script später leicht in einen Express-Server oder ein anderes Backend eingebunden werden.

### Infor Data Fabric (Compass JDBC)

```bash
# Beispiel: SDA (M3) Abfrage, OCUSMA
python3 python/compass_query.py \
  --scheme datalake \
  --sql "select * from OCUSMA" \
  --limit 10
```

Hinweise:

- `.ionapi` (default `Infor Compass JDBC Driver.ionapi`) und JDBC-JAR werden automatisch aus `${MFDAPPS_CREDENTIALS_DIR}/ionapi` bzw. `${MFDAPPS_CREDENTIALS_DIR}/jdbc` geladen. Bei Bedarf per `--ionapi` bzw. `--jdbc-jar` überschreibbar.
- `--scheme` bestimmt das Ziel (`datalake` ist Standard). Nur für `sourcedata` ist `--catalog` erforderlich (z.B. `M3BE`).
- SQL kann auch aus einer Datei stammen (`--sql-file`). Mit `--output table` wird das Ergebnis als TSV ausgegeben, Standard ist JSON.

### Compass → SQLite Cache

```bash
python3 python/compass_to_sqlite.py \
  --scheme datalake \
  --sql-file apps/AppObjektstruktur/sql/wagons_base_prd.sql \
  --table wagons \
  --sqlite-db "$MFDAPPS_RUNTIME_ROOT/cache.db" \
  --mode replace
```

### ERP Wagennummern → SQLite (`RSRD_ERP_WAGONNO`)

```bash
python3 python/load_erp_wagons.py \
  --scheme datalake \
  --sql \"SELECT SERN FROM MILOIN WHERE EQTP = '100' AND STAT = '20'\" \
  --table RSRD_ERP_WAGONNO \
  --sqlite-db "$MFDAPPS_RUNTIME_ROOT/cache.db"
```

- Das Skript transformiert `SERN` automatisch in eine numerische Variante (alle Nicht-Ziffern entfernt) und speichert beide Formen.
- Standardmäßig wird die Tabelle geleert und neu befüllt; mit `--append` können mehrere Läufe angehängt werden.
- Nach dem Lauf stehen die Daten über SQLite (z.B. in DBeaver oder `/api/rsrd2/wagons`) für Vergleiche bereit.

### RSRD2 Sync

Um rolling-stock-Stammdaten aus dem RSRD2-Portal nach SQLite zu holen:

```bash
export RSRD_WSDL_URL="https://<host>/rsrd2?wsdl"
export RSRD_SOAP_USER="..."
export RSRD_SOAP_PASS="..."
# optional: export RSRD_DB_PATH="$MFDAPPS_RUNTIME_ROOT/cache.db"

python3 python/rsrd2_sync.py --wagons 338012345678901 338009876543210 --snapshots
```

Das Skript besitzt drei Arbeitsmodi:

1. `--mode stage` (oder UI-Button **JSON aus RSRD laden**)  
   - Liest Wagennummern aus `RSRD_ERP_WAGONNO` und ruft den SOAP-Service.  
   - Speichert die Rohantwort je Wagen als JSON in `RSRD_WAGON_JSON` und aktualisiert `rsrd_wagons` (zur Anzeige im UI).
2. `--mode process` (oder UI-Button **JSON verarbeiten**)  
   - Liest die JSON-Staging-Tabelle, löst alle Elemente gemäß `RSRD.wsdl` auf und schreibt sie in `RSRD_WAGON_DATA`.  
   - Jede SOAP-Eigenschaft wird zu einer separaten Spalte (z.B. `ADMINISTRATIVEDATASET_OWNERNAME`, `DESIGNDATASET_LOADTABLE_0_ROUTECLASSPAYLOADS_3_MAXPAYLOAD`), sodass Vergleiche direkt in SQLite/SQL möglich sind.
3. `--mode full` (oder alter Endpoint `/api/rsrd2/sync_all`)  
   - Kombiniert beide Schritte unmittelbar hintereinander.

Der typische Workflow sieht so aus:

| Schritt | CLI | UI |
| --- | --- | --- |
| ERP-Wagennummern + Stammdaten laden | `python3 python/load_erp_wagons.py ...` + `python3 python/compass_to_sqlite.py --sql-file apps/AppRSRD/sql/rsrd_erp_full.sql --table RSRD_ERP_DATA ...` | Button **Aus ERP laden** (`/api/rsrd2/load_erp`) |
| JSON aus RSRD laden | `python3 python/rsrd2_sync.py --mode stage --wagons ...` | Button **JSON aus RSRD laden** (`/api/rsrd2/fetch_json`) |
| JSON verarbeiten | `python3 python/rsrd2_sync.py --mode process` | Button **JSON verarbeiten** (`/api/rsrd2/process_json`) |

Alle relevanten Tabellen liegen standardmäßig in `${MFDAPPS_RUNTIME_ROOT}/cache.db`:

- `RSRD_ERP_WAGONNO` – reine Wagennummern (aus Compass/ERP PRD)  
- `RSRD_ERP_DATA` – alle ERP-Felder gemäß Stored Procedure (inkl. neuer Spalten `WG-DATLETZG4.0`, `WG-DATLETZG4.2`, `WG-REVPERIODE`, `UPLOAD`, `TIME_UPLOAD`)  
- `RSRD_WAGON_JSON` – ROH-JSON aus RSRD  
- `RSRD_WAGON_DATA` – vollständig aufgelöste JSON-Felder für Vergleiche

Der FastAPI-Server stellt darüber hinaus `/api/rsrd2/wagons` (Anzeige des Caches) und `/api/rsrd2/sync` / `/api/rsrd2/sync_all` (Legacy-Gesamtablauf) zur Verfügung.

Falls der RSRD-Anbieter andere Header-Informationen erwartet (z.B. Sendercode, MessageType), können diese in `.env` via `RSRD_MESSAGE_TYPE`, `RSRD_SENDER_CODE`, `RSRD_RECIPIENT_CODE` usw. überschrieben werden (siehe `.env.template`).

- Erwartet Aliasse in der SQL-Select-Klausel; sie werden als Spaltennamen in SQLite verwendet.
- `--mode` steuert, wie die Tabelle befüllt wird:
  - `replace` (Default): Tabelle droppen und neu anlegen.
  - `truncate`: Tabelle behalten, Inhalte löschen, dann einfügen.
  - `append`: Nur anhängen.
- SQL-Dateien liegen app-spezifisch, z. B. `apps/AppObjektstruktur/sql/` (`wagons_base_prd.sql`, `wagons_base_tst.sql`) oder `apps/AppRSRD/sql/rsrd_erp_full.sql`. Alternativ kann das Statement direkt via `--sql` angegeben werden.
- Wenn du statt Data Lake auf Source Data Access zugreifen möchtest, setze `--scheme sourcedata --catalog <Katalog>`.

### FastAPI UI & Progress Loader

Nachdem die SQLite-Daten mit `python/compass_to_sqlite.py` erstellt wurden:

1. Abhängigkeiten installieren: `pip install -r python/requirements.txt` (ein Mal)
2. Server starten:

```bash
source .venv/bin/activate
uvicorn python.web_server:app --reload
```

3. Browser öffnen: [http://localhost:8000](http://localhost:8000)

Features der UI:

- Nutzt das öffentliche Infor CSS (`https://design.infor.com/components/css/ids-enterprise.css`) für vertrautes Look & Feel.
- Zeigt zunächst den Ladebildschirm mit den Phasen „VERBINDE ...“ und „LADE DATEN VON M3“. Der Fortschrittsbalken basiert auf der vollständigen Zeilenanzahl (`COUNT(*)`) aus SQLite und lädt anschließend chunks à 250 Datensätze, bis alle Wagen übertragen sind.
- Erst nach abgeschlossenem Ladevorgang erscheinen die Inhalte, paginiert mit 10 Datensätzen pro Seite. Über die Buttons „Zurück“ / „Weiter“ lässt sich durch die 3500+ Einträge blättern.
- Ein Button „Neu laden“ triggert serverseitig `python/compass_to_sqlite.py`, löscht die vorhandene SQLite-Datei und importiert alle Wagen erneut (praktisch, wenn sich Basisdaten geändert haben).
