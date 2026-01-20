# MFD Automation

Zentrales Repository für interne MFD-Automatisierungsprojekte. Aktuell enthält es zwei Module:

1. **SPAREPART – Objektstrukturtausch / Wagenumbau** (bestehendes UI + API)
2. **RSRD2 Sync** (in Vorbereitung, eigene UI-Sektion innerhalb derselben App)

Beide Module teilen sich Infrastruktur wie IonAPI-Zugänge, das Compass/SQLite-Caching und den FastAPI-Server.

## Struktur

- `credentials/` – lokale Secrets (z.B. `.ionapi`). Wird nicht versioniert.
- `python/m3_api_call.py` – CLI, die MOS256MI (oder andere MI) aufruft und das Ergebnis als JSON ausgibt.
- `python/compass_query.py` – CLI für Infor Data Fabric / Compass JDBC.
- `python/compass_to_sqlite.py` – Lädt Compass-Abfragen direkt in eine SQLite-Tabelle.
- `python/load_erp_wagons.py` – holt ERP-Wagennummern per Compass und speichert sie in `RSRD_ERP_WAGONNO`.
- `python/rsrd2_sync.py` – SOAP/RSRD2-Connector, schreibt Wagenstammdaten in SQLite.
- `python/web_server.py` – FastAPI-Server inkl. REST-Endpunkten für die SQLite-Daten.
- `frontend/` – statische UI (Infor CSS) mit Ladebalken und Pagination.
- `scripts/run-m3-call.js` – Node.js-Brücke, die das Python-Skript ausführt und die Antwort für weitere Verarbeitung bereitstellt.

## Voraussetzungen

1. Python 3.10+ (Skripte nutzen automatisch ein virtuelles Environment)
2. Node.js 18+ (nur für bestehende Skripte; das neue UI/Backend läuft komplett unter Python/FastAPI)
3. Eine gültige `.ionapi` Datei in `credentials/ionapi/` (z.B. `MFD_Backend_Python.ionapi`)
4. Für Compass-Abfragen zusätzlich eine `.ionapi` für den JDBC-Zugang (z.B. `Infor Compass JDBC Driver.ionapi`) sowie das JDBC JAR in `credentials/jdbc/`.

## Setup & Start

```bash
# einmalig
./scripts/bootstrap.sh

# danach
./scripts/dev-server.sh
```

- `bootstrap.sh` erstellt bei Bedarf `.env` aus `.env.template`, richtet `.venv` inklusive Dependencies ein und lädt die ERP-Wagennummern in `RSRD_ERP_WAGONNO`.
- `dev-server.sh` liest automatisch `.env`, aktiviert das virtuelle Environment und startet `uvicorn`.

Alle Python-Skripte und der FastAPI-Server laden die `.env` selbstständig (via `python-dotenv`). Manuelle `export`-Befehle sind nur noch nötig, wenn Umgebungsvariablen bewusst überschrieben werden sollen.

## Nutzung

### Direkt per Python

```bash
python3 python/m3_api_call.py \\
  --program MOS256MI \\
  --transaction LstAsBuild \\
  --params-json '{\"MTRL\":\"EXAMPLE\",\"SERN\":\"00 00 0000 000-0\"}' \\
  --use-example
```

Ohne weitere Argumente sucht das Skript automatisch nach einer `.ionapi` in `credentials/ionapi/`.

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

- `.ionapi` (default `Infor Compass JDBC Driver.ionapi`) und JDBC-JAR werden automatisch aus `credentials/ionapi/` bzw. `credentials/jdbc/` geladen. Bei Bedarf per `--ionapi` bzw. `--jdbc-jar` überschreibbar.
- `--scheme` bestimmt das Ziel (`datalake` ist Standard). Nur für `sourcedata` ist `--catalog` erforderlich (z.B. `M3BE`).
- SQL kann auch aus einer Datei stammen (`--sql-file`). Mit `--output table` wird das Ergebnis als TSV ausgegeben, Standard ist JSON.

### Compass → SQLite Cache

```bash
python3 python/compass_to_sqlite.py \
  --scheme datalake \
  --sql-file sql/wagons_base_prd.sql \
  --table wagons \
  --sqlite-db data/cache.db \
  --mode replace
```

### ERP Wagennummern → SQLite (`RSRD_ERP_WAGONNO`)

```bash
python3 python/load_erp_wagons.py \
  --scheme datalake \
  --sql \"SELECT SERN FROM MILOIN WHERE EQTP = '100' AND STAT = '20'\" \
  --table RSRD_ERP_WAGONNO \
  --sqlite-db data/cache.db
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
# optional: export RSRD_DB_PATH="data/cache.db"

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
| ERP-Wagennummern + Stammdaten laden | `python3 python/load_erp_wagons.py ...` + `python3 python/compass_to_sqlite.py --sql-file sql/rsrd_erp_full.sql --table RSRD_ERP_DATA ...` | Button **Aus ERP laden** (`/api/rsrd2/load_erp`) |
| JSON aus RSRD laden | `python3 python/rsrd2_sync.py --mode stage --wagons ...` | Button **JSON aus RSRD laden** (`/api/rsrd2/fetch_json`) |
| JSON verarbeiten | `python3 python/rsrd2_sync.py --mode process` | Button **JSON verarbeiten** (`/api/rsrd2/process_json`) |

Alle relevanten Tabellen liegen im Standard-SQLite `data/cache.db`:

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
- Im Ordner `sql/` liegen Beispiele (`wagons_base_prd.sql`, `wagons_base_tst.sql`) mit dem oben gezeigten Select. Alternativ kann das Statement direkt via `--sql` angegeben werden.
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
