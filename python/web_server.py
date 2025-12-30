"""FastAPI server serving the loader UI and paginated wagon data."""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import json
import re
from pathlib import Path
from typing import List, Dict, Any
import threading
import uuid

from datetime import datetime

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .env_loader import load_project_dotenv
from .rsrd2_sync import (
    WAGONS_TABLE as RSRD_WAGONS_TABLE,
    SNAPSHOTS_TABLE as RSRD_SNAPSHOTS_TABLE,
    init_db as rsrd_init_db,
    sync_wagons as rsrd_sync_wagons,
)

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "cache.db"
FRONTEND_DIR = PROJECT_ROOT / "frontend"
IONAPI_DIR = PROJECT_ROOT / "credentials" / "ionapi"
DEFAULT_TABLE = "wagons"
SPAREPARTS_TABLE = "spareparts"
SPAREPARTS_SWAP_TABLE = "sparepart_swaps"
RSRD_ERP_TABLE = "RSRD_ERP_WAGONNO"
RSRD_ERP_FULL_TABLE = "RSRD_ERP_DATA"
DEFAULT_SCHEME = os.getenv("SPAREPART_SCHEME", "datalake")
SQL_FILE = PROJECT_ROOT / "sql" / "wagons_base.sql"
SPAREPARTS_SQL_FILE = PROJECT_ROOT / "sql" / "spareparts_base.sql"
RSRD_ERP_SQL_FILE = PROJECT_ROOT / "sql" / "rsrd_erp_full.sql"
DEFAULT_ENV = os.getenv("SPAREPART_ENV", "live").lower()
ENV_SUFFIXES = {"live": "", "test": "_test"}
ENV_IONAPI = {
    "live": {
        "compass": IONAPI_DIR / "Infor Compass JDBC Driver.ionapi",
        "mi": IONAPI_DIR / "MFD_Backend_Python.ionapi",
    },
    "test": {
        "compass": IONAPI_DIR / "Infor Compass JDBC Driver_TST.ionapi",
        "mi": IONAPI_DIR / "TST_MFD_Backend_Python.ionapi",
    },
}

JOB_LOG_LIMIT = 2000
PROGRESS_LINE = re.compile(r"^\d+/\d+\s+Datensätze gespeichert \.\.\.$")
_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}

app = FastAPI(title="SPAREPART Loader API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail=f"SQLite DB nicht gefunden: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_swap_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            WAGEN_ITNO TEXT NOT NULL,
            WAGEN_SERN TEXT NOT NULL,
            ORIGINAL_ITNO TEXT NOT NULL,
            ORIGINAL_SERN TEXT NOT NULL,
            ERSATZ_ITNO TEXT NOT NULL,
            ERSATZ_SERN TEXT NOT NULL,
            USER TEXT,
            UPLOAD TEXT DEFAULT 'N',
            TIMESTAMP TEXT,
            PRIMARY KEY (WAGEN_ITNO, WAGEN_SERN, ORIGINAL_ITNO, ORIGINAL_SERN)
        )
        """
    )


def _validate_table(table: str) -> str:
    if not table.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Ungültiger Tabellenname.")
    return table


def _normalize_env(env: str | None) -> str:
    value = (env or DEFAULT_ENV).lower()
    if value not in ENV_SUFFIXES:
        raise HTTPException(status_code=400, detail="Ungültige Umgebung.")
    return value


def _table_for(base: str, env: str | None) -> str:
    normalized = _normalize_env(env)
    return f"{base}{ENV_SUFFIXES[normalized]}"


def _ionapi_path(env: str, kind: str) -> Path:
    normalized = _normalize_env(env)
    env_config = ENV_IONAPI.get(normalized)
    if not env_config or kind not in env_config:
        raise HTTPException(status_code=400, detail=f"Ionapi-Konfiguration fehlt für {normalized}/{kind}")
    path = env_config[kind]
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"Ionapi-Datei nicht gefunden: {path}")
    return path


def _ensure_rsrd_tables(conn: sqlite3.Connection) -> None:
    rsrd_init_db(conn)


def _fetch_erp_wagon_numbers(conn: sqlite3.Connection, limit: int | None = None) -> List[str]:
    try:
        query = f"SELECT wagon_sern, wagon_sern_numeric FROM {RSRD_ERP_TABLE} ORDER BY wagon_sern"
        params: List[int] = []
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(query, params).fetchall()
    except sqlite3.OperationalError as exc:  # table missing
        raise HTTPException(status_code=404, detail=f"Tabelle {RSRD_ERP_TABLE} nicht gefunden.") from exc
    wagon_numbers = []
    for row in rows:
        sern_numeric = (row["wagon_sern_numeric"] or "").strip()
        sern = (row["wagon_sern"] or "").strip()
        number = sern_numeric or sern
        if number:
            wagon_numbers.append(number)
    return wagon_numbers


def _ensure_table(
    conn: sqlite3.Connection,
    table: str,
    template: str | None = None,
) -> str:
    table = _validate_table(table)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    )
    if cursor.fetchone() is None:
        if template:
            template = _validate_table(template)
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" AS SELECT * FROM "{template}" WHERE 0=1'
            )
        else:
            raise HTTPException(status_code=404, detail=f"Tabelle '{table}' nicht gefunden.")
    return table


@app.get("/api/wagons/count")
def wagons_count(
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    env_table = _table_for(table, env)
    with _connect() as conn:
        table_name = _ensure_table(conn, env_table, table)
        total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return {"table": table_name, "total": total, "env": _normalize_env(env)}


@app.get("/api/wagons/chunk")
def wagons_chunk(
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    env_table = _table_for(table, env)
    with _connect() as conn:
        table_name = _ensure_table(conn, env_table, table)
        cursor = conn.execute(
            f"SELECT * FROM {table_name} LIMIT ? OFFSET ?",
            (limit, offset),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return {
        "table": table_name,
        "rows": rows,
        "offset": offset,
        "limit": limit,
        "returned": len(rows),
        "total": total,
        "env": _normalize_env(env),
    }


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


def _run_compass_to_sqlite(sql_file: Path, table: str, env: str) -> subprocess.CompletedProcess[str]:
    ionapi = _ionapi_path(env, "compass")
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "python" / "compass_to_sqlite.py"),
        "--scheme",
        DEFAULT_SCHEME,
        "--sql-file",
        str(sql_file),
        "--table",
        table,
        "--sqlite-db",
        str(DB_PATH),
        "--mode",
        "replace",
        "--ionapi",
        str(ionapi),
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def _build_load_erp_cmd(env: str) -> List[str]:
    ionapi = _ionapi_path(env, "compass")
    return [
        sys.executable,
        str(PROJECT_ROOT / "python" / "load_erp_wagons.py"),
        "--scheme",
        DEFAULT_SCHEME,
        "--sqlite-db",
        str(DB_PATH),
        "--ionapi",
        str(ionapi),
    ]


def _build_erp_full_cmd(env: str) -> List[str]:
    if not RSRD_ERP_SQL_FILE.exists():
        raise FileNotFoundError(f"SQL-Datei nicht gefunden: {RSRD_ERP_SQL_FILE}")
    ionapi = _ionapi_path(env, "compass")
    return [
        sys.executable,
        str(PROJECT_ROOT / "python" / "compass_to_sqlite.py"),
        "--scheme",
        DEFAULT_SCHEME,
        "--sql-file",
        str(RSRD_ERP_SQL_FILE),
        "--table",
        RSRD_ERP_FULL_TABLE,
        "--sqlite-db",
        str(DB_PATH),
        "--mode",
        "replace",
        "--ionapi",
        str(ionapi),
    ]


def _create_job(job_type: str, env: str) -> Dict[str, Any]:
    job_id = uuid.uuid4().hex
    job = {
        "id": job_id,
        "type": job_type,
        "env": _normalize_env(env),
        "status": "running",
        "logs": [],
        "result": None,
        "error": None,
        "started": datetime.utcnow().isoformat(),
        "finished": None,
    }
    with _jobs_lock:
        _jobs[job_id] = job
    return job


def _append_job_log(job_id: str, message: str) -> None:
    if not message:
        return
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        logs = job.setdefault("logs", [])
        logs.append(message)
        if len(logs) > JOB_LOG_LIMIT:
            del logs[: len(logs) - JOB_LOG_LIMIT]


def _finish_job(job_id: str, status: str, result: Dict[str, Any] | None = None, error: str | None = None) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job["status"] = status
        job["result"] = result
        job["error"] = error
        job["finished"] = datetime.utcnow().isoformat()


def _job_snapshot(job_id: str) -> Dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job nicht gefunden.")
        snapshot = dict(job)
        snapshot["logs"] = list(job.get("logs", []))
        return snapshot


def _start_subprocess_job(
    job_type: str,
    cmd: List[str],
    env: str,
    finalize_fn,
) -> Dict[str, Any]:
    job = _create_job(job_type, env)

    def runner() -> None:
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Start fehlgeschlagen: {exc}")
            _finish_job(job["id"], "error", error=str(exc))
            return
        assert process.stdout is not None
        try:
            for line in process.stdout:
                text = line.strip()
                if not text or PROGRESS_LINE.match(text):
                    continue
                _append_job_log(job["id"], text)
            returncode = process.wait()
            if returncode != 0:
                message = f"Prozess endete mit Code {returncode}"
                _append_job_log(job["id"], message)
                _finish_job(job["id"], "error", error=message)
                return
            result = finalize_fn(job["id"])
            _finish_job(job["id"], "success", result=result)
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))
        finally:
            try:
                process.stdout.close()
            except Exception:
                pass

    threading.Thread(target=runner, daemon=True).start()
    return job


def _finalize_load_erp(job_id: str, env: str) -> Dict[str, Any]:
    with _connect() as conn:
        numbers_table = _ensure_table(conn, RSRD_ERP_TABLE, RSRD_ERP_TABLE)
        count_wagons = conn.execute(f"SELECT COUNT(*) FROM {numbers_table}").fetchone()[0]
    message = f"ERP-Wagennummern geladen: {count_wagons}."
    _append_job_log(job_id, message)
    return {"count_wagons": count_wagons}


def _finalize_load_erp_full(job_id: str, env: str) -> Dict[str, Any]:
    with _connect() as conn:
        full_table = _ensure_table(conn, RSRD_ERP_FULL_TABLE, None)
        count_full = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
    message = f"ERP-Wagenattribute geladen: {count_full}."
    _append_job_log(job_id, message)
    return {"count_full": count_full}


def _reload_spareparts_table(env: str) -> None:
    if not SPAREPARTS_SQL_FILE.exists():
        return
    table_name = _table_for(SPAREPARTS_TABLE, env)
    result = _run_compass_to_sqlite(SPAREPARTS_SQL_FILE, table_name, env)
    if result.returncode != 0:
        print(
            f"Ersatzteil-Reload fehlgeschlagen: {result.stderr or result.stdout}",
            file=sys.stderr,
        )


@app.post("/api/reload")
def reload_database(
    background_tasks: BackgroundTasks,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    if not SQL_FILE.exists():
        raise HTTPException(status_code=500, detail=f"SQL-Datei nicht gefunden: {SQL_FILE}")

    table_name = _table_for(DEFAULT_TABLE, env)
    result = _run_compass_to_sqlite(SQL_FILE, table_name, env)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Reload fehlgeschlagen: {result.stderr or result.stdout}",
        )

    background_tasks.add_task(_reload_spareparts_table, env)
    return {"message": "Reload erfolgreich", "stdout": result.stdout, "env": _normalize_env(env)}


@app.get("/api/objstrk")
def objstrk(
    mtrl: str = Query(..., min_length=1),
    sern: str = Query(..., min_length=1),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    """Lädt Objektstruktur über MOS256MI (Debug: rohe Antwort zurückgeben)."""
    ionapi = _ionapi_path(env, "mi")
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "python" / "m3_api_call.py"),
        "--program",
        "MOS256MI",
        "--transaction",
        "LstAsBuild",
        "--params-json",
        json.dumps({"MTRL": mtrl, "SERN": sern, "EXPA": "1", "MEVA": "1"}),
        "--ionapi",
        str(ionapi),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr or result.stdout or "MOS256 fehlgeschlagen")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Ungültige MOS256 Antwort: {exc}") from exc


@app.get("/api/spareparts/search")
def spareparts_search(
    eqtp: str = Query(..., min_length=1),
    type_filter: str = Query("", max_length=80, alias="type"),
    item: str = Query("", max_length=80),
    serial: str = Query("", max_length=80),
    facility: str = Query("", max_length=40),
    bin: str = Query("", max_length=80),
    limit: int = Query(50, ge=1, le=200),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    table_name = _table_for(SPAREPARTS_TABLE, env)
    with _connect() as conn:
        _ensure_table(conn, table_name, SPAREPARTS_TABLE)
        clauses = ["TEILEART = ?", "UPPER(IFNULL(LAGERPLATZ, '')) <> 'INSTALLED'"]
        params: list[str] = [eqtp]
        if type_filter:
            clauses.append('"WAGEN-TYP" LIKE ?')
            params.append(f"%{type_filter}%")
        if item:
            clauses.append('"BAUREIHE" LIKE ?')
            params.append(f"%{item}%")
        if serial:
            clauses.append('"SERIENNUMMER" LIKE ?')
            params.append(f"%{serial}%")
        if facility:
            clauses.append('"LAGERORT" LIKE ?')
            params.append(f"%{facility}%")
        if bin:
            clauses.append('"LAGERPLATZ" LIKE ?')
            params.append(f"%{bin}%")
        sql = (
            f'SELECT ID, "BAUREIHE", "ITNO", "SERIENNUMMER", "WAGEN-TYP", LAGERORT, LAGERPLATZ '
            f"FROM {table_name} "
            f"WHERE {' AND '.join(clauses)} "
            f'ORDER BY "BAUREIHE", "SERIENNUMMER" '
            f"LIMIT ?"
        )
        params.append(limit)
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    return {"rows": rows, "eqtp": eqtp, "env": _normalize_env(env)}


@app.get("/api/spareparts/filters")
def spareparts_filters(
    eqtp: str = Query(..., min_length=1),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    table_name = _table_for(SPAREPARTS_TABLE, env)
    with _connect() as conn:
        _ensure_table(conn, table_name, SPAREPARTS_TABLE)

        def fetch(column: str, limit: int = 250) -> list[str]:
            sql = (
                f'SELECT DISTINCT "{column}" '
                f'FROM {table_name} '
                f'WHERE TEILEART = ? '
                f'AND UPPER(IFNULL(LAGERPLATZ, "")) <> "INSTALLED" '
                f'AND IFNULL("{column}", "") <> "" '
                f'ORDER BY "{column}" LIMIT {limit}'
            )
            return [row[0] for row in conn.execute(sql, (eqtp,)).fetchall()]

        return {
            "types": fetch("WAGEN-TYP"),
            "items": fetch("BAUREIHE"),
            "serials": fetch("SERIENNUMMER"),
            "facilities": fetch("LAGERORT"),
            "bins": fetch("LAGERPLATZ"),
        }


@app.get("/api/spareparts/selections")
def spareparts_selections(
    mtrl: str = Query(..., min_length=1),
    sern: str = Query(..., min_length=1),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    table_name = _table_for(SPAREPARTS_SWAP_TABLE, env)
    with _connect() as conn:
        _ensure_swap_table(conn, table_name)
        rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT *
                FROM {table_name}
                WHERE WAGEN_ITNO = ? AND WAGEN_SERN = ?
                """,
                (mtrl, sern),
            ).fetchall()
        ]
    return {"rows": rows, "env": _normalize_env(env)}


@app.post("/api/spareparts/select")
def spareparts_select(
    env: str = Query(DEFAULT_ENV),
    payload: dict = Body(...),
) -> dict:
    required = [
        "WAGEN_ITNO",
        "WAGEN_SERN",
        "ORIGINAL_ITNO",
        "ORIGINAL_SERN",
        "ERSATZ_ITNO",
        "ERSATZ_SERN",
    ]
    for field in required:
        if not payload.get(field):
            raise HTTPException(status_code=400, detail=f"Feld {field} ist erforderlich.")

    user = payload.get("USER") or os.getenv("SPAREPART_USER", "UNBEKANNT")
    upload_flag = payload.get("UPLOAD") or "N"
    timestamp = payload.get("TIMESTAMP") or datetime.utcnow().isoformat(timespec="seconds")

    table_name = _table_for(SPAREPARTS_SWAP_TABLE, env)
    with _connect() as conn:
        _ensure_swap_table(conn, table_name)
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                WAGEN_ITNO, WAGEN_SERN, ORIGINAL_ITNO, ORIGINAL_SERN,
                ERSATZ_ITNO, ERSATZ_SERN, USER, UPLOAD, TIMESTAMP
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(WAGEN_ITNO, WAGEN_SERN, ORIGINAL_ITNO, ORIGINAL_SERN)
            DO UPDATE SET
                ERSATZ_ITNO=excluded.ERSATZ_ITNO,
                ERSATZ_SERN=excluded.ERSATZ_SERN,
                USER=excluded.USER,
                UPLOAD=excluded.UPLOAD,
                TIMESTAMP=excluded.TIMESTAMP
            """,
            (
                payload["WAGEN_ITNO"],
                payload["WAGEN_SERN"],
                payload["ORIGINAL_ITNO"],
                payload["ORIGINAL_SERN"],
                payload["ERSATZ_ITNO"],
                payload["ERSATZ_SERN"],
                user,
                upload_flag,
                timestamp,
            ),
        )
        conn.commit()
    return {
        "message": "Ersatzteil gespeichert",
        "record": {
            **payload,
            "USER": user,
            "UPLOAD": upload_flag,
            "TIMESTAMP": timestamp,
            "env": _normalize_env(env),
        },
    }


@app.delete("/api/spareparts/select")
def spareparts_delete(
    env: str = Query(DEFAULT_ENV),
    payload: dict = Body(...),
) -> dict:
    required = ["WAGEN_ITNO", "WAGEN_SERN", "ORIGINAL_ITNO", "ORIGINAL_SERN"]
    for field in required:
        if not payload.get(field):
            raise HTTPException(status_code=400, detail=f"Feld {field} ist erforderlich.")
    table_name = _table_for(SPAREPARTS_SWAP_TABLE, env)
    with _connect() as conn:
        _ensure_swap_table(conn, table_name)
        cursor = conn.execute(
            f"""
            DELETE FROM {table_name}
            WHERE WAGEN_ITNO = ? AND WAGEN_SERN = ? AND ORIGINAL_ITNO = ? AND ORIGINAL_SERN = ?
            """,
            (
                payload["WAGEN_ITNO"],
                payload["WAGEN_SERN"],
                payload["ORIGINAL_ITNO"],
                payload["ORIGINAL_SERN"],
            ),
        )
        conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Kein Eintrag zum Löschen gefunden.")
    return {"message": "Ersatzteilzuordnung gelöscht", "env": _normalize_env(env)}


@app.get("/api/spareparts/swaps")
def spareparts_swaps(
    upload: str = Query("N"),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    flag = (upload or "").strip().upper()
    table_name = _table_for(SPAREPARTS_SWAP_TABLE, env)
    with _connect() as conn:
        _ensure_swap_table(conn, table_name)
        base_query = f"SELECT rowid AS ID, * FROM {table_name}"
        params: List[str] = []
        if flag:
            base_query += " WHERE UPPER(COALESCE(UPLOAD, '')) = ?"
            params.append(flag)
        base_query += " ORDER BY COALESCE(TIMESTAMP, '') DESC"
        rows = [dict(row) for row in conn.execute(base_query, params).fetchall()]
    return {"rows": rows, "env": _normalize_env(env)}


@app.post("/api/rsrd2/load_erp")
def rsrd2_load_erp(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _start_subprocess_job(
        "load_erp",
        _build_load_erp_cmd(env),
        env,
        lambda job_id: _finalize_load_erp(job_id, env),
    )
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/rsrd2/load_erp_full")
def rsrd2_load_erp_full(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _start_subprocess_job(
        "load_erp_full",
        _build_erp_full_cmd(env),
        env,
        lambda job_id: _finalize_load_erp_full(job_id, env),
    )
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.get("/api/rsrd2/jobs/{job_id}")
def rsrd2_job_status(job_id: str) -> dict:
    return _job_snapshot(job_id)


@app.get("/api/rsrd2/wagons")
def rsrd2_wagons(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> dict:
    with _connect() as conn:
        _ensure_rsrd_tables(conn)
        rows = [
            {
                "wagon_id": row["wagon_id"],
                "updated_at": row["updated_at"],
                "data": json.loads(row["data_json"]),
            }
            for row in conn.execute(
                f"""
                SELECT wagon_id, data_json, updated_at
                FROM {RSRD_WAGONS_TABLE}
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
        ]
        total = conn.execute(f"SELECT COUNT(*) FROM {RSRD_WAGONS_TABLE}").fetchone()[0]
    return {"rows": rows, "limit": limit, "offset": offset, "total": total}


@app.post("/api/rsrd2/sync")
def rsrd2_sync(payload: dict = Body(...)) -> dict:
    wagons = payload.get("wagons") or []
    if not isinstance(wagons, list) or not all(isinstance(item, str) for item in wagons):
        raise HTTPException(status_code=400, detail="Feld 'wagons' muss eine Liste von Wagennummern sein.")
    snapshots = bool(payload.get("snapshots", True))
    try:
        rsrd_sync_wagons(wagons, keep_snapshots=snapshots)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"synced": len(wagons), "snapshots": snapshots}


@app.post("/api/rsrd2/sync_all")
def rsrd2_sync_all(
    limit: int | None = Query(None, gt=0),
    snapshots: bool = Query(True),
) -> dict:
    with _connect() as conn:
        wagons = _fetch_erp_wagon_numbers(conn, limit)
    if not wagons:
        raise HTTPException(status_code=404, detail="Keine Wagennummern in RSRD_ERP_WAGONNO gefunden.")
    try:
        stats = rsrd_sync_wagons(wagons, keep_snapshots=snapshots, mode="full")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"synced": len(wagons), "staged": stats["staged"], "processed": stats["processed"], "snapshots": snapshots}


@app.post("/api/rsrd2/fetch_json")
def rsrd2_fetch_json(
    limit: int | None = Query(None, gt=0),
    snapshots: bool = Query(False),
) -> dict:
    with _connect() as conn:
        wagons = _fetch_erp_wagon_numbers(conn, limit)
    if not wagons:
        raise HTTPException(status_code=404, detail="Keine Wagennummern in RSRD_ERP_WAGONNO gefunden.")
    try:
        stats = rsrd_sync_wagons(wagons, keep_snapshots=snapshots, mode="stage")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"staged": stats["staged"], "snapshots": snapshots}


@app.post("/api/rsrd2/process_json")
def rsrd2_process_json(limit: int | None = Query(None, gt=0)) -> dict:
    try:
        stats = rsrd_sync_wagons([], keep_snapshots=False, mode="process", process_limit=limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"processed": stats["processed"], "limit": limit}


# Serve frontend assets
if FRONTEND_DIR.exists():
    app.mount(
        "/",
        StaticFiles(directory=FRONTEND_DIR, html=True),
        name="frontend",
    )
else:
    @app.get("/")
    def placeholder() -> dict:
        return {"message": "Frontend noch nicht angelegt. Lege Dateien in /frontend/ ab."}
