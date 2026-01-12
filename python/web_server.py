"""FastAPI server serving the loader UI and paginated wagon data."""
from __future__ import annotations

import os
import time
from xml.sax.saxutils import escape as xml_escape
import sqlite3
import subprocess
import sys
import json
import re
from urllib.parse import urlsplit, urlunsplit, urlencode
from pathlib import Path
from typing import List, Dict, Any, Mapping
import threading
import uuid

from datetime import datetime

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Body, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .env_loader import load_project_dotenv
from .rsrd2_sync import (
    RSRDTables,
    BASE_DETAIL_TABLE,
    BASE_JSON_TABLE,
    BASE_SNAPSHOTS_TABLE,
    BASE_WAGONS_TABLE,
    init_db as rsrd_init_db,
    sync_wagons as rsrd_sync_wagons,
    tables_for_env as rsrd_tables_for_env,
)
from .rsrd_compare import (
    build_erp_payload,
    compare_erp_to_rsrd,
    serialize_diffs,
    serialize_payload,
)
from .m3_api_call import (
    load_ionapi_config,
    get_access_token_service_account,
    build_base_url,
    call_m3_mi_get,
)

load_project_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "cache.db"
API_LOG_PATH = PROJECT_ROOT / "data" / "API.log"
FRONTEND_DIR = PROJECT_ROOT / "frontend"
IONAPI_DIR = PROJECT_ROOT / "credentials" / "ionapi"
DEFAULT_TABLE = "wagons"
SPAREPARTS_TABLE = "spareparts"
SPAREPARTS_SWAP_TABLE = "sparepart_swaps"
WAGENUMBAU_TABLE = "Wagenumbau_Wagons"
RENUMBER_WAGON_TABLE = "RENUMBER_WAGON"
RENUMBER_EXTRA_COLUMNS = [
    "SEQ",
    "WAGEN_ITNO",
    "WAGEN_SERN",
    "NEW_SERN",
    "NEW_BAUREIHE",
    "NEW_PART_ITNO",
    "NEW_PART_SER2",
    "UMBAU_DATUM",
    "UMBAU_ART",
    "PLPN",
    "MWNO",
    "MOS180_STATUS",
    "MOS050_STATUS",
    "CRS335_STATUS",
    "STS046_STATUS",
    "STS046_ADD_STATUS",
    "MMS240_STATUS",
    "CUSEXT_STATUS",
    "OUT",
    "UPDATED_AT",
    "IN",
    "TIMESTAMP_IN",
]
RSRD_ERP_TABLE = "RSRD_ERP_WAGONNO"
RSRD_ERP_FULL_TABLE = "RSRD_ERP_DATA"
RSRD_UPLOAD_TABLE = "RSRD_WAGON_UPLOAD"
DEFAULT_SCHEME = os.getenv("SPAREPART_SCHEME", "datalake")
SQL_FILE = PROJECT_ROOT / "sql" / "wagons_base.sql"
SPAREPARTS_SQL_FILE = PROJECT_ROOT / "sql" / "spareparts_base.sql"
RSRD_ERP_SQL_FILE = PROJECT_ROOT / "sql" / "rsrd_erp_full.sql"
DEFAULT_ENV = os.getenv("SPAREPART_ENV", "prd").lower()
ENV_ALIASES = {
    "live": "prd",
    "prod": "prd",
    "prd": "prd",
    "test": "tst",
    "tst": "tst",
}
ENV_SUFFIXES = {"prd": "_PRD", "tst": "_TST"}
ENV_IONAPI = {
    "prd": {
        "compass": IONAPI_DIR / "Infor Compass JDBC Driver.ionapi",
        "mi": IONAPI_DIR / "MFD_Backend_Python.ionapi",
    },
    "tst": {
        "compass": IONAPI_DIR / "Infor Compass JDBC Driver_TST.ionapi",
        "mi": IONAPI_DIR / "TST_MFD_Backend_Python_new.ionapi",
    },
}
MOS125_DRY_RUN = os.getenv("SPAREPART_MOS125_DRY_RUN", "1").strip().lower() in {"1", "true", "yes", "y"}
CMS100_RETRY_DELAY_SEC = float(os.getenv("SPAREPART_CMS100_RETRY_DELAY", "3").strip() or "3")
CMS100_RETRY_MAX = int(os.getenv("SPAREPART_CMS100_RETRY_MAX", "0").strip() or "0")
WAGON_CMS100_RETRY_DELAY_SEC = float(os.getenv("SPAREPART_WAGON_CMS100_RETRY_DELAY", "5").strip() or "5")
MOS170_RETRY_DELAY_SEC = float(os.getenv("SPAREPART_MOS170_RETRY_DELAY", "3").strip() or "3")
MOS170_RETRY_MAX = int(os.getenv("SPAREPART_MOS170_RETRY_MAX", "5").strip() or "5")
API_LOG_ONLY = [
    value.strip()
    for value in os.getenv("SPAREPART_API_LOG_ONLY", "").split(",")
    if value.strip()
]
IPS_COMPANY = os.getenv("SPAREPART_IPS_COMPANY", "").strip()
IPS_DIVISION = os.getenv("SPAREPART_IPS_DIVISION", "").strip()
MOS180_FACI = os.getenv("SPAREPART_MOS180_FACI", "100").strip()
MOS180_RESP = os.getenv("SPAREPART_MOS180_RESP", "CHRUPP").strip()
MOS180_APRB = os.getenv("SPAREPART_MOS180_APRB", "CHRUPP").strip()
MOS050_LOCATION = os.getenv("SPAREPART_MOS050_LOCATION", "EINBAU").strip()
MOS050_SERVICE = os.getenv("SPAREPART_MOS050_SERVICE", "MOS050_MONTAGE").strip()
MOS050_OPERATION = os.getenv("SPAREPART_MOS050_OPERATION", "MOS050").strip()
MOS050_NAMESPACE = os.getenv("SPAREPART_MOS050_NAMESPACE", "").strip()
MOS050_BODY_TAG = os.getenv("SPAREPART_MOS050_BODY_TAG", "MOS050").strip()
CRS335_ACRF = os.getenv("SPAREPART_CRS335_ACRF", "").strip()

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


@app.on_event("startup")
def _prepare_env_tables() -> None:
    if not DB_PATH.exists():
        return
    with _connect() as conn:
        _ensure_env_tables(conn)
        conn.commit()


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
    normalized = ENV_ALIASES.get(value)
    if not normalized:
        raise HTTPException(status_code=400, detail="Ungültige Umgebung.")
    return normalized


def _effective_dry_run(env: str | None) -> bool:
    normalized = _normalize_env(env)
    if normalized == "prd":
        return True
    return MOS125_DRY_RUN


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


def _sanitize_url(value: str) -> str:
    if not value:
        return ""
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    hostname = parts.hostname or ""
    netloc = hostname
    if parts.port:
        netloc = f"{hostname}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _safe_ionapi_url(env: str, kind: str) -> str:
    try:
        ionapi = _ionapi_path(env, kind)
    except HTTPException:
        return ""
    try:
        data = json.loads(ionapi.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if isinstance(data, dict):
        return _sanitize_url(str(data.get("iu") or ""))
    return ""


def _resolve_rsrd_wsdl(env: str | None) -> str:
    normalized = _normalize_env(env)
    suffix = "PRD" if normalized == "prd" else "TST"
    value = os.getenv(f"RSRD_WSDL_URL_{suffix}") or os.getenv("RSRD_WSDL_URL") or ""
    return _sanitize_url(value)


def _rsrd_tables(env: str | None) -> RSRDTables:
    return rsrd_tables_for_env(_normalize_env(env))


def _ensure_rsrd_tables(conn: sqlite3.Connection, env: str | None) -> RSRDTables:
    tables = _rsrd_tables(env)
    rsrd_init_db(conn, tables=tables)
    return tables


def _ensure_rsrd_upload_table(conn: sqlite3.Connection, env: str | None) -> str:
    table_name = _table_for(RSRD_UPLOAD_TABLE, env)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            wagon_number_freight TEXT PRIMARY KEY,
            rsrd_wagon_id TEXT,
            payload_json TEXT NOT NULL,
            diff_json TEXT NOT NULL,
            rsrd_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    return table_name


def _fetch_erp_wagon_numbers(
    conn: sqlite3.Connection,
    env: str | None,
    limit: int | None = None,
) -> List[str]:
    table_name = _table_for(RSRD_ERP_TABLE, env)
    try:
        query = f"SELECT wagon_sern, wagon_sern_numeric FROM {table_name} ORDER BY wagon_sern"
        params: List[int] = []
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(query, params).fetchall()
    except sqlite3.OperationalError as exc:  # table missing
        raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.") from exc
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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    )
    return cursor.fetchone() is not None


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    existing = {
        row[1]
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        if row and len(row) > 1
    }
    for col in columns:
        if col not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [
        row[1]
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        if row and len(row) > 1
    ]


def _columns_from_sql_file(sql_path: Path) -> List[str]:
    if not sql_path.exists():
        return []
    try:
        sql_text = sql_path.read_text(encoding="utf-8")
    except OSError:
        return []
    matches = re.findall(r"\bas\s+(['\"])([^'\"]+)\1", sql_text, flags=re.IGNORECASE)
    columns: List[str] = []
    seen = set()
    for _, name in matches:
        if name not in seen:
            columns.append(name)
            seen.add(name)
    return columns


def _create_table_from_columns(conn: sqlite3.Connection, table: str, columns: List[str]) -> None:
    if _table_exists(conn, table) or not columns:
        return
    column_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({column_defs})')


def _clone_table_schema(conn: sqlite3.Connection, source: str, target: str) -> None:
    if _table_exists(conn, target) or not _table_exists(conn, source):
        return
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{target}" AS SELECT * FROM "{source}" WHERE 0=1')


def _ordered_columns(columns: List[str], order_hint: List[str] | None) -> List[str]:
    if not order_hint:
        return columns
    ordered = [col for col in order_hint if col in columns]
    remaining = [col for col in columns if col not in ordered]
    ordered.extend(sorted(remaining, key=lambda name: name.upper()))
    return ordered


def _rebuild_table_with_order(
    conn: sqlite3.Connection,
    table: str,
    ordered_columns: List[str],
) -> None:
    if not ordered_columns:
        return
    existing_columns = _table_columns(conn, table)
    if existing_columns == ordered_columns:
        return
    temp_name = f"{table}__tmp"
    column_defs = ", ".join(f'"{col}" TEXT' for col in ordered_columns)
    copy_columns = [col for col in ordered_columns if col in existing_columns]
    column_list = ", ".join(f'"{col}"' for col in copy_columns)
    try:
        conn.execute("BEGIN")
        conn.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
        conn.execute(f'CREATE TABLE "{temp_name}" ({column_defs})')
        if copy_columns:
            conn.execute(
                f'INSERT INTO "{temp_name}" ({column_list}) SELECT {column_list} FROM "{table}"'
            )
        conn.execute(f'DROP TABLE "{table}"')
        conn.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table}"')
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _ensure_env_table_pair(
    conn: sqlite3.Connection,
    base: str,
    columns_hint: List[str] | None = None,
    extra_columns: List[str] | None = None,
    enforce_order: bool = False,
) -> None:
    prd_table = _table_for(base, "prd")
    tst_table = _table_for(base, "tst")
    prd_exists = _table_exists(conn, prd_table)
    tst_exists = _table_exists(conn, tst_table)

    if not prd_exists and not tst_exists and columns_hint:
        _create_table_from_columns(conn, prd_table, columns_hint)
        _create_table_from_columns(conn, tst_table, columns_hint)
    elif not prd_exists and tst_exists:
        _clone_table_schema(conn, tst_table, prd_table)
    elif not tst_exists and prd_exists:
        _clone_table_schema(conn, prd_table, tst_table)

    prd_exists = _table_exists(conn, prd_table)
    tst_exists = _table_exists(conn, tst_table)
    if not prd_exists and not tst_exists:
        return

    prd_columns = _table_columns(conn, prd_table) if prd_exists else []
    tst_columns = _table_columns(conn, tst_table) if tst_exists else []
    merged = list(
        dict.fromkeys(prd_columns + tst_columns + (columns_hint or []) + (extra_columns or []))
    )

    if prd_exists:
        _ensure_columns(conn, prd_table, merged)
    if tst_exists:
        _ensure_columns(conn, tst_table, merged)

    if enforce_order:
        ordered = _ordered_columns(merged, columns_hint)
        if prd_exists:
            _rebuild_table_with_order(conn, prd_table, ordered)
        if tst_exists:
            _rebuild_table_with_order(conn, tst_table, ordered)


def _ensure_renumber_schema(conn: sqlite3.Connection, table_name: str) -> None:
    if not _table_exists(conn, table_name):
        return
    existing_info = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    existing_columns = [row[1] for row in existing_info if row and len(row) > 1]
    missing = [col for col in RENUMBER_EXTRA_COLUMNS if col not in existing_columns]
    if not missing:
        return
    base_columns = [col for col in existing_columns if col not in RENUMBER_EXTRA_COLUMNS]
    ordered_columns = base_columns + [
        col for col in RENUMBER_EXTRA_COLUMNS if col in existing_columns or col in missing
    ]
    temp_name = f"{table_name}__tmp"
    column_defs = ", ".join(f'"{col}" TEXT' for col in ordered_columns)
    copy_columns = [col for col in ordered_columns if col in existing_columns]
    column_list = ", ".join(f'"{col}"' for col in copy_columns)
    try:
        conn.execute("BEGIN")
        conn.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
        conn.execute(f'CREATE TABLE "{temp_name}" ({column_defs})')
        if copy_columns:
            conn.execute(
                f'INSERT INTO "{temp_name}" ({column_list}) SELECT {column_list} FROM "{table_name}"'
            )
        conn.execute(f'DROP TABLE "{table_name}"')
        conn.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _ensure_env_tables(conn: sqlite3.Connection) -> None:
    wagons_columns = _columns_from_sql_file(SQL_FILE)
    spareparts_columns = _columns_from_sql_file(SPAREPARTS_SQL_FILE)
    rsrd_full_columns = _columns_from_sql_file(RSRD_ERP_SQL_FILE)

    _ensure_env_table_pair(conn, DEFAULT_TABLE, columns_hint=wagons_columns, enforce_order=True)
    _ensure_env_table_pair(conn, WAGENUMBAU_TABLE, columns_hint=wagons_columns, enforce_order=True)
    _ensure_env_table_pair(conn, SPAREPARTS_TABLE, columns_hint=spareparts_columns, enforce_order=True)
    _ensure_env_table_pair(conn, RSRD_ERP_FULL_TABLE, columns_hint=rsrd_full_columns, enforce_order=True)
    _ensure_env_table_pair(
        conn,
        RSRD_ERP_TABLE,
        columns_hint=["wagon_sern", "wagon_sern_numeric", "updated_at"],
        enforce_order=True,
    )
    _ensure_env_table_pair(
        conn,
        RENUMBER_WAGON_TABLE,
        columns_hint=RENUMBER_EXTRA_COLUMNS,
        extra_columns=RENUMBER_EXTRA_COLUMNS,
    )
    _ensure_env_table_pair(conn, SPAREPARTS_SWAP_TABLE)

    _ensure_swap_table(conn, _table_for(SPAREPARTS_SWAP_TABLE, "prd"))
    _ensure_swap_table(conn, _table_for(SPAREPARTS_SWAP_TABLE, "tst"))
    _ensure_rsrd_upload_table(conn, "prd")
    _ensure_rsrd_upload_table(conn, "tst")
    _ensure_rsrd_tables(conn, "prd")
    _ensure_rsrd_tables(conn, "tst")
    _ensure_env_table_pair(conn, RSRD_UPLOAD_TABLE)
    _ensure_env_table_pair(conn, BASE_WAGONS_TABLE)
    _ensure_env_table_pair(conn, BASE_SNAPSHOTS_TABLE)
    _ensure_env_table_pair(conn, BASE_JSON_TABLE)
    _ensure_env_table_pair(conn, BASE_DETAIL_TABLE)

    _ensure_renumber_schema(conn, _table_for(RENUMBER_WAGON_TABLE, "prd"))
    _ensure_renumber_schema(conn, _table_for(RENUMBER_WAGON_TABLE, "tst"))


def _clear_table_rows(table_base: str, env: str) -> None:
    table_name = _table_for(table_base, env)
    with _connect() as conn:
        if _table_exists(conn, table_name):
            conn.execute(f'DELETE FROM "{table_name}"')
            conn.commit()


def _update_job(job_id: str, **updates: Any) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.update(updates)


def _append_job_result(job_id: str, result: Dict[str, Any]) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.setdefault("results", []).append(result)


def _format_yyyymmdd(value: str) -> str:
    if not value:
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return digits


def _format_ddmmyy(value: str) -> str:
    if not value:
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) >= 8:
        if digits.startswith(("19", "20")):
            yyyymmdd = digits[:8]
            return f"{yyyymmdd[6:8]}{yyyymmdd[4:6]}{yyyymmdd[2:4]}"
        ddmmyyyy = digits[:8]
        return f"{ddmmyyyy[0:4]}{ddmmyyyy[6:8]}"
    if len(digits) >= 6:
        return digits[:6]
    return digits


def _hierarchy_level(value: str) -> int:
    if not value:
        return 0
    return str(value).count("-")


def _row_value(row: sqlite3.Row, *keys: str) -> str:
    for key in keys:
        if key in row.keys():
            value = row[key]
            if value is not None and value != "":
                return str(value)
    return ""


def _model_suffix(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    if "_" in cleaned:
        return cleaned.split("_")[-1]
    return cleaned


def _wagon_serial_suffix(value: str) -> str:
    cleaned = re.sub(r"\s+", "", value or "")
    if not cleaned:
        return ""
    suffix = cleaned[-10:] if len(cleaned) > 10 else cleaned
    trimmed = suffix.lstrip("0")
    return trimmed or suffix


def _compute_part_updates(
    row: sqlite3.Row,
    new_sern: str,
    new_baureihe: str,
) -> tuple[str, str]:
    itno = _row_value(row, "ITNO")
    ser2 = _row_value(row, "SER2")
    old_model = _model_suffix(_row_value(row, "WAGEN_ITNO"))
    new_model = _model_suffix(new_baureihe)
    new_itno = ""
    if itno and old_model and new_model:
        if itno.endswith(f"_{old_model}") or itno.endswith(old_model):
            new_itno = itno[: -len(old_model)] + new_model
            if new_itno == itno:
                new_itno = ""
    old_suffix = _wagon_serial_suffix(_row_value(row, "WAGEN_SERN"))
    new_suffix = _wagon_serial_suffix(new_sern)
    new_ser2 = ""
    if ser2 and "-" in ser2 and old_suffix and new_suffix:
        if ser2.endswith(old_suffix):
            new_ser2 = ser2[: -len(old_suffix)] + new_suffix
            if new_ser2 == ser2:
                new_ser2 = ""
    return new_itno, new_ser2


def _needs_renumber(row: sqlite3.Row) -> bool:
    if not _row_value(row, "SER2"):
        return False
    return bool(_row_value(row, "NEW_PART_ITNO") or _row_value(row, "NEW_PART_SER2"))


def _is_ok_status(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized.startswith("ok") or "success" in normalized or "erfolg" in normalized


def _renumber_row_key(row: Mapping[str, Any]) -> str:
    cfgl = row.get("CFGL") or row.get("MFGL") or ""
    parts = [
        str(cfgl),
        str(row.get("ITNO") or ""),
        str(row.get("SER2") or ""),
        str(row.get("MTRL") or ""),
        str(row.get("SERN") or ""),
    ]
    return "||".join(parts)


def _renumber_sort_key(row: Dict[str, Any]) -> str:
    return str(row.get("CFGL") or row.get("MFGL") or "")


def _wagon_log_context(row: sqlite3.Row) -> Dict[str, str]:
    return {
        "itno": _row_value(row, "WAGEN_ITNO", "NEW_BAUREIHE"),
        "sern": _row_value(row, "WAGEN_SERN", "NEW_SERN"),
        "new_itno": _row_value(row, "NEW_BAUREIHE"),
        "new_sern": _row_value(row, "NEW_SERN"),
    }


def _mi_error_message(payload: Any) -> str:
    if payload is None:
        return "Leere Antwort"
    if isinstance(payload, dict):
        status_code = payload.get("status_code") or payload.get("statusCode")
        if status_code and int(status_code) != 200:
            return f"HTTP {status_code}"
        if "text" in payload and payload.get("text"):
            text = str(payload.get("text"))
            if "error" in text.lower() or "fehler" in text.lower():
                return text
        response = payload.get("MIResponse") or payload.get("response") or payload
        if isinstance(response, dict):
            for key in ("ErrorNumber", "errorNumber", "ErrorCode", "errorCode", "Error"):
                value = response.get(key)
                if value is None or value == "":
                    continue
                if str(value) not in {"0", "00"}:
                    return f"{key}={value}"
            messages = response.get("Messages") or response.get("messages")
            if isinstance(messages, dict):
                message_entries = messages.get("Message") or messages.get("message") or []
                if not isinstance(message_entries, list):
                    message_entries = [message_entries]
                for entry in message_entries:
                    if not isinstance(entry, dict):
                        continue
                    msg_type = str(entry.get("MessageType") or entry.get("messageType") or "").strip()
                    msg_text = str(entry.get("MessageText") or entry.get("messageText") or "").strip()
                    if msg_type in {"2", "3", "4", "E", "ERROR"}:
                        return msg_text or f"MessageType={msg_type}"
                    if msg_text and ("error" in msg_text.lower() or "fehler" in msg_text.lower()):
                        return msg_text
            for key in (
                "ErrorMessage",
                "errorMessage",
                "ErrorMsg",
                "errorMsg",
                "ErrorText",
                "errorText",
            ):
                value = response.get(key)
                if value:
                    return str(value)
            message = response.get("Message") or response.get("message")
            if message:
                text = str(message)
                if "error" in text.lower() or "fehler" in text.lower():
                    return text
        for key, value in payload.items():
            lower = key.lower()
            if lower in {"error", "errormessage", "errormsg", "errortext"} and value:
                return str(value)
            if isinstance(value, (dict, list)):
                nested = _mi_error_message(value)
                if nested:
                    return nested
    if isinstance(payload, list):
        for item in payload:
            nested = _mi_error_message(item)
            if nested:
                return nested
    return ""


def _build_m3_request_url(base_url: str, program: str, transaction: str, params: Dict[str, Any]) -> str:
    base = base_url.rstrip("/") if base_url else ""
    path = f"/M3/m3api-rest/execute/{program}/{transaction}"
    url = f"{base}{path}" if base else path
    if params:
        return f"{url}?{urlencode(params)}"
    return url


def _build_ips_request_url(base_url: str, service_name: str) -> str:
    base = base_url.rstrip("/") if base_url else ""
    path = f"/M3/ips/service/{service_name}"
    return f"{base}{path}" if base else path


def _build_ips_envelope(
    service_name: str,
    operation: str,
    params: Dict[str, str],
    namespace_override: str | None = None,
    body_tag_override: str | None = None,
) -> str:
    namespace = namespace_override or f"http://schemas.infor.com/ips/{service_name}/{operation}"
    parts = []
    for key, value in params.items():
        safe = xml_escape(value or "")
        parts.append(f"<chg:{key}>{safe}</chg:{key}>")
    body = "".join(parts)
    header = "<soapenv:Header/>"
    if IPS_COMPANY or IPS_DIVISION:
        cred_parts = []
        if IPS_COMPANY:
            cred_parts.append(f"<cred:company>{xml_escape(IPS_COMPANY)}</cred:company>")
        if IPS_DIVISION:
            cred_parts.append(f"<cred:division>{xml_escape(IPS_DIVISION)}</cred:division>")
        cred_body = "".join(cred_parts)
        header = f"<soapenv:Header><cred:lws>{cred_body}</cred:lws></soapenv:Header>"
    body_tag = body_tag_override or service_name
    return (
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:chg="{namespace}" xmlns:cred="http://lawson.com/ws/credentials">'
        f"{header}"
        "<soapenv:Body>"
        f"<chg:{operation}><chg:{body_tag}>{body}</chg:{body_tag}></chg:{operation}>"
        "</soapenv:Body>"
        "</soapenv:Envelope>"
    )


def _call_ips_service(
    base_url: str,
    access_token: str,
    service_name: str,
    operation: str,
    params: Dict[str, str],
    namespace_override: str | None = None,
    body_tag_override: str | None = None,
) -> Dict[str, Any]:
    url = _build_ips_request_url(base_url, service_name)
    body = _build_ips_envelope(
        service_name,
        operation,
        params,
        namespace_override=namespace_override,
        body_tag_override=body_tag_override,
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "text/xml",
        "Content-Type": "text/xml; charset=utf-8",
    }
    import requests

    resp = requests.post(url, headers=headers, data=body.encode("utf-8"), timeout=60)
    return {
        "status_code": resp.status_code,
        "text": resp.text,
        "request_body": body,
    }


def _append_api_log(
    action: str,
    params: Dict[str, Any],
    response: Any,
    ok: bool,
    error: str | None = None,
    env: str | None = None,
    wagon: Dict[str, str] | None = None,
    dry_run: bool | None = None,
    request_url: str | None = None,
    program: str = "MOS125MI",
    transaction: str = "RemoveInstall",
    request_method: str = "GET",
    status: str | None = None,
) -> None:
    if API_LOG_ONLY and action not in API_LOG_ONLY:
        return
    entry = {
        "ts": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
        "env": env or "",
        "action": action,
        "wagon": wagon or {},
        "program": program,
        "transaction": transaction,
        "dry_run": bool(dry_run),
        "request": {"method": request_method, "url": request_url or ""},
        "ok": ok,
        "params": params,
        "error": error or "",
        "response": response,
    }
    if status is not None:
        entry["status"] = status
    try:
        API_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with API_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True, default=str))
            handle.write("\n")
    except Exception:  # noqa: BLE001
        pass


def _clear_api_log() -> None:
    try:
        API_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        API_LOG_PATH.write_text("", encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def _build_mos125_params(row: sqlite3.Row, mode: str = "out") -> Dict[str, str]:
    cfgr = _row_value(row, "CFGL", "MFGL")
    level = _hierarchy_level(cfgr)
    base_trtm = 10000 + (level * 1000)
    params = {
        "RITP": "UMA",
        "RESP": "CHRUPP",
        "TRDT": _format_yyyymmdd(_row_value(row, "UMBAU_DATUM")),
        "WHLO": "ZUM",
        "RSC4": "UMB",
        "TRTM": str(base_trtm),
    }
    if mode == "in":
        new_baureihe = _row_value(row, "NEW_BAUREIHE")
        new_sern = _row_value(row, "NEW_SERN")
        cfgr_value = str(cfgr or "").strip()
        top_level = cfgr_value.isdigit() and int(cfgr_value) in {1, 2, 3, 4}
        parent_itno = _row_value(row, "MTRL")
        parent_sern = _row_value(row, "SERN")
        if top_level:
            parent_itno = new_baureihe or _row_value(row, "WAGEN_ITNO") or parent_itno
            parent_sern = new_sern or _row_value(row, "WAGEN_SERN") or parent_sern
        else:
            parent_row = {
                "ITNO": parent_itno,
                "SER2": parent_sern,
                "WAGEN_ITNO": _row_value(row, "WAGEN_ITNO"),
                "WAGEN_SERN": _row_value(row, "WAGEN_SERN"),
            }
            updated_parent_itno, updated_parent_sern = _compute_part_updates(parent_row, new_sern, new_baureihe)
            parent_itno = updated_parent_itno or parent_itno
            parent_sern = updated_parent_sern or parent_sern
        part_itno = _row_value(row, "NEW_PART_ITNO") or _row_value(row, "ITNO")
        part_ser2 = _row_value(row, "NEW_PART_SER2") or _row_value(row, "SER2")
        params["RITP"] = "UME"
        params["TRTM"] = str(base_trtm + 10)
        params["CFGL"] = cfgr
        params["TWSL"] = "EINBAU"
        params["NHAI"] = parent_itno
        params["NHSI"] = parent_sern
        params["ITNI"] = part_itno
        params["BANI"] = part_ser2
    else:
        params["CFGR"] = cfgr
        params["TWSL"] = "AUSBAU"
        params["NHAR"] = _row_value(row, "MTRL")
        params["NHSR"] = _row_value(row, "SERN")
        params["ITNR"] = _row_value(row, "ITNO")
        params["BANR"] = _row_value(row, "SER2")
    return params


def _build_mos170_params(row: sqlite3.Row) -> Dict[str, str]:
    umbau_datum = _format_yyyymmdd(_row_value(row, "UMBAU_DATUM"))
    return {
        "ITNO": _row_value(row, "ITNO"),
        "BANO": _row_value(row, "SER2"),
        "STRT": "002",
        "SUFI": "SERIENNUMMER AENDERN",
        "STDT": umbau_datum,
        "FIDT": umbau_datum,
        "RESP": "CHRUPP",
        "WHLO": "ZUM",
    }


# BEGIN WAGON RENNUMBERING
def _build_mos170_wagon_params(itno: str, sern: str, umbau_datum: str, whlo: str) -> Dict[str, str]:
    umbau_value = _format_yyyymmdd(umbau_datum)
    return {
        "ITNO": itno,
        "BANO": sern,
        "STRT": "002",
        "SUFI": "SERIENNUMMER AENDERN",
        "STDT": umbau_value,
        "FIDT": umbau_value,
        "RESP": "CHRUPP",
        "WHLO": whlo,
    }
# END WAGON RENNUMBERING

def _extract_plpn(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    rows = _extract_mi_rows({"response": response})
    for row in rows:
        value = row.get("PLPN") or row.get("plpn") or row.get("PlannedOrder")
        if value:
            return str(value)
    for key in ("PLPN", "plpn", "PlannedOrder"):
        value = response.get(key)
        if value:
            return str(value)
    return ""


def _build_cms100_params(plpn: str) -> Dict[str, str]:
    return {
        "QOPLPN": plpn,
        "QOPLPS": "0",
        "QOPLP2": "0",
    }


def _build_ips_mos100_params(row: sqlite3.Row) -> Dict[str, str]:
    itno = _row_value(row, "ITNO")
    ser2 = _row_value(row, "SER2")
    new_itno = _row_value(row, "NEW_PART_ITNO") or itno
    new_ser2 = _row_value(row, "NEW_PART_SER2") or ser2
    return {
        "WorkOrderNumber": _row_value(row, "MWNO"),
        "Product": itno,
        "NewItemNumber": new_itno,
        "NewLotNumber": new_ser2,
    }


def _build_mos180_params(row: sqlite3.Row) -> Dict[str, str]:
    return {
        "FACI": MOS180_FACI,
        "MWNO": _row_value(row, "MWNO"),
        "RESP": MOS180_RESP,
        "APRB": MOS180_APRB,
    }


def _build_mos050_params(row: sqlite3.Row) -> Dict[str, str]:
    product = _row_value(row, "ITNO")
    mwno = _row_value(row, "MWNO")
    return {
        "WHFACI": "100",
        "WHMWNO": mwno,
        "WHPRNO": product,
        "WHWHSL": MOS050_LOCATION,
        "WWRPDT": _format_ddmmyy(_row_value(row, "UMBAU_DATUM")),
        "WHBANO": "",
        "WLBREF": "",
    }


def _build_mms240_params(new_itno: str, new_sern: str) -> Dict[str, str]:
    clean_serial = re.sub(r"[^0-9]", "", new_sern)
    return {
        "ITNO": new_itno,
        "SERN": new_sern,
        "FLNO": clean_serial,
    }


def _build_cusext_params(new_itno: str, new_sern: str) -> Dict[str, str]:
    return {
        "FILE": "MILOIN",
        "PK01": new_itno,
        "PK02": new_sern,
        "A130": "2 - DOT-Übertragung aktiv",
    }


def _extract_mwno(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    rows = _extract_mi_rows({"response": response})
    for row in rows:
        value = (
            row.get("QOMWNO")
            or row.get("qomwno")
            or row.get("MWNO")
            or row.get("mwno")
            or row.get("WorkOrderNumber")
        )
        if value:
            return str(value)
    for key in ("QOMWNO", "qomwno", "MWNO", "mwno", "WorkOrderNumber"):
        value = response.get(key)
        if value:
            return str(value)
    return ""


def _build_crs335_params(acrf: str, new_sern: str, new_baureihe: str) -> Dict[str, str]:
    return {
        "ACRF": acrf,
        "TX40": new_sern,
        "TX15": new_baureihe,
    }


def _build_sts046_params(whlo: str, geit: str, itno: str, bano: str) -> Dict[str, str]:
    return {
        "WHLO": whlo,
        "GEIT": geit,
        "ITNO": itno,
        "BANO": bano,
    }




def _ensure_wagon_data(table: str, env: str) -> str:
    env_table = _table_for(table, env)
    with _connect() as conn:
        if _table_exists(conn, env_table):
            return env_table

    result = _run_compass_to_sqlite(SQL_FILE, env_table, env)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Reload fehlgeschlagen: {result.stderr or result.stdout}",
        )
    return env_table


def _extract_mi_rows(payload: dict) -> List[Dict[str, Any]]:
    response = payload.get("response") or {}
    records = (
        response.get("MIRecord")
        or response.get("MIRecords")
        or response.get("MIResponse")
        or []
    )
    if not isinstance(records, list):
        records = [records]
    rows: List[Dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        name_values = record.get("NameValue") or record.get("nameValue") or []
        if not isinstance(name_values, list):
            name_values = [name_values]
        row: Dict[str, Any] = {}
        for entry in name_values:
            if not isinstance(entry, dict):
                continue
            name = entry.get("Name") or entry.get("name")
            if not name:
                continue
            value = entry.get("Value") if entry.get("Value") is not None else entry.get("value")
            row[str(name)] = "" if value is None else value
        if row:
            rows.append(row)
    return rows


def _store_mi_rows(
    table_base: str,
    env: str,
    rows: List[Dict[str, Any]],
    wagon_itno: str | None = None,
    wagon_sern: str | None = None,
) -> str:
    base = _validate_table(table_base)
    table_name = _table_for(base, env)
    existing_records: List[Dict[str, Any]] = []
    if base == RENUMBER_WAGON_TABLE and rows:
        if wagon_itno or wagon_sern:
            for row in rows:
                if wagon_itno:
                    row["WAGEN_ITNO"] = wagon_itno
                if wagon_sern:
                    row["WAGEN_SERN"] = wagon_sern
        with _connect() as conn:
            if _table_exists(conn, table_name):
                _ensure_renumber_schema(conn, table_name)
                existing_records = [
                    dict(row)
                    for row in conn.execute(f'SELECT * FROM "{table_name}" ORDER BY rowid').fetchall()
                ]
        if existing_records:
            existing_map: Dict[str, List[Dict[str, Any]]] = {}
            for record in existing_records:
                key = _renumber_row_key(record)
                existing_map.setdefault(key, []).append(record)
            for row in rows:
                key = _renumber_row_key(row)
                candidates = existing_map.get(key)
                if not candidates:
                    continue
                existing = candidates.pop(0)
                if _is_ok_status(existing.get("OUT")):
                    row["OUT"] = existing.get("OUT") or ""
                    row["UPDATED_AT"] = existing.get("UPDATED_AT") or ""
                if existing.get("IN"):
                    row["IN"] = existing.get("IN") or ""
                    row["TIMESTAMP_IN"] = existing.get("TIMESTAMP_IN") or ""
                if existing.get("PLPN"):
                    row["PLPN"] = existing.get("PLPN") or ""
                if existing.get("MWNO"):
                    row["MWNO"] = existing.get("MWNO") or ""
                if existing.get("MOS180_STATUS"):
                    row["MOS180_STATUS"] = existing.get("MOS180_STATUS") or ""
                if existing.get("MOS050_STATUS"):
                    row["MOS050_STATUS"] = existing.get("MOS050_STATUS") or ""
                if existing.get("CRS335_STATUS"):
                    row["CRS335_STATUS"] = existing.get("CRS335_STATUS") or ""
                if existing.get("STS046_STATUS"):
                    row["STS046_STATUS"] = existing.get("STS046_STATUS") or ""
                if existing.get("STS046_ADD_STATUS"):
                    row["STS046_ADD_STATUS"] = existing.get("STS046_ADD_STATUS") or ""
                if existing.get("MMS240_STATUS"):
                    row["MMS240_STATUS"] = existing.get("MMS240_STATUS") or ""
                if existing.get("CUSEXT_STATUS"):
                    row["CUSEXT_STATUS"] = existing.get("CUSEXT_STATUS") or ""
                if existing.get("NEW_PART_ITNO"):
                    row["NEW_PART_ITNO"] = existing.get("NEW_PART_ITNO") or ""
                if existing.get("NEW_PART_SER2"):
                    row["NEW_PART_SER2"] = existing.get("NEW_PART_SER2") or ""
        for index, row in enumerate(rows, start=1):
            row["SEQ"] = str(index)
    columns: List[str] = []
    if rows:
        seen = set()
        for key in rows[0].keys():
            if key not in seen:
                columns.append(key)
                seen.add(key)
        for row in rows[1:]:
            for key in row.keys():
                if key not in seen:
                    columns.append(key)
                    seen.add(key)
    else:
        columns = ["EMPTY"]
    if base == RENUMBER_WAGON_TABLE:
        for extra in RENUMBER_EXTRA_COLUMNS:
            if extra not in columns:
                columns.append(extra)
    with _connect() as conn:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        column_list = ", ".join(f'"{col}" TEXT' for col in columns)
        conn.execute(f'CREATE TABLE "{table_name}" ({column_list})')
        if rows:
            placeholders = ", ".join("?" for _ in columns)
            column_names = ", ".join(f'"{c}"' for c in columns)
            insert_sql = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
            data = [[row.get(col, "") for col in columns] for row in rows]
            conn.executemany(insert_sql, data)
        conn.commit()
    return table_name


@app.get("/api/meta/targets")
def meta_targets(env: str = Query(DEFAULT_ENV)) -> dict:
    normalized = _normalize_env(env)
    rsrd_tables = _rsrd_tables(env)
    sqlite_url = f"sqlite:///{DB_PATH.resolve().as_posix()}"
    return {
        "env": normalized,
        "urls": {
            "compass": _safe_ionapi_url(env, "compass"),
            "mi": _safe_ionapi_url(env, "mi"),
            "rsrd_wsdl": _resolve_rsrd_wsdl(env),
            "sqlite": sqlite_url,
        },
        "tables": {
            "wagons": _table_for(DEFAULT_TABLE, env),
            "wagenumbau_wagons": _table_for(WAGENUMBAU_TABLE, env),
            "renumber_wagon": _table_for(RENUMBER_WAGON_TABLE, env),
            "spareparts": _table_for(SPAREPARTS_TABLE, env),
            "sparepart_swaps": _table_for(SPAREPARTS_SWAP_TABLE, env),
            "rsrd_erp_numbers": _table_for(RSRD_ERP_TABLE, env),
            "rsrd_erp_full": _table_for(RSRD_ERP_FULL_TABLE, env),
            "rsrd_upload": _table_for(RSRD_UPLOAD_TABLE, env),
            "rsrd": {
                "wagons": rsrd_tables.wagons,
                "snapshots": rsrd_tables.snapshots,
                "json": rsrd_tables.json,
                "detail": rsrd_tables.detail,
            },
        },
    }


@app.get("/api/wagons/count")
def wagons_count(
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    table_name = _ensure_wagon_data(table, env)
    with _connect() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    return {"table": table_name, "total": total, "env": _normalize_env(env)}


@app.get("/api/wagons/chunk")
def wagons_chunk(
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    table_name = _ensure_wagon_data(table, env)
    with _connect() as conn:
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


@app.get("/api/wagons/exists")
def wagons_exists(
    sern: str = Query(..., min_length=1),
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    env_table = _table_for(table, env)
    template = None if table == DEFAULT_TABLE else _table_for(DEFAULT_TABLE, env)
    with _connect() as conn:
        table_name = _ensure_table(conn, env_table, template)
        row = conn.execute(
            f'SELECT 1 FROM "{table_name}" WHERE "SERIENNUMMER" = ? LIMIT 1',
            (sern,),
        ).fetchone()
    return {"table": table_name, "exists": row is not None, "env": _normalize_env(env)}


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


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
    table_name = _table_for(RSRD_ERP_TABLE, env)
    return [
        sys.executable,
        str(PROJECT_ROOT / "python" / "load_erp_wagons.py"),
        "--scheme",
        DEFAULT_SCHEME,
        "--sqlite-db",
        str(DB_PATH),
        "--table",
        table_name,
        "--ionapi",
        str(ionapi),
    ]


def _build_erp_full_cmd(env: str) -> List[str]:
    if not RSRD_ERP_SQL_FILE.exists():
        raise FileNotFoundError(f"SQL-Datei nicht gefunden: {RSRD_ERP_SQL_FILE}")
    ionapi = _ionapi_path(env, "compass")
    table_name = _table_for(RSRD_ERP_FULL_TABLE, env)
    return [
        sys.executable,
        str(PROJECT_ROOT / "python" / "compass_to_sqlite.py"),
        "--scheme",
        DEFAULT_SCHEME,
        "--sql-file",
        str(RSRD_ERP_SQL_FILE),
        "--table",
        table_name,
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
        numbers_table = _ensure_table(conn, _table_for(RSRD_ERP_TABLE, env), None)
        count_wagons = conn.execute(f"SELECT COUNT(*) FROM {numbers_table}").fetchone()[0]
    message = f"ERP-Wagennummern geladen: {count_wagons}."
    _append_job_log(job_id, message)
    return {"count_wagons": count_wagons}


def _finalize_load_erp_full(job_id: str, env: str) -> Dict[str, Any]:
    with _connect() as conn:
        full_table = _ensure_table(conn, _table_for(RSRD_ERP_FULL_TABLE, env), None)
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
    table: str = DEFAULT_TABLE,
    env: str = Query(DEFAULT_ENV),
) -> dict:
    if not SQL_FILE.exists():
        raise HTTPException(status_code=500, detail=f"SQL-Datei nicht gefunden: {SQL_FILE}")

    table = _validate_table(table)
    table_name = _table_for(table, env)
    result = _run_compass_to_sqlite(SQL_FILE, table_name, env)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Reload fehlgeschlagen: {result.stderr or result.stdout}",
        )

    if table == DEFAULT_TABLE:
        background_tasks.add_task(_reload_spareparts_table, env)
    return {"message": "Reload erfolgreich", "stdout": result.stdout, "env": _normalize_env(env)}


@app.get("/api/objstrk")
def objstrk(
    mtrl: str = Query(..., min_length=1),
    sern: str = Query(..., min_length=1),
    store_table: str | None = Query(None),
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
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Ungültige MOS256 Antwort: {exc}") from exc

    if store_table:
        rows = _extract_mi_rows(payload)
        try:
            if store_table == RENUMBER_WAGON_TABLE:
                _clear_api_log()
            if store_table == RENUMBER_WAGON_TABLE and not rows:
                _clear_table_rows(store_table, env)
            else:
                _store_mi_rows(store_table, env, rows, wagon_itno=mtrl, wagon_sern=sern)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Objektstruktur speichern fehlgeschlagen: {exc}") from exc

    return payload


@app.post("/api/renumber/update")
def renumber_update(payload: dict = Body(...), env: str = Query(DEFAULT_ENV)) -> dict:
    table_name = _table_for(RENUMBER_WAGON_TABLE, env)
    new_sern = (payload.get("new_sern") or "").strip()
    new_baureihe = (payload.get("new_baureihe") or "").strip()
    umbau_datum = (payload.get("umbau_datum") or "").strip()
    umbau_art = (payload.get("umbau_art") or "").strip()
    if not new_sern or not new_baureihe or not umbau_datum or not umbau_art:
        raise HTTPException(status_code=400, detail="Pflichtfelder fehlen.")
    timestamp = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    with _connect() as conn:
        if not _table_exists(conn, table_name):
            raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
        _ensure_renumber_schema(conn, table_name)
        conn.execute(
            f"""UPDATE "{table_name}" SET
            "NEW_SERN"=?,
            "NEW_BAUREIHE"=?,
            "UMBAU_DATUM"=?,
            "UMBAU_ART"=?,
            "NEW_PART_ITNO"=?,
            "NEW_PART_SER2"=?,
            "PLPN"=?,
            "MWNO"=?,
            "MOS180_STATUS"=?,
            "OUT"=?,
            "UPDATED_AT"=?,
            "IN"=?,
            "TIMESTAMP_IN"=?
            """,
            (new_sern, new_baureihe, umbau_datum, umbau_art, "", "", "", "", "", "", timestamp, "", ""),
        )
        rows = conn.execute(f'SELECT rowid AS seq, * FROM "{table_name}"').fetchall()
        for row in rows:
            new_itno, new_ser2 = _compute_part_updates(row, new_sern, new_baureihe)
            conn.execute(
                f'UPDATE "{table_name}" SET "NEW_PART_ITNO"=?, "NEW_PART_SER2"=? WHERE rowid=?',
                (new_itno, new_ser2, row["seq"]),
            )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    return {"table": table_name, "updated": total, "env": _normalize_env(env)}


@app.post("/api/renumber/run")
def renumber_run(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("renumber_run", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            total = len(rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], "Teile werden ausgebaut")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            ok_count = 0
            error_count = 0
            env_label = _normalize_env(env).upper()
            with _connect() as conn:
                for idx, row in enumerate(rows, start=1):
                    params = _build_mos125_params(row, mode="out")
                    wagon_ctx = _wagon_log_context(row)
                    request_url = _build_m3_request_url(base_url, "MOS125MI", "RemoveInstall", params)
                    if not params["TRDT"]:
                        out = "ERROR: UMBAU_DATUM fehlt"
                        ok = False
                        _append_api_log(
                            "ausbau",
                            params,
                            {"error": "UMBAU_DATUM fehlt"},
                            ok,
                            "UMBAU_DATUM fehlt",
                            env=env_label,
                            wagon=wagon_ctx,
                            dry_run=dry_run,
                            request_url=request_url,
                        )
                    elif dry_run:
                        out = "DRYRUN"
                        ok = True
                        _append_api_log(
                            "ausbau",
                            params,
                            {"dry_run": True},
                            ok,
                            None,
                            env=env_label,
                            wagon=wagon_ctx,
                            dry_run=dry_run,
                            request_url=request_url,
                        )
                    else:
                        try:
                            response = call_m3_mi_get(
                                base_url, token, "MOS125MI", "RemoveInstall", params
                            )
                            error_message = _mi_error_message(response)
                            if error_message:
                                out = f"ERROR: {error_message}"
                                ok = False
                            else:
                                out = "OK"
                                ok = True
                            _append_api_log(
                                "ausbau",
                                params,
                                response,
                                ok,
                                error_message,
                                env=env_label,
                                wagon=wagon_ctx,
                                dry_run=dry_run,
                                request_url=request_url,
                            )
                        except Exception as exc:  # noqa: BLE001
                            out = f"ERROR: {exc}"
                            ok = False
                            _append_api_log(
                                "ausbau",
                                params,
                                {"error": str(exc)},
                                ok,
                                str(exc),
                                env=env_label,
                                wagon=wagon_ctx,
                                dry_run=dry_run,
                                request_url=request_url,
                            )

                    conn.execute(
                        f'UPDATE "{table_name}" SET "OUT"=?, "UPDATED_AT"=? WHERE rowid=?',
                        (out, datetime.utcnow().isoformat(sep=" ", timespec="seconds"), row["seq"]),
                    )
                    conn.commit()

                    result = {
                        "seq": row["seq"],
                        "cfgr": params["CFGR"],
                        "itno": params["ITNR"],
                        "ser2": params["BANR"],
                        "out": out,
                        "ok": ok,
                    }
                    with _jobs_lock:
                        job_ref = _jobs.get(job["id"])
                        if job_ref is not None:
                            job_ref["processed"] = idx
                            job_ref.setdefault("results", []).append(result)
                    if ok:
                        ok_count += 1
                    else:
                        error_count += 1
                    status = "ERROR" if not ok else ("DRYRUN" if dry_run else "OK")
                    _append_job_log(
                        job["id"],
                        f"Teile werden ausgebaut: {idx}/{len(rows)} {status}",
                    )

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/mos170")
def renumber_mos170(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("mos170_addprop", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            target_rows = [row for row in rows if _needs_renumber(row)]
            total = len(target_rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"MOS170MI AddProp: {total} Positionen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            processed = 0
            pending_rows = list(target_rows)
            attempt = 1
            while pending_rows:
                if MOS170_RETRY_MAX and attempt > MOS170_RETRY_MAX:
                    _append_job_log(
                        job["id"],
                        f"MOS170MI AddProp: Abbruch nach {MOS170_RETRY_MAX} Versuchen, {len(pending_rows)} ohne PLPN.",
                    )
                    break
                _append_job_log(
                    job["id"],
                    f"MOS170MI AddProp: Versuch {attempt} für {len(pending_rows)} Positionen.",
                )
                next_pending = []
                with _connect() as conn:
                    for row in pending_rows:
                        params = _build_mos170_params(row)
                        request_url = _build_m3_request_url(base_url, "MOS170MI", "AddProp", params)
                        required_missing = not params.get("ITNO") or not params.get("BANO") or not params.get("STDT")
                        if required_missing:
                            ok = False
                            error_message = "Pflichtfelder fehlen"
                            response = {"error": error_message}
                        elif dry_run:
                            ok = True
                            error_message = None
                            response = {"dry_run": True}
                        else:
                            try:
                                response = call_m3_mi_get(base_url, token, "MOS170MI", "AddProp", params)
                                error_message = _mi_error_message(response)
                                ok = not bool(error_message)
                            except Exception as exc:  # noqa: BLE001
                                response = {"error": str(exc)}
                                error_message = str(exc)
                                ok = False

                        plpn = _extract_plpn(response) if ok else ""
                        log_response = {"plpn": plpn, "response": response}
                        conn.execute(
                            f'UPDATE "{table_name}" SET "PLPN"=? WHERE rowid=?',
                            (plpn, row["seq"]),
                        )
                        conn.commit()
                        _append_api_log(
                            "ih_addprop",
                            params,
                            log_response,
                            ok,
                            error_message,
                            env=env_label,
                            wagon=_wagon_log_context(row),
                            dry_run=dry_run,
                            request_url=request_url,
                            program="MOS170MI",
                            transaction="AddProp",
                        )
                        if not plpn:
                            _append_api_log(
                                "ih_addprop_missing_plpn",
                                params,
                                log_response,
                                False,
                                "PLPN fehlt",
                                env=env_label,
                                wagon=_wagon_log_context(row),
                                dry_run=dry_run,
                                request_url=request_url,
                                program="MOS170MI",
                                transaction="AddProp",
                            )
                            next_pending.append(row)

                        processed += 1
                        with _jobs_lock:
                            job_ref = _jobs.get(job["id"])
                            if job_ref is not None:
                                job_ref["processed"] = processed
                        if ok:
                            ok_count += 1
                        else:
                            error_count += 1

                if not next_pending:
                    break
                if dry_run:
                    _append_job_log(job["id"], "MOS170MI AddProp: Dry-Run aktiv, keine weiteren Versuche.")
                    break
                total = total + len(next_pending)
                _update_job(job["id"], total=total)
                _append_job_log(job["id"], f"Warte {MOS170_RETRY_DELAY_SEC} Sekunden auf ERP ...")
                time.sleep(MOS170_RETRY_DELAY_SEC)
                attempt += 1
                pending_rows = next_pending

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/cms100")
@app.post("/api/renumber/mos170/plpn")
def renumber_cms100(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("mos170_plpn", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)

            def _load_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
                return conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    WHERE IFNULL("PLPN", '') <> '' AND IFNULL("MWNO", '') = ''
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            with _connect() as conn:
                cms_rows = _load_rows(conn)
            total = len(cms_rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"MOS170 PLPN: {total} Positionen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            processed = 0
            attempt = 1
            while cms_rows:
                if CMS100_RETRY_MAX and attempt > CMS100_RETRY_MAX:
                    _append_job_log(
                        job["id"],
                        f"MOS170 PLPN: Abbruch nach {CMS100_RETRY_MAX} Versuchen, {len(cms_rows)} ohne MWNO.",
                    )
                    break
                _append_job_log(job["id"], f"MOS170 PLPN: Versuch {attempt} für {len(cms_rows)} Positionen.")
                with _connect() as conn:
                    for row in cms_rows:
                        plpn = _row_value(row, "PLPN")
                        params = _build_cms100_params(plpn)
                        request_url = _build_m3_request_url(base_url, "CMS100MI", "Lst_PLPN_MWNO", params)
                        if not plpn:
                            ok = False
                            error_message = "PLPN fehlt"
                            response = {"error": error_message}
                        elif dry_run:
                            ok = True
                            error_message = None
                            response = {"dry_run": True}
                        else:
                            try:
                                response = call_m3_mi_get(base_url, token, "CMS100MI", "Lst_PLPN_MWNO", params)
                                error_message = _mi_error_message(response)
                                ok = not bool(error_message)
                            except Exception as exc:  # noqa: BLE001
                                response = {"error": str(exc)}
                                error_message = str(exc)
                                ok = False

                        mwno = _extract_mwno(response) if ok else ""
                        log_response = {"qomwno": mwno, "response": response}
                        conn.execute(
                            f'UPDATE "{table_name}" SET "MWNO"=? WHERE rowid=?',
                            (mwno, row["seq"]),
                        )
                        conn.commit()
                        _append_api_log(
                            "mos170_plpn",
                            params,
                            log_response,
                            ok,
                            error_message,
                            env=env_label,
                            wagon=_wagon_log_context(row),
                            dry_run=dry_run,
                            request_url=request_url,
                            program="CMS100MI",
                            transaction="Lst_PLPN_MWNO",
                        )
                        if not mwno:
                            _append_api_log(
                                "mos170_plpn_missing_mwno",
                                params,
                                log_response,
                                False,
                                "QOMWNO fehlt",
                                env=env_label,
                                wagon=_wagon_log_context(row),
                                dry_run=dry_run,
                                request_url=request_url,
                                program="CMS100MI",
                                transaction="Lst_PLPN_MWNO",
                            )

                        processed += 1
                        with _jobs_lock:
                            job_ref = _jobs.get(job["id"])
                            if job_ref is not None:
                                job_ref["processed"] = processed
                        if ok:
                            ok_count += 1
                        else:
                            error_count += 1

                with _connect() as conn:
                    cms_rows = _load_rows(conn)
                if not cms_rows:
                    break
                if dry_run:
                    _append_job_log(job["id"], "MOS170 PLPN: Dry-Run aktiv, keine weiteren Versuche.")
                    break
                if CMS100_RETRY_MAX and attempt >= CMS100_RETRY_MAX:
                    _append_job_log(
                        job["id"],
                        f"MOS170 PLPN: keine MWNO nach {CMS100_RETRY_MAX} Versuchen.",
                    )
                    break
                total = total + len(cms_rows)
                _update_job(job["id"], total=total)
                _append_job_log(job["id"], f"Warte {CMS100_RETRY_DELAY_SEC} Sekunden auf ERP ...")
                time.sleep(CMS100_RETRY_DELAY_SEC)
                attempt += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/mos100")
def renumber_mos100(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("ips_mos100_chgsern", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    WHERE IFNULL("MWNO", '') <> ''
                      AND (IFNULL("NEW_PART_ITNO", '') <> '' OR IFNULL("NEW_PART_SER2", '') <> '')
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            total = len(rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"IPS MOS100 Chg_SERN: {total} Positionen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            processed = 0
            with _connect() as conn:
                for row in rows:
                    params = _build_ips_mos100_params(row)
                    request_url = _build_ips_request_url(base_url, "MOS100")
                    mwno = params.get("WorkOrderNumber") or ""
                    if not mwno:
                        ok = False
                        error_message = "MWNO fehlt"
                        response = {"error": error_message}
                    elif dry_run:
                        ok = True
                        error_message = None
                        response = {"dry_run": True}
                    else:
                        try:
                            response = _call_ips_service(base_url, token, "MOS100", "Chg_SERN", params)
                            ok = int(response.get("status_code") or 0) < 400
                            error_message = None if ok else f"HTTP {response.get('status_code')}"
                        except Exception as exc:  # noqa: BLE001
                            response = {"error": str(exc)}
                            error_message = str(exc)
                            ok = False

                    _append_api_log(
                        "ips_mos100_chgsern",
                        params,
                        response,
                        ok,
                        error_message,
                        env=env_label,
                        wagon=_wagon_log_context(row),
                        dry_run=dry_run,
                        request_url=request_url,
                        program="MOS100",
                        transaction="Chg_SERN",
                        request_method="POST",
                    )
                    processed += 1
                    with _jobs_lock:
                        job_ref = _jobs.get(job["id"])
                        if job_ref is not None:
                            job_ref["processed"] = processed
                    if ok:
                        ok_count += 1
                    else:
                        error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


# BEGIN WAGON RENNUMBERING
@app.post("/api/renumber/wagon")
def renumber_wagon(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("wagon_renumber", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                row = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC LIMIT 1"""
                ).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Keine Daten in der Umnummerierungs-Tabelle.")

            old_itno = _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO")
            old_sern = _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN")
            new_itno = _row_value(row, "NEW_BAUREIHE") or old_itno
            new_sern = _row_value(row, "NEW_SERN") or old_sern
            umbau_datum = _row_value(row, "UMBAU_DATUM")
            if not old_itno or not old_sern or not new_itno or not new_sern or not umbau_datum:
                raise HTTPException(status_code=400, detail="Pflichtfelder für Wagen fehlen.")

            whlo = ""
            wagon_table = _table_for(WAGENUMBAU_TABLE, env)
            with _connect() as conn:
                if _table_exists(conn, wagon_table):
                    columns = {
                        row[1]
                        for row in conn.execute(f'PRAGMA table_info("{wagon_table}")').fetchall()
                        if row and len(row) > 1
                    }
                    if "LAGERORT" in columns:
                        result = conn.execute(
                            f'SELECT "LAGERORT" FROM "{wagon_table}" WHERE "BAUREIHE"=? AND "SERIENNUMMER"=? LIMIT 1',
                            (old_itno, old_sern),
                        ).fetchone()
                        if result and result[0]:
                            whlo = str(result[0])
            if not whlo:
                raise HTTPException(status_code=400, detail="LAGERORT fehlt für den Wagen.")

            _update_job(job["id"], total=5, processed=0, results=[])
            _append_job_log(job["id"], "Starte Wagen-Umnummerierung")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            processed = 0

            # MOS170 AddProp / PLPN
            plpn = ""
            attempt = 1
            while True:
                if MOS170_RETRY_MAX and attempt > MOS170_RETRY_MAX:
                    break
                params = _build_mos170_wagon_params(old_itno, old_sern, umbau_datum, whlo)
                request_url = _build_m3_request_url(base_url, "MOS170MI", "AddProp", params)
                action = "wagon_mos170_addprop" if attempt == 1 else "wagon_mos170_plpn"
                if not params.get("ITNO") or not params.get("BANO") or not params.get("STDT"):
                    ok = False
                    error_message = "Pflichtfelder fehlen"
                    response = {"error": error_message}
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    plpn = "DRYRUN"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "MOS170MI", "AddProp", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        plpn = _extract_plpn(response) if ok else ""
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False

                log_response = {"plpn": plpn, "response": response}
                _append_api_log(
                    action,
                    params,
                    log_response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={"itno": old_itno, "sern": old_sern, "new_itno": new_itno, "new_sern": new_sern},
                    dry_run=dry_run,
                    request_url=request_url,
                    program="MOS170MI",
                    transaction="AddProp",
                )
                processed += 1
                _update_job(job["id"], processed=processed)
                if plpn:
                    break
                if dry_run:
                    break
                if MOS170_RETRY_DELAY_SEC:
                    time.sleep(MOS170_RETRY_DELAY_SEC)
                attempt += 1

            if not plpn:
                raise HTTPException(status_code=500, detail="PLPN fehlt nach MOS170.")

            # CMS100 MWNO
            mwno = ""
            attempt = 1
            while True:
                if CMS100_RETRY_MAX and attempt > CMS100_RETRY_MAX:
                    break
                params = _build_cms100_params(plpn)
                request_url = _build_m3_request_url(base_url, "CMS100MI", "Lst_PLPN_MWNO", params)
                if dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    mwno = "DRYRUN"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "CMS100MI", "Lst_PLPN_MWNO", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        mwno = _extract_mwno(response) if ok else ""
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False

                log_response = {"qomwno": mwno, "response": response}
                _append_api_log(
                    "wagon_cms100_lst_plpn_mwno",
                    params,
                    log_response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={"itno": old_itno, "sern": old_sern, "new_itno": new_itno, "new_sern": new_sern},
                    dry_run=dry_run,
                    request_url=request_url,
                    program="CMS100MI",
                    transaction="Lst_PLPN_MWNO",
                )
                processed += 1
                _update_job(job["id"], processed=processed)
                if mwno:
                    break
                if dry_run:
                    break
                if WAGON_CMS100_RETRY_DELAY_SEC:
                    time.sleep(WAGON_CMS100_RETRY_DELAY_SEC)
                attempt += 1

            if not mwno:
                raise HTTPException(status_code=500, detail="MWNO fehlt nach CMS100.")

            # IPS MOS100 Chg_SERN
            params = {
                "WorkOrderNumber": mwno,
                "Product": old_itno,
                "NewItemNumber": new_itno,
                "NewLotNumber": new_sern,
            }
            request_url = _build_ips_request_url(base_url, "MOS100")
            if dry_run:
                ok = True
                error_message = None
                response = {"dry_run": True}
            else:
                try:
                    response = _call_ips_service(base_url, token, "MOS100", "Chg_SERN", params)
                    ok = int(response.get("status_code") or 0) < 400
                    error_message = None if ok else f"HTTP {response.get('status_code')}"
                except Exception as exc:  # noqa: BLE001
                    response = {"error": str(exc)}
                    error_message = str(exc)
                    ok = False

            _append_api_log(
                "wagon_ips_mos100_chgsern",
                params,
                response,
                ok,
                error_message,
                env=env_label,
                wagon={"itno": old_itno, "sern": old_sern, "new_itno": new_itno, "new_sern": new_sern},
                dry_run=dry_run,
                request_url=request_url,
                program="MOS100",
                transaction="Chg_SERN",
                request_method="POST",
            )
            processed += 1
            _update_job(job["id"], processed=processed)
            if not ok:
                raise HTTPException(status_code=500, detail="MOS100 Chg_SERN fehlgeschlagen.")

            # MOS180 Approve
            params = {
                "FACI": MOS180_FACI,
                "MWNO": mwno,
                "RESP": MOS180_RESP,
                "APRB": MOS180_APRB,
            }
            request_url = _build_m3_request_url(base_url, "MOS180MI", "Approve", params)
            if dry_run:
                ok = True
                error_message = None
                response = {"dry_run": True}
            else:
                try:
                    response = call_m3_mi_get(base_url, token, "MOS180MI", "Approve", params)
                    error_message = _mi_error_message(response)
                    ok = not bool(error_message)
                except Exception as exc:  # noqa: BLE001
                    response = {"error": str(exc)}
                    error_message = str(exc)
                    ok = False

            _append_api_log(
                "wagon_mos180_approve",
                params,
                response,
                ok,
                error_message,
                env=env_label,
                wagon={"itno": old_itno, "sern": old_sern, "new_itno": new_itno, "new_sern": new_sern},
                dry_run=dry_run,
                request_url=request_url,
                program="MOS180MI",
                transaction="Approve",
            )
            processed += 1
            _update_job(job["id"], processed=processed)
            if not ok:
                raise HTTPException(status_code=500, detail="MOS180 Approve fehlgeschlagen.")

            # CRS335 UpdCtrlObj
            acrf_value = ""
            with _connect() as conn:
                if _table_exists(conn, wagon_table):
                    columns = {
                        row[1]
                        for row in conn.execute(f'PRAGMA table_info("{wagon_table}")').fetchall()
                        if row and len(row) > 1
                    }
                    if "ACRF" in columns:
                        result = conn.execute(
                            f'SELECT "ACRF" FROM "{wagon_table}" WHERE "BAUREIHE"=? AND "SERIENNUMMER"=? LIMIT 1',
                            (old_itno, old_sern),
                        ).fetchone()
                        if result and result[0]:
                            acrf_value = str(result[0])
            acrf_value = acrf_value or CRS335_ACRF
            params = _build_crs335_params(acrf_value, new_sern, new_itno)
            request_url = _build_m3_request_url(base_url, "CRS335MI", "UpdCtrlObj", params)
            if not params.get("ACRF"):
                ok = False
                error_message = "ACRF fehlt"
                response = {"error": error_message}
            elif dry_run:
                ok = True
                error_message = None
                response = {"dry_run": True}
            else:
                try:
                    response = call_m3_mi_get(base_url, token, "CRS335MI", "UpdCtrlObj", params)
                    error_message = _mi_error_message(response)
                    ok = not bool(error_message)
                except Exception as exc:  # noqa: BLE001
                    response = {"error": str(exc)}
                    error_message = str(exc)
                    ok = False

            _append_api_log(
                "wagon_crs335_updctrlobj",
                params,
                response,
                ok,
                error_message,
                env=env_label,
                wagon={"itno": old_itno, "sern": old_sern, "new_itno": new_itno, "new_sern": new_sern},
                dry_run=dry_run,
                request_url=request_url,
                program="CRS335MI",
                transaction="UpdCtrlObj",
            )
            processed += 1
            _update_job(job["id"], processed=processed)
            if not ok:
                raise HTTPException(status_code=500, detail="CRS335 fehlgeschlagen.")

            _finish_job(
                job["id"],
                "success",
                result={"total": processed, "ok": processed, "error": 0},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}
# END WAGON RENNUMBERING

@app.post("/api/renumber/install")
def renumber_install(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("renumber_install", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END DESC"""
                ).fetchall()

            total = len(rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"Starte MOS125MI Einbau: {total} Positionen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            ok_count = 0
            error_count = 0
            env_label = _normalize_env(env).upper()
            with _connect() as conn:
                for idx, row in enumerate(rows, start=1):
                    params = _build_mos125_params(row, mode="in")
                    wagon_ctx = _wagon_log_context(row)
                    request_url = _build_m3_request_url(base_url, "MOS125MI", "RemoveInstall", params)
                    if not params["TRDT"]:
                        status = "ERROR: UMBAU_DATUM fehlt"
                        ok = False
                        _append_api_log(
                            "einbau",
                            params,
                            {"error": "UMBAU_DATUM fehlt"},
                            ok,
                            "UMBAU_DATUM fehlt",
                            env=env_label,
                            wagon=wagon_ctx,
                            dry_run=dry_run,
                            request_url=request_url,
                        )
                    elif dry_run:
                        status = "DRYRUN"
                        ok = True
                        _append_api_log(
                            "einbau",
                            params,
                            {"dry_run": True},
                            ok,
                            None,
                            env=env_label,
                            wagon=wagon_ctx,
                            dry_run=dry_run,
                            request_url=request_url,
                        )
                    else:
                        try:
                            response = call_m3_mi_get(
                                base_url, token, "MOS125MI", "RemoveInstall", params
                            )
                            error_message = _mi_error_message(response)
                            if error_message:
                                status = f"ERROR: {error_message}"
                                ok = False
                            else:
                                status = "OK"
                                ok = True
                            _append_api_log(
                                "einbau",
                                params,
                                response,
                                ok,
                                error_message,
                                env=env_label,
                                wagon=wagon_ctx,
                                dry_run=dry_run,
                                request_url=request_url,
                            )
                        except Exception as exc:  # noqa: BLE001
                            status = f"ERROR: {exc}"
                            ok = False
                            _append_api_log(
                                "einbau",
                                params,
                                {"error": str(exc)},
                                ok,
                                str(exc),
                                env=env_label,
                                wagon=wagon_ctx,
                                dry_run=dry_run,
                                request_url=request_url,
                            )

                    conn.execute(
                        f'UPDATE "{table_name}" SET "IN"=?, "TIMESTAMP_IN"=? WHERE rowid=?',
                        (status, datetime.utcnow().isoformat(sep=" ", timespec="seconds"), row["seq"]),
                    )
                    conn.commit()

                    result = {
                        "seq": row["seq"],
                        "cfgr": params.get("CFGL") or params.get("CFGR") or "",
                        "itno": params.get("ITNI") or params.get("ITNR") or _row_value(row, "ITNO"),
                        "ser2": _row_value(row, "SER2"),
                        "in": status,
                        "ok": ok,
                    }
                    with _jobs_lock:
                        job_ref = _jobs.get(job["id"])
                        if job_ref is not None:
                            job_ref["processed"] = idx
                            job_ref.setdefault("results", []).append(result)
                    if ok:
                        ok_count += 1
                    else:
                        error_count += 1
                    status_label = "ERROR" if not ok else ("DRYRUN" if dry_run else "OK")
                    _append_job_log(
                        job["id"],
                        f"{idx}/{total} {status_label} CFGL={params.get('CFGL', '')} ITNI={params.get('ITNI', '')} BANI={params.get('BANI', '')}",
                    )

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/mos180")
def renumber_mos180(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("mos180_approve", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    WHERE IFNULL("MWNO", '') <> ''
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            mwno_map: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                mwno = _row_value(row, "MWNO")
                if not mwno:
                    continue
                entry = mwno_map.get(mwno)
                if entry is None:
                    mwno_map[mwno] = {"rowids": [row["seq"]], "row": row}
                else:
                    entry["rowids"].append(row["seq"])

            total = len(mwno_map)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"MOS180MI Approve: {total} MWNO.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (mwno, entry) in enumerate(mwno_map.items(), start=1):
                row = entry["row"]
                params = _build_mos180_params(row)
                request_url = _build_m3_request_url(base_url, "MOS180MI", "Approve", params)
                mwno = params.get("MWNO") or mwno
                if not mwno:
                    ok = False
                    error_message = "MWNO fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "MOS180MI", "Approve", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "mos180_approve",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon=_wagon_log_context(row),
                    dry_run=dry_run,
                    request_url=request_url,
                    program="MOS180MI",
                    transaction="Approve",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": _row_value(row, "NEW_BAUREIHE")
                        or _row_value(row, "WAGEN_ITNO")
                        or _row_value(row, "ITNO"),
                        "sern": _row_value(row, "NEW_SERN")
                        or _row_value(row, "WAGEN_SERN")
                        or _row_value(row, "SERN"),
                        "mwno": mwno,
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    for rowid in entry["rowids"]:
                        conn.execute(
                            f'UPDATE "{table_name}" SET "MOS180_STATUS"=? WHERE rowid=?',
                            (status_label, rowid),
                        )
                    conn.commit()
                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/mos050")
def renumber_mos050(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("mos050_montage", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    WHERE IFNULL("MWNO", '') <> ''
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            target_rows = [row for row in rows if _needs_renumber(row)]
            total = len(target_rows)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"MOS050 Montage: {total} Positionen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, row in enumerate(target_rows, start=1):
                params = _build_mos050_params(row)
                request_url = _build_ips_request_url(base_url, MOS050_SERVICE)
                mwno = params.get("WHMWNO") or params.get("WorkOrderNumber") or ""
                if not mwno:
                    ok = False
                    error_message = "MWNO fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = _call_ips_service(
                            base_url,
                            token,
                            MOS050_SERVICE,
                            MOS050_OPERATION,
                            params,
                            namespace_override=MOS050_NAMESPACE or None,
                            body_tag_override=MOS050_BODY_TAG or None,
                        )
                        ok = int(response.get("status_code") or 0) < 400
                        error_message = None if ok else f"HTTP {response.get('status_code')}"
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "ips_mos050_montage",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon=_wagon_log_context(row),
                    dry_run=dry_run,
                    request_url=request_url,
                    program=MOS050_SERVICE or "MOS050",
                    transaction=MOS050_OPERATION or "Montage",
                    request_method="POST",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": _row_value(row, "NEW_BAUREIHE")
                        or _row_value(row, "WAGEN_ITNO")
                        or _row_value(row, "ITNO"),
                        "sern": _row_value(row, "NEW_SERN")
                        or _row_value(row, "WAGEN_SERN")
                        or _row_value(row, "SERN"),
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    conn.execute(
                        f'UPDATE "{table_name}" SET "MOS050_STATUS"=? WHERE rowid=?',
                        (status_label, row["seq"]),
                    )
                    conn.commit()
                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/mms240")
def renumber_mms240(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("mms240_upd", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            if not rows:
                _finish_job(job["id"], "success", result={"total": 0, "ok": 0, "error": 0})
                return

            wagons: Dict[tuple[str, str], Dict[str, Any]] = {}
            for row in rows:
                wagon_itno = _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO")
                wagon_sern = _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN")
                wagon_key = (wagon_itno, wagon_sern)
                entry = wagons.get(wagon_key)
                if not entry:
                    wagons[wagon_key] = {
                        "new_itno": _row_value(row, "NEW_BAUREIHE") or wagon_itno,
                        "new_sern": _row_value(row, "NEW_SERN") or wagon_sern,
                        "rowids": [row["seq"]],
                    }
                else:
                    entry["rowids"].append(row["seq"])

            total = len(wagons)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"MMS240MI Upd: {total} Wagen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (wagon_key, entry) in enumerate(wagons.items(), start=1):
                new_itno = entry["new_itno"]
                new_sern = entry["new_sern"]
                params = _build_mms240_params(new_itno, new_sern)
                request_url = _build_m3_request_url(base_url, "MMS240MI", "Upd", params)

                if not new_itno or not new_sern:
                    ok = False
                    error_message = "ITNO/SERN fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "MMS240MI", "Upd", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "mms240_upd",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={
                        "itno": wagon_key[0],
                        "sern": wagon_key[1],
                        "new_itno": new_itno,
                        "new_sern": new_sern,
                    },
                    dry_run=dry_run,
                    request_url=request_url,
                    program="MMS240MI",
                    transaction="Upd",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": new_itno,
                        "sern": new_sern,
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    for rowid in entry["rowids"]:
                        conn.execute(
                            f'UPDATE "{table_name}" SET "MMS240_STATUS"=? WHERE rowid=?',
                            (status_label, rowid),
                        )
                    conn.commit()

                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/cusext")
def renumber_cusext(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("cusext_addfieldvalue", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            if not rows:
                _finish_job(job["id"], "success", result={"total": 0, "ok": 0, "error": 0})
                return

            wagons: Dict[tuple[str, str], Dict[str, Any]] = {}
            for row in rows:
                wagon_itno = _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO")
                wagon_sern = _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN")
                wagon_key = (wagon_itno, wagon_sern)
                entry = wagons.get(wagon_key)
                if not entry:
                    wagons[wagon_key] = {
                        "new_itno": _row_value(row, "NEW_BAUREIHE") or wagon_itno,
                        "new_sern": _row_value(row, "NEW_SERN") or wagon_sern,
                        "rowids": [row["seq"]],
                    }
                else:
                    entry["rowids"].append(row["seq"])

            total = len(wagons)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"CUSEXTMI AddFieldValue: {total} Wagen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (wagon_key, entry) in enumerate(wagons.items(), start=1):
                new_itno = entry["new_itno"]
                new_sern = entry["new_sern"]
                params = _build_cusext_params(new_itno, new_sern)
                request_url = _build_m3_request_url(base_url, "CUSEXTMI", "AddFieldValue", params)

                if not new_itno or not new_sern:
                    ok = False
                    error_message = "ITNO/SERN fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "CUSEXTMI", "AddFieldValue", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "cusext_addfieldvalue",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={
                        "itno": wagon_key[0],
                        "sern": wagon_key[1],
                        "new_itno": new_itno,
                        "new_sern": new_sern,
                    },
                    dry_run=dry_run,
                    request_url=request_url,
                    program="CUSEXTMI",
                    transaction="AddFieldValue",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": new_itno,
                        "sern": new_sern,
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    for rowid in entry["rowids"]:
                        conn.execute(
                            f'UPDATE "{table_name}" SET "CUSEXT_STATUS"=? WHERE rowid=?',
                            (status_label, rowid),
                        )
                    conn.commit()

                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/crs335")
def renumber_crs335(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("crs335_updctrlobj", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            if not rows:
                _finish_job(job["id"], "success", result={"total": 0, "ok": 0, "error": 0})
                return

            wagons: Dict[tuple[str, str], Dict[str, str]] = {}
            for row in rows:
                wagon_key = (
                    _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO"),
                    _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN"),
                )
                if wagon_key in wagons:
                    continue
                wagons[wagon_key] = {
                    "new_sern": _row_value(row, "NEW_SERN") or _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN"),
                    "new_baureihe": _row_value(row, "NEW_BAUREIHE") or _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO"),
                }
            acrf_by_wagon: Dict[tuple[str, str], str] = {}
            wagon_table = _table_for(WAGENUMBAU_TABLE, env)
            with _connect() as conn:
                if _table_exists(conn, wagon_table):
                    columns = {
                        row[1]
                        for row in conn.execute(f'PRAGMA table_info("{wagon_table}")').fetchall()
                        if row and len(row) > 1
                    }
                    if "ACRF" in columns:
                        for wagon_key in wagons.keys():
                            row = conn.execute(
                                f'SELECT "ACRF" FROM "{wagon_table}" WHERE "BAUREIHE"=? AND "SERIENNUMMER"=? LIMIT 1',
                                (wagon_key[0], wagon_key[1]),
                            ).fetchone()
                            if row and row[0]:
                                acrf_by_wagon[wagon_key] = str(row[0])

            total = len(wagons)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"CRS335MI UpdCtrlObj: {total} Wagen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (wagon_key, values) in enumerate(wagons.items(), start=1):
                acrf_value = acrf_by_wagon.get(wagon_key) or CRS335_ACRF
                params = _build_crs335_params(acrf_value, values["new_sern"], values["new_baureihe"])
                request_url = _build_m3_request_url(base_url, "CRS335MI", "UpdCtrlObj", params)
                if not params.get("ACRF"):
                    ok = False
                    error_message = "ACRF fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "CRS335MI", "UpdCtrlObj", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "crs335_updctrlobj",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={"itno": wagon_key[0], "sern": wagon_key[1]},
                    dry_run=dry_run,
                    request_url=request_url,
                    program="CRS335MI",
                    transaction="UpdCtrlObj",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": values["new_baureihe"],
                        "sern": values["new_sern"],
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    conn.execute(
                        f'UPDATE "{table_name}" SET "CRS335_STATUS"=? WHERE "WAGEN_ITNO"=? AND "WAGEN_SERN"=?',
                        (status_label, wagon_key[0], wagon_key[1]),
                    )
                    conn.commit()

                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/sts046")
def renumber_sts046(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("sts046_delgenitem", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            wagon_table = _table_for(WAGENUMBAU_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            if not rows:
                _finish_job(job["id"], "success", result={"total": 0, "ok": 0, "error": 0})
                return

            wagon_meta: Dict[tuple[str, str], Dict[str, str]] = {}
            with _connect() as conn:
                if _table_exists(conn, wagon_table):
                    columns = {
                        row[1]
                        for row in conn.execute(f'PRAGMA table_info("{wagon_table}")').fetchall()
                        if row and len(row) > 1
                    }
                    if {"LAGERORT", "ACMC"} <= columns:
                        wagon_rows = conn.execute(
                            f'SELECT "BAUREIHE","SERIENNUMMER","LAGERORT","ACMC" FROM "{wagon_table}"'
                        ).fetchall()
                        for row in wagon_rows:
                            key = (str(row[0] or ""), str(row[1] or ""))
                            wagon_meta[key] = {
                                "WHLO": str(row[2] or ""),
                                "GEIT": str(row[3] or ""),
                            }

            wagons: Dict[tuple[str, str], Dict[str, Any]] = {}
            for row in rows:
                wagon_itno = _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO")
                wagon_sern = _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN")
                wagon_key = (wagon_itno, wagon_sern)
                entry = wagons.get(wagon_key)
                if not entry:
                    wagons[wagon_key] = {
                        "itno": wagon_itno,
                        "bano": wagon_sern,
                        "rowids": [row["seq"]],
                    }
                else:
                    entry["rowids"].append(row["seq"])

            total = len(wagons)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"STS046MI DelGenItem: {total} Wagen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (wagon_key, entry) in enumerate(wagons.items(), start=1):
                meta = wagon_meta.get(wagon_key) or {}
                whlo = meta.get("WHLO", "")
                geit = meta.get("GEIT", "")
                itno = entry["itno"]
                bano = entry["bano"]
                params = _build_sts046_params(whlo, geit, itno, bano)
                request_url = _build_m3_request_url(base_url, "STS046MI", "DelGenItem", params)

                if not whlo or not geit or not itno:
                    ok = False
                    error_message = "WHLO/GEIT/ITNO fehlt"
                    response = {"error": error_message}
                    status_label = "NOK"
                elif dry_run:
                    ok = True
                    error_message = None
                    response = {"dry_run": True}
                    status_label = "OK"
                else:
                    try:
                        response = call_m3_mi_get(base_url, token, "STS046MI", "DelGenItem", params)
                        error_message = _mi_error_message(response)
                        ok = not bool(error_message)
                        status_label = "OK" if ok else "NOK"
                    except Exception as exc:  # noqa: BLE001
                        response = {"error": str(exc)}
                        error_message = str(exc)
                        ok = False
                        status_label = "NOK"

                _append_api_log(
                    "sts046_delgenitem",
                    params,
                    response,
                    ok,
                    error_message,
                    env=env_label,
                    wagon={"itno": wagon_key[0], "sern": wagon_key[1]},
                    dry_run=dry_run,
                    request_url=request_url,
                    program="STS046MI",
                    transaction="DelGenItem",
                    status=status_label,
                )
                _append_job_result(
                    job["id"],
                    {
                        "itno": wagon_key[0],
                        "sern": wagon_key[1],
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    for rowid in entry["rowids"]:
                        conn.execute(
                            f'UPDATE "{table_name}" SET "STS046_STATUS"=? WHERE rowid=?',
                            (status_label, rowid),
                        )
                    conn.commit()

                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.post("/api/renumber/sts046/add")
def renumber_sts046_add(env: str = Query(DEFAULT_ENV)) -> dict:
    job = _create_job("sts046_addgenitem", env)

    def _worker() -> None:
        try:
            table_name = _table_for(RENUMBER_WAGON_TABLE, env)
            wagon_table = _table_for(WAGENUMBAU_TABLE, env)
            with _connect() as conn:
                if not _table_exists(conn, table_name):
                    raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
                _ensure_renumber_schema(conn, table_name)
                rows = conn.execute(
                    f"""SELECT rowid AS seq, * FROM "{table_name}"
                    ORDER BY CASE
                      WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
                      ELSE CAST("SEQ" AS INTEGER)
                    END ASC"""
                ).fetchall()

            if not rows:
                _finish_job(job["id"], "success", result={"total": 0, "ok": 0, "error": 0})
                return

            wagon_meta: Dict[tuple[str, str], Dict[str, str]] = {}
            acmc_by_baureihe: Dict[str, str] = {}
            with _connect() as conn:
                if _table_exists(conn, wagon_table):
                    columns = {
                        row[1]
                        for row in conn.execute(f'PRAGMA table_info("{wagon_table}")').fetchall()
                        if row and len(row) > 1
                    }
                    if {"LAGERORT", "ACMC"} <= columns:
                        wagon_rows = conn.execute(
                            f'SELECT "BAUREIHE","SERIENNUMMER","LAGERORT","ACMC" FROM "{wagon_table}"'
                        ).fetchall()
                        for row in wagon_rows:
                            baureihe = str(row[0] or "")
                            sern = str(row[1] or "")
                            whlo = str(row[2] or "")
                            acmc = str(row[3] or "")
                            wagon_meta[(baureihe, sern)] = {"WHLO": whlo, "GEIT": acmc}
                            if baureihe and acmc and baureihe not in acmc_by_baureihe:
                                acmc_by_baureihe[baureihe] = acmc

            wagons: Dict[tuple[str, str], Dict[str, Any]] = {}
            for row in rows:
                wagon_itno = _row_value(row, "WAGEN_ITNO") or _row_value(row, "ITNO")
                wagon_sern = _row_value(row, "WAGEN_SERN") or _row_value(row, "SERN")
                wagon_key = (wagon_itno, wagon_sern)
                entry = wagons.get(wagon_key)
                if not entry:
                    wagons[wagon_key] = {
                        "new_itno": _row_value(row, "NEW_BAUREIHE") or wagon_itno,
                        "new_sern": _row_value(row, "NEW_SERN") or wagon_sern,
                        "rowids": [row["seq"]],
                    }
                else:
                    entry["rowids"].append(row["seq"])

            total = len(wagons)
            _update_job(job["id"], total=total, processed=0, results=[])
            _append_job_log(job["id"], f"STS046MI AddGenItem: {total} Wagen.")

            dry_run = _effective_dry_run(env)
            ionapi_path = _ionapi_path(env, "mi")
            ion_cfg = load_ionapi_config(str(ionapi_path))
            base_url = build_base_url(ion_cfg)
            token = ""
            if not dry_run:
                token = get_access_token_service_account(ion_cfg)

            env_label = _normalize_env(env).upper()
            ok_count = 0
            error_count = 0
            for idx, (wagon_key, entry) in enumerate(wagons.items(), start=1):
                meta = wagon_meta.get(wagon_key) or {}
                whlo = meta.get("WHLO", "")
                new_itno = entry["new_itno"]
                new_sern = entry["new_sern"]
                old_geit = meta.get("GEIT", "")
                new_geit = acmc_by_baureihe.get(new_itno, "")
                geits = []
                if new_geit:
                    geits.append(new_geit)
                if old_geit and old_geit != new_geit:
                    geits.append(old_geit)
                if not geits:
                    geits = [""]

                all_ok = True
                status_label = "OK"
                for geit in geits:
                    params = _build_sts046_params(whlo, geit, new_itno, new_sern)
                    request_url = _build_m3_request_url(base_url, "STS046MI", "AddGenItem", params)

                    if not whlo or not geit or not new_itno:
                        ok = False
                        error_message = "WHLO/GEIT/ITNO fehlt"
                        response = {"error": error_message}
                        status_label = "NOK"
                    elif dry_run:
                        ok = True
                        error_message = None
                        response = {"dry_run": True}
                        status_label = "OK"
                    else:
                        try:
                            response = call_m3_mi_get(base_url, token, "STS046MI", "AddGenItem", params)
                            error_message = _mi_error_message(response)
                            ok = not bool(error_message)
                            status_label = "OK" if ok else "NOK"
                        except Exception as exc:  # noqa: BLE001
                            response = {"error": str(exc)}
                            error_message = str(exc)
                            ok = False
                            status_label = "NOK"

                    _append_api_log(
                        "sts046_addgenitem",
                        params,
                        response,
                        ok,
                        error_message,
                        env=env_label,
                        wagon={"itno": wagon_key[0], "sern": wagon_key[1]},
                        dry_run=dry_run,
                        request_url=request_url,
                        program="STS046MI",
                        transaction="AddGenItem",
                        status=status_label,
                    )
                    if not ok:
                        all_ok = False

                status_label = "OK" if all_ok else "NOK"
                _append_job_result(
                    job["id"],
                    {
                        "itno": new_itno,
                        "sern": new_sern,
                        "status": status_label,
                    },
                )
                with _connect() as conn:
                    for rowid in entry["rowids"]:
                        conn.execute(
                            f'UPDATE "{table_name}" SET "STS046_ADD_STATUS"=? WHERE rowid=?',
                            (status_label, rowid),
                        )
                    conn.commit()

                with _jobs_lock:
                    job_ref = _jobs.get(job["id"])
                    if job_ref is not None:
                        job_ref["processed"] = idx
                if ok:
                    ok_count += 1
                else:
                    error_count += 1

            _finish_job(
                job["id"],
                "success",
                result={"total": total, "ok": ok_count, "error": error_count},
            )
        except Exception as exc:  # noqa: BLE001
            _append_job_log(job["id"], f"Fehler: {exc}")
            _finish_job(job["id"], "error", error=str(exc))

    threading.Thread(target=_worker, daemon=True).start()
    return {"job_id": job["id"], "status": job["status"], "env": job["env"]}


@app.get("/api/renumber/debug")
def renumber_debug(env: str = Query(DEFAULT_ENV)) -> dict:
    table_name = _table_for(RENUMBER_WAGON_TABLE, env)
    with _connect() as conn:
        if not _table_exists(conn, table_name):
            raise HTTPException(status_code=404, detail=f"Tabelle {table_name} nicht gefunden.")
        _ensure_columns(conn, table_name, RENUMBER_EXTRA_COLUMNS)
    rows = conn.execute(
        f"""SELECT rowid AS seq, * FROM "{table_name}"
        ORDER BY CASE
          WHEN "SEQ" IS NULL OR "SEQ" = '' THEN rowid
          ELSE CAST("SEQ" AS INTEGER)
        END"""
    ).fetchall()

    calls: List[Dict[str, Any]] = []
    for row in rows:
        cfgr = _row_value(row, "CFGL", "MFGL")
        level = _hierarchy_level(cfgr)
        trtm = str(10000 + (level * 1000))
        umbau_datum = _row_value(row, "UMBAU_DATUM")
        call = {
            "SEQ": row["seq"],
            "RITP": "UMA",
            "RESP": "CHRUPP",
            "TRDT": _format_yyyymmdd(umbau_datum),
            "WHLO": "ZUM",
            "TWSL": "AUSBAU",
            "RSC4": "UMB",
            "TRTM": trtm,
            "CFGR": cfgr,
            "NHAR": _row_value(row, "MTRL"),
            "NHSR": _row_value(row, "SERN"),
            "ITNR": _row_value(row, "ITNO"),
            "BANR": _row_value(row, "SER2"),
        }
        calls.append(call)
    return {"table": table_name, "calls": calls, "env": _normalize_env(env)}


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
def rsrd2_wagons(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    with _connect() as conn:
        tables = _ensure_rsrd_tables(conn, env)
        rows = [
            {
                "wagon_id": row["wagon_id"],
                "updated_at": row["updated_at"],
                "data": json.loads(row["data_json"]),
            }
            for row in conn.execute(
                f"""
                SELECT wagon_id, data_json, updated_at
                FROM {tables.wagons}
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
        ]
        total = conn.execute(f"SELECT COUNT(*) FROM {tables.wagons}").fetchone()[0]
    return {"rows": rows, "limit": limit, "offset": offset, "total": total, "env": _normalize_env(env)}


@app.post("/api/rsrd2/compare")
def rsrd2_compare(
    limit: int | None = Query(None, gt=0),
    offset: int = Query(0, ge=0),
    create_upload: bool = Query(True),
    include_all: bool = Query(False),
    env: str = Query(DEFAULT_ENV),
    payload: dict | None = Body(default=None),
) -> dict:
    wagons = payload.get("wagons") if payload else None
    if wagons is not None:
        if not isinstance(wagons, list) or not all(isinstance(item, (str, int)) for item in wagons):
            raise HTTPException(status_code=400, detail="Feld 'wagons' muss eine Liste von Wagennummern sein.")
        wagons = [str(item) for item in wagons]

    with _connect() as conn:
        tables = _ensure_rsrd_tables(conn, env)
        upload_table = _ensure_rsrd_upload_table(conn, env)
        erp_full_table = _table_for(RSRD_ERP_FULL_TABLE, env)

        where_clause = ""
        params: List[Any] = []
        if wagons:
            placeholders = ", ".join("?" for _ in wagons)
            where_clause = f"WHERE CAST(e.WAGEN_SERIENNUMMER AS TEXT) IN ({placeholders})"
            params.extend(wagons)

        total_query = f"SELECT COUNT(*) FROM {erp_full_table} e {where_clause}"
        total = conn.execute(total_query, params).fetchone()[0]

        query = f"""
            SELECT
                e.*,
                r.wagon_id AS rsrd_wagon_id,
                r.wagon_number_freight AS rsrd_wagon_number_freight,
                r.administrative_json,
                r.design_json,
                r.dataset_json
            FROM {erp_full_table} e
            LEFT JOIN {tables.detail} r
                ON r.wagon_number_freight = CAST(e.WAGEN_SERIENNUMMER AS TEXT)
            {where_clause}
            ORDER BY CAST(e.WAGEN_SERIENNUMMER AS TEXT)
        """
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params = params + [limit, offset]

        rows = conn.execute(query, params).fetchall()

        results = []
        created = 0
        for row in rows:
            erp_row = dict(row)
            admin = json.loads(row["administrative_json"]) if row["administrative_json"] else {}
            design = json.loads(row["design_json"]) if row["design_json"] else {}
            dataset = json.loads(row["dataset_json"]) if row["dataset_json"] else {}
            meta = dataset.get("RSRD2MetaData") if isinstance(dataset, dict) else {}
            diffs = compare_erp_to_rsrd(erp_row, admin, design, meta or {}, include_all=include_all)
            diff_count = sum(1 for diff in diffs if not diff.get("equal"))

            payload_obj = build_erp_payload(erp_row)
            wagon_number = (payload_obj.get("AdministrativeDataSet") or {}).get("WagonNumberFreight")
            wagon_number_str = str(wagon_number) if wagon_number is not None else None

            if create_upload and diff_count > 0 and wagon_number_str:
                now = datetime.utcnow().isoformat(timespec="seconds")
                conn.execute(
                    f"""
                    INSERT INTO {upload_table} (
                        wagon_number_freight,
                        rsrd_wagon_id,
                        payload_json,
                        diff_json,
                        rsrd_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(wagon_number_freight) DO UPDATE SET
                        rsrd_wagon_id=excluded.rsrd_wagon_id,
                        payload_json=excluded.payload_json,
                        diff_json=excluded.diff_json,
                        rsrd_json=excluded.rsrd_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        wagon_number_str,
                        row["rsrd_wagon_id"],
                        serialize_payload(payload_obj),
                        serialize_diffs(diffs),
                        row["dataset_json"],
                        now,
                        now,
                    ),
                )
                created += 1

            results.append(
                {
                    "wagon_number_freight": wagon_number_str,
                    "rsrd_wagon_id": row["rsrd_wagon_id"],
                    "rsrd_missing": not bool(row["dataset_json"]),
                    "diff_count": diff_count,
                    "differences": diffs,
                }
            )

        conn.commit()

    return {
        "rows": results,
        "limit": limit,
        "offset": offset,
        "total": total,
        "created": created,
        "env": _normalize_env(env),
    }


@app.post("/api/rsrd2/sync")
def rsrd2_sync(env: str = Query(DEFAULT_ENV), payload: dict = Body(...)) -> dict:
    wagons = payload.get("wagons") or []
    if not isinstance(wagons, list) or not all(isinstance(item, str) for item in wagons):
        raise HTTPException(status_code=400, detail="Feld 'wagons' muss eine Liste von Wagennummern sein.")
    snapshots = bool(payload.get("snapshots", True))
    try:
        tables = _rsrd_tables(env)
        rsrd_sync_wagons(wagons, keep_snapshots=snapshots, tables=tables, env=env)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"synced": len(wagons), "snapshots": snapshots, "env": _normalize_env(env)}


@app.post("/api/rsrd2/sync_all")
def rsrd2_sync_all(
    limit: int | None = Query(None, gt=0),
    snapshots: bool = Query(True),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    with _connect() as conn:
        wagons = _fetch_erp_wagon_numbers(conn, env, limit)
    if not wagons:
        raise HTTPException(status_code=404, detail="Keine Wagennummern im ERP-Cache gefunden.")
    try:
        tables = _rsrd_tables(env)
        stats = rsrd_sync_wagons(wagons, keep_snapshots=snapshots, mode="full", tables=tables, env=env)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "synced": len(wagons),
        "staged": stats["staged"],
        "processed": stats["processed"],
        "snapshots": snapshots,
        "env": _normalize_env(env),
    }


@app.post("/api/rsrd2/fetch_json")
def rsrd2_fetch_json(
    limit: int | None = Query(None, gt=0),
    snapshots: bool = Query(False),
    env: str = Query(DEFAULT_ENV),
) -> dict:
    with _connect() as conn:
        wagons = _fetch_erp_wagon_numbers(conn, env, limit)
    if not wagons:
        raise HTTPException(status_code=404, detail="Keine Wagennummern im ERP-Cache gefunden.")
    try:
        tables = _rsrd_tables(env)
        stats = rsrd_sync_wagons(wagons, keep_snapshots=snapshots, mode="stage", tables=tables, env=env)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"staged": stats["staged"], "snapshots": snapshots, "env": _normalize_env(env)}


@app.post("/api/rsrd2/process_json")
def rsrd2_process_json(limit: int | None = Query(None, gt=0), env: str = Query(DEFAULT_ENV)) -> dict:
    try:
        tables = _rsrd_tables(env)
        stats = rsrd_sync_wagons(
            [],
            keep_snapshots=False,
            mode="process",
            process_limit=limit,
            tables=tables,
            env=env,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"processed": stats["processed"], "limit": limit, "env": _normalize_env(env)}


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
