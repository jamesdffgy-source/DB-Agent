"""Local expiring credentials for DB-Agent.

Only SHA-256 token hashes are persisted. Raw tokens are returned once when a
credential is issued and must never be written to logs or audit events.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "db_identities.db"
_WRITE_LOCK = threading.RLock()
_ROLES = frozenset({"viewer", "operator", "admin"})
_SCOPE_MODES = frozenset({"all", "restricted"})
_LABEL_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,64}$")
_DATABASE_REF_RE = re.compile(r"^[0-9a-f]{64}$")
_TABLE_NAME_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,128}$")
_COLUMN_NAME_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,128}$")
_ROW_OPERATORS = frozenset({
    "eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in",
    "is_null", "is_not_null",
})
MIN_TTL_HOURS = 1
MAX_TTL_HOURS = 24 * 365
DEFAULT_TTL_HOURS = 24 * 30
MAX_DATABASE_SCOPES = 64
MAX_TABLE_SCOPES = 256
MAX_COLUMN_SCOPES = 1024
MAX_ROW_SCOPE_FILTERS = 256
MAX_ROW_FILTERS_PER_TABLE = 4
MAX_ROW_FILTER_VALUES = 20
SCHEMA_VERSION = 5


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _token_hash(token: Any) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _credential_ref(token_hash: str) -> str:
    return hashlib.sha256(f"dbagent-credential:{token_hash}".encode("ascii")).hexdigest()[:16]


def _validate_label(label: Any) -> str:
    value = str(label or "").strip()
    if not _LABEL_RE.fullmatch(value):
        raise ValueError("凭据名称必须为 1–64 个可打印字符")
    return value


def _validate_role(role: Any) -> str:
    value = str(role or "").strip().lower()
    if value not in _ROLES:
        raise ValueError("role 必须是 viewer、operator 或 admin")
    return value


def _validate_ttl(ttl_hours: Any) -> int:
    if isinstance(ttl_hours, bool):
        raise ValueError("ttl_hours 必须是整数")
    try:
        value = int(ttl_hours)
    except (TypeError, ValueError) as exc:
        raise ValueError("ttl_hours 必须是整数") from exc
    if value < MIN_TTL_HOURS or value > MAX_TTL_HOURS:
        raise ValueError(f"ttl_hours 必须在 {MIN_TTL_HOURS}–{MAX_TTL_HOURS} 之间")
    return value


def _validate_database_scope(
    scope_mode: Any,
    database_refs: Any,
    table_scopes: Any = None,
    column_scopes: Any = None,
    row_scopes: Any = None,
) -> tuple[
    str,
    tuple[str, ...],
    dict[str, tuple[str, ...]],
    dict[str, dict[str, tuple[str, ...]]],
    dict[str, dict[str, tuple[dict, ...]]],
]:
    mode = str(scope_mode or "").strip().lower()
    if mode not in _SCOPE_MODES:
        raise ValueError("scope_mode 必须是 all 或 restricted")
    if database_refs is None:
        raw_refs = []
    elif isinstance(database_refs, (list, tuple, set, frozenset)):
        raw_refs = list(database_refs)
    else:
        raise ValueError("database_refs 必须是数据库引用列表")
    refs = []
    seen = set()
    for raw in raw_refs:
        ref = str(raw or "").strip().lower()
        if not _DATABASE_REF_RE.fullmatch(ref):
            raise ValueError("database_refs 必须全部是 64 位小写 SHA-256 引用")
        if ref not in seen:
            refs.append(ref)
            seen.add(ref)
    if len(refs) > MAX_DATABASE_SCOPES:
        raise ValueError(f"database_refs 最多允许 {MAX_DATABASE_SCOPES} 项")
    if mode == "all" and refs:
        raise ValueError("all 范围不能同时指定 database_refs")
    if mode == "restricted" and not refs:
        raise ValueError("restricted 范围至少需要一个 database_ref")
    if table_scopes is None:
        raw_table_scopes = {}
    elif isinstance(table_scopes, dict):
        raw_table_scopes = table_scopes
    else:
        raise ValueError("table_scopes 必须是 database_ref 到表名列表的对象")
    normalized_tables: dict[str, tuple[str, ...]] = {}
    table_count = 0
    for raw_ref, raw_tables in raw_table_scopes.items():
        database_ref = str(raw_ref or "").strip().lower()
        if database_ref not in seen:
            raise ValueError("table_scopes 只能引用已授权数据库")
        if not isinstance(raw_tables, (list, tuple, set, frozenset)):
            raise ValueError("table_scopes 的每个值必须是表名列表")
        tables = []
        table_seen = set()
        for raw_table in raw_tables:
            table = str(raw_table or "").strip()
            if not _TABLE_NAME_RE.fullmatch(table):
                raise ValueError("table_scopes 表名必须为 1–128 个可打印字符")
            folded = table.casefold()
            if folded not in table_seen:
                tables.append(table)
                table_seen.add(folded)
        if not tables:
            raise ValueError("table_scopes 中的受限数据库至少需要一个表")
        table_count += len(tables)
        normalized_tables[database_ref] = tuple(sorted(tables, key=str.casefold))
    if table_count > MAX_TABLE_SCOPES:
        raise ValueError(f"table_scopes 合计最多允许 {MAX_TABLE_SCOPES} 个表")
    if mode == "all" and normalized_tables:
        raise ValueError("all 数据库范围不能同时指定 table_scopes")
    if column_scopes is None:
        raw_column_scopes = {}
    elif isinstance(column_scopes, dict):
        raw_column_scopes = column_scopes
    else:
        raise ValueError("column_scopes 必须是 database_ref 到表字段对象的映射")
    normalized_columns: dict[str, dict[str, tuple[str, ...]]] = {}
    column_count = 0
    for raw_ref, raw_tables in raw_column_scopes.items():
        database_ref = str(raw_ref or "").strip().lower()
        scoped_tables = normalized_tables.get(database_ref)
        if scoped_tables is None:
            raise ValueError("column_scopes 只能引用已限定表的授权数据库")
        if not isinstance(raw_tables, dict):
            raise ValueError("column_scopes 的每个数据库值必须是表到字段列表的对象")
        canonical_tables = {table.casefold(): table for table in scoped_tables}
        normalized_for_database: dict[str, tuple[str, ...]] = {}
        for raw_table, raw_columns in raw_tables.items():
            folded_table = str(raw_table or "").strip().casefold()
            table = canonical_tables.get(folded_table)
            if table is None:
                raise ValueError("column_scopes 只能引用 table_scopes 中已授权的表")
            if not isinstance(raw_columns, (list, tuple, set, frozenset)):
                raise ValueError("column_scopes 的每个表值必须是字段名列表")
            columns = []
            column_seen = set()
            for raw_column in raw_columns:
                column = str(raw_column or "").strip()
                if not _COLUMN_NAME_RE.fullmatch(column):
                    raise ValueError("column_scopes 字段名必须为 1–128 个可打印字符")
                folded_column = column.casefold()
                if folded_column not in column_seen:
                    columns.append(column)
                    column_seen.add(folded_column)
            if not columns:
                raise ValueError("column_scopes 中的受限表至少需要一个字段")
            column_count += len(columns)
            normalized_for_database[table] = tuple(sorted(columns, key=str.casefold))
        if normalized_for_database:
            normalized_columns[database_ref] = dict(sorted(
                normalized_for_database.items(), key=lambda item: item[0].casefold(),
            ))
    if column_count > MAX_COLUMN_SCOPES:
        raise ValueError(f"column_scopes 合计最多允许 {MAX_COLUMN_SCOPES} 个字段")
    if mode == "all" and normalized_columns:
        raise ValueError("all 数据库范围不能同时指定 column_scopes")
    if row_scopes is None:
        raw_row_scopes = {}
    elif isinstance(row_scopes, dict):
        raw_row_scopes = row_scopes
    else:
        raise ValueError("row_scopes 必须是 database_ref 到表过滤列表的映射")

    def normalize_value(value: Any, label: str) -> Any:
        if value is None or isinstance(value, bool) or isinstance(value, int):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{label}必须是有限数值")
            return value
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized or len(normalized) > 240 \
                    or any(ord(char) < 32 for char in normalized):
                raise ValueError(f"{label}必须是 1–240 个可打印字符")
            return normalized
        raise ValueError(f"{label}只支持字符串、数字、布尔值或 null")

    normalized_rows: dict[str, dict[str, tuple[dict, ...]]] = {}
    row_filter_count = 0
    for raw_ref, raw_tables in raw_row_scopes.items():
        database_ref = str(raw_ref or "").strip().lower()
        scoped_tables = normalized_tables.get(database_ref)
        if scoped_tables is None:
            raise ValueError("row_scopes 只能引用已限定表的授权数据库")
        if not isinstance(raw_tables, dict):
            raise ValueError("row_scopes 的每个数据库值必须是表到过滤列表的对象")
        canonical_tables = {table.casefold(): table for table in scoped_tables}
        normalized_for_database: dict[str, tuple[dict, ...]] = {}
        for raw_table, raw_filters in raw_tables.items():
            table = canonical_tables.get(str(raw_table or "").strip().casefold())
            if table is None:
                raise ValueError("row_scopes 只能引用 table_scopes 中已授权的表")
            if not isinstance(raw_filters, list) or not raw_filters:
                raise ValueError("row_scopes 中的受限表必须提供非空过滤列表")
            if len(raw_filters) > MAX_ROW_FILTERS_PER_TABLE:
                raise ValueError(
                    f"每张表最多允许 {MAX_ROW_FILTERS_PER_TABLE} 条行过滤条件"
                )
            normalized_filters = []
            for index, raw_filter in enumerate(raw_filters, start=1):
                if not isinstance(raw_filter, dict) or set(raw_filter) - {
                    "column", "operator", "value",
                }:
                    raise ValueError("行过滤只支持 column、operator 和 value")
                column = str(raw_filter.get("column") or "").strip()
                if not _COLUMN_NAME_RE.fullmatch(column):
                    raise ValueError("行过滤字段必须为 1–128 个可打印字符")
                operator = str(raw_filter.get("operator") or "").strip().lower()
                if operator not in _ROW_OPERATORS:
                    raise ValueError("行过滤 operator 不受支持")
                label = f"第 {index} 条行过滤值"
                if operator in {"is_null", "is_not_null"}:
                    value = None
                elif operator in {"in", "not_in"}:
                    raw_values = raw_filter.get("value")
                    if not isinstance(raw_values, list) or not raw_values \
                            or len(raw_values) > MAX_ROW_FILTER_VALUES:
                        raise ValueError(
                            f"IN/NOT IN 必须提供 1–{MAX_ROW_FILTER_VALUES} 个值"
                        )
                    value = [normalize_value(item, label) for item in raw_values]
                else:
                    value = normalize_value(raw_filter.get("value"), label)
                normalized_filters.append({
                    "column": column, "operator": operator, "value": value,
                })
            row_filter_count += len(normalized_filters)
            normalized_for_database[table] = tuple(normalized_filters)
        if normalized_for_database:
            normalized_rows[database_ref] = dict(sorted(
                normalized_for_database.items(), key=lambda item: item[0].casefold(),
            ))
    if row_filter_count > MAX_ROW_SCOPE_FILTERS:
        raise ValueError(f"row_scopes 合计最多允许 {MAX_ROW_SCOPE_FILTERS} 条过滤")
    if mode == "all" and normalized_rows:
        raise ValueError("all 数据库范围不能同时指定 row_scopes")
    return (
        mode, tuple(sorted(refs)), normalized_tables, normalized_columns,
        normalized_rows,
    )


def validate_database_scope(
    scope_mode: Any,
    database_refs: Any,
    table_scopes: Any = None,
    column_scopes: Any = None,
    row_scopes: Any = None,
) -> dict:
    """Return a normalized public scope payload without changing storage."""
    mode, refs, tables, columns, rows = _validate_database_scope(
        scope_mode, database_refs, table_scopes, column_scopes, row_scopes,
    )
    return {
        "mode": mode,
        "databaseRefs": list(refs),
        "tableScopes": {ref: list(names) for ref, names in tables.items()},
        "columnScopes": {
            ref: {table: list(names) for table, names in scoped_tables.items()}
            for ref, scoped_tables in columns.items()
        },
        "rowScopes": {
            ref: {table: [dict(item) for item in filters] for table, filters in scoped.items()}
            for ref, scoped in rows.items()
        },
    }


@contextmanager
def _connect():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _WRITE_LOCK, _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_credentials (
                credential_id TEXT PRIMARY KEY,
                credential_ref TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL,
                role TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credential_table_scopes (
                credential_id TEXT NOT NULL,
                database_ref TEXT NOT NULL,
                table_name TEXT NOT NULL,
                PRIMARY KEY (credential_id, database_ref, table_name),
                FOREIGN KEY (credential_id)
                    REFERENCES local_credentials(credential_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_credential_table_scopes_ref "
            "ON credential_table_scopes(database_ref, table_name, credential_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credential_column_scopes (
                credential_id TEXT NOT NULL,
                database_ref TEXT NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                PRIMARY KEY (credential_id, database_ref, table_name, column_name),
                FOREIGN KEY (credential_id)
                    REFERENCES local_credentials(credential_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_credential_column_scopes_ref "
            "ON credential_column_scopes(database_ref, table_name, column_name, credential_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credential_row_scopes (
                credential_id TEXT NOT NULL,
                database_ref TEXT NOT NULL,
                table_name TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                column_name TEXT NOT NULL,
                operator TEXT NOT NULL,
                value_json TEXT NOT NULL,
                PRIMARY KEY (credential_id, database_ref, table_name, ordinal),
                FOREIGN KEY (credential_id)
                    REFERENCES local_credentials(credential_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_credential_row_scopes_ref "
            "ON credential_row_scopes(database_ref, table_name, credential_id)"
        )
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(local_credentials)").fetchall()
        }
        if "scope_mode" not in columns:
            # v1 credentials predate database scopes. Preserve their access as
            # explicit legacy all-database credentials instead of silently
            # revoking live tokens during the schema upgrade.
            conn.execute(
                "ALTER TABLE local_credentials "
                "ADD COLUMN scope_mode TEXT NOT NULL DEFAULT 'all'"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credential_database_scopes (
                credential_id TEXT NOT NULL,
                database_ref TEXT NOT NULL,
                PRIMARY KEY (credential_id, database_ref),
                FOREIGN KEY (credential_id)
                    REFERENCES local_credentials(credential_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_credential_database_scopes_ref "
            "ON credential_database_scopes(database_ref, credential_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_credentials_state "
            "ON local_credentials(revoked_at, expires_at, created_at DESC)"
        )
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _scope_data(
    conn: sqlite3.Connection,
    credential_id: str,
) -> tuple[
    list[str], dict[str, list[str]], dict[str, dict[str, list[str]]],
    dict[str, dict[str, list[dict]]],
]:
    refs = [
        str(row["database_ref"])
        for row in conn.execute(
            "SELECT database_ref FROM credential_database_scopes "
            "WHERE credential_id = ? ORDER BY database_ref ASC",
            (credential_id,),
        ).fetchall()
    ]
    table_scopes: dict[str, list[str]] = {}
    for row in conn.execute(
        "SELECT database_ref, table_name FROM credential_table_scopes "
        "WHERE credential_id = ? ORDER BY database_ref ASC, table_name COLLATE NOCASE ASC",
        (credential_id,),
    ).fetchall():
        table_scopes.setdefault(str(row["database_ref"]), []).append(str(row["table_name"]))
    column_scopes: dict[str, dict[str, list[str]]] = {}
    for row in conn.execute(
        "SELECT database_ref, table_name, column_name FROM credential_column_scopes "
        "WHERE credential_id = ? ORDER BY database_ref ASC, "
        "table_name COLLATE NOCASE ASC, column_name COLLATE NOCASE ASC",
        (credential_id,),
    ).fetchall():
        column_scopes.setdefault(str(row["database_ref"]), {}).setdefault(
            str(row["table_name"]), [],
        ).append(str(row["column_name"]))
    row_scopes: dict[str, dict[str, list[dict]]] = {}
    for row in conn.execute(
        "SELECT database_ref, table_name, column_name, operator, value_json "
        "FROM credential_row_scopes WHERE credential_id = ? "
        "ORDER BY database_ref ASC, table_name COLLATE NOCASE ASC, ordinal ASC",
        (credential_id,),
    ).fetchall():
        row_scopes.setdefault(str(row["database_ref"]), {}).setdefault(
            str(row["table_name"]), [],
        ).append({
            "column": str(row["column_name"]),
            "operator": str(row["operator"]),
            "value": json.loads(str(row["value_json"])),
        })
    return refs, table_scopes, column_scopes, row_scopes


def _public_row(
    row: sqlite3.Row,
    *,
    database_refs: Optional[list[str]] = None,
    table_scopes: Optional[dict[str, list[str]]] = None,
    column_scopes: Optional[dict[str, dict[str, list[str]]]] = None,
    row_scopes: Optional[dict[str, dict[str, list[dict]]]] = None,
    now: Optional[datetime] = None,
) -> dict:
    now_iso = _iso(now or _utc_now())
    revoked_at = str(row["revoked_at"] or "")
    expires_at = str(row["expires_at"])
    if revoked_at:
        status = "revoked"
    elif expires_at <= now_iso:
        status = "expired"
    else:
        status = "active"
    return {
        "id": str(row["credential_id"]),
        "credentialRef": str(row["credential_ref"]),
        "label": str(row["label"]),
        "role": str(row["role"]),
        "createdAt": str(row["created_at"]),
        "expiresAt": expires_at,
        "revokedAt": revoked_at or None,
        "status": status,
        "databaseScope": {
            "mode": str(row["scope_mode"] or "all"),
            "databaseRefs": list(database_refs or []),
            "tableScopes": dict(table_scopes or {}),
            "columnScopes": dict(column_scopes or {}),
            "rowScopes": dict(row_scopes or {}),
        },
    }


def issue_credential(
    *,
    label: Any,
    role: Any,
    ttl_hours: Any = DEFAULT_TTL_HOURS,
    scope_mode: Any = "all",
    database_refs: Any = None,
    table_scopes: Any = None,
    column_scopes: Any = None,
    row_scopes: Any = None,
    now: Optional[datetime] = None,
) -> dict:
    """Create a credential and return its raw token exactly once."""
    safe_label = _validate_label(label)
    safe_role = _validate_role(role)
    safe_ttl = _validate_ttl(ttl_hours)
    (
        safe_scope_mode,
        safe_database_refs,
        safe_table_scopes,
        safe_column_scopes,
        safe_row_scopes,
    ) = _validate_database_scope(
        scope_mode, database_refs, table_scopes, column_scopes, row_scopes,
    )
    issued_at = (now or _utc_now()).astimezone(timezone.utc)
    expires_at = issued_at + timedelta(hours=safe_ttl)
    token = f"id_{secrets.token_urlsafe(32)}"
    token_hash = _token_hash(token)
    record = {
        "credential_id": str(uuid.uuid4()),
        "credential_ref": _credential_ref(token_hash),
        "label": safe_label,
        "role": safe_role,
        "token_hash": token_hash,
        "created_at": _iso(issued_at),
        "expires_at": _iso(expires_at),
        "scope_mode": safe_scope_mode,
    }
    with _WRITE_LOCK, _connect() as conn:
        conn.execute(
            """
            INSERT INTO local_credentials(
                credential_id, credential_ref, label, role, token_hash,
                created_at, expires_at, revoked_at, scope_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?)
            """,
            tuple(record[key] for key in (
                "credential_id", "credential_ref", "label", "role", "token_hash",
                "created_at", "expires_at", "scope_mode",
            )),
        )
        conn.executemany(
            "INSERT INTO credential_database_scopes(credential_id, database_ref) "
            "VALUES (?, ?)",
            ((record["credential_id"], ref) for ref in safe_database_refs),
        )
        conn.executemany(
            "INSERT INTO credential_row_scopes(" 
            "credential_id, database_ref, table_name, ordinal, column_name, operator, value_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    record["credential_id"], ref, table, ordinal,
                    item["column"], item["operator"],
                    json.dumps(item["value"], ensure_ascii=False, separators=(",", ":")),
                )
                for ref, scoped_tables in safe_row_scopes.items()
                for table, filters in scoped_tables.items()
                for ordinal, item in enumerate(filters)
            ),
        )
        conn.executemany(
            "INSERT INTO credential_table_scopes(credential_id, database_ref, table_name) "
            "VALUES (?, ?, ?)",
            (
                (record["credential_id"], ref, table)
                for ref, tables in safe_table_scopes.items()
                for table in tables
            ),
        )
        conn.executemany(
            "INSERT INTO credential_column_scopes(" 
            "credential_id, database_ref, table_name, column_name) VALUES (?, ?, ?, ?)",
            (
                (record["credential_id"], ref, table, column)
                for ref, scoped_tables in safe_column_scopes.items()
                for table, columns in scoped_tables.items()
                for column in columns
            ),
        )
        row = conn.execute(
            "SELECT * FROM local_credentials WHERE credential_id = ?",
            (record["credential_id"],),
        ).fetchone()
    result = _public_row(
        row,
        database_refs=list(safe_database_refs),
        table_scopes={ref: list(tables) for ref, tables in safe_table_scopes.items()},
        column_scopes={
            ref: {table: list(columns) for table, columns in scoped_tables.items()}
            for ref, scoped_tables in safe_column_scopes.items()
        },
        row_scopes={
            ref: {table: [dict(item) for item in filters] for table, filters in scoped.items()}
            for ref, scoped in safe_row_scopes.items()
        },
        now=issued_at,
    )
    result["token"] = token
    return result


def authenticate(token: Any, *, now: Optional[datetime] = None) -> Optional[dict]:
    supplied = str(token or "")
    if not supplied.startswith("id_") or len(supplied) < 32:
        return None
    token_hash = _token_hash(supplied)
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM local_credentials WHERE token_hash = ?",
            (token_hash,),
        ).fetchone()
        refs, table_scopes, column_scopes, row_scopes = (
            _scope_data(conn, str(row["credential_id"])) if row else ([], {}, {}, {})
        )
    if row is None:
        return None
    public = _public_row(
        row, database_refs=refs, table_scopes=table_scopes,
        column_scopes=column_scopes, row_scopes=row_scopes, now=now,
    )
    return public if public["status"] == "active" else None


def get_credential(credential_id: Any, *, now: Optional[datetime] = None) -> Optional[dict]:
    value = str(credential_id or "").strip()
    if not value:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM local_credentials WHERE credential_id = ?",
            (value,),
        ).fetchone()
        refs, table_scopes, column_scopes, row_scopes = (
            _scope_data(conn, value) if row else ([], {}, {}, {})
        )
    return _public_row(
        row, database_refs=refs, table_scopes=table_scopes,
        column_scopes=column_scopes, row_scopes=row_scopes, now=now,
    ) if row else None


def list_credentials(*, now: Optional[datetime] = None) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM local_credentials ORDER BY created_at DESC, credential_id DESC"
        ).fetchall()
        scopes_by_id = {
            str(row["credential_id"]): _scope_data(conn, str(row["credential_id"]))
            for row in rows
        }
    return [
        _public_row(
            row,
            database_refs=scopes_by_id[str(row["credential_id"])][0],
            table_scopes=scopes_by_id[str(row["credential_id"])][1],
            column_scopes=scopes_by_id[str(row["credential_id"])][2],
            row_scopes=scopes_by_id[str(row["credential_id"])][3],
            now=now,
        )
        for row in rows
    ]


def revoke_credential(
    credential_id: Any,
    *,
    now: Optional[datetime] = None,
) -> dict:
    value = str(credential_id or "").strip()
    if not value:
        raise ValueError("credential_id 不能为空")
    revoked_at = _iso(now or _utc_now())
    with _WRITE_LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM local_credentials WHERE credential_id = ?",
            (value,),
        ).fetchone()
        if row is None:
            raise KeyError("凭据不存在")
        if str(row["revoked_at"] or ""):
            raise ValueError("凭据已经吊销")
        conn.execute(
            "UPDATE local_credentials SET revoked_at = ? WHERE credential_id = ?",
            (revoked_at, value),
        )
        row = conn.execute(
            "SELECT * FROM local_credentials WHERE credential_id = ?",
            (value,),
        ).fetchone()
        refs, table_scopes, column_scopes, row_scopes = _scope_data(conn, value)
    return _public_row(
        row, database_refs=refs, table_scopes=table_scopes,
        column_scopes=column_scopes, row_scopes=row_scopes, now=now,
    )
