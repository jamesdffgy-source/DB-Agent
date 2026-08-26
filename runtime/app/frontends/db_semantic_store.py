"""DBQuill 本地语义目录持久化。

语义定义按稳定数据库标识隔离，保存业务术语到表、字段、枚举值和受控指标的映射。
本模块只负责八类语义定义的通用持久化；schema 校验与自然语言解析由
dbquill_core.SemanticCatalog 完成。
"""
from __future__ import annotations

import json
import hashlib
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "db_semantics.db"
_WRITE_LOCK = threading.RLock()


@contextmanager
def _connect():
    """事务连接上下文；始终显式提交/回滚并关闭 Windows 文件句柄。"""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS semantic_entries (
                id TEXT PRIMARY KEY,
                database_key TEXT NOT NULL,
                kind TEXT NOT NULL,
                term TEXT NOT NULL,
                term_key TEXT NOT NULL,
                table_name TEXT DEFAULT '',
                column_name TEXT DEFAULT '',
                value_json TEXT DEFAULT '',
                aggregation TEXT DEFAULT '',
                description TEXT DEFAULT '',
                created_at REAL DEFAULT 0,
                updated_at REAL DEFAULT 0,
                UNIQUE(database_key, term_key)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_semantic_database ON semantic_entries(database_key, updated_at DESC)"
        )


def _row_to_dict(row: sqlite3.Row) -> dict:
    payload = None
    if row["value_json"]:
        try:
            payload = json.loads(row["value_json"])
        except json.JSONDecodeError:
            payload = row["value_json"]
    kind = row["kind"]
    dimension_hierarchy = None
    dimension_filters = []
    if kind == "dimension" and isinstance(payload, dict):
        if "hierarchy" in payload or "filters" in payload:
            dimension_hierarchy = payload.get("hierarchy")
            dimension_filters = payload.get("filters") if isinstance(payload.get("filters"), list) else []
        else:
            # SemanticCatalog 2.6/2.7 只把 hierarchy 对象写入 value_json。
            dimension_hierarchy = payload
    return {
        "id": row["id"],
        "kind": kind,
        "term": row["term"],
        "table": row["table_name"],
        "column": row["column_name"],
        "value": payload if kind == "enum_value" else None,
        "aggregation": row["aggregation"],
        "filters": (
            payload if kind == "metric" and isinstance(payload, list)
            else dimension_filters if kind == "dimension"
            else []
        ),
        "formula": payload if kind == "ratio_metric" else None,
        "calendar": payload if kind == "business_calendar" else None,
        "hierarchy": dimension_hierarchy,
        "default_grain": payload if kind == "time_field" and isinstance(payload, str) else "",
        "description": row["description"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def _entry_payload(entry: dict) -> str:
    kind = entry.get("kind")
    if kind == "enum_value":
        payload = entry.get("value")
    elif kind == "metric":
        payload = entry.get("filters") or []
    elif kind == "ratio_metric":
        payload = entry.get("formula")
    elif kind == "business_calendar":
        payload = entry.get("calendar")
    elif kind == "dimension":
        hierarchy = entry.get("hierarchy")
        filters = entry.get("filters") or []
        if not hierarchy and not filters:
            return ""
        payload = {"hierarchy": hierarchy, "filters": filters}
    elif kind == "time_field":
        payload = entry.get("default_grain") or ""
        if not payload:
            return ""
    else:
        return ""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _revision_from_rows(rows) -> str:
    content = [
        {
            "id": row["id"],
            "kind": row["kind"],
            "term": row["term"],
            "table": row["table_name"],
            "column": row["column_name"],
            "value_json": row["value_json"],
            "aggregation": row["aggregation"],
            "description": row["description"],
        }
        for row in rows
    ]
    canonical = json.dumps(content, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def list_entries(database_key: str) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM semantic_entries
            WHERE database_key = ?
            ORDER BY kind, term_key
            """,
            (database_key,),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def list_entries_with_revision(database_key: str) -> tuple[list[dict], str]:
    """在同一读事务中返回目录快照和内容版本，供乐观并发预检使用。"""
    with _WRITE_LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM semantic_entries WHERE database_key = ? ORDER BY kind, term_key, id",
            (database_key,),
        ).fetchall()
        return [_row_to_dict(row) for row in rows], _revision_from_rows(rows)


def catalog_revision(database_key: str) -> str:
    return list_entries_with_revision(database_key)[1]


def upsert_entry(database_key: str, entry: dict) -> dict:
    now = time.time()
    entry_id = str(entry.get("id") or "").strip()[:64] or uuid.uuid4().hex[:16]
    term = str(entry.get("term") or "").strip()
    value_json = _entry_payload(entry)
    with _WRITE_LOCK, _connect() as conn:
        existing = conn.execute(
            "SELECT database_key, created_at FROM semantic_entries WHERE id = ?",
            (entry_id,),
        ).fetchone()
        if existing and existing["database_key"] != database_key:
            raise ValueError("语义定义 ID 已属于其他数据库")
        created_at = existing["created_at"] if existing else now
        conn.execute(
            """
            INSERT INTO semantic_entries (
                id, database_key, kind, term, term_key, table_name, column_name,
                value_json, aggregation, description, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind = excluded.kind,
                term = excluded.term,
                term_key = excluded.term_key,
                table_name = excluded.table_name,
                column_name = excluded.column_name,
                value_json = excluded.value_json,
                aggregation = excluded.aggregation,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            (
                entry_id,
                database_key,
                entry["kind"],
                term,
                term.casefold(),
                entry.get("table") or "",
                entry.get("column") or "",
                value_json,
                entry.get("aggregation") or "",
                entry.get("description") or "",
                created_at,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM semantic_entries WHERE id = ? AND database_key = ?",
            (entry_id, database_key),
        ).fetchone()
    return _row_to_dict(row)


def delete_entry(database_key: str, entry_id: str) -> bool:
    with _WRITE_LOCK, _connect() as conn:
        cursor = conn.execute(
            "DELETE FROM semantic_entries WHERE database_key = ? AND id = ?",
            (database_key, entry_id),
        )
        return cursor.rowcount > 0


def import_entries(database_key: str, entries: list[dict], expected_revision: str) -> list[dict]:
    """在一个事务中合并已验证条目；同名覆盖并保留原 ID，其余新增。"""
    if not isinstance(entries, list):
        raise ValueError("导入条目必须是列表")
    now = time.time()
    with _WRITE_LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM semantic_entries WHERE database_key = ? ORDER BY kind, term_key, id",
            (database_key,),
        ).fetchall()
        if _revision_from_rows(rows) != expected_revision:
            raise ValueError("语义目录在预检后已发生变化，请重新预检")
        existing = {str(row["term_key"]): row for row in rows}
        saved_ids = []
        for entry in entries:
            term = str(entry.get("term") or "").strip()
            term_key = term.casefold()
            row = existing.get(term_key)
            entry_id = str(row["id"]) if row else uuid.uuid4().hex[:16]
            created_at = float(row["created_at"]) if row else now
            conn.execute(
                """
                INSERT INTO semantic_entries (
                    id, database_key, kind, term, term_key, table_name, column_name,
                    value_json, aggregation, description, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    kind = excluded.kind,
                    term = excluded.term,
                    term_key = excluded.term_key,
                    table_name = excluded.table_name,
                    column_name = excluded.column_name,
                    value_json = excluded.value_json,
                    aggregation = excluded.aggregation,
                    description = excluded.description,
                    updated_at = excluded.updated_at
                """,
                (
                    entry_id, database_key, entry["kind"], term, term_key,
                    entry.get("table") or "", entry.get("column") or "",
                    _entry_payload(entry), entry.get("aggregation") or "",
                    entry.get("description") or "", created_at, now,
                ),
            )
            saved_ids.append(entry_id)
        if not saved_ids:
            return []
        placeholders = ",".join("?" for _ in saved_ids)
        saved = conn.execute(
            f"SELECT * FROM semantic_entries WHERE database_key = ? AND id IN ({placeholders})",
            (database_key, *saved_ids),
        ).fetchall()
    by_id = {row["id"]: _row_to_dict(row) for row in saved}
    return [by_id[entry_id] for entry_id in saved_ids]
