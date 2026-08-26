"""Persistent, access-scope-aware cache for generated database charts."""
from __future__ import annotations

import json
import re
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "dbquill_chart_cache.sqlite"
_LOCK = threading.RLock()
_SCHEMA_VERSION = 2
_MAX_CHARTS = 128
_MAX_CHART_BYTES = 2 * 1024 * 1024
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _connect() -> sqlite3.Connection:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with _LOCK, closing(_connect()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS chart_snapshots (
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                chart_count INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                PRIMARY KEY (database_ref, access_scope_ref)
            );
            CREATE TABLE IF NOT EXISTS chart_entries (
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                table_name TEXT NOT NULL,
                position INTEGER NOT NULL,
                chart_json TEXT NOT NULL,
                PRIMARY KEY (database_ref, access_scope_ref, table_name),
                FOREIGN KEY (database_ref, access_scope_ref)
                    REFERENCES chart_snapshots(database_ref, access_scope_ref)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_chart_entries_order
                ON chart_entries(database_ref, access_scope_ref, position);
            """
        )
        conn.commit()


def _validate_key(database_ref: str, access_scope_ref: str) -> tuple[str, str]:
    database_ref = str(database_ref or "").strip().lower()
    access_scope_ref = str(access_scope_ref or "").strip().lower()
    if not _HEX_64.fullmatch(database_ref):
        raise ValueError("database_ref must be a SHA-256 value")
    if access_scope_ref != "all" and not _HEX_64.fullmatch(access_scope_ref):
        raise ValueError("access_scope_ref must be all or a SHA-256 value")
    return database_ref, access_scope_ref


def _normalize_charts(charts: list[dict[str, Any]]) -> list[tuple[str, str]]:
    if not isinstance(charts, list) or len(charts) > _MAX_CHARTS:
        raise ValueError(f"chart count must be between 0 and {_MAX_CHARTS}")
    normalized: list[tuple[str, str]] = []
    seen: set[str] = set()
    for chart in charts:
        if not isinstance(chart, dict):
            raise ValueError("chart payload must be an object")
        meta = chart.get("meta")
        table = str(meta.get("table") if isinstance(meta, dict) else "").strip()
        if not table or len(table) > 512 or table.casefold() in seen:
            raise ValueError("each cached chart must have a unique table name")
        payload = json.dumps(
            chart, ensure_ascii=False, separators=(",", ":"), allow_nan=False,
        )
        if len(payload.encode("utf-8")) > _MAX_CHART_BYTES:
            raise ValueError("one chart payload exceeds the cache limit")
        seen.add(table.casefold())
        normalized.append((table, payload))
    return normalized


def load_snapshot(database_ref: str, access_scope_ref: str) -> Optional[dict[str, Any]]:
    database_ref, access_scope_ref = _validate_key(database_ref, access_scope_ref)
    init_db()
    with _LOCK, closing(_connect()) as conn:
        header = conn.execute(
            "SELECT source_fingerprint, generated_at, chart_count, schema_version "
            "FROM chart_snapshots WHERE database_ref=? AND access_scope_ref=?",
            (database_ref, access_scope_ref),
        ).fetchone()
        if header is None or int(header["schema_version"]) != _SCHEMA_VERSION:
            return None
        rows = conn.execute(
            "SELECT table_name, chart_json FROM chart_entries "
            "WHERE database_ref=? AND access_scope_ref=? ORDER BY position",
            (database_ref, access_scope_ref),
        ).fetchall()
    if len(rows) != int(header["chart_count"]):
        return None
    charts: list[dict[str, Any]] = []
    try:
        for row in rows:
            chart = json.loads(row["chart_json"])
            if not isinstance(chart, dict):
                return None
            meta = chart.get("meta")
            if not isinstance(meta, dict) or str(meta.get("table") or "") != row["table_name"]:
                return None
            charts.append(chart)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return {
        "sourceFingerprint": str(header["source_fingerprint"]),
        "generatedAt": str(header["generated_at"]),
        "charts": charts,
    }


def replace_snapshot(
    database_ref: str,
    access_scope_ref: str,
    source_fingerprint: str,
    charts: list[dict[str, Any]],
    *,
    generated_at: str = "",
) -> dict[str, Any]:
    database_ref, access_scope_ref = _validate_key(database_ref, access_scope_ref)
    source_fingerprint = str(source_fingerprint or "").strip().lower()
    if not _HEX_64.fullmatch(source_fingerprint):
        raise ValueError("source_fingerprint must be a SHA-256 value")
    entries = _normalize_charts(charts)
    generated_at = str(generated_at or "").strip() or datetime.now(timezone.utc).isoformat()
    init_db()
    with _LOCK, closing(_connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO chart_snapshots(database_ref, access_scope_ref, "
            "source_fingerprint, generated_at, chart_count, schema_version) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(database_ref, access_scope_ref) DO UPDATE SET "
            "source_fingerprint=excluded.source_fingerprint, "
            "generated_at=excluded.generated_at, chart_count=excluded.chart_count, "
            "schema_version=excluded.schema_version",
            (
                database_ref, access_scope_ref, source_fingerprint,
                generated_at, len(entries), _SCHEMA_VERSION,
            ),
        )
        conn.execute(
            "DELETE FROM chart_entries WHERE database_ref=? AND access_scope_ref=?",
            (database_ref, access_scope_ref),
        )
        conn.executemany(
            "INSERT INTO chart_entries(database_ref, access_scope_ref, table_name, "
            "position, chart_json) VALUES (?, ?, ?, ?, ?)",
            [
                (database_ref, access_scope_ref, table, position, payload)
                for position, (table, payload) in enumerate(entries)
            ],
        )
        conn.commit()
    return {
        "sourceFingerprint": source_fingerprint,
        "generatedAt": generated_at,
        "chartCount": len(entries),
    }
