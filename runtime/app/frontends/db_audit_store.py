"""DBQuill 本地操作审计账本。

账本只接受受控、脱敏后的元数据，不保存原始自然语言问题、SQL、结果行、
连接串、文件路径或凭据。事件只追加写入，并通过 SHA-256 前向哈希链提供
篡改检测；本模块不暴露更新或删除历史事件的接口。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import uuid
import zipfile
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "db_audit.db"
_WRITE_LOCK = threading.RLock()

SCHEMA_VERSION = 1
EXPORT_FORMAT = "dbquill-audit-ledger"
BACKUP_FORMAT = "dbquill-audit-backup"
ANCHOR_FORMAT = "dbquill-audit-anchor"
ARCHIVE_FORMAT = "dbquill-audit-archive"
RESTORE_DRILL_FORMAT = "dbquill-audit-restore-drill"
EXTERNAL_BACKUP_FORMAT = "dbquill-audit-external-backup"
LEDGER_ASSESSMENT_FORMAT = "dbquill-audit-ledger-assessment"
CORRUPT_EVIDENCE_FORMAT = "dbquill-audit-corrupt-ledger-evidence"
EXTERNAL_TARGET_CONFIG_FORMAT = "dbquill-audit-external-target-config"
EXTERNAL_TARGET_STATE_FORMAT = "dbquill-audit-external-target-state"
EXTERNAL_TARGET_ATTEMPT_FORMAT = "dbquill-audit-external-target-attempt"
_LEGACY_FORMATS = {
    BACKUP_FORMAT: "dbagent-audit-backup",
    ANCHOR_FORMAT: "dbagent-audit-anchor",
    ARCHIVE_FORMAT: "dbagent-audit-archive",
    RESTORE_DRILL_FORMAT: "dbagent-audit-restore-drill",
    EXTERNAL_BACKUP_FORMAT: "dbagent-audit-external-backup",
    LEDGER_ASSESSMENT_FORMAT: "dbagent-audit-ledger-assessment",
    CORRUPT_EVIDENCE_FORMAT: "dbagent-audit-corrupt-ledger-evidence",
    EXTERNAL_TARGET_CONFIG_FORMAT: "dbagent-audit-external-target-config",
    EXTERNAL_TARGET_STATE_FORMAT: "dbagent-audit-external-target-state",
    EXTERNAL_TARGET_ATTEMPT_FORMAT: "dbagent-audit-external-target-attempt",
}
_GENESIS_HASH = "0" * 64
_MAX_DETAILS_BYTES = 4096
_MAX_SUMMARY_LENGTH = 120
_MAX_ACTION_LENGTH = 64
_ACTION_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_REF_RE = re.compile(r"^[0-9a-f]{12,64}$")


def _format_supported(value: Any, current: str) -> bool:
    """Accept DBQuill artifacts and read-only v0.1 format identifiers."""
    return value in {current, _LEGACY_FORMATS.get(current)}

_CATEGORIES = frozenset({
    "nl_operation", "write_confirmation", "write_execution",
    "semantic_change", "schedule_change", "schedule_execution",
    "memory_change", "audit_backup", "audit_control", "access_control", "system",
})
_OUTCOMES = frozenset({
    "pending", "approved", "succeeded", "failed", "rejected", "cancelled",
})
_RISKS = frozenset({"low", "medium", "high"})
_ACTORS = frozenset({
    "local_user", "local_viewer", "local_operator", "local_admin",
    "scheduler", "system",
})

# 详情字段采用显式白名单。调用方若误传 question/sql/password 等原文，写入会失败。
_DETAIL_KEYS = frozenset({
    "question_sha256", "question_length", "sql_sha256", "error_sha256",
    "answer_kind", "operation_action", "operation_mode", "operation_status",
    "result_rows", "dataset_count", "affected_rows", "target_count", "column_count",
    "target_refs", "confirm_ref", "semantic_kind", "semantic_ref",
    "entry_count", "added_count", "updated_count", "skipped_count",
    "error_type", "http_status", "cancel_requested", "audit_warning",
    "schedule_ref", "task_type", "trigger", "blocked_write",
    "backup_ref", "backup_count", "legacy_log_count",
    "required_role", "request_role", "route_ref", "http_method",
    "approval_policy", "credential_ref", "credential_role", "expires_in_hours",
    "database_scope_mode", "database_scope_count",
    "table_scope_database_count", "table_scope_table_count",
    "column_scope_table_count", "column_scope_column_count",
    "row_scope_table_count", "row_scope_filter_count",
    "pending_sequence", "disposition", "evidence_sha256",
    "memory_ref", "memory_layer", "memory_status", "reflection_stage",
})
_INTEGER_KEYS = frozenset({
    "question_length", "result_rows", "dataset_count", "affected_rows",
    "target_count", "column_count", "entry_count", "added_count", "updated_count",
    "skipped_count", "http_status", "backup_count", "legacy_log_count",
    "expires_in_hours",
    "database_scope_count", "table_scope_database_count", "table_scope_table_count",
    "column_scope_table_count", "column_scope_column_count",
    "row_scope_table_count", "row_scope_filter_count",
    "pending_sequence",
})
_HASH_KEYS = frozenset({
    "question_sha256", "sql_sha256", "error_sha256", "evidence_sha256",
})
_REF_KEYS = frozenset({
    "confirm_ref", "semantic_ref", "schedule_ref", "backup_ref", "route_ref",
    "credential_ref", "memory_ref",
})
_PENDING_DISPOSITIONS = frozenset({
    "verified_no_change", "verified_completed", "superseded",
})


def sha256_text(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def short_ref(value: Any, length: int = 16) -> str:
    length = max(12, min(int(length), 64))
    return sha256_text(value)[:length]


def database_ref(database_key: Any) -> str:
    return sha256_text(database_key)


@contextmanager
def _connect():
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
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
            CREATE TABLE IF NOT EXISTS audit_events (
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                occurred_at TEXT NOT NULL,
                category TEXT NOT NULL,
                action TEXT NOT NULL,
                outcome TEXT NOT NULL,
                risk TEXT NOT NULL,
                actor TEXT NOT NULL,
                database_ref TEXT NOT NULL,
                session_ref TEXT NOT NULL,
                run_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                details_json TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                event_hash TEXT NOT NULL UNIQUE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(occurred_at DESC, sequence DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_filter "
            "ON audit_events(database_ref, category, outcome, sequence DESC)"
        )


def _bounded_token(value: Any, *, field: str, maximum: int = 64) -> str:
    token = str(value or "").strip()
    if len(token) > maximum:
        raise ValueError(f"{field} 过长")
    return token


def _safe_details(details: Optional[dict]) -> dict:
    if details is None:
        return {}
    if not isinstance(details, dict):
        raise ValueError("audit details 必须是对象")
    unknown = sorted(set(details) - _DETAIL_KEYS)
    if unknown:
        raise ValueError(f"audit details 包含未允许字段: {', '.join(unknown)}")
    clean: dict[str, Any] = {}
    for key, value in details.items():
        if value is None or value == "":
            continue
        if key in _INTEGER_KEYS:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"audit details.{key} 必须是非负整数")
            clean[key] = value
        elif key in _HASH_KEYS:
            token = str(value).strip().lower()
            if not _HEX_64_RE.fullmatch(token):
                raise ValueError(f"audit details.{key} 必须是 SHA-256")
            clean[key] = token
        elif key in _REF_KEYS:
            token = str(value).strip().lower()
            if not _REF_RE.fullmatch(token):
                raise ValueError(f"audit details.{key} 必须是脱敏引用")
            clean[key] = token
        elif key == "target_refs":
            if not isinstance(value, list) or len(value) > 32:
                raise ValueError("audit details.target_refs 必须是不超过 32 项的列表")
            refs = []
            for item in value:
                token = str(item).strip().lower()
                if not _REF_RE.fullmatch(token):
                    raise ValueError("audit details.target_refs 必须全部是脱敏引用")
                refs.append(token)
            clean[key] = refs
        elif key in {"cancel_requested", "audit_warning", "blocked_write"}:
            if not isinstance(value, bool):
                raise ValueError(f"audit details.{key} 必须是布尔值")
            clean[key] = value
        else:
            token = _bounded_token(value, field=f"details.{key}", maximum=80)
            clean[key] = token
    encoded = json.dumps(clean, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > _MAX_DETAILS_BYTES:
        raise ValueError("audit details 过大")
    return clean


def _canonical_payload(event: dict) -> str:
    fields = {
        "sequence": event["sequence"],
        "event_id": event["event_id"],
        "occurred_at": event["occurred_at"],
        "category": event["category"],
        "action": event["action"],
        "outcome": event["outcome"],
        "risk": event["risk"],
        "actor": event["actor"],
        "database_ref": event["database_ref"],
        "session_ref": event["session_ref"],
        "run_id": event["run_id"],
        "correlation_id": event["correlation_id"],
        "summary": event["summary"],
        "details": event["details"],
        "previous_hash": event["previous_hash"],
    }
    return json.dumps(fields, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def append_event(
    *,
    category: str,
    action: str,
    outcome: str,
    summary: str,
    risk: str = "low",
    actor: str = "local_user",
    database_key: str = "",
    session_id: str = "",
    run_id: str = "",
    correlation_id: str = "",
    details: Optional[dict] = None,
    _database_ref_override: str = "",
) -> dict:
    category = _bounded_token(category, field="category")
    action = _bounded_token(action, field="action", maximum=_MAX_ACTION_LENGTH)
    outcome = _bounded_token(outcome, field="outcome")
    risk = _bounded_token(risk, field="risk")
    actor = _bounded_token(actor, field="actor")
    summary = _bounded_token(summary, field="summary", maximum=_MAX_SUMMARY_LENGTH)
    run_id = _bounded_token(run_id, field="run_id")
    correlation_id = _bounded_token(correlation_id, field="correlation_id")
    if category not in _CATEGORIES:
        raise ValueError(f"不支持的审计分类: {category}")
    if not _ACTION_RE.fullmatch(action):
        raise ValueError("audit action 格式无效")
    if outcome not in _OUTCOMES:
        raise ValueError(f"不支持的审计结果: {outcome}")
    if risk not in _RISKS:
        raise ValueError(f"不支持的风险级别: {risk}")
    if actor not in _ACTORS:
        raise ValueError(f"不支持的审计主体: {actor}")
    if not summary:
        raise ValueError("audit summary 不能为空")
    clean_details = _safe_details(details)
    stored_database_ref = ""
    if _database_ref_override:
        stored_database_ref = str(_database_ref_override).strip().lower()
        if not _HEX_64_RE.fullmatch(stored_database_ref):
            raise ValueError("database_ref override 必须是 SHA-256")
    else:
        stored_database_ref = database_ref(database_key)
    event_id = uuid.uuid4().hex[:20]
    occurred_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")

    with _WRITE_LOCK, _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        previous = conn.execute(
            "SELECT sequence, event_hash FROM audit_events ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        sequence = (int(previous["sequence"]) + 1) if previous else 1
        previous_hash = str(previous["event_hash"]) if previous else _GENESIS_HASH
        event = {
            "sequence": sequence,
            "event_id": event_id,
            "occurred_at": occurred_at,
            "category": category,
            "action": action,
            "outcome": outcome,
            "risk": risk,
            "actor": actor,
            "database_ref": stored_database_ref,
            "session_ref": short_ref(session_id) if session_id else "",
            "run_id": run_id,
            "correlation_id": correlation_id,
            "summary": summary,
            "details": clean_details,
            "previous_hash": previous_hash,
        }
        event_hash = sha256_text(_canonical_payload(event))
        conn.execute(
            """
            INSERT INTO audit_events (
                sequence, event_id, occurred_at, category, action, outcome, risk,
                actor, database_ref, session_ref, run_id, correlation_id, summary,
                details_json, previous_hash, event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sequence, event_id, occurred_at, category, action, outcome, risk,
                actor, event["database_ref"], event["session_ref"], run_id,
                correlation_id, summary,
                json.dumps(clean_details, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                previous_hash, event_hash,
            ),
        )
    event["event_hash"] = event_hash
    return event


def _row_to_event(row: sqlite3.Row) -> dict:
    try:
        details = json.loads(row["details_json"] or "{}")
    except json.JSONDecodeError:
        details = None
    return {
        "sequence": int(row["sequence"]),
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "category": row["category"],
        "action": row["action"],
        "outcome": row["outcome"],
        "risk": row["risk"],
        "actor": row["actor"],
        "database_ref": row["database_ref"],
        "session_ref": row["session_ref"],
        "run_id": row["run_id"],
        "correlation_id": row["correlation_id"],
        "summary": row["summary"],
        "details": details,
        "previous_hash": row["previous_hash"],
        "event_hash": row["event_hash"],
    }


def _integrity_from_rows(rows) -> dict:
    expected_previous = _GENESIS_HASH
    expected_sequence = 1
    for row in rows:
        event = _row_to_event(row)
        if event["details"] is None:
            return {
                "ok": False, "count": len(rows), "error_sequence": event["sequence"],
                "error": "details_json 无效",
            }
        if event["sequence"] != expected_sequence:
            return {
                "ok": False, "count": len(rows), "error_sequence": event["sequence"],
                "error": "事件序号不连续",
            }
        if event["previous_hash"] != expected_previous:
            return {
                "ok": False, "count": len(rows), "error_sequence": event["sequence"],
                "error": "前序哈希不匹配",
            }
        expected_hash = sha256_text(_canonical_payload(event))
        if event["event_hash"] != expected_hash:
            return {
                "ok": False, "count": len(rows), "error_sequence": event["sequence"],
                "error": "事件哈希不匹配",
            }
        expected_previous = event["event_hash"]
        expected_sequence += 1
    return {
        "ok": True,
        "count": len(rows),
        "head_hash": expected_previous if rows else _GENESIS_HASH,
        "verified_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }


def _integrity_from_event_dicts(events: list[dict]) -> dict:
    expected_previous = _GENESIS_HASH
    expected_sequence = 1
    required = {
        "sequence", "event_id", "occurred_at", "category", "action", "outcome",
        "risk", "actor", "database_ref", "session_ref", "run_id",
        "correlation_id", "summary", "details", "previous_hash", "event_hash",
    }
    for raw_event in events:
        if not isinstance(raw_event, dict) or set(raw_event) != required:
            return {"ok": False, "count": len(events), "error": "archive event structure invalid"}
        event = dict(raw_event)
        if not isinstance(event.get("details"), dict):
            return {
                "ok": False, "count": len(events),
                "error_sequence": event.get("sequence"), "error": "archive event details invalid",
            }
        if event.get("sequence") != expected_sequence:
            return {
                "ok": False, "count": len(events),
                "error_sequence": event.get("sequence"), "error": "archive sequence discontinuity",
            }
        if event.get("previous_hash") != expected_previous:
            return {
                "ok": False, "count": len(events),
                "error_sequence": event.get("sequence"), "error": "archive previous hash mismatch",
            }
        expected_hash = sha256_text(_canonical_payload(event))
        if event.get("event_hash") != expected_hash:
            return {
                "ok": False, "count": len(events),
                "error_sequence": event.get("sequence"), "error": "archive event hash mismatch",
            }
        expected_previous = expected_hash
        expected_sequence += 1
    return {
        "ok": True,
        "count": len(events),
        "head_hash": expected_previous if events else _GENESIS_HASH,
        "verified_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }


def _read_ledger_rows(path: Path):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError("审计账本文件不存在")
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM audit_events ORDER BY sequence ASC").fetchall()
    finally:
        conn.close()


def _verify_ledger_file(path: Path) -> dict:
    return _integrity_from_rows(_read_ledger_rows(path))


def list_events(
    *,
    limit: int = 100,
    before_sequence: Optional[int] = None,
    category: str = "",
    outcome: str = "",
    database_key: Optional[str] = None,
) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    clauses = []
    params: list[Any] = []
    if before_sequence is not None:
        clauses.append("sequence < ?")
        params.append(max(1, int(before_sequence)))
    if category:
        if category not in _CATEGORIES:
            raise ValueError("不支持的审计分类筛选")
        clauses.append("category = ?")
        params.append(category)
    if outcome:
        if outcome not in _OUTCOMES:
            raise ValueError("不支持的审计结果筛选")
        clauses.append("outcome = ?")
        params.append(outcome)
    if database_key is not None:
        clauses.append("database_ref = ?")
        params.append(database_ref(database_key))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT * FROM audit_events {where} ORDER BY sequence DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
    return [_row_to_event(row) for row in rows]


def verify_chain() -> dict:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM audit_events ORDER BY sequence ASC").fetchall()
    return _integrity_from_rows(rows)


def reconciliation_status(
    *,
    limit: int = 50,
    database_key: Optional[str] = None,
) -> dict:
    """Find approved/pending operations that do not yet have a terminal event."""
    integrity = verify_chain()
    if not integrity.get("ok"):
        return {
            "ok": False, "unresolved_count": 0, "by_category": {}, "items": [],
            "error": "审计账本完整性异常，无法对账",
        }
    with _connect() as conn:
        if database_key is None:
            rows = conn.execute(
                "SELECT * FROM audit_events ORDER BY sequence ASC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audit_events WHERE database_ref = ? "
                "ORDER BY sequence ASC",
                (database_ref(database_key),),
            ).fetchall()
    pending: dict[str, list[dict]] = {}
    manually_resolved_count = 0

    def open_item(key: str, event: dict) -> None:
        if event.get("correlation_id"):
            pending.setdefault(key, []).append(event)

    def close_item(key: str) -> None:
        queue = pending.get(key)
        if queue:
            queue.pop(0)
            if not queue:
                pending.pop(key, None)

    terminal = {"succeeded", "failed", "rejected", "cancelled"}
    for row in rows:
        event = _row_to_event(row)
        category = event["category"]
        action = event["action"]
        outcome = event["outcome"]
        correlation = event["correlation_id"]
        if not correlation:
            continue
        if category == "audit_control" and action == "resolve_pending" \
                and outcome == "succeeded":
            details = event.get("details") or {}
            pending_sequence = details.get("pending_sequence")
            disposition = details.get("disposition")
            evidence_sha256 = details.get("evidence_sha256")
            if (
                isinstance(pending_sequence, int)
                and pending_sequence > 0
                and disposition in _PENDING_DISPOSITIONS
                and isinstance(evidence_sha256, str)
                and _HEX_64_RE.fullmatch(evidence_sha256)
            ):
                for key, queue in list(pending.items()):
                    target = next((
                        item for item in queue
                        if item["sequence"] == pending_sequence
                        and item["correlation_id"] == correlation
                        and item["database_ref"] == event["database_ref"]
                    ), None)
                    if target is not None:
                        queue.remove(target)
                        if not queue:
                            pending.pop(key, None)
                        manually_resolved_count += 1
                        break
        elif category == "write_confirmation" and action == "approve" and outcome == "approved":
            open_item(f"write:{correlation}", event)
        elif category == "write_execution" and action == "execute" and outcome in terminal:
            close_item(f"write:{correlation}")
        elif category == "semantic_change":
            key = f"semantic:{action}:{correlation}"
            if outcome == "approved":
                open_item(key, event)
            elif outcome in terminal:
                close_item(key)
        elif category == "memory_change":
            key = f"memory:{action}:{correlation}"
            if outcome == "approved":
                open_item(key, event)
            elif outcome in terminal:
                close_item(key)
        elif category == "schedule_change":
            key = f"schedule_change:{action}:{correlation}"
            if outcome == "approved":
                open_item(key, event)
            elif outcome in terminal:
                close_item(key)
        elif category == "schedule_execution":
            key = f"schedule_run:{correlation}"
            if outcome == "pending":
                open_item(key, event)
            elif outcome in terminal:
                close_item(key)
        elif category == "access_control" and action in {
            "credential_issue", "credential_revoke",
        }:
            key = f"access_control:{action}:{correlation}"
            if outcome == "approved":
                open_item(key, event)
            elif outcome in terminal:
                close_item(key)

    unresolved = [event for queue in pending.values() for event in queue]
    unresolved.sort(key=lambda item: item["sequence"], reverse=True)
    by_category: dict[str, int] = {}
    for event in unresolved:
        by_category[event["category"]] = by_category.get(event["category"], 0) + 1
    safe_items = [{
        "sequence": event["sequence"],
        "occurred_at": event["occurred_at"],
        "category": event["category"],
        "action": event["action"],
        "risk": event["risk"],
        "database_ref": event["database_ref"],
        "correlation_id": event["correlation_id"],
    } for event in unresolved[:max(1, min(int(limit), 10000))]]
    return {
        "ok": True,
        "scope": "database" if database_key is not None else "all",
        "unresolved_count": len(unresolved),
        "manually_resolved_count": manually_resolved_count,
        "by_category": by_category,
        "items": safe_items,
        "checked_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }


def resolve_pending_event(
    sequence: Any,
    *,
    disposition: Any,
    evidence_sha256: Any,
    expected_database_key: Optional[str] = None,
    actor: str = "local_admin",
) -> dict:
    """Append a manual disposition for one currently unresolved event.

    This never edits or deletes the original event. The evidence value must
    already be a SHA-256 so ticket text, paths, or operator notes are not
    persisted in the ledger.
    """
    if isinstance(sequence, bool):
        raise ValueError("pending sequence 必须是正整数")
    try:
        pending_sequence = int(sequence)
    except (TypeError, ValueError) as exc:
        raise ValueError("pending sequence 必须是正整数") from exc
    if pending_sequence <= 0:
        raise ValueError("pending sequence 必须是正整数")
    normalized_disposition = str(disposition or "").strip().lower()
    if normalized_disposition not in _PENDING_DISPOSITIONS:
        raise ValueError("不支持的未决处置结论")
    normalized_evidence = str(evidence_sha256 or "").strip().lower()
    if not _HEX_64_RE.fullmatch(normalized_evidence):
        raise ValueError("人工处置必须提供脱敏证据 SHA-256")
    if actor != "local_admin":
        raise ValueError("人工处置只能由本地管理员登记")

    with _WRITE_LOCK:
        integrity = verify_chain()
        if not integrity.get("ok"):
            raise RuntimeError("审计账本完整性异常，人工处置已阻断")
        status = reconciliation_status(
            limit=10000,
            database_key=expected_database_key,
        )
        target = next((
            item for item in status.get("items", [])
            if item.get("sequence") == pending_sequence
        ), None)
        if target is None:
            raise KeyError("未决事件不存在、已自动闭合或已人工处置")
        resolution = append_event(
            category="audit_control",
            action="resolve_pending",
            outcome="succeeded",
            summary="管理员已追加未决事件处置记录",
            risk="high",
            actor=actor,
            correlation_id=str(target["correlation_id"]),
            details={
                "pending_sequence": pending_sequence,
                "disposition": normalized_disposition,
                "evidence_sha256": normalized_evidence,
            },
            _database_ref_override=str(target["database_ref"]),
        )
    return {
        "sequence": resolution["sequence"],
        "event_id": resolution["event_id"],
        "event_hash": resolution["event_hash"],
        "pending_sequence": pending_sequence,
        "disposition": normalized_disposition,
        "evidence_sha256": normalized_evidence,
        "warning": "人工处置是追加式管理员判断，不等于数据库与审计账本之间的原子事实。",
    }


def retention_status(
    retention_days: Any = 365,
    *,
    now: Optional[datetime] = None,
) -> dict:
    """Assess the contiguous historical prefix eligible for external archive."""
    if isinstance(retention_days, bool):
        raise ValueError("retention_days 必须是 30–3650 的整数")
    try:
        days = int(retention_days)
    except (TypeError, ValueError) as exc:
        raise ValueError("retention_days 必须是 30–3650 的整数") from exc
    if days < 30 or days > 3650:
        raise ValueError("retention_days 必须是 30–3650 的整数")
    reference = now or datetime.now(timezone.utc).astimezone()
    if reference.tzinfo is None:
        raise ValueError("retention status reference time 必须包含时区")
    cutoff = reference - timedelta(days=days)
    integrity = verify_chain()
    if not integrity.get("ok"):
        return {
            "ok": False, "retention_days": days, "due_count": 0,
            "recommended_through_sequence": 0,
            "error": "审计账本完整性异常，无法评估归档前缀",
        }
    with _connect() as conn:
        rows = conn.execute(
            "SELECT sequence, occurred_at FROM audit_events ORDER BY sequence ASC"
        ).fetchall()
    through_sequence = 0
    through_time = ""
    for row in rows:
        try:
            occurred_at = datetime.fromisoformat(str(row["occurred_at"]))
        except ValueError:
            break
        if occurred_at.tzinfo is None or occurred_at > cutoff:
            break
        through_sequence = int(row["sequence"])
        through_time = str(row["occurred_at"])
    return {
        "ok": True,
        "retention_days": days,
        "cutoff_at": cutoff.isoformat(timespec="seconds"),
        "ledger_count": int(integrity.get("count") or 0),
        "due_count": through_sequence,
        "recommended_through_sequence": through_sequence,
        "recommended_through_time": through_time,
        "destructive_action": False,
        "note": "评估只建议非破坏性外部归档，不会删除或截断当前账本。",
    }


def _backup_dir() -> Path:
    return Path(_DATA_DIR) / "audit_backups"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _backup_id(value: Any) -> str:
    token = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-z_-]{12,64}", token):
        raise ValueError("审计备份标识无效")
    return token


def _backup_paths(backup_id: str) -> tuple[Path, Path]:
    token = _backup_id(backup_id)
    directory = _backup_dir().resolve()
    database = (directory / f"{token}.db").resolve()
    manifest = (directory / f"{token}.json").resolve()
    if database.parent != directory or manifest.parent != directory:
        raise ValueError("审计备份路径越界")
    return database, manifest


def create_backup(*, reason: str = "manual") -> dict:
    """Create and verify a consistent SQLite backup plus a SHA-256 manifest."""
    reason = _bounded_token(reason, field="backup reason", maximum=32) or "manual"
    if reason not in {"manual", "pre_restore"}:
        raise ValueError("不支持的审计备份原因")
    with _WRITE_LOCK:
        integrity = verify_chain()
        if not integrity.get("ok"):
            raise RuntimeError("审计账本完整性异常，拒绝创建备份")
        backup_id = (
            datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            + "_" + uuid.uuid4().hex[:12]
        )
        database_path, manifest_path = _backup_paths(backup_id)
        directory = database_path.parent
        directory.mkdir(parents=True, exist_ok=True)
        temp_database = directory / f".{backup_id}.db.tmp"
        temp_manifest = directory / f".{backup_id}.json.tmp"
        try:
            with closing(sqlite3.connect(str(_DB_PATH), timeout=10)) as source, \
                    closing(sqlite3.connect(str(temp_database), timeout=10)) as target:
                source.backup(target)
            backup_integrity = _verify_ledger_file(temp_database)
            if (
                not backup_integrity.get("ok")
                or backup_integrity.get("count") != integrity.get("count")
                or backup_integrity.get("head_hash") != integrity.get("head_hash")
            ):
                raise RuntimeError("审计备份与源账本不一致")
            database_sha256 = _file_sha256(temp_database)
            manifest = {
                "format": BACKUP_FORMAT,
                "schema_version": SCHEMA_VERSION,
                "backup_id": backup_id,
                "created_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                "reason": reason,
                "database_file": database_path.name,
                "database_sha256": database_sha256,
                "count": backup_integrity["count"],
                "head_hash": backup_integrity["head_hash"],
            }
            temp_manifest.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            os.replace(temp_database, database_path)
            os.replace(temp_manifest, manifest_path)
        finally:
            for temporary in (temp_database, temp_manifest):
                if temporary.exists():
                    temporary.unlink()
    return {**manifest, "valid": True}


def verify_backup(backup_id: str) -> dict:
    database_path, manifest_path = _backup_paths(backup_id)
    if not database_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError("审计备份不完整")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("审计备份清单无效") from exc
    required = {
        "format", "schema_version", "backup_id", "created_at", "reason",
        "database_file", "database_sha256", "count", "head_hash",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("审计备份清单结构无效")
    if (
        not _format_supported(manifest["format"], BACKUP_FORMAT)
        or manifest["schema_version"] != SCHEMA_VERSION
        or manifest["backup_id"] != _backup_id(backup_id)
        or manifest["database_file"] != database_path.name
        or manifest["reason"] not in {"manual", "pre_restore"}
        or not isinstance(manifest["count"], int)
        or manifest["count"] < 0
        or not _HEX_64_RE.fullmatch(str(manifest["head_hash"]))
        or not _HEX_64_RE.fullmatch(str(manifest["database_sha256"]))
    ):
        raise ValueError("审计备份清单内容无效")
    if _file_sha256(database_path) != manifest["database_sha256"]:
        raise RuntimeError("审计备份文件哈希不匹配")
    integrity = _verify_ledger_file(database_path)
    if (
        not integrity.get("ok")
        or integrity.get("count") != manifest["count"]
        or integrity.get("head_hash") != manifest["head_hash"]
    ):
        raise RuntimeError("审计备份事件链校验失败")
    return {**manifest, "valid": True, "verified_at": integrity["verified_at"]}


def _external_backup_payload(manifest: dict) -> dict:
    return {
        key: manifest[key]
        for key in (
            "format", "schema_version", "bundle_version", "bundle_id", "created_at",
            "source_backup_id", "source_created_at", "source_reason",
            "database_file", "database_sha256", "event_count", "head_hash",
        )
    }


def _external_backup_payload_sha256(manifest: dict) -> str:
    canonical = json.dumps(
        _external_backup_payload(manifest),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _external_backup_directory(output_dir: Any) -> Path:
    raw = str(output_dir or "").strip()
    if not raw:
        raise ValueError("外部审计备份输出目录不能为空")
    directory = Path(raw).expanduser().resolve()
    managed = Path(_DATA_DIR).resolve()
    if directory == managed or managed in directory.parents:
        raise ValueError("外部审计备份不能写入审计账本受管目录")
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("外部审计备份输出位置不是目录")
    return directory


def _validate_external_backup_manifest(manifest: Any) -> dict:
    required = {
        "format", "schema_version", "bundle_version", "bundle_id", "created_at",
        "source_backup_id", "source_created_at", "source_reason", "database_file",
        "database_sha256", "event_count", "head_hash", "payload_sha256",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("外部审计备份清单结构无效")
    try:
        created_at = datetime.fromisoformat(str(manifest.get("created_at") or ""))
        source_created_at = datetime.fromisoformat(
            str(manifest.get("source_created_at") or "")
        )
    except ValueError as exc:
        raise ValueError("外部审计备份清单时间无效") from exc
    if (
        not _format_supported(manifest.get("format"), EXTERNAL_BACKUP_FORMAT)
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("bundle_version") != 1
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", str(manifest.get("bundle_id")))
        or created_at.tzinfo is None
        or source_created_at.tzinfo is None
        or _backup_id(manifest.get("source_backup_id")) != manifest.get("source_backup_id")
        or manifest.get("source_reason") not in {"manual", "pre_restore"}
        or manifest.get("database_file") != "audit.db"
        or not isinstance(manifest.get("event_count"), int)
        or manifest.get("event_count") < 0
        or not _HEX_64_RE.fullmatch(str(manifest.get("database_sha256")))
        or not _HEX_64_RE.fullmatch(str(manifest.get("head_hash")))
        or not _HEX_64_RE.fullmatch(str(manifest.get("payload_sha256")))
    ):
        raise ValueError("外部审计备份清单内容无效")
    if _external_backup_payload_sha256(manifest) != manifest["payload_sha256"]:
        raise RuntimeError("外部审计备份清单载荷哈希不匹配")
    return manifest


@contextmanager
def _materialize_external_backup(bundle_file: Any):
    """Yield a verified external backup database in a temporary directory."""
    path = Path(str(bundle_file or "").strip()).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("外部审计备份包不存在")
    try:
        archive = zipfile.ZipFile(path, "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise ValueError("外部审计备份包不是有效 ZIP") from exc
    with archive:
        infos = archive.infolist()
        if len(infos) != 2 or {info.filename for info in infos} != {
            "manifest.json", "audit.db",
        }:
            raise ValueError("外部审计备份包条目结构无效")
        by_name = {info.filename: info for info in infos}
        manifest_info = by_name["manifest.json"]
        database_info = by_name["audit.db"]
        for info in infos:
            if (
                info.is_dir()
                or info.flag_bits & 0x1
                or info.compress_type != zipfile.ZIP_STORED
                or info.compress_size != info.file_size
            ):
                raise ValueError("外部审计备份包只允许未加密、未压缩的普通文件")
        if manifest_info.file_size <= 0 or manifest_info.file_size > 64 * 1024:
            raise ValueError("外部审计备份清单大小无效")
        if database_info.file_size <= 0 or database_info.file_size > path.stat().st_size:
            raise ValueError("外部审计备份数据库大小无效")
        try:
            manifest = json.loads(archive.read(manifest_info).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("外部审计备份清单无效") from exc
        manifest = _validate_external_backup_manifest(manifest)
        with tempfile.TemporaryDirectory(prefix="dbquill-audit-external-") as temp_dir:
            materialized = Path(temp_dir) / "audit.db"
            with archive.open(database_info, "r") as source, open(materialized, "wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            if _file_sha256(materialized) != manifest["database_sha256"]:
                raise RuntimeError("外部审计备份数据库哈希不匹配")
            integrity = _verify_ledger_file(materialized)
            if (
                not integrity.get("ok")
                or integrity.get("count") != manifest["event_count"]
                or integrity.get("head_hash") != manifest["head_hash"]
            ):
                raise RuntimeError("外部审计备份事件链校验失败")
            yield manifest, materialized, path, integrity


def create_external_backup(backup_id: str, output_dir: Any) -> dict:
    """Atomically export a verified local backup as one portable ZIP bundle."""
    verified = verify_backup(backup_id)
    source_database, _ = _backup_paths(backup_id)
    directory = _external_backup_directory(output_dir)
    bundle_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "_" + uuid.uuid4().hex[:12]
    )
    manifest = {
        "format": EXTERNAL_BACKUP_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "bundle_version": 1,
        "bundle_id": bundle_id,
        "created_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "source_backup_id": verified["backup_id"],
        "source_created_at": verified["created_at"],
        "source_reason": verified["reason"],
        "database_file": "audit.db",
        "database_sha256": verified["database_sha256"],
        "event_count": verified["count"],
        "head_hash": verified["head_hash"],
    }
    manifest["payload_sha256"] = _external_backup_payload_sha256(manifest)
    filename = f"dbquill-audit-external-backup-{bundle_id}.zip"
    target = (directory / filename).resolve()
    temporary = (directory / f".{filename}.{uuid.uuid4().hex}.tmp").resolve()
    if target.parent != directory or temporary.parent != directory:
        raise ValueError("外部审计备份输出路径越界")
    try:
        with zipfile.ZipFile(
            temporary, "w", compression=zipfile.ZIP_STORED, allowZip64=True,
        ) as archive:
            archive.writestr(
                "manifest.json",
                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                compress_type=zipfile.ZIP_STORED,
            )
            archive.write(source_database, "audit.db", compress_type=zipfile.ZIP_STORED)
        os.replace(temporary, target)
        try:
            return verify_external_backup(target)
        except Exception:
            if target.exists():
                target.unlink()
            raise
    finally:
        if temporary.exists():
            temporary.unlink()


def verify_external_backup(bundle_file: Any) -> dict:
    """Verify a portable backup without depending on the current live ledger."""
    with _materialize_external_backup(bundle_file) as (
        manifest, _database, path, integrity,
    ):
        return {
            **manifest,
            "bundle_file": str(path),
            "bundle_sha256": _file_sha256(path),
            "valid": True,
            "verified_at": integrity["verified_at"],
            "independent_of_current_ledger": True,
            "destructive_action": False,
            "warning": "备份包没有数字签名；只有存入独立受保护位置才具备异地恢复价值。",
        }


def _managed_control_path(filename: str) -> Path:
    managed = Path(_DATA_DIR).resolve()
    path = (managed / filename).resolve()
    if path.parent != managed:
        raise RuntimeError("审计目标控制文件路径越界")
    return path


def _external_target_config_path() -> Path:
    return _managed_control_path("audit_external_target.json")


def _external_target_state_path() -> Path:
    return _managed_control_path("audit_external_target_state.json")


def _external_target_history_path() -> Path:
    return _managed_control_path("audit_external_target_history.jsonl")


def _atomic_write_json(path: Path, payload: dict) -> None:
    parent = path.resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
    temporary = (parent / f".{path.name}.{uuid.uuid4().hex}.tmp").resolve()
    if temporary.parent != parent:
        raise RuntimeError("原子 JSON 临时路径越界")
    try:
        with open(temporary, "x", encoding="utf-8", newline="") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write(os.linesep)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _target_payload_sha256(payload: dict, fields: tuple[str, ...]) -> str:
    canonical = json.dumps(
        {key: payload[key] for key in fields},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_TARGET_CONFIG_FIELDS = (
    "format", "config_version", "target_id", "configured_at", "target_kind",
    "directory",
)
_TARGET_STATE_FIELDS = (
    "format", "state_version", "target_id", "synchronized_at", "backup_id",
    "bundle_id", "bundle_file", "bundle_sha256", "event_count", "head_hash",
)
_TARGET_ATTEMPT_FIELDS = (
    "format", "attempt_version", "attempt_id", "attempted_at", "target_id",
    "outcome", "backup_id", "bundle_id", "bundle_sha256", "event_count",
    "head_hash", "error_type", "error_sha256",
)


def _external_target_directory(value: Any, *, require_exists: bool) -> Path:
    raw = str(value or "").strip()
    if not raw or len(raw) > 4096 or any(ord(char) < 32 for char in raw):
        raise ValueError("外部审计备份目标目录无效")
    source = Path(raw).expanduser()
    if not source.is_absolute():
        raise ValueError("外部审计备份目标必须是绝对目录")
    directory = source.resolve()
    managed = Path(_DATA_DIR).resolve()
    if (
        directory == managed
        or managed in directory.parents
        or directory in managed.parents
    ):
        raise ValueError("外部审计备份目标必须位于审计受管目录之外")
    if require_exists and not directory.is_dir():
        raise FileNotFoundError("外部审计备份目标目录不可用")
    if directory.exists() and not directory.is_dir():
        raise ValueError("外部审计备份目标不是目录")
    return directory


def _validate_external_target_config(config: Any) -> dict:
    required = set(_TARGET_CONFIG_FIELDS) | {"payload_sha256"}
    if not isinstance(config, dict) or set(config) != required:
        raise ValueError("外部审计备份目标配置结构无效")
    try:
        configured_at = datetime.fromisoformat(str(config.get("configured_at") or ""))
    except ValueError as exc:
        raise ValueError("外部审计备份目标配置时间无效") from exc
    if (
        not _format_supported(config.get("format"), EXTERNAL_TARGET_CONFIG_FORMAT)
        or config.get("config_version") != 1
        or not re.fullmatch(r"[0-9a-f]{32}", str(config.get("target_id")))
        or configured_at.tzinfo is None
        or config.get("target_kind") != "filesystem_directory"
        or not _HEX_64_RE.fullmatch(str(config.get("payload_sha256")))
    ):
        raise ValueError("外部审计备份目标配置内容无效")
    directory = _external_target_directory(config.get("directory"), require_exists=False)
    if str(directory) != config["directory"]:
        raise ValueError("外部审计备份目标目录不是规范绝对路径")
    if _target_payload_sha256(config, _TARGET_CONFIG_FIELDS) != config["payload_sha256"]:
        raise RuntimeError("外部审计备份目标配置载荷哈希不匹配")
    return dict(config)


def _load_external_target_config(*, require_available: bool) -> dict:
    path = _external_target_config_path()
    if not path.is_file():
        raise FileNotFoundError("尚未配置外部审计备份目标")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("外部审计备份目标配置无法读取") from exc
    config = _validate_external_target_config(config)
    if require_available:
        _external_target_directory(config["directory"], require_exists=True)
    return config


def configure_external_backup_target(
    directory: Any,
    *,
    expected_current_target_id: Optional[str] = None,
    confirmation: str,
) -> dict:
    """Configure one explicit filesystem target; never moves or deletes backups."""
    if confirmation != "CONFIGURE_AUDIT_BACKUP_TARGET":
        raise ValueError("缺少外部审计备份目标配置确认短语")
    target_directory = _external_target_directory(directory, require_exists=True)
    expected = str(expected_current_target_id or "").strip().lower()
    if expected and not re.fullmatch(r"[0-9a-f]{32}", expected):
        raise ValueError("当前外部审计备份目标标识无效")
    with _WRITE_LOCK:
        path = _external_target_config_path()
        existing = None
        if path.exists():
            existing = _load_external_target_config(require_available=False)
            if not expected:
                raise RuntimeError("外部审计备份目标已配置；替换时必须绑定当前 target ID")
            if existing["target_id"] != expected:
                raise RuntimeError("外部审计备份目标已变化，拒绝替换")
        elif expected:
            raise RuntimeError("当前没有可匹配的外部审计备份目标")
        config = {
            "format": EXTERNAL_TARGET_CONFIG_FORMAT,
            "config_version": 1,
            "target_id": uuid.uuid4().hex,
            "configured_at": datetime.now(timezone.utc).astimezone().isoformat(
                timespec="seconds"
            ),
            "target_kind": "filesystem_directory",
            "directory": str(target_directory),
        }
        config["payload_sha256"] = _target_payload_sha256(
            config, _TARGET_CONFIG_FIELDS,
        )
        _atomic_write_json(path, config)
    return {
        **config,
        "replaced_target_id": existing["target_id"] if existing else None,
        "destructive_action": False,
        "warning": "配置只声明目标，不证明它位于独立设备或受保护信任域。",
    }


def _validate_external_target_state(state: Any) -> dict:
    required = set(_TARGET_STATE_FIELDS) | {"payload_sha256"}
    if not isinstance(state, dict) or set(state) != required:
        raise ValueError("外部审计备份目标状态结构无效")
    try:
        synchronized_at = datetime.fromisoformat(str(state.get("synchronized_at") or ""))
    except ValueError as exc:
        raise ValueError("外部审计备份目标状态时间无效") from exc
    bundle_id = str(state.get("bundle_id") or "")
    expected_filenames = {
        f"dbquill-audit-external-backup-{bundle_id}.zip",
        f"dbagent-audit-external-backup-{bundle_id}.zip",
    }
    if (
        not _format_supported(state.get("format"), EXTERNAL_TARGET_STATE_FORMAT)
        or state.get("state_version") != 1
        or not re.fullmatch(r"[0-9a-f]{32}", str(state.get("target_id")))
        or synchronized_at.tzinfo is None
        or _backup_id(state.get("backup_id")) != state.get("backup_id")
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", bundle_id)
        or state.get("bundle_file") not in expected_filenames
        or not _HEX_64_RE.fullmatch(str(state.get("bundle_sha256")))
        or not isinstance(state.get("event_count"), int)
        or state["event_count"] < 0
        or not _HEX_64_RE.fullmatch(str(state.get("head_hash")))
        or not _HEX_64_RE.fullmatch(str(state.get("payload_sha256")))
    ):
        raise ValueError("外部审计备份目标状态内容无效")
    if _target_payload_sha256(state, _TARGET_STATE_FIELDS) != state["payload_sha256"]:
        raise RuntimeError("外部审计备份目标状态载荷哈希不匹配")
    return dict(state)


def _load_external_target_state() -> Optional[dict]:
    path = _external_target_state_path()
    if not path.exists():
        return None
    if not path.is_file():
        raise RuntimeError("外部审计备份目标状态不是普通文件")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("外部审计备份目标状态无法读取") from exc
    return _validate_external_target_state(state)


def _validate_external_target_attempt(attempt: Any) -> dict:
    required = set(_TARGET_ATTEMPT_FIELDS) | {"payload_sha256"}
    if not isinstance(attempt, dict) or set(attempt) != required:
        raise ValueError("外部审计备份同步尝试结构无效")
    try:
        attempted_at = datetime.fromisoformat(str(attempt.get("attempted_at") or ""))
    except ValueError as exc:
        raise ValueError("外部审计备份同步尝试时间无效") from exc
    outcome = attempt.get("outcome")
    if (
        not _format_supported(attempt.get("format"), EXTERNAL_TARGET_ATTEMPT_FORMAT)
        or attempt.get("attempt_version") != 1
        or not re.fullmatch(r"[0-9a-f]{32}", str(attempt.get("attempt_id")))
        or attempted_at.tzinfo is None
        or not re.fullmatch(r"[0-9a-f]{32}", str(attempt.get("target_id")))
        or outcome not in {"succeeded", "failed"}
        or not _HEX_64_RE.fullmatch(str(attempt.get("payload_sha256")))
    ):
        raise ValueError("外部审计备份同步尝试内容无效")
    backup_fields = (
        attempt.get("backup_id"), attempt.get("bundle_id"),
        attempt.get("bundle_sha256"), attempt.get("event_count"),
        attempt.get("head_hash"),
    )
    if any(value is not None for value in backup_fields):
        bundle_id = str(attempt.get("bundle_id") or "")
        if (
            any(value is None for value in backup_fields)
            or _backup_id(attempt.get("backup_id")) != attempt.get("backup_id")
            or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", bundle_id)
            or not _HEX_64_RE.fullmatch(str(attempt.get("bundle_sha256")))
            or not isinstance(attempt.get("event_count"), int)
            or attempt["event_count"] < 0
            or not _HEX_64_RE.fullmatch(str(attempt.get("head_hash")))
        ):
            raise ValueError("外部审计备份同步尝试包摘要无效")
    if outcome == "succeeded":
        if any(value is None for value in backup_fields):
            raise ValueError("成功同步尝试缺少包摘要")
        if attempt.get("error_type") is not None or attempt.get("error_sha256") is not None:
            raise ValueError("成功同步尝试不能包含错误")
    else:
        if (
            not isinstance(attempt.get("error_type"), str)
            or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]{0,63}", attempt["error_type"],
            )
            or not _HEX_64_RE.fullmatch(str(attempt.get("error_sha256")))
        ):
            raise ValueError("失败同步尝试错误摘要无效")
    if _target_payload_sha256(attempt, _TARGET_ATTEMPT_FIELDS) != attempt["payload_sha256"]:
        raise RuntimeError("外部审计备份同步尝试载荷哈希不匹配")
    return dict(attempt)


def _append_external_target_attempt(
    config: dict,
    *,
    outcome: str,
    external: Optional[dict] = None,
    error: Optional[BaseException] = None,
) -> dict:
    if outcome not in {"succeeded", "failed"}:
        raise ValueError("外部审计备份同步尝试结果无效")
    if outcome == "succeeded" and external is None:
        raise ValueError("成功同步尝试缺少外部包")
    if outcome == "failed" and error is None:
        raise ValueError("失败同步尝试缺少错误")
    attempt = {
        "format": EXTERNAL_TARGET_ATTEMPT_FORMAT,
        "attempt_version": 1,
        "attempt_id": uuid.uuid4().hex,
        "attempted_at": datetime.now(timezone.utc).astimezone().isoformat(
            timespec="seconds"
        ),
        "target_id": config["target_id"],
        "outcome": outcome,
        "backup_id": external["source_backup_id"] if external else None,
        "bundle_id": external["bundle_id"] if external else None,
        "bundle_sha256": external["bundle_sha256"] if external else None,
        "event_count": external["event_count"] if external else None,
        "head_hash": external["head_hash"] if external else None,
        "error_type": type(error).__name__[:64] if error else None,
        "error_sha256": sha256_text(str(error)) if error else None,
    }
    attempt["payload_sha256"] = _target_payload_sha256(
        attempt, _TARGET_ATTEMPT_FIELDS,
    )
    attempt = _validate_external_target_attempt(attempt)
    path = _external_target_history_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        attempt, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ) + os.linesep
    with open(path, "a", encoding="utf-8", newline="") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return attempt


def external_backup_target_history(
    *, limit: int = 50, current_target_only: bool = True,
) -> dict:
    limit = max(1, min(int(limit), 200))
    path = _external_target_history_path()
    if not path.exists():
        records = []
    else:
        if not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
            raise RuntimeError("外部审计备份同步历史文件无效或超限")
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError("外部审计备份同步历史无法读取") from exc
        records = []
        seen = set()
        for line in lines:
            if not line.strip():
                raise ValueError("外部审计备份同步历史包含空记录")
            try:
                record = _validate_external_target_attempt(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError("外部审计备份同步历史 JSON 无效") from exc
            if record["attempt_id"] in seen:
                raise RuntimeError("外部审计备份同步历史包含重复 attempt ID")
            seen.add(record["attempt_id"])
            records.append(record)
    target_id = None
    if current_target_only:
        config = _load_external_target_config(require_available=False)
        target_id = config["target_id"]
        records = [item for item in records if item["target_id"] == target_id]
    consecutive_failures = 0
    for item in reversed(records):
        if item["outcome"] != "failed":
            break
        consecutive_failures += 1
    return {
        "target_id": target_id,
        "total_matching": len(records),
        "consecutive_failures": consecutive_failures,
        "items": list(reversed(records[-limit:])),
        "destructive_action": False,
    }


def external_backup_target_status() -> dict:
    try:
        config = _load_external_target_config(require_available=False)
    except FileNotFoundError:
        return {
            "configured": False,
            "available": False,
            "last_success": None,
            "destructive_action": False,
        }
    directory = _external_target_directory(config["directory"], require_exists=False)
    available = directory.is_dir()
    state = _load_external_target_state()
    last_success = state if state and state["target_id"] == config["target_id"] else None
    history = external_backup_target_history(limit=1, current_target_only=True)
    return {
        "configured": True,
        "available": available,
        "target_id": config["target_id"],
        "target_kind": config["target_kind"],
        "directory": config["directory"],
        "configured_at": config["configured_at"],
        "free_bytes": shutil.disk_usage(directory).free if available else None,
        "last_success": last_success,
        "last_attempt": history["items"][0] if history["items"] else None,
        "consecutive_failures": history["consecutive_failures"],
        "previous_target_state_retained": state is not None and last_success is None,
        "destructive_action": False,
        "warning": "目录可用不等于独立介质、不可变存储或灾备 SLA 已验证。",
    }


def probe_external_backup_target(*, confirmation: str) -> dict:
    if confirmation != "PROBE_AUDIT_BACKUP_TARGET":
        raise ValueError("缺少外部审计备份目标探测确认短语")
    config = _load_external_target_config(require_available=True)
    directory = _external_target_directory(config["directory"], require_exists=True)
    probe_id = uuid.uuid4().hex
    temporary = (directory / f".dbquill-audit-probe-{probe_id}.tmp").resolve()
    promoted = (directory / f".dbquill-audit-probe-{probe_id}.check").resolve()
    if temporary.parent != directory or promoted.parent != directory:
        raise ValueError("外部审计备份目标探测路径越界")
    payload = os.urandom(64)
    cleanup_ok = False
    try:
        with open(temporary, "xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, promoted)
        if promoted.read_bytes() != payload:
            raise RuntimeError("外部审计备份目标探测读回不一致")
    finally:
        for candidate in (temporary, promoted):
            if candidate.exists():
                candidate.unlink()
        cleanup_ok = not temporary.exists() and not promoted.exists()
    if not cleanup_ok:
        raise RuntimeError("外部审计备份目标探测文件未清理")
    usage = shutil.disk_usage(directory)
    return {
        "target_id": config["target_id"],
        "target_kind": config["target_kind"],
        "tested_at": datetime.now(timezone.utc).astimezone().isoformat(
            timespec="seconds"
        ),
        "write_read": True,
        "atomic_replace": True,
        "temporary_cleanup": True,
        "free_bytes": usage.free,
        "temporary_mutation": True,
        "destructive_action": False,
    }


def synchronize_external_backup_target(*, confirmation: str) -> dict:
    if confirmation != "SYNC_AUDIT_BACKUP_TARGET":
        raise ValueError("缺少外部审计备份目标同步确认短语")
    config = None
    external = None
    recording_attempt = False
    try:
        with _WRITE_LOCK:
            config = _load_external_target_config(require_available=True)
            directory = _external_target_directory(
                config["directory"], require_exists=True,
            )
            backup = create_backup(reason="manual")
            external = create_external_backup(backup["backup_id"], directory)
            bundle_path = Path(external["bundle_file"]).resolve()
            if bundle_path.parent != directory:
                raise RuntimeError("同步后的外部审计备份路径不属于当前目标")
            current_config = _load_external_target_config(require_available=True)
            if (
                current_config["target_id"] != config["target_id"]
                or current_config["directory"] != config["directory"]
            ):
                raise RuntimeError("同步期间外部审计备份目标已变化；保留外部包但不登记成功")
            state = {
                "format": EXTERNAL_TARGET_STATE_FORMAT,
                "state_version": 1,
                "target_id": config["target_id"],
                "synchronized_at": datetime.now(timezone.utc).astimezone().isoformat(
                    timespec="seconds"
                ),
                "backup_id": external["source_backup_id"],
                "bundle_id": external["bundle_id"],
                "bundle_file": bundle_path.name,
                "bundle_sha256": external["bundle_sha256"],
                "event_count": external["event_count"],
                "head_hash": external["head_hash"],
            }
            state["payload_sha256"] = _target_payload_sha256(
                state, _TARGET_STATE_FIELDS,
            )
            _atomic_write_json(_external_target_state_path(), state)
            recording_attempt = True
            attempt = _append_external_target_attempt(
                config, outcome="succeeded", external=external,
            )
    except Exception as exc:
        if config is not None and not recording_attempt:
            try:
                _append_external_target_attempt(
                    config, outcome="failed", external=external, error=exc,
                )
            except Exception as history_exc:
                raise RuntimeError("目标同步失败且脱敏尝试历史写入失败") from history_exc
        raise
    return {
        "target_id": config["target_id"],
        "backup": external,
        "last_success": state,
        "attempt": attempt,
        "deleted_existing_files": False,
        "destructive_action": False,
        "warning": "同步不会清理目标中的任何既有备份；保留和删除需要单独授权。",
    }


def verify_latest_external_target_backup() -> dict:
    config = _load_external_target_config(require_available=True)
    state = _load_external_target_state()
    if state is None or state["target_id"] != config["target_id"]:
        raise FileNotFoundError("当前外部审计备份目标尚无成功同步记录")
    directory = _external_target_directory(config["directory"], require_exists=True)
    bundle_path = (directory / state["bundle_file"]).resolve()
    if bundle_path.parent != directory:
        raise RuntimeError("外部审计备份目标状态路径越界")
    verified = verify_external_backup(bundle_path)
    if (
        verified["bundle_id"] != state["bundle_id"]
        or verified["source_backup_id"] != state["backup_id"]
        or verified["bundle_sha256"] != state["bundle_sha256"]
        or verified["event_count"] != state["event_count"]
        or verified["head_hash"] != state["head_hash"]
    ):
        raise RuntimeError("外部审计备份目标最新副本与成功状态不一致")
    return {
        "target_id": config["target_id"],
        "last_success": state,
        "backup": verified,
        "valid": True,
        "destructive_action": False,
    }


def check_external_backup_target_health(*, max_age_hours: int = 25) -> dict:
    if isinstance(max_age_hours, bool):
        raise ValueError("外部审计备份健康时限无效")
    max_age_hours = int(max_age_hours)
    if not 1 <= max_age_hours <= 8760:
        raise ValueError("外部审计备份健康时限必须为 1–8760 小时")
    history = external_backup_target_history(limit=1, current_target_only=True)
    if not history["items"]:
        raise RuntimeError("当前外部审计备份目标没有同步尝试历史")
    last_attempt = history["items"][0]
    if last_attempt["outcome"] != "succeeded":
        raise RuntimeError("当前外部审计备份目标最近一次同步失败")
    verified = verify_latest_external_target_backup()
    synchronized_at = datetime.fromisoformat(
        verified["last_success"]["synchronized_at"]
    )
    now = datetime.now(timezone.utc).astimezone()
    age_seconds = (now - synchronized_at).total_seconds()
    if age_seconds < -300:
        raise RuntimeError("外部审计备份最近成功时间位于未来")
    if age_seconds > max_age_hours * 3600:
        raise RuntimeError("外部审计备份最近成功已经超过健康时限")
    return {
        "healthy": True,
        "target_id": verified["target_id"],
        "max_age_hours": max_age_hours,
        "age_seconds": max(0, int(age_seconds)),
        "last_attempt": last_attempt,
        "last_success": verified["last_success"],
        "bundle_valid": True,
        "destructive_action": False,
    }


_LEDGER_FILE_SPECS = (
    ("database", "ledger.db", ""),
    ("wal", "ledger.db-wal", "-wal"),
    ("shm", "ledger.db-shm", "-shm"),
)


def _managed_ledger_files() -> list[tuple[str, str, Path]]:
    target = Path(_DB_PATH).resolve()
    managed = Path(_DATA_DIR).resolve()
    if target.parent != managed:
        raise RuntimeError("审计账本路径不在受管目录内")
    files = []
    for role, archive_file, suffix in _LEDGER_FILE_SPECS:
        path = Path(str(target) + suffix).resolve()
        if path.parent != managed:
            raise RuntimeError("审计账本附属文件路径越界")
        files.append((role, archive_file, path))
    return files


def _snapshot_current_ledger_files() -> list[dict]:
    snapshot = []
    for role, archive_file, path in _managed_ledger_files():
        if not path.exists():
            if role == "database":
                raise FileNotFoundError("当前审计账本文件不存在")
            continue
        if not path.is_file():
            raise RuntimeError("审计账本现场包含非普通文件")
        size_before = path.stat().st_size
        digest = _file_sha256(path)
        size_after = path.stat().st_size
        if size_before != size_after:
            raise RuntimeError("审计账本文件在读取期间发生变化；请完全退出桌面应用后重试")
        snapshot.append({
            "role": role,
            "archive_file": archive_file,
            "size": size_after,
            "sha256": digest,
        })
    return snapshot


def _normalized_ledger_integrity(path: Path) -> dict:
    try:
        raw = _verify_ledger_file(path)
    except Exception as exc:
        return {
            "integrity_ok": False,
            "integrity_count": None,
            "integrity_head_hash": None,
            "integrity_error_sequence": None,
            "integrity_error_type": type(exc).__name__[:64],
            "integrity_error_sha256": sha256_text(str(exc)),
        }
    if raw.get("ok"):
        return {
            "integrity_ok": True,
            "integrity_count": int(raw.get("count") or 0),
            "integrity_head_hash": str(raw.get("head_hash")),
            "integrity_error_sequence": None,
            "integrity_error_type": "",
            "integrity_error_sha256": None,
        }
    error_sequence = raw.get("error_sequence")
    return {
        "integrity_ok": False,
        "integrity_count": int(raw.get("count") or 0),
        "integrity_head_hash": None,
        "integrity_error_sequence": (
            int(error_sequence) if isinstance(error_sequence, int) else None
        ),
        "integrity_error_type": "chain_validation",
        "integrity_error_sha256": sha256_text(raw.get("error") or "unknown"),
    }


def _ledger_assessment_payload(assessment: dict) -> dict:
    return {
        key: assessment[key]
        for key in (
            "format", "schema_version", "assessment_version", "files",
            "integrity_ok", "integrity_count", "integrity_head_hash",
            "integrity_error_sequence", "integrity_error_type",
            "integrity_error_sha256",
        )
    }


def _ledger_assessment_token(assessment: dict) -> str:
    canonical = json.dumps(
        _ledger_assessment_payload(assessment),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def assess_current_ledger() -> dict:
    """Fingerprint the live ledger without changing or repairing it."""
    with _WRITE_LOCK:
        before = _snapshot_current_ledger_files()
        paths = {
            role: path for role, _archive_file, path in _managed_ledger_files()
        }
        # Opening a WAL database read-only can still create -wal/-shm files. Verify a
        # byte-for-byte temporary snapshot so the live evidence is physically untouched.
        with tempfile.TemporaryDirectory(prefix="dbquill-audit-assessment-") as temp_dir:
            isolated_database = Path(temp_dir) / "ledger.db"
            for item in before:
                shutil.copy2(paths[item["role"]], Path(temp_dir) / item["archive_file"])
            integrity = _normalized_ledger_integrity(isolated_database)
        after = _snapshot_current_ledger_files()
        if before != after:
            raise RuntimeError("审计账本现场在评估期间发生变化；请完全退出桌面应用后重试")
        assessment = {
            "format": LEDGER_ASSESSMENT_FORMAT,
            "schema_version": SCHEMA_VERSION,
            "assessment_version": 1,
            "files": before,
            **integrity,
        }
        return {
            **assessment,
            "assessment_token": _ledger_assessment_token(assessment),
            "assessed_at": datetime.now(timezone.utc).astimezone().isoformat(
                timespec="seconds"
            ),
            "requires_evidence_quarantine": not integrity["integrity_ok"],
            "destructive_action": False,
            "warning": "评估令牌只绑定当前文件现场；任何文件变化都会使令牌失效。",
        }


def _corrupt_evidence_payload(manifest: dict) -> dict:
    return {
        key: manifest[key]
        for key in (
            "format", "schema_version", "bundle_version", "evidence_id",
            "created_at", "assessment_token", "files", "integrity_ok",
            "integrity_count", "integrity_head_hash", "integrity_error_sequence",
            "integrity_error_type", "integrity_error_sha256",
        )
    }


def _corrupt_evidence_payload_sha256(manifest: dict) -> str:
    canonical = json.dumps(
        _corrupt_evidence_payload(manifest),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_evidence_files(files: Any) -> list[dict]:
    if not isinstance(files, list) or not 1 <= len(files) <= 3:
        raise ValueError("损坏账本证据文件清单无效")
    expected = {role: archive for role, archive, _suffix in _LEDGER_FILE_SPECS}
    normalized = []
    seen = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {
            "role", "archive_file", "size", "sha256",
        }:
            raise ValueError("损坏账本证据文件条目结构无效")
        role = item.get("role")
        if (
            role not in expected
            or role in seen
            or item.get("archive_file") != expected[role]
            or not isinstance(item.get("size"), int)
            or item["size"] < 0
            or not _HEX_64_RE.fullmatch(str(item.get("sha256")))
        ):
            raise ValueError("损坏账本证据文件条目内容无效")
        seen.add(role)
        normalized.append(dict(item))
    if not normalized or normalized[0]["role"] != "database" or "database" not in seen:
        raise ValueError("损坏账本证据缺少数据库主文件")
    expected_order = [role for role, _archive, _suffix in _LEDGER_FILE_SPECS if role in seen]
    if [item["role"] for item in normalized] != expected_order:
        raise ValueError("损坏账本证据文件顺序无效")
    return normalized


def _validate_corrupt_evidence_manifest(manifest: Any) -> dict:
    required = {
        "format", "schema_version", "bundle_version", "evidence_id", "created_at",
        "assessment_token", "files", "integrity_ok", "integrity_count",
        "integrity_head_hash", "integrity_error_sequence", "integrity_error_type",
        "integrity_error_sha256", "payload_sha256",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("损坏账本证据清单结构无效")
    try:
        created_at = datetime.fromisoformat(str(manifest.get("created_at") or ""))
    except ValueError as exc:
        raise ValueError("损坏账本证据清单时间无效") from exc
    files = _validate_evidence_files(manifest.get("files"))
    error_sequence = manifest.get("integrity_error_sequence")
    error_type = manifest.get("integrity_error_type")
    if (
        not _format_supported(manifest.get("format"), CORRUPT_EVIDENCE_FORMAT)
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("bundle_version") != 1
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", str(manifest.get("evidence_id")))
        or created_at.tzinfo is None
        or not _HEX_64_RE.fullmatch(str(manifest.get("assessment_token")))
        or manifest.get("integrity_ok") is not False
        or (
            manifest.get("integrity_count") is not None
            and (
                not isinstance(manifest.get("integrity_count"), int)
                or manifest["integrity_count"] < 0
            )
        )
        or manifest.get("integrity_head_hash") is not None
        or (
            error_sequence is not None
            and (not isinstance(error_sequence, int) or error_sequence < 1)
        )
        or not isinstance(error_type, str)
        or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", error_type)
        or not _HEX_64_RE.fullmatch(str(manifest.get("integrity_error_sha256")))
        or not _HEX_64_RE.fullmatch(str(manifest.get("payload_sha256")))
    ):
        raise ValueError("损坏账本证据清单内容无效")
    normalized = dict(manifest)
    normalized["files"] = files
    assessment = {
        "format": LEDGER_ASSESSMENT_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "assessment_version": 1,
        "files": files,
        "integrity_ok": False,
        "integrity_count": manifest["integrity_count"],
        "integrity_head_hash": None,
        "integrity_error_sequence": error_sequence,
        "integrity_error_type": error_type,
        "integrity_error_sha256": manifest["integrity_error_sha256"],
    }
    if _ledger_assessment_token(assessment) != manifest["assessment_token"]:
        raise RuntimeError("损坏账本证据评估令牌不匹配")
    if _corrupt_evidence_payload_sha256(normalized) != manifest["payload_sha256"]:
        raise RuntimeError("损坏账本证据清单载荷哈希不匹配")
    return normalized


def _external_evidence_directory(output_dir: Any) -> Path:
    raw = str(output_dir or "").strip()
    if not raw:
        raise ValueError("损坏账本证据输出目录不能为空")
    directory = Path(raw).expanduser().resolve()
    managed = Path(_DATA_DIR).resolve()
    if directory == managed or managed in directory.parents:
        raise ValueError("损坏账本证据不能写入审计账本受管目录")
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("损坏账本证据输出位置不是目录")
    return directory


def create_corrupt_ledger_evidence(
    output_dir: Any,
    *,
    expected_assessment_token: str,
    confirmation: str,
) -> dict:
    """Preserve a stable corrupt ledger snapshot in a portable ZIP."""
    if confirmation != "PRESERVE_CORRUPT_AUDIT_LEDGER":
        raise ValueError("缺少损坏审计账本证据保全确认短语")
    expected = str(expected_assessment_token or "").strip().lower()
    if not _HEX_64_RE.fullmatch(expected):
        raise ValueError("审计账本现场评估令牌无效")
    with _WRITE_LOCK:
        assessment = assess_current_ledger()
        if assessment["integrity_ok"]:
            raise RuntimeError("当前审计账本完整性正常，拒绝创建损坏现场证据")
        if assessment["assessment_token"] != expected:
            raise RuntimeError("当前审计账本现场已变化，拒绝证据保全")
        directory = _external_evidence_directory(output_dir)
        evidence_id = (
            datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            + "_" + uuid.uuid4().hex[:12]
        )
        manifest = {
            "format": CORRUPT_EVIDENCE_FORMAT,
            "schema_version": SCHEMA_VERSION,
            "bundle_version": 1,
            "evidence_id": evidence_id,
            "created_at": datetime.now(timezone.utc).astimezone().isoformat(
                timespec="seconds"
            ),
            "assessment_token": expected,
            "files": assessment["files"],
            "integrity_ok": False,
            "integrity_count": assessment["integrity_count"],
            "integrity_head_hash": None,
            "integrity_error_sequence": assessment["integrity_error_sequence"],
            "integrity_error_type": assessment["integrity_error_type"],
            "integrity_error_sha256": assessment["integrity_error_sha256"],
        }
        manifest["payload_sha256"] = _corrupt_evidence_payload_sha256(manifest)
        filename = f"dbquill-audit-corrupt-evidence-{evidence_id}.zip"
        target = (directory / filename).resolve()
        temporary = (directory / f".{filename}.{uuid.uuid4().hex}.tmp").resolve()
        if target.parent != directory or temporary.parent != directory:
            raise ValueError("损坏账本证据输出路径越界")
        paths = {
            role: path for role, _archive_file, path in _managed_ledger_files()
        }
        try:
            with zipfile.ZipFile(
                temporary, "w", compression=zipfile.ZIP_STORED, allowZip64=True,
            ) as archive:
                archive.writestr(
                    "manifest.json",
                    json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                    compress_type=zipfile.ZIP_STORED,
                )
                for item in manifest["files"]:
                    archive.write(
                        paths[item["role"]], item["archive_file"],
                        compress_type=zipfile.ZIP_STORED,
                    )
            if assess_current_ledger()["assessment_token"] != expected:
                raise RuntimeError("证据复制期间审计账本现场发生变化")
            os.replace(temporary, target)
            try:
                return verify_corrupt_ledger_evidence(target)
            except Exception:
                if target.exists():
                    target.unlink()
                raise
        finally:
            if temporary.exists():
                temporary.unlink()


def verify_corrupt_ledger_evidence(evidence_file: Any) -> dict:
    """Verify corrupt-ledger evidence without consulting the live ledger."""
    path = Path(str(evidence_file or "").strip()).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("损坏账本证据包不存在")
    try:
        archive = zipfile.ZipFile(path, "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise ValueError("损坏账本证据包不是有效 ZIP") from exc
    with archive:
        infos = archive.infolist()
        manifest_infos = [info for info in infos if info.filename == "manifest.json"]
        if len(manifest_infos) != 1:
            raise ValueError("损坏账本证据包清单条目无效")
        manifest_info = manifest_infos[0]
        if manifest_info.file_size <= 0 or manifest_info.file_size > 64 * 1024:
            raise ValueError("损坏账本证据清单大小无效")
        try:
            manifest = json.loads(archive.read(manifest_info).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("损坏账本证据清单无效") from exc
        manifest = _validate_corrupt_evidence_manifest(manifest)
        expected_names = {"manifest.json"} | {
            item["archive_file"] for item in manifest["files"]
        }
        if len(infos) != len(expected_names) or {info.filename for info in infos} != expected_names:
            raise ValueError("损坏账本证据包条目结构无效")
        by_name = {info.filename: info for info in infos}
        for info in infos:
            if (
                info.is_dir()
                or info.flag_bits & 0x1
                or info.compress_type != zipfile.ZIP_STORED
                or info.compress_size != info.file_size
            ):
                raise ValueError("损坏账本证据包只允许未加密、未压缩的普通文件")
        for item in manifest["files"]:
            info = by_name[item["archive_file"]]
            if info.file_size != item["size"]:
                raise RuntimeError("损坏账本证据文件大小不匹配")
            digest = hashlib.sha256()
            with archive.open(info, "r") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != item["sha256"]:
                raise RuntimeError("损坏账本证据文件哈希不匹配")
    return {
        **manifest,
        "evidence_file": str(path),
        "evidence_sha256": _file_sha256(path),
        "valid": True,
        "verified_at": datetime.now(timezone.utc).astimezone().isoformat(
            timespec="seconds"
        ),
        "independent_of_current_ledger": True,
        "destructive_action": False,
        "warning": "证据包没有数字签名；请复制到独立、只读或受保护的位置。",
    }


def _external_restore_drill_directory(output_dir: Any) -> Path:
    raw = str(output_dir or "").strip()
    if not raw:
        raise ValueError("恢复演练输出目录不能为空")
    directory = Path(raw).expanduser().resolve()
    managed = Path(_DATA_DIR).resolve()
    if directory == managed or managed in directory.parents:
        raise ValueError("恢复演练不能写入审计账本受管目录")
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("恢复演练输出位置不是目录")
    return directory


def _restore_drill_payload(report: dict) -> dict:
    payload = {
        key: report[key]
        for key in (
            "format", "schema_version", "drill_id", "created_at",
            "backup_id", "backup_created_at", "backup_database_sha256",
            "event_count", "head_hash", "restored_database_sha256",
            "live_event_count", "live_head_hash", "live_ledger_unchanged",
            "restored_artifact_retained", "destructive_action",
        )
    }
    if "report_version" in report:
        payload.update({
            "report_version": report["report_version"],
            "source_kind": report["source_kind"],
            "source_bundle_id": report["source_bundle_id"],
        })
    return payload


def _restore_drill_payload_sha256(report: dict) -> str:
    canonical = json.dumps(
        _restore_drill_payload(report),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _run_restore_drill(
    verified_backup: dict,
    backup_database: Path,
    output_dir: Any,
    *,
    source_kind: str,
    source_bundle_id: str = "",
) -> dict:
    """Materialize one verified source without replacing the live ledger."""
    current_before = verify_chain()
    if not current_before.get("ok"):
        raise RuntimeError("当前审计账本完整性异常，拒绝恢复演练")
    directory = _external_restore_drill_directory(output_dir)
    drill_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "_" + uuid.uuid4().hex[:12]
    )
    filename = f"dbquill-audit-restore-drill-{drill_id}.json"
    target_report = (directory / filename).resolve()
    temporary_database = (directory / f".{filename}.{uuid.uuid4().hex}.db.tmp").resolve()
    temporary_report = (directory / f".{filename}.{uuid.uuid4().hex}.tmp").resolve()
    if any(path.parent != directory for path in (
        target_report, temporary_database, temporary_report,
    )):
        raise ValueError("恢复演练输出路径越界")
    try:
        with closing(sqlite3.connect(str(backup_database), timeout=10)) as source, \
                closing(sqlite3.connect(str(temporary_database), timeout=10)) as target:
            source.backup(target)
        restored = _verify_ledger_file(temporary_database)
        if (
            not restored.get("ok")
            or restored.get("count") != verified_backup["count"]
            or restored.get("head_hash") != verified_backup["head_hash"]
        ):
            raise RuntimeError("恢复演练临时账本校验失败")
        restored_database_sha256 = _file_sha256(temporary_database)
        current_after = verify_chain()
        if not current_after.get("ok"):
            raise RuntimeError("恢复演练后当前审计账本完整性异常")
        live_unchanged = (
            current_after.get("count") == current_before.get("count")
            and current_after.get("head_hash") == current_before.get("head_hash")
        )
        if not live_unchanged:
            raise RuntimeError("恢复演练期间当前审计账本已变化")
        report = {
            "format": RESTORE_DRILL_FORMAT,
            "schema_version": SCHEMA_VERSION,
            "report_version": 2,
            "source_kind": source_kind,
            "source_bundle_id": source_bundle_id,
            "drill_id": drill_id,
            "created_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "backup_id": verified_backup["backup_id"],
            "backup_created_at": verified_backup["created_at"],
            "backup_database_sha256": verified_backup["database_sha256"],
            "event_count": verified_backup["count"],
            "head_hash": verified_backup["head_hash"],
            "restored_database_sha256": restored_database_sha256,
            "live_event_count": int(current_before.get("count") or 0),
            "live_head_hash": str(current_before.get("head_hash") or _GENESIS_HASH),
            "live_ledger_unchanged": True,
            "restored_artifact_retained": False,
            "destructive_action": False,
        }
        report["payload_sha256"] = _restore_drill_payload_sha256(report)
        temporary_report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        os.replace(temporary_report, target_report)
    finally:
        for temporary in (temporary_database, temporary_report):
            if temporary.exists():
                temporary.unlink()
    return {
        **report,
        "report_file": str(target_report),
        "valid": True,
        "warning": "报告不是数字签名；只有保存在独立受保护位置才增加恢复证据价值。",
    }


def run_restore_drill(backup_id: str, output_dir: Any) -> dict:
    """Drill a managed local backup without replacing the live ledger."""
    verified = verify_backup(backup_id)
    database, _ = _backup_paths(backup_id)
    return _run_restore_drill(
        verified,
        database,
        output_dir,
        source_kind="local_backup",
    )


def run_external_restore_drill(bundle_file: Any, output_dir: Any) -> dict:
    """Drill a portable external backup without requiring its managed source."""
    with _materialize_external_backup(bundle_file) as (
        manifest, database, _path, _integrity,
    ):
        verified = {
            "backup_id": manifest["source_backup_id"],
            "created_at": manifest["source_created_at"],
            "database_sha256": manifest["database_sha256"],
            "count": manifest["event_count"],
            "head_hash": manifest["head_hash"],
        }
        return _run_restore_drill(
            verified,
            database,
            output_dir,
            source_kind="external_backup",
            source_bundle_id=manifest["bundle_id"],
        )


def verify_restore_drill(
    report_file: Any,
    *,
    external_backup_file: Any = None,
) -> dict:
    """Verify a restore-drill report against its still-available source backup."""
    path = Path(str(report_file or "").strip()).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("恢复演练报告不存在")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("恢复演练报告无效") from exc
    legacy_keys = {
        "format", "schema_version", "drill_id", "created_at", "backup_id",
        "backup_created_at", "backup_database_sha256", "event_count", "head_hash",
        "restored_database_sha256", "live_event_count", "live_head_hash",
        "live_ledger_unchanged", "restored_artifact_retained", "destructive_action",
        "payload_sha256",
    }
    version_two_keys = legacy_keys | {
        "report_version", "source_kind", "source_bundle_id",
    }
    if not isinstance(report, dict) or frozenset(report) not in {
        frozenset(legacy_keys), frozenset(version_two_keys),
    }:
        raise ValueError("恢复演练报告结构无效")
    report_version = 1 if "report_version" not in report else report["report_version"]
    try:
        created_at = datetime.fromisoformat(str(report.get("created_at") or ""))
        backup_created_at = datetime.fromisoformat(
            str(report.get("backup_created_at") or "")
        )
    except ValueError as exc:
        raise ValueError("恢复演练报告时间无效") from exc
    if (
        not _format_supported(report.get("format"), RESTORE_DRILL_FORMAT)
        or report.get("schema_version") != SCHEMA_VERSION
        or report_version not in {1, 2}
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", str(report.get("drill_id")))
        or created_at.tzinfo is None
        or backup_created_at.tzinfo is None
        or _backup_id(report.get("backup_id")) != report.get("backup_id")
        or not isinstance(report.get("event_count"), int)
        or report.get("event_count") < 0
        or not isinstance(report.get("live_event_count"), int)
        or report.get("live_event_count") < 0
        or not all(_HEX_64_RE.fullmatch(str(report.get(key))) for key in (
            "backup_database_sha256", "head_hash", "restored_database_sha256",
            "live_head_hash", "payload_sha256",
        ))
        or report.get("live_ledger_unchanged") is not True
        or report.get("restored_artifact_retained") is not False
        or report.get("destructive_action") is not False
    ):
        raise ValueError("恢复演练报告内容无效")
    if report_version == 2 and (
        report.get("source_kind") not in {"local_backup", "external_backup"}
        or not isinstance(report.get("source_bundle_id"), str)
        or (
            report.get("source_kind") == "local_backup"
            and report.get("source_bundle_id") != ""
        )
        or (
            report.get("source_kind") == "external_backup"
            and not re.fullmatch(
                r"[0-9]{14}_[0-9a-f]{12}", report.get("source_bundle_id")
            )
        )
    ):
        raise ValueError("恢复演练报告来源无效")
    if _restore_drill_payload_sha256(report) != report["payload_sha256"]:
        raise RuntimeError("恢复演练报告载荷哈希不匹配")
    if report_version == 2 and report.get("source_kind") == "external_backup":
        if external_backup_file is None:
            raise ValueError("复验外部备份演练必须提供 external_backup_file")
        external = verify_external_backup(external_backup_file)
        if external["bundle_id"] != report["source_bundle_id"]:
            raise RuntimeError("恢复演练报告与外部备份包标识不一致")
        verified_backup = {
            "backup_id": external["source_backup_id"],
            "created_at": external["source_created_at"],
            "database_sha256": external["database_sha256"],
            "count": external["event_count"],
            "head_hash": external["head_hash"],
        }
    else:
        verified_backup = verify_backup(report["backup_id"])
    if any((
        verified_backup["created_at"] != report["backup_created_at"],
        verified_backup["database_sha256"] != report["backup_database_sha256"],
        verified_backup["count"] != report["event_count"],
        verified_backup["head_hash"] != report["head_hash"],
    )):
        raise RuntimeError("恢复演练报告与源备份不一致")
    return {
        **report,
        "report_file": str(path),
        "valid": True,
        "verified_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "warning": "报告是点时校验记录且没有数字签名；源备份必须继续独立保护。",
    }


def list_backups(*, limit: int = 100) -> list[dict]:
    directory = _backup_dir()
    if not directory.is_dir():
        return []
    items = []
    manifests = sorted(directory.glob("*.json"), key=lambda item: item.name, reverse=True)
    for manifest_path in manifests[:max(1, min(int(limit), 200))]:
        backup_id = manifest_path.stem
        try:
            items.append(verify_backup(backup_id))
        except Exception as exc:
            items.append({
                "backup_id": backup_id,
                "valid": False,
                "error_type": type(exc).__name__,
            })
    return items


def backup_status() -> dict:
    backups = list_backups()
    valid = [item for item in backups if item.get("valid")]
    return {
        "count": len(backups),
        "valid_count": len(valid),
        "invalid_count": len(backups) - len(valid),
        "latest": valid[0] if valid else None,
    }


def _anchor_payload(anchor: dict) -> dict:
    return {
        key: anchor[key]
        for key in (
            "format", "schema_version", "anchor_id", "created_at",
            "event_count", "head_hash",
        )
    }


def _anchor_payload_sha256(anchor: dict) -> str:
    canonical = json.dumps(
        _anchor_payload(anchor), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _external_anchor_directory(output_dir: Any) -> Path:
    raw = str(output_dir or "").strip()
    if not raw:
        raise ValueError("外部锚点输出目录不能为空")
    directory = Path(raw).expanduser().resolve()
    managed = Path(_DATA_DIR).resolve()
    if directory == managed or managed in directory.parents:
        raise ValueError("外部锚点不能写入审计账本受管目录")
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("外部锚点输出位置不是目录")
    return directory


def create_external_anchor(output_dir: Any) -> dict:
    """Write a portable ledger-head receipt outside the managed audit directory."""
    integrity = verify_chain()
    if not integrity.get("ok"):
        raise RuntimeError("审计账本完整性异常，拒绝创建外部锚点")
    directory = _external_anchor_directory(output_dir)
    anchor_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "_" + uuid.uuid4().hex[:12]
    )
    anchor = {
        "format": ANCHOR_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "anchor_id": anchor_id,
        "created_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "event_count": int(integrity["count"]),
        "head_hash": str(integrity["head_hash"]),
    }
    anchor["payload_sha256"] = _anchor_payload_sha256(anchor)
    filename = f"dbquill-audit-anchor-{anchor_id}.json"
    target = (directory / filename).resolve()
    if target.parent != directory:
        raise ValueError("外部锚点输出路径越界")
    temporary = (directory / f".{filename}.{uuid.uuid4().hex}.tmp").resolve()
    if temporary.parent != directory:
        raise ValueError("外部锚点临时路径越界")
    try:
        temporary.write_text(
            json.dumps(anchor, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {**anchor, "anchor_file": str(target), "valid": True}


def verify_external_anchor(anchor_file: Any) -> dict:
    """Verify an anchor file and the corresponding prefix in the current ledger."""
    path = Path(str(anchor_file or "").strip()).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("外部锚点文件不存在")
    try:
        anchor = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("外部锚点文件无效") from exc
    expected_keys = {
        "format", "schema_version", "anchor_id", "created_at",
        "event_count", "head_hash", "payload_sha256",
    }
    if not isinstance(anchor, dict) or set(anchor) != expected_keys:
        raise ValueError("外部锚点结构无效")
    if (
        not _format_supported(anchor["format"], ANCHOR_FORMAT)
        or anchor["schema_version"] != SCHEMA_VERSION
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", str(anchor["anchor_id"]))
        or not isinstance(anchor["event_count"], int)
        or anchor["event_count"] < 0
        or not _HEX_64_RE.fullmatch(str(anchor["head_hash"]))
        or not _HEX_64_RE.fullmatch(str(anchor["payload_sha256"]))
    ):
        raise ValueError("外部锚点内容无效")
    if _anchor_payload_sha256(anchor) != anchor["payload_sha256"]:
        raise RuntimeError("外部锚点载荷哈希不匹配")
    integrity = verify_chain()
    if not integrity.get("ok"):
        raise RuntimeError("当前审计账本完整性异常，无法验证外部锚点")
    count = int(anchor["event_count"])
    current_count = int(integrity["count"])
    if count > current_count:
        raise RuntimeError("当前审计账本早于外部锚点")
    if count == 0:
        prefix_hash = _GENESIS_HASH
    else:
        with _connect() as conn:
            row = conn.execute(
                "SELECT event_hash FROM audit_events WHERE sequence = ?", (count,),
            ).fetchone()
        if row is None:
            raise RuntimeError("当前审计账本缺少外部锚点对应序号")
        prefix_hash = str(row["event_hash"])
    if prefix_hash != anchor["head_hash"]:
        raise RuntimeError("当前审计账本历史前缀与外部锚点不一致")
    return {
        **anchor,
        "anchor_file": str(path),
        "valid": True,
        "current_event_count": current_count,
        "events_since_anchor": current_count - count,
        "current_head_hash": integrity["head_hash"],
        "verified_at": integrity["verified_at"],
    }


def _archive_payload(archive: dict) -> dict:
    return {
        key: archive[key]
        for key in (
            "format", "schema_version", "archive_id", "created_at",
            "event_count", "first_sequence", "last_sequence", "head_hash", "events",
        )
    }


def _archive_payload_sha256(archive: dict) -> str:
    canonical = json.dumps(
        _archive_payload(archive),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _external_archive_directory(output_dir: Any) -> Path:
    raw = str(output_dir or "").strip()
    if not raw:
        raise ValueError("外部归档输出目录不能为空")
    directory = Path(raw).expanduser().resolve()
    managed = Path(_DATA_DIR).resolve()
    if directory == managed or managed in directory.parents:
        raise ValueError("外部归档不能写入审计账本受管目录")
    directory.mkdir(parents=True, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("外部归档输出位置不是目录")
    return directory


def create_external_archive(
    output_dir: Any,
    *,
    through_sequence: Optional[int] = None,
) -> dict:
    """Atomically write a non-destructive, portable ledger-prefix archive."""
    integrity = verify_chain()
    if not integrity.get("ok"):
        raise RuntimeError("审计账本完整性异常，拒绝创建外部归档")
    current_count = int(integrity.get("count") or 0)
    if current_count <= 0:
        raise ValueError("审计账本为空，没有可归档事件")
    if through_sequence is None:
        last_sequence = current_count
    else:
        if isinstance(through_sequence, bool):
            raise ValueError("through_sequence 必须是有效事件序号")
        try:
            last_sequence = int(through_sequence)
        except (TypeError, ValueError) as exc:
            raise ValueError("through_sequence 必须是有效事件序号") from exc
    if last_sequence < 1 or last_sequence > current_count:
        raise ValueError("through_sequence 超出当前审计账本范围")
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_events WHERE sequence <= ? ORDER BY sequence ASC",
            (last_sequence,),
        ).fetchall()
    events = [_row_to_event(row) for row in rows]
    prefix_integrity = _integrity_from_event_dicts(events)
    if not prefix_integrity.get("ok") or prefix_integrity.get("count") != last_sequence:
        raise RuntimeError("审计归档前缀链校验失败")
    directory = _external_archive_directory(output_dir)
    archive_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "_" + uuid.uuid4().hex[:12]
    )
    archive = {
        "format": ARCHIVE_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "archive_id": archive_id,
        "created_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "event_count": len(events),
        "first_sequence": 1,
        "last_sequence": last_sequence,
        "head_hash": str(prefix_integrity["head_hash"]),
        "events": events,
    }
    archive["payload_sha256"] = _archive_payload_sha256(archive)
    filename = f"dbquill-audit-archive-{archive_id}.json"
    target = (directory / filename).resolve()
    temporary = (directory / f".{filename}.{uuid.uuid4().hex}.tmp").resolve()
    if target.parent != directory or temporary.parent != directory:
        raise ValueError("外部归档输出路径越界")
    try:
        temporary.write_text(
            json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return verify_external_archive(target)


def verify_external_archive(archive_file: Any) -> dict:
    """Verify archive contents and the corresponding current ledger prefix."""
    path = Path(str(archive_file or "").strip()).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError("外部审计归档文件不存在")
    try:
        archive = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("外部审计归档文件无效") from exc
    expected_keys = {
        "format", "schema_version", "archive_id", "created_at", "event_count",
        "first_sequence", "last_sequence", "head_hash", "events", "payload_sha256",
    }
    if not isinstance(archive, dict) or set(archive) != expected_keys:
        raise ValueError("外部审计归档结构无效")
    events = archive.get("events")
    if (
        not _format_supported(archive.get("format"), ARCHIVE_FORMAT)
        or archive.get("schema_version") != SCHEMA_VERSION
        or not re.fullmatch(r"[0-9]{14}_[0-9a-f]{12}", str(archive.get("archive_id")))
        or not isinstance(events, list)
        or not isinstance(archive.get("event_count"), int)
        or archive.get("event_count") != len(events)
        or archive.get("event_count") <= 0
        or archive.get("first_sequence") != 1
        or archive.get("last_sequence") != archive.get("event_count")
        or not _HEX_64_RE.fullmatch(str(archive.get("head_hash")))
        or not _HEX_64_RE.fullmatch(str(archive.get("payload_sha256")))
    ):
        raise ValueError("外部审计归档内容无效")
    if _archive_payload_sha256(archive) != archive["payload_sha256"]:
        raise RuntimeError("外部审计归档载荷哈希不匹配")
    archived_integrity = _integrity_from_event_dicts(events)
    if (
        not archived_integrity.get("ok")
        or archived_integrity.get("count") != archive["event_count"]
        or archived_integrity.get("head_hash") != archive["head_hash"]
    ):
        raise RuntimeError("外部审计归档事件链校验失败")
    current = verify_chain()
    if not current.get("ok"):
        raise RuntimeError("当前审计账本完整性异常，无法验证归档前缀")
    current_count = int(current.get("count") or 0)
    if archive["last_sequence"] > current_count:
        raise RuntimeError("当前审计账本早于外部归档")
    with _connect() as conn:
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE sequence = ?",
            (archive["last_sequence"],),
        ).fetchone()
    if row is None or str(row["event_hash"]) != archive["head_hash"]:
        raise RuntimeError("当前审计账本历史前缀与外部归档不一致")
    return {
        "format": archive["format"],
        "schema_version": archive["schema_version"],
        "archive_id": archive["archive_id"],
        "created_at": archive["created_at"],
        "event_count": archive["event_count"],
        "first_sequence": archive["first_sequence"],
        "last_sequence": archive["last_sequence"],
        "head_hash": archive["head_hash"],
        "payload_sha256": archive["payload_sha256"],
        "archive_file": str(path),
        "valid": True,
        "current_event_count": current_count,
        "events_since_archive": current_count - archive["event_count"],
        "current_head_hash": current["head_hash"],
        "verified_at": current["verified_at"],
        "destructive_action": False,
        "warning": "归档不会删除当前账本；只有复制到独立受保护位置才增加外部证据价值。",
    }


def _stage_verified_database(
    verified_backup: dict,
    backup_database: Path,
    temp_restore: Path,
) -> dict:
    with closing(sqlite3.connect(str(backup_database), timeout=10)) as source, \
            closing(sqlite3.connect(str(temp_restore), timeout=10)) as target:
        source.backup(target)
    restored_check = _verify_ledger_file(temp_restore)
    if (
        not restored_check.get("ok")
        or restored_check.get("head_hash") != verified_backup["head_hash"]
        or restored_check.get("count") != verified_backup["count"]
    ):
        raise RuntimeError("恢复临时账本校验失败")
    return restored_check


def _remove_sqlite_temporary_artifacts(database_path: Path) -> None:
    parent = Path(database_path).resolve().parent
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(database_path) + suffix).resolve()
        if candidate.parent != parent:
            raise RuntimeError("SQLite 临时文件路径越界")
        if candidate.exists():
            if not candidate.is_file():
                raise RuntimeError("SQLite 临时路径不是普通文件")
            candidate.unlink()


def _restore_verified_database(
    verified_backup: dict,
    backup_database: Path,
    *,
    expected_current_head: str,
) -> dict:
    expected_current_head = str(expected_current_head or "").strip().lower()
    if not _HEX_64_RE.fullmatch(expected_current_head):
        raise ValueError("当前账本 head hash 无效")
    with _WRITE_LOCK:
        current = verify_chain()
        if not current.get("ok"):
            raise RuntimeError("当前审计账本完整性异常，拒绝自动恢复")
        if current.get("head_hash") != expected_current_head:
            raise RuntimeError("当前审计账本已变化，拒绝恢复")
        safety_backup = create_backup(reason="pre_restore")
        target_path = Path(_DB_PATH).resolve()
        managed_directory = Path(_DATA_DIR).resolve()
        if target_path.parent != managed_directory:
            raise RuntimeError("审计账本路径不在受管目录内")
        temp_restore = managed_directory / f".{uuid.uuid4().hex}.restore.tmp"
        try:
            _stage_verified_database(verified_backup, backup_database, temp_restore)
            checkpoint = sqlite3.connect(str(_DB_PATH), timeout=1, isolation_level=None)
            try:
                checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                checkpoint.execute("BEGIN EXCLUSIVE")
                checkpoint.execute("ROLLBACK")
            finally:
                checkpoint.close()
            os.replace(temp_restore, target_path)
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(target_path) + suffix).resolve()
                if sidecar.parent != managed_directory:
                    raise RuntimeError("审计账本附属文件路径越界")
                if sidecar.exists():
                    sidecar.unlink()
        finally:
            _remove_sqlite_temporary_artifacts(temp_restore)
        restored = verify_chain()
        if not restored.get("ok") or restored.get("head_hash") != verified_backup["head_hash"]:
            raise RuntimeError("审计账本恢复后校验失败")
    return {
        "safety_backup_id": safety_backup["backup_id"],
        "integrity": restored,
    }


def restore_backup(
    backup_id: str,
    *,
    expected_current_head: str,
    confirmation: str,
) -> dict:
    """Restore a verified managed backup through the offline CLI only."""
    if confirmation != "RESTORE_AUDIT_LEDGER":
        raise ValueError("缺少审计账本恢复确认短语")
    verified = verify_backup(backup_id)
    database, _ = _backup_paths(backup_id)
    result = _restore_verified_database(
        verified,
        database,
        expected_current_head=expected_current_head,
    )
    return {"restored_backup_id": verified["backup_id"], **result}


def restore_external_backup(
    bundle_file: Any,
    *,
    expected_current_head: str,
    confirmation: str,
) -> dict:
    """Restore a verified portable backup through the offline CLI only."""
    if confirmation != "RESTORE_EXTERNAL_AUDIT_BACKUP":
        raise ValueError("缺少外部审计备份恢复确认短语")
    init_db()
    with _materialize_external_backup(bundle_file) as (
        manifest, database, _path, _integrity,
    ):
        verified = {
            "backup_id": manifest["source_backup_id"],
            "created_at": manifest["source_created_at"],
            "database_sha256": manifest["database_sha256"],
            "count": manifest["event_count"],
            "head_hash": manifest["head_hash"],
        }
        result = _restore_verified_database(
            verified,
            database,
            expected_current_head=expected_current_head,
        )
    return {
        "restored_external_bundle_id": manifest["bundle_id"],
        "restored_backup_id": manifest["source_backup_id"],
        **result,
    }


def _replace_corrupt_ledger_from_verified_database(
    verified_backup: dict,
    backup_database: Path,
    *,
    expected_assessment_token: str,
) -> dict:
    """Replace a corrupt ledger after evidence preservation, with local rollback copies."""
    with _WRITE_LOCK:
        current = assess_current_ledger()
        if current["integrity_ok"]:
            raise RuntimeError("当前审计账本完整性正常，拒绝使用损坏现场恢复通道")
        if current["assessment_token"] != expected_assessment_token:
            raise RuntimeError("当前审计账本现场已变化，拒绝灾备恢复")
        managed_files = _managed_ledger_files()
        target_path = managed_files[0][2]
        managed_directory = Path(_DATA_DIR).resolve()
        temp_restore = managed_directory / f".{uuid.uuid4().hex}.corrupt-restore.tmp"
        rollback_id = uuid.uuid4().hex
        rollback_paths: dict[str, Path] = {}
        original_paths = {role: path for role, _archive, path in managed_files}
        expected_files = {item["role"]: item for item in current["files"]}
        switched = False
        restored = None
        try:
            _stage_verified_database(verified_backup, backup_database, temp_restore)
            for role, item in expected_files.items():
                rollback = managed_directory / f".{rollback_id}.{role}.rollback.tmp"
                shutil.copy2(original_paths[role], rollback)
                if (
                    rollback.stat().st_size != item["size"]
                    or _file_sha256(rollback) != item["sha256"]
                ):
                    raise RuntimeError("损坏账本现场回滚副本校验失败")
                rollback_paths[role] = rollback
            if assess_current_ledger()["assessment_token"] != expected_assessment_token:
                raise RuntimeError("恢复切换前审计账本现场发生变化")
            os.replace(temp_restore, target_path)
            switched = True
            for role in ("wal", "shm"):
                sidecar = original_paths[role]
                if sidecar.exists():
                    sidecar.unlink()
            restored = verify_chain()
            if (
                not restored.get("ok")
                or restored.get("head_hash") != verified_backup["head_hash"]
                or restored.get("count") != verified_backup["count"]
            ):
                raise RuntimeError("损坏账本灾备恢复后校验失败")
        except Exception:
            if switched:
                try:
                    for role in ("wal", "shm"):
                        sidecar = original_paths[role]
                        if sidecar.exists():
                            sidecar.unlink()
                    database_rollback = rollback_paths.get("database")
                    if database_rollback is None:
                        raise RuntimeError("缺少数据库主文件回滚副本")
                    os.replace(database_rollback, target_path)
                    rollback_paths.pop("database", None)
                    for role in ("wal", "shm"):
                        rollback = rollback_paths.pop(role, None)
                        if rollback is not None:
                            os.replace(rollback, original_paths[role])
                    if (
                        assess_current_ledger()["assessment_token"]
                        != expected_assessment_token
                    ):
                        raise RuntimeError("损坏现场自动回滚后的文件指纹不一致")
                except Exception as rollback_exc:
                    raise RuntimeError("灾备恢复失败且损坏现场自动回滚失败") from rollback_exc
            raise
        finally:
            _remove_sqlite_temporary_artifacts(temp_restore)
            for rollback in rollback_paths.values():
                if rollback.exists():
                    rollback.unlink()
    return {"integrity": restored}


def restore_external_backup_over_corrupt_ledger(
    bundle_file: Any,
    *,
    expected_assessment_token: str,
    evidence_output_dir: Any,
    confirmation: str,
) -> dict:
    """Offline disaster recovery that preserves the corrupt live ledger first."""
    if confirmation != "RESTORE_CORRUPT_AUDIT_LEDGER":
        raise ValueError("缺少损坏审计账本灾备恢复确认短语")
    expected = str(expected_assessment_token or "").strip().lower()
    if not _HEX_64_RE.fullmatch(expected):
        raise ValueError("审计账本现场评估令牌无效")
    with _materialize_external_backup(bundle_file) as (
        manifest, database, _path, _integrity,
    ):
        verified = {
            "backup_id": manifest["source_backup_id"],
            "created_at": manifest["source_created_at"],
            "database_sha256": manifest["database_sha256"],
            "count": manifest["event_count"],
            "head_hash": manifest["head_hash"],
        }
        with _WRITE_LOCK:
            evidence = create_corrupt_ledger_evidence(
                evidence_output_dir,
                expected_assessment_token=expected,
                confirmation="PRESERVE_CORRUPT_AUDIT_LEDGER",
            )
            if evidence["assessment_token"] != expected or not evidence["valid"]:
                raise RuntimeError("损坏账本证据保全校验失败")
            result = _replace_corrupt_ledger_from_verified_database(
                verified,
                database,
                expected_assessment_token=expected,
            )
    return {
        "restored_external_bundle_id": manifest["bundle_id"],
        "restored_backup_id": manifest["source_backup_id"],
        "corrupt_evidence_id": evidence["evidence_id"],
        "corrupt_evidence_file": evidence["evidence_file"],
        "corrupt_evidence_sha256": evidence["evidence_sha256"],
        "safety_backup_id": None,
        "recovery_mode": "corrupt_ledger_evidence_preserved",
        **result,
    }


def export_events(*, database_key: Optional[str] = None, limit: int = 200) -> dict:
    integrity = verify_chain()
    if not integrity["ok"]:
        raise RuntimeError(
            f"审计账本完整性校验失败（sequence={integrity.get('error_sequence')}）"
        )
    return {
        "format": EXPORT_FORMAT,
        "schema_version": SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "scope": "database" if database_key is not None else "all",
        "integrity": integrity,
        "events": list_events(database_key=database_key, limit=limit),
    }
