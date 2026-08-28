"""Execution-grounded layered memory for DBQuill.

The store keeps the memory boundary deliberately narrow:

* L0 is an immutable policy returned by :func:`policy_view` and is never stored.
* L1 is a compact token-to-reference index.
* L2 stores verified routing facts, not database values or user claims.
* L3 stores typed, non-executable read strategies.
* L4 stores bounded, redacted execution episodes.

Only completed database reads can strengthen L2/L3.  SQL text, result rows,
credentials, connection strings and model prompts are never persisted here.
All records are bound to a database reference, an access-scope reference and a
schema fingerprint so memory cannot cross a permission or schema boundary.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "db_memory.db"
_LOCK = threading.RLock()

SCHEMA_VERSION = 1
PROMOTION_SUPPORT = 3
MAX_EPISODES_PER_SCOPE = 500
_READ_INTENTS = frozenset({"query", "retrieve", "compose"})
_SAFE_ACTIONS = frozenset({
    "inspect_schema", "search_schema", "search_values", "sample_rows",
    "find_relations", "run_sql", "retrieve", "query", "analyze",
    "select",
})
_CORRECTION_RE = re.compile(
    r"^(?:不对|不是|错了|纠正|更正|我的意思(?:是)?|我说的是|你理解错了|"
    r"no[,， ]|wrong[,， ]|i mean\b)",
    re.IGNORECASE,
)
_SECRET_RE = re.compile(
    r"(?i)(password|passwd|pwd|token|api[_ -]?key|secret)\s*[:=]\s*([^\s,;]+)"
)
_URI_SECRET_RE = re.compile(r"(?i)\b(mysql|postgres(?:ql)?)://[^\s]+")
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_EN_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{1,31}")
_CJK_RUN_RE = re.compile(r"[\u3400-\u9fff]{2,32}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _load_json(value: Any, fallback: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback
    return parsed


def _hash(*parts: Any) -> str:
    payload = "\x1f".join(str(part or "") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def session_ref(session_id: Any) -> str:
    return _hash("dbquill-memory-session-v1", session_id)[:32]


def run_ref(run_id: Any) -> str:
    return _hash("dbquill-memory-run-v1", run_id)[:32]


def schema_fingerprint(schema: Any) -> str:
    """Hash only authorized schema names/types/relations, never sample values."""
    rows = []
    tables = getattr(schema, "tables", {}) or {}
    for table_name, table in sorted(tables.items(), key=lambda item: str(item[0]).casefold()):
        columns = []
        for column in getattr(table, "columns", []) or []:
            columns.append({
                "name": str(getattr(column, "name", "")),
                "type": str(getattr(column, "type", "")),
                "nullable": bool(getattr(column, "nullable", True)),
                "pk": bool(getattr(column, "pk", False)),
                "fk_table": str(getattr(column, "fk_table", "") or ""),
                "fk_column": str(getattr(column, "fk_column", "") or ""),
            })
        rows.append({"table": str(table_name), "columns": columns})
    return _hash("dbquill-schema-v1", _json(rows))


def redact_preview(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = _SECRET_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _URI_SECRET_RE.sub("[REDACTED_CONNECTION_URI]", text)
    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    return text[:limit]


def question_tokens(value: Any, limit: int = 36) -> list[str]:
    """Return language-agnostic lexical probes for an L1 inverted index."""
    text = str(value or "").casefold()
    weighted: list[str] = []
    for token in _EN_TOKEN_RE.findall(text):
        if token not in weighted:
            weighted.append(token)
    for run in _CJK_RUN_RE.findall(text):
        if run not in weighted:
            weighted.append(run)
        for size in (2, 3):
            for index in range(max(0, len(run) - size + 1)):
                token = run[index:index + size]
                if token not in weighted:
                    weighted.append(token)
                if len(weighted) >= limit:
                    return weighted
    return weighted[:limit]


def policy_view() -> dict:
    return {
        "version": "dbquill-memory-policy-v1",
        "label": "L0 · 记忆宪章",
        "immutable": True,
        "rules": [
            "只有完成的数据库执行证据可以强化长期记忆",
            "记忆严格绑定数据库、授权范围和结构版本",
            "不保存 SQL、结果行、连接信息、凭据或模型提示词",
            "策略只能描述白名单只读动作，不能包含可执行代码",
            "纠错会降权；首次或每次纠错后均需三次成功证据才能晋级",
        ],
    }


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _LOCK, closing(_connect()) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS memory_episodes (
                id TEXT PRIMARY KEY,
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                session_ref TEXT NOT NULL,
                run_ref TEXT NOT NULL,
                question_sha256 TEXT NOT NULL,
                question_preview TEXT NOT NULL,
                tokens_json TEXT NOT NULL,
                intent TEXT NOT NULL,
                action TEXT NOT NULL,
                target_tables_json TEXT NOT NULL,
                action_sequence_json TEXT NOT NULL,
                outcome TEXT NOT NULL,
                result_count INTEGER NOT NULL DEFAULT 0,
                evidence_count INTEGER NOT NULL DEFAULT 0,
                corrected INTEGER NOT NULL DEFAULT 0,
                fact_id TEXT,
                strategy_id TEXT,
                evidence_fingerprint TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_memory_episode_scope
                ON memory_episodes(database_ref, access_scope_ref, schema_fingerprint, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_memory_episode_session
                ON memory_episodes(database_ref, access_scope_ref, session_ref, created_at DESC);

            CREATE TABLE IF NOT EXISTS memory_facts (
                id TEXT PRIMARY KEY,
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                signature TEXT NOT NULL,
                trigger_tokens_json TEXT NOT NULL,
                target_tables_json TEXT NOT NULL,
                action_sequence_json TEXT NOT NULL,
                support_count INTEGER NOT NULL DEFAULT 0,
                correction_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(database_ref, access_scope_ref, schema_fingerprint, signature)
            );
            CREATE INDEX IF NOT EXISTS idx_memory_fact_scope
                ON memory_facts(database_ref, access_scope_ref, schema_fingerprint, updated_at DESC);

            CREATE TABLE IF NOT EXISTS memory_strategies (
                id TEXT PRIMARY KEY,
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                signature TEXT NOT NULL,
                trigger_tokens_json TEXT NOT NULL,
                intent TEXT NOT NULL,
                target_tables_json TEXT NOT NULL,
                action_sequence_json TEXT NOT NULL,
                support_count INTEGER NOT NULL DEFAULT 0,
                correction_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'candidate',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                promoted_at TEXT,
                UNIQUE(database_ref, access_scope_ref, schema_fingerprint, signature)
            );
            CREATE INDEX IF NOT EXISTS idx_memory_strategy_scope
                ON memory_strategies(database_ref, access_scope_ref, schema_fingerprint, updated_at DESC);

            CREATE TABLE IF NOT EXISTS memory_index (
                database_ref TEXT NOT NULL,
                access_scope_ref TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                token TEXT NOT NULL,
                layer TEXT NOT NULL,
                target_id TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 1.0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(database_ref, access_scope_ref, schema_fingerprint, token, layer, target_id)
            );
            CREATE INDEX IF NOT EXISTS idx_memory_index_lookup
                ON memory_index(database_ref, access_scope_ref, schema_fingerprint, token, layer);
            """
        )
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()


def _normalize_tables(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    tables: list[str] = []
    for value in values:
        table = re.sub(r"[^\w\u3400-\u9fff .-]", "", str(value or "")).strip()[:128]
        if table and table not in tables:
            tables.append(table)
        if len(tables) >= 12:
            break
    return tables


def _normalize_actions(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    actions: list[str] = []
    for value in values:
        action = str(value or "").strip().lower()
        if action in _SAFE_ACTIONS and (not actions or actions[-1] != action):
            actions.append(action)
        if len(actions) >= 12:
            break
    return actions


def extract_action_sequence(answer: Any) -> list[str]:
    steps = answer.get("steps") if isinstance(answer, dict) else getattr(answer, "steps", [])
    operation = answer.get("operation") if isinstance(answer, dict) else getattr(answer, "operation", None)
    actions: list[str] = []
    if isinstance(steps, list):
        for step in steps:
            if not isinstance(step, dict):
                continue
            action = str(step.get("action") or "").strip().lower()
            if action in _SAFE_ACTIONS and (not actions or actions[-1] != action):
                actions.append(action)
    if not actions and isinstance(operation, dict):
        action = str(operation.get("action") or "").strip().lower()
        if action in _SAFE_ACTIONS:
            actions.append(action)
    return actions[:12]


def _route_signature(intent: str, tokens: list[str], tables: list[str], actions: list[str]) -> str:
    # The learned object is a typed route through the current schema, not an
    # exact question cache. Trigger tokens are accumulated separately so
    # paraphrases can strengthen the same route without merging different
    # table/action plans.
    return _hash("route-v2", intent, _json(tables), _json(actions))


def _index_targets(
    conn: sqlite3.Connection,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    tokens: Iterable[str],
    layer: str,
    target_id: str,
    weight: float,
) -> None:
    now = _now()
    for token in list(dict.fromkeys(tokens))[:36]:
        conn.execute(
            """INSERT INTO memory_index(
                   database_ref, access_scope_ref, schema_fingerprint,
                   token, layer, target_id, weight, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(database_ref, access_scope_ref, schema_fingerprint, token, layer, target_id)
               DO UPDATE SET weight=excluded.weight, updated_at=excluded.updated_at""",
            (database_ref, access_scope_ref, schema_ref, token, layer, target_id, weight, now),
        )


def _mark_previous_correction(
    conn: sqlite3.Connection,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    session: str,
) -> Optional[str]:
    row = conn.execute(
        """SELECT id, fact_id, strategy_id FROM memory_episodes
           WHERE database_ref=? AND access_scope_ref=? AND schema_fingerprint=?
                 AND session_ref=?
                 AND corrected=0
           ORDER BY created_at DESC LIMIT 1""",
        (database_ref, access_scope_ref, schema_ref, session),
    ).fetchone()
    if row is None:
        return None
    now = _now()
    conn.execute(
        "UPDATE memory_episodes SET corrected=1, updated_at=? WHERE id=?",
        (now, row["id"]),
    )
    if row["fact_id"]:
        conn.execute(
            """UPDATE memory_facts SET correction_count=correction_count+1,
                   status='active', updated_at=? WHERE id=?""",
            (now, row["fact_id"]),
        )
    if row["strategy_id"]:
        conn.execute(
            """UPDATE memory_strategies SET correction_count=correction_count+1,
                   status=CASE WHEN status='disabled' THEN 'disabled' ELSE 'candidate' END,
                   promoted_at=NULL, updated_at=? WHERE id=?""",
            (now, row["strategy_id"]),
        )
    return str(row["id"])


def _upsert_route_memory(
    conn: sqlite3.Connection,
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    intent: str,
    tokens: list[str],
    tables: list[str],
    actions: list[str],
) -> tuple[str, str, str]:
    signature = _route_signature(intent, tokens, tables, actions)
    now = _now()
    fact_row = conn.execute(
        """SELECT id, trigger_tokens_json FROM memory_facts WHERE database_ref=? AND access_scope_ref=?
           AND schema_fingerprint=? AND signature=?""",
        (database_ref, access_scope_ref, schema_ref, signature),
    ).fetchone()
    fact_id = str(fact_row["id"]) if fact_row else uuid.uuid4().hex
    if fact_row:
        fact_tokens = list(dict.fromkeys([
            *_load_json(fact_row["trigger_tokens_json"], []), *tokens,
        ]))[:36]
        conn.execute(
            """UPDATE memory_facts SET support_count=support_count+1,
               trigger_tokens_json=?, updated_at=? WHERE id=?""",
            (_json(fact_tokens), now, fact_id),
        )
    else:
        conn.execute(
            """INSERT INTO memory_facts(
                   id, database_ref, access_scope_ref, schema_fingerprint, signature,
                   trigger_tokens_json, target_tables_json, action_sequence_json,
                   support_count, correction_count, status, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 'active', ?, ?)""",
            (
                fact_id, database_ref, access_scope_ref, schema_ref, signature,
                _json(tokens[:24]), _json(tables), _json(actions), now, now,
            ),
        )

    strategy_row = conn.execute(
        """SELECT id, trigger_tokens_json, support_count, correction_count, status FROM memory_strategies
           WHERE database_ref=? AND access_scope_ref=? AND schema_fingerprint=? AND signature=?""",
        (database_ref, access_scope_ref, schema_ref, signature),
    ).fetchone()
    strategy_id = str(strategy_row["id"]) if strategy_row else uuid.uuid4().hex
    if strategy_row:
        strategy_tokens = list(dict.fromkeys([
            *_load_json(strategy_row["trigger_tokens_json"], []), *tokens,
        ]))[:36]
        support = int(strategy_row["support_count"]) + 1
        corrections = int(strategy_row["correction_count"])
        previous_status = str(strategy_row["status"])
        status = previous_status
        promoted_at = None
        if previous_status != "disabled":
            status = (
                "promoted"
                if support >= PROMOTION_SUPPORT * (corrections + 1)
                else "candidate"
            )
            promoted_at = now if status == "promoted" else None
        conn.execute(
            """UPDATE memory_strategies SET support_count=?, trigger_tokens_json=?,
                   status=?, promoted_at=COALESCE(?, promoted_at), updated_at=? WHERE id=?""",
            (support, _json(strategy_tokens), status, promoted_at, now, strategy_id),
        )
    else:
        support = 1
        status = "candidate"
        conn.execute(
            """INSERT INTO memory_strategies(
                   id, database_ref, access_scope_ref, schema_fingerprint, signature,
                   trigger_tokens_json, intent, target_tables_json, action_sequence_json,
                   support_count, correction_count, status, created_at, updated_at, promoted_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 'candidate', ?, ?, NULL)""",
            (
                strategy_id, database_ref, access_scope_ref, schema_ref, signature,
                _json(tokens[:24]), intent, _json(tables), _json(actions), now, now,
            ),
        )
    _index_targets(conn, database_ref, access_scope_ref, schema_ref, tokens, "l2", fact_id, 1.3)
    _index_targets(conn, database_ref, access_scope_ref, schema_ref, tokens, "l3", strategy_id, 1.6)
    return fact_id, strategy_id, status


def record_episode(
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    session_id: str,
    run_id: str,
    question: str,
    answer: Any,
) -> dict:
    """Archive one bounded execution and crystallize only successful reads."""
    operation = answer.get("operation") if isinstance(answer, dict) else getattr(answer, "operation", None)
    operation = operation if isinstance(operation, dict) else {}
    intent = str(operation.get("intent") or "").strip().lower()
    mode = str(operation.get("mode") or "").strip().lower()
    kind = str(answer.get("kind") if isinstance(answer, dict) else getattr(answer, "kind", ""))
    error = answer.get("error") if isinstance(answer, dict) else getattr(answer, "error", None)
    if mode != "read" or intent not in _READ_INTENTS or kind in {"conversation", "clarification"}:
        return {"stored": False, "reason": "not_a_completed_read"}

    tables = _normalize_tables(operation.get("target_tables"))
    actions = _normalize_actions(extract_action_sequence(answer))
    if not actions:
        return {"stored": False, "reason": "no_database_action"}
    tokens = question_tokens(redact_preview(question, limit=1200))
    corrected_previous = bool(_CORRECTION_RE.search(str(question or "").strip()))
    rows = answer.get("rows") if isinstance(answer, dict) else getattr(answer, "rows", [])
    datasets = answer.get("datasets") if isinstance(answer, dict) else getattr(answer, "datasets", [])
    evidence = answer.get("evidence") if isinstance(answer, dict) else getattr(answer, "evidence", [])
    result_count = len(rows or []) + sum(len(item.get("rows") or []) for item in (datasets or []) if isinstance(item, dict))
    evidence_count = len(evidence or [])
    succeeded = not error and kind not in {"error"} and str(operation.get("status") or "") != "failed"
    outcome = "succeeded" if succeeded else "failed"
    question_sha = _hash(question)
    sess_ref = session_ref(session_id)
    execution_ref = run_ref(run_id)
    fingerprint = _hash(
        "dbquill-memory-evidence-v1", database_ref, access_scope_ref, schema_ref,
        execution_ref, question_sha, intent, _json(tables), _json(actions), outcome,
        result_count, evidence_count,
    )
    now = _now()

    with _LOCK, closing(_connect()) as conn:
        existing = conn.execute(
            "SELECT id FROM memory_episodes WHERE evidence_fingerprint=?", (fingerprint,),
        ).fetchone()
        if existing:
            return {"stored": False, "reason": "duplicate", "episodeId": existing["id"]}
        corrected_episode_id = None
        if corrected_previous:
            corrected_episode_id = _mark_previous_correction(
                conn, database_ref, access_scope_ref, schema_ref, sess_ref,
            )
        fact_id = None
        strategy_id = None
        strategy_status = None
        if succeeded:
            fact_id, strategy_id, strategy_status = _upsert_route_memory(
                conn,
                database_ref=database_ref,
                access_scope_ref=access_scope_ref,
                schema_ref=schema_ref,
                intent=intent,
                tokens=tokens,
                tables=tables,
                actions=actions,
            )
        episode_id = uuid.uuid4().hex
        conn.execute(
            """INSERT INTO memory_episodes(
                   id, database_ref, access_scope_ref, schema_fingerprint,
                   session_ref, run_ref, question_sha256, question_preview,
                   tokens_json, intent, action, target_tables_json,
                   action_sequence_json, outcome, result_count, evidence_count,
                   corrected, fact_id, strategy_id, evidence_fingerprint,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)""",
            (
                episode_id, database_ref, access_scope_ref, schema_ref, sess_ref,
                execution_ref, question_sha, redact_preview(question), _json(tokens),
                intent, str(operation.get("action") or actions[-1])[:32], _json(tables),
                _json(actions), outcome, result_count, evidence_count, fact_id,
                strategy_id, fingerprint, now, now,
            ),
        )
        _index_targets(conn, database_ref, access_scope_ref, schema_ref, tokens, "l4", episode_id, 1.0)
        stale_rows = conn.execute(
            """SELECT id, fact_id, strategy_id, outcome, corrected
               FROM memory_episodes WHERE database_ref=? AND access_scope_ref=?
               ORDER BY created_at DESC LIMIT -1 OFFSET ?""",
            (database_ref, access_scope_ref, MAX_EPISODES_PER_SCOPE),
        ).fetchall()
        _delete_episode_rows(conn, stale_rows)
        conn.commit()
    return {
        "stored": True,
        "episodeId": episode_id,
        "correctedEpisodeId": corrected_episode_id,
        "factId": fact_id,
        "strategyId": strategy_id,
        "strategyStatus": strategy_status,
    }


def _ranked_ids(
    conn: sqlite3.Connection,
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    tokens: list[str],
    layer: str,
    limit: int,
) -> list[str]:
    if not tokens:
        return []
    placeholders = ",".join("?" for _ in tokens)
    rows = conn.execute(
        f"""SELECT target_id, SUM(weight) AS score, COUNT(*) AS overlap
            FROM memory_index WHERE database_ref=? AND access_scope_ref=?
              AND schema_fingerprint=? AND layer=? AND token IN ({placeholders})
            GROUP BY target_id ORDER BY score DESC, overlap DESC, MAX(updated_at) DESC LIMIT ?""",
        [database_ref, access_scope_ref, schema_ref, layer, *tokens, int(limit)],
    ).fetchall()
    return [str(row["target_id"]) for row in rows]


def _rows_by_ids(conn: sqlite3.Connection, table: str, ids: list[str]) -> list[sqlite3.Row]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(f"SELECT * FROM {table} WHERE id IN ({placeholders})", ids).fetchall()
    by_id = {str(row["id"]): row for row in rows}
    return [by_id[item] for item in ids if item in by_id]


def _fact_view(row: sqlite3.Row) -> dict:
    support = int(row["support_count"])
    corrections = int(row["correction_count"])
    return {
        "id": row["id"],
        "triggers": _load_json(row["trigger_tokens_json"], [])[:12],
        "targetTables": _load_json(row["target_tables_json"], []),
        "actions": _load_json(row["action_sequence_json"], []),
        "supportCount": support,
        "correctionCount": corrections,
        "confidence": round(support / max(1, support + corrections), 2),
        "status": row["status"],
        "updatedAt": row["updated_at"],
    }


def _strategy_view(row: sqlite3.Row) -> dict:
    support = int(row["support_count"])
    corrections = int(row["correction_count"])
    return {
        "id": row["id"],
        "triggers": _load_json(row["trigger_tokens_json"], [])[:12],
        "intent": row["intent"],
        "targetTables": _load_json(row["target_tables_json"], []),
        "actions": _load_json(row["action_sequence_json"], []),
        "supportCount": support,
        "correctionCount": corrections,
        "confidence": round(support / max(1, support + corrections), 2),
        "status": row["status"],
        "promotionThreshold": PROMOTION_SUPPORT * (corrections + 1),
        "promotedAt": row["promoted_at"],
        "updatedAt": row["updated_at"],
    }


def _episode_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "questionPreview": row["question_preview"],
        "intent": row["intent"],
        "action": row["action"],
        "targetTables": _load_json(row["target_tables_json"], []),
        "actions": _load_json(row["action_sequence_json"], []),
        "outcome": row["outcome"],
        "resultCount": int(row["result_count"]),
        "evidenceCount": int(row["evidence_count"]),
        "corrected": bool(row["corrected"]),
        "createdAt": row["created_at"],
    }


def recall(
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    question: str,
    limit: int = 4,
) -> dict:
    tokens = question_tokens(question)
    with closing(_connect()) as conn:
        l2_ids = _ranked_ids(
            conn, database_ref=database_ref, access_scope_ref=access_scope_ref,
            schema_ref=schema_ref, tokens=tokens, layer="l2", limit=limit,
        )
        l3_ids = _ranked_ids(
            conn, database_ref=database_ref, access_scope_ref=access_scope_ref,
            schema_ref=schema_ref, tokens=tokens, layer="l3", limit=limit,
        )
        l4_ids = _ranked_ids(
            conn, database_ref=database_ref, access_scope_ref=access_scope_ref,
            schema_ref=schema_ref, tokens=tokens, layer="l4", limit=limit,
        )
        facts = [_fact_view(row) for row in _rows_by_ids(conn, "memory_facts", l2_ids)]
        strategies = [
            _strategy_view(row) for row in _rows_by_ids(conn, "memory_strategies", l3_ids)
            if str(row["status"]) != "disabled"
        ]
        episodes = [_episode_view(row) for row in _rows_by_ids(conn, "memory_episodes", l4_ids)]

    promoted = [item for item in strategies if item["status"] == "promoted"]
    advisory_items = promoted
    advisory = []
    for item in advisory_items[:2]:
        advisory.append({
            "status": item["status"],
            "intent": item["intent"],
            "target_tables": item["targetTables"],
            "read_actions": item["actions"],
            "support_count": item["supportCount"],
            "correction_count": item["correctionCount"],
        })
    return {
        "version": "dbquill-layered-memory-v1",
        "policy": policy_view(),
        "queryTokens": tokens[:16],
        "l1": {"matchedReferences": len(l2_ids) + len(l3_ids) + len(l4_ids)},
        "l2": facts,
        "l3": strategies,
        "l4": episodes,
        "plannerAdvisory": advisory,
    }


def workspace(
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    limit: int = 60,
) -> dict:
    with closing(_connect()) as conn:
        counts = {}
        for key, table in (("l1", "memory_index"), ("l2", "memory_facts"), ("l3", "memory_strategies"), ("l4", "memory_episodes")):
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table} WHERE database_ref=? AND access_scope_ref=? AND schema_fingerprint=?",
                (database_ref, access_scope_ref, schema_ref),
            ).fetchone()
            counts[key] = int(row["count"])
        stale = conn.execute(
            """SELECT COUNT(*) AS count FROM memory_episodes WHERE database_ref=?
               AND access_scope_ref=? AND schema_fingerprint<>?""",
            (database_ref, access_scope_ref, schema_ref),
        ).fetchone()
        fact_rows = conn.execute(
            """SELECT * FROM memory_facts WHERE database_ref=? AND access_scope_ref=?
               AND schema_fingerprint=? ORDER BY support_count DESC, updated_at DESC LIMIT ?""",
            (database_ref, access_scope_ref, schema_ref, int(limit)),
        ).fetchall()
        strategy_rows = conn.execute(
            """SELECT * FROM memory_strategies WHERE database_ref=? AND access_scope_ref=?
               AND schema_fingerprint=? ORDER BY
               CASE status WHEN 'promoted' THEN 0 WHEN 'candidate' THEN 1 ELSE 2 END,
               support_count DESC, updated_at DESC LIMIT ?""",
            (database_ref, access_scope_ref, schema_ref, int(limit)),
        ).fetchall()
        episode_rows = conn.execute(
            """SELECT * FROM memory_episodes WHERE database_ref=? AND access_scope_ref=?
               AND schema_fingerprint=? ORDER BY created_at DESC LIMIT ?""",
            (database_ref, access_scope_ref, schema_ref, int(limit)),
        ).fetchall()
    return {
        "version": "dbquill-layered-memory-v1",
        "policy": policy_view(),
        "schemaFingerprint": schema_ref,
        "counts": counts,
        "staleEpisodeCount": int(stale["count"]),
        "facts": [_fact_view(row) for row in fact_rows],
        "strategies": [_strategy_view(row) for row in strategy_rows],
        "episodes": [_episode_view(row) for row in episode_rows],
    }


def set_strategy_enabled(
    strategy_id: str,
    *,
    database_ref: str,
    access_scope_ref: str,
    schema_ref: str,
    enabled: bool,
) -> dict:
    with _LOCK, closing(_connect()) as conn:
        row = conn.execute(
            """SELECT * FROM memory_strategies WHERE id=? AND database_ref=?
               AND access_scope_ref=? AND schema_fingerprint=?""",
            (strategy_id, database_ref, access_scope_ref, schema_ref),
        ).fetchone()
        if row is None:
            raise ValueError("memory strategy not found")
        if enabled:
            status = (
                "promoted" if int(row["support_count"]) >= (
                    PROMOTION_SUPPORT * (int(row["correction_count"]) + 1)
                ) else "candidate"
            )
        else:
            status = "disabled"
        now = _now()
        conn.execute(
            """UPDATE memory_strategies SET status=?, promoted_at=CASE WHEN ?='promoted'
               THEN COALESCE(promoted_at, ?) ELSE NULL END, updated_at=? WHERE id=?""",
            (status, status, now, now, strategy_id),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM memory_strategies WHERE id=?", (strategy_id,)).fetchone()
    return _strategy_view(updated)


def _remaining_route_evidence(
    conn: sqlite3.Connection, column: str, target_id: str,
) -> tuple[int, int, list[str]]:
    if column not in {"fact_id", "strategy_id"}:
        raise ValueError("invalid route evidence column")
    rows = conn.execute(
        f"""SELECT tokens_json, corrected FROM memory_episodes
            WHERE {column}=? AND outcome='succeeded' ORDER BY created_at DESC""",
        (target_id,),
    ).fetchall()
    tokens: list[str] = []
    for row in rows:
        for token in _load_json(row["tokens_json"], []):
            if token not in tokens:
                tokens.append(token)
            if len(tokens) >= 36:
                break
        if len(tokens) >= 36:
            break
    return len(rows), sum(int(row["corrected"]) for row in rows), tokens


def _refresh_fact_after_episode_removal(
    conn: sqlite3.Connection, fact_id: str,
) -> None:
    row = conn.execute("SELECT * FROM memory_facts WHERE id=?", (fact_id,)).fetchone()
    if row is None:
        return
    support, corrections, tokens = _remaining_route_evidence(conn, "fact_id", fact_id)
    conn.execute("DELETE FROM memory_index WHERE layer='l2' AND target_id=?", (fact_id,))
    if support <= 0:
        conn.execute("DELETE FROM memory_facts WHERE id=?", (fact_id,))
        return
    now = _now()
    conn.execute(
        """UPDATE memory_facts SET support_count=?, correction_count=?,
           trigger_tokens_json=?, status='active', updated_at=? WHERE id=?""",
        (support, corrections, _json(tokens), now, fact_id),
    )
    _index_targets(
        conn, row["database_ref"], row["access_scope_ref"],
        row["schema_fingerprint"], tokens, "l2", fact_id, 1.3,
    )


def _refresh_strategy_after_episode_removal(
    conn: sqlite3.Connection, strategy_id: str,
) -> None:
    row = conn.execute(
        "SELECT * FROM memory_strategies WHERE id=?", (strategy_id,),
    ).fetchone()
    if row is None:
        return
    support, corrections, tokens = _remaining_route_evidence(
        conn, "strategy_id", strategy_id,
    )
    conn.execute(
        "DELETE FROM memory_index WHERE layer='l3' AND target_id=?", (strategy_id,),
    )
    if support <= 0:
        conn.execute("DELETE FROM memory_strategies WHERE id=?", (strategy_id,))
        return
    status = str(row["status"])
    if status != "disabled":
        status = (
            "promoted"
            if support >= PROMOTION_SUPPORT * (corrections + 1)
            else "candidate"
        )
    now = _now()
    promoted_at = (row["promoted_at"] or now) if status == "promoted" else None
    conn.execute(
        """UPDATE memory_strategies SET support_count=?, correction_count=?,
           trigger_tokens_json=?, status=?, promoted_at=?, updated_at=? WHERE id=?""",
        (support, corrections, _json(tokens), status, promoted_at, now, strategy_id),
    )
    _index_targets(
        conn, row["database_ref"], row["access_scope_ref"],
        row["schema_fingerprint"], tokens, "l3", strategy_id, 1.6,
    )


def _delete_episode_rows(
    conn: sqlite3.Connection, rows: Iterable[sqlite3.Row],
) -> None:
    fact_ids: set[str] = set()
    strategy_ids: set[str] = set()
    for row in rows:
        conn.execute(
            "DELETE FROM memory_index WHERE layer='l4' AND target_id=?", (row["id"],),
        )
        conn.execute("DELETE FROM memory_episodes WHERE id=?", (row["id"],))
        if str(row["outcome"]) == "succeeded" and row["fact_id"]:
            fact_ids.add(str(row["fact_id"]))
        if str(row["outcome"]) == "succeeded" and row["strategy_id"]:
            strategy_ids.add(str(row["strategy_id"]))
    for fact_id in fact_ids:
        _refresh_fact_after_episode_removal(conn, fact_id)
    for strategy_id in strategy_ids:
        _refresh_strategy_after_episode_removal(conn, strategy_id)


def delete_session(
    *, database_ref: str, access_scope_ref: str, session_id: str,
) -> int:
    sess_ref = session_ref(session_id)
    with _LOCK, closing(_connect()) as conn:
        rows = conn.execute(
            """SELECT id, fact_id, strategy_id, outcome, corrected
               FROM memory_episodes WHERE database_ref=? AND access_scope_ref=?
               AND session_ref=?""",
            (database_ref, access_scope_ref, sess_ref),
        ).fetchall()
        _delete_episode_rows(conn, rows)
        conn.commit()
    return len(rows)


def clear_scope(
    *, database_ref: str, access_scope_ref: str, schema_ref: Optional[str] = None,
) -> dict:
    clauses = "database_ref=? AND access_scope_ref=?"
    params: list[Any] = [database_ref, access_scope_ref]
    if schema_ref:
        clauses += " AND schema_fingerprint=?"
        params.append(schema_ref)
    deleted = {}
    with _LOCK, closing(_connect()) as conn:
        for key, table in (("l1", "memory_index"), ("l4", "memory_episodes"), ("l2", "memory_facts"), ("l3", "memory_strategies")):
            cursor = conn.execute(f"DELETE FROM {table} WHERE {clauses}", params)
            deleted[key] = max(0, int(cursor.rowcount))
        conn.commit()
    return deleted


init_db()
