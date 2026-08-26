"""DB Agent 会话持久化 store（SQLite 落盘）。

- 数据目录：本文件同目录 data/db_sessions.db（WAL 模式）
- 表 sessions：会话元信息（含 title 供重命名/搜索）
- 表 messages：多轮消息（user/assistant）
- 线程安全：每次操作新建连接（bridge 内多线程 + aiohttp 事件循环共用）
- 兼容旧内存态：bridge 内存 _DB_SESSIONS 继续作热缓存，本 store 为权威持久层
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "db_sessions.db"
_HISTORY_MAX = 14  # 与 desktop_bridge._DB_SESSION_HISTORY_MAX 对齐
_DISPLAY_PAYLOAD_MAX_BYTES = 768 * 1024


@contextmanager
def _connect():
    """事务连接上下文；退出时提交/回滚并显式关闭 Windows 文件句柄。"""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
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
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                title TEXT DEFAULT '',
                db_id TEXT DEFAULT '',
                access_scope_ref TEXT NOT NULL DEFAULT 'all',
                last_question TEXT DEFAULT '',
                count INTEGER DEFAULT 0,
                created_at REAL DEFAULT 0,
                updated_at REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT DEFAULT '',
                display_payload TEXT DEFAULT '',
                created_at REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_sid ON messages(session_id, id)"
        )
        message_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(messages)").fetchall()
        }
        if "display_payload" not in message_columns:
            conn.execute(
                "ALTER TABLE messages ADD COLUMN display_payload TEXT DEFAULT ''"
            )
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        if "access_scope_ref" not in columns:
            conn.execute(
                "ALTER TABLE sessions "
                "ADD COLUMN access_scope_ref TEXT NOT NULL DEFAULT 'all'"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_clarifications (
                session_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at REAL DEFAULT 0
            )
            """
        )


def upsert_session(
    sid: str,
    db_id: str = "",
    last_question: str = "",
    count: int = 0,
    created_at: float | None = None,
    updated_at: float | None = None,
    title: str = "",
    access_scope_ref: str = "all",
) -> None:
    now = time.time()
    created_at = created_at if created_at is not None else now
    updated_at = updated_at if updated_at is not None else now
    if not title:
        title = last_question[:30] or "未命名会话"
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sessions (
                id, title, db_id, access_scope_ref, last_question,
                count, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                db_id = excluded.db_id,
                access_scope_ref = excluded.access_scope_ref,
                last_question = excluded.last_question,
                count = excluded.count,
                updated_at = excluded.updated_at,
                title = CASE WHEN sessions.title = '' THEN excluded.title ELSE sessions.title END
            """,
            (
                sid, title, db_id, access_scope_ref or "all", last_question,
                count, created_at, updated_at,
            ),
        )


def append_message(
    sid: str,
    role: str,
    content: str,
    display_payload: dict | None = None,
) -> None:
    """Append model context text plus an optional UI-only result snapshot.

    ``content`` remains the compact text consumed by ``get_history``.  The
    structured snapshot is returned only by ``get_session`` so switching chats
    can reconstruct read-only tables without expanding the LLM context.
    """
    payload_text = ""
    if isinstance(display_payload, dict):
        candidate = json.dumps(
            display_payload, ensure_ascii=False, separators=(",", ":"),
        )
        if len(candidate.encode("utf-8")) <= _DISPLAY_PAYLOAD_MAX_BYTES:
            payload_text = candidate
    with _connect() as conn:
        conn.execute(
            "INSERT INTO messages "
            "(session_id, role, content, display_payload, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (sid, role, content[:4000], payload_text, time.time()),
        )
        # 裁剪每会话消息条数（防无限膨胀）：保留最近 _HISTORY_MAX * 2 条
        conn.execute(
            """
            DELETE FROM messages WHERE session_id = ? AND id NOT IN (
                SELECT id FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?
            )
            """,
            (sid, sid, _HISTORY_MAX * 2),
        )


def get_history(sid: str, limit: int = _HISTORY_MAX) -> list:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
            (sid, limit),
        ).fetchall()
    return [{"role": r[0], "content": r[1]} for r in reversed(rows)]


def set_pending_clarification(sid: str, payload: dict) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO pending_clarifications (session_id, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (sid, serialized, time.time()),
        )


def get_pending_clarification(sid: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT payload FROM pending_clarifications WHERE session_id = ?",
            (sid,),
        ).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(row[0])
        return value if isinstance(value, dict) else None
    except (TypeError, json.JSONDecodeError):
        return None


def clear_pending_clarification(sid: str) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM pending_clarifications WHERE session_id = ?", (sid,))


def get_session(sid: str, *, include_messages: bool = True) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, title, db_id, access_scope_ref, last_question, "
            "count, created_at, updated_at FROM sessions WHERE id = ?",
            (sid,),
        ).fetchone()
        if row is None:
            return None
        msgs = conn.execute(
            "SELECT role, content, display_payload FROM messages "
            "WHERE session_id = ? ORDER BY id",
            (sid,),
        ).fetchall() if include_messages else []
    messages = []
    for message in msgs:
        item = {"role": message[0], "content": message[1]}
        if message[2]:
            try:
                payload = json.loads(message[2])
                if isinstance(payload, dict):
                    item["display"] = payload
            except (TypeError, json.JSONDecodeError):
                pass
        messages.append(item)
    return {
        "id": row[0],
        "title": row[1],
        "dbId": row[2],
        "accessScopeRef": row[3] or "all",
        "lastQuestion": row[4],
        "count": row[5],
        "createdAt": row[6],
        "updatedAt": row[7],
        "messages": messages,
    }


def list_sessions(q: str = "", limit: int = 200) -> list:
    with _connect() as conn:
        if q:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT id, title, db_id, access_scope_ref, last_question,
                       count, created_at, updated_at
                FROM sessions
                WHERE title LIKE ? OR last_question LIKE ? OR db_id LIKE ?
                ORDER BY updated_at DESC LIMIT ?
                """,
                (like, like, like, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, title, db_id, access_scope_ref, last_question,
                       count, created_at, updated_at
                FROM sessions ORDER BY updated_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return [
        {
            "id": r[0],
            "title": r[1],
            "dbId": r[2],
            "accessScopeRef": r[3] or "all",
            "lastQuestion": r[4],
            "count": r[5],
            "createdAt": r[6],
            "updatedAt": r[7],
        }
        for r in rows
    ]


def rename_session(sid: str, title: str) -> bool:
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE sessions SET title = ?, updated_at = ? WHERE id = ?",
            (title[:100], time.time(), sid),
        )
        return cur.rowcount > 0


def delete_session(sid: str) -> bool:
    with _connect() as conn:
        conn.execute("DELETE FROM pending_clarifications WHERE session_id = ?", (sid,))
        conn.execute("DELETE FROM messages WHERE session_id = ?", (sid,))
        cur = conn.execute("DELETE FROM sessions WHERE id = ?", (sid,))
        return cur.rowcount > 0


def touch_updated_at(sid: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE sessions SET updated_at = ? WHERE id = ?", (time.time(), sid)
        )
