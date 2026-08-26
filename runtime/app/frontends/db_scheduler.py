"""DBQuill 只读定时操作调度器（bridge 进程内，threading 轮询）。

- 任务 JSON：frontends/data/db_sched_tasks/{task_id}.json
- 执行日志：frontends/data/db_sched_logs/YYYY-MM-DD_{任务名}.md（append，审计风格）
- 调度模式：
  - once      : {"mode":"once","runAt":"ISO"}        到期执行一次后 enabled=False
  - interval  : {"mode":"interval","minutes":N}       每 N 分钟
  - daily     : {"mode":"daily","hour":H,"minute":M}  每天 HH:MM
  - weekly    : {"mode":"weekly","weekday":0-6,"hour":H,"minute":M} 每周几 HH:MM
- 任务类型：
  - sql : 定时执行单条只读 SELECT/WITH 查询
  - nl  : 定时向 DBQuill 提问；若规划为写操作，只提示需要交互确认，不自动批准
- 线程安全：本模块所有公共函数带锁；执行在线程中运行不阻塞 aiohttp
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent / "data"
_TASKS_DIR = _DATA_DIR / "db_sched_tasks"
_LOGS_DIR = _DATA_DIR / "db_sched_logs"
_POLL_SECONDS = 60
_lock = threading.RLock()
_stop_evt = threading.Event()
_thread: threading.Thread | None = None
_resolver = None  # dbId -> {"path": str, "conn": dict|None}
_audit_sink = None  # callable(**event) -> dict|None
_SAFE_LOG_MARKER = "<!-- DBAGENT_SAFE_SCHED_LOG_V2 -->"


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ts() -> float:
    return time.time()


def _sanitize_name(name: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff-]+", "_", name)[:60] or "task"


def _short_ref(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()[:16]


def _task_file(tid: str) -> Path:
    return _TASKS_DIR / f"{tid}.json"


def _load_task(tid: str) -> dict | None:
    try:
        return json.loads(_task_file(tid).read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_task(task: dict) -> None:
    _TASKS_DIR.mkdir(parents=True, exist_ok=True)
    _task_file(task["id"]).write_text(
        json.dumps(task, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _next_run_at(task: dict) -> float | None:
    """计算任务的下一次应执行时间戳（epoch）；无法调度返回 None。"""
    if not task.get("enabled", True):
        return None
    sch = task.get("schedule") or {}
    mode = sch.get("mode", "interval")
    now = datetime.now()
    if mode == "once":
        run_at = sch.get("runAt")
        if run_at:
            try:
                t = datetime.fromisoformat(run_at)
            except Exception:
                t = now + timedelta(seconds=60)
            return t.timestamp() if t.timestamp() > now else now.timestamp()
        # 无 runAt：默认 1 分钟后
        return (now + timedelta(seconds=60)).timestamp()
    if mode == "interval":
        minutes = max(1, int(sch.get("minutes") or 60))
        last = task.get("lastRunAtTs") or task.get("createdAtTs") or now.timestamp()
        return last + minutes * 60
    if mode in ("daily", "weekly"):
        hour = int(sch.get("hour") or 0)
        minute = int(sch.get("minute") or 0)
        weekday = int(sch.get("weekday") or 0)
        nxt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        if mode == "weekly":
            delta = (weekday - nxt.weekday()) % 7
            if delta == 0 and nxt <= now:
                delta = 7
            nxt += timedelta(days=delta)
        return nxt.timestamp()
    return None


# ---------------------------------------------------------------------------
# 执行器
# ---------------------------------------------------------------------------
def _execute_sql(path: str, sql: str) -> dict:
    """Execute one bounded physical read-only query."""
    sql_stripped = (sql or "").strip().rstrip(";")
    if not sql_stripped:
        return {"ok": False, "error": "SQL 为空"}
    import dbquill_core as dc

    connector = dc.DBConnector(path)
    result = dc.SQLSecurity(connector, max_rows=100, timeout_s=15.0).execute(sql_stripped)
    if result.error:
        blocked_write = bool(re.search(
            r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE)\b",
            sql_stripped, re.IGNORECASE,
        ))
        return {
            "ok": False,
            "error": result.error,
            "blocked_write": blocked_write,
            "requires_confirmation": blocked_write,
        }
    return {
        "ok": True,
        "columns": result.columns,
        "rows": result.rows,
        "affected": result.row_count,
        "truncated": result.truncated,
    }


def _validate_scheduled_sql(sql: str) -> None:
    import dbquill_core as dc

    try:
        dc.SQLSecurity(None, max_rows=100, timeout_s=15.0).validate(sql)
    except Exception as exc:
        raise ValueError(
            "定时 SQL 仅允许单条 SELECT/WITH 查询；写操作必须在对话中预览并显式确认"
        ) from exc


def _emit_audit(
    task: dict,
    *,
    category: str,
    action: str,
    outcome: str,
    summary: str,
    risk: str,
    run_id: str = "",
    correlation_id: str = "",
    details: dict | None = None,
    actor: str = "scheduler",
    strict: bool = False,
):
    if _audit_sink is None:
        if strict:
            raise RuntimeError("审计账本不可用")
        return None
    try:
        return _audit_sink(
            db_id=str(task.get("dbId") or ""),
            category=category,
            action=action,
            outcome=outcome,
            summary=summary,
            risk=risk,
            run_id=run_id,
            correlation_id=correlation_id,
            details=details or {},
            actor=actor,
            strict=strict,
        )
    except Exception:
        if strict:
            raise
        return None


def _resolve(path_or_db: str) -> str | None:
    """把任务里的 dbId 解析为本地文件路径（远程连接任务仅支持 SQL 型且用连接执行时抛错提示）。"""
    if not path_or_db:
        return None
    if os.path.isfile(path_or_db):
        return path_or_db
    if _resolver:
        entry = _resolver(path_or_db)
        if entry:
            p = entry.get("path")
            if p and os.path.isfile(p):
                return p
            if entry.get("conn"):
                raise ValueError("远程连接数据库暂不支持定时 SQL 直连；请将数据库文件挂载后再建定时任务")
    return None


def run_task_once(task: dict, *, trigger: str = "scheduled") -> dict:
    """执行单个任务，返回执行结果摘要（也会写日志文件 + 更新任务状态）。"""
    tid = task["id"]
    tname = task.get("name") or tid
    db_id = task.get("dbId") or ""
    ttype = task.get("type", "sql")
    run_id = uuid.uuid4().hex[:20]
    schedule_ref = _short_ref(tid)
    audit_details = {
        "schedule_ref": schedule_ref,
        "task_type": ttype,
        "trigger": "manual" if trigger == "manual" else "scheduled",
    }
    intent_event = _emit_audit(
        task,
        category="schedule_execution",
        action="run",
        outcome="pending",
        summary="定时只读任务开始执行",
        risk="low",
        run_id=run_id,
        correlation_id=run_id,
        details=audit_details,
    )
    started = _ts()
    result: dict = {"ok": False, "summary": "", "error": None}
    try:
        path = _resolve(db_id)
        if not path:
            raise ValueError(f"无法解析数据库: {db_id}")
        if ttype == "sql":
            sql = task.get("sql") or ""
            r = _execute_sql(path, sql)
            result = r
            result["summary"] = (
                f"SQL 执行成功，影响 {r.get('affected', 0)} 行"
                if r.get("ok")
                else f"SQL 执行失败: {r.get('error')}"
            )
        else:  # nl
            prompt = task.get("prompt") or ""
            if not prompt:
                raise ValueError("自然语言任务缺少 prompt")
            result = _run_nl(path, prompt)
    except Exception as exc:
        result = {
            "ok": False,
            "summary": "定时任务执行异常",
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
        traceback.print_exc()
    result["took_s"] = round(_ts() - started, 1)
    result["runAt"] = _now_iso()
    result["taskId"] = tid
    result["taskName"] = tname
    result["runId"] = run_id
    terminal_details = dict(audit_details)
    if isinstance(result.get("rows"), list):
        terminal_details["result_rows"] = len(result["rows"])
    if result.get("blocked_write"):
        terminal_details["blocked_write"] = True
    if result.get("error"):
        terminal_details["error_type"] = str(result.get("error_type") or "TaskExecutionError")
        terminal_details["error_sha256"] = hashlib.sha256(
            str(result["error"]).encode("utf-8")
        ).hexdigest()
    outcome = "succeeded" if result.get("ok") else (
        "rejected" if result.get("requires_confirmation") else "failed"
    )
    result_event = _emit_audit(
        task,
        category="schedule_execution",
        action="run",
        outcome=outcome,
        summary=(
            "定时任务因写操作需交互确认而停止"
            if result.get("requires_confirmation")
            else "定时只读任务执行完成"
        ),
        risk="high" if result.get("requires_confirmation") else "low",
        run_id=run_id,
        correlation_id=run_id,
        details=terminal_details,
    )
    if intent_event is None or result_event is None:
        result["audit_warning"] = True
    _write_log(task, result)
    _update_task_status(task, result)
    return result


def _run_nl(path: str, prompt: str) -> dict:
    """NL task: execute reads; stop at the write confirmation boundary."""
    import dbquill_core as dc

    agent = dc.DBQuillAgent(
        llm_cfg=(
            os.environ.get("DBQUILL_MODEL_PROFILE")
            or os.environ.get("DBAGENT_MODEL_PROFILE")
            or "default"
        ),
        db_path=path,
        sample_rows=3,
        max_rows=500,
        timeout_s=15.0,
    )
    ans = agent.ask(prompt, history=None)
    if getattr(ans, "kind", "error") == "clarification":
        clarification = getattr(ans, "clarification", None) or {}
        missing = clarification.get("missing_label") or "必要信息"
        return {
            "ok": False,
            "summary": f"自然语言任务需要补充{missing}，未执行任何数据库操作",
            "error": f"任务指令存在歧义：缺少{missing}",
        }
    if getattr(ans, "confirm_id", None):
        return {
            "ok": False,
            "summary": "任务已停止：写操作必须在对话中查看预览并显式确认",
            "error": "定时自然语言任务不能自动批准写操作",
            "error_type": "ScheduledWriteRequiresConfirmation",
            "blocked_write": True,
            "requires_confirmation": True,
        }
    return {
        "ok": getattr(ans, "kind", "error") != "error",
        "summary": (getattr(ans, "narrative", "") or "")[:2000],
        "columns": list(getattr(ans, "columns", []) or []),
        "rows": list(getattr(ans, "rows", []) or [])[:50],
        "sql": getattr(ans, "sql", None),
    }


def _update_task_status(task: dict, result: dict) -> None:
    with _lock:
        saved = _load_task(task["id"])
        if saved is None:
            return
        saved["lastRunAt"] = _now_iso()
        saved["lastRunAtTs"] = _ts()
        saved["lastStatus"] = "ok" if result.get("ok") else "error"
        saved["lastError"] = (
            str(result.get("error_type") or "TaskExecutionError")
            if result.get("error") else None
        )
        saved["lastSummary"] = (
            "写操作需要交互确认，未执行"
            if result.get("requires_confirmation")
            else ("只读任务执行成功" if result.get("ok") else "只读任务执行失败")
        )
        saved["runCount"] = int(saved.get("runCount") or 0) + 1
        if saved.get("schedule", {}).get("mode") == "once":
            saved["enabled"] = False
        _save_task(saved)


def _write_log(task: dict, result: dict) -> None:
    """Append only non-sensitive run metadata; the audit ledger is authoritative."""
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{datetime.now():%Y-%m-%d}_runs.md"
    fp = _LOGS_DIR / fname
    if not fp.exists():
        fp.write_text(_SAFE_LOG_MARKER + "\n# 脱敏调度运行索引\n", encoding="utf-8")
    with open(fp, "a", encoding="utf-8") as f:
        f.write(f"\n## 执行时间：{result.get('runAt')}\n")
        f.write(f"- 任务引用：{_short_ref(result.get('taskId'))}\n")
        f.write(f"- 运行引用：{_short_ref(result.get('runId'))}\n")
        f.write(f"- 类型：{task.get('type')} | 状态：{'✅ 成功' if result.get('ok') else '❌ 失败'}\n")
        f.write(f"- 耗时：{result.get('took_s')}s\n")
        f.write(f"- 结论：{'需要交互确认，未执行写入' if result.get('requires_confirmation') else ('完成' if result.get('ok') else '失败')}\n")
        f.write("\n")


def _redact_legacy_logs() -> int:
    """Remove raw SQL/questions/results left by the legacy Markdown logger."""
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    redacted = 0
    for fp in sorted(_LOGS_DIR.glob("*.md")):
        try:
            raw = fp.read_text(encoding="utf-8")
        except Exception:
            continue
        if raw.startswith(_SAFE_LOG_MARKER):
            continue
        entry_count = len(re.findall(r"^## 执行时间：", raw, re.MULTILINE))
        replacement = (
            _SAFE_LOG_MARKER
            + "\n# 历史调度日志（已脱敏）\n\n"
            + f"旧版日志中的原始 SQL、问题、结果与错误详情已移除；保留执行段数量：{entry_count}。\n"
        )
        target = _LOGS_DIR / f"legacy_{_short_ref(fp.name)}.md"
        temporary = _LOGS_DIR / f".{uuid.uuid4().hex}.tmp"
        temporary.write_text(replacement, encoding="utf-8")
        os.replace(temporary, target)
        if fp.resolve() != target.resolve() and fp.exists():
            fp.unlink()
        redacted += 1
    return redacted


def _disable_legacy_write_tasks() -> int:
    disabled = 0
    for fp in sorted(_TASKS_DIR.glob("*.json")):
        task = _load_task(fp.stem)
        if not task or task.get("type") != "sql" or not task.get("enabled", True):
            continue
        try:
            _validate_scheduled_sql(str(task.get("sql") or ""))
        except ValueError:
            task["enabled"] = False
            task["lastStatus"] = "error"
            task["lastError"] = "ScheduledWriteRequiresConfirmation"
            task["lastSummary"] = "旧版定时写任务已停用；请在对话中预览并确认"
            _save_task(task)
            disabled += 1
    return disabled


# ---------------------------------------------------------------------------
# 调度循环
# ---------------------------------------------------------------------------
def _poll_loop() -> None:
    while not _stop_evt.is_set():
        try:
            with _lock:
                tasks = list_tasks()
            now = _ts()
            for t in tasks:
                if not t.get("enabled", True):
                    continue
                try:
                    nxt = _next_run_at(t)
                except Exception:
                    nxt = None
                if nxt is not None and nxt <= now + 1:
                    threading.Thread(
                        target=run_task_once,
                        args=(t,),
                        kwargs={"trigger": "scheduled"},
                        daemon=True,
                    ).start()
                    # 防止同一分钟重复触发：interval 任务由 lastRunAtTs 推进；once 由 enabled 关闭
        except Exception:
            traceback.print_exc()
        _stop_evt.wait(_POLL_SECONDS)


def start(resolver=None, audit_sink=None) -> None:
    global _thread, _resolver, _audit_sink
    _resolver = resolver
    _audit_sink = audit_sink
    _TASKS_DIR.mkdir(parents=True, exist_ok=True)
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    redacted_logs = _redact_legacy_logs()
    disabled_tasks = _disable_legacy_write_tasks()
    if redacted_logs or disabled_tasks:
        details = {}
        if redacted_logs:
            details["legacy_log_count"] = redacted_logs
        if disabled_tasks:
            details["target_count"] = disabled_tasks
        _emit_audit(
            {}, category="system", action="scheduler_safety_migration",
            outcome="succeeded", summary="定时任务安全迁移已应用", risk="medium",
            details=details, actor="system",
        )
    with _lock:
        if _thread is None or not _thread.is_alive():
            _stop_evt.clear()
            _thread = threading.Thread(target=_poll_loop, name="db-scheduler", daemon=True)
            _thread.start()


def stop() -> None:
    _stop_evt.set()


# ---------------------------------------------------------------------------
# CRUD（供 bridge HTTP handler 调用）
# ---------------------------------------------------------------------------
def list_tasks() -> list:
    _TASKS_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    for fp in sorted(_TASKS_DIR.glob("*.json")):
        try:
            t = json.loads(fp.read_text(encoding="utf-8"))
            out.append(_decorate(t))
        except Exception:
            continue
    return sorted(out, key=lambda x: x.get("createdAtTs") or 0, reverse=True)


def _decorate(t: dict) -> dict:
    t = dict(t)
    t["nextRunAt"] = None
    try:
        nxt = _next_run_at(t)
        if nxt:
            t["nextRunAt"] = datetime.fromtimestamp(nxt).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return t


def get_task(tid: str) -> dict | None:
    t = _load_task(tid)
    return _decorate(t) if t else None


def create_task(data: dict) -> dict:
    tid = uuid.uuid4().hex[:12]
    task_type = "sql" if str(data.get("type") or "sql") == "sql" else "nl"
    sql = str(data.get("sql") or "").strip()
    prompt = str(data.get("prompt") or "").strip()
    # Compatibility with an older cached frontend that sent natural-language
    # content through the shared `sql` field.
    if task_type == "nl" and not prompt and sql:
        prompt, sql = sql, ""
    task = {
        "id": tid,
        "name": str(data.get("name") or "未命名任务").strip()[:80],
        "dbId": str(data.get("dbId") or "").strip(),
        "type": task_type,
        "sql": sql,
        "prompt": prompt,
        "schedule": _normalize_schedule(data.get("schedule")),
        "enabled": bool(data.get("enabled", True)),
        "createdAt": _now_iso(),
        "createdAtTs": _ts(),
        "lastRunAt": None,
        "lastRunAtTs": None,
        "lastStatus": None,
        "lastError": None,
        "runCount": 0,
    }
    if task["type"] == "sql" and not task["sql"]:
        raise ValueError("SQL 任务必须填写 SQL")
    if task["type"] == "sql":
        _validate_scheduled_sql(task["sql"])
    if task["type"] == "nl" and not task["prompt"]:
        raise ValueError("自然语言任务必须填写提问内容")
    _save_task(task)
    return get_task(tid)


def update_task(tid: str, data: dict) -> dict | None:
    with _lock:
        t = _load_task(tid)
        if t is None:
            return None
        if "name" in data:
            t["name"] = str(data["name"]).strip()[:80]
        if "dbId" in data:
            t["dbId"] = str(data["dbId"]).strip()
        if "type" in data:
            t["type"] = "sql" if str(data["type"]) == "sql" else "nl"
        if "sql" in data:
            t["sql"] = str(data["sql"]).strip()
        if "prompt" in data:
            t["prompt"] = str(data["prompt"]).strip()
        if t["type"] == "nl" and not t.get("prompt") and t.get("sql"):
            t["prompt"], t["sql"] = t["sql"], ""
        if "schedule" in data:
            t["schedule"] = _normalize_schedule(data["schedule"])
        if "enabled" in data:
            t["enabled"] = bool(data["enabled"])
        if t["type"] == "sql" and not t["sql"]:
            raise ValueError("SQL 任务必须填写 SQL")
        if t["type"] == "sql":
            disabling_only = set(data).issubset({"enabled"}) and data.get("enabled") is False
            if not disabling_only:
                _validate_scheduled_sql(t["sql"])
        _save_task(t)
        return get_task(tid)


def delete_task(tid: str) -> bool:
    with _lock:
        fp = _task_file(tid)
        if fp.exists():
            fp.unlink()
            return True
        return False


def run_now(tid: str) -> dict | None:
    t = _load_task(tid)
    if t is None:
        return None
    return run_task_once(t, trigger="manual")


def _normalize_schedule(sch) -> dict:
    if not isinstance(sch, dict):
        sch = {}
    mode = str(sch.get("mode") or "interval")
    out = {"mode": mode}
    if mode == "once":
        run_at = str(sch.get("runAt") or "").strip()
        if run_at:
            try:
                out["runAt"] = datetime.fromisoformat(run_at).isoformat()
            except Exception:
                out["runAt"] = (datetime.now() + timedelta(minutes=1)).isoformat()
        else:
            out["runAt"] = (datetime.now() + timedelta(minutes=1)).isoformat()
    elif mode == "interval":
        out["minutes"] = max(1, int(sch.get("minutes") or 60))
    elif mode in ("daily", "weekly"):
        out["hour"] = min(23, max(0, int(sch.get("hour") or 0)))
        out["minute"] = min(59, max(0, int(sch.get("minute") or 0)))
        if mode == "weekly":
            out["weekday"] = min(6, max(0, int(sch.get("weekday") or 0)))
    else:
        out = {"mode": "interval", "minutes": 60}
    return out
def list_logs(limit: int = 100) -> list:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(_LOGS_DIR.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for fp in files[:limit]:
        out.append({
            "file": fp.name,
            "mtime": datetime.fromtimestamp(fp.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "summary": "脱敏运行元数据；完整事件请查看审计记录",
        })
    return out
