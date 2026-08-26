"""Run a reproducible BIRD Mini-Dev SQLite execution benchmark.

The BIRD Mini-Dev package contains real SQLite database contents, questions,
expert evidence and gold SQL.  This runner sends the question through
DBQuill's production NL2SQL and read-only SQLSecurity path, then compares the
predicted and gold result sets on a separately opened physical read-only
connection.  The primary metric follows BIRD's official Execution Accuracy
definition: duplicate rows and row ordering are ignored.

Raw public questions, SQL predictions, previews and checkpoints are written
only below benchmark_results/, which is git-ignored.  Model endpoints and API
keys are never persisted.  The runner accepts SELECT-only Mini-Dev data; it
does not run or auto-confirm CRUD statements.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import io
import json
import math
import os
import random
import sqlite3
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

import dbquill_core as dc  # noqa: E402
from nl2db_evaluation import _redacted_model_identity  # noqa: E402


DEFAULT_PACKAGE_ROOT = (
    ROOT / "benchmark_data" / "bird_minidev_package" / "minidev" / "MINIDEV"
)
DEFAULT_OFFICIAL_REPO = ROOT / "benchmark_data" / "bird_mini_dev_official_repo"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "bird_minidev.json"
DEFAULT_MARKDOWN = ROOT / "benchmark_results" / "bird_minidev.md"
DEFAULT_SEED = 20260821
DIFFICULTY_ORDER = ("simple", "moderate", "challenging")
SCORING_CONTRACT = "bird-official-ex-set-v1"
EVIDENCE_CONTRACT = "question-plus-expert-evidence-v1"
DESCRIPTION_CONTRACT = "question-relevant-column-dictionary-v1"
RELATION_CONTRACT = "sqlite-pragma-plus-official-declared-fk-v1"
INFRASTRUCTURE_ERROR_CATEGORY = "llm_infrastructure_error"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text.rstrip() + "\n", encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _git_commit(repo: Path) -> str | None:
    if not (repo / ".git").exists():
        return None
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def _load_cases(dataset_path: Path) -> list[dict]:
    raw = json.loads(dataset_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("BIRD Mini-Dev 数据集必须是非空 JSON 数组")
    prepared: list[dict] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"BIRD case {index} 不是对象")
        difficulty = str(item.get("difficulty") or "").strip().lower()
        if difficulty not in DIFFICULTY_ORDER:
            raise ValueError(f"BIRD case {index} 难度无效: {difficulty}")
        db_id = str(item.get("db_id") or "").strip()
        question = str(item.get("question") or "").strip()
        gold_sql = str(item.get("SQL") or "").strip()
        if not db_id or not question or not gold_sql:
            raise ValueError(f"BIRD case {index} 缺少 db_id/question/SQL")
        prepared.append({
            "id": f"bird-mini-{int(item.get('question_id', index)):04d}",
            "index": index,
            "question_id": int(item.get("question_id", index)),
            "db_id": db_id,
            "difficulty": difficulty,
            "question": question,
            "evidence": str(item.get("evidence") or "").strip(),
            "gold_sql": gold_sql,
        })
    return prepared


def _round_robin_sample(
    cases: list[dict], quota: int, seed: int, difficulty: str,
) -> list[dict]:
    by_db: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        by_db[case["db_id"]].append(case)
    rng = random.Random(f"{seed}:{difficulty}")
    db_names = sorted(by_db)
    rng.shuffle(db_names)
    for db_id in db_names:
        rng.shuffle(by_db[db_id])
    selected: list[dict] = []
    while len(selected) < quota:
        advanced = False
        for db_id in db_names:
            if by_db[db_id] and len(selected) < quota:
                selected.append(by_db[db_id].pop())
                advanced = True
        if not advanced:
            break
    return selected


def _select_cases(cases: list[dict], sample_size: int, seed: int) -> list[dict]:
    if sample_size <= 0 or sample_size >= len(cases):
        return list(cases)
    base, remainder = divmod(sample_size, len(DIFFICULTY_ORDER))
    selected: list[dict] = []
    for index, difficulty in enumerate(DIFFICULTY_ORDER):
        quota = base + (1 if index < remainder else 0)
        bucket = [case for case in cases if case["difficulty"] == difficulty]
        selected.extend(_round_robin_sample(bucket, quota, seed, difficulty))
    return sorted(selected, key=lambda case: int(case["index"]))


def _database_path(package_root: Path, db_id: str) -> Path:
    path = package_root / "dev_databases" / db_id / f"{db_id}.sqlite"
    if not path.is_file():
        raise FileNotFoundError(f"BIRD SQLite 数据库不存在: {path}")
    return path.resolve()


def _description_files(db_path: Path) -> list[Path]:
    root = db_path.parent / "database_description"
    return sorted(root.glob("*.csv"), key=lambda item: item.name.casefold()) if root.is_dir() else []


def _description_fingerprint(db_path: Path) -> str | None:
    files = _description_files(db_path)
    if not files:
        return None
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_sha256_file(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _apply_column_descriptions(db_path: Path, schema: dc.SchemaSnapshot) -> dict:
    """Load BIRD's public CSV dictionaries into generic DBColumn metadata."""
    tables = {name.casefold(): table for name, table in schema.tables.items()}
    loaded_files = 0
    loaded_columns = 0
    for path in _description_files(db_path):
        table = tables.get(path.stem.casefold())
        if table is None:
            continue
        columns = {column.name.casefold(): column for column in table.columns}
        raw = path.read_bytes()
        try:
            decoded = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            # The official Mini-Dev package contains a small number of legacy
            # Windows-1252 bullets/quotes.  The fallback is deterministic and
            # the original file bytes remain bound by description_sha256.
            decoded = raw.decode("cp1252")
        for row in csv.DictReader(io.StringIO(decoded, newline="")):
            physical = str(
                row.get("original_column_name") or row.get("column_name") or ""
            ).strip().strip('"`[]')
            column = columns.get(physical.casefold())
            if column is None:
                continue
            column.semantic_name = str(row.get("column_name") or "").strip()
            column.description = str(row.get("column_description") or "").strip()
            column.value_description = str(row.get("value_description") or "").strip()
            if column.semantic_name or column.description or column.value_description:
                loaded_columns += 1
        loaded_files += 1
    return {"files": loaded_files, "columns": loaded_columns}


def _apply_declared_foreign_keys(schema_record: dict, schema: dc.SchemaSnapshot) -> int:
    """Restore relationships declared by BIRD metadata but absent from SQLite DDL."""
    table_names = list(schema_record.get("table_names_original") or [])
    column_names = list(schema_record.get("column_names_original") or [])
    tables = {name.casefold(): table for name, table in schema.tables.items()}
    applied = 0
    for pair in schema_record.get("foreign_keys") or []:
        if not isinstance(pair, list) or len(pair) != 2:
            continue
        source_index, target_index = (int(pair[0]), int(pair[1]))
        if not (0 <= source_index < len(column_names) and 0 <= target_index < len(column_names)):
            continue
        source_table_index, source_column_name = column_names[source_index]
        target_table_index, target_column_name = column_names[target_index]
        if not (
            0 <= int(source_table_index) < len(table_names)
            and 0 <= int(target_table_index) < len(table_names)
        ):
            continue
        source_table = tables.get(str(table_names[int(source_table_index)]).casefold())
        target_table = tables.get(str(table_names[int(target_table_index)]).casefold())
        if source_table is None or target_table is None:
            continue
        source_column = next((
            column for column in source_table.columns
            if column.name.casefold() == str(source_column_name).casefold()
        ), None)
        target_column = next((
            column for column in target_table.columns
            if column.name.casefold() == str(target_column_name).casefold()
        ), None)
        if source_column is None or target_column is None:
            continue
        source_column.fk_table = target_table.name
        source_column.fk_column = target_column.name
        applied += 1
    return applied


def _model_question(case: dict, include_evidence: bool) -> str:
    question = str(case["question"]).strip()
    evidence = str(case.get("evidence") or "").strip()
    if include_evidence and evidence:
        return (
            question
            + "\nRelevant business evidence supplied by the user: "
            + evidence
        )
    return question


class _CapturingSQLSecurity(dc.SQLSecurity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_sql = ""

    def execute(self, sql: str) -> dc.SQLResult:
        self.raw_sql = str(sql or "").strip()
        return super().execute(sql)


def _safe_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return f"<blob {len(value)}B:{hashlib.sha256(value).hexdigest()[:12]}>"
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and len(value) > 240:
            return value[:237] + "..."
        return value
    return str(value)


def _preview(rows: list[tuple], limit: int = 3) -> list[list[Any]]:
    return [[_safe_value(value) for value in row] for row in rows[:limit]]


def _result_hash(rows: list[tuple]) -> str:
    normalized = [
        json.dumps([_safe_value(value) for value in row], ensure_ascii=False, separators=(",", ":"))
        for row in rows
    ]
    normalized.sort()
    return _sha256_bytes("\n".join(normalized).encode("utf-8"))


def _execute_for_score(
    db_path: Path,
    sql: str,
    timeout_s: float,
    max_rows: int,
) -> dict:
    started = time.perf_counter()
    connector = dc.DBConnector(str(db_path))
    validator = dc.SQLSecurity(connector, max_rows=max_rows, timeout_s=timeout_s)
    try:
        validator.validate(sql)
    except Exception as exc:
        return {
            "rows": [],
            "columns": [],
            "error": f"{type(exc).__name__}: {exc}",
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    conn = connector.connect()
    try:
        deadline = time.monotonic() + timeout_s

        def _check_progress() -> int:
            return 1 if time.monotonic() > deadline else 0

        conn.set_progress_handler(_check_progress, 1000)
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [str(item[0]) for item in (cursor.description or [])]
        rows: list[tuple] = []
        while True:
            chunk = cursor.fetchmany(min(2048, max_rows + 1 - len(rows)))
            if not chunk:
                break
            rows.extend(tuple(row) for row in chunk)
            if len(rows) > max_rows:
                raise RuntimeError(f"result exceeds evaluation cap ({max_rows} rows)")
        return {
            "rows": rows,
            "columns": columns,
            "error": None,
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    except Exception as exc:
        message = str(exc)
        if "interrupted" in message.lower():
            message = f"query timeout (>{timeout_s}s)"
        return {
            "rows": [],
            "columns": [],
            "error": f"{type(exc).__name__}: {message}",
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    finally:
        connector.close(conn)


def _score_prediction(
    db_path: Path,
    predicted_sql: str,
    gold_sql: str,
    timeout_s: float,
    max_rows: int,
) -> dict:
    predicted = _execute_for_score(db_path, predicted_sql, timeout_s, max_rows)
    gold = _execute_for_score(db_path, gold_sql, timeout_s, max_rows)
    if gold["error"]:
        return {
            "execution_exact": False,
            "strict_sequence_match": False,
            "error_category": "gold_execution_error",
            "evaluation_error": gold["error"],
            "prediction_execution_ms": predicted["latency_ms"],
            "gold_execution_ms": gold["latency_ms"],
        }
    if predicted["error"]:
        return {
            "execution_exact": False,
            "strict_sequence_match": False,
            "error_category": "prediction_execution_error",
            "evaluation_error": predicted["error"],
            "prediction_execution_ms": predicted["latency_ms"],
            "gold_execution_ms": gold["latency_ms"],
            "gold_row_count": len(gold["rows"]),
            "gold_column_count": len(gold["columns"]),
            "gold_preview": _preview(gold["rows"]),
        }
    predicted_rows = predicted["rows"]
    gold_rows = gold["rows"]
    execution_exact = set(predicted_rows) == set(gold_rows)
    strict_sequence = predicted_rows == gold_rows
    if execution_exact:
        category = None
    elif len(predicted["columns"]) != len(gold["columns"]):
        category = "projection_shape"
    elif len(predicted_rows) != len(gold_rows):
        category = "row_cardinality"
    else:
        category = "value_join_or_filter"
    return {
        "execution_exact": execution_exact,
        "strict_sequence_match": strict_sequence,
        "empty_result_match": execution_exact and not gold_rows,
        "error_category": category,
        "prediction_execution_ms": predicted["latency_ms"],
        "gold_execution_ms": gold["latency_ms"],
        "predicted_row_count": len(predicted_rows),
        "gold_row_count": len(gold_rows),
        "predicted_column_count": len(predicted["columns"]),
        "gold_column_count": len(gold["columns"]),
        "predicted_result_hash": _result_hash(predicted_rows),
        "gold_result_hash": _result_hash(gold_rows),
        "predicted_preview": _preview(predicted_rows),
        "gold_preview": _preview(gold_rows),
    }


def _is_llm_infrastructure_error(message: Any) -> bool:
    text = str(message or "").strip()
    return text.startswith((
        "LLM 服务请求失败（",
        "LLM 服务调用失败（",
        "LLM 服务响应中断（",
    ))


def _run_case(
    case: dict,
    db_path: Path,
    schema: dc.SchemaSnapshot,
    llm_cfg: str,
    include_evidence: bool,
    eval_timeout_s: float,
    max_eval_rows: int,
) -> dict:
    started = time.perf_counter()
    connector = dc.DBConnector(str(db_path))
    security = _CapturingSQLSecurity(connector, max_rows=100, timeout_s=8)
    executor = dc.NL2SQLExecutor(security, schema, llm_cfg=llm_cfg)
    model_question = _model_question(case, include_evidence)
    try:
        answer = executor.answer(model_question)
        generation_ms = round((time.perf_counter() - started) * 1000, 3)
        raw_sql = executor.last_generated_sql or security.raw_sql
        result = {
            "id": case["id"],
            "index": case["index"],
            "question_id": case["question_id"],
            "db_id": case["db_id"],
            "difficulty": case["difficulty"],
            "question": case["question"],
            "evidence": case["evidence"] if include_evidence else None,
            "gold_sql": case["gold_sql"],
            "predicted_sql": raw_sql,
            "rejected_candidate_sql": (
                executor.last_candidate_sql if not raw_sql else None
            ),
            "semantic_contract_hint": executor.last_semantic_hint or None,
            "semantic_repair_count": executor.semantic_repair_count,
            "candidate_search": executor.last_candidate_search,
            "declared_intent": (
                executor.last_query_intent.as_dict()
                if executor.last_query_intent.is_declared() else None
            ),
            "relational_contract": (
                executor.last_relational_contract.as_dict()
                if executor.last_relational_contract.is_declared() else None
            ),
            "native_relational_plan": (
                executor.last_relational_plan.as_dict()
                if executor.last_relational_plan is not None else None
            ),
            "answer_kind": answer.kind,
            "clarification_missing": (answer.clarification or {}).get("missing"),
            "execution_error": answer.error,
            "generation_latency_ms": generation_ms,
        }
        if answer.kind == "query" and raw_sql and not answer.error:
            result.update(_score_prediction(
                db_path, raw_sql, case["gold_sql"], eval_timeout_s, max_eval_rows,
            ))
        else:
            if _is_llm_infrastructure_error(answer.error):
                category = INFRASTRUCTURE_ERROR_CATEGORY
            elif (
                answer.kind == "clarification"
                and (answer.clarification or {}).get("missing") == "table_relationship"
            ):
                category = "relation_gate"
            else:
                category = "generation_or_production_error"
            result.update({
                "execution_exact": False,
                "strict_sequence_match": False,
                "error_category": category,
            })
    except Exception as exc:
        result = {
            "id": case["id"],
            "index": case["index"],
            "question_id": case["question_id"],
            "db_id": case["db_id"],
            "difficulty": case["difficulty"],
            "question": case["question"],
            "evidence": case["evidence"] if include_evidence else None,
            "gold_sql": case["gold_sql"],
            "predicted_sql": security.raw_sql,
            "answer_kind": "exception",
            "clarification_missing": None,
            "execution_exact": False,
            "strict_sequence_match": False,
            "error_category": "benchmark_runtime_error",
            "execution_error": f"{type(exc).__name__}: {exc}",
            "generation_latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    result["total_latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return result


def _rate(passed: int, total: int) -> float | None:
    return round(passed / total, 4) if total else None


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _summary(results: list[dict], target_total: int | None = None) -> dict:
    ordered = sorted(results, key=lambda item: int(item["index"]))
    target_total = len(ordered) if target_total is None else target_total
    infrastructure = [
        item for item in ordered
        if item.get("error_category") == INFRASTRUCTURE_ERROR_CATEGORY
    ]
    scoreable = [
        item for item in ordered
        if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
    ]
    exact = sum(bool(item.get("execution_exact")) for item in scoreable)
    strict = sum(bool(item.get("strict_sequence_match")) for item in scoreable)
    production = sum(
        item.get("answer_kind") == "query"
        and bool(item.get("predicted_sql"))
        and not item.get("execution_error")
        for item in scoreable
    )
    empty_exact = sum(bool(item.get("empty_result_match")) for item in scoreable)
    semantic_contract = {
        "repaired_then_accepted": sum(
            int(item.get("semantic_repair_count") or 0) > 0
            and item.get("answer_kind") == "query"
            for item in scoreable
        ),
        "rejected_known_conflict": sum(
            item.get("clarification_missing") == "query_semantics"
            for item in scoreable
        ),
        "declared_intent_coverage": sum(
            bool(item.get("declared_intent")) for item in scoreable
        ),
    }
    candidate_search = {
        "triggered": sum(bool(item.get("candidate_search")) for item in scoreable),
        "primary_accepted": sum(
            (item.get("candidate_search") or {}).get("status") == "primary_accepted"
            for item in scoreable
        ),
        "unique_alternative_accepted": sum(
            (item.get("candidate_search") or {}).get("status")
            == "unique_alternative_accepted"
            for item in scoreable
        ),
        "ambiguous_fail_closed": sum(
            (item.get("candidate_search") or {}).get("status")
            == "ambiguous_alternatives"
            for item in scoreable
        ),
        "no_eligible_fail_closed": sum(
            (item.get("candidate_search") or {}).get("status")
            == "no_eligible_candidate"
            for item in scoreable
        ),
    }
    target_ir = {
        key: sum(bool((item.get("relational_contract") or {}).get(key)) for item in scoreable)
        for key in (
            "relation_paths", "aggregation_stages", "filter_requirements",
            "ordering_requirements", "set_requirements",
            "distinct_count_requirements", "ratio_requirements",
            "correlation_requirements",
        )
    }
    target_ir["any"] = sum(
        any((item.get("relational_contract") or {}).get(key) for key in target_ir)
        for item in scoreable
    )
    native_planner = {
        "compiled_and_executed": sum(
            bool(item.get("native_relational_plan")) for item in scoreable
        ),
        "execution_exact": sum(
            bool(item.get("native_relational_plan"))
            and bool(item.get("execution_exact"))
            for item in scoreable
        ),
    }
    by_difficulty = {}
    for difficulty in DIFFICULTY_ORDER:
        target = [item for item in ordered if item["difficulty"] == difficulty]
        scoped = [
            item for item in target
            if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
        ]
        passed = sum(bool(item.get("execution_exact")) for item in scoped)
        by_difficulty[difficulty] = {
            "passed": passed,
            "total": len(scoped),
            "attempted": len(target),
            "rate": _rate(passed, len(scoped)),
        }
    by_database = {}
    for db_id in sorted({str(item["db_id"]) for item in ordered}):
        target = [item for item in ordered if item["db_id"] == db_id]
        scoped = [
            item for item in target
            if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
        ]
        passed = sum(bool(item.get("execution_exact")) for item in scoped)
        by_database[db_id] = {
            "passed": passed,
            "total": len(scoped),
            "attempted": len(target),
            "rate": _rate(passed, len(scoped)),
        }
    categories = Counter(
        str(item.get("error_category"))
        for item in ordered
        if not item.get("execution_exact")
    )
    generation_latencies = [
        float(item.get("generation_latency_ms") or 0.0) for item in scoreable
    ]
    return {
        "coverage": {
            "scoreable": len(scoreable),
            "attempted": len(ordered),
            "target": target_total,
            "infrastructure_failures": len(infrastructure),
            "rate": _rate(len(scoreable), target_total),
            "complete": len(scoreable) == target_total and not infrastructure,
        },
        "execution_accuracy": {
            "passed": exact,
            "total": len(scoreable),
            "rate": _rate(exact, len(scoreable)),
        },
        "raw_lower_bound_execution_accuracy": {
            "passed": exact,
            "total": target_total,
            "rate": _rate(exact, target_total),
        },
        "strict_sequence_match": {
            "passed": strict,
            "total": len(scoreable),
            "rate": _rate(strict, len(scoreable)),
        },
        "production_query_success": {
            "passed": production,
            "total": len(scoreable),
            "rate": _rate(production, len(scoreable)),
        },
        "empty_result_exact_matches": empty_exact,
        "semantic_contract": semantic_contract,
        "target_relational_ir": target_ir,
        "native_relational_planner": native_planner,
        "bounded_candidate_search": candidate_search,
        "by_difficulty": by_difficulty,
        "by_database": by_database,
        "error_categories": dict(categories.most_common()),
        "generation_latency_ms": {
            "median": round(statistics.median(generation_latencies), 3) if generation_latencies else 0.0,
            "p95": round(_percentile(generation_latencies, 0.95), 3),
            "max": round(max(generation_latencies), 3) if generation_latencies else 0.0,
            "sum": round(sum(generation_latencies), 3),
        },
    }


def _sample_fingerprint(cases: list[dict], include_evidence: bool) -> str:
    payload = [
        [
            case["index"], case["question_id"], case["db_id"], case["difficulty"],
            case["question"], case["evidence"] if include_evidence else None,
            case["gold_sql"],
        ]
        for case in cases
    ]
    return _sha256_bytes(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def _markdown(payload: dict) -> str:
    summary = payload["summary"]
    exact = summary["execution_accuracy"]
    strict = summary["strict_sequence_match"]
    production = summary["production_query_success"]
    coverage = summary["coverage"]
    lines = [
        "# DBQuill BIRD Mini-Dev SQLite Benchmark",
        "",
        f"- 运行状态：{payload['status']}",
        f"- 模型：{payload['model_identity'].get('model') or payload['model_identity'].get('name')}",
        f"- 样本：{payload['sample']['size']} / {payload['dataset']['total_cases']}，seed={payload['sample']['seed']}",
        f"- 可评分覆盖：{coverage['scoreable']}/{payload['sample']['size']}（基础设施失败 {coverage['infrastructure_failures']}）",
        f"- 已覆盖样本集合式 Execution Accuracy：{exact['passed']}/{exact['total']} ({(exact['rate'] or 0) * 100:.1f}%)",
        f"- 严格行顺序匹配：{strict['passed']}/{strict['total']} ({(strict['rate'] or 0) * 100:.1f}%)",
        f"- 生产只读查询链成功：{production['passed']}/{production['total']} ({(production['rate'] or 0) * 100:.1f}%)",
        f"- 空结果 Exact 命中：{summary['empty_result_exact_matches']}",
        f"- 本地关系计划确定性执行：{summary['native_relational_planner']['compiled_and_executed']}（Execution Exact {summary['native_relational_planner']['execution_exact']}）",
        "",
        "> 评分复现 BIRD 官方集合比较：忽略重复行和行顺序。只有覆盖完整时才构成该固定样本的最终分数；基础设施失败不计作 SQL 语义失败。严格顺序与空结果命中单独报告。",
        "",
        "## 难度分层",
        "",
        "| 难度 | 通过 | 可评分 | 已尝试 | 目标数 | 比率 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for difficulty in DIFFICULTY_ORDER:
        item = summary["by_difficulty"][difficulty]
        lines.append(
            f"| {difficulty} | {item['passed']} | {item['total']} | {item['attempted']} | "
            f"{payload['sample']['difficulty_counts'].get(difficulty, 0)} | {(item['rate'] or 0) * 100:.1f}% |"
        )
    lines.extend(["", "## 主要差距", "", "| 分类 | 数量 |", "|---|---:|"])
    for category, count in summary["error_categories"].items():
        lines.append(f"| {category} | {count} |")
    lines.extend([
        "",
        f"Prompt 契约：{payload['prompt_contract']}  ",
        f"评分契约：{payload['scoring_contract']}  ",
        f"证据契约：{payload['evidence_contract']}  ",
        f"列字典契约：{payload['description_contract']}  ",
        f"关系契约：{payload['relation_contract']}  ",
        f"样本指纹：{payload['sample']['fingerprint']}  ",
        f"数据集 SHA-256：{payload['dataset']['json_sha256']}",
    ])
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBQuill BIRD Mini-Dev SQLite benchmark")
    parser.add_argument("--package-root", type=Path, default=DEFAULT_PACKAGE_ROOT)
    parser.add_argument("--official-repo", type=Path, default=DEFAULT_OFFICIAL_REPO)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--sample-size", type=int, default=60, help="0 means all 500 cases")
    parser.add_argument(
        "--case-id", action="append", default=[],
        help="Diagnostic subset; repeat with BIRD question id or bird-mini-NNNN",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--sample-rows", type=int, default=5)
    parser.add_argument("--eval-timeout", type=float, default=30.0)
    parser.add_argument("--max-eval-rows", type=int, default=200000)
    parser.add_argument(
        "--llm-cfg",
        default=(
            os.environ.get("DBQUILL_MODEL_PROFILE")
            or os.environ.get("DBAGENT_MODEL_PROFILE")
            or "default"
        ),
    )
    parser.add_argument("--omit-evidence", action="store_true")
    parser.add_argument(
        "--omit-descriptions", action="store_true",
        help="Ablation: do not load official database-description CSV metadata",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.sample_size < 0:
        raise ValueError("--sample-size 不能小于 0")
    if not 1 <= args.workers <= 16:
        raise ValueError("--workers 必须在 1..16")
    if not 0 <= args.sample_rows <= 20:
        raise ValueError("--sample-rows 必须在 0..20")
    if not 1 <= args.eval_timeout <= 300:
        raise ValueError("--eval-timeout 必须在 1..300 秒")
    if not 100 <= args.max_eval_rows <= 1000000:
        raise ValueError("--max-eval-rows 必须在 100..1000000")

    package_root = args.package_root.resolve()
    official_repo = args.official_repo.resolve()
    dataset_path = package_root / "mini_dev_sqlite.json"
    tables_path = package_root / "dev_tables.json"
    gold_path = package_root / "mini_dev_sqlite_gold.sql"
    for required in (dataset_path, tables_path, gold_path):
        if not required.is_file():
            raise FileNotFoundError(f"缺少 BIRD Mini-Dev 文件: {required}")

    cases = _load_cases(dataset_path)
    schema_records = {
        str(item.get("db_id")): item
        for item in json.loads(tables_path.read_text(encoding="utf-8"))
    }
    selected = _select_cases(cases, args.sample_size, args.seed)
    if args.case_id:
        wanted = {
            value if str(value).startswith("bird-mini-")
            else f"bird-mini-{int(value):04d}"
            for value in args.case_id
        }
        selected = sorted(
            [case for case in cases if case["id"] in wanted],
            key=lambda case: int(case["index"]),
        )
        missing_case_ids = sorted(wanted - {case["id"] for case in selected})
        if missing_case_ids:
            raise ValueError("未知 --case-id: " + ", ".join(missing_case_ids))
    include_evidence = not args.omit_evidence
    sample_fingerprint = _sample_fingerprint(selected, include_evidence)
    selected_db_ids = sorted({case["db_id"] for case in selected})
    db_paths = {db_id: _database_path(package_root, db_id) for db_id in selected_db_ids}
    database_sha256 = {db_id: _sha256_file(path) for db_id, path in db_paths.items()}
    include_descriptions = not args.omit_descriptions
    description_sha256 = {
        db_id: _description_fingerprint(path)
        for db_id, path in db_paths.items()
    } if include_descriptions else {}

    schemas: dict[str, dc.SchemaSnapshot] = {}
    for db_id in selected_db_ids:
        connector = dc.DBConnector(str(db_paths[db_id]))
        schemas[db_id] = dc.SchemaDiscovery(
            connector, sample_rows=args.sample_rows,
        ).discover()
        relation_count = _apply_declared_foreign_keys(
            schema_records.get(db_id) or {}, schemas[db_id],
        )
        description_stats = (
            _apply_column_descriptions(db_paths[db_id], schemas[db_id])
            if include_descriptions else {"files": 0, "columns": 0}
        )
        print(
            f"[BIRD schema] {db_id}: {len(schemas[db_id].tables)} tables, "
            f"{relation_count} declared relations, "
            f"{description_stats['columns']} described columns",
            flush=True,
        )

    prompt_material = "\n\n".join([
        inspect.getsource(dc.SchemaSnapshot),
        inspect.getsource(dc.RelationalAlgebraContract),
        inspect.getsource(dc.RelationalColumnRef),
        inspect.getsource(dc.RelationalJoinEdge),
        inspect.getsource(dc.RelationalAggregate),
        inspect.getsource(dc.RelationalFilterPredicate),
        inspect.getsource(dc.RelationalRanking),
        inspect.getsource(dc.RelationalQueryPlan),
        inspect.getsource(dc.RelationalSetBranch),
        inspect.getsource(dc.RelationalSetQueryPlan),
        inspect.getsource(dc.RelationalScalarAggregatePlan),
        inspect.getsource(dc.RelationalScalarRankingPlan),
        inspect.getsource(dc.SQLiteRelationalPlanRenderer),
        inspect.getsource(dc.NL2SQLExecutor),
        inspect.getsource(_apply_column_descriptions),
        inspect.getsource(_apply_declared_foreign_keys),
        EVIDENCE_CONTRACT if include_evidence else "question-only-v1",
        DESCRIPTION_CONTRACT if include_descriptions else "schema-names-only-v1",
        RELATION_CONTRACT,
    ])
    prompt_contract = "nl2sql-" + _sha256_bytes(prompt_material.encode("utf-8"))[:16]
    model_identity = _redacted_model_identity(args.llm_cfg)
    dataset = {
        "name": "BIRD Mini-Dev SQLite",
        "metric": "official_execution_accuracy_set_rows",
        "official_repository": "https://github.com/bird-bench/mini_dev",
        "repository_commit": _git_commit(official_repo),
        "json_sha256": _sha256_file(dataset_path),
        "tables_sha256": _sha256_file(tables_path),
        "gold_sha256": _sha256_file(gold_path),
        "database_sha256": database_sha256,
        "description_sha256": description_sha256,
        "total_cases": len(cases),
        "database_count": len({case["db_id"] for case in cases}),
        "select_only": True,
    }
    payload = {
        "schema_version": 3,
        "status": "prepared" if args.prepare_only else "running",
        "started_at_unix": time.time(),
        "dataset": dataset,
        "sample": {
            "strategy": (
                "explicit_case_ids" if args.case_id
                else "balanced_difficulty_round_robin_database"
            ),
            "size": len(selected),
            "seed": args.seed,
            "fingerprint": sample_fingerprint,
            "difficulty_counts": dict(Counter(case["difficulty"] for case in selected)),
            "database_count": len(selected_db_ids),
        },
        "llm_cfg": args.llm_cfg,
        "model_identity": model_identity,
        "prompt_contract": prompt_contract,
        "scoring_contract": SCORING_CONTRACT,
        "evidence_contract": EVIDENCE_CONTRACT if include_evidence else "question-only-v1",
        "description_contract": (
            DESCRIPTION_CONTRACT if include_descriptions else "schema-names-only-v1"
        ),
        "relation_contract": RELATION_CONTRACT,
        "schema_sample_rows": args.sample_rows,
        "eval_timeout_s": args.eval_timeout,
        "max_eval_rows": args.max_eval_rows,
        "limitations": [
            "sample score is not an official leaderboard submission",
            "official EX ignores duplicate rows and row ordering",
            "empty-result exact matches are counted separately",
            "official database-description CSV metadata is question-ranked and context-bounded",
            "officially declared foreign keys are restored when SQLite DDL omits them; undeclared joins remain blocked",
            "SELECT-only; CRUD is not executed or auto-confirmed",
        ],
        "results": [],
        "summary": _summary([], len(selected)),
    }

    output = args.output.resolve()
    markdown = args.markdown.resolve()
    if args.resume and output.is_file():
        existing = json.loads(output.read_text(encoding="utf-8"))
        contracts_match = (
            existing.get("schema_version") == 3
            and existing.get("sample", {}).get("fingerprint") == sample_fingerprint
            and existing.get("prompt_contract") == prompt_contract
            and existing.get("model_identity") == model_identity
            and existing.get("scoring_contract") == SCORING_CONTRACT
            and existing.get("dataset", {}).get("database_sha256") == database_sha256
            and existing.get("dataset", {}).get("description_sha256") == description_sha256
            and existing.get("description_contract") == payload.get("description_contract")
            and existing.get("relation_contract") == RELATION_CONTRACT
            and existing.get("schema_sample_rows") == args.sample_rows
        )
        if not contracts_match:
            raise RuntimeError(
                "无法 resume：样本、Prompt、模型、评分、Schema 或数据库契约已变化"
            )
        payload = existing
        payload["status"] = "running"

    completed = {
        item["id"]: item
        for item in payload.get("results") or []
        if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
    }
    if args.prepare_only:
        payload["summary"] = _summary(list(completed.values()), len(selected))
        _atomic_json(output, payload)
        _atomic_text(markdown, _markdown(payload))
        print(
            f"Prepared {len(selected)} BIRD cases across {len(selected_db_ids)} databases; "
            f"sample={sample_fingerprint[:16]}",
            flush=True,
        )
        return 0

    pending = [case for case in selected if case["id"] not in completed]
    total = len(selected)
    print(
        f"BIRD benchmark: {len(completed)}/{total} resumed, {len(pending)} pending, "
        f"workers={args.workers}, prompt={prompt_contract}",
        flush=True,
    )
    run_started = time.perf_counter()
    infrastructure_block = None
    pending_iterator = iter(pending)

    def _submit(executor, case):
        return executor.submit(
            _run_case,
            case,
            db_paths[case["db_id"]],
            schemas[case["db_id"]],
            args.llm_cfg,
            include_evidence,
            args.eval_timeout,
            args.max_eval_rows,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for _ in range(min(args.workers, len(pending))):
            case = next(pending_iterator, None)
            if case is not None:
                futures[_submit(executor, case)] = case
        while futures:
            done, _not_done = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future)
                result = future.result()
                completed[result["id"]] = result
                if result.get("error_category") == INFRASTRUCTURE_ERROR_CATEGORY:
                    infrastructure_block = result.get("execution_error") or "LLM 基础设施错误"
                ordered = sorted(completed.values(), key=lambda item: int(item["index"]))
                payload["results"] = ordered
                payload["summary"] = _summary(ordered, len(selected))
                _atomic_json(output, payload)
                print(
                    f"[BIRD] {len(completed)}/{total} {result['id']} {result['difficulty']}: "
                    f"{'PASS' if result['execution_exact'] else 'FAIL'} "
                    f"{result.get('error_category') or ''} "
                    f"({result['generation_latency_ms']:.0f} ms)",
                    flush=True,
                )
            while not infrastructure_block and len(futures) < args.workers:
                case = next(pending_iterator, None)
                if case is None:
                    break
                futures[_submit(executor, case)] = case

    payload["status"] = "blocked_external_dependency" if infrastructure_block else "completed"
    if infrastructure_block:
        payload["run_error"] = infrastructure_block
    payload["completed_at_unix"] = time.time()
    payload["run_wall_ms"] = round((time.perf_counter() - run_started) * 1000, 3)
    payload["results"] = sorted(completed.values(), key=lambda item: int(item["index"]))
    payload["summary"] = _summary(payload["results"], len(selected))
    _atomic_json(output, payload)
    _atomic_text(markdown, _markdown(payload))
    exact = payload["summary"]["execution_accuracy"]
    print(
        f"{payload['status']}: execution_accuracy={exact['passed']}/{exact['total']} "
        f"({(exact['rate'] or 0) * 100:.1f}%), report={output}",
        flush=True,
    )
    return 2 if infrastructure_block else 0


if __name__ == "__main__":
    raise SystemExit(main())
