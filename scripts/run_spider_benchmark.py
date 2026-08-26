"""Run a reproducible Spider 1.0 dev exact-set-match benchmark.

The official Spider repository contains the complete dev questions, gold SQL,
schema metadata and evaluator, but not the database contents.  This runner can
therefore build schema-faithful empty SQLite databases, execute DBQuill's
production NL2SQL path against them, and score the raw generated SQL with the
official value-insensitive Exact Set Match implementation.

This is deliberately *not* reported as execution accuracy or Test Suite
Accuracy.  Empty databases cannot validate condition values or result rows.
Raw predictions/checkpoints live under benchmark_results/ and are git-ignored.
Only redacted model identity is persisted; endpoints and API keys are never
written.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import importlib.util
import json
import math
import os
import random
import re
import sqlite3
import statistics
import subprocess
import sys
import time
import types
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

import dbquill_core as dc  # noqa: E402
from nl2db_evaluation import _redacted_model_identity  # noqa: E402


DEFAULT_SPIDER_REPO = ROOT / "benchmark_data" / "spider_official_repo"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "spider_dev.json"
DEFAULT_MARKDOWN = ROOT / "benchmark_results" / "spider_dev.md"
DEFAULT_SEED = 20260820
HARDNESS_ORDER = ("easy", "medium", "hard", "extra")
SCORING_CONTRACT = "spider-exact-set-match-adapter-v5"
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


def _load_official_modules(repo: Path):
    evaluation_path = repo / "evaluation.py"
    process_path = repo / "process_sql.py"
    if not evaluation_path.is_file() or not process_path.is_file():
        raise FileNotFoundError(
            "Spider 官方评测代码缺失。请先克隆 https://github.com/taoyds/spider "
            f"到 {repo}"
        )
    repo_text = str(repo)
    if repo_text not in sys.path:
        sys.path.insert(0, repo_text)
    try:
        import process_sql as process_sql_module  # noqa: PLC0415
    except ModuleNotFoundError as exc:
        if exc.name != "nltk":
            raise
        # The official parser only imports nltk.word_tokenize.  Keep this
        # benchmark dependency-free by providing the SQL-token subset it
        # needs; all 1,034 gold statements are parsed during preparation, so
        # tokenizer drift fails before any model call.
        nltk_shim = types.ModuleType("nltk")
        nltk_shim.word_tokenize = _compatible_word_tokenize
        sys.modules["nltk"] = nltk_shim
        sys.modules.pop("process_sql", None)
        import process_sql as process_sql_module  # noqa: PLC0415

    spec = importlib.util.spec_from_file_location("dbquill_spider_evaluation", evaluation_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 Spider 官方评测模块")
    evaluation_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evaluation_module)
    return evaluation_module, process_sql_module


_SQL_TOKEN_RE = re.compile(
    r"__val_\d+_\d+__|"
    r"[A-Za-z_][A-Za-z0-9_$]*(?:\.(?:[A-Za-z_][A-Za-z0-9_$]*|\*))?|"
    r"\d+(?:\.\d+)?|<>|!=|>=|<=|==|[(),;=<>+*/%-]|\S"
)


def _compatible_word_tokenize(value: str) -> list[str]:
    """Dependency-free tokenizer for the official Spider SQL parser."""
    return _SQL_TOKEN_RE.findall(str(value))


def _dataset_paths(repo: Path) -> tuple[Path, Path]:
    candidates = (
        repo / "evaluation_examples" / "examples",
        repo,
    )
    for base in candidates:
        dev = base / "dev.json"
        tables = base / "tables.json"
        if dev.is_file() and tables.is_file():
            return dev, tables
    raise FileNotFoundError(f"在 {repo} 中找不到 dev.json/tables.json")


def _git_commit(repo: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and re.fullmatch(r"[0-9a-fA-F]{40}", value) else None


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _sqlite_type(spider_type: str) -> str:
    normalized = str(spider_type or "text").casefold()
    if normalized in {"number", "numeric", "real", "float", "double", "decimal"}:
        return "REAL"
    if normalized in {"boolean", "bool"}:
        return "INTEGER"
    if normalized in {"time", "date", "datetime", "timestamp", "year"}:
        return "TEXT"
    return "TEXT"


def _schema_fingerprint(schema: dict) -> str:
    relevant = {
        key: schema.get(key)
        for key in (
            "db_id", "table_names_original", "column_names_original", "column_types",
            "primary_keys", "foreign_keys",
        )
    }
    return _sha256_bytes(
        json.dumps(relevant, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )[:16]


def _create_schema_database(path: Path, schema: dict) -> None:
    """Create an empty SQLite database that preserves Spider names, PKs and FKs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    if temporary.exists():
        temporary.unlink()

    table_names = list(schema["table_names_original"])
    columns = list(schema["column_names_original"])
    column_types = list(schema["column_types"])
    primary_keys = {int(value) for value in schema.get("primary_keys") or []}
    foreign_keys = [tuple(map(int, pair)) for pair in schema.get("foreign_keys") or []]

    by_table: dict[int, list[int]] = defaultdict(list)
    for index, (table_index, _column_name) in enumerate(columns):
        if int(table_index) >= 0:
            by_table[int(table_index)].append(index)

    connection = sqlite3.connect(temporary)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        if any(str(name).casefold() == "sqlite_sequence" for name in table_names):
            # SQLite reserves sqlite_* object names.  An AUTOINCREMENT table
            # creates the real internal sqlite_sequence(name, seq) schema.
            connection.execute(
                'CREATE TABLE "__dbquill_sequence_seed" '
                '("id" INTEGER PRIMARY KEY AUTOINCREMENT)'
            )
            connection.execute('DROP TABLE "__dbquill_sequence_seed"')
        for table_index, table_name in enumerate(table_names):
            if str(table_name).casefold() == "sqlite_sequence":
                continue
            definitions: list[str] = []
            indexes = by_table.get(table_index, [])
            for column_index in indexes:
                column_name = columns[column_index][1]
                definitions.append(
                    f"{_quote_identifier(column_name)} {_sqlite_type(column_types[column_index])}"
                )

            table_primary_keys = [
                columns[index][1]
                for index in indexes
                if index in primary_keys
            ]
            if table_primary_keys:
                definitions.append(
                    "PRIMARY KEY (" + ", ".join(map(_quote_identifier, table_primary_keys)) + ")"
                )

            for source_index, target_index in foreign_keys:
                if int(columns[source_index][0]) != table_index:
                    continue
                target_table_index = int(columns[target_index][0])
                definitions.append(
                    "FOREIGN KEY ("
                    + _quote_identifier(columns[source_index][1])
                    + ") REFERENCES "
                    + _quote_identifier(table_names[target_table_index])
                    + " ("
                    + _quote_identifier(columns[target_index][1])
                    + ")"
                )
            if not definitions:
                definitions.append('"__dbquill_placeholder" INTEGER')
            connection.execute(
                f"CREATE TABLE {_quote_identifier(table_name)} (" + ", ".join(definitions) + ")"
            )
        connection.commit()
        integrity = connection.execute("PRAGMA quick_check").fetchone()
        if not integrity or str(integrity[0]).casefold() != "ok":
            raise RuntimeError(f"SQLite quick_check 失败: {integrity}")
    finally:
        connection.close()
    os.replace(temporary, path)


def _prepare_databases(cache_root: Path, schemas: dict[str, dict], db_ids: Iterable[str]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for db_id in sorted(set(db_ids)):
        schema = schemas[db_id]
        fingerprint = _schema_fingerprint(schema)
        path = cache_root / fingerprint / db_id / f"{db_id}.sqlite"
        if not path.is_file():
            _create_schema_database(path, schema)
        paths[db_id] = path
    return paths


def _official_sql(
    evaluation: Any,
    process_sql: Any,
    db_path: Path,
    schema_record: dict,
    sql: str,
) -> tuple[Any, Any]:
    schema = process_sql.Schema(process_sql.get_schema(str(db_path)))
    parsed = process_sql.get_sql(schema, sql)
    valid = evaluation.build_valid_col_units(parsed["from"]["table_units"], schema)
    parsed = evaluation.rebuild_sql_val(parsed)
    parsed = evaluation.rebuild_sql_col(
        valid,
        parsed,
        evaluation.build_foreign_key_map(schema_record),
    )
    return schema, parsed


_SQL_ALIAS_STOP_WORDS = {
    "as", "cross", "except", "full", "group", "having", "inner", "intersect",
    "join", "left", "limit", "natural", "on", "order", "outer", "right", "union",
    "where",
}

_SQL_LEXEME = re.compile(
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|\[[^\]]*\]"
    r"|[A-Za-z_][A-Za-z0-9_$]*|[(),;]"
)


def _strip_projection_aliases(sql: str) -> str:
    """Remove result-column aliases which Spider ignores but cannot parse."""

    tokens: list[dict[str, Any]] = []
    depth = 0
    for match in _SQL_LEXEME.finditer(sql):
        value = match.group(0)
        if value == ")":
            depth = max(0, depth - 1)
        tokens.append({
            "text": value,
            "lower": value.casefold(),
            "start": match.start(),
            "end": match.end(),
            "depth": depth,
        })
        if value == "(":
            depth += 1

    removals: set[tuple[int, int]] = set()
    for select_index, token in enumerate(tokens):
        if token["lower"] != "select":
            continue
        select_depth = token["depth"]
        from_index = None
        for index in range(select_index + 1, len(tokens)):
            candidate = tokens[index]
            if candidate["depth"] == select_depth and candidate["lower"] == "from":
                from_index = index
                break
        if from_index is None:
            continue
        for index in range(select_index + 1, from_index - 1):
            alias_marker = tokens[index]
            alias = tokens[index + 1]
            if alias_marker["depth"] != select_depth or alias_marker["lower"] != "as":
                continue
            raw_alias = alias["text"]
            if raw_alias[:1] in {'"', '`', '['}:
                expected_close = ']' if raw_alias[:1] == '[' else raw_alias[:1]
                alias_name = (
                    raw_alias[1:-1] if raw_alias.endswith(expected_close) else ""
                )
            else:
                alias_name = raw_alias
            if alias["depth"] != select_depth or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_$]*", alias_name,
            ):
                continue
            following = tokens[index + 2] if index + 2 < len(tokens) else None
            ends_projection = index + 2 == from_index or (
                following is not None
                and following["depth"] == select_depth
                and following["text"] == ","
            )
            if not ends_projection:
                continue
            start = alias_marker["start"]
            while start > 0 and sql[start - 1] in " \t":
                start -= 1
            removals.add((start, alias["end"]))

    normalized = sql
    for start, end in sorted(removals, reverse=True):
        normalized = normalized[:start] + normalized[end:]
    return normalized


def _official_parser_compatible_sql(sql: str, schema_record: dict) -> str:
    """Normalize valid SQLite syntax omitted by Spider's legacy parser.

    SQLite and DBQuill both accept ``FROM table alias``.  Spider 1.0's
    process_sql.py, however, only records aliases introduced with ``AS`` and
    raises KeyError for the otherwise-valid shorthand. It also tokenizes the
    ``LEFT``/``INNER`` modifier as an alias even though the exact-set-match AST
    does not represent join type. Normalize a copy used exclusively by the
    evaluator so these adapter quirks are not counted as model failures.
    Production SQL and the persisted raw prediction remain untouched; outer
    join semantics remain visible in the raw prediction and execution result.
    """

    table_names = {
        str(name).casefold()
        for name in schema_record.get("table_names_original", [])
    }
    column_names = {
        str(item[1]).casefold()
        for item in schema_record.get("column_names_original", [])
        if isinstance(item, (list, tuple)) and len(item) >= 2
    }

    # The projection-lock compiler quotes physical identifiers for production
    # safety. Spider 1.0's legacy tokenizer interprets double-quoted words as
    # string values, even though SQLite accepts them as identifiers. Dequote
    # only an identifier-shaped token that is known to the supplied schema.
    # Literal values and names that require quoting remain untouched.  An
    # unqualified known column is included because SQLite itself resolves that
    # double-quoted token as an identifier in these already-executed queries,
    # while Spider's tokenizer incorrectly treats it as a string value.
    qualified_identifier = re.compile(
        r"(?P<qualifier>[A-Za-z_][A-Za-z0-9_$]*)"
        r"(?P<spacing1>\s*)\.(?P<spacing2>\s*)"
        r'"(?P<identifier>[A-Za-z_][A-Za-z0-9_$]*)"'
    )

    def dequote_schema_identifier(match: re.Match[str]) -> str:
        if match.group("identifier").casefold() not in column_names:
            return match.group(0)
        return (
            f"{match.group('qualifier')}{match.group('spacing1')}."
            f"{match.group('spacing2')}{match.group('identifier')}"
        )
    quoted_source = re.compile(
        r'\b(?P<clause>FROM|JOIN)(?P<spacing>\s+)'
        r'"(?P<table>[A-Za-z_][A-Za-z0-9_$]*)"',
        flags=re.IGNORECASE,
    )

    def dequote_schema_table(match: re.Match[str]) -> str:
        if match.group("table").casefold() not in table_names:
            return match.group(0)
        return (
            f"{match.group('clause')}{match.group('spacing')}"
            f"{match.group('table')}"
        )
    unqualified_identifier = re.compile(
        r'"(?P<identifier>[A-Za-z_][A-Za-z0-9_$]*)"'
    )

    def dequote_unqualified_column(match: re.Match[str]) -> str:
        if match.group("identifier").casefold() not in column_names:
            return match.group(0)
        return match.group("identifier")
    source_pattern = re.compile(
        r"\b(?P<clause>FROM|JOIN)(?P<spacing1>\s+)"
        r"(?P<table>[A-Za-z_][A-Za-z0-9_$]*)(?P<spacing2>\s+)"
        r"(?P<alias>[A-Za-z_][A-Za-z0-9_$]*)",
        flags=re.IGNORECASE,
    )

    def add_as(match: re.Match[str]) -> str:
        table = match.group("table")
        alias = match.group("alias")
        if table.casefold() not in table_names or alias.casefold() in _SQL_ALIAS_STOP_WORDS:
            return match.group(0)
        return (
            f"{match.group('clause')}{match.group('spacing1')}{table}"
            f"{match.group('spacing2')}AS {alias}"
        )

    normalized = _strip_projection_aliases(sql)
    normalized = re.sub(
        r"\b(?:LEFT(?:\s+OUTER)?|INNER)\s+JOIN\b",
        "JOIN",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = qualified_identifier.sub(dequote_schema_identifier, normalized)
    normalized = quoted_source.sub(dequote_schema_table, normalized)
    normalized = unqualified_identifier.sub(dequote_unqualified_column, normalized)
    return source_pattern.sub(add_as, normalized)


def _hardness(evaluation: Any, process_sql: Any, db_path: Path, schema_record: dict, sql: str) -> str:
    _schema, parsed = _official_sql(evaluation, process_sql, db_path, schema_record, sql)
    return str(evaluation.Evaluator().eval_hardness(parsed))


def _round_robin_sample(cases: list[dict], quota: int, seed: int, hardness: str) -> list[dict]:
    by_db: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        by_db[case["db_id"]].append(case)
    rng = random.Random(f"{seed}:{hardness}")
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
    base, remainder = divmod(sample_size, len(HARDNESS_ORDER))
    selected: list[dict] = []
    for index, hardness in enumerate(HARDNESS_ORDER):
        quota = base + (1 if index < remainder else 0)
        bucket = [case for case in cases if case["hardness"] == hardness]
        selected.extend(_round_robin_sample(bucket, quota, seed, hardness))
    return sorted(selected, key=lambda case: int(case["index"]))


class _CapturingSQLSecurity(dc.SQLSecurity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_sql = ""

    def execute(self, sql: str) -> dc.SQLResult:
        self.raw_sql = str(sql or "").strip()
        return super().execute(sql)


def _component_gaps(partial: dict) -> list[str]:
    groups = (
        ("select", "select_schema_link"),
        ("where", "filter_condition"),
        ("group", "group_having"),
        ("order", "order_limit"),
        ("IUEN", "set_operation"),
        ("keywords", "sql_structure"),
    )
    gaps = []
    for official_name, category in groups:
        score = partial.get(official_name) or {}
        if float(score.get("f1", 0.0)) < 1.0:
            gaps.append(category)
    return gaps or ["other_structure"]


def _score_prediction(
    evaluation: Any,
    process_sql: Any,
    db_path: Path,
    schema_record: dict,
    gold_sql: str,
    predicted_sql: str,
) -> dict:
    evaluated_sql = _official_parser_compatible_sql(predicted_sql, schema_record)
    try:
        _schema, gold = _official_sql(evaluation, process_sql, db_path, schema_record, gold_sql)
        _schema, predicted = _official_sql(
            evaluation, process_sql, db_path, schema_record, evaluated_sql,
        )
    except Exception as exc:  # valid SQLite can still exceed Spider's legacy grammar
        return {
            "exact_match": False,
            "error_category": "official_parser_unsupported",
            "component_gaps": ["official_parser_unsupported"],
            "evaluation_error": f"{type(exc).__name__}: {exc}",
            "evaluated_sql": evaluated_sql if evaluated_sql != predicted_sql else None,
        }
    evaluator = evaluation.Evaluator()
    exact = bool(evaluator.eval_exact_match(predicted, gold))
    gaps = [] if exact else _component_gaps(evaluator.partial_scores)
    return {
        "exact_match": exact,
        "error_category": None if exact else gaps[0],
        "component_gaps": gaps,
        "evaluated_sql": evaluated_sql if evaluated_sql != predicted_sql else None,
    }


def _rescore_result(
    result: dict,
    db_path: Path,
    schema_record: dict,
    evaluation: Any,
    process_sql: Any,
) -> dict:
    rescored = dict(result)
    previous_category = rescored.get("error_category")
    for key in (
        "exact_match", "error_category", "component_gaps", "evaluation_error", "evaluated_sql",
    ):
        rescored.pop(key, None)
    predicted_sql = str(rescored.get("predicted_sql") or "").strip()
    if rescored.get("answer_kind") == "query" and predicted_sql:
        rescored.update(_score_prediction(
            evaluation,
            process_sql,
            db_path,
            schema_record,
            str(rescored["gold_sql"]),
            predicted_sql,
        ))
    else:
        category = (
            INFRASTRUCTURE_ERROR_CATEGORY
            if _is_llm_infrastructure_error(rescored.get("execution_error"))
            else (
                "relation_gate"
                if rescored.get("clarification_missing") == "table_relationship"
                or previous_category == "relation_gate"
                else "generation_or_execution_error"
            )
        )
        rescored.update({
            "exact_match": False,
            "error_category": category,
            "component_gaps": [category],
        })
    return rescored


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
    schema_record: dict,
    evaluation: Any,
    process_sql: Any,
    llm_cfg: str,
) -> dict:
    started = time.perf_counter()
    connector = dc.DBConnector(str(db_path))
    schema = dc.SchemaDiscovery(connector, sample_rows=0).discover()
    security = _CapturingSQLSecurity(connector, max_rows=100, timeout_s=8)
    executor = dc.NL2SQLExecutor(security, schema, llm_cfg=llm_cfg)
    try:
        answer = executor.answer(case["question"])
        raw_sql = executor.last_generated_sql or security.raw_sql
        result = {
            "id": case["id"],
            "index": case["index"],
            "db_id": case["db_id"],
            "hardness": case["hardness"],
            "question": case["question"],
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
        }
        if answer.kind != "query" or not raw_sql:
            category = (
                INFRASTRUCTURE_ERROR_CATEGORY
                if _is_llm_infrastructure_error(answer.error)
                else (
                    "relation_gate" if answer.kind == "clarification"
                    and (answer.clarification or {}).get("missing") == "table_relationship"
                    else "generation_or_execution_error"
                )
            )
            result.update({
                "exact_match": False,
                "error_category": category,
                "component_gaps": [category],
            })
        else:
            result.update(
                _score_prediction(
                    evaluation, process_sql, db_path, schema_record,
                    case["gold_sql"], raw_sql,
                )
            )
    except Exception as exc:  # keep the rest of the public benchmark running
        result = {
            "id": case["id"],
            "index": case["index"],
            "db_id": case["db_id"],
            "hardness": case["hardness"],
            "question": case["question"],
            "gold_sql": case["gold_sql"],
            "predicted_sql": security.raw_sql,
            "answer_kind": "exception",
            "clarification_missing": None,
            "exact_match": False,
            "error_category": "benchmark_runtime_error",
            "component_gaps": ["benchmark_runtime_error"],
            "execution_error": f"{type(exc).__name__}: {exc}",
        }
    result["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
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
    results = sorted(results, key=lambda item: int(item["index"]))
    target_total = len(results) if target_total is None else target_total
    infrastructure = [
        item for item in results
        if item.get("error_category") == INFRASTRUCTURE_ERROR_CATEGORY
    ]
    scoreable = [
        item for item in results
        if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
    ]
    passed = sum(bool(item.get("exact_match")) for item in scoreable)
    valid = sum(
        bool(item.get("predicted_sql")) and item.get("answer_kind") == "query"
        for item in scoreable
    )
    parseable = sum(
        bool(item.get("predicted_sql"))
        and item.get("answer_kind") == "query"
        and item.get("error_category") != "official_parser_unsupported"
        for item in scoreable
    )
    by_hardness = {}
    for hardness in HARDNESS_ORDER:
        attempted = [item for item in results if item["hardness"] == hardness]
        scoped = [
            item for item in attempted
            if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
        ]
        exact = sum(bool(item.get("exact_match")) for item in scoped)
        by_hardness[hardness] = {
            "passed": exact,
            "total": len(scoped),
            "attempted": len(attempted),
            "rate": _rate(exact, len(scoped)),
        }
    latencies = [float(item.get("latency_ms") or 0.0) for item in scoreable]
    error_categories = Counter(
        str(item.get("error_category"))
        for item in results
        if not item.get("exact_match")
    )
    component_gaps = Counter(
        gap
        for item in results
        if not item.get("exact_match")
        for gap in item.get("component_gaps") or []
    )
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
        "exact_match": sum(
            bool(item.get("native_relational_plan")) and bool(item.get("exact_match"))
            for item in scoreable
        ),
    }
    return {
        "coverage": {
            "scoreable": len(scoreable),
            "attempted": len(results),
            "target": target_total,
            "infrastructure_failures": len(infrastructure),
            "rate": _rate(len(scoreable), target_total),
            "complete": len(scoreable) == target_total and not infrastructure,
        },
        "exact_match": {
            "passed": passed,
            "total": len(scoreable),
            "rate": _rate(passed, len(scoreable)),
        },
        "raw_lower_bound_exact_match": {
            "passed": passed,
            "total": target_total,
            "rate": _rate(passed, target_total),
        },
        "valid_sql": {
            "passed": valid,
            "total": len(scoreable),
            "rate": _rate(valid, len(scoreable)),
        },
        "official_ast_parseable": {
            "passed": parseable,
            "total": len(scoreable),
            "rate": _rate(parseable, len(scoreable)),
        },
        "exact_among_parseable": {
            "passed": passed,
            "total": parseable,
            "rate": _rate(passed, parseable),
        },
        "by_hardness": by_hardness,
        "error_categories": dict(error_categories.most_common()),
        "component_gaps": dict(component_gaps.most_common()),
        "semantic_contract": semantic_contract,
        "target_relational_ir": target_ir,
        "native_relational_planner": native_planner,
        "bounded_candidate_search": candidate_search,
        "latency_ms": {
            "median": round(statistics.median(latencies), 3) if latencies else 0.0,
            "p95": round(_percentile(latencies, 0.95), 3),
            "max": round(max(latencies), 3) if latencies else 0.0,
            "sum": round(sum(latencies), 3),
        },
    }


def _sample_fingerprint(cases: list[dict]) -> str:
    payload = [
        [case["index"], case["db_id"], case["hardness"], case["question"], case["gold_sql"]]
        for case in cases
    ]
    return _sha256_bytes(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def _markdown(payload: dict) -> str:
    summary = payload["summary"]
    exact = summary["exact_match"]
    valid = summary["valid_sql"]
    parseable = summary["official_ast_parseable"]
    exact_parseable = summary["exact_among_parseable"]
    coverage = summary["coverage"]
    lines = [
        "# DBQuill Spider 1.0 Dev Benchmark",
        "",
        f"- 运行状态：`{payload['status']}`",
        f"- 模型：`{payload['model_identity'].get('model') or payload['model_identity'].get('name')}`",
        f"- 样本：{payload['sample']['size']} / {payload['dataset']['total_cases']}，难度平衡，seed={payload['sample']['seed']}",
        f"- 可评分覆盖：{coverage['scoreable']}/{payload['sample']['size']}（基础设施失败 {coverage['infrastructure_failures']}）",
        f"- 已覆盖样本官方 Exact Set Match（忽略条件值）：{exact['passed']}/{exact['total']} ({(exact['rate'] or 0) * 100:.1f}%)",
        f"- 生成 SQL 可执行/可解析为查询：{valid['passed']}/{valid['total']} ({(valid['rate'] or 0) * 100:.1f}%)",
        f"- 官方旧 AST 解析器可覆盖：{parseable['passed']}/{parseable['total']} ({(parseable['rate'] or 0) * 100:.1f}%)",
        f"- AST 可覆盖样本内 Exact Set Match：{exact_parseable['passed']}/{exact_parseable['total']} ({(exact_parseable['rate'] or 0) * 100:.1f}%)",
        f"- 本地关系计划确定性执行：{summary['native_relational_planner']['compiled_and_executed']}（Exact {summary['native_relational_planner']['exact_match']}）",
        "",
        "> 该通道使用官方 schema 元数据构造空 SQLite，能验证 schema linking、SQL 结构与生产安全执行边界；"
        "不能验证条件值或结果行，因此不是 Execution Accuracy/Test Suite Accuracy。只有可评分覆盖完整时才构成"
        "该固定样本的最终分数；基础设施失败不计作 SQL 语义失败。",
        "",
        "## 难度分层",
        "",
        "| 难度 | 通过 | 可评分 | 已尝试 | 目标数 | 比率 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for hardness in HARDNESS_ORDER:
        item = summary["by_hardness"][hardness]
        lines.append(
            f"| {hardness} | {item['passed']} | {item['total']} | {item['attempted']} | "
            f"{payload['sample']['hardness_counts'].get(hardness, 0)} | {(item['rate'] or 0) * 100:.1f}% |"
        )
    lines.extend(["", "## 主要差距", "", "| 分类 | 数量 |", "|---|---:|"])
    for category, count in summary["component_gaps"].items():
        lines.append(f"| `{category}` | {count} |")
    lines.extend([
        "",
        f"Prompt 契约：`{payload['prompt_contract']}`  ",
        f"评分契约：`{payload['scoring_contract']}`  ",
        f"样本指纹：`{payload['sample']['fingerprint']}`  ",
        f"Spider dev SHA-256：`{payload['dataset']['dev_sha256']}`",
    ])
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBQuill Spider 1.0 dev benchmark")
    parser.add_argument("--spider-repo", type=Path, default=DEFAULT_SPIDER_REPO)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--sample-size", type=int, default=100, help="0 means all 1034 dev cases")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--llm-cfg",
        default=(
            os.environ.get("DBQUILL_MODEL_PROFILE")
            or os.environ.get("DBAGENT_MODEL_PROFILE")
            or "default"
        ),
    )
    parser.add_argument(
        "--case-id", action="append", default=[],
        help="repeatable exact Spider case id for diagnostic reruns",
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

    repo = args.spider_repo.resolve()
    evaluation, process_sql = _load_official_modules(repo)
    dev_path, tables_path = _dataset_paths(repo)
    dev_cases = json.loads(dev_path.read_text(encoding="utf-8"))
    table_records = json.loads(tables_path.read_text(encoding="utf-8"))
    schemas = {record["db_id"]: record for record in table_records}
    db_ids = {case["db_id"] for case in dev_cases}
    missing = sorted(db_ids - schemas.keys())
    if missing:
        raise RuntimeError("Spider tables.json 缺少 schema: " + ", ".join(missing))

    cache_root = ROOT / "benchmark_data" / "spider_synthetic_db"
    db_paths = _prepare_databases(cache_root, schemas, db_ids)
    prepared: list[dict] = []
    for index, case in enumerate(dev_cases):
        prepared.append({
            "id": f"spider-dev-{index:04d}",
            "index": index,
            "db_id": case["db_id"],
            "question": case["question"],
            "gold_sql": case["query"],
            "hardness": _hardness(
                evaluation, process_sql, db_paths[case["db_id"]], schemas[case["db_id"]], case["query"],
            ),
        })
    if args.case_id:
        requested = list(dict.fromkeys(args.case_id))
        by_id = {case["id"]: case for case in prepared}
        unknown = [case_id for case_id in requested if case_id not in by_id]
        if unknown:
            raise ValueError("unknown --case-id: " + ", ".join(unknown))
        selected = [by_id[case_id] for case_id in requested]
    else:
        selected = _select_cases(prepared, args.sample_size, args.seed)
    sample_fingerprint = _sample_fingerprint(selected)
    # The effective generation contract includes dynamic schema hints and the
    # pre-execution semantic retry, not only the first prompt string.  Hash the
    # complete executor source so a helper-only behavior change cannot silently
    # resume or compare against predictions produced by a different contract.
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
    ])
    prompt_contract = "nl2sql-" + _sha256_bytes(prompt_material.encode("utf-8"))[:16]
    model_identity = _redacted_model_identity(args.llm_cfg)
    dataset = {
        "name": "Spider 1.0 dev",
        "metric": "official_exact_set_match_without_values",
        "execution_accuracy": "not_measured_schema_only",
        "official_repository": "https://github.com/taoyds/spider",
        "repository_commit": _git_commit(repo),
        "dev_sha256": _sha256_file(dev_path),
        "tables_sha256": _sha256_file(tables_path),
        "total_cases": len(prepared),
        "database_count": len(db_ids),
    }
    payload = {
        "schema_version": 2,
        "status": "prepared" if args.prepare_only else "running",
        "started_at_unix": time.time(),
        "dataset": dataset,
        "sample": {
            "strategy": "balanced_hardness_round_robin_database",
            "size": len(selected),
            "seed": args.seed,
            "fingerprint": sample_fingerprint,
            "hardness_counts": dict(Counter(case["hardness"] for case in selected)),
            "database_count": len({case["db_id"] for case in selected}),
        },
        "llm_cfg": args.llm_cfg,
        "model_identity": model_identity,
        "prompt_contract": prompt_contract,
        "scoring_contract": SCORING_CONTRACT,
        "limitations": [
            "schema-only empty SQLite; condition values and result rows are not evaluated",
            "not Spider Test Suite Accuracy or execution accuracy",
            "sample score is not an official leaderboard submission",
            "valid SQLite constructs outside Spider's legacy parser grammar score zero",
        ],
        "results": [],
        "summary": _summary([], len(selected)),
    }
    output = args.output.resolve()
    markdown = args.markdown.resolve()
    if args.resume and output.is_file():
        existing = json.loads(output.read_text(encoding="utf-8"))
        generation_contracts_match = (
            existing.get("sample", {}).get("fingerprint") == sample_fingerprint
            and existing.get("prompt_contract") == prompt_contract
            and existing.get("model_identity") == model_identity
        )
        if not generation_contracts_match:
            raise RuntimeError("无法 resume：样本、Prompt 或模型契约已变化，请使用新输出文件")
        payload = existing
        payload["status"] = "running"
        payload.pop("run_error", None)
        payload["schema_version"] = 2
        if payload.get("scoring_contract") != SCORING_CONTRACT:
            rescored_results = []
            for result in payload.get("results") or []:
                db_id = str(result["db_id"])
                rescored_results.append(_rescore_result(
                    result,
                    db_paths[db_id],
                    schemas[db_id],
                    evaluation,
                    process_sql,
                ))
            payload["results"] = sorted(
                rescored_results, key=lambda item: int(item["index"]),
            )
            payload["summary"] = _summary(payload["results"], len(selected))
            payload["scoring_contract"] = SCORING_CONTRACT
            print(
                f"Rescored {len(rescored_results)} stored predictions with {SCORING_CONTRACT}",
                flush=True,
            )
    completed = {
        item["id"]: item
        for item in payload.get("results") or []
        if item.get("error_category") != INFRASTRUCTURE_ERROR_CATEGORY
    }
    if args.prepare_only:
        payload["status"] = "prepared"
        payload["results"] = sorted(
            completed.values(), key=lambda item: int(item["index"]),
        )
        payload["summary"] = _summary(payload["results"], len(selected))
        _atomic_json(output, payload)
        _atomic_text(markdown, _markdown(payload))
        print(
            f"Prepared {len(selected)} cases across {payload['sample']['database_count']} databases; "
            f"sample={sample_fingerprint[:16]}",
            flush=True,
        )
        return 0

    pending = [case for case in selected if case["id"] not in completed]
    total = len(selected)
    print(
        f"Spider benchmark: {len(completed)}/{total} resumed, {len(pending)} pending, "
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
            evaluation,
            process_sql,
            args.llm_cfg,
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
                ordered_results = sorted(
                    completed.values(), key=lambda item: int(item["index"]),
                )
                payload["results"] = ordered_results
                payload["summary"] = _summary(ordered_results, len(selected))
                _atomic_json(output, payload)
                print(
                    f"[Spider] {len(completed)}/{total} {result['id']} {result['hardness']}: "
                    f"{'PASS' if result['exact_match'] else 'FAIL'} "
                    f"{result.get('error_category') or ''} ({result['latency_ms']:.0f} ms)",
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
    exact = payload["summary"]["exact_match"]
    print(
        f"{payload['status']}: exact={exact['passed']}/{exact['total']} "
        f"({(exact['rate'] or 0) * 100:.1f}%), "
        f"report={output}",
        flush=True,
    )
    return 2 if infrastructure_block else 0


if __name__ == "__main__":
    raise SystemExit(main())
