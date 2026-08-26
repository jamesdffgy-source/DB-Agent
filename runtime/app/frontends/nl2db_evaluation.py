#!/usr/bin/env python3
"""Versioned, repeatable evaluation for DB-Agent's native NL-to-Database chain.

The offline channel intentionally does not call an LLM. Planner cases use a
human-labelled oracle intent so they measure deterministic operation planning,
    target resolution, semantic resolution and clarification gates. Calendar,
    dimension and trend cases measure their bounded deterministic compilers.
    Reference SQL cases measure the execution boundary, not SQL generation. Real
    model NL2SQL is an explicit optional channel and is never merged into the
    offline score.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import dbagent_core as dc
import model_baseline_contract as model_baselines


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATASET = Path(__file__).with_name("evaluation") / "nl2db_cases.json"
DEFAULT_MARKDOWN_REPORT = ROOT / "docs" / "EVALUATION_REPORT.md"
DEFAULT_JSON_REPORT = ROOT / "docs" / "EVALUATION_REPORT.json"
DEFAULT_LLM_CFG = os.environ.get("DBAGENT_MODEL_PROFILE", "default")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_llm_config(llm_cfg: str) -> dict:
    app_root = str(Path(__file__).resolve().parent.parent)
    frontends = str(Path(__file__).resolve().parent)
    if app_root not in sys.path:
        sys.path.insert(0, app_root)
    if frontends not in sys.path:
        sys.path.insert(0, frontends)
    import model_gateway  # noqa: PLC0415

    try:
        return dict(model_gateway.get_profile(llm_cfg))
    except ValueError:
        return {}


def _redacted_model_identity(llm_cfg: str, config: dict | None = None) -> dict:
    """Return stable comparison metadata without endpoint URLs or credentials."""
    source = dict(config) if config is not None else _load_llm_config(llm_cfg)
    base = str(source.get("base_url") or "").strip().rstrip("/").lower()
    mode = str(source.get("api_mode") or "chat_completions").strip().lower().replace("-", "_")
    if mode in {"response", "responses"}:
        mode = "responses"
    else:
        mode = "chat_completions"
    return {
        "config_name": str(llm_cfg),
        "name": str(source.get("name") or ""),
        "model": str(source.get("model") or ""),
        "api_mode": mode,
        "endpoint_fingerprint": hashlib.sha256(base.encode("utf-8")).hexdigest()[:16] if base else None,
    }


def _latency_summary(values: list[float], total: float) -> dict:
    ordered = sorted(values)
    middle = len(ordered) // 2
    median = (
        (ordered[middle - 1] + ordered[middle]) / 2
        if len(ordered) % 2 == 0 else ordered[middle]
    ) if ordered else 0
    return {
        "total": round(total, 3),
        "median": round(median, 3),
        "maximum": round(max(ordered), 3) if ordered else 0,
    }


def _load_dataset(path: Path) -> dict:
    try:
        dataset = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法读取评测集 {path}: {exc}") from exc
    if dataset.get("schema_version") != 1:
        raise RuntimeError("评测集 schema_version 必须为 1")
    required = (
        "suite_version", "planner_cases", "multi_metric_cases", "calendar_cases",
        "dimension_cases", "trend_cases", "graph_cases", "large_schema_cases",
        "reference_sql_cases", "security_cases", "model_cases",
    )
    missing = [name for name in required if name not in dataset]
    if missing:
        raise RuntimeError("评测集缺少字段: " + ", ".join(missing))
    all_ids = [
        str(case.get("id") or "")
        for group in required[1:]
        for case in dataset[group]
    ]
    if not all(all_ids) or len(all_ids) != len(set(all_ids)):
        raise RuntimeError("评测用例 ID 不能为空且必须全局唯一")
    return dataset


def _create_fixture(path: Path) -> None:
    # sqlite3.Connection 的上下文管理器只管理事务，不负责 close；Windows
    # 临时文件必须显式关闭，否则完整测试套件后可能因 GC 时机出现 WinError 32。
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL,
                city TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at DATE NOT NULL,
                updated_at TEXT
            );
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL
            );
            CREATE TABLE order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL
            );
            CREATE TABLE isolated_events (
                id INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL
            );
            CREATE TABLE holidays (
                holiday_date DATE PRIMARY KEY,
                name TEXT NOT NULL,
                is_working INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE timestamp_events (
                id INTEGER PRIMARY KEY,
                occurred_at TIMESTAMP NOT NULL
            );
            CREATE TABLE dst_events (
                id INTEGER PRIMARY KEY,
                occurred_at TIMESTAMP NOT NULL
            );
            """
        )
        conn.executemany(
            "INSERT INTO customers(id, name, region, city, status) VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Alice", "华东", "上海", "active"),
                (2, "Bob", "华北", "北京", "active"),
                (3, "Carol", "华东", "杭州", "inactive"),
                (4, "Dana", "华南", "深圳", "active"),
            ],
        )
        conn.executemany(
            "INSERT INTO orders(id, customer_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)",
            [
                (1, 1, 120.5, "paid", "2026-08-01"),
                (2, 1, 80.0, "pending", "2026-08-02"),
                (3, 2, 200.0, "paid", "2026-08-03"),
                (4, 3, 50.0, "cancelled", "2026-07-31"),
                (5, 2, 25.5, "paid", "2026-08-05"),
            ],
        )
        conn.executemany(
            "INSERT INTO products(id, name, category) VALUES (?, ?, ?)",
            [(1, "Widget", "A"), (2, "Service", "B")],
        )
        conn.executemany(
            "INSERT INTO order_items(id, order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?, ?)",
            [
                (1, 1, 1, 2, 50.0),
                (2, 1, 2, 1, 20.5),
                (3, 3, 1, 4, 50.0),
                (4, 5, 2, 1, 25.5),
            ],
        )
        conn.execute("INSERT INTO isolated_events(id, event_type) VALUES (1, 'startup')")
        conn.executemany(
            "INSERT INTO holidays(holiday_date, name, is_working) VALUES (?, ?, ?)",
            [
                ("2026-08-03", "Company Holiday", 0),
                ("2026-08-08", "Make-up Workday", 1),
            ],
        )
        conn.executemany(
            "INSERT INTO timestamp_events(id, occurred_at) VALUES (?, ?)",
            [
                (1, "2026-08-01T15:59:59Z"),
                (2, "2026-08-01T16:00:00Z"),
                (3, "2026-08-02T15:59:59Z"),
                (4, "2026-08-02T16:00:00Z"),
            ],
        )
        conn.executemany(
            "INSERT INTO dst_events(id, occurred_at) VALUES (?, ?)",
            [
                (1, "2024-01-01T04:30:00Z"),
                (2, "2024-01-01T05:30:00Z"),
                (3, "2024-07-01T03:30:00Z"),
                (4, "2024-07-01T04:30:00Z"),
                (5, "2024-07-01T04:30:00+08:00"),
                (6, "not-a-timestamp"),
            ],
        )
        conn.commit()


def _large_schema_fixture() -> dc.SchemaSnapshot:
    """构造稳定的 64 表关系图，验证大 schema 下的关系事实而非模型能力。"""
    tables = {
        "customers": dc.DBTable("customers", [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("name", "TEXT"),
        ]),
        "orders": dc.DBTable("orders", [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("customer_id", "INTEGER", fk_table="customers", fk_column="id"),
        ]),
        "order_items": dc.DBTable("order_items", [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("order_id", "INTEGER", fk_table="orders", fk_column="id"),
            dc.DBColumn("product_id", "INTEGER", fk_table="products", fk_column="id"),
        ]),
        "products": dc.DBTable("products", [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("category_id", "INTEGER", fk_table="categories", fk_column="id"),
            dc.DBColumn("supplier_id", "INTEGER", fk_table="suppliers", fk_column="id"),
        ]),
        "categories": dc.DBTable("categories", [dc.DBColumn("id", "INTEGER", pk=True)]),
        "suppliers": dc.DBTable("suppliers", [dc.DBColumn("id", "INTEGER", pk=True)]),
        "audit_events": dc.DBTable("audit_events", [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("event_type", "TEXT"),
        ]),
    }
    for index in range(57):
        name = f"archive_{index + 1:02d}"
        tables[name] = dc.DBTable(name, [
            dc.DBColumn("id", "INTEGER", pk=True),
            dc.DBColumn("payload", "TEXT"),
            dc.DBColumn("created_at", "TEXT"),
        ])
    return dc.SchemaSnapshot(db_path="synthetic:large-schema", tables=tables)


def _stable_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return str(value)
        return round(value, 9)
    if isinstance(value, list):
        return [_stable_value(item) for item in value]
    return value


def _rows_match(actual: list[list[Any]], expected: list[list[Any]], ordered: bool) -> bool:
    left = [_stable_value(row) for row in actual]
    right = [_stable_value(row) for row in expected]
    if ordered:
        return left == right
    key = lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True)  # noqa: E731
    return sorted(left, key=key) == sorted(right, key=key)


def _summary(cases: list[dict]) -> dict:
    passed = sum(1 for case in cases if case["passed"])
    total = len(cases)
    return {
        "passed": passed,
        "total": total,
        "rate": round(passed / total, 4) if total else None,
        "cases": cases,
    }


def _planner_error_category(keys: Iterable[str]) -> str:
    names = set(keys)
    if "clarification" in names:
        return "ambiguity_gate"
    if "semantic_terms" in names:
        return "semantic_resolution"
    if "target_tables" in names:
        return "target_resolution"
    if names & {"action", "mode"}:
        return "operation_mapping"
    return "policy_metadata"


def _evaluate_planner(dataset: dict, schema: dc.SchemaSnapshot) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    planner = dc.NaturalLanguageDatabasePlanner(schema, catalog)
    results = []
    for case in dataset["planner_cases"]:
        expected = case["expected"]
        try:
            plan = planner.plan_schema(case["question"])
            if plan is None:
                intent = dc.IntentResult(
                    intent=case["oracle_intent"],
                    confidence=1.0,
                    reasoning="固定评测集人工标注意图",
                )
                plan = planner.from_intent(case["question"], intent)
            clarification = planner.clarification_for(case["question"], plan)
            semantic = catalog.resolve(case["question"])
            actual = {
                "action": plan.action,
                "mode": plan.mode,
                "target_tables": sorted(plan.target_tables),
                "risk": plan.risk,
                "requires_confirmation": plan.requires_confirmation,
                "clarification": clarification["missing"] if clarification else None,
                "semantic_terms": sorted(match["term"] for match in semantic.matches),
            }
            comparable_expected = dict(expected)
            comparable_expected["target_tables"] = sorted(expected.get("target_tables") or [])
            comparable_expected["semantic_terms"] = sorted(expected.get("semantic_terms") or [])
            mismatches = [key for key, value in comparable_expected.items() if actual.get(key) != value]
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": _planner_error_category(mismatches) if mismatches else None,
                "mismatches": mismatches,
                "expected": comparable_expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001 - case-level reporting must continue
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_reference_sql(dataset: dict, security: dc.SQLSecurity) -> dict:
    results = []
    for case in dataset["reference_sql_cases"]:
        result = security.execute(case["sql"])
        if result.error:
            passed = False
            category = "execution_error"
        elif result.columns != case["expected_columns"]:
            passed = False
            category = "result_columns"
        elif not _rows_match(result.rows, case["expected_rows"], bool(case.get("ordered", True))):
            passed = False
            category = "result_rows"
        else:
            passed = True
            category = None
        results.append({
            "id": case["id"],
            "question": case["question"],
            "passed": passed,
            "error_category": category,
            "sql": result.sql,
            "error": result.error,
            "expected_columns": case["expected_columns"],
            "actual_columns": result.columns,
            "expected_rows": case["expected_rows"],
            "actual_rows": result.rows,
        })
    return _summary(results)


def _evaluate_calendar_compiler(
    dataset: dict,
    schema: dc.SchemaSnapshot,
    security: dc.SQLSecurity,
    connector: dc.DBConnector,
) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    executor = dc.DeterministicCalendarQueryExecutor(security, schema, connector)
    results = []
    for case in dataset["calendar_cases"]:
        expected = case["expected"]
        try:
            semantic = catalog.resolve(case["question"])
            answer = executor.answer(case["question"], semantic)
            actual_compiled = answer is not None
            mismatches = []
            if actual_compiled != bool(expected["compiled"]):
                mismatches.append("compiled")
            actual = {"compiled": actual_compiled}
            if answer is not None:
                plan = answer.calendar_plan or {}
                actual.update({
                    "mode": plan.get("mode"),
                    "date_range": plan.get("date_range"),
                    "timezone_conversion": (plan.get("rules") or {}).get("timezone_conversion"),
                    "tzdata_version": (plan.get("rules") or {}).get("tzdata_version"),
                    "iana_version": (plan.get("rules") or {}).get("iana_version"),
                    "columns": answer.columns,
                    "rows": answer.rows,
                    "error": answer.error,
                    "sql": answer.sql,
                })
                for key in (
                    "mode", "date_range", "timezone_conversion", "tzdata_version",
                    "iana_version", "columns",
                ):
                    if key in expected and actual.get(key) != expected[key]:
                        mismatches.append(key)
                if "rows" in expected and not _rows_match(
                    actual["rows"], expected["rows"], bool(case.get("ordered", True)),
                ):
                    mismatches.append("rows")
                if answer.error:
                    mismatches.append("execution_error")
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": "calendar_compiler" if mismatches else None,
                "mismatches": mismatches,
                "expected": expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001 - case-level reporting must continue
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_multi_metric_compiler(
    dataset: dict,
    schema: dc.SchemaSnapshot,
    security: dc.SQLSecurity,
    connector: dc.DBConnector,
) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    executor = dc.DeterministicMultiMetricQueryExecutor(security, schema, connector)
    results = []
    for case in dataset["multi_metric_cases"]:
        expected = case["expected"]
        try:
            semantic = catalog.resolve(case["question"])
            answer = executor.answer(case["question"], semantic)
            actual_compiled = answer is not None
            mismatches = []
            if actual_compiled != bool(expected["compiled"]):
                mismatches.append("compiled")
            actual = {"compiled": actual_compiled}
            if answer is not None:
                plan = answer.metric_plan or {}
                actual.update({
                    "table": plan.get("table"),
                    "measure_terms": [
                        item.get("term") for item in plan.get("measures") or []
                    ],
                    "global_filters": plan.get("global_filters") or [],
                    "columns": answer.columns,
                    "rows": answer.rows,
                    "error": answer.error,
                    "sql": answer.sql,
                })
                for key in (
                    "table", "measure_terms", "global_filters", "columns",
                ):
                    if key in expected and actual.get(key) != expected[key]:
                        mismatches.append(key)
                if "rows" in expected and not _rows_match(
                    actual["rows"], expected["rows"], bool(case.get("ordered", True)),
                ):
                    mismatches.append("rows")
                if answer.error:
                    mismatches.append("execution_error")
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": "multi_metric_compiler" if mismatches else None,
                "mismatches": mismatches,
                "expected": expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001 - case-level reporting must continue
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_dimension_compiler(
    dataset: dict,
    schema: dc.SchemaSnapshot,
    security: dc.SQLSecurity,
    connector: dc.DBConnector,
) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    executor = dc.DeterministicDimensionQueryExecutor(
        security, schema, connector, catalog,
    )
    results = []
    for case in dataset["dimension_cases"]:
        expected = case["expected"]
        try:
            semantic = catalog.resolve(case["question"])
            answer = executor.answer(case["question"], semantic)
            actual_compiled = answer is not None
            mismatches = []
            if actual_compiled != bool(expected["compiled"]):
                mismatches.append("compiled")
            actual = {"compiled": actual_compiled}
            if answer is not None:
                plan = answer.dimension_plan or {}
                actual.update({
                    "mode": plan.get("mode"),
                    "dimension_terms": [
                        item.get("term") for item in plan.get("dimensions") or []
                    ],
                    "measure_terms": [
                        item.get("term") for item in (
                            plan.get("measures") or [plan.get("measure") or {}]
                        )
                    ],
                    "dimension_filter_count": len(plan.get("dimension_filters") or []),
                    "columns": answer.columns,
                    "rows": answer.rows,
                    "error": answer.error,
                    "sql": answer.sql,
                })
                for key in (
                    "mode", "dimension_terms", "measure_terms",
                    "dimension_filter_count", "columns",
                ):
                    if key in expected and actual.get(key) != expected[key]:
                        mismatches.append(key)
                if "rows" in expected and not _rows_match(
                    actual["rows"], expected["rows"], bool(case.get("ordered", True)),
                ):
                    mismatches.append("rows")
                if answer.error:
                    mismatches.append("execution_error")
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": "dimension_compiler" if mismatches else None,
                "mismatches": mismatches,
                "expected": expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001 - case-level reporting must continue
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_trend_compiler(
    dataset: dict,
    schema: dc.SchemaSnapshot,
    security: dc.SQLSecurity,
    connector: dc.DBConnector,
) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    executor = dc.DeterministicTrendQueryExecutor(
        security, schema, connector, catalog,
        reference_date=date.fromisoformat(dataset["reference_date"]),
    )
    results = []
    for case in dataset["trend_cases"]:
        expected = case["expected"]
        try:
            semantic = catalog.resolve(case["question"])
            answer = executor.answer(case["question"], semantic)
            actual_compiled = answer is not None
            mismatches = []
            if actual_compiled != bool(expected["compiled"]):
                mismatches.append("compiled")
            actual = {"compiled": actual_compiled}
            if answer is not None:
                plan = answer.trend_plan or {}
                actual.update({
                    "grain": plan.get("grain"),
                    "grain_source": plan.get("grain_source"),
                    "measure_terms": [
                        item.get("term") for item in (
                            plan.get("measures") or [plan.get("measure") or {}]
                        )
                    ],
                    "storage_basis": (plan.get("rules") or {}).get("storage_basis"),
                    "timezone_conversion": (plan.get("rules") or {}).get("timezone_conversion"),
                    "tzdata_version": (plan.get("rules") or {}).get("tzdata_version"),
                    "iana_version": (plan.get("rules") or {}).get("iana_version"),
                    "date_range": plan.get("date_range"),
                    "window_source": (plan.get("rules") or {}).get("time_window_source"),
                    "reference_date": (plan.get("rules") or {}).get("reference_date"),
                    "reference_source": (plan.get("rules") or {}).get("reference_source"),
                    "calendar_term": (plan.get("rules") or {}).get("calendar_term"),
                    "calendar_mode": (plan.get("rules") or {}).get("calendar_mode"),
                    "week_start_iso": (plan.get("rules") or {}).get("week_start_iso"),
                    "columns": answer.columns,
                    "rows": answer.rows,
                    "error": answer.error,
                    "sql": answer.sql,
                })
                for key in (
                    "grain", "grain_source", "measure_terms", "storage_basis", "timezone_conversion",
                    "tzdata_version", "iana_version", "date_range", "window_source",
                    "reference_date", "reference_source", "calendar_term", "calendar_mode",
                    "week_start_iso", "columns",
                ):
                    if key in expected and actual.get(key) != expected[key]:
                        mismatches.append(key)
                if "rows" in expected and not _rows_match(
                    actual["rows"], expected["rows"], bool(case.get("ordered", True)),
                ):
                    mismatches.append("rows")
                if answer.error:
                    mismatches.append("execution_error")
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": "trend_compiler" if mismatches else None,
                "mismatches": mismatches,
                "expected": expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001 - case-level reporting must continue
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_graph(dataset: dict, schema: dc.SchemaSnapshot) -> dict:
    catalog = dc.SemanticCatalog(schema, dataset.get("semantic_entries") or [], strict=True)
    planner = dc.OperationGraphPlanner(schema, catalog)
    validator = dc.OperationGraphValidator()
    executor = dc.OperationGraphExecutor(object(), object(), schema=schema)
    results = []
    for case in dataset["graph_cases"]:
        expected = case["expected"]
        try:
            graph = planner.plan_compose(case["question"])
            ordered = validator.validate(graph)
            relation_node = next((node for node in ordered if node.tool == "inspect_relations"), None)
            relation_connected = None
            relation_error = None
            if relation_node is not None:
                try:
                    relation_connected = executor._inspect_relations(  # noqa: SLF001 - deterministic gate probe
                        list(relation_node.parameters.get("tables") or []),
                        relation_node.input,
                    )["connected"]
                except dc.OrchestratorError as exc:
                    relation_connected = False
                    relation_error = str(exc)
            actual = {
                "strategy": graph.strategy,
                "target_tables": sorted(graph.target_tables),
                "nodes": [{
                    "node_id": node.node_id,
                    "tool": node.tool,
                    "depends_on": list(node.depends_on),
                    "output_type": node.output_contract.get("type"),
                } for node in ordered],
                "relation_connected": relation_connected,
            }
            comparable_expected = dict(expected)
            comparable_expected["target_tables"] = sorted(expected.get("target_tables") or [])
            mismatches = [key for key, value in comparable_expected.items() if actual.get(key) != value]
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": not mismatches,
                "error_category": "graph_contract" if mismatches else None,
                "mismatches": mismatches,
                "expected": comparable_expected,
                "actual": actual,
                "relation_error": relation_error,
            })
        except Exception as exc:  # noqa: BLE001
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "graph_runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_large_schema(dataset: dict) -> dict:
    schema = _large_schema_fixture()
    analyzer = dc.SchemaRelationAnalyzer(schema)
    results = []
    for case in dataset["large_schema_cases"]:
        try:
            expected = case["expected"]
            relation = analyzer.analyze(case.get("tables") or [], case.get("question") or "")
            compact = schema.compact()
            actual = {
                "table_count": len(schema.tables),
                "connected": relation["connected"],
                "paths": relation["paths"],
                "invalid_tables": relation.get("invalid_tables") or [],
                "compact_table_count": compact.count("TABLE "),
            }
            mismatches = [
                key for key, value in expected.items()
                if actual.get(key) != value
            ]
            results.append({
                "id": case["id"],
                "question": case.get("question") or "",
                "passed": not mismatches,
                "error_category": "large_schema_relation" if mismatches else None,
                "mismatches": mismatches,
                "expected": expected,
                "actual": actual,
            })
        except Exception as exc:  # noqa: BLE001
            results.append({
                "id": case["id"],
                "question": case.get("question") or "",
                "passed": False,
                "error_category": "large_schema_runtime_error",
                "error": str(exc),
            })
    return _summary(results)


def _evaluate_security(dataset: dict, read_security: dc.SQLSecurity) -> dict:
    write_security = dc.WriteSecurity()
    results = []
    for case in dataset["security_cases"]:
        error = None
        try:
            if case["path"] == "read":
                read_security.validate(case["sql"])
            elif case["path"] == "write":
                write_security.validate_write(case["sql"])
            else:
                raise ValueError(f"未知安全路径: {case['path']}")
            allowed = True
        except (dc.SQLSecurityError, dc.WriteSecurityError) as exc:
            allowed = False
            error = str(exc)
        except Exception as exc:  # noqa: BLE001
            allowed = False
            error = f"评测运行异常: {exc}"
        expected_allowed = bool(case["expected_allowed"])
        passed = allowed == expected_allowed
        category = None
        if not passed:
            category = "unsafe_miss" if not expected_allowed else "false_positive"
        results.append({
            "id": case["id"],
            "path": case["path"],
            "passed": passed,
            "error_category": category,
            "expected_allowed": expected_allowed,
            "actual_allowed": allowed,
            "error": error,
        })
    summary = _summary(results)
    metrics = {}
    for path in ("read", "write"):
        scoped = [case for case in results if case["path"] == path]
        expected_blocked = [case for case in scoped if not case["expected_allowed"]]
        expected_allowed = [case for case in scoped if case["expected_allowed"]]
        blocked = sum(1 for case in expected_blocked if not case["actual_allowed"])
        accepted = sum(1 for case in expected_allowed if case["actual_allowed"])
        metrics[path] = {
            "unsafe_blocked": blocked,
            "unsafe_total": len(expected_blocked),
            "unsafe_recall": round(blocked / len(expected_blocked), 4) if expected_blocked else None,
            "policy_valid_accepted": accepted,
            "policy_valid_total": len(expected_allowed),
            "policy_valid_acceptance": round(accepted / len(expected_allowed), 4) if expected_allowed else None,
        }
    summary["metrics"] = metrics
    return summary


def _evaluate_model(dataset: dict, db_path: Path, llm_cfg: str) -> dict:
    results = []
    total_started = time.perf_counter()
    identity = _redacted_model_identity(llm_cfg)
    prompt_contract = "nl2sql-" + hashlib.sha256(
        dc.NL2SQLExecutor._SYSTEM_PROMPT.encode("utf-8"),
    ).hexdigest()[:16]
    agent = dc.DBAgent(
        db_path=str(db_path),
        semantic_entries=dataset.get("semantic_entries") or [],
        llm_cfg=llm_cfg,
        sample_rows=5,
        max_rows=100,
        timeout_s=15,
    )
    total_cases = len(dataset["model_cases"])
    for index, case in enumerate(dataset["model_cases"], start=1):
        case_started = time.perf_counter()
        try:
            question = agent.semantic_catalog.resolve(case["question"]).resolved_question
            answer = agent.nl2sql.answer(question)
            passed = (
                answer.kind == "query"
                and not answer.error
                and _rows_match(answer.rows, case["expected_rows"], bool(case.get("ordered", True)))
            )
            category = None if passed else ("model_error" if answer.error else "result_rows")
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": passed,
                "error_category": category,
                "sql": answer.sql,
                "error": answer.error,
                "expected_rows": case["expected_rows"],
                "actual_rows": answer.rows,
                "latency_ms": round((time.perf_counter() - case_started) * 1000, 3),
            })
        except Exception as exc:  # noqa: BLE001 - preserve remaining model cases
            results.append({
                "id": case["id"],
                "question": case["question"],
                "passed": False,
                "error_category": "model_error",
                "error": str(exc),
                "latency_ms": round((time.perf_counter() - case_started) * 1000, 3),
            })
        completed = results[-1]
        print(
            f"[Model Eval] {index}/{total_cases} {case['id']}: "
            f"{'PASS' if completed['passed'] else 'FAIL'} "
            f"({completed['latency_ms']:.0f} ms)",
            flush=True,
        )
    summary = _summary(results)
    elapsed_ms = (time.perf_counter() - total_started) * 1000
    summary.update({
        "status": "completed" if summary["passed"] == summary["total"] else "completed_with_failures",
        "llm_cfg": llm_cfg,
        "execution_accuracy": summary["rate"],
        "model_identity": identity,
        "prompt_contract": prompt_contract,
        "latency_ms": _latency_summary(
            [float(case["latency_ms"]) for case in results], elapsed_ms,
        ),
    })
    return summary


def run_suite(
    dataset_path: Path | str = DEFAULT_DATASET,
    *,
    with_model: bool = False,
    llm_cfg: str = DEFAULT_LLM_CFG,
) -> dict:
    """Run the fixed suite and return a JSON-serializable result."""
    path = Path(dataset_path)
    dataset = _load_dataset(path)
    dataset_sha256 = _sha256_file(path)
    with tempfile.TemporaryDirectory(prefix="dbagent-eval-") as temp_dir:
        db_path = Path(temp_dir) / "fixture.db"
        _create_fixture(db_path)
        connector = dc.DBConnector(str(db_path))
        schema = dc.SchemaDiscovery(connector, sample_rows=5).discover()
        read_security = dc.SQLSecurity(connector, max_rows=100, timeout_s=5)
        planner = _evaluate_planner(dataset, schema)
        multi_metric_compiler = _evaluate_multi_metric_compiler(
            dataset, schema, read_security, connector,
        )
        calendar_compiler = _evaluate_calendar_compiler(
            dataset, schema, read_security, connector,
        )
        dimension_compiler = _evaluate_dimension_compiler(
            dataset, schema, read_security, connector,
        )
        trend_compiler = _evaluate_trend_compiler(
            dataset, schema, read_security, connector,
        )
        graph = _evaluate_graph(dataset, schema)
        large_schema = _evaluate_large_schema(dataset)
        reference_sql = _evaluate_reference_sql(dataset, read_security)
        security = _evaluate_security(dataset, read_security)
        if with_model:
            model = _evaluate_model(dataset, db_path, llm_cfg)
        else:
            model = {
                "status": "not_run",
                "reason": "未提供 --with-model；离线门禁不会访问模型服务。",
                "passed": None,
                "total": len(dataset["model_cases"]),
                "rate": None,
                "execution_accuracy": None,
                "cases": [],
            }

    offline_groups = (
        planner, multi_metric_compiler, calendar_compiler, dimension_compiler, trend_compiler,
        graph, large_schema, reference_sql, security,
    )
    offline_passed = sum(group["passed"] for group in offline_groups)
    offline_total = sum(group["total"] for group in offline_groups)
    errors = Counter(
        case["error_category"]
        for group in offline_groups
        for case in group["cases"]
        if case.get("error_category")
    )
    return {
        "schema_version": 1,
        "suite_version": dataset["suite_version"],
        "dataset_sha256": dataset_sha256,
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "dataset": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
        "scope": {
            "planner": "人工标注 oracle_intent 下的确定性操作规划精确匹配；不评估模型意图分类。",
            "multi_metric_compiler": "SQLite 同表 2–6 个受控普通指标的一次性聚合、指标过滤隔离、全局枚举过滤和保守回退。",
            "calendar_compiler": "SQLite 声明型 DATE/时间戳字段上的财年、财季、工作日编译、执行结果与保守回退。",
            "dimension_compiler": "SQLite 单表业务维度分组、同表层级下钻、受控指标和复杂形状回退。",
            "trend_compiler": "SQLite 单表 DATE/显式时间戳上的日、周、月、季度、年聚合、受控指标和保守回退。",
            "operation_graph": "确定性动态节点、跨表关系预检和节点输出契约精确匹配。",
            "large_schema": "64 表合成 schema 的多跳关系、隔离表、非法表和紧凑索引事实匹配。",
            "reference_sql": "人工参考 SQL 的安全执行与结果匹配；不评估自然语言生成 SQL。",
            "security": "读写校验器对固定策略有效/无效语句的接受与拦截。",
            "model_nl2sql": "可选真实模型 SQL 生成执行准确率；与离线指标隔离。",
        },
        "offline": {
            "passed": offline_passed,
            "total": offline_total,
            "rate": round(offline_passed / offline_total, 4) if offline_total else None,
            "all_cases_passed": offline_passed == offline_total,
            "error_categories": dict(sorted(errors.items())),
            "planner": planner,
            "multi_metric_compiler": multi_metric_compiler,
            "calendar_compiler": calendar_compiler,
            "dimension_compiler": dimension_compiler,
            "trend_compiler": trend_compiler,
            "operation_graph": graph,
            "large_schema": large_schema,
            "reference_sql": reference_sql,
            "security": security,
        },
        "model_nl2sql": model,
    }


def _percent(value: Any) -> str:
    return "未运行" if value is None else f"{float(value) * 100:.1f}%"


def _case_status(case: dict) -> str:
    if case["passed"]:
        return "通过"
    return "失败（" + str(case.get("error_category") or "unknown") + "）"


def render_markdown(result: dict) -> str:
    offline = result["offline"]
    planner = offline["planner"]
    multi_metric_compiler = offline["multi_metric_compiler"]
    calendar_compiler = offline["calendar_compiler"]
    dimension_compiler = offline["dimension_compiler"]
    trend_compiler = offline["trend_compiler"]
    graph = offline["operation_graph"]
    large_schema = offline["large_schema"]
    sql = offline["reference_sql"]
    security = offline["security"]
    read = security["metrics"]["read"]
    write = security["metrics"]["write"]
    model = result["model_nl2sql"]
    lines = [
        "# NL-to-Database 固定评测报告",
        "",
        f"生成时间：{result['generated_at']}",
        "",
        f"评测集版本：`{result['suite_version']}`",
        "",
        "> 口径说明：离线总通过率不是模型 NL2SQL 准确率。规划用例使用人工标注意图；参考 SQL 用例使用人工 SQL。只有显式运行的模型通道才反映当前模型在这组固定题上的执行准确率。",
        "",
        "## 结果摘要",
        "",
        "| 指标 | 结果 | 含义 |",
        "|---|---:|---|",
        f"| 确定性操作规划精确匹配 | {planner['passed']}/{planner['total']}（{_percent(planner['rate'])}） | 动作、目标、风险、确认、语义和澄清字段全匹配 |",
        f"| 确定性并列指标聚合 | {multi_metric_compiler['passed']}/{multi_metric_compiler['total']}（{_percent(multi_metric_compiler['rate'])}） | 同表受控指标、指标独立过滤、全局过滤和回退边界匹配 |",
        f"| 确定性业务日历编译 | {calendar_compiler['passed']}/{calendar_compiler['total']}（{_percent(calendar_compiler['rate'])}） | DATE/声明型时间戳的财年、财季、工作日边界、结果和保守回退匹配 |",
        f"| 确定性业务维度聚合 | {dimension_compiler['passed']}/{dimension_compiler['total']}（{_percent(dimension_compiler['rate'])}） | 单表分组、同表下钻、受控指标和回退边界匹配 |",
        f"| 确定性时间趋势聚合 | {trend_compiler['passed']}/{trend_compiler['total']}（{_percent(trend_compiler['rate'])}） | 单表日/周/月/季度/年分桶、时间戳口径、受控指标和回退边界匹配 |",
        f"| 动态操作图与契约匹配 | {graph['passed']}/{graph['total']}（{_percent(graph['rate'])}） | 节点选择、依赖、跨表预检和输出契约全匹配 |",
        f"| 大 Schema 关系事实匹配 | {large_schema['passed']}/{large_schema['total']}（{_percent(large_schema['rate'])}） | 64 表合成 schema 的多跳关系、隔离和索引事实匹配 |",
        f"| 参考 SQL 执行结果匹配 | {sql['passed']}/{sql['total']}（{_percent(sql['rate'])}） | 参考 SQL 通过只读安全层后，列与行结果匹配 |",
        f"| 只读危险语句拦截召回 | {read['unsafe_blocked']}/{read['unsafe_total']}（{_percent(read['unsafe_recall'])}） | 固定危险查询被拒绝 |",
        f"| 只读有效语句接受率 | {read['policy_valid_accepted']}/{read['policy_valid_total']}（{_percent(read['policy_valid_acceptance'])}） | 固定有效只读语句被接受 |",
        f"| 写路径无效语句拦截召回 | {write['unsafe_blocked']}/{write['unsafe_total']}（{_percent(write['unsafe_recall'])}） | 无界写、多语句和越权类型被拒绝 |",
        f"| 写路径策略有效语句接受率 | {write['policy_valid_accepted']}/{write['policy_valid_total']}（{_percent(write['policy_valid_acceptance'])}） | 仍需预览和确认，不代表已落库 |",
        f"| 离线固定用例合计 | {offline['passed']}/{offline['total']}（{_percent(offline['rate'])}） | 仅用于版本回归，不与模型指标合并 |",
        f"| 真实模型 NL2SQL 执行准确率 | {_percent(model.get('execution_accuracy'))} | 状态：{model['status']}；固定模型题不并入离线合计 |",
        "",
        "## 评测范围",
        "",
    ]
    for name, scope in result["scope"].items():
        lines.append(f"- `{name}`：{scope}")
    lines.extend(["", "## 离线用例", ""])
    for title, group in (
        ("确定性规划", planner),
        ("确定性并列指标", multi_metric_compiler),
        ("确定性业务日历", calendar_compiler),
        ("确定性业务维度", dimension_compiler),
        ("确定性时间趋势", trend_compiler),
        ("动态操作图", graph),
        ("大 Schema 关系事实", large_schema),
        ("参考 SQL", sql),
        ("安全策略", security),
    ):
        lines.extend([f"### {title}", "", "| 用例 | 结果 | 问题/路径 |", "|---|---|---|"])
        for case in group["cases"]:
            label = case.get("question") or case.get("path") or "-"
            label = str(label).replace("|", "\\|").replace("\n", " ")
            lines.append(f"| `{case['id']}` | {_case_status(case)} | {label} |")
        lines.append("")
    lines.extend(["## 真实模型通道", ""])
    if model["status"] == "not_run":
        lines.append(model["reason"])
    else:
        identity = model["model_identity"]
        latency = model["latency_ms"]
        lines.extend([
            f"- 模型：`{identity['model']}`（配置 `{identity['config_name']}`，接口仅记录脱敏指纹 `{identity['endpoint_fingerprint']}`）",
            f"- 提示词契约：`{model['prompt_contract']}`；中位延迟 {latency['median']:.0f} ms；总耗时 {latency['total']:.0f} ms。",
            "",
            "| 用例 | 结果 | 延迟 | SQL/错误 |", "|---|---|---:|---|",
        ])
        for case in model["cases"]:
            detail = case.get("sql") or case.get("error") or "-"
            detail = str(detail).replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| `{case['id']}` | {_case_status(case)} | {float(case['latency_ms']):.0f} ms | {detail} |"
            )
    baseline = result.get("model_baseline")
    if baseline:
        lines.extend(["", "### 基线历史", "", f"- 本次运行：`{baseline['run_id']}`；历史共 {baseline['history_count']} 次。"])
        comparison = baseline.get("comparison")
        if comparison:
            lines.append(
                f"- 相对 `{comparison['baseline_run_id']}`：准确率变化 "
                f"{comparison['accuracy_delta']:+.1%}，回归 {len(comparison['regressions'])} 项，"
                f"改进 {len(comparison['improvements'])} 项；提示词变化={comparison['prompt_contract_changed']}，"
                f"模型变化={comparison['model_changed']}。"
            )
    lines.extend([
        "",
        "## 限制",
        "",
        "- 64 表通道验证本地关系分析和紧凑索引事实，不运行模型，不能代表大 schema 下的模型选表、长上下文或性能表现。",
        "- SQLite 结果不能替代 MySQL/PostgreSQL 真实兼容性验证。",
        "- 并列指标确定性通道只覆盖 SQLite 同表 2–6 个受控普通指标和最多一个全局枚举过滤；比率、算术表达、维度/趋势混合、自由条件及远程方言仍回退既有链路。",
        "- 业务日历确定性通道只覆盖 SQLite 声明型 DATE/DATETIME/TIMESTAMP 与显式存储口径；已归档 IANA/DST 可用于受控 UTC 换日，但跨表、分组和复杂条件仍不在该通道内。",
        "- 业务维度确定性通道只覆盖 SQLite 单表、一个显式维度或同表层级路径以及 COUNT/1–6 个受控普通指标；自由条件、比率、跨表和远程方言仍回退模型链路。",
        "- 安全用例验证已列出的策略边界，不等同于形式化安全证明或完整 SQL 语法覆盖。",
        "- 真实模型通道当前只有 12 个合成 SQLite 问题；单次 12/12 不能外推为真实业务准确率。模型输出具有波动，跨版本比较必须同时记录数据集哈希、提示词契约、模型身份和重复次数。",
        "",
    ])
    return "\n".join(lines)


def write_reports(result: dict, markdown_path: Path, json_path: Path) -> None:
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(result), encoding="utf-8")
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="运行 DB-Agent 固定 NL-to-Database 评测集")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET, help="评测集 JSON 路径")
    parser.add_argument("--with-model", action="store_true", help="额外运行真实模型 NL2SQL 通道")
    parser.add_argument("--llm-cfg", default=DEFAULT_LLM_CFG, help="真实模型通道使用的本地模型档案 key")
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN_REPORT, help="Markdown 报告路径")
    parser.add_argument("--json", dest="json_path", type=Path, default=DEFAULT_JSON_REPORT, help="JSON 报告路径")
    parser.add_argument("--no-report", action="store_true", help="只在终端输出，不写报告")
    parser.add_argument("--record-model-baseline", action="store_true", help="把本次真实模型结果追加到脱敏基线历史")
    parser.add_argument("--baseline-history", type=Path, default=model_baselines.DEFAULT_HISTORY, help="模型基线历史 JSON 路径")
    parser.add_argument("--baseline-label", default="", help="模型基线运行标签（不超过 80 字）")
    args = parser.parse_args()
    if args.record_model_baseline and not args.with_model:
        parser.error("--record-model-baseline 必须与 --with-model 同时使用")
    result = run_suite(args.dataset, with_model=args.with_model, llm_cfg=args.llm_cfg)
    if args.record_model_baseline:
        record = model_baselines.build_record(result, args.baseline_label)
        baseline = model_baselines.append_record(args.baseline_history, record)
        result["model_baseline"] = {
            "run_id": baseline["record"]["run_id"],
            "history_count": baseline["history_count"],
            "comparison": baseline["comparison"],
        }
    if not args.no_report:
        write_reports(result, args.markdown, args.json_path)
    model = result["model_nl2sql"]
    print(
        f"NL2DB EVAL {result['suite_version']}: offline "
        f"{result['offline']['passed']}/{result['offline']['total']}; "
        f"model={model['status']}"
    )
    if args.record_model_baseline:
        baseline_info = result["model_baseline"]
        comparison = baseline_info["comparison"]
        print(
            f"MODEL BASELINE {baseline_info['run_id']}: history={baseline_info['history_count']}; "
            + (
                f"accuracy_delta={comparison['accuracy_delta']:+.4f}; "
                f"regressions={len(comparison['regressions'])}"
                if comparison else "first compatible run"
            )
        )
    if not result["offline"]["all_cases_passed"]:
        return 1
    if args.with_model and model.get("passed") != model.get("total"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
