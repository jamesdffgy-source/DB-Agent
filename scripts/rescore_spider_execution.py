#!/usr/bin/env python3
"""Rescore stored DBQuill Spider predictions on released SQLite content.

This is deliberately a single-database execution agreement metric.  It uses
one official released database per schema and therefore must not be reported
as Spider Test Suite Accuracy, which requires multiple generated databases.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from spider_execution_scoring import (
    COMPARATOR_CONTRACT,
    REFERENCE_IMPLEMENTATION,
    score_execution,
    sha256_file,
    sha256_json,
)


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

from dbquill_core import (
    DBConnector,
    NL2SQLExecutor,
    SQLSecurity,
    SQLiteRelationalPlanRenderer,
    SchemaDiscovery,
)

DEFAULT_SOURCE = ROOT / "benchmark_results" / "spider_relational_ir_15_deepseek_full_run1.json"
DEFAULT_HISTORICAL_REPO = ROOT / "benchmark_data" / "spider_official_repo"
DEFAULT_OFFICIAL_DATA = ROOT / "benchmark_data" / "spider_data_official" / "spider_data"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "spider_relational_ir_15_execution_rescore.json"
DEFAULT_MARKDOWN = ROOT / "benchmark_results" / "spider_relational_ir_15_execution_rescore.md"
INFRASTRUCTURE_ERROR_CATEGORY = "llm_infrastructure_error"
SCORING_CONTRACT = "dbquill-spider-single-db-execution-rescore-v1"
ARCHITECTURE_REPLAY_CONTRACT = "dbquill-current-local-semantic-gate-replay-v2"


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(value.rstrip() + "\n", encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _dataset_paths(root: Path) -> tuple[Path, Path]:
    for base in (root / "evaluation_examples" / "examples", root):
        dev = base / "dev.json"
        tables = base / "tables.json"
        if dev.is_file() and tables.is_file():
            return dev, tables
    raise FileNotFoundError(f"cannot find dev.json/tables.json under {root}")


def _case_identity(case: dict) -> tuple[str, str, str]:
    return str(case.get("db_id")), str(case.get("question")), str(case.get("query"))


def _canonical_schema(record: dict) -> dict:
    return {
        key: record.get(key)
        for key in (
            "db_id", "table_names_original", "column_names_original", "column_types",
            "primary_keys", "foreign_keys",
        )
    }


def _validate_dataset_compatibility(
    source: dict,
    historical_dev_path: Path,
    historical_tables_path: Path,
    official_dev_path: Path,
    official_tables_path: Path,
) -> dict:
    source_dataset = source.get("dataset") or {}
    historical_hashes = {
        "dev_sha256": sha256_file(historical_dev_path),
        "tables_sha256": sha256_file(historical_tables_path),
    }
    for key, actual in historical_hashes.items():
        recorded = source_dataset.get(key)
        if recorded != actual:
            raise RuntimeError(
                f"source result {key}={recorded!r} does not match historical file {actual}"
            )

    historical_dev = json.loads(historical_dev_path.read_text(encoding="utf-8"))
    official_dev = json.loads(official_dev_path.read_text(encoding="utf-8"))
    if len(historical_dev) != len(official_dev):
        raise RuntimeError("historical and official dev case counts differ")
    case_mismatches = [
        index for index, (old, new) in enumerate(zip(historical_dev, official_dev))
        if _case_identity(old) != _case_identity(new)
    ]
    if case_mismatches:
        raise RuntimeError(
            "historical and official dev identities differ at indices: "
            + ", ".join(map(str, case_mismatches[:20]))
        )

    historical_tables = json.loads(historical_tables_path.read_text(encoding="utf-8"))
    official_tables = json.loads(official_tables_path.read_text(encoding="utf-8"))
    old_schemas = {item["db_id"]: _canonical_schema(item) for item in historical_tables}
    new_schemas = {item["db_id"]: _canonical_schema(item) for item in official_tables}
    if old_schemas != new_schemas:
        mismatches = sorted(
            db_id for db_id in set(old_schemas) | set(new_schemas)
            if old_schemas.get(db_id) != new_schemas.get(db_id)
        )
        raise RuntimeError(
            "historical and official canonical schemas differ: "
            + ", ".join(mismatches[:20])
        )

    result_mismatches = []
    for result in source.get("results") or []:
        index = int(result["index"])
        if not 0 <= index < len(official_dev):
            result_mismatches.append(str(result.get("id")))
            continue
        case = official_dev[index]
        identity = (
            str(result.get("db_id")), str(result.get("question")),
            str(result.get("gold_sql")),
        )
        if identity != _case_identity(case):
            result_mismatches.append(str(result.get("id")))
    if result_mismatches:
        raise RuntimeError(
            "stored result identities do not match official dev: "
            + ", ".join(result_mismatches[:20])
        )

    return {
        "historical_raw_hashes_match_source": True,
        "dev_case_identity_match": True,
        "dev_case_count": len(official_dev),
        "canonical_schema_match": True,
        "canonical_schema_count": len(new_schemas),
        "stored_result_identity_match": True,
        "stored_result_count": len(source.get("results") or []),
        "official_dev_sha256": sha256_file(official_dev_path),
        "official_tables_sha256": sha256_file(official_tables_path),
    }


def _database_manifest(database_root: Path, db_ids: set[str]) -> tuple[dict[str, Path], dict]:
    paths: dict[str, Path] = {}
    entries = []
    for db_id in sorted(db_ids):
        path = database_root / db_id / f"{db_id}.sqlite"
        if not path.is_file():
            raise FileNotFoundError(f"missing released database: {path}")
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        try:
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
        finally:
            connection.close()
        if not quick_check or str(quick_check[0]).casefold() != "ok":
            raise RuntimeError(f"SQLite quick_check failed for {db_id}: {quick_check}")
        entry = {
            "db_id": db_id,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        entries.append(entry)
        paths[db_id] = path
    return paths, {
        "database_count": len(entries),
        "total_bytes": sum(item["bytes"] for item in entries),
        "manifest_sha256": sha256_json(entries),
        "entries": entries,
    }


def _rate(passed: int, total: int) -> float | None:
    return round(passed / total, 4) if total else None


def _architecture_replay(
    item: dict,
    database_path: Path,
    schema_cache: dict[str, Any],
    *,
    timeout_s: float,
    max_rows: int,
) -> dict:
    """Replay the current local semantic architecture without an LLM call."""
    predicted_sql = str(item.get("predicted_sql") or "").strip()
    rejected_sql = str(item.get("rejected_candidate_sql") or "").strip()
    sql = predicted_sql or rejected_sql
    candidate_source = (
        "predicted_sql" if predicted_sql else
        "rejected_candidate_sql" if rejected_sql else ""
    )
    if not sql:
        return {
            "status": "not_applicable",
            "reason": "stored run has no query or rejected candidate",
        }
    db_id = str(item["db_id"])
    connector = DBConnector(str(database_path))
    schema = schema_cache.get(db_id)
    if schema is None:
        schema = SchemaDiscovery(connector, sample_rows=5).discover()
        schema_cache[db_id] = schema
    executor = NL2SQLExecutor(SQLSecurity(connector), schema)
    contract = executor._compile_relational_contract(str(item.get("question") or ""))
    executor.last_relational_contract = contract
    question = str(item.get("question") or "")
    initial_conflict = executor._semantic_conflict(question, sql)
    conflict = initial_conflict
    local_repair = None
    local_repair_execution = None
    native_plan = executor._compile_native_relational_plan(
        question, contract, allowed_tables=None,
    )
    native_plan_dict = None
    if native_plan is not None:
        native_plan_dict = native_plan.as_dict()
        native_sql = SQLiteRelationalPlanRenderer(schema).render(native_plan)
        native_conflict = executor._semantic_conflict(question, native_sql)
        if native_conflict is None:
            sql = native_sql
            candidate_source = "native_relational_plan_compiler"
            conflict = None
            native_score = score_execution(
                database_path,
                sql,
                str(item.get("gold_sql") or ""),
                timeout_s=timeout_s,
                max_rows=max_rows,
            )
            local_repair_execution = {
                key: native_score.get(key)
                for key in (
                    "agreement", "score_status", "error_category",
                    "evaluation_error", "comparison_reason",
                    "predicted_row_count", "gold_row_count",
                    "predicted_column_count", "gold_column_count",
                    "predicted_result_hash", "gold_result_hash",
                )
                if key in native_score
            }
            local_repair = {
                "selected": {"candidate_id": candidate_source, "sql": sql},
                "diagnostic": {
                    "status": "native_relational_plan_compiled",
                    "selection_basis": "typed_contract_before_model_generation",
                },
            }
        else:
            native_plan_dict = {
                **native_plan_dict,
                "render_conflict": native_conflict.as_dict(),
            }
    if conflict is not None:
        local_repair = executor._try_local_contract_repair(
            question=question,
            bad_sql=sql,
            conflict=conflict,
            allowed_tables=None,
        )
        if local_repair is not None and local_repair.get("selected") is not None:
            selected = local_repair["selected"]
            sql = str(selected.get("sql") or "").strip()
            candidate_source = str(selected.get("candidate_id") or "local_compiler")
            conflict = executor._semantic_conflict(question, sql)
            local_repair_score = score_execution(
                database_path,
                sql,
                str(item.get("gold_sql") or ""),
                timeout_s=timeout_s,
                max_rows=max_rows,
            )
            local_repair_execution = {
                key: local_repair_score.get(key)
                for key in (
                    "agreement", "score_status", "error_category",
                    "evaluation_error", "comparison_reason",
                    "predicted_row_count", "gold_row_count",
                    "predicted_column_count", "gold_column_count",
                    "predicted_result_hash", "gold_result_hash",
                )
                if key in local_repair_score
            }
    status = (
        "needs_clarification" if contract.ambiguities
        else "rejected" if conflict is not None
        else "accepted"
    )
    v17_features = [
        name for name, value in (
            ("predicate_literal_provenance", contract.predicate_literal_policies),
            ("all_values_output_grain", [
                requirement for requirement in contract.set_requirements
                if requirement.get("operator") == "ALL_VALUES"
                and requirement.get("row_grain")
                and requirement.get("relation_path")
            ]),
            ("distinct_output_tuple", contract.distinct_row_requirements),
            ("boolean_scope_ambiguity", contract.ambiguities),
            ("resolved_boolean_filter_scope", contract.boolean_filter_requirements),
            ("spending_sum_measure", [
                requirement for requirement in contract.aggregate_requirements
                if str(requirement.get("function") or "").upper() == "SUM"
                and "spending_amount_measure" in contract.evidence
            ]),
        ) if value
    ]
    v18_features = [
        name for name, value in (
            ("quoted_literal_filter", (
                "unique_sample_grounded_quoted_literal" in contract.evidence
            )),
            ("qualified_anti_relationship", (
                "explicit_negative_relationship" in contract.evidence
                and "unique_sample_grounded_quoted_literal" in contract.evidence
            )),
            ("what_superlative_cardinality", (
                "single_row" == contract.tie_policy
                and str(item.get("question") or "").strip().casefold().startswith(
                    "what is"
                )
            )),
            ("exact_value_operator_gate", (
                initial_conflict is not None
                and initial_conflict.code == "wildcard_literal_broadening"
            )),
            ("deterministic_single_row_tie_breaker", (
                bool(contract.tie_breaker_columns)
            )),
            ("all_values_visible_tuple_set", (
                "all_values_projection_preserves_set_semantics"
                in contract.evidence
            )),
        ) if value
    ]
    return {
        "status": status,
        "candidate_source": candidate_source,
        "contract_version": contract.version,
        "contract_actionable": contract.is_actionable(),
        "relational_contract": contract.as_dict(),
        "native_relational_plan": native_plan_dict,
        "v17_features": v17_features,
        "v18_features": v18_features,
        "local_repair_status": (
            (local_repair.get("diagnostic") or {}).get("status")
            if local_repair is not None else None
        ),
        "local_repair_sql": sql if local_repair is not None else None,
        "final_sql": sql if status == "accepted" else None,
        "local_repair_execution": local_repair_execution,
        "original_conflict_code": (
            initial_conflict.code if initial_conflict is not None else None
        ),
        "original_conflict_constraints": (
            initial_conflict.constraints if initial_conflict is not None else None
        ),
        "conflict_code": conflict.code if conflict is not None else None,
        "conflict_message": conflict.message if conflict is not None else None,
    }


def _architecture_replay_summary(results: list[dict]) -> dict:
    applicable = [
        item for item in results
        if (item.get("architecture_replay") or {}).get("status")
        != "not_applicable"
    ]
    errors = [
        item for item in applicable
        if (item.get("architecture_replay") or {}).get("status") == "error"
    ]
    statuses = Counter(
        str((item.get("architecture_replay") or {}).get("status") or "missing")
        for item in applicable
    )
    by_execution = Counter()
    for item in applicable:
        execution = "execution_pass" if item.get("execution_agreement") else "execution_fail"
        gate = str((item.get("architecture_replay") or {}).get("status") or "missing")
        by_execution[f"{execution}__{gate}"] += 1
    feature_cases = Counter(
        feature
        for item in applicable
        for feature in (item.get("architecture_replay") or {}).get("v17_features") or []
    )
    v18_feature_cases = Counter(
        feature
        for item in applicable
        for feature in (item.get("architecture_replay") or {}).get("v18_features") or []
    )
    local_repair_cases = Counter(
        str((item.get("architecture_replay") or {}).get("local_repair_status"))
        for item in applicable
        if (item.get("architecture_replay") or {}).get("local_repair_status")
    )
    local_repair_execution_improvements = [
        str(item.get("id")) for item in applicable
        if not item.get("execution_agreement")
        and bool(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("agreement")
        )
    ]
    stable_tie_resolution_divergences = [
        str(item.get("id")) for item in applicable
        if item.get("execution_agreement")
        and str(
            (item.get("architecture_replay") or {}).get("local_repair_status") or ""
        ) == "local_deterministic_tie_compiled"
        and not bool(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("agreement")
        )
        and str(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("score_status") or ""
        ) == "scored"
        and str(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("error_category") or ""
        ) in {"row_order_only", "value_join_or_filter"}
        and (
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("predicted_row_count")
            == ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("gold_row_count")
        )
        and (
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("predicted_column_count")
            == ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("gold_column_count")
        )
    ]
    local_repair_execution_regressions = [
        str(item.get("id")) for item in applicable
        if item.get("execution_agreement")
        and str(item.get("id")) not in stable_tie_resolution_divergences
        and (item.get("architecture_replay") or {}).get("local_repair_execution")
        and not bool(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("agreement")
        )
    ]
    local_repair_benchmark_errors = [
        str(item.get("id")) for item in applicable
        if str(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("score_status") or ""
        ) == "benchmark_error"
    ]
    return {
        "contract": ARCHITECTURE_REPLAY_CONTRACT,
        "applicable": len(applicable),
        "errors": len(errors),
        "statuses": dict(statuses),
        "execution_gate_matrix": dict(by_execution),
        "v17_feature_cases": dict(feature_cases),
        "v18_feature_cases": dict(v18_feature_cases),
        "local_repair_cases": dict(local_repair_cases),
        "local_repair_execution_improvements": local_repair_execution_improvements,
        "stable_tie_resolution_divergences": stable_tie_resolution_divergences,
        "local_repair_execution_regressions": local_repair_execution_regressions,
        "local_repair_benchmark_errors": local_repair_benchmark_errors,
        "interpretation": (
            "This is a counterfactual replay over stored SQL, not a regenerated "
            "model benchmark. Single-database execution agreement is evidence, "
            "not ground truth, so gate disagreement is reported rather than "
            "automatically labeled correct or incorrect."
        ),
    }


def _summary(results: list[dict]) -> dict:
    excluded = [item for item in results if item["score_status"] == "excluded_infrastructure"]
    benchmark_errors = [item for item in results if item["score_status"] == "benchmark_error"]
    scored = [item for item in results if item["score_status"] == "scored"]
    passed = [item for item in scored if item["execution_agreement"]]
    nonempty = [item for item in scored if not item.get("gold_empty")]
    nonempty_passed = [item for item in nonempty if item["execution_agreement"]]
    empty_matches = [item for item in scored if item.get("empty_result_match")]

    transitions = Counter()
    for item in scored:
        exact = bool(item.get("exact_match"))
        execution = bool(item.get("execution_agreement"))
        transitions[
            "both_pass" if exact and execution else
            "exact_only" if exact else
            "execution_only" if execution else
            "both_fail"
        ] += 1

    by_hardness = {}
    for hardness in ("easy", "medium", "hard", "extra"):
        scoped = [item for item in scored if item.get("hardness") == hardness]
        count = sum(bool(item["execution_agreement"]) for item in scoped)
        by_hardness[hardness] = {
            "passed": count,
            "total": len(scoped),
            "rate": _rate(count, len(scoped)),
        }
    execution_ms = [
        float(item.get("prediction_execution_ms") or 0)
        + float(item.get("gold_execution_ms") or 0)
        for item in scored
    ]
    parser_unsupported = [
        item for item in scored if item.get("source_error_category") == "official_parser_unsupported"
    ]
    return {
        "coverage": {
            "scored": len(scored),
            "attempted": len(results),
            "excluded_infrastructure": len(excluded),
            "benchmark_errors": len(benchmark_errors),
            "complete": not excluded and not benchmark_errors and len(scored) == len(results),
        },
        "single_database_execution_agreement": {
            "passed": len(passed),
            "total": len(scored),
            "rate": _rate(len(passed), len(scored)),
        },
        "nonempty_gold_execution_agreement": {
            "passed": len(nonempty_passed),
            "total": len(nonempty),
            "rate": _rate(len(nonempty_passed), len(nonempty)),
        },
        "empty_result_matches": len(empty_matches),
        "exact_execution_transitions": dict(transitions),
        "official_parser_unsupported": {
            "execution_passed": sum(item["execution_agreement"] for item in parser_unsupported),
            "total": len(parser_unsupported),
        },
        "by_hardness": by_hardness,
        "failure_categories": dict(Counter(
            str(item.get("execution_error_category") or "unknown")
            for item in scored if not item["execution_agreement"]
        ).most_common()),
        "execution_latency_ms": {
            "median_combined": round(statistics.median(execution_ms), 3) if execution_ms else 0.0,
            "max_combined": round(max(execution_ms), 3) if execution_ms else 0.0,
        },
    }


def _markdown(payload: dict) -> str:
    summary = payload["summary"]
    metric = summary["single_database_execution_agreement"]
    nonempty = summary["nonempty_gold_execution_agreement"]
    transitions = summary["exact_execution_transitions"]
    replay = summary["architecture_replay"]
    lines = [
        "# DBQuill Spider 单数据库执行重评分",
        "",
        f"- 来源运行：`{payload['source']['prompt_contract']}` / `{payload['source']['sample_fingerprint'][:16]}`",
        f"- 单数据库执行一致：{metric['passed']}/{metric['total']} ({(metric['rate'] or 0) * 100:.1f}%)",
        f"- 非空 gold 执行一致：{nonempty['passed']}/{nonempty['total']} ({(nonempty['rate'] or 0) * 100:.1f}%)",
        f"- 空结果一致（证据较弱）：{summary['empty_result_matches']}",
        f"- 旧 parser unsupported 中真实执行一致：{summary['official_parser_unsupported']['execution_passed']}/{summary['official_parser_unsupported']['total']}",
        "",
        "> 该结果只在每个 schema 的一个官方发布 SQLite 数据库上比较。它能发现大量结构 Exact 的假阴性，"
        "但空结果与数据偶然性仍可能产生假阳性；没有多组扰动数据库，不能称为 Spider Test Suite Accuracy。",
        "",
        "## Exact 与执行迁移",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
        f"| Exact 与执行都通过 | {transitions.get('both_pass', 0)} |",
        f"| 仅 Exact 通过 | {transitions.get('exact_only', 0)} |",
        f"| 仅执行通过 | {transitions.get('execution_only', 0)} |",
        f"| 两者都失败 | {transitions.get('both_fail', 0)} |",
        "",
        "## 当前架构反事实重放",
        "",
        f"- 可重放候选：{replay['applicable']}",
        f"- 当前门禁接受：{replay['statuses'].get('accepted', 0)}",
        f"- 当前门禁拒绝：{replay['statuses'].get('rejected', 0)}",
        f"- 当前门禁要求澄清：{replay['statuses'].get('needs_clarification', 0)}",
        f"- 重放错误：{replay['errors']}",
        f"- 本地修复执行改进：{len(replay['local_repair_execution_improvements'])}",
        f"- 稳定并列策略与未定序 gold 分歧：{len(replay['stable_tie_resolution_divergences'])}",
        f"- 本地修复执行回归：{len(replay['local_repair_execution_regressions'])}",
        "",
        "> 这是把当前本地语义门禁应用到历史 SQL 的反事实重放，不是重新调用模型。"
        "单数据库执行一致也不是真值，因此门禁与执行的差异只作架构诊断，不直接判成误杀或漏检。",
        "",
        "## 执行失败分类",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
    ]
    for category, count in summary["failure_categories"].items():
        lines.append(f"| `{category}` | {count} |")
    lines.extend([
        "",
        f"评分契约：`{payload['scoring_contract']}`  ",
        f"结果比较器：`{payload['metric']['comparator_contract']}`  ",
        f"数据库清单：`{payload['dataset']['databases']['manifest_sha256']}`",
    ])
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rescore stored Spider predictions on released SQLite databases")
    parser.add_argument("--source-result", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--historical-repo", type=Path, default=DEFAULT_HISTORICAL_REPO)
    parser.add_argument("--official-data", type=Path, default=DEFAULT_OFFICIAL_DATA)
    parser.add_argument("--database-root", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--max-rows", type=int, default=200_000)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_path = args.source_result.resolve()
    source = json.loads(source_path.read_text(encoding="utf-8"))
    historical_dev, historical_tables = _dataset_paths(args.historical_repo.resolve())
    official_data = args.official_data.resolve()
    official_dev, official_tables = _dataset_paths(official_data)
    compatibility = _validate_dataset_compatibility(
        source, historical_dev, historical_tables, official_dev, official_tables,
    )
    database_root = (
        args.database_root.resolve() if args.database_root else official_data / "database"
    )
    db_ids = {str(item["db_id"]) for item in source.get("results") or []}
    database_paths, database_manifest = _database_manifest(database_root, db_ids)

    results = []
    schema_cache: dict[str, Any] = {}
    started = time.perf_counter()
    for position, item in enumerate(source.get("results") or [], 1):
        result = {
            "id": item.get("id"),
            "index": item.get("index"),
            "db_id": item.get("db_id"),
            "hardness": item.get("hardness"),
            "question": item.get("question"),
            "gold_sql": item.get("gold_sql"),
            "predicted_sql": item.get("predicted_sql"),
            "exact_match": bool(item.get("exact_match")),
            "source_error_category": item.get("error_category"),
            "native_relational_plan": bool(item.get("native_relational_plan")),
        }
        if item.get("error_category") == INFRASTRUCTURE_ERROR_CATEGORY:
            result.update({
                "score_status": "excluded_infrastructure",
                "execution_agreement": False,
                "execution_error_category": INFRASTRUCTURE_ERROR_CATEGORY,
            })
        elif item.get("answer_kind") != "query" or not str(item.get("predicted_sql") or "").strip():
            result.update({
                "score_status": "scored",
                "execution_agreement": False,
                "execution_error_category": item.get("error_category") or "no_query_prediction",
                "gold_empty": None,
            })
        else:
            score = score_execution(
                database_paths[str(item["db_id"])],
                str(item["predicted_sql"]),
                str(item["gold_sql"]),
                timeout_s=args.timeout,
                max_rows=args.max_rows,
            )
            result.update({
                "score_status": score["score_status"],
                "execution_agreement": bool(score["agreement"]),
                "execution_error_category": score.get("error_category"),
                **{key: value for key, value in score.items() if key not in {
                    "score_status", "agreement", "error_category",
                }},
            })
        try:
            result["architecture_replay"] = _architecture_replay(
                item,
                database_paths[str(item["db_id"])],
                schema_cache,
                timeout_s=args.timeout,
                max_rows=args.max_rows,
            )
        except Exception as exc:  # noqa: BLE001 - benchmark must record the case
            result["architecture_replay"] = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
        results.append(result)
        status = "PASS" if result["execution_agreement"] else "FAIL"
        print(
            f"[{position}/{len(source.get('results') or [])}] {result['id']} {status} "
            f"{result.get('execution_error_category') or ''}",
            flush=True,
        )

    summary = _summary(results)
    summary["architecture_replay"] = _architecture_replay_summary(results)
    replay_summary = summary["architecture_replay"]
    if replay_summary["local_repair_benchmark_errors"]:
        raise RuntimeError(
            "local repair execution benchmark errors: "
            + ", ".join(replay_summary["local_repair_benchmark_errors"])
        )
    if replay_summary["local_repair_execution_regressions"]:
        raise RuntimeError(
            "local repair execution regressions: "
            + ", ".join(replay_summary["local_repair_execution_regressions"])
        )
    if summary["coverage"]["benchmark_errors"]:
        raise RuntimeError(
            f"execution rescore has {summary['coverage']['benchmark_errors']} benchmark errors; "
            "refusing to publish a partial metric"
        )
    if summary["architecture_replay"]["errors"]:
        raise RuntimeError(
            "architecture replay has "
            f"{summary['architecture_replay']['errors']} errors; refusing to publish"
        )
    payload = {
        "schema_version": 1,
        "status": "completed",
        "completed_at_unix": time.time(),
        "run_wall_ms": round((time.perf_counter() - started) * 1000, 3),
        "source": {
            "result_sha256": sha256_file(source_path),
            "prompt_contract": source.get("prompt_contract"),
            "scoring_contract": source.get("scoring_contract"),
            "sample_fingerprint": (source.get("sample") or {}).get("fingerprint"),
            "sample_size": (source.get("sample") or {}).get("size"),
            "model_identity": source.get("model_identity"),
        },
        "dataset": {
            "name": "Spider 1.0 dev released SQLite content",
            "official_page": "https://yale-lily.github.io/spider",
            "official_dev_sha256": compatibility.pop("official_dev_sha256"),
            "official_tables_sha256": compatibility.pop("official_tables_sha256"),
            "compatibility": compatibility,
            "databases": database_manifest,
        },
        "metric": {
            "name": "single_database_execution_agreement",
            "comparator_contract": COMPARATOR_CONTRACT,
            "reference": REFERENCE_IMPLEMENTATION,
            "predicted_values_evaluated": True,
            "distinct_preserved": True,
            "empty_result_matches_reported_separately": True,
            "is_test_suite_accuracy": False,
            "limitation": "one released SQLite database per schema; no perturbation databases",
        },
        "architecture_replay": {
            "contract": ARCHITECTURE_REPLAY_CONTRACT,
            "model_calls": 0,
            "uses_current_local_schema_and_semantic_gates": True,
        },
        "scoring_contract": SCORING_CONTRACT,
        "execution_limits": {"timeout_s": args.timeout, "max_rows": args.max_rows},
        "results": results,
        "summary": summary,
    }
    _atomic_json(args.output.resolve(), payload)
    _atomic_text(args.markdown.resolve(), _markdown(payload))
    metric = summary["single_database_execution_agreement"]
    print(
        f"Single-database execution agreement: {metric['passed']}/{metric['total']} "
        f"({(metric['rate'] or 0) * 100:.1f}%)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
