#!/usr/bin/env python3
"""Rescore stored DBQuill Spider predictions on official test-suite DBs."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import time
from collections import Counter
from pathlib import Path

from rescore_spider_execution import _dataset_paths, _validate_dataset_compatibility
from spider_execution_scoring import (
    REFERENCE_IMPLEMENTATION,
    TEST_SUITE_COMPARATOR_CONTRACT,
    normalize_test_suite_sql,
    score_test_suite,
    sha256_file,
    sha256_json,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "benchmark_results" / "spider_relational_ir_15_deepseek_full_run1.json"
DEFAULT_SINGLE = ROOT / "benchmark_results" / "spider_relational_ir_15_execution_rescore.json"
DEFAULT_HISTORICAL_REPO = ROOT / "benchmark_data" / "spider_official_repo"
DEFAULT_OFFICIAL_DATA = ROOT / "benchmark_data" / "spider_data_official" / "spider_data"
DEFAULT_SUITE_REPO = ROOT / "benchmark_data" / "test_suite_sql_eval"
DEFAULT_ARCHIVE = ROOT / "benchmark_data" / "testsuitedatabases.zip"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "spider_relational_ir_15_test_suite_rescore.json"
DEFAULT_MARKDOWN = ROOT / "benchmark_results" / "spider_relational_ir_15_test_suite_rescore.md"
EXPECTED_UPSTREAM_COMMIT = "e97acc546ecbee8fa27fa8dbf025ef61493a876c"
EXPECTED_ARCHIVE_SHA256 = "9ec24ea8debc6bd04abfe137b5f1a739b5a8836f32c0464e4dfc94eb7f41da96"
SCORING_CONTRACT = "dbquill-spider-test-suite-execution-rescore-v1"
INFRASTRUCTURE_ERROR_CATEGORY = "llm_infrastructure_error"


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


def _git_head(repo: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.stdout.strip()


def _suite_manifest(database_root: Path, db_ids: set[str]) -> tuple[dict[str, list[Path]], dict]:
    suites: dict[str, list[Path]] = {}
    entries: list[dict] = []
    for db_id in sorted(db_ids):
        directory = database_root / db_id
        paths = sorted(path.resolve() for path in directory.glob("*.sqlite") if path.is_file())
        primary = (directory / f"{db_id}.sqlite").resolve()
        if primary not in paths:
            raise FileNotFoundError(f"missing primary test-suite database: {primary}")
        for path in paths:
            with path.open("rb") as handle:
                header = handle.read(16)
            if header != b"SQLite format 3\x00":
                raise RuntimeError(f"invalid SQLite header: {path}")
            entries.append({
                "db_id": db_id,
                "name": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            })
        connection = sqlite3.connect(f"file:{primary}?mode=ro", uri=True)
        try:
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
        finally:
            connection.close()
        if not quick_check or str(quick_check[0]).casefold() != "ok":
            raise RuntimeError(f"SQLite quick_check failed for {db_id}: {quick_check}")
        suites[db_id] = paths
    return suites, {
        "database_count": len(entries),
        "schema_count": len(suites),
        "total_bytes": sum(item["bytes"] for item in entries),
        "manifest_sha256": sha256_json(entries),
        "by_schema": {
            db_id: {
                "database_count": len(paths),
                "total_bytes": sum(path.stat().st_size for path in paths),
            }
            for db_id, paths in suites.items()
        },
    }


def _load_single_result(path: Path, source_sha256: str) -> dict[str, dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    recorded = str((payload.get("source") or {}).get("result_sha256") or "")
    if recorded != source_sha256:
        raise RuntimeError(
            f"single-database result source hash {recorded!r} != {source_sha256!r}"
        )
    return {str(item["id"]): item for item in payload.get("results") or []}


def _rate(passed: int, total: int) -> float | None:
    return round(passed / total, 4) if total else None


def _metric(results: list[dict], key: str) -> dict:
    scored = [item for item in results if item["score_status"] == "scored"]
    passed = [item for item in scored if bool((item.get(key) or {}).get("agreement"))]
    return {"passed": len(passed), "total": len(scored), "rate": _rate(len(passed), len(scored))}


def _summary(results: list[dict]) -> dict:
    benchmark_errors = [item for item in results if item["score_status"] == "benchmark_error"]
    excluded = [item for item in results if item["score_status"] == "excluded_infrastructure"]
    scored = [item for item in results if item["score_status"] == "scored"]
    strict = _metric(results, "strict_product")
    upstream = _metric(results, "upstream_compatible")
    transitions = Counter()
    for item in scored:
        single = bool(item.get("single_database_agreement"))
        suite = bool((item.get("strict_product") or {}).get("agreement"))
        transitions[
            "both_pass" if single and suite else
            "single_only" if single else
            "suite_only" if suite else
            "both_fail"
        ] += 1
    by_hardness = {}
    for hardness in ("easy", "medium", "hard", "extra"):
        scoped = [item for item in scored if item.get("hardness") == hardness]
        passed = sum(bool((item.get("strict_product") or {}).get("agreement")) for item in scoped)
        by_hardness[hardness] = {
            "passed": passed,
            "total": len(scoped),
            "rate": _rate(passed, len(scoped)),
        }
    failures = [item for item in scored if not bool((item.get("strict_product") or {}).get("agreement"))]
    repaired = [item for item in scored if item.get("local_repair")]
    repair_strict_improvements = [
        str(item.get("id")) for item in repaired
        if not bool((item.get("strict_product") or {}).get("agreement"))
        and bool(((item.get("local_repair") or {}).get("strict_product") or {}).get("agreement"))
    ]
    stable_tie_resolution_divergences = [
        str(item.get("id")) for item in repaired
        if bool((item.get("strict_product") or {}).get("agreement"))
        and not bool(((item.get("local_repair") or {}).get("strict_product") or {}).get("agreement"))
        and str((item.get("local_repair") or {}).get("status") or "")
        == "local_deterministic_tie_compiled"
    ]
    repair_strict_regressions = [
        str(item.get("id")) for item in repaired
        if bool((item.get("strict_product") or {}).get("agreement"))
        and not bool(((item.get("local_repair") or {}).get("strict_product") or {}).get("agreement"))
        and str(item.get("id")) not in stable_tie_resolution_divergences
    ]
    return {
        "coverage": {
            "attempted": len(results),
            "scored": len(scored),
            "excluded_infrastructure": len(excluded),
            "benchmark_errors": len(benchmark_errors),
            "complete": not excluded and not benchmark_errors and len(scored) == len(results),
        },
        "upstream_compatible_test_suite_accuracy": upstream,
        "strict_product_test_suite_accuracy": strict,
        "distinct_sensitive_cases": sum(
            bool((item.get("upstream_compatible") or {}).get("agreement"))
            != bool((item.get("strict_product") or {}).get("agreement"))
            for item in scored
        ),
        "single_to_test_suite_transitions": dict(transitions),
        "newly_exposed_single_database_false_positives": transitions.get("single_only", 0),
        "strict_failure_categories": dict(Counter(
            str((item.get("strict_product") or {}).get("error_category") or "unknown")
            for item in failures
        ).most_common()),
        "by_hardness_strict": by_hardness,
        "local_repair_test_suite": {
            "cases": len(repaired),
            "strict_improvements": repair_strict_improvements,
            "stable_tie_resolution_divergences": stable_tie_resolution_divergences,
            "strict_regressions": repair_strict_regressions,
        },
    }


def _markdown(payload: dict) -> str:
    summary = payload["summary"]
    upstream = summary["upstream_compatible_test_suite_accuracy"]
    strict = summary["strict_product_test_suite_accuracy"]
    transitions = summary["single_to_test_suite_transitions"]
    repairs = summary["local_repair_test_suite"]
    lines = [
        "# DBQuill Spider 多数据库 Test Suite 重评分",
        "",
        f"- 官方兼容口径（预测值、忽略 DISTINCT）：{upstream['passed']}/{upstream['total']} ({(upstream['rate'] or 0) * 100:.1f}%)",
        f"- 严格产品口径（预测值、保留 DISTINCT）：{strict['passed']}/{strict['total']} ({(strict['rate'] or 0) * 100:.1f}%)",
        f"- 单库通过、扰动库失败：{summary['newly_exposed_single_database_false_positives']}",
        f"- DISTINCT 敏感题：{summary['distinct_sensitive_cases']}",
        f"- 本地修复多库复核：{repairs['cases']} 条；改进 {len(repairs['strict_improvements'])}、"
        f"稳定并列策略分歧 {len(repairs['stable_tie_resolution_divergences'])}、"
        f"非并列回归 {len(repairs['strict_regressions'])}",
        "",
        "> 每条 SQL 必须在同 schema 的全部官方扰动数据库上与 gold 结果一致。项目会预测值，"
        "因此不注入 gold 值；严格产品口径额外保留 DISTINCT 语义。执行仍经过 DBQuill 只读、"
        "单语句、超时和结果上限门禁，因此是官方语义的有界安全实现。",
        "",
        "## 单数据库到 Test Suite 迁移",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
        f"| 两者都通过 | {transitions.get('both_pass', 0)} |",
        f"| 仅单数据库通过 | {transitions.get('single_only', 0)} |",
        f"| 仅 Test Suite 通过 | {transitions.get('suite_only', 0)} |",
        f"| 两者都失败 | {transitions.get('both_fail', 0)} |",
        "",
        "## 严格口径失败分类",
        "",
        "| 分类 | 数量 |",
        "|---|---:|",
    ]
    for category, count in summary["strict_failure_categories"].items():
        lines.append(f"| `{category}` | {count} |")
    lines.extend([
        "",
        f"评分契约：`{payload['scoring_contract']}`  ",
        f"比较器：`{payload['metric']['comparator_contract']}`  ",
        f"测试库清单：`{payload['dataset']['databases']['manifest_sha256']}`",
    ])
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rescore Spider predictions on official test-suite databases")
    parser.add_argument("--source-result", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--single-database-result", type=Path, default=DEFAULT_SINGLE)
    parser.add_argument("--historical-repo", type=Path, default=DEFAULT_HISTORICAL_REPO)
    parser.add_argument("--official-data", type=Path, default=DEFAULT_OFFICIAL_DATA)
    parser.add_argument("--test-suite-repo", type=Path, default=DEFAULT_SUITE_REPO)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--max-rows", type=int, default=200_000)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_path = args.source_result.resolve()
    source_sha256 = sha256_file(source_path)
    source = json.loads(source_path.read_text(encoding="utf-8"))
    historical_dev, historical_tables = _dataset_paths(args.historical_repo.resolve())
    official_dev, official_tables = _dataset_paths(args.official_data.resolve())
    compatibility = _validate_dataset_compatibility(
        source, historical_dev, historical_tables, official_dev, official_tables,
    )
    suite_repo = args.test_suite_repo.resolve()
    upstream_commit = _git_head(suite_repo)
    if upstream_commit != EXPECTED_UPSTREAM_COMMIT:
        raise RuntimeError(f"test-suite evaluator commit {upstream_commit} != {EXPECTED_UPSTREAM_COMMIT}")
    archive = args.archive.resolve()
    archive_sha256 = sha256_file(archive)
    if archive_sha256 != EXPECTED_ARCHIVE_SHA256:
        raise RuntimeError(f"test-suite archive SHA-256 {archive_sha256} != {EXPECTED_ARCHIVE_SHA256}")
    db_ids = {str(item["db_id"]) for item in source.get("results") or []}
    suites, suite_manifest = _suite_manifest(suite_repo / "database", db_ids)
    single_results = _load_single_result(args.single_database_result.resolve(), source_sha256)

    results: list[dict] = []
    started = time.perf_counter()
    source_results = source.get("results") or []
    for position, item in enumerate(source_results, 1):
        case_id = str(item.get("id"))
        single_case = single_results.get(case_id, {})
        result = {
            "id": case_id,
            "index": item.get("index"),
            "db_id": item.get("db_id"),
            "hardness": item.get("hardness"),
            "question": item.get("question"),
            "gold_sql": item.get("gold_sql"),
            "predicted_sql": item.get("predicted_sql"),
            "exact_match": bool(item.get("exact_match")),
            "single_database_agreement": bool(single_case.get("execution_agreement")),
        }
        if item.get("error_category") == INFRASTRUCTURE_ERROR_CATEGORY:
            result.update({"score_status": "excluded_infrastructure"})
        elif item.get("answer_kind") != "query" or not str(item.get("predicted_sql") or "").strip():
            failed = {
                "agreement": False,
                "score_status": "scored",
                "error_category": item.get("error_category") or "no_query_prediction",
                "databases_total": len(suites[str(item["db_id"])]),
                "databases_checked": 0,
            }
            result.update({
                "score_status": "scored",
                "strict_product": failed,
                "upstream_compatible": dict(failed),
                "distinct_removed_for_upstream": False,
            })
        else:
            paths = suites[str(item["db_id"])]
            predicted_sql = str(item["predicted_sql"])
            gold_sql = str(item["gold_sql"])
            strict = score_test_suite(
                paths, predicted_sql, gold_sql, keep_distinct=True,
                timeout_s=args.timeout, max_rows=args.max_rows,
            )
            distinct_removed = (
                normalize_test_suite_sql(predicted_sql, keep_distinct=True)
                != normalize_test_suite_sql(predicted_sql, keep_distinct=False)
                or normalize_test_suite_sql(gold_sql, keep_distinct=True)
                != normalize_test_suite_sql(gold_sql, keep_distinct=False)
            )
            upstream = score_test_suite(
                paths, predicted_sql, gold_sql, keep_distinct=False,
                timeout_s=args.timeout, max_rows=args.max_rows,
            ) if distinct_removed else dict(strict)
            statuses = {strict["score_status"], upstream["score_status"]}
            result.update({
                "score_status": "benchmark_error" if "benchmark_error" in statuses else "scored",
                "strict_product": strict,
                "upstream_compatible": upstream,
                "distinct_removed_for_upstream": distinct_removed,
            })
        architecture_replay = single_case.get("architecture_replay") or {}
        repair_sql = str(architecture_replay.get("local_repair_sql") or "").strip()
        repair_status = str(architecture_replay.get("local_repair_status") or "")
        if repair_sql and repair_status:
            paths = suites[str(item["db_id"])]
            gold_sql = str(item["gold_sql"])
            repair_strict = score_test_suite(
                paths, repair_sql, gold_sql, keep_distinct=True,
                timeout_s=args.timeout, max_rows=args.max_rows,
            )
            repair_distinct_removed = (
                normalize_test_suite_sql(repair_sql, keep_distinct=True)
                != normalize_test_suite_sql(repair_sql, keep_distinct=False)
                or normalize_test_suite_sql(gold_sql, keep_distinct=True)
                != normalize_test_suite_sql(gold_sql, keep_distinct=False)
            )
            repair_upstream = score_test_suite(
                paths, repair_sql, gold_sql, keep_distinct=False,
                timeout_s=args.timeout, max_rows=args.max_rows,
            ) if repair_distinct_removed else dict(repair_strict)
            result["local_repair"] = {
                "status": repair_status,
                "sql": repair_sql,
                "strict_product": repair_strict,
                "upstream_compatible": repair_upstream,
                "distinct_removed_for_upstream": repair_distinct_removed,
            }
            if "benchmark_error" in {
                repair_strict["score_status"], repair_upstream["score_status"],
            }:
                result["score_status"] = "benchmark_error"
        results.append(result)
        strict_pass = bool((result.get("strict_product") or {}).get("agreement"))
        upstream_pass = bool((result.get("upstream_compatible") or {}).get("agreement"))
        print(
            f"[{position}/{len(source_results)}] {case_id} "
            f"strict={'PASS' if strict_pass else 'FAIL'} "
            f"upstream={'PASS' if upstream_pass else 'FAIL'}",
            flush=True,
        )

    summary = _summary(results)
    if summary["coverage"]["benchmark_errors"]:
        raise RuntimeError(
            f"test-suite rescore has {summary['coverage']['benchmark_errors']} benchmark errors; "
            "refusing to publish a partial metric"
        )
    repair_regressions = summary["local_repair_test_suite"]["strict_regressions"]
    if repair_regressions:
        raise RuntimeError(
            "local repair strict test-suite regressions: "
            + ", ".join(repair_regressions)
        )
    payload = {
        "schema_version": 1,
        "status": "completed",
        "completed_at_unix": time.time(),
        "run_wall_ms": round((time.perf_counter() - started) * 1000, 3),
        "source": {
            "result_sha256": source_sha256,
            "single_database_result_sha256": sha256_file(args.single_database_result.resolve()),
            "prompt_contract": source.get("prompt_contract"),
            "scoring_contract": source.get("scoring_contract"),
            "sample_fingerprint": (source.get("sample") or {}).get("fingerprint"),
            "sample_size": (source.get("sample") or {}).get("size"),
            "model_identity": source.get("model_identity"),
        },
        "dataset": {
            "name": "Spider 1.0 official distilled test-suite databases",
            "official_repository": REFERENCE_IMPLEMENTATION["repository"],
            "official_commit": upstream_commit,
            "archive_sha256": archive_sha256,
            "official_dev_sha256": compatibility.pop("official_dev_sha256"),
            "official_tables_sha256": compatibility.pop("official_tables_sha256"),
            "compatibility": compatibility,
            "databases": suite_manifest,
        },
        "metric": {
            "comparator_contract": TEST_SUITE_COMPARATOR_CONTRACT,
            "predicted_values_evaluated": True,
            "gold_values_plugged": False,
            "upstream_compatible_distinct_preserved": False,
            "strict_product_distinct_preserved": True,
            "all_schema_databases_required": True,
            "bounded_read_only_adaptation": True,
        },
        "scoring_contract": SCORING_CONTRACT,
        "execution_limits": {"timeout_s": args.timeout, "max_rows": args.max_rows},
        "results": results,
        "summary": summary,
    }
    _atomic_json(args.output.resolve(), payload)
    _atomic_text(args.markdown.resolve(), _markdown(payload))
    strict = summary["strict_product_test_suite_accuracy"]
    upstream = summary["upstream_compatible_test_suite_accuracy"]
    print(f"Strict product TSA: {strict['passed']}/{strict['total']} ({(strict['rate'] or 0) * 100:.1f}%)")
    print(f"Upstream-compatible TSA: {upstream['passed']}/{upstream['total']} ({(upstream['rate'] or 0) * 100:.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
