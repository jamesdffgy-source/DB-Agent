#!/usr/bin/env python3
"""Replay the current local semantic architecture over a stored BIRD run.

This is deliberately model-free. It validates the stored run against the
fixed Mini-Dev package, rebuilds production schema metadata, applies only the
same local compilers available in the product, and reports gate changes
without presenting them as a regenerated benchmark score.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from collections import Counter
from pathlib import Path

import run_bird_benchmark as bird


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "benchmark_results" / "bird_relgraph_full.json"
DEFAULT_PACKAGE = ROOT / "benchmark_data" / "bird_minidev_package" / "minidev" / "MINIDEV"
DEFAULT_REPO = ROOT / "benchmark_data" / "bird_mini_dev_official_repo"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "bird_relational_ir_18_architecture_replay.json"
DEFAULT_MARKDOWN = ROOT / "benchmark_results" / "bird_relational_ir_18_architecture_replay.md"
REPLAY_CONTRACT = "dbquill-bird-local-semantic-architecture-replay-v2"


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


def _validate_source(source: dict, source_path: Path, package: Path, repo: Path) -> dict:
    if source.get("status") != "completed":
        raise RuntimeError("stored BIRD run is not complete")
    dataset = source.get("dataset") or {}
    expected_commit = str(dataset.get("repository_commit") or "")
    actual_commit = _git_head(repo)
    if not expected_commit or actual_commit != expected_commit:
        raise RuntimeError(
            f"BIRD repository commit {actual_commit!r} != {expected_commit!r}"
        )
    files = {
        "json_sha256": package / "mini_dev_sqlite.json",
        "tables_sha256": package / "dev_tables.json",
        "gold_sha256": package / "mini_dev_sqlite_gold.sql",
    }
    verified_files = {}
    for key, path in files.items():
        actual = bird._sha256_file(path)
        expected = str(dataset.get(key) or "")
        if actual != expected:
            raise RuntimeError(f"BIRD {key} {actual!r} != {expected!r}")
        verified_files[key] = actual
    results = source.get("results") or []
    expected_size = int((source.get("sample") or {}).get("size") or 0)
    if not results or len(results) != expected_size:
        raise RuntimeError(
            f"stored BIRD result size {len(results)} != sample size {expected_size}"
        )
    db_ids = sorted({str(item.get("db_id") or "") for item in results})
    db_paths = {db_id: bird._database_path(package, db_id) for db_id in db_ids}
    expected_databases = dataset.get("database_sha256") or {}
    for db_id, path in db_paths.items():
        if bird._sha256_file(path) != str(expected_databases.get(db_id) or ""):
            raise RuntimeError(f"BIRD database hash mismatch: {db_id}")
    expected_descriptions = dataset.get("description_sha256") or {}
    for db_id, path in db_paths.items():
        if bird._description_fingerprint(path) != expected_descriptions.get(db_id):
            raise RuntimeError(f"BIRD description hash mismatch: {db_id}")
    return {
        "source_path": str(source_path),
        "source_sha256": bird._sha256_file(source_path),
        "repository_commit": actual_commit,
        "verified_files": verified_files,
        "database_count": len(db_paths),
        "case_count": len(results),
        "db_paths": db_paths,
    }


def _features(contract, initial_conflict) -> list[str]:
    return [
        name for name, value in (
            ("quoted_literal_filter", (
                "unique_sample_grounded_quoted_literal" in contract.evidence
            )),
            ("qualified_anti_relationship", (
                "explicit_negative_relationship" in contract.evidence
                and "unique_sample_grounded_quoted_literal" in contract.evidence
            )),
            ("deterministic_single_row_tie_breaker", contract.tie_breaker_columns),
            ("all_values_visible_tuple_set", (
                "all_values_projection_preserves_set_semantics" in contract.evidence
            )),
            ("exact_value_operator_gate", (
                initial_conflict is not None
                and initial_conflict.code == "wildcard_literal_broadening"
            )),
        ) if value
    ]


def _replay_item(item: dict, path: Path, schema) -> dict:
    predicted = str(item.get("predicted_sql") or "").strip()
    rejected = str(item.get("rejected_candidate_sql") or "").strip()
    sql = predicted or rejected
    if not sql:
        return {"status": "not_applicable", "reason": "stored run has no candidate"}
    question = bird._model_question(item, include_evidence=True)
    connector = bird.dc.DBConnector(str(path))
    executor = bird.dc.NL2SQLExecutor(bird.dc.SQLSecurity(connector), schema)
    contract = executor._compile_relational_contract(question)
    executor.last_relational_contract = contract
    initial_conflict = executor._semantic_conflict(question, sql)
    conflict = initial_conflict
    repair = None
    repair_score = None
    native_plan = executor._compile_native_relational_plan(
        question, contract, allowed_tables=None,
    )
    native_plan_dict = None
    if native_plan is not None:
        native_plan_dict = native_plan.as_dict()
        native_sql = bird.dc.SQLiteRelationalPlanRenderer(schema).render(native_plan)
        native_conflict = executor._semantic_conflict(question, native_sql)
        if native_conflict is None:
            sql = native_sql
            conflict = None
            repair = {
                "selected": {
                    "candidate_id": "native_relational_plan_compiler",
                    "sql": sql,
                },
                "diagnostic": {
                    "status": "native_relational_plan_compiled",
                    "selection_basis": "typed_contract_before_model_generation",
                },
            }
            repair_score = bird._score_prediction(
                path, sql, str(item.get("gold_sql") or ""), 30.0, 200_000,
            )
        else:
            native_plan_dict = {
                **native_plan_dict,
                "render_conflict": native_conflict.as_dict(),
            }
    if conflict is not None:
        repair = executor._try_local_contract_repair(
            question=question,
            bad_sql=sql,
            conflict=conflict,
            allowed_tables=None,
        )
        if repair is not None and repair.get("selected") is not None:
            sql = str(repair["selected"].get("sql") or "").strip()
            conflict = executor._semantic_conflict(question, sql)
            repair_score = bird._score_prediction(
                path, sql, str(item.get("gold_sql") or ""), 30.0, 200_000,
            )
    status = (
        "needs_clarification" if contract.ambiguities
        else "rejected" if conflict is not None
        else "accepted"
    )
    return {
        "status": status,
        "candidate_source": (
            str(repair["selected"].get("candidate_id"))
            if repair is not None and repair.get("selected") is not None
            else "predicted_sql" if predicted else "rejected_candidate_sql"
        ),
        "contract_version": contract.version,
        "relational_contract": contract.as_dict(),
        "native_relational_plan": native_plan_dict,
        "features": _features(contract, initial_conflict),
        "original_conflict_code": (
            initial_conflict.code if initial_conflict is not None else None
        ),
        "original_conflict_constraints": (
            initial_conflict.constraints if initial_conflict is not None else None
        ),
        "conflict_code": conflict.code if conflict is not None else None,
        "conflict_message": conflict.message if conflict is not None else None,
        "local_repair_status": (
            (repair.get("diagnostic") or {}).get("status")
            if repair is not None else None
        ),
        "local_repair_sql": sql if repair is not None else None,
        "local_repair_execution": repair_score,
    }


def _summary(results: list[dict]) -> dict:
    applicable = [
        item for item in results
        if (item.get("architecture_replay") or {}).get("status") != "not_applicable"
    ]
    statuses = Counter(
        str((item.get("architecture_replay") or {}).get("status") or "missing")
        for item in applicable
    )
    matrix = Counter()
    for item in applicable:
        prior = "execution_pass" if item.get("prior_execution_exact") else "execution_fail"
        status = str((item.get("architecture_replay") or {}).get("status") or "missing")
        matrix[f"{prior}__{status}"] += 1
    repairs = Counter(
        str((item.get("architecture_replay") or {}).get("local_repair_status"))
        for item in applicable
        if (item.get("architecture_replay") or {}).get("local_repair_status")
    )
    features = Counter(
        feature
        for item in applicable
        for feature in (item.get("architecture_replay") or {}).get("features") or []
    )
    old_correct_not_accepted = [
        item["id"] for item in applicable
        if item.get("prior_execution_exact")
        and (item.get("architecture_replay") or {}).get("status") != "accepted"
    ]
    repair_execution_regressions = [
        item["id"] for item in applicable
        if (item.get("architecture_replay") or {}).get("local_repair_execution")
        and item.get("prior_execution_exact")
        and not bool(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("execution_exact")
        )
    ]
    repair_execution_improvements = [
        item["id"] for item in applicable
        if (item.get("architecture_replay") or {}).get("local_repair_execution")
        and not item.get("prior_execution_exact")
        and bool(
            ((item.get("architecture_replay") or {}).get("local_repair_execution") or {})
            .get("execution_exact")
        )
    ]
    return {
        "applicable": len(applicable),
        "statuses": dict(statuses),
        "execution_gate_matrix": dict(matrix),
        "feature_cases": dict(features),
        "local_repair_cases": dict(repairs),
        "old_correct_not_accepted": old_correct_not_accepted,
        "local_repair_execution_improvements": repair_execution_improvements,
        "local_repair_execution_regressions": repair_execution_regressions,
        "interpretation": (
            "Model-free replay over stored SQL. Gate acceptance and local repair "
            "are architecture evidence, not a regenerated BIRD accuracy score."
        ),
    }


def _markdown(payload: dict) -> str:
    summary = payload["summary"]
    return "\n".join([
        "# DBQuill BIRD 当前架构反事实回放",
        "",
        f"- 适用候选：{summary['applicable']}",
        f"- 状态：`{json.dumps(summary['statuses'], ensure_ascii=False)}`",
        f"- 本地修复：`{json.dumps(summary['local_repair_cases'], ensure_ascii=False)}`",
        f"- 历史正确但当前未接受：{len(summary['old_correct_not_accepted'])}",
        f"- 本地修复执行改进：{len(summary['local_repair_execution_improvements'])}",
        f"- 本地修复执行回归：{len(summary['local_repair_execution_regressions'])}",
        "",
        "> 这是固定旧 SQL 的零模型反事实回放，不是新的模型生成分数。",
        "",
        f"回放契约：`{payload['replay_contract']}`",
    ])


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay current architecture over stored BIRD SQL")
    parser.add_argument("--source-result", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--package-root", type=Path, default=DEFAULT_PACKAGE)
    parser.add_argument("--official-repo", type=Path, default=DEFAULT_REPO)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_path = args.source_result.resolve()
    package = args.package_root.resolve()
    source = json.loads(source_path.read_text(encoding="utf-8"))
    validation = _validate_source(
        source, source_path, package, args.official_repo.resolve(),
    )
    db_paths = validation.pop("db_paths")
    schema_records = {
        str(item.get("db_id")): item
        for item in json.loads((package / "dev_tables.json").read_text(encoding="utf-8"))
    }
    sample_rows = int(source.get("schema_sample_rows") or 0)
    schemas = {}
    for db_id, path in db_paths.items():
        schema = bird.dc.SchemaDiscovery(
            bird.dc.DBConnector(str(path)), sample_rows=sample_rows,
        ).discover()
        bird._apply_declared_foreign_keys(schema_records.get(db_id) or {}, schema)
        bird._apply_column_descriptions(path, schema)
        schemas[db_id] = schema
    started = time.perf_counter()
    results = []
    for position, item in enumerate(source.get("results") or [], 1):
        replay = _replay_item(
            item, db_paths[str(item["db_id"])], schemas[str(item["db_id"])],
        )
        results.append({
            "id": item.get("id"),
            "db_id": item.get("db_id"),
            "difficulty": item.get("difficulty"),
            "question": item.get("question"),
            "prior_execution_exact": bool(item.get("execution_exact")),
            "architecture_replay": replay,
        })
        print(
            f"[{position}/{len(source['results'])}] {item.get('id')} "
            f"{replay.get('status')}",
            flush=True,
        )
    payload = {
        "schema_version": 1,
        "status": "completed",
        "completed_at_unix": time.time(),
        "run_wall_ms": round((time.perf_counter() - started) * 1000, 3),
        "replay_contract": REPLAY_CONTRACT,
        "source": validation,
        "results": results,
        "summary": _summary(results),
    }
    _atomic_json(args.output.resolve(), payload)
    _atomic_text(args.markdown.resolve(), _markdown(payload))
    summary = payload["summary"]
    if summary["old_correct_not_accepted"] or summary["local_repair_execution_regressions"]:
        raise RuntimeError("BIRD replay found a current architecture regression")
    print(
        "BIRD architecture replay: "
        f"accepted={summary['statuses'].get('accepted', 0)}, "
        f"rejected={summary['statuses'].get('rejected', 0)}, "
        "old-correct-regressions=0",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
