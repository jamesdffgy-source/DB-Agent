#!/usr/bin/env python3
"""Cross-check DB-Agent's upstream-compatible TSA against the public evaluator."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from spider_execution_scoring import sha256_file


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULT = ROOT / "benchmark_results" / "spider_relational_ir_15_test_suite_rescore.json"
DEFAULT_REPO = ROOT / "benchmark_data" / "test_suite_sql_eval"
DEFAULT_VENDOR = DEFAULT_REPO / "vendor"
DEFAULT_OUTPUT = ROOT / "benchmark_results" / "spider_relational_ir_15_test_suite_upstream_verification.json"
EXPECTED_COMMIT = "e97acc546ecbee8fa27fa8dbf025ef61493a876c"
VERIFICATION_CONTRACT = "spider-public-eval-exec-match-crosscheck-v1"


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-check TSA with the fixed public evaluator")
    parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--test-suite-repo", type=Path, default=DEFAULT_REPO)
    parser.add_argument("--vendor", type=Path, default=DEFAULT_VENDOR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result_path = args.result.resolve()
    repo = args.test_suite_repo.resolve()
    vendor = args.vendor.resolve()
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    if head != EXPECTED_COMMIT:
        raise RuntimeError(f"upstream commit {head} != {EXPECTED_COMMIT}")
    if not (vendor / "sqlparse").is_dir():
        raise FileNotFoundError(
            f"sqlparse is missing under {vendor}; install it into this ignored D-drive directory"
        )
    sys.path.insert(0, str(vendor))
    sys.path.insert(0, str(repo))
    exec_eval = importlib.import_module("exec_eval")
    sqlparse = importlib.import_module("sqlparse")

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    outcomes: list[dict] = []
    started = time.perf_counter()
    cases = payload.get("results") or []
    for position, item in enumerate(cases, 1):
        predicted = str(item.get("predicted_sql") or "").strip()
        gold = str(item.get("gold_sql") or "").strip()
        expected = bool((item.get("upstream_compatible") or {}).get("agreement"))
        if not predicted:
            actual = False
        else:
            if not re.match(r"^(?:SELECT|WITH)\b", predicted, re.IGNORECASE):
                raise RuntimeError(f"refusing to pass non-query prediction to upstream: {item.get('id')}")
            db_id = str(item["db_id"])
            primary = repo / "database" / db_id / f"{db_id}.sqlite"
            actual = bool(exec_eval.eval_exec_match(
                db=str(primary),
                p_str=predicted,
                g_str=gold,
                plug_value=False,
                keep_distinct=False,
                progress_bar_for_each_datapoint=False,
            ))
        outcomes.append({
            "id": item.get("id"),
            "expected": expected,
            "upstream": actual,
            "match": expected == actual,
        })
        print(
            f"[{position}/{len(cases)}] {item.get('id')} "
            f"upstream={'PASS' if actual else 'FAIL'} "
            f"crosscheck={'OK' if expected == actual else 'MISMATCH'}",
            flush=True,
        )
    mismatches = [item for item in outcomes if not item["match"]]
    verification = {
        "schema_version": 1,
        "status": "completed" if not mismatches else "mismatch",
        "contract": VERIFICATION_CONTRACT,
        "result_sha256": sha256_file(result_path),
        "upstream_repository": "https://github.com/taoyds/test-suite-sql-eval",
        "upstream_commit": head,
        "sqlparse_version": getattr(sqlparse, "__version__", "unknown"),
        "plug_value": False,
        "keep_distinct": False,
        "total": len(outcomes),
        "matched": len(outcomes) - len(mismatches),
        "mismatches": mismatches,
        "run_wall_ms": round((time.perf_counter() - started) * 1000, 3),
        "outcomes": outcomes,
    }
    _atomic_json(args.output.resolve(), verification)
    if mismatches:
        raise RuntimeError(f"{len(mismatches)} upstream cross-check mismatches")
    print(f"Upstream cross-check: {len(outcomes)}/{len(outcomes)} matched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
