#!/usr/bin/env python3
"""Compare repeated DBQuill Spider runs without hiding per-case instability."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import statistics
from typing import Any


COMPARABILITY_PATHS = (
    ("dataset", "dev_sha256"),
    ("dataset", "tables_sha256"),
    ("sample", "fingerprint"),
    ("sample", "size"),
    ("sample", "seed"),
    ("llm_cfg",),
    ("model_identity",),
    ("prompt_contract",),
    ("scoring_contract",),
)
TARGET_IR_KEYS = (
    "relation_paths",
    "aggregation_stages",
    "ratio_requirements",
    "correlation_requirements",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare 2+ completed Spider benchmark JSON reports",
    )
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    return parser.parse_args()


def _read(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("status") != "completed":
        raise ValueError(f"run is not completed: {path}")
    if not isinstance(payload.get("results"), list) or not payload["results"]:
        raise ValueError(f"run has no results: {path}")
    return payload


def _at(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = payload
    for key in path:
        value = value[key]
    return value


def _rate(passed: int, total: int) -> float | None:
    return round(passed / total, 4) if total else None


def _sample_stddev(values: list[float]) -> float:
    return round(statistics.stdev(values), 4) if len(values) >= 2 else 0.0


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


def _validate_comparable(payloads: list[dict[str, Any]], paths: list[Path]) -> dict[str, Any]:
    baseline = payloads[0]
    contract: dict[str, Any] = {}
    for path in COMPARABILITY_PATHS:
        expected = _at(baseline, path)
        contract[".".join(path)] = expected
        for index, payload in enumerate(payloads[1:], start=1):
            observed = _at(payload, path)
            if observed != expected:
                raise ValueError(
                    f"incomparable runs: {paths[0]} and {paths[index]} differ at "
                    f"{'.'.join(path)} ({expected!r} != {observed!r})"
                )
    return contract


def _run_summary(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary"]
    return {
        "file": str(path),
        "exact_passed": summary["exact_match"]["passed"],
        "exact_rate": summary["exact_match"]["rate"],
        "valid_sql_passed": summary["valid_sql"]["passed"],
        "parseable_passed": summary["official_ast_parseable"]["passed"],
        "exact_among_parseable": summary["exact_among_parseable"]["rate"],
        "by_hardness": summary["by_hardness"],
        "errors": summary["error_categories"],
        "target_ir": summary.get("target_relational_ir", {}),
        "candidate_search": summary.get("bounded_candidate_search", {}),
        "latency_ms": summary["latency_ms"],
        "wall_ms": payload.get("run_wall_ms"),
    }


def analyze(paths: list[Path]) -> dict[str, Any]:
    if len(paths) < 2:
        raise ValueError("at least two runs are required")
    payloads = [_read(path) for path in paths]
    comparability = _validate_comparable(payloads, paths)
    maps = [
        {str(item["id"]): item for item in payload["results"]}
        for payload in payloads
    ]
    ids = set(maps[0])
    for index, mapping in enumerate(maps[1:], start=1):
        if set(mapping) != ids:
            raise ValueError(f"run {index + 1} has a different case-id set")
    ordered_ids = [
        item["id"]
        for item in sorted(payloads[0]["results"], key=lambda row: int(row["index"]))
    ]

    exact_counts = [payload["summary"]["exact_match"]["passed"] for payload in payloads]
    exact_rates = [float(value) / len(ordered_ids) for value in exact_counts]
    stable_pass: list[str] = []
    stable_fail: list[str] = []
    unstable: list[dict[str, Any]] = []
    case_rows: list[dict[str, Any]] = []
    stable_failure_categories: Counter[str] = Counter()
    stable_failure_components: Counter[str] = Counter()
    hardness_stability: dict[str, Counter[str]] = defaultdict(Counter)
    ir_union = Counter()
    ir_case_union: set[str] = set()

    for case_id in ordered_ids:
        rows = [mapping[case_id] for mapping in maps]
        outcomes = [bool(row.get("exact_match")) for row in rows]
        pass_count = sum(outcomes)
        hardness = str(rows[0].get("hardness") or "unknown")
        state = "stable_pass" if pass_count == len(rows) else "stable_fail" if pass_count == 0 else "unstable"
        hardness_stability[hardness][state] += 1
        categories = [str(row.get("error_category") or "pass") for row in rows]
        components = [list(row.get("component_gaps") or []) for row in rows]
        ir_keys = sorted({
            key
            for row in rows
            for key in TARGET_IR_KEYS
            if (row.get("relational_contract") or {}).get(key)
        })
        if ir_keys:
            ir_case_union.add(case_id)
            for key in ir_keys:
                ir_union[key] += 1
        record = {
            "id": case_id,
            "index": rows[0]["index"],
            "db_id": rows[0]["db_id"],
            "hardness": hardness,
            "question": rows[0]["question"],
            "outcomes": outcomes,
            "pass_count": pass_count,
            "state": state,
            "error_categories": categories,
            "target_ir": ir_keys,
            "latency_ms": [row.get("latency_ms") for row in rows],
        }
        case_rows.append(record)
        if state == "stable_pass":
            stable_pass.append(case_id)
        elif state == "stable_fail":
            stable_fail.append(case_id)
            stable_failure_categories.update(
                category for category in categories if category != "pass"
            )
            stable_failure_components.update(
                component
                for run_components in components
                for component in run_components
            )
        else:
            unstable.append(record)

    all_latencies = [
        float(row.get("latency_ms") or 0.0)
        for payload in payloads
        for row in payload["results"]
    ]
    total_cases = len(ordered_ids)
    run_count = len(payloads)
    majority_threshold = run_count // 2 + 1
    majority_oracle = sum(
        int(row["pass_count"]) >= majority_threshold for row in case_rows
    )
    any_pass_oracle = sum(int(row["pass_count"]) > 0 for row in case_rows)

    return {
        "schema_version": 1,
        "run_count": run_count,
        "case_count": total_cases,
        "comparability": comparability,
        "runs": [_run_summary(path, payload) for path, payload in zip(paths, payloads)],
        "exact": {
            "counts": exact_counts,
            "rates": [round(value, 4) for value in exact_rates],
            "mean_passed": round(statistics.mean(exact_counts), 3),
            "mean_rate": round(statistics.mean(exact_rates), 4),
            "sample_stddev_points": _sample_stddev([value * 100 for value in exact_rates]),
            "min_passed": min(exact_counts),
            "max_passed": max(exact_counts),
        },
        "stability": {
            "stable_pass": len(stable_pass),
            "stable_fail": len(stable_fail),
            "unstable": len(unstable),
            "stable_decision_rate": _rate(len(stable_pass) + len(stable_fail), total_cases),
            "majority_oracle_passed": majority_oracle,
            "any_run_oracle_passed": any_pass_oracle,
            "stable_pass_ids": stable_pass,
            "stable_fail_ids": stable_fail,
            "unstable_cases": unstable,
            "by_hardness": {
                hardness: dict(counts)
                for hardness, counts in sorted(hardness_stability.items())
            },
        },
        "stable_failure_evidence": {
            "error_observations": dict(stable_failure_categories.most_common()),
            "component_observations": dict(stable_failure_components.most_common()),
        },
        "target_ir_union": {
            **{key: ir_union[key] for key in TARGET_IR_KEYS},
            "any": len(ir_case_union),
            "case_ids": sorted(ir_case_union),
        },
        "latency_ms_all_calls": {
            "median": round(statistics.median(all_latencies), 3),
            "p95": round(_percentile(all_latencies, 0.95), 3),
            "max": round(max(all_latencies), 3),
            "sum": round(sum(all_latencies), 3),
        },
        "cases": case_rows,
    }


def _markdown(report: dict[str, Any]) -> str:
    exact = report["exact"]
    stability = report["stability"]
    lines = [
        "# DBQuill Spider repeated-run stability report",
        "",
        f"- Runs: {report['run_count']}; fixed cases per run: {report['case_count']}",
        f"- Exact counts: {', '.join(str(item) for item in exact['counts'])}",
        f"- Mean exact: {exact['mean_rate'] * 100:.1f}% "
        f"(sample SD {exact['sample_stddev_points']:.2f} percentage points)",
        f"- Stable pass / stable fail / unstable: "
        f"{stability['stable_pass']} / {stability['stable_fail']} / {stability['unstable']}",
        f"- Stable per-case decision rate: {(stability['stable_decision_rate'] or 0) * 100:.1f}%",
        f"- Oracle only (not deployable): majority {stability['majority_oracle_passed']}/"
        f"{report['case_count']}; any-run {stability['any_run_oracle_passed']}/"
        f"{report['case_count']}",
        "",
        "> Oracle figures use gold outcomes after the fact. They measure available headroom from "
        "sampling, not a production selection policy.",
        "",
        "## Per-run metrics",
        "",
        "| Run | Exact | Valid SQL | Legacy AST | Exact / parseable | Median ms | P95 ms | Max ms | Target IR any |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for index, run in enumerate(report["runs"], start=1):
        lines.append(
            f"| {index} | {run['exact_passed']}/{report['case_count']} | "
            f"{run['valid_sql_passed']} | {run['parseable_passed']} | "
            f"{(run['exact_among_parseable'] or 0) * 100:.1f}% | "
            f"{run['latency_ms']['median']:.0f} | {run['latency_ms']['p95']:.0f} | "
            f"{run['latency_ms']['max']:.0f} | {run['target_ir'].get('any', 0)} |"
        )
    lines.extend([
        "",
        "## Stable failure observations",
        "",
        "Counts below are observations across all runs for cases that failed every run.",
        "",
        "| Error category | Observations |",
        "|---|---:|",
    ])
    for category, count in report["stable_failure_evidence"]["error_observations"].items():
        lines.append(f"| `{category}` | {count} |")
    lines.extend([
        "",
        "## Unstable cases",
        "",
        "| Case | Hardness | Outcomes | Error categories |",
        "|---|---|---|---|",
    ])
    for row in stability["unstable_cases"]:
        outcomes = "".join("1" if value else "0" for value in row["outcomes"])
        categories = " / ".join(row["error_categories"])
        lines.append(f"| `{row['id']}` | {row['hardness']} | `{outcomes}` | {categories} |")
    ir = report["target_ir_union"]
    latency = report["latency_ms_all_calls"]
    lines.extend([
        "",
        "## Architecture signals",
        "",
        f"- Target Relational IR reached {ir['any']}/{report['case_count']} unique cases across all runs.",
        f"- All-call latency median/P95/max: {latency['median']:.0f} / "
        f"{latency['p95']:.0f} / {latency['max']:.0f} ms.",
        "- Legacy Spider AST failures are kept separate from executable-query failures; "
        "this report does not reinterpret unsupported valid SQL as exact matches.",
        "",
        f"Prompt contract: `{report['comparability']['prompt_contract']}`  ",
        f"Scoring contract: `{report['comparability']['scoring_contract']}`  ",
        f"Sample fingerprint: `{report['comparability']['sample.fingerprint']}`",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    paths = [path.resolve() for path in args.runs]
    report = analyze(paths)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    args.markdown.write_text(_markdown(report), encoding="utf-8")
    print(
        "Spider stability: exact="
        + "/".join(str(item) for item in report["exact"]["counts"])
        + f", stable={report['stability']['stable_pass']} pass / "
        + f"{report['stability']['stable_fail']} fail, "
        + f"unstable={report['stability']['unstable']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
