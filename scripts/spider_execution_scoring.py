"""Read-only Spider execution scoring primitives.

The comparison contract follows the public Spider test-suite evaluator's
denotation semantics: duplicate rows matter, result-column order may differ,
and row order matters when the gold query requests ORDER BY.  This module is
an independent bounded implementation for DBQuill's released single-database
evidence and official multi-database test-suite evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

import dbquill_core as dc  # noqa: E402


COMPARATOR_CONTRACT = "dbquill-spider-single-db-denotation-v1"
TEST_SUITE_COMPARATOR_CONTRACT = "dbquill-spider-test-suite-denotation-v1"
REFERENCE_IMPLEMENTATION = {
    "repository": "https://github.com/taoyds/test-suite-sql-eval",
    "commit": "e97acc546ecbee8fa27fa8dbf025ef61493a876c",
    "behavior": "bag rows, result-column permutation, gold ORDER BY preserves row order",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"blob_bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def preview_rows(rows: list[tuple], limit: int = 3) -> list[list[Any]]:
    return [[_safe_value(value) for value in row] for row in rows[:limit]]


def result_hash(rows: list[tuple]) -> str:
    encoded_rows = [
        json.dumps(
            [_safe_value(value) for value in row],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for row in rows
    ]
    encoded_rows.sort()
    return hashlib.sha256("\n".join(encoded_rows).encode("utf-8")).hexdigest()


def gold_order_matters(sql: str) -> bool:
    """Match Spider test-suite behavior without treating literals as clauses."""
    code = dc._sql_code_only(str(sql or ""), mask_identifiers=False)
    return bool(re.search(r"\bORDER\s+BY\b", code, re.IGNORECASE))


def upstream_postprocess(sql: str) -> str:
    """Apply the public evaluator's execution-only operator normalization."""
    return str(sql or "").replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")


def remove_distinct_keywords(sql: str) -> str:
    """Remove executable DISTINCT keywords without touching literals/comments.

    The public evaluator removes every token whose value is ``distinct`` when
    ``--keep_distinct`` is absent.  Keeping source length stable here makes the
    same semantic transformation without adding its ``sqlparse`` dependency.
    """
    source = str(sql or "")
    code = dc._sql_code_only(source)
    output = list(source)
    for match in re.finditer(r"\bDISTINCT\b", code, re.IGNORECASE):
        output[match.start():match.end()] = " " * (match.end() - match.start())
    return "".join(output)


def normalize_test_suite_sql(sql: str, *, keep_distinct: bool) -> str:
    normalized = upstream_postprocess(sql)
    return normalized if keep_distinct else remove_distinct_keywords(normalized)


def _canonical_cell(value: Any) -> tuple[str, Any]:
    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("number", int(value))
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return ("float", "nan")
        if isinstance(value, float) and math.isinf(value):
            return ("float", "inf" if value > 0 else "-inf")
        return ("number", value)
    if isinstance(value, bytes):
        return ("blob", value)
    return ("text", str(value))


def _canonical_row(row: Iterable[Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(_canonical_cell(value) for value in row)


def _column_signature(
    rows: list[tuple],
    column: int,
    order_matters: bool,
) -> tuple:
    values = [_canonical_cell(row[column]) for row in rows]
    if order_matters:
        return tuple(values)
    counts = Counter(values)
    return tuple(sorted(counts.items(), key=lambda item: repr(item[0])))


def result_equivalent(
    gold_rows: list[tuple],
    predicted_rows: list[tuple],
    *,
    order_matters: bool,
    permutation_budget: int = 100_000,
) -> tuple[bool, dict]:
    """Compare denotations with bag rows and a global column permutation.

    The search is deterministic and pruned by complete column signatures.  A
    budget breach is reported as an evaluation error instead of being counted
    as a model failure.
    """
    if len(gold_rows) != len(predicted_rows):
        return False, {"reason": "row_cardinality", "permutations_checked": 0}
    if not gold_rows:
        return True, {"reason": "both_empty", "permutations_checked": 0}

    gold_columns = len(gold_rows[0])
    predicted_columns = len(predicted_rows[0])
    if any(len(row) != gold_columns for row in gold_rows):
        raise ValueError("gold result contains inconsistent row widths")
    if any(len(row) != predicted_columns for row in predicted_rows):
        raise ValueError("predicted result contains inconsistent row widths")
    if gold_columns != predicted_columns:
        return False, {"reason": "projection_shape", "permutations_checked": 0}

    gold_signatures = [
        _column_signature(gold_rows, index, order_matters)
        for index in range(gold_columns)
    ]
    predicted_signatures = [
        _column_signature(predicted_rows, index, order_matters)
        for index in range(predicted_columns)
    ]
    candidates = [
        [
            predicted_index
            for predicted_index, signature in enumerate(predicted_signatures)
            if signature == gold_signatures[gold_index]
        ]
        for gold_index in range(gold_columns)
    ]
    if any(not choices for choices in candidates):
        return False, {"reason": "column_value_mismatch", "permutations_checked": 0}

    canonical_gold = [_canonical_row(row) for row in gold_rows]
    gold_relation = canonical_gold if order_matters else Counter(canonical_gold)
    assignment = [-1] * gold_columns
    used: set[int] = set()
    checked = 0
    exhausted = False

    def search(depth: int) -> bool:
        nonlocal checked, exhausted
        if depth == gold_columns:
            checked += 1
            if checked > permutation_budget:
                exhausted = True
                return False
            permuted = [
                _canonical_row(row[index] for index in assignment)
                for row in predicted_rows
            ]
            relation = permuted if order_matters else Counter(permuted)
            return relation == gold_relation
        unassigned = [index for index in range(gold_columns) if assignment[index] < 0]
        gold_index = min(
            unassigned,
            key=lambda index: (
                sum(candidate not in used for candidate in candidates[index]), index,
            ),
        )
        for predicted_index in candidates[gold_index]:
            if predicted_index in used:
                continue
            assignment[gold_index] = predicted_index
            used.add(predicted_index)
            if search(depth + 1):
                return True
            used.remove(predicted_index)
            assignment[gold_index] = -1
            if exhausted:
                return False
        return False

    equivalent = search(0)
    if exhausted:
        raise RuntimeError(
            f"column permutation search exceeded budget ({permutation_budget})"
        )
    return equivalent, {
        "reason": "equivalent" if equivalent else "value_or_row_association",
        "permutations_checked": checked,
    }


def execute_readonly(
    db_path: Path,
    sql: str,
    *,
    timeout_s: float = 10.0,
    max_rows: int = 200_000,
) -> dict:
    """Execute one production-safe SELECT on a physical read-only connection."""
    if timeout_s <= 0:
        raise ValueError("timeout_s must be positive")
    if max_rows < 1:
        raise ValueError("max_rows must be positive")
    started = time.perf_counter()
    connector = dc.DBConnector(str(db_path))
    validator = dc.SQLSecurity(
        connector,
        max_rows=max_rows + 1,
        timeout_s=timeout_s,
    )
    try:
        executable_sql = validator.validate(sql)
    except Exception as exc:
        return {
            "rows": [],
            "columns": [],
            "error": f"{type(exc).__name__}: {exc}",
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }

    connection = connector.connect()
    try:
        deadline = time.monotonic() + timeout_s

        def check_progress() -> int:
            return 1 if time.monotonic() > deadline else 0

        connection.set_progress_handler(check_progress, 1000)
        cursor = connection.cursor()
        cursor.execute(executable_sql)
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
        if "interrupted" in message.casefold():
            message = f"query timeout (>{timeout_s}s)"
        return {
            "rows": [],
            "columns": [],
            "error": f"{type(exc).__name__}: {message}",
            "latency_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    finally:
        connector.close(connection)


def score_execution(
    db_path: Path,
    predicted_sql: str,
    gold_sql: str,
    *,
    timeout_s: float = 10.0,
    max_rows: int = 200_000,
) -> dict:
    """Score one prediction on one released Spider database."""
    gold = execute_readonly(db_path, gold_sql, timeout_s=timeout_s, max_rows=max_rows)
    if gold["error"]:
        return {
            "agreement": False,
            "score_status": "benchmark_error",
            "error_category": "gold_execution_error",
            "evaluation_error": gold["error"],
            "gold_execution_ms": gold["latency_ms"],
        }
    predicted = execute_readonly(
        db_path, predicted_sql, timeout_s=timeout_s, max_rows=max_rows,
    )
    if predicted["error"]:
        return {
            "agreement": False,
            "score_status": "scored",
            "error_category": "prediction_execution_error",
            "evaluation_error": predicted["error"],
            "prediction_execution_ms": predicted["latency_ms"],
            "gold_execution_ms": gold["latency_ms"],
            "gold_row_count": len(gold["rows"]),
            "gold_column_count": len(gold["columns"]),
            "gold_empty": not gold["rows"],
            "gold_preview": preview_rows(gold["rows"]),
        }

    order_matters = gold_order_matters(gold_sql)
    try:
        equivalent, comparison = result_equivalent(
            gold["rows"], predicted["rows"], order_matters=order_matters,
        )
    except Exception as exc:
        return {
            "agreement": False,
            "score_status": "benchmark_error",
            "error_category": "comparator_error",
            "evaluation_error": f"{type(exc).__name__}: {exc}",
            "prediction_execution_ms": predicted["latency_ms"],
            "gold_execution_ms": gold["latency_ms"],
        }

    category = None
    if not equivalent:
        if len(predicted["columns"]) != len(gold["columns"]):
            category = "projection_shape"
        elif len(predicted["rows"]) != len(gold["rows"]):
            category = "row_cardinality"
        elif order_matters:
            unordered_equivalent, _ = result_equivalent(
                gold["rows"], predicted["rows"], order_matters=False,
            )
            category = "row_order_only" if unordered_equivalent else "value_join_or_filter"
        else:
            category = "value_join_or_filter"

    gold_empty = not gold["rows"]
    return {
        "agreement": equivalent,
        "score_status": "scored",
        "error_category": category,
        "order_matters": order_matters,
        "empty_result_match": equivalent and gold_empty,
        "gold_empty": gold_empty,
        "comparison_reason": comparison["reason"],
        "column_permutations_checked": comparison["permutations_checked"],
        "prediction_execution_ms": predicted["latency_ms"],
        "gold_execution_ms": gold["latency_ms"],
        "predicted_row_count": len(predicted["rows"]),
        "gold_row_count": len(gold["rows"]),
        "predicted_column_count": len(predicted["columns"]),
        "gold_column_count": len(gold["columns"]),
        "predicted_result_hash": result_hash(predicted["rows"]),
        "gold_result_hash": result_hash(gold["rows"]),
        "predicted_preview": preview_rows(predicted["rows"]),
        "gold_preview": preview_rows(gold["rows"]),
    }


def score_test_suite(
    db_paths: Iterable[Path],
    predicted_sql: str,
    gold_sql: str,
    *,
    keep_distinct: bool,
    timeout_s: float = 10.0,
    max_rows: int = 200_000,
) -> dict:
    """Require denotation agreement on every supplied perturbation database.

    Values in the predicted SQL are evaluated as-is and are never replaced by
    gold literals.  DBQuill's read-only validation, timeout and result cap are
    retained around every execution.
    """
    paths = [Path(path).resolve() for path in db_paths]
    if not paths:
        raise ValueError("test suite requires at least one SQLite database")
    predicted = normalize_test_suite_sql(predicted_sql, keep_distinct=keep_distinct)
    gold = normalize_test_suite_sql(gold_sql, keep_distinct=keep_distinct)
    checked = 0
    empty_matches = 0
    prediction_ms = 0.0
    gold_ms = 0.0
    for db_path in paths:
        score = score_execution(
            db_path,
            predicted,
            gold,
            timeout_s=timeout_s,
            max_rows=max_rows,
        )
        checked += 1
        prediction_ms += float(score.get("prediction_execution_ms") or 0)
        gold_ms += float(score.get("gold_execution_ms") or 0)
        empty_matches += int(bool(score.get("empty_result_match")))
        if score["score_status"] == "benchmark_error":
            return {
                "agreement": False,
                "score_status": "benchmark_error",
                "error_category": score.get("error_category"),
                "evaluation_error": score.get("evaluation_error"),
                "databases_total": len(paths),
                "databases_checked": checked,
                "failure_database": db_path.name,
                "failure_database_path": str(db_path),
                "empty_database_matches": empty_matches,
                "prediction_execution_ms": round(prediction_ms, 3),
                "gold_execution_ms": round(gold_ms, 3),
            }
        if not score["agreement"]:
            return {
                "agreement": False,
                "score_status": "scored",
                "error_category": score.get("error_category"),
                "databases_total": len(paths),
                "databases_checked": checked,
                "failure_database": db_path.name,
                "failure_database_path": str(db_path),
                "empty_database_matches": empty_matches,
                "prediction_execution_ms": round(prediction_ms, 3),
                "gold_execution_ms": round(gold_ms, 3),
                "failure_evidence": {
                    key: value for key, value in score.items()
                    if key not in {"agreement", "score_status", "error_category"}
                },
            }
    return {
        "agreement": True,
        "score_status": "scored",
        "error_category": None,
        "databases_total": len(paths),
        "databases_checked": checked,
        "failure_database": None,
        "failure_database_path": None,
        "empty_database_matches": empty_matches,
        "prediction_execution_ms": round(prediction_ms, 3),
        "gold_execution_ms": round(gold_ms, 3),
    }
