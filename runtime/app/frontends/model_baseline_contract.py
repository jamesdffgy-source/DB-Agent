#!/usr/bin/env python3
"""Append-only, redacted history for real-model NL2SQL evaluation runs."""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_HISTORY = ROOT / "docs" / "MODEL_BASELINES.json"
IDENTITY_KEYS = {"config_name", "name", "model", "api_mode", "endpoint_fingerprint"}
RECORD_KEYS = {
    "run_id", "recorded_at", "label", "suite_version", "dataset_sha256",
    "prompt_contract", "model", "status", "passed", "total",
    "execution_accuracy", "latency_ms", "cases",
}
CASE_KEYS = {"id", "passed", "error_category", "latency_ms", "sql", "error"}
LATENCY_KEYS = {"total", "median", "maximum"}


def _text(value: Any, label: str, maximum: int, *, allow_empty: bool = False) -> str:
    text = str(value or "").strip()
    if (not text and not allow_empty) or len(text) > maximum:
        raise ValueError(f"模型基线 {label} 无效")
    return text


def validate_identity(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != IDENTITY_KEYS:
        raise ValueError("模型基线身份字段无效")
    endpoint = raw.get("endpoint_fingerprint")
    if endpoint is not None and not re.fullmatch(r"[0-9a-f]{16}", str(endpoint)):
        raise ValueError("模型基线 endpoint_fingerprint 无效")
    return {
        "config_name": _text(raw.get("config_name"), "config_name", 64),
        "name": _text(raw.get("name"), "name", 128, allow_empty=True),
        "model": _text(raw.get("model"), "model", 128),
        "api_mode": _text(raw.get("api_mode"), "api_mode", 32),
        "endpoint_fingerprint": None if endpoint is None else str(endpoint),
    }


def _number(value: Any, label: str, *, minimum: float = 0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum:
        raise ValueError(f"模型基线 {label} 无效")
    return round(float(value), 3)


def validate_case(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != CASE_KEYS:
        raise ValueError("模型基线用例字段无效")
    if not isinstance(raw.get("passed"), bool):
        raise ValueError("模型基线用例 passed 无效")
    category = raw.get("error_category")
    sql = raw.get("sql")
    error = raw.get("error")
    return {
        "id": _text(raw.get("id"), "case.id", 128),
        "passed": raw["passed"],
        "error_category": None if category is None else _text(category, "error_category", 64),
        "latency_ms": _number(raw.get("latency_ms"), "case.latency_ms"),
        "sql": None if sql is None else _text(sql, "case.sql", 8000),
        "error": None if error is None else _text(error, "case.error", 1000),
    }


def validate_record(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != RECORD_KEYS:
        raise ValueError("模型基线记录字段无效")
    run_id = _text(raw.get("run_id"), "run_id", 64)
    if not re.fullmatch(r"model-[0-9a-f]{32}", run_id):
        raise ValueError("模型基线 run_id 无效")
    recorded_at = _text(raw.get("recorded_at"), "recorded_at", 40)
    try:
        parsed_at = datetime.fromisoformat(recorded_at)
    except ValueError as exc:
        raise ValueError("模型基线 recorded_at 无效") from exc
    if parsed_at.utcoffset() is None:
        raise ValueError("模型基线 recorded_at 必须包含时区")
    status = str(raw.get("status") or "")
    if status not in {"completed", "completed_with_failures"}:
        raise ValueError("模型基线 status 无效")
    passed = raw.get("passed")
    total = raw.get("total")
    if (
        isinstance(passed, bool) or not isinstance(passed, int)
        or isinstance(total, bool) or not isinstance(total, int)
        or total < 1 or not 0 <= passed <= total
    ):
        raise ValueError("模型基线通过数无效")
    accuracy = raw.get("execution_accuracy")
    if isinstance(accuracy, bool) or not isinstance(accuracy, (int, float)) or not 0 <= accuracy <= 1:
        raise ValueError("模型基线 execution_accuracy 无效")
    if round(float(accuracy), 4) != round(passed / total, 4):
        raise ValueError("模型基线 execution_accuracy 与通过数不一致")
    latency = raw.get("latency_ms")
    if not isinstance(latency, dict) or set(latency) != LATENCY_KEYS:
        raise ValueError("模型基线 latency_ms 无效")
    checked_latency = {key: _number(latency[key], f"latency_ms.{key}") for key in LATENCY_KEYS}
    if checked_latency["total"] < checked_latency["maximum"] or checked_latency["maximum"] < checked_latency["median"]:
        raise ValueError("模型基线 latency_ms 顺序无效")
    cases_raw = raw.get("cases")
    if not isinstance(cases_raw, list) or len(cases_raw) != total:
        raise ValueError("模型基线 cases 数量无效")
    cases = [validate_case(case) for case in cases_raw]
    ids = [case["id"] for case in cases]
    if len(ids) != len(set(ids)) or sum(case["passed"] for case in cases) != passed:
        raise ValueError("模型基线用例 ID 或通过数不一致")
    dataset_sha256 = _text(raw.get("dataset_sha256"), "dataset_sha256", 64)
    if not re.fullmatch(r"[0-9a-f]{64}", dataset_sha256):
        raise ValueError("模型基线 dataset_sha256 无效")
    prompt_contract = _text(raw.get("prompt_contract"), "prompt_contract", 64)
    if not re.fullmatch(r"nl2sql-[0-9a-f]{16}", prompt_contract):
        raise ValueError("模型基线 prompt_contract 无效")
    expected_status = "completed" if passed == total else "completed_with_failures"
    if status != expected_status:
        raise ValueError("模型基线 status 与通过数不一致")
    return {
        "run_id": run_id,
        "recorded_at": recorded_at,
        "label": _text(raw.get("label"), "label", 80, allow_empty=True),
        "suite_version": _text(raw.get("suite_version"), "suite_version", 64),
        "dataset_sha256": dataset_sha256,
        "prompt_contract": prompt_contract,
        "model": validate_identity(raw.get("model")),
        "status": status,
        "passed": passed,
        "total": total,
        "execution_accuracy": round(float(accuracy), 4),
        "latency_ms": checked_latency,
        "cases": cases,
    }


def load_history(path: Path = DEFAULT_HISTORY) -> dict:
    target = Path(path).resolve()
    if not target.exists():
        return {"schema_version": 1, "runs": []}
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"无法读取模型基线历史: {exc}") from exc
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "runs"}:
        raise ValueError("模型基线历史字段无效")
    if raw.get("schema_version") != 1 or not isinstance(raw.get("runs"), list):
        raise ValueError("模型基线历史 schema_version/runs 无效")
    runs = [validate_record(item) for item in raw["runs"]]
    ids = [item["run_id"] for item in runs]
    if len(ids) != len(set(ids)):
        raise ValueError("模型基线历史包含重复 run_id")
    return {"schema_version": 1, "runs": runs}


def build_record(result: dict, label: str = "") -> dict:
    model = result.get("model_nl2sql") or {}
    if model.get("status") not in {"completed", "completed_with_failures"}:
        raise ValueError("只有已完成的真实模型通道可以登记基线")
    cases = [{
        "id": case.get("id"),
        "passed": bool(case.get("passed")),
        "error_category": case.get("error_category"),
        "latency_ms": case.get("latency_ms", 0),
        "sql": case.get("sql") or None,
        "error": case.get("error") or None,
    } for case in model.get("cases") or []]
    return validate_record({
        "run_id": "model-" + uuid.uuid4().hex,
        "recorded_at": result.get("generated_at"),
        "label": str(label or "").strip(),
        "suite_version": result.get("suite_version"),
        "dataset_sha256": result.get("dataset_sha256"),
        "prompt_contract": model.get("prompt_contract"),
        "model": model.get("model_identity"),
        "status": model.get("status"),
        "passed": model.get("passed"),
        "total": model.get("total"),
        "execution_accuracy": model.get("execution_accuracy"),
        "latency_ms": model.get("latency_ms"),
        "cases": cases,
    })


def _same_benchmark(left: dict, right: dict) -> bool:
    return all(left[key] == right[key] for key in ("suite_version", "dataset_sha256"))


def compare_records(baseline: dict, current: dict) -> dict:
    older = validate_record(baseline)
    newer = validate_record(current)
    if not _same_benchmark(older, newer):
        raise ValueError("模型基线评测版本或数据集不兼容")
    old_cases = {case["id"]: case for case in older["cases"]}
    new_cases = {case["id"]: case for case in newer["cases"]}
    if set(old_cases) != set(new_cases):
        raise ValueError("模型基线用例集合不一致")
    regressions = sorted(case_id for case_id in old_cases if old_cases[case_id]["passed"] and not new_cases[case_id]["passed"])
    improvements = sorted(case_id for case_id in old_cases if not old_cases[case_id]["passed"] and new_cases[case_id]["passed"])
    return {
        "baseline_run_id": older["run_id"],
        "current_run_id": newer["run_id"],
        "accuracy_delta": round(newer["execution_accuracy"] - older["execution_accuracy"], 4),
        "passed_delta": newer["passed"] - older["passed"],
        "median_latency_delta_ms": round(
            newer["latency_ms"]["median"] - older["latency_ms"]["median"], 3,
        ),
        "prompt_contract_changed": older["prompt_contract"] != newer["prompt_contract"],
        "model_changed": older["model"] != newer["model"],
        "baseline_prompt_contract": older["prompt_contract"],
        "current_prompt_contract": newer["prompt_contract"],
        "baseline_model": older["model"],
        "current_model": newer["model"],
        "regressions": regressions,
        "improvements": improvements,
    }


def append_record(path: Path, record: dict) -> dict:
    target = Path(path).resolve()
    checked = validate_record(record)
    history = load_history(target)
    if any(item["run_id"] == checked["run_id"] for item in history["runs"]):
        raise ValueError("模型基线 run_id 已存在，历史只能追加新运行")
    previous = next(
        (item for item in reversed(history["runs"]) if _same_benchmark(item, checked)), None,
    )
    history["runs"].append(checked)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {
        "record": checked,
        "comparison": compare_records(previous, checked) if previous is not None else None,
        "history_count": len(history["runs"]),
    }


def latest_summary(path: Path = DEFAULT_HISTORY) -> dict:
    history = load_history(path)
    if not history["runs"]:
        return {"status": "not_recorded", "run_count": 0, "latest": None}
    latest = history["runs"][-1]
    return {
        "status": "recorded",
        "run_count": len(history["runs"]),
        "latest": {
            "run_id": latest["run_id"],
            "recorded_at": latest["recorded_at"],
            "suite_version": latest["suite_version"],
            "dataset_sha256": latest["dataset_sha256"],
            "prompt_contract": latest["prompt_contract"],
            "model": latest["model"],
            "passed": latest["passed"],
            "total": latest["total"],
            "execution_accuracy": latest["execution_accuracy"],
        },
    }
