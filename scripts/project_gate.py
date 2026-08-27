#!/usr/bin/env python3
"""DBQuill completion gate: documentation freshness, tests and source fingerprint."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import py_compile
import re
import shutil
import subprocess
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
STATE_FILE = DOCS / "PROJECT_STATE.json"
REQUIRED_DOCS = (
    DOCS / "PROJECT_MEMORY.md",
    DOCS / "CURRENT_STATUS.md",
    DOCS / "TECH_STACK.md",
    DOCS / "DECISIONS.md",
    DOCS / "PROGRESS.md",
)
TRACKED_SUFFIXES = {
    ".py", ".pyw", ".ps1", ".cmd", ".html", ".css", ".js", ".json", ".png", ".ico", ".zip",
    ".md", ".txt", ".yml", ".yaml", ".lock", ".woff2",
}
BINARY_FINGERPRINT_SUFFIXES = {".ico", ".png", ".woff2", ".zip"}
EXCLUDED_PARTS = {
    "__pycache__",
    "data",
    "temp",
    "site-packages",
}
LOCAL_ONLY_FILES = {
    Path("runtime/app/model_profiles.json"),
}
COMPILE_TARGETS = (
    ROOT / "dbquill_launcher.pyw",
    ROOT / "runtime/app/frontends/dbquill_core.py",
    ROOT / "runtime/app/frontends/model_gateway.py",
    ROOT / "runtime/app/frontends/model_profiles.py",
    ROOT / "runtime/app/frontends/upload_storage.py",
    ROOT / "runtime/app/frontends/desktop_bridge.py",
    ROOT / "runtime/app/frontends/db_scheduler.py",
    ROOT / "runtime/app/frontends/db_sessions_store.py",
    ROOT / "runtime/app/frontends/db_semantic_store.py",
    ROOT / "runtime/app/frontends/db_audit_store.py",
    ROOT / "runtime/app/frontends/db_chart_cache.py",
    ROOT / "runtime/app/frontends/db_access_control.py",
    ROOT / "runtime/app/frontends/db_identity_store.py",
    ROOT / "runtime/app/frontends/nl2db_evaluation.py",
    ROOT / "runtime/app/frontends/model_baseline_contract.py",
    ROOT / "runtime/app/frontends/timezone_release_contract.py",
    ROOT / "runtime/app/frontends/test_security_regressions.py",
    ROOT / "scripts/manage_timezone_release.py",
    ROOT / "scripts/manage_audit_ledger.py",
    ROOT / "scripts/manage_local_roles.py",
    ROOT / "scripts/manage_local_identities.py",
    ROOT / "scripts/run_spider_benchmark.py",
    ROOT / "scripts/spider_execution_scoring.py",
    ROOT / "scripts/rescore_spider_execution.py",
    ROOT / "scripts/rescore_spider_test_suite.py",
    ROOT / "scripts/replay_bird_architecture.py",
    ROOT / "scripts/verify_spider_test_suite_upstream.py",
    ROOT / "scripts/analyze_spider_runs.py",
    ROOT / "scripts/run_bird_benchmark.py",
    ROOT / "scripts/check_repository_hygiene.py",
    ROOT / "scripts/check_source_provenance.py",
    ROOT / "scripts/doctor.py",
    ROOT / "scripts/smoke_startup.py",
)


def fail(message: str) -> None:
    raise RuntimeError(message)


def controlled_files() -> list[Path]:
    roots = [
        ROOT / "README.md",
        ROOT / "README.zh-CN.md",
        ROOT / "SECURITY.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "CODE_OF_CONDUCT.md",
        ROOT / "SUPPORT.md",
        ROOT / "CHANGELOG.md",
        ROOT / "LICENSE",
        ROOT / "THIRD_PARTY_NOTICES.md",
        ROOT / ".gitignore",
        ROOT / ".gitattributes",
        ROOT / ".python-version",
        ROOT / "requirements.txt",
        ROOT / "requirements.lock",
        ROOT / ".github",
        ROOT / "third_party",
        ROOT / "docs",
        ROOT / "dbquill_launcher.pyw",
        ROOT / "runtime/app",
        ROOT / "scripts",
    ]
    files: list[Path] = []
    for item in roots:
        candidates = [item] if item.is_file() else item.rglob("*")
        for path in candidates:
            if not path.is_file() or (
                path.suffix.lower() not in TRACKED_SUFFIXES
                and path.name not in {"LICENSE", ".gitignore", ".gitattributes", ".python-version"}
            ):
                continue
            relative = path.relative_to(ROOT)
            if relative == Path("docs/PROJECT_STATE.json"):
                continue
            if any(part.lower() in EXCLUDED_PARTS for part in relative.parts):
                continue
            if relative in LOCAL_ONLY_FILES:
                continue
            files.append(path)
    return sorted(set(files), key=lambda value: value.as_posix().lower())


def source_fingerprint(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(ROOT).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = _fingerprint_content(path)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _fingerprint_content(path: Path) -> bytes:
    """Canonicalize text newlines so Windows checkout policy cannot change state."""
    content = path.read_bytes()
    if path.suffix.lower() not in BINARY_FINGERPRINT_SUFFIXES:
        content = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return content


def _changed_paths() -> set[Path]:
    try:
        changed = subprocess.run(
            ["git", "-C", str(ROOT), "diff", "--name-only", "-z", "HEAD"],
            capture_output=True,
            check=True,
        ).stdout
        untracked = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "--others", "--exclude-standard", "-z"],
            capture_output=True,
            check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return set()
    raw = (changed + untracked).decode("utf-8", errors="surrogateescape")
    return {Path(value) for value in raw.split("\0") if value}


def validate_docs(files: list[Path]) -> None:
    missing = [str(path.relative_to(ROOT)) for path in REQUIRED_DOCS if not path.is_file() or path.stat().st_size < 80]
    if missing:
        fail("缺少项目记忆文件或内容为空: " + ", ".join(missing))
    changed = _changed_paths()
    changed_sources = [
        path for path in files
        if path.relative_to(ROOT) in changed
        and path.relative_to(ROOT).parts[0].lower() != "docs"
    ]
    if not changed_sources:
        return
    latest_source = max(path.stat().st_mtime for path in changed_sources)
    for path in (DOCS / "CURRENT_STATUS.md", DOCS / "PROGRESS.md"):
        if path.stat().st_mtime + 1 < latest_source:
            fail(f"{path.relative_to(ROOT)} 早于最新源码修改；请实时更新状态和进度")


def run_python_checks() -> int:
    for path in COMPILE_TARGETS:
        py_compile.compile(str(path), doraise=True)
    test_root = ROOT / "runtime/app/frontends"
    sys.path.insert(0, str(test_root))
    suite = unittest.defaultTestLoader.discover(str(test_root), pattern="test_security_regressions.py")
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if not result.wasSuccessful():
        fail("自动化回归失败")
    return result.testsRun


def run_evaluation_checks() -> dict:
    test_root = ROOT / "runtime/app/frontends"
    sys.path.insert(0, str(test_root))
    from nl2db_evaluation import run_suite  # noqa: PLC0415

    result = run_suite(with_model=False)
    offline = result["offline"]
    if not offline["all_cases_passed"]:
        details = ", ".join(
            f"{name}={count}"
            for name, count in offline["error_categories"].items()
        ) or "unknown"
        fail(
            f"NL-to-Database 固定评测失败: {offline['passed']}/{offline['total']}，"
            f"错误分类: {details}"
        )
    return {
        "suite_version": result["suite_version"],
        "offline_passed": offline["passed"],
        "offline_total": offline["total"],
        "model_status": result["model_nl2sql"]["status"],
    }


def run_timezone_checks() -> dict:
    test_root = ROOT / "runtime/app/frontends"
    sys.path.insert(0, str(test_root))
    import timezone_release_contract as contract  # noqa: PLC0415

    report = contract.validate_contract(run_probes=True)
    active = report["active"]
    return {
        "active_release_id": report["active_release_id"],
        "rollback_release_id": report["rollback_release_id"],
        "release_count": report["release_count"],
        "tzdata_version": active["tzdata_version"],
        "iana_version": active["iana_version"],
        "archive_sha256": active["sha256"],
        "zones_count": active["zones_count"],
        "probes_passed": active["probes_passed"],
        "probes_total": active["probes_total"],
    }


def run_model_baseline_checks() -> dict:
    test_root = ROOT / "runtime/app/frontends"
    sys.path.insert(0, str(test_root))
    import model_baseline_contract as contract  # noqa: PLC0415

    return contract.latest_summary()


def run_javascript_check() -> None:
    node = shutil.which("node")
    if not node:
        fail("未找到 Node.js，无法执行前端 JavaScript 语法门禁")
    static_root = ROOT / "runtime/app/frontends/desktop/static"
    html = (static_root / "db.html").read_text(encoding="utf-8")
    scripts = re.findall(r"<script[^>]*>([\s\S]*?)</script>", html, flags=re.IGNORECASE)
    sources = [
        (f"db.html inline script {index}", source)
        for index, source in enumerate(scripts, start=1)
        if source.strip()
    ]
    sources.extend(
        (path.name, path.read_text(encoding="utf-8"))
        for path in sorted(static_root.glob("*.js"))
    )
    checked = 0
    for label, source in sources:
        result = subprocess.run(
            [node, "--check"],
            input=source,
            text=True,
            encoding="utf-8",
            errors="strict",
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            details = (result.stderr or result.stdout).strip()
            fail(f"前端 JavaScript 语法检查失败（{label}）: {details}")
        checked += 1
    if checked == 0:
        fail("没有发现可检查的项目前端 JavaScript")


def load_state() -> dict:
    if not STATE_FILE.is_file():
        fail("尚未生成 docs/PROJECT_STATE.json；完成记录后运行 -Record")
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"门禁状态文件无效: {exc}")


def write_state(
    fingerprint: str,
    file_count: int,
    summary: str,
    test_count: int,
    evaluation: dict,
    timezone_release: dict,
    model_baseline: dict,
    repository_hygiene: dict,
) -> None:
    state = {
        "schema_version": 5,
        "recorded_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "source_fingerprint": fingerprint,
        "controlled_file_count": file_count,
        "python": sys.version.split()[0],
        "test_command": "scripts/check_project.cmd",
        "tests_passed": test_count,
        "evaluation": evaluation,
        "timezone_release": timezone_release,
        "model_baseline": model_baseline,
        "repository_hygiene": repository_hygiene,
        "summary": summary.strip(),
    }
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="DBQuill 项目完成门禁")
    parser.add_argument("--record", action="store_true", help="验证通过后记录当前源码指纹")
    parser.add_argument("--summary", default="", help="本次已记录并验证的工作摘要")
    args = parser.parse_args()

    files = controlled_files()
    if not files:
        fail("没有发现受控源码文件")
    validate_docs(files)
    fingerprint = source_fingerprint(files)
    from check_repository_hygiene import validate_repository  # noqa: PLC0415
    repository_hygiene = validate_repository()
    test_count = run_python_checks()
    evaluation = run_evaluation_checks()
    timezone_release = run_timezone_checks()
    model_baseline = run_model_baseline_checks()
    run_javascript_check()
    if model_baseline.get("status") == "recorded":
        latest_model = model_baseline["latest"]
        model_note = (
            f"真实模型基线 {latest_model['suite_version']} "
            f"{latest_model['passed']}/{latest_model['total']}"
        )
    else:
        model_note = "真实模型基线未登记"

    if args.record:
        if not args.summary.strip():
            fail("-Record 必须提供非空 -Summary")
        write_state(
            fingerprint, len(files), args.summary, test_count, evaluation, timezone_release,
            model_baseline,
            repository_hygiene,
        )
        print(
            f"PASS: 已验证并记录 {len(files)} 个受控文件，{test_count} 项测试通过，"
            f"NL-to-Database 离线评测 {evaluation['offline_passed']}/{evaluation['offline_total']}；"
            f"时区发布 {timezone_release['active_release_id']} 探针 "
            f"{timezone_release['probes_passed']}/{timezone_release['probes_total']}；{model_note}。"
        )
        return 0

    state = load_state()
    if state.get("source_fingerprint") != fingerprint:
        fail("源码已在最近一次验证记录后发生变化；请更新进度并运行 -Record")
    if state.get("timezone_release") != timezone_release:
        fail("时区发布状态已在最近一次验证记录后发生变化；请更新进度并运行 -Record")
    if state.get("model_baseline") != model_baseline:
        fail("真实模型基线历史已在最近一次验证记录后发生变化；请更新进度并运行 -Record")
    if state.get("repository_hygiene") != repository_hygiene:
        fail("仓库卫生状态已在最近一次验证记录后发生变化；请更新进度并运行 -Record")
    print(
        f"PASS: 项目记录与源码一致；{test_count} 项测试通过；"
        f"NL-to-Database 离线评测 {evaluation['offline_passed']}/{evaluation['offline_total']}；"
        f"时区发布 {timezone_release['active_release_id']} 探针 "
        f"{timezone_release['probes_passed']}/{timezone_release['probes_total']}；"
        f"{model_note}；"
        f"最近登记 {state.get('recorded_at', 'unknown')}。"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
