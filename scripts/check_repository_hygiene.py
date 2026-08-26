#!/usr/bin/env python3
"""Validate the source repository's reproducibility and release hygiene."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    Path("README.md"),
    Path("README.zh-CN.md"),
    Path("SECURITY.md"),
    Path("LICENSE"),
    Path("THIRD_PARTY_NOTICES.md"),
    Path("CONTRIBUTING.md"),
    Path("CODE_OF_CONDUCT.md"),
    Path("SUPPORT.md"),
    Path("CHANGELOG.md"),
    Path(".python-version"),
    Path("requirements.txt"),
    Path("requirements.lock"),
    Path(".github/workflows/ci.yml"),
    Path(".github/workflows/release.yml"),
    Path(".github/ISSUE_TEMPLATE/bug_report.yml"),
    Path(".github/ISSUE_TEMPLATE/feature_request.yml"),
    Path(".github/PULL_REQUEST_TEMPLATE.md"),
    Path("scripts/run_python.cmd"),
    Path("scripts/bootstrap_dev.cmd"),
    Path("scripts/start_dbquill.cmd"),
    Path("scripts/install_and_start.cmd"),
    Path("scripts/doctor.cmd"),
    Path("scripts/doctor.py"),
    Path("scripts/smoke_startup.py"),
    Path("docs/assets/dbquill-overview.png"),
    Path("docs/assets/dbquill-handdrawn-workflow.png"),
    Path("third_party/licenses/APACHE-2.0.txt"),
    Path("third_party/licenses/BSD-3-Clause-D3.txt"),
)
TEXT_SUFFIXES = {
    "", ".cmd", ".css", ".html", ".ini", ".js", ".json", ".lock",
    ".md", ".ps1", ".py", ".pyw", ".txt", ".yaml", ".yml",
}
IGNORED_SCAN_PARTS = {".git", ".venv", ".cache", "runtime/python"}
SENSITIVE_EXACT_PATHS = {
    "runtime/app/model_profiles.json",
}
SENSITIVE_SUFFIXES = {
    ".db", ".db-shm", ".db-wal", ".key", ".log", ".p12", ".pem",
    ".pfx", ".sqlite", ".sqlite3",
}
SECRET_PATTERNS = (
    ("OpenAI-compatible API token", re.compile(r"sk-[A-Za-z0-9_-]{20,}")),
    ("GitHub token", re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}")),
    ("AWS access key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("Google API key", re.compile(r"AIza[0-9A-Za-z_-]{30,}")),
    ("private key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
)


class RepositoryHygieneError(RuntimeError):
    """The candidate repository contains a release-hygiene violation."""


def _fail(message: str) -> None:
    raise RepositoryHygieneError(message)


def _git_candidate_paths() -> list[Path]:
    result = subprocess.run(
        [
            "git", "-C", str(ROOT), "ls-files", "--cached", "--others",
            "--exclude-standard", "-z",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        details = result.stderr.decode("utf-8", errors="replace").strip()
        _fail(f"无法枚举 Git 候选文件: {details or 'git ls-files failed'}")
    values = result.stdout.decode("utf-8", errors="surrogateescape").split("\0")
    return sorted(
        {Path(value) for value in values if value and (ROOT / value).is_file()},
        key=lambda value: value.as_posix().lower(),
    )


def _canonical_package(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _parse_exact_requirements(path: Path, *, require_hash: bool) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    packages: dict[str, str] = {}
    for match in re.finditer(r"(?m)^([A-Za-z0-9_.-]+)==([^\s\\]+)", text):
        raw_name, version = match.groups()
        name = _canonical_package(raw_name)
        if name in packages:
            _fail(f"{path.name} contains duplicate package: {name}")
        packages[name] = version
        if require_hash:
            block_end = text.find("\n", match.end())
            if block_end < 0:
                block_end = len(text)
            next_end = text.find("\n", block_end + 1)
            if next_end < 0:
                next_end = len(text)
            block = text[match.start():next_end]
            if not re.search(r"--hash=sha256:[0-9a-f]{64}", block):
                _fail(f"{path.name} entry has no sha256 hash: {name}=={version}")
    meaningful = [
        line.strip() for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith(("#", "--hash="))
    ]
    if len(packages) != len(meaningful):
        _fail(f"{path.name} must contain only exact package==version entries")
    if not packages:
        _fail(f"{path.name} has no dependencies")
    return packages


def _check_requirements() -> tuple[int, int]:
    direct = _parse_exact_requirements(ROOT / "requirements.txt", require_hash=False)
    locked = _parse_exact_requirements(ROOT / "requirements.lock", require_hash=True)
    missing = [
        f"{name}=={version}" for name, version in direct.items()
        if locked.get(name) != version
    ]
    if missing:
        _fail("direct dependencies missing from lock: " + ", ".join(missing))
    if (ROOT / ".python-version").read_text(encoding="utf-8").strip() != "3.12":
        _fail(".python-version must select the supported CPython 3.12 series")
    return len(direct), len(locked)


def _check_candidate_paths(paths: list[Path]) -> None:
    violations = []
    for relative in paths:
        normalized = relative.as_posix().lower()
        name = relative.name.lower()
        if normalized in SENSITIVE_EXACT_PATHS:
            violations.append(relative.as_posix())
        elif name == ".env" or (name.startswith(".env.") and name != ".env.example"):
            violations.append(relative.as_posix())
        elif any(normalized.endswith(suffix) for suffix in SENSITIVE_SUFFIXES):
            violations.append(relative.as_posix())
    if violations:
        _fail("candidate repository contains local/sensitive files: " + ", ".join(violations))


def _scan_secrets(paths: list[Path]) -> int:
    scanned = 0
    for relative in paths:
        if relative.suffix.lower() not in TEXT_SUFFIXES:
            continue
        path = ROOT / relative
        if path.stat().st_size > 5_000_000:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        scanned += 1
        for label, pattern in SECRET_PATTERNS:
            if pattern.search(text):
                _fail(f"possible {label} in {relative.as_posix()}")
    return scanned


def _check_redistributed_assets() -> None:
    required_assets = (
        "runtime/app/frontends/desktop/static/vendor/echarts.min.js",
        "runtime/app/frontends/timezone_releases/tzdata-2026.3-iana-2026c.zip",
    )
    for relative in required_assets:
        if not (ROOT / relative).is_file():
            _fail(f"redistributed asset missing: {relative}")
    font_root = ROOT / "runtime/app/frontends/desktop/static/assets/fonts"
    if font_root.exists() and any(path.is_file() for path in font_root.rglob("*")):
        _fail("desktop release must use system fonts and must not bundle inherited font files")


def _check_runtime_provenance(paths: list[Path]) -> None:
    # Store fingerprints instead of retired identifiers so the prevention gate
    # itself does not re-publish their names.
    forbidden_name_hashes = {
        "4d11c682da5e8bcf3f05687d528611966dc9a5d154a9dbf54871ee8e81168b4b",
        "5ec4a8923d2dfeb8382ae3278ab3fc02bdc0af1841035839ee5bbb5e9212989d",
        "505da56227a55047fb6f2164d524562a0a1c68e6aebbd489d4ed32fd37921621",
        "90f475cfab11e313e9a6bc1874879df40bfefd07ff333434689e6cde799cc0da",
        "49e2ffd6fe52f60a83acd5195354180abaada0756fb1558bc2f7f26794145722",
        "304b7803ebb0c9cd632cc5d128faaa0bdf84af257f4c0e6b29e018b014bef9d4",
        "006f25b629087408b611fd3e65ff5a1652020b295796228c97d455d71d957434",
    }
    forbidden_token_hashes = {
        "613ec739d794ca7b90d9d489d2948f79aedb6840316de394f3efc26d4c3e15f2",
        "731f0b55f686739ae9f192425836a5600bfda130830f7658aca046a0215d4145",
        "2ab9abaae3efe262cd3e757554f55d3d64c70fe1b9534e6e9fa215bd17d9d9b7",
        "79fb4d483d5fa645729e6e2525099214c2fd3b4a5f8e817503eae28063ac9954",
    }
    runtime_paths = [
        relative for relative in paths
        if relative.as_posix().startswith("runtime/app/")
        or relative.as_posix() == "dbquill_launcher.pyw"
    ]
    bad_names = [
        relative.as_posix() for relative in runtime_paths
        if hashlib.sha256(relative.name.encode("utf-8")).hexdigest() in forbidden_name_hashes
    ]
    if bad_names:
        _fail("retired external runtime files returned: " + ", ".join(bad_names))
    for relative in runtime_paths:
        if relative.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            text = (ROOT / relative).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        token_hashes = {
            hashlib.sha256(token.encode("utf-8")).hexdigest()
            for token in re.findall(r"[A-Za-z_][A-Za-z0-9_-]*", text)
        }
        if token_hashes & forbidden_token_hashes:
            _fail(f"retired external runtime marker in {relative.as_posix()}")


def _check_ci_contract() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    required_fragments = (
        "actions/checkout@v7",
        "actions/setup-python@v7",
        ".\\scripts\\bootstrap_dev.cmd",
        ".\\scripts\\doctor.cmd",
        "DBQUILL_PYTHON",
        ".\\scripts\\check_project.cmd",
        "scripts\\smoke_startup.py",
        "contents: read",
    )
    missing = [fragment for fragment in required_fragments if fragment not in workflow]
    if missing:
        _fail("CI workflow is missing: " + ", ".join(missing))


def validate_repository() -> dict:
    missing = [path.as_posix() for path in REQUIRED_FILES if not (ROOT / path).is_file()]
    if missing:
        _fail("required release files missing: " + ", ".join(missing))
    for relative in REQUIRED_FILES:
        if (ROOT / relative).stat().st_size < 1:
            _fail(f"required release file is empty: {relative.as_posix()}")
    paths = _git_candidate_paths()
    _check_candidate_paths(paths)
    _check_runtime_provenance(paths)
    text_files = _scan_secrets(paths)
    direct_count, locked_count = _check_requirements()
    _check_redistributed_assets()
    _check_ci_contract()
    return {
        "candidate_files": len(paths),
        "text_files_scanned": text_files,
        "direct_dependencies": direct_count,
        "locked_dependencies": locked_count,
        "credential_findings": 0,
        "redistributed_assets_verified": 2,
    }


def main() -> int:
    try:
        report = validate_repository()
    except RepositoryHygieneError as exc:
        print(f"FAIL: {exc}")
        return 1
    print(json.dumps({"ok": True, **report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
