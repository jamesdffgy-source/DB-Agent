#!/usr/bin/env python3
"""Check whether a Windows source installation can start DB-Agent."""

from __future__ import annotations

import importlib
import importlib.metadata
import json
import os
import platform
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_IMPORTS = (
    ("aiohttp", "aiohttp"),
    ("openpyxl", "openpyxl"),
    ("psycopg2", "psycopg2-binary"),
    ("pymysql", "pymysql"),
    ("requests", "requests"),
    ("webview", "pywebview"),
)
REQUIRED_FILES = (
    ROOT / "dbagent_launcher.pyw",
    ROOT / "runtime/app/frontends/desktop_bridge.py",
    ROOT / "runtime/app/frontends/desktop/static/db.html",
)


def _webview2_candidates() -> list[Path]:
    roots = [
        os.environ.get("ProgramFiles(x86)"),
        os.environ.get("ProgramFiles"),
        os.environ.get("LOCALAPPDATA"),
    ]
    patterns = (
        "Microsoft/EdgeWebView/Application/*/msedgewebview2.exe",
        "Microsoft/EdgeWebView/Application/msedgewebview2.exe",
    )
    found: list[Path] = []
    for raw_root in roots:
        if not raw_root:
            continue
        base = Path(raw_root)
        for pattern in patterns:
            found.extend(path for path in base.glob(pattern) if path.is_file())
    return sorted(set(found))


def main() -> int:
    errors: list[str] = []
    warnings: list[str] = []

    if sys.version_info[:2] != (3, 12):
        errors.append(f"CPython 3.12 is required; found {platform.python_version()}")
    if os.name != "nt":
        errors.append(f"the desktop release supports Windows; found {platform.system()}")

    missing_files = [str(path.relative_to(ROOT)) for path in REQUIRED_FILES if not path.is_file()]
    if missing_files:
        errors.append("missing application files: " + ", ".join(missing_files))

    versions: dict[str, str] = {}
    for module_name, distribution_name in REQUIRED_IMPORTS:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # import errors are the actionable result here
            errors.append(f"cannot import {module_name}: {type(exc).__name__}: {exc}")
            continue
        try:
            versions[module_name] = importlib.metadata.version(distribution_name)
        except importlib.metadata.PackageNotFoundError:
            versions[module_name] = "installed"

    runtime_temp = ROOT / "runtime/app/temp"
    try:
        runtime_temp.mkdir(parents=True, exist_ok=True)
        probe = runtime_temp / ".write-probe"
        probe.write_text("ok", encoding="ascii")
        probe.unlink()
    except OSError as exc:
        errors.append(f"runtime data directory is not writable: {exc}")

    webview2 = _webview2_candidates()
    if not webview2:
        warnings.append(
            "Microsoft Edge WebView2 Runtime was not detected. Install the Evergreen Runtime "
            "if the desktop window cannot open."
        )

    report = {
        "ok": not errors,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "executable": sys.executable,
        "dependencies": versions,
        "webview2_detected": bool(webview2),
        "errors": errors,
        "warnings": warnings,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
