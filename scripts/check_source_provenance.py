#!/usr/bin/env python3
"""Compare release candidates with a local reference tree without copying it.

The audit reports exact file matches and unusually large token-shingle overlap.
It prints paths and metrics only; file contents and credentials are never emitted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEXT_EXTENSIONS = {
    ".py", ".pyw", ".js", ".css", ".html", ".cmd", ".ps1",
    ".md", ".txt", ".json", ".yml", ".yaml", ".toml", ".lock",
}
REFERENCE_EXCLUDED_DIRECTORIES = {".git", "venv", ".venv", "__pycache__", "temp"}
REPOSITORY_EXCLUDED_PREFIXES = (
    "runtime/python/", "benchmark_data/", "benchmark_results/", "artifacts/",
)


def _release_candidates() -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        capture_output=True,
        check=True,
    )
    relative_paths = result.stdout.decode("utf-8", errors="surrogateescape").split("\0")
    return [
        ROOT / relative
        for relative in relative_paths
        if relative
        and (ROOT / relative).is_file()
        and not relative.replace("\\", "/").startswith(REPOSITORY_EXCLUDED_PREFIXES)
    ]


def _reference_files(reference: Path) -> list[Path]:
    found: list[Path] = []
    for directory, children, filenames in os.walk(reference):
        children[:] = [name for name in children if name not in REFERENCE_EXCLUDED_DIRECTORIES]
        found.extend(Path(directory) / filename for filename in filenames)
    return found


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _tokens(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError):
        return []
    return re.findall(
        r"[A-Za-z_]\w*|[\u4e00-\u9fff]+|\d+|==|!=|<=|>=|:=|[-+*/%&|^~<>]=?|[{}()\[\].,:;]",
        text,
    )


def _token_shingles(tokens: list[str], width: int = 20) -> set[bytes]:
    if len(tokens) < width:
        return set()
    return {
        hashlib.blake2b("\x1f".join(tokens[index:index + width]).encode("utf-8"), digest_size=12).digest()
        for index in range(len(tokens) - width + 1)
    }


def compare(reference: Path) -> dict:
    reference = reference.resolve()
    repository_files = _release_candidates()
    reference_files = _reference_files(reference)

    reference_hashes: dict[str, list[Path]] = {}
    for path in reference_files:
        reference_hashes.setdefault(_sha256(path), []).append(path)
    exact = []
    for path in repository_files:
        for other in reference_hashes.get(_sha256(path), []):
            exact.append({
                "repository": path.relative_to(ROOT).as_posix(),
                "reference": other.relative_to(reference).as_posix(),
            })

    reference_text: dict[str, list[tuple[Path, set[bytes]]]] = {}
    for path in reference_files:
        extension = path.suffix.lower()
        if extension not in TEXT_EXTENSIONS:
            continue
        shingles = _token_shingles(_tokens(path))
        if shingles:
            reference_text.setdefault(extension, []).append((path, shingles))

    overlap = []
    for path in repository_files:
        extension = path.suffix.lower()
        if extension not in TEXT_EXTENSIONS:
            continue
        own = _token_shingles(_tokens(path))
        if not own:
            continue
        for other, candidate in reference_text.get(extension, []):
            shared = len(own & candidate)
            containment = shared / len(own)
            if shared >= 80 and containment >= 0.03:
                overlap.append({
                    "repository": path.relative_to(ROOT).as_posix(),
                    "reference": other.relative_to(reference).as_posix(),
                    "shared_20_token_shingles": shared,
                    "repository_containment": round(containment, 4),
                })

    return {
        "reference": str(reference),
        "repository_files": len(repository_files),
        "reference_files": len(reference_files),
        "exact_matches": sorted(exact, key=lambda item: (item["repository"], item["reference"])),
        "high_overlap": sorted(
            overlap,
            key=lambda item: (-item["repository_containment"], -item["shared_20_token_shingles"]),
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", type=Path, required=True)
    args = parser.parse_args()
    if not args.reference.is_dir():
        parser.error("--reference must be an existing directory")
    report = compare(args.reference)
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 1 if report["exact_matches"] or report["high_overlap"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
