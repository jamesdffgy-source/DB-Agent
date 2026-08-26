#!/usr/bin/env python3
"""Prepare, validate, activate, or roll back DB-Agent IANA release archives."""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime/app/frontends"
sys.path.insert(0, str(FRONTENDS))

import timezone_release_contract as contract  # noqa: E402


def _read_probes(path: Path | None, manifest: dict) -> list[dict]:
    if path is None:
        active = manifest["releases"][manifest["active_release_id"]]
        return [dict(item) for item in active["probes"]]
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("探针文件必须是 JSON 数组")
    return [contract.validate_probe(item) for item in raw]


def prepare(args: argparse.Namespace) -> dict:
    manifest = contract.load_manifest(args.manifest)
    item_id = contract.release_id(args.tzdata_version, args.iana_version)
    release_dir = Path(args.manifest).resolve().parent
    final_archive = release_dir / f"{item_id}.zip"
    nonce = uuid.uuid4().hex
    staging_archive = release_dir / f".{item_id}.{nonce}.candidate.zip"
    staging_manifest = release_dir / f".manifest.{nonce}.candidate.json"
    original_manifest = Path(args.manifest).resolve().read_bytes()
    promoted_new_archive = False
    try:
        built = contract.build_release_archive(
            args.source_root,
            args.zones_file,
            staging_archive,
            args.tzdata_version,
            args.iana_version,
        )
        probes = _read_probes(args.probes, manifest)
        candidate = {
            "tzdata_version": built["tzdata_version"],
            "iana_version": built["iana_version"],
            "archive": staging_archive.name,
            "sha256": built["sha256"],
            "zones_count": built["zones_count"],
            "probes": probes,
        }
        staged_releases = {key: dict(value) for key, value in manifest["releases"].items()}
        staged_releases[item_id] = candidate
        staged_manifest = {
            "schema_version": 1,
            "active_release_id": manifest["active_release_id"],
            "rollback_release_id": manifest["rollback_release_id"],
            "releases": staged_releases,
        }
        contract.write_manifest_atomic(staging_manifest, staged_manifest)
        staged = contract.load_manifest(staging_manifest)
        report = contract.validate_release_archive(staged, item_id, run_probes=True)

        existing = manifest["releases"].get(item_id)
        registered_candidate = {**candidate, "archive": final_archive.name}
        if existing is not None and existing != registered_candidate:
            raise ValueError("同一时区版本号已经登记，不能以不同内容覆盖")
        if final_archive.exists():
            if contract.sha256_file(final_archive) != built["sha256"]:
                raise ValueError("目标时区发布包已存在且内容不同，拒绝覆盖")
            staging_archive.unlink()
        else:
            os.replace(staging_archive, final_archive)
            promoted_new_archive = True

        releases = {key: dict(value) for key, value in manifest["releases"].items()}
        releases[item_id] = registered_candidate
        updated = {
            "schema_version": 1,
            "active_release_id": manifest["active_release_id"],
            "rollback_release_id": manifest["rollback_release_id"],
            "releases": releases,
        }
        contract.write_manifest_atomic(args.manifest, updated)
        validated = contract.load_manifest(args.manifest)
        final_report = contract.validate_release_archive(validated, item_id, run_probes=True)
        return {"action": "prepared", **final_report}
    except Exception:
        contract.restore_manifest(args.manifest, original_manifest)
        if promoted_new_archive and final_archive.exists():
            final_archive.unlink()
        raise
    finally:
        for temporary in (staging_archive, staging_manifest):
            if temporary.exists():
                temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="DB-Agent 固定 IANA 发布管理")
    parser.add_argument(
        "--manifest", type=Path, default=contract.DEFAULT_MANIFEST,
        help="版本清单路径",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="验证全部已登记发布包与探针")

    prepare_parser = subparsers.add_parser("prepare", help="从本地 tzdata 目录准备候选发布")
    prepare_parser.add_argument("--source-root", type=Path, required=True)
    prepare_parser.add_argument("--zones-file", type=Path, required=True)
    prepare_parser.add_argument("--tzdata-version", required=True)
    prepare_parser.add_argument("--iana-version", required=True)
    prepare_parser.add_argument("--probes", type=Path)

    activate_parser = subparsers.add_parser("activate", help="激活已准备发布并保留上一发布")
    activate_parser.add_argument("release_id")
    subparsers.add_parser("rollback", help="切回清单中的上一发布")
    args = parser.parse_args()

    if args.command == "status":
        report = contract.validate_contract(args.manifest, run_probes=True)
        result = {"action": "status", **report}
    elif args.command == "prepare":
        result = prepare(args)
    elif args.command == "activate":
        manifest = contract.switch_active_release(args.manifest, args.release_id)
        result = {
            "action": "activated",
            "active_release_id": manifest["active_release_id"],
            "rollback_release_id": manifest["rollback_release_id"],
        }
    else:
        manifest = contract.load_manifest(args.manifest)
        rollback_id = manifest.get("rollback_release_id")
        if not rollback_id:
            raise ValueError("当前清单没有可回滚发布")
        manifest = contract.switch_active_release(args.manifest, rollback_id)
        result = {
            "action": "rolled_back",
            "active_release_id": manifest["active_release_id"],
            "rollback_release_id": manifest["rollback_release_id"],
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
