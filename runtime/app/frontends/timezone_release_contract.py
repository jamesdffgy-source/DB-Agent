#!/usr/bin/env python3
"""Versioned, hash-bound IANA release archives for DB-Agent.

The runtime reads TZif files from project-owned ZIP archives. Multiple releases
may coexist so semantic calendars remain reproducible after an upgrade, while
the active release only controls newly created definitions.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
DEFAULT_RELEASE_DIR = ROOT / "timezone_releases"
DEFAULT_MANIFEST = DEFAULT_RELEASE_DIR / "manifest.json"
RELEASE_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{2,79}")
VERSION_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,31}")
ARCHIVE_ZONE_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+.-]*(?:/[A-Za-z0-9_+.-]+)*")
ZONE_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+.-]*(?:/[A-Za-z0-9_+.-]+)+")
RELEASE_KEYS = {
    "tzdata_version", "iana_version", "archive", "sha256", "zones_count", "probes",
}
PROBE_KEYS = {"id", "zone", "utc", "expected_date"}


def _valid_zone_name(value: str, *, require_region: bool = False) -> bool:
    pattern = ZONE_RE if require_region else ARCHIVE_ZONE_RE
    return bool(
        pattern.fullmatch(value)
        and all(part not in {".", ".."} for part in value.split("/"))
    )


def _clean_version(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not VERSION_RE.fullmatch(text):
        raise ValueError(f"{label} 格式无效")
    return text


def release_id(tzdata_version: str, iana_version: str) -> str:
    tzdata = _clean_version(tzdata_version, "tzdata_version").lower()
    iana = _clean_version(iana_version, "iana_version").lower()
    value = f"tzdata-{tzdata}-iana-{iana}"
    if not RELEASE_ID_RE.fullmatch(value):
        raise ValueError("时区发布 ID 格式无效")
    return value


def version_token(release: dict) -> str:
    return f"tzdata-{release['tzdata_version']}/iana-{release['iana_version']}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("探针 utc 必须是带 Z/+00:00 的 ISO UTC 时间")
    return parsed.astimezone(timezone.utc)


def validate_probe(raw: Any) -> dict:
    if not isinstance(raw, dict) or set(raw) != PROBE_KEYS:
        raise ValueError("时区探针字段必须为 id/zone/utc/expected_date")
    probe_id = str(raw.get("id") or "").strip()
    zone = str(raw.get("zone") or "").strip()
    expected = str(raw.get("expected_date") or "").strip()
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,79}", probe_id):
        raise ValueError("时区探针 id 格式无效")
    if not _valid_zone_name(zone, require_region=True):
        raise ValueError("时区探针 zone 格式无效")
    _parse_utc(str(raw.get("utc") or ""))
    datetime.strptime(expected, "%Y-%m-%d")
    return {"id": probe_id, "zone": zone, "utc": str(raw["utc"]), "expected_date": expected}


def validate_manifest(raw: Any, manifest_path: Path = DEFAULT_MANIFEST) -> dict:
    if not isinstance(raw, dict) or set(raw) != {
        "schema_version", "active_release_id", "rollback_release_id", "releases",
    }:
        raise ValueError("时区发布清单字段无效")
    if raw.get("schema_version") != 1:
        raise ValueError("时区发布清单 schema_version 必须为 1")
    active = str(raw.get("active_release_id") or "").strip()
    rollback_raw = raw.get("rollback_release_id")
    rollback = None if rollback_raw is None else str(rollback_raw).strip()
    releases_raw = raw.get("releases")
    if not RELEASE_ID_RE.fullmatch(active) or not isinstance(releases_raw, dict):
        raise ValueError("时区发布清单缺少有效 active_release_id/releases")
    if rollback is not None and not RELEASE_ID_RE.fullmatch(rollback):
        raise ValueError("时区发布清单 rollback_release_id 无效")

    releases: dict[str, dict] = {}
    for item_id, item in releases_raw.items():
        if not isinstance(item_id, str) or not RELEASE_ID_RE.fullmatch(item_id):
            raise ValueError("时区发布 ID 无效")
        if not isinstance(item, dict) or set(item) != RELEASE_KEYS:
            raise ValueError(f"时区发布 {item_id} 字段无效")
        tzdata = _clean_version(item.get("tzdata_version"), "tzdata_version")
        iana = _clean_version(item.get("iana_version"), "iana_version")
        if item_id != release_id(tzdata, iana):
            raise ValueError(f"时区发布 {item_id} 与版本号不一致")
        archive = str(item.get("archive") or "").strip()
        if Path(archive).name != archive or not archive.endswith(".zip"):
            raise ValueError(f"时区发布 {item_id} archive 必须是清单同目录下的 ZIP 文件")
        checksum = str(item.get("sha256") or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ValueError(f"时区发布 {item_id} sha256 无效")
        zones_count = item.get("zones_count")
        if isinstance(zones_count, bool) or not isinstance(zones_count, int) or zones_count < 1:
            raise ValueError(f"时区发布 {item_id} zones_count 无效")
        probes_raw = item.get("probes")
        if not isinstance(probes_raw, list) or len(probes_raw) < 4 or len(probes_raw) > 64:
            raise ValueError(f"时区发布 {item_id} 必须包含 4–64 个探针")
        probes = [validate_probe(probe) for probe in probes_raw]
        probe_ids = [probe["id"] for probe in probes]
        if len(probe_ids) != len(set(probe_ids)):
            raise ValueError(f"时区发布 {item_id} 探针 ID 不能重复")
        releases[item_id] = {
            "tzdata_version": tzdata,
            "iana_version": iana,
            "archive": archive,
            "sha256": checksum,
            "zones_count": zones_count,
            "probes": probes,
        }
    if active not in releases:
        raise ValueError("active_release_id 不在 releases 中")
    if rollback is not None and rollback not in releases:
        raise ValueError("rollback_release_id 不在 releases 中")
    if rollback == active:
        raise ValueError("active_release_id 与 rollback_release_id 不能相同")
    return {
        "schema_version": 1,
        "active_release_id": active,
        "rollback_release_id": rollback,
        "releases": releases,
        "manifest_path": str(Path(manifest_path).resolve()),
    }


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict:
    target = Path(path).resolve()
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"无法读取时区发布清单: {exc}") from exc
    return validate_manifest(raw, target)


def archive_path(manifest: dict, item_id: str) -> Path:
    manifest_dir = Path(manifest["manifest_path"]).parent
    return manifest_dir / manifest["releases"][item_id]["archive"]


def find_release(manifest: dict, tzdata_version: str, iana_version: str) -> tuple[str, dict] | None:
    for item_id, release in manifest["releases"].items():
        if release["tzdata_version"] == tzdata_version and release["iana_version"] == iana_version:
            return item_id, release
    return None


def release_for_token(manifest: dict, token: str) -> tuple[str, dict] | None:
    for item_id, release in manifest["releases"].items():
        if version_token(release) == str(token):
            return item_id, release
    return None


def load_zone(manifest: dict, item_id: str, zone: str) -> ZoneInfo:
    if not _valid_zone_name(str(zone or "")):
        raise ValueError("IANA 区域名称格式无效")
    target = archive_path(manifest, item_id)
    try:
        with zipfile.ZipFile(target, "r") as package:
            payload = package.read(f"zoneinfo/{zone}")
    except KeyError as exc:
        raise ValueError(f"发布包不包含 IANA 区域 {zone}") from exc
    except zipfile.BadZipFile as exc:
        raise ValueError(f"时区发布包不是有效 ZIP: {item_id}") from exc
    return ZoneInfo.from_file(io.BytesIO(payload), key=zone)


def validate_release_archive(manifest: dict, item_id: str, run_probes: bool = True) -> dict:
    release = manifest["releases"][item_id]
    target = archive_path(manifest, item_id)
    if not target.is_file():
        raise ValueError(f"时区发布包不存在: {target}")
    actual_hash = sha256_file(target)
    if actual_hash != release["sha256"]:
        raise ValueError(f"时区发布包哈希不一致: {item_id}")
    try:
        with zipfile.ZipFile(target, "r") as package:
            name_list = package.namelist()
            if len(name_list) != len(set(name_list)):
                raise ValueError(f"时区发布包包含重复条目: {item_id}")
            names = set(name_list)
            metadata = json.loads(package.read("release.json").decode("utf-8"))
            zone_entries = [
                name for name in names
                if name.startswith("zoneinfo/") and not name.endswith("/")
            ]
            infos = [package.getinfo(name) for name in zone_entries]
            if (
                any(info.file_size < 44 or info.file_size > 1024 * 1024 for info in infos)
                or sum(info.file_size for info in infos) > 64 * 1024 * 1024
            ):
                raise ValueError(f"时区发布包包含异常大小的 TZif 条目: {item_id}")
            if any(package.read(info.filename, pwd=None)[:4] != b"TZif" for info in infos):
                raise ValueError(f"时区发布包包含无效 TZif 条目: {item_id}")
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"时区发布包元数据无效: {item_id}") from exc
    except zipfile.BadZipFile as exc:
        raise ValueError(f"时区发布包不是有效 ZIP: {item_id}") from exc
    invalid_entries = [
        name for name in names
        if name != "release.json"
        and (
            not name.startswith("zoneinfo/")
            or not _valid_zone_name(name[len("zoneinfo/"):])
        )
    ]
    if invalid_entries:
        raise ValueError(f"时区发布包包含无效条目: {item_id}")
    if metadata != {
        "schema_version": 1,
        "release_id": item_id,
        "tzdata_version": release["tzdata_version"],
        "iana_version": release["iana_version"],
        "zones_count": release["zones_count"],
    }:
        raise ValueError(f"时区发布包元数据与清单不一致: {item_id}")
    if len(zone_entries) != release["zones_count"]:
        raise ValueError(f"时区发布包区域数量不一致: {item_id}")

    results = []
    if run_probes:
        for probe in release["probes"]:
            actual = _parse_utc(probe["utc"]).astimezone(
                load_zone(manifest, item_id, probe["zone"]),
            ).date().isoformat()
            results.append({
                "id": probe["id"],
                "zone": probe["zone"],
                "expected_date": probe["expected_date"],
                "actual_date": actual,
                "passed": actual == probe["expected_date"],
            })
        failed = [result["id"] for result in results if not result["passed"]]
        if failed:
            raise ValueError(f"时区发布探针失败: {item_id}: {', '.join(failed)}")
    return {
        "release_id": item_id,
        "tzdata_version": release["tzdata_version"],
        "iana_version": release["iana_version"],
        "version_token": version_token(release),
        "archive": str(target),
        "sha256": actual_hash,
        "zones_count": release["zones_count"],
        "probes_passed": len(results),
        "probes_total": len(release["probes"]) if run_probes else 0,
        "probe_results": results,
    }


def validate_contract(path: Path = DEFAULT_MANIFEST, run_probes: bool = True) -> dict:
    manifest = load_manifest(path)
    releases = [
        validate_release_archive(manifest, item_id, run_probes=run_probes)
        for item_id in manifest["releases"]
    ]
    active = next(item for item in releases if item["release_id"] == manifest["active_release_id"])
    return {
        "available": True,
        "active_release_id": manifest["active_release_id"],
        "rollback_release_id": manifest["rollback_release_id"],
        "release_count": len(releases),
        "active": active,
        "releases": releases,
        "manifest_path": manifest["manifest_path"],
    }


def _zip_write(package: zipfile.ZipFile, name: str, payload: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    package.writestr(info, payload)


def build_release_archive(
    source_root: Path,
    zones_file: Path,
    output: Path,
    tzdata_version: str,
    iana_version: str,
) -> dict:
    source = Path(source_root).resolve()
    zones_path = Path(zones_file).resolve()
    tzdata = _clean_version(tzdata_version, "tzdata_version")
    iana = _clean_version(iana_version, "iana_version")
    item_id = release_id(tzdata, iana)
    zones = [line.strip() for line in zones_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not zones or len(zones) != len(set(zones)):
        raise ValueError("zones 文件为空或包含重复区域")
    for zone in zones:
        if not _valid_zone_name(zone) or not (source / Path(*zone.split("/"))).is_file():
            raise ValueError(f"zones 文件引用无效区域: {zone}")
    metadata = {
        "schema_version": 1,
        "release_id": item_id,
        "tzdata_version": tzdata,
        "iana_version": iana,
        "zones_count": len(zones),
    }
    target = Path(output).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    try:
        with zipfile.ZipFile(temporary, "w") as package:
            _zip_write(
                package,
                "release.json",
                (json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"),
            )
            for zone in sorted(zones):
                payload = (source / Path(*zone.split("/"))).read_bytes()
                _zip_write(package, f"zoneinfo/{zone}", payload)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {**metadata, "archive": target.name, "sha256": sha256_file(target)}


def write_manifest_atomic(path: Path, manifest: dict) -> None:
    target = Path(path).resolve()
    serializable = {
        key: value for key, value in manifest.items() if key != "manifest_path"
    }
    validated = validate_manifest(serializable, target)
    payload = {
        key: value for key, value in validated.items() if key != "manifest_path"
    }
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)


def restore_manifest(path: Path, payload: bytes) -> None:
    """Atomically restore a previously read manifest payload."""
    target = Path(path).resolve()
    temporary = target.with_suffix(target.suffix + ".restore.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def switch_active_release(path: Path, target_release_id: str) -> dict:
    manifest = load_manifest(path)
    target_id = str(target_release_id or "").strip()
    if target_id not in manifest["releases"]:
        raise ValueError("目标时区发布不存在")
    current_id = manifest["active_release_id"]
    if target_id == current_id:
        raise ValueError("目标时区发布已经处于激活状态")
    validate_release_archive(manifest, target_id, run_probes=True)
    updated = {
        "schema_version": 1,
        "active_release_id": target_id,
        "rollback_release_id": current_id,
        "releases": manifest["releases"],
    }
    original = Path(path).read_bytes()
    try:
        write_manifest_atomic(path, updated)
        validate_contract(path, run_probes=True)
    except Exception:
        restore_manifest(path, original)
        raise
    return load_manifest(path)
