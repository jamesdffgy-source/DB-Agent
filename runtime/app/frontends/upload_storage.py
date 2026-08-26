"""Bounded local storage for files uploaded through the desktop API."""
from __future__ import annotations

import hashlib
import os
import re
import secrets
import time
from pathlib import Path


class UploadStorageError(ValueError):
    pass


class UploadStorage:
    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _display_name(raw_name: str) -> str:
        leaf = str(raw_name or "file").replace("\\", "/").rsplit("/", 1)[-1].strip()
        leaf = re.sub(r"[\x00-\x1f<>:\"|?*]", "_", leaf).strip(" .")
        return (leaf or "file")[:180]

    @staticmethod
    def _bucket_name(session_id: str) -> str:
        value = str(session_id or "").strip()
        if not value:
            return "unassigned"
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]

    def store(self, session_id: str, original_name: str, content: bytes) -> tuple[Path, str]:
        if not content:
            raise UploadStorageError("empty file")
        safe_name = self._display_name(original_name)
        bucket = self.root / self._bucket_name(session_id)
        bucket.mkdir(parents=True, exist_ok=True)
        destination = bucket / f"{secrets.token_hex(8)}-{safe_name}"
        destination.write_bytes(content)
        return destination.resolve(), safe_name

    def sweep(self, retention_days: int = 30) -> dict[str, int]:
        cutoff = time.time() - max(1, int(retention_days)) * 86_400
        removed_files = 0
        removed_directories = 0
        for bucket in list(self.root.iterdir()):
            if not bucket.is_dir():
                continue
            for entry in list(bucket.iterdir()):
                try:
                    if entry.is_file() and entry.stat().st_mtime < cutoff:
                        entry.unlink()
                        removed_files += 1
                except OSError:
                    continue
            try:
                if not any(bucket.iterdir()):
                    os.rmdir(bucket)
                    removed_directories += 1
            except OSError:
                continue
        return {"files": removed_files, "directories": removed_directories}
