"""Local JSON storage for DB-Agent model connection profiles.

The public repository contains only an empty example document.  The live file is
created under ``runtime/app`` and is excluded from version control because it may
contain API credentials.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import threading
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests


DOCUMENT_VERSION = 1
PROFILE_FILENAME = "model_profiles.json"
_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,63}$")


class ModelProfileError(ValueError):
    """A local model profile failed validation or could not be found."""


def _as_int(value: Any, *, field: str, default: int, minimum: int, maximum: int) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ModelProfileError(f"{field} must be an integer") from exc
    if not minimum <= number <= maximum:
        raise ModelProfileError(f"{field} must be between {minimum} and {maximum}")
    return number


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _validate_base_url(value: Any) -> str:
    base_url = str(value or "").strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ModelProfileError("baseUrl must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password:
        raise ModelProfileError("baseUrl must not contain embedded credentials")
    return base_url


class ModelProfileStore:
    """Thread-safe CRUD store with atomic replacement of the local JSON file."""

    def __init__(self, app_root: Path):
        self.app_root = Path(app_root).resolve()
        self.path = self.app_root / PROFILE_FILENAME
        self._lock = threading.RLock()

    @property
    def profile_path(self) -> str:
        return str(self.path)

    @staticmethod
    def _empty_document() -> dict:
        return {"version": DOCUMENT_VERSION, "profiles": []}

    def _read_unlocked(self) -> dict:
        if not self.path.exists():
            return self._empty_document()
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ModelProfileError("model profile file is unreadable") from exc
        if not isinstance(document, dict) or document.get("version") != DOCUMENT_VERSION:
            raise ModelProfileError("unsupported model profile document version")
        profiles = document.get("profiles")
        if not isinstance(profiles, list) or any(not isinstance(item, dict) for item in profiles):
            raise ModelProfileError("model profile document is malformed")
        keys = [str(item.get("key") or "") for item in profiles]
        if len(keys) != len(set(keys)) or any(not _KEY_PATTERN.fullmatch(key) for key in keys):
            raise ModelProfileError("model profile keys are invalid or duplicated")
        return {"version": DOCUMENT_VERSION, "profiles": [dict(item) for item in profiles]}

    def _write_unlocked(self, document: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{secrets.token_hex(6)}.tmp")
        payload = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
        try:
            temporary.write_text(payload, encoding="utf-8")
            with contextlib.suppress(OSError):
                os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
        finally:
            if temporary.exists():
                with contextlib.suppress(OSError):
                    temporary.unlink()

    @staticmethod
    def _new_key(existing: set[str]) -> str:
        while True:
            candidate = f"profile-{secrets.token_hex(5)}"
            if candidate not in existing:
                return candidate

    @staticmethod
    def _normalized_profile(
        data: dict,
        *,
        key: str,
        existing: Optional[dict] = None,
        require_api_key: bool,
    ) -> dict:
        previous = dict(existing or {})
        model = str(data.get("model") if "model" in data else previous.get("model") or "").strip()
        name = str(data.get("name") if "name" in data else previous.get("name") or "").strip()
        base_value = data.get("baseUrl") if "baseUrl" in data else previous.get("base_url")
        base_url = _validate_base_url(base_value)
        api_key = str(data.get("apiKey") or "").strip() or str(previous.get("api_key") or "").strip()
        if not model:
            raise ModelProfileError("model is required")
        if require_api_key and not api_key:
            raise ModelProfileError("apiKey is required")
        mode = str(data.get("apiMode") if "apiMode" in data else previous.get("api_mode") or "chat_completions").strip()
        if mode not in {"chat_completions", "responses"}:
            raise ModelProfileError("apiMode must be chat_completions or responses")
        profile = {
            "key": key,
            "name": name,
            "model": model,
            "base_url": base_url,
            "api_key": api_key,
            "api_mode": mode,
            "retry_count": _as_int(
                data.get("retryCount") if "retryCount" in data else previous.get("retry_count"),
                field="retryCount", default=1, minimum=0, maximum=10,
            ),
            "connect_timeout": _as_int(
                data.get("connectTimeout") if "connectTimeout" in data else previous.get("connect_timeout"),
                field="connectTimeout", default=10, minimum=1, maximum=120,
            ),
            "read_timeout": _as_int(
                data.get("readTimeout") if "readTimeout" in data else previous.get("read_timeout"),
                field="readTimeout", default=120, minimum=1, maximum=1800,
            ),
            "total_timeout": _as_int(
                data.get("totalTimeout") if "totalTimeout" in data else previous.get("total_timeout"),
                field="totalTimeout", default=600, minimum=10, maximum=1800,
            ),
            "stream": _as_bool(
                data.get("stream") if "stream" in data else previous.get("stream"), True,
            ),
            "verify_tls": _as_bool(
                data.get("verifyTls") if "verifyTls" in data else previous.get("verify_tls"), True,
            ),
        }
        for field, incoming in (
            ("temperature", "temperature"),
            ("max_tokens", "maxTokens"),
            ("reasoning_effort", "reasoningEffort"),
            ("proxy_url", "proxyUrl"),
        ):
            value = data.get(incoming) if incoming in data else previous.get(field)
            if value not in (None, ""):
                profile[field] = value
        return profile

    @staticmethod
    def _public(profile: dict) -> dict:
        return {
            "id": profile["key"],
            "key": profile["key"],
            "name": profile.get("name") or profile.get("model") or profile["key"],
            "model": profile.get("model", ""),
            "hasKey": bool(str(profile.get("api_key") or "").strip()),
        }

    def list_model_profiles(self) -> list[dict]:
        with self._lock:
            return [self._public(item) for item in self._read_unlocked()["profiles"]]

    def _find_unlocked(self, document: dict, key: str) -> tuple[int, dict]:
        for index, profile in enumerate(document["profiles"]):
            if profile.get("key") == key:
                return index, dict(profile)
        raise ModelProfileError("model profile not found")

    def get_runtime_profile(self, key: str = "default") -> dict:
        with self._lock:
            document = self._read_unlocked()
            if not document["profiles"]:
                raise ModelProfileError("no model profile is configured")
            if key in {"", "default"}:
                return dict(document["profiles"][0])
            return self._find_unlocked(document, key)[1]

    def get_model_profile(self, key: str) -> dict:
        with self._lock:
            profile = self._find_unlocked(self._read_unlocked(), key)[1]
        return {
            **self._public(profile),
            "baseUrl": profile.get("base_url", ""),
            "apiMode": profile.get("api_mode", "chat_completions"),
            "retryCount": profile.get("retry_count", 1),
            "connectTimeout": profile.get("connect_timeout", 10),
            "readTimeout": profile.get("read_timeout", 120),
            "totalTimeout": profile.get("total_timeout", 600),
            "stream": profile.get("stream", True),
            "verifyTls": profile.get("verify_tls", True),
            "keyTail": str(profile.get("api_key") or "")[-4:],
        }

    def add_model_profile(self, data: dict) -> dict:
        with self._lock:
            document = self._read_unlocked()
            key = self._new_key({item["key"] for item in document["profiles"]})
            document["profiles"].append(
                self._normalized_profile(data, key=key, require_api_key=True)
            )
            self._write_unlocked(document)
            return {"profileKey": key, "profiles": [self._public(item) for item in document["profiles"]]}

    def update_model_profile(self, key: str, data: dict) -> dict:
        with self._lock:
            document = self._read_unlocked()
            index, previous = self._find_unlocked(document, key)
            document["profiles"][index] = self._normalized_profile(
                data, key=key, existing=previous, require_api_key=False,
            )
            self._write_unlocked(document)
            return {"profileKey": key, "profiles": [self._public(item) for item in document["profiles"]]}

    def delete_model_profile(self, key: str) -> dict:
        with self._lock:
            document = self._read_unlocked()
            index, _ = self._find_unlocked(document, key)
            del document["profiles"][index]
            self._write_unlocked(document)
            return {"profileKey": key, "profiles": [self._public(item) for item in document["profiles"]]}

    def test_model_profile(self, data: dict) -> dict:
        key = str(data.get("profileKey") or "").strip()
        supplied_base = str(data.get("baseUrl") or "").strip()
        supplied_secret = str(data.get("apiKey") or "").strip()
        model = str(data.get("model") or "").strip()
        if key and not (supplied_base and supplied_secret):
            profile = self.get_runtime_profile(key)
            base_url = profile["base_url"]
            api_key = profile["api_key"]
            model = model or profile["model"]
        else:
            base_url = _validate_base_url(supplied_base)
            api_key = supplied_secret
        if not api_key:
            raise ModelProfileError("apiKey is required")
        try:
            response = requests.get(
                f"{base_url.rstrip('/')}/models",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=(5, 10),
            )
        except requests.RequestException as exc:
            return {"ok": False, "detail": type(exc).__name__}
        if response.status_code >= 400:
            return {"ok": False, "httpStatus": response.status_code, "detail": f"HTTP {response.status_code}"}
        result = {"ok": True, "httpStatus": response.status_code}
        if model:
            try:
                identifiers = {
                    str(item.get("id"))
                    for item in (response.json().get("data") or [])
                    if isinstance(item, dict)
                }
            except (ValueError, AttributeError):
                identifiers = set()
            if identifiers:
                result["hasModel"] = model in identifiers
        return result
