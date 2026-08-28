"""Local role-token derivation and HTTP permission policy for DBQuill."""
from __future__ import annotations

import base64
import hashlib
import hmac
from pathlib import PurePosixPath
from typing import Optional


ROLE_ORDER = {"viewer": 0, "operator": 1, "admin": 2}
ROLE_LABELS = {"viewer": "查看者", "operator": "操作员", "admin": "管理员"}
POLICY_VERSION = "local-rbac-v1"
_STATIC_SUFFIXES = frozenset({
    ".css", ".js", ".png", ".ico", ".svg", ".woff", ".woff2", ".ttf",
    ".html", ".map",
})


def _derived_token(admin_token: str, role: str) -> str:
    digest = hmac.new(
        admin_token.encode("utf-8"),
        f"dbagent:{POLICY_VERSION}:{role}".encode("ascii"),
        hashlib.sha256,
    ).digest()
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    prefix = "vw" if role == "viewer" else "op"
    return f"{prefix}_{encoded}"


def derive_role_tokens(admin_token: str) -> dict[str, str]:
    token = str(admin_token or "").strip()
    if len(token) < 24:
        raise ValueError("管理员 token 长度不足")
    return {
        "viewer": _derived_token(token, "viewer"),
        "operator": _derived_token(token, "operator"),
        "admin": token,
    }


def role_for_token(supplied: str, role_tokens: dict[str, str]) -> Optional[str]:
    supplied = str(supplied or "")
    matched = None
    # Compare every token to avoid role-dependent short-circuit timing.
    for role in ("viewer", "operator", "admin"):
        token = str(role_tokens.get(role) or "")
        equal = bool(supplied and token) and hmac.compare_digest(supplied, token)
        if equal:
            matched = role
    return matched


def role_allows(actual: str, required: str) -> bool:
    return ROLE_ORDER.get(str(actual), -1) >= ROLE_ORDER.get(str(required), 99)


def token_fingerprint(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:16]


def required_role(method: str, path: str) -> str:
    """Return the minimum role for one HTTP method/path without reading a body."""
    method = str(method or "GET").upper()
    path = "/" + str(path or "").lstrip("/")
    if method == "OPTIONS":
        return "viewer"
    if path in {"/status", "/ws", "/db", "/db/auth/context"}:
        return "viewer"
    if path == "/db/auth/credentials" or path.startswith("/db/auth/credentials/"):
        return "admin"
    if path == "/model-profiles" or path.startswith("/model-profiles/"):
        return "admin"
    suffix = PurePosixPath(path).suffix.lower()
    if method in {"GET", "HEAD"} and suffix in _STATIC_SUFFIXES:
        return "viewer"
    if method in {"GET", "HEAD"}:
        if path.startswith("/db/"):
            return "viewer"
        return "admin"

    if method == "POST" and (
        path == "/db/ask"
        or (path.startswith("/db/ask/") and path.endswith("/cancel"))
        or path in {"/db/chart-data", "/db/charts-auto", "/db/charts-cache-status"}
        or (path.startswith("/db/schedules/") and path.endswith("/run"))
    ):
        return "viewer"
    if method == "POST" and path == "/upload":
        return "operator"
    if method == "POST" and (
        path == "/db/audit/backups"
        or path == "/db/audit/reconciliation/resolve"
        or path == "/db/write/prepare-create-table"
    ):
        return "admin"
    if method == "DELETE" and path.startswith("/db/databases/"):
        return "admin"
    if path.startswith("/db/"):
        return "operator"
    return "admin"


def capabilities(role: str) -> dict[str, bool]:
    return {
        "read": role_allows(role, "viewer"),
        "manage_workspace": role_allows(role, "operator"),
        "manage_memory": role_allows(role, "operator"),
        "approve_bounded_write": role_allows(role, "operator"),
        "approve_high_risk_write": role_allows(role, "admin"),
        "detach_database": role_allows(role, "admin"),
        "create_audit_backup": role_allows(role, "admin"),
        "resolve_audit_pending": role_allows(role, "admin"),
        "manage_credentials": role_allows(role, "admin"),
    }
