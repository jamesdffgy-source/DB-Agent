#!/usr/bin/env python3
"""DBQuill 本地 HTTP/WS 桥接。

这里只承载当前产品主链：本地鉴权、模型配置、数据源、NL-to-Database、
写入确认、会话、图表、语义目录、审计与调度。WebSocket 仅用于桥接存活通知，
业务命令和数据都经过 HTTP API。
"""
from __future__ import annotations

import asyncio, contextlib, contextvars, hashlib, hmac, json, math, os, re, secrets, sys
import threading, time, traceback, uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from aiohttp import web, WSMsgType
import sqlite3

try:
    import db_sessions_store as _sess_store
    _sess_store.init_db()
except Exception:
    _sess_store = None
try:
    import db_scheduler as _db_sched
except Exception:
    _db_sched = None
try:
    import db_semantic_store as _semantic_store
    _semantic_store.init_db()
except Exception:
    _semantic_store = None
try:
    import db_audit_store as _audit_store
    _audit_store.init_db()
except Exception:
    _audit_store = None
try:
    import db_identity_store as _identity_store
    _identity_store.init_db()
except Exception:
    _identity_store = None
try:
    import db_chart_cache as _chart_cache
    _chart_cache.init_db()
except Exception:
    _chart_cache = None
import db_access_control as _access_control
from model_profiles import ModelProfileStore
from upload_storage import UploadStorage, UploadStorageError

APP_DIR = Path(__file__).resolve().parent


def find_app_root() -> Path:
    """Return the fixed DBQuill runtime directory (``runtime/app``)."""
    return APP_DIR.parent.resolve()


DEFAULT_APP_ROOT = find_app_root()


def _load_bridge_token() -> str:
    """Return the persistent secret used to authenticate the loopback HTTP API."""
    configured = str(os.environ.get("BRIDGE_TOKEN") or "").strip()
    if configured:
        return configured
    token_file = DEFAULT_APP_ROOT / "temp" / "bridge.token"
    token_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with token_file.open("x", encoding="ascii") as f:
            token = secrets.token_urlsafe(32)
            f.write(token)
            return token
    except FileExistsError:
        token = token_file.read_text(encoding="ascii").strip()
        if token:
            return token
        token = secrets.token_urlsafe(32)
        token_file.write_text(token, encoding="ascii")
        return token


BRIDGE_TOKEN = _load_bridge_token()
_AUTH_COOKIE = "dbquill_bridge_token"
_LEGACY_AUTH_COOKIE = "dbagent_bridge_token"
_ROLE_TOKENS = _access_control.derive_role_tokens(BRIDGE_TOKEN)
_REQUEST_ROLE = contextvars.ContextVar("dbquill_request_role", default="admin")
_REQUEST_PRINCIPAL = contextvars.ContextVar("dbquill_request_principal", default=None)


def _current_role() -> str:
    role = str(_REQUEST_ROLE.get() or "admin")
    return role if role in _access_control.ROLE_ORDER else "admin"


def _current_principal() -> dict:
    principal = _REQUEST_PRINCIPAL.get()
    if isinstance(principal, dict):
        return principal
    role = _current_role()
    return {
        "kind": "shared_role",
        "role": role,
        "label": _access_control.ROLE_LABELS[role],
        "credentialRef": None,
        "expiresAt": None,
        "databaseScope": {
            "mode": "all", "databaseRefs": [], "tableScopes": {}, "columnScopes": {},
            "rowScopes": {},
        },
    }


def _principal_for_token(supplied: str) -> Optional[dict]:
    role = _access_control.role_for_token(supplied, _ROLE_TOKENS)
    if role is not None:
        return {
            "kind": "shared_role",
            "role": role,
            "label": _access_control.ROLE_LABELS[role],
            "credentialRef": None,
            "expiresAt": None,
            "databaseScope": {
                "mode": "all", "databaseRefs": [], "tableScopes": {}, "columnScopes": {},
                "rowScopes": {},
            },
        }
    if _identity_store is None:
        return None
    credential = _identity_store.authenticate(supplied)
    if not credential:
        return None
    return {
        "kind": "credential",
        "role": credential["role"],
        "label": credential["label"],
        "credentialRef": credential["credentialRef"],
        "expiresAt": credential["expiresAt"],
        "databaseScope": credential.get("databaseScope") or {
            "mode": "all", "databaseRefs": [], "tableScopes": {}, "columnScopes": {},
            "rowScopes": {},
        },
    }


def _current_database_scope() -> dict:
    principal = _current_principal()
    raw = principal.get("databaseScope")
    if principal.get("kind") != "credential" or not isinstance(raw, dict):
        return {
            "mode": "all", "databaseRefs": [], "tableScopes": {}, "columnScopes": {},
            "rowScopes": {},
        }
    mode = str(raw.get("mode") or "all")
    refs = raw.get("databaseRefs")
    table_scopes = raw.get("tableScopes")
    column_scopes = raw.get("columnScopes")
    row_scopes = raw.get("rowScopes")
    return {
        "mode": "restricted" if mode == "restricted" else "all",
        "databaseRefs": [str(ref) for ref in refs] if isinstance(refs, list) else [],
        "tableScopes": {
            str(ref): [str(table) for table in tables]
            for ref, tables in table_scopes.items()
            if isinstance(tables, list)
        } if isinstance(table_scopes, dict) else {},
        "columnScopes": {
            str(ref): {
                str(table): [str(column) for column in columns]
                for table, columns in scoped_tables.items()
                if isinstance(columns, list)
            }
            for ref, scoped_tables in column_scopes.items()
            if isinstance(scoped_tables, dict)
        } if isinstance(column_scopes, dict) else {},
        "rowScopes": {
            str(ref): {
                str(table): [dict(item) for item in filters if isinstance(item, dict)]
                for table, filters in scoped_tables.items()
                if isinstance(filters, list)
            }
            for ref, scoped_tables in row_scopes.items()
            if isinstance(scoped_tables, dict)
        } if isinstance(row_scopes, dict) else {},
    }


def _database_scope_is_restricted() -> bool:
    return _current_database_scope()["mode"] == "restricted"


def _current_audit_actor() -> str:
    return {
        "viewer": "local_viewer",
        "operator": "local_operator",
        "admin": "local_admin",
    }[_current_role()]

for _s in (sys.stdout, sys.stderr):
    with contextlib.suppress(Exception):
        _s.reconfigure(encoding="utf-8", errors="replace")


manager = ModelProfileStore(DEFAULT_APP_ROOT)


# ---------------------------------------------------------------------------
# Transport layer: WS liveness channel
# ---------------------------------------------------------------------------

_WEBSOCKETS: Set[web.WebSocketResponse] = set()


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    _WEBSOCKETS.add(ws)
    await ws.send_str(json.dumps({
        "type": "bridge-ready",
        "appRoot": str(manager.app_root),
        "http": True,
        "wsEventsOnly": True,
    }, ensure_ascii=False))
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            with contextlib.suppress(Exception):
                data = json.loads(msg.data)
                if data.get("action") == "ping":
                    await ws.send_str(json.dumps(
                        {"type": "pong", "ts": time.time()},
                        ensure_ascii=False,
                    ))
    _WEBSOCKETS.discard(ws)
    return ws
# ---------------------------------------------------------------------------
# Transport layer: HTTP command/data API
# ---------------------------------------------------------------------------

def cors_headers():
    # The desktop UI is same-origin. Deliberately emit no CORS headers: a web
    # page from another origin must never be able to read this local API.
    return {}


def _request_token(request) -> str:
    return str(
        request.headers.get("X-DBQuill-Token")
        or request.headers.get("X-DBAgent-Token")
        or request.query.get("token")
        or request.cookies.get(_AUTH_COOKIE)
        or request.cookies.get(_LEGACY_AUTH_COOKIE)
        or ""
    )


def _origin_allowed(request) -> bool:
    origin = str(request.headers.get("Origin") or "").rstrip("/")
    if not origin:
        return True
    return hmac.compare_digest(origin, f"{request.scheme}://{request.host}")


def _set_auth_cookie(resp: web.StreamResponse, token: str) -> None:
    resp.set_cookie(
        _AUTH_COOKIE,
        token,
        httponly=True,
        samesite="Strict",
        path="/",
        max_age=365 * 24 * 60 * 60,
    )


def _set_role_headers(resp: web.StreamResponse, role: str) -> None:
    resp.headers["X-DBQuill-Role"] = role
    # Response compatibility for local v0.1 integrations.
    resp.headers["X-DBAgent-Role"] = role


@web.middleware
async def cors_middleware(request, handler):
    supplied = _request_token(request)
    principal = _principal_for_token(supplied)
    if principal is None:
        response = web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        if request.cookies.get(_AUTH_COOKIE):
            response.del_cookie(_AUTH_COOKIE, path="/")
        return response
    role = str(principal["role"])
    if not _origin_allowed(request):
        return web.json_response({"ok": False, "error": "cross-origin request denied"}, status=403)
    role_context = _REQUEST_ROLE.set(role)
    principal_context = _REQUEST_PRINCIPAL.set(principal)
    try:
        required_role = _access_control.required_role(request.method, request.path)
        if not _access_control.role_allows(role, required_role):
            try:
                _audit_append(
                    "",
                    category="access_control", action="deny", outcome="rejected",
                    summary="本地角色权限拒绝请求", risk="medium",
                    actor=_current_audit_actor(),
                    details={
                        "required_role": required_role,
                        "request_role": role,
                        "route_ref": _audit_ref(request.path),
                        "http_method": request.method.upper(),
                    },
                )
            except Exception:
                pass
            return web.json_response({
                "ok": False,
                "error": "forbidden: insufficient local role",
                "role": role,
                "requiredRole": required_role,
            }, status=403)
        if _database_scope_is_restricted() and (
            request.path == "/db/auth/credentials"
            or request.path.startswith("/db/auth/credentials/")
            or request.path == "/db/audit/backups"
        ):
            _audit_database_scope_denial(request, "", global_operation=True)
            return web.json_response({
                "ok": False,
                "error": "forbidden: global database scope required",
                "databaseScope": "restricted",
            }, status=403)
        denied_db_id = await _request_database_scope_denial(request)
        if denied_db_id is not None:
            _audit_database_scope_denial(request, denied_db_id)
            # Do not disclose whether an out-of-scope database, run, session,
            # or schedule identifier exists.
            return web.json_response({
                "ok": False,
                "error": "database not attached or not available",
            }, status=404)
        # The launcher bootstraps the HttpOnly cookie once through ?token=.
        # Redirect immediately so the secret does not remain in history/referrers.
        if request.query.get("token") and request.cookies.get(_AUTH_COOKIE) != supplied:
            clean_query = [(k, v) for k, v in request.query.items() if k != "token"]
            resp = web.Response(
                status=302,
                headers={
                    "Location": str(request.rel_url.with_query(clean_query)),
                    "Referrer-Policy": "no-referrer",
                },
            )
            _set_auth_cookie(resp, supplied)
            _set_role_headers(resp, role)
            return resp
        if request.method == "OPTIONS":
            resp = web.Response(status=204)
            _set_role_headers(resp, role)
            return resp
        resp = await handler(request)
        if request.cookies.get(_AUTH_COOKIE) != supplied:
            _set_auth_cookie(resp, supplied)
        _set_role_headers(resp, role)
        return resp
    finally:
        _REQUEST_PRINCIPAL.reset(principal_context)
        _REQUEST_ROLE.reset(role_context)


def json_ok(data: dict, status: int = 200):
    return web.json_response(data, status=status, headers=cors_headers(), dumps=lambda x: json.dumps(x, ensure_ascii=False, default=str))


async def read_json(request) -> dict:
    # Database-scope middleware may have parsed JSON first. aiohttp retains the
    # cached bytes even though can_read_body becomes false after that read.
    if request.can_read_body or getattr(request, "_read_bytes", None) is not None:
        try:
            data = await request.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


async def status_handler(request):
    role = _current_role()
    principal = _current_principal()
    database_scope = _database_scope_summary()
    payload = {
        "ok": True,
        "running": True,
        "ready": True,
        "authRequired": True,
        "ws": "/ws",
        "transport": {"http": True, "wsLivenessOnly": True},
        "access": {
            "policyVersion": _access_control.POLICY_VERSION,
            "role": role,
            "label": _access_control.ROLE_LABELS[role],
            "principalKind": principal["kind"],
            "credentialRef": principal.get("credentialRef"),
            "expiresAt": principal.get("expiresAt"),
            "databaseScope": database_scope,
        },
    }
    if role == "admin":
        payload["appRoot"] = str(manager.app_root)
        payload["profilePath"] = manager.profile_path
    return json_ok(payload)


async def db_auth_context_handler(request):
    role = _current_role()
    principal = _current_principal()
    capabilities = _access_control.capabilities(role)
    database_scope = _database_scope_summary()
    if database_scope["mode"] == "restricted":
        capabilities["manage_credentials"] = False
        capabilities["create_audit_backup"] = False
    if database_scope.get("rowScopeFilterCount", 0):
        capabilities["approve_bounded_write"] = False
        capabilities["approve_high_risk_write"] = False
    return json_ok({
        "ok": True,
        "access": {
            "policyVersion": _access_control.POLICY_VERSION,
            "role": role,
            "label": _access_control.ROLE_LABELS[role],
            "capabilities": capabilities,
            "databaseScope": database_scope,
            "tokenFingerprint": _access_control.token_fingerprint(_request_token(request)),
            "principal": {
                "kind": principal["kind"],
                "label": principal["label"],
                "credentialRef": principal.get("credentialRef"),
                "expiresAt": principal.get("expiresAt"),
                "databaseScope": database_scope,
            },
        },
    })


async def db_auth_credentials_handler(request):
    if _identity_store is None:
        return json_ok({"ok": False, "error": "本地凭据存储不可用"}, 503)
    if request.method == "GET":
        return json_ok({"ok": True, "credentials": _identity_store.list_credentials()})

    body = await read_json(request)
    role = str(body.get("role") or "").strip().lower()
    if role not in _access_control.ROLE_ORDER:
        return json_ok({"ok": False, "error": "role 必须是 viewer、operator 或 admin"}, 400)
    ttl_value = body.get("ttlHours", _identity_store.DEFAULT_TTL_HOURS)
    if isinstance(ttl_value, bool):
        return json_ok({"ok": False, "error": "ttlHours 必须是整数"}, 400)
    try:
        ttl_hours = int(ttl_value)
    except (TypeError, ValueError):
        return json_ok({"ok": False, "error": "ttlHours 必须是整数"}, 400)
    if not _identity_store.MIN_TTL_HOURS <= ttl_hours <= _identity_store.MAX_TTL_HOURS:
        return json_ok({
            "ok": False,
            "error": (
                f"ttlHours 必须在 {_identity_store.MIN_TTL_HOURS}–"
                f"{_identity_store.MAX_TTL_HOURS} 之间"
            ),
        }, 400)
    raw_scope = body.get("databaseScope")
    if not isinstance(raw_scope, dict):
        return json_ok({
            "ok": False,
            "error": "databaseScope 必须明确指定 all 或 restricted 范围",
        }, 400)
    try:
        normalized_scope = _identity_store.validate_database_scope(
            raw_scope.get("mode"), raw_scope.get("databaseRefs"),
            raw_scope.get("tableScopes"),
            raw_scope.get("columnScopes"),
            raw_scope.get("rowScopes"),
        )
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, 400)
    scope_mode = normalized_scope["mode"]
    database_refs = normalized_scope["databaseRefs"]
    table_scopes = normalized_scope["tableScopes"]
    column_scopes = normalized_scope["columnScopes"]
    row_scopes = normalized_scope["rowScopes"]
    if scope_mode == "restricted":
        entries_by_ref = _known_database_entries_by_ref()
        unknown_refs = sorted(set(database_refs) - set(entries_by_ref))
        if unknown_refs:
            return json_ok({
                "ok": False,
                "error": "databaseRefs 包含当前未接入的数据库引用",
            }, 400)
        normalized_table_scopes = {}
        for database_ref, requested_tables in table_scopes.items():
            database_entry = entries_by_ref[database_ref]
            if database_entry.get("conn"):
                return json_ok({
                    "ok": False,
                    "error": "表级授权目前仅支持本地 SQLite 数据库",
                }, 400)
            actual_by_name = {
                str(table).casefold(): str(table)
                for table in database_entry.get("tables") or []
            }
            if any(str(table).casefold() not in actual_by_name for table in requested_tables):
                return json_ok({
                    "ok": False,
                    "error": "tableScopes 包含当前数据库中不存在的表",
                }, 400)
            normalized_table_scopes[database_ref] = [
                actual_by_name[str(table).casefold()] for table in requested_tables
            ]
        table_scopes = normalized_table_scopes
        normalized_column_scopes = {}
        for database_ref, requested_tables in column_scopes.items():
            database_entry = entries_by_ref[database_ref]
            path = str(database_entry.get("path") or "")
            normalized_for_database = {}
            for requested_table, requested_columns in requested_tables.items():
                actual_table = next(
                    table for table in table_scopes[database_ref]
                    if table.casefold() == str(requested_table).casefold()
                )
                try:
                    physical_columns = _db_table_columns(path, actual_table)
                except Exception:
                    return json_ok({
                        "ok": False,
                        "error": "无法读取当前表结构，字段级凭据未发行",
                    }, 400)
                actual_by_name = {
                    str(column).casefold(): str(column)
                    for column in physical_columns
                }
                if any(
                    str(column).casefold() not in actual_by_name
                    for column in requested_columns
                ):
                    return json_ok({
                        "ok": False,
                        "error": "columnScopes 包含当前表中不存在的字段",
                    }, 400)
                normalized_for_database[actual_table] = [
                    actual_by_name[str(column).casefold()]
                    for column in requested_columns
                ]
            normalized_column_scopes[database_ref] = normalized_for_database
        column_scopes = normalized_column_scopes
        normalized_row_scopes = {}
        for database_ref, requested_tables in row_scopes.items():
            database_entry = entries_by_ref[database_ref]
            path = str(database_entry.get("path") or "")
            normalized_for_database = {}
            for requested_table, requested_filters in requested_tables.items():
                actual_table = next(
                    table for table in table_scopes[database_ref]
                    if table.casefold() == str(requested_table).casefold()
                )
                try:
                    physical_columns = _db_table_columns(path, actual_table)
                except Exception:
                    return json_ok({
                        "ok": False,
                        "error": "无法读取当前表结构，行级凭据未发行",
                    }, 400)
                actual_by_name = {
                    str(column).casefold(): str(column) for column in physical_columns
                }
                normalized_filters = []
                for item in requested_filters:
                    actual_column = actual_by_name.get(str(item["column"]).casefold())
                    if actual_column is None:
                        return json_ok({
                            "ok": False,
                            "error": "rowScopes 包含当前表中不存在的字段",
                        }, 400)
                    normalized_filters.append({**item, "column": actual_column})
                normalized_for_database[actual_table] = normalized_filters
            normalized_row_scopes[database_ref] = normalized_for_database
        row_scopes = normalized_row_scopes
    correlation_id = f"credential-issue-{uuid.uuid4().hex[:16]}"
    details = {
        "request_role": _current_role(),
        "credential_role": role,
        "expires_in_hours": ttl_hours,
        "database_scope_mode": scope_mode,
        "database_scope_count": len(database_refs),
        "table_scope_database_count": len(table_scopes),
        "table_scope_table_count": sum(len(tables) for tables in table_scopes.values()),
        "column_scope_table_count": sum(len(tables) for tables in column_scopes.values()),
        "column_scope_column_count": sum(
            len(columns)
            for tables in column_scopes.values()
            for columns in tables.values()
        ),
        "row_scope_table_count": sum(len(tables) for tables in row_scopes.values()),
        "row_scope_filter_count": sum(
            len(filters)
            for tables in row_scopes.values()
            for filters in tables.values()
        ),
    }
    try:
        _audit_append(
            "", category="access_control", action="credential_issue",
            outcome="approved", summary="管理员批准发行本地凭据", risk="high",
            correlation_id=correlation_id, details=details, strict=True,
        )
        credential = _identity_store.issue_credential(
            label=body.get("label"), role=role, ttl_hours=ttl_hours,
            scope_mode=scope_mode, database_refs=database_refs,
            table_scopes=table_scopes,
            column_scopes=column_scopes,
            row_scopes=row_scopes,
        )
    except (AuditGateError, ValueError) as exc:
        _audit_append(
            "", category="access_control", action="credential_issue",
            outcome="failed", summary="本地凭据发行失败", risk="high",
            correlation_id=correlation_id,
            details={**details, "error_type": type(exc).__name__},
        )
        status = 503 if isinstance(exc, AuditGateError) else 400
        return json_ok({"ok": False, "error": str(exc)}, status)
    _audit_append(
        "", category="access_control", action="credential_issue",
        outcome="succeeded", summary="本地凭据已发行", risk="high",
        correlation_id=correlation_id,
        details={**details, "credential_ref": credential["credentialRef"]},
    )
    return json_ok({
        "ok": True,
        "warning": "token 仅在本次响应显示；请安全保存。",
        "credential": credential,
    }, 201)


async def db_auth_credential_revoke_handler(request):
    if _identity_store is None:
        return json_ok({"ok": False, "error": "本地凭据存储不可用"}, 503)
    credential_id = str(request.match_info.get("credential_id") or "").strip()
    current = _identity_store.get_credential(credential_id)
    if current is None:
        return json_ok({"ok": False, "error": "凭据不存在"}, 404)
    correlation_id = f"credential-revoke-{uuid.uuid4().hex[:16]}"
    details = {
        "request_role": _current_role(),
        "credential_role": current["role"],
        "credential_ref": current["credentialRef"],
    }
    try:
        _audit_append(
            "", category="access_control", action="credential_revoke",
            outcome="approved", summary="管理员批准吊销本地凭据", risk="high",
            correlation_id=correlation_id, details=details, strict=True,
        )
        credential = _identity_store.revoke_credential(credential_id)
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, 503)
    except (ValueError, KeyError) as exc:
        _audit_append(
            "", category="access_control", action="credential_revoke",
            outcome="failed", summary="本地凭据吊销失败", risk="high",
            correlation_id=correlation_id,
            details={**details, "error_type": type(exc).__name__},
        )
        return json_ok({"ok": False, "error": str(exc)}, 409)
    _audit_append(
        "", category="access_control", action="credential_revoke",
        outcome="succeeded", summary="本地凭据已吊销", risk="high",
        correlation_id=correlation_id, details=details,
    )
    return json_ok({"ok": True, "credential": credential})


async def model_profiles_handler(request):
    try:
        pid = request.match_info.get("id")
        if pid is not None:
            if request.method == "GET":
                return json_ok({"profile": manager.get_model_profile(pid)})
            if request.method == "PUT":
                return json_ok({"ok": True, **manager.update_model_profile(pid, await read_json(request))})
            if request.method == "DELETE":
                return json_ok({"ok": True, **manager.delete_model_profile(pid)})
            return json_ok({"ok": False, "error": "method not allowed"}, status=405)
        if request.method == "POST":
            return json_ok({"ok": True, **manager.add_model_profile(await read_json(request))})
        return json_ok({"profiles": manager.list_model_profiles()})
    except ValueError as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=500)


async def model_profile_test_handler(request):
    """POST /model-profiles/test：模型配置在线连通性探测（admin）。"""
    try:
        return json_ok({"ok": True, **manager.test_model_profile(await read_json(request))})
    except ValueError as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=500)


_uploads = UploadStorage(Path(DEFAULT_APP_ROOT) / "temp" / "desktop_uploads")
with contextlib.suppress(OSError):
    _uploads.sweep(30)

_UPLOAD_MAX_BYTES = 200 * 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024


async def _receive_multipart_upload(request):
    """Stream one multipart file to a temporary path, then publish atomically."""
    if not request.content_type.startswith("multipart/"):
        raise web.HTTPUnsupportedMediaType(
            text=json.dumps({"ok": False, "error": "multipart/form-data upload required"}),
            content_type="application/json",
        )

    reader = await request.multipart()
    session_id = str(request.query.get("sid") or "")
    received = None

    while True:
        part = await reader.next()
        if part is None:
            break
        if part.name == "sid" and received is None:
            session_id = (await part.text())[:512]
            continue
        if part.name != "file":
            while await part.read_chunk(_UPLOAD_CHUNK_BYTES):
                pass
            continue
        if received is not None:
            raise UploadStorageError("only one file may be uploaded at a time")

        original_name = str(part.filename or "file")
        if Path(original_name).suffix.lower() == ".xls":
            raise web.HTTPUnsupportedMediaType(
                text=json.dumps(
                    {
                        "ok": False,
                        "error": "旧版 Excel .xls 暂不支持，请先转换为 .xlsx 后再上传。",
                    },
                    ensure_ascii=False,
                ),
                content_type="application/json",
            )

        destination, staging, safe_name = _uploads.allocate(session_id, original_name)
        size = 0
        try:
            with staging.open("xb") as target:
                while True:
                    chunk = await part.read_chunk(_UPLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > _UPLOAD_MAX_BYTES:
                        raise UploadStorageError("file too large (>200MB)")
                    target.write(chunk)
            if size == 0:
                raise UploadStorageError("empty file")
            _uploads.commit(staging, destination)
        except Exception:
            _uploads.discard(staging)
            raise
        received = (destination, safe_name, original_name)

    if received is None:
        raise UploadStorageError("missing file")
    return received


async def upload_handler(request):
    """Stream a multipart upload to local storage and return its absolute path.

    Files are grouped per session under desktop_uploads/<sid>/ so deleting a
    session can purge its attachments. Missing sid falls back to a _misc bucket.
    Returns: {ok: true, path: "<abs path>"}
    """
    try:
        fpath, safe_name, original_name = await _receive_multipart_upload(request)
    except web.HTTPRequestEntityTooLarge:
        return json_ok({"ok": False, "error": "file too large (>200MB)"}, status=413)
    except web.HTTPException:
        raise
    except UploadStorageError as exc:
        status = 413 if "too large" in str(exc).lower() else 400
        return json_ok({"ok": False, "error": str(exc)}, status=status)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"upload failed: {exc}"}, status=400)

    ext = Path(original_name).suffix.lower()
    # 上传的是 sqlite → 自动 attach 到 /db/*（供 DBQuill 直接对话）；csv → 先转 sqlite 再 attach
    auto_db = None
    if ext == ".csv":
        try:
            tbl_name = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff]+", "_", Path(safe_name).stem).strip("_") or "csv_data"
            db_path = str(fpath.with_suffix(".db"))
            tables = _csv_to_sqlite(str(fpath), db_path, tbl_name)
            db_id = uuid.uuid4().hex[:12]
            entry = {
                "id": db_id, "name": safe_name, "path": db_path,
                "tables": tables, "preset": False, "source": "upload-csv",
                "kind": "csv", "attachedAt": time.time(),
            }
            if _register_database_entry(entry):
                auto_db = _db_view(db_id)
            else:
                globals()["_LAST_UPLOAD_ERR"] = "当前凭据未授权接入上传的数据源"
        except Exception as exc:
            auto_db = None  # 转换失败则静默跳过，保持原上传行为；错误经 warning 透出
            import traceback as _tb
            _tb.print_exc()
            globals()["_LAST_UPLOAD_ERR"] = "%s: %s" % (type(exc).__name__, exc)
    elif ext == ".xlsx":
        try:
            db_path = str(fpath.with_suffix(".db"))
            tables = _excel_to_sqlite(str(fpath), db_path)
            db_id = uuid.uuid4().hex[:12]
            entry = {
                "id": db_id, "name": safe_name, "path": db_path,
                "tables": tables, "preset": False, "source": "upload-excel",
                "kind": "excel", "attachedAt": time.time(),
            }
            if _register_database_entry(entry):
                auto_db = _db_view(db_id)
            else:
                globals()["_LAST_UPLOAD_ERR"] = "当前凭据未授权接入上传的数据源"
        except Exception as exc:
            auto_db = None  # 转换失败则静默跳过，保持原上传行为；错误经 warning 透出
            import traceback as _tb
            _tb.print_exc()
            globals()["_LAST_UPLOAD_ERR"] = "%s: %s" % (type(exc).__name__, exc)
    elif ext in (".db", ".sqlite", ".sqlite3"):
        try:
            tables = _db_validate_and_summarize(str(fpath))
            db_id = uuid.uuid4().hex[:12]
            entry = {
                "id": db_id, "name": safe_name, "path": str(fpath.resolve()),
                "tables": tables, "preset": False, "source": "upload",
                "kind": "sqlite", "attachedAt": time.time(),
            }
            if _register_database_entry(entry):
                auto_db = _db_view(db_id)
            else:
                globals()["_LAST_UPLOAD_ERR"] = "当前凭据未授权接入上传的数据源"
        except Exception:
            auto_db = None  # 非合法 sqlite 则静默跳过，保持原上传行为
    resp = {"ok": True, "path": str(fpath), "db": auto_db}
    if auto_db is None and ext in (".csv", ".xlsx", ".db", ".sqlite", ".sqlite3"):
        resp["warning"] = _LAST_UPLOAD_ERR if "_LAST_UPLOAD_ERR" in globals() else "csv conversion failed (no error captured)"
    return json_ok(resp)


# DBQuill —— /db/* 的 NL-to-Database 入口
# 依赖 frontends/dbquill_core.py（规划、查询、检索、写入安全与执行核心）
# ---------------------------------------------------------------------------

_DB_AGENT_DBS: Dict[str, dict] = {}   # dbId -> {id,name,path,tables,attachedAt}
_DB_AGENT_CACHE: Dict[str, Any] = {}  # dbId -> DBQuillAgent 实例（懒加载）
_DB_RUNS: Dict[str, dict] = {}        # runId -> 进度/结果
_DB_RUN_CANCEL_EVENTS: Dict[str, threading.Event] = {}
_DB_RUNS_MAX = 200
_SEMANTIC_IMPORTS: Dict[str, dict] = {}
_SEMANTIC_IMPORTS_LOCK = threading.RLock()
_SEMANTIC_IMPORT_TTL_SECONDS = 5 * 60
_SEMANTIC_IMPORT_MAX_PENDING = 100
_SEMANTIC_IMPORT_MAX_BYTES = 512 * 1024
_SEMANTIC_IMPORT_REQUEST_MAX_BYTES = 640 * 1024
_SEMANTIC_EXPORT_FORMAT = "dbquill-semantic-catalog"
_SEMANTIC_IMPORT_FORMATS = frozenset({_SEMANTIC_EXPORT_FORMAT, "dbagent-semantic-catalog"})
_SEMANTIC_EXPORT_SCHEMA_VERSION = 8
_SEMANTIC_IMPORT_SCHEMA_VERSIONS = frozenset({1, 2, 3, 4, 5, 6, 7, 8})


def _db_agent_core():
    """懒加载 dbquill_core（同目录），失败时抛清晰错误而不是弄崩 bridge。"""
    import sys as _sys
    from pathlib import Path as _Path
    here = str(_Path(__file__).resolve().parent)
    if here not in _sys.path:
        _sys.path.insert(0, here)
    try:
        import dbquill_core
    except Exception as exc:
        raise RuntimeError(f"dbquill_core 加载失败: {exc}") from exc
    return dbquill_core


def _default_model_profile() -> str:
    """Prefer the DBQuill setting while accepting the v0.1 environment name."""
    return str(
        os.environ.get("DBQUILL_MODEL_PROFILE")
        or os.environ.get("DBAGENT_MODEL_PROFILE")
        or "default"
    ).strip() or "default"


def _db_semantic_key(entry: dict) -> str:
    """返回不含密码的稳定数据源标识，使同一数据库重新接入后仍能恢复语义目录。"""
    if entry.get("conn"):
        cfg = entry["conn"]
        return "remote:{dialect}:{host}:{port}:{database}".format(
            dialect=str(cfg.get("dialect") or "").lower(),
            host=str(cfg.get("host") or "").lower(),
            port=str(cfg.get("port") or ""),
            database=str(cfg.get("database") or "").lower(),
        )
    path = str(entry.get("path") or "").strip()
    return f"sqlite:{os.path.normcase(os.path.abspath(path))}"


def _database_scope_ref(entry: dict) -> str:
    """Stable, non-reversible database identifier used by credential scopes."""
    key = _db_semantic_key(entry)
    return hashlib.sha256(
        f"dbagent-database-scope-v1:{key}".encode("utf-8")
    ).hexdigest()


def _database_entry_allowed(entry: Optional[dict]) -> bool:
    if not entry:
        return False
    scope = _current_database_scope()
    if scope["mode"] != "restricted":
        return True
    return _database_scope_ref(entry) in set(scope["databaseRefs"])


def _db_entry_unchecked(db_id: str) -> Optional[dict]:
    return _DB_AGENT_DBS.get(str(db_id or ""))


def _database_id_allowed(db_id: str) -> bool:
    if not _database_scope_is_restricted():
        return True
    return _database_entry_allowed(_db_entry_unchecked(db_id))


def _all_database_entries_unchecked() -> list[dict]:
    return list(_DB_AGENT_DBS.values())


def _known_database_refs() -> set[str]:
    return {_database_scope_ref(entry) for entry in _all_database_entries_unchecked()}


def _known_database_entries_by_ref() -> dict[str, dict]:
    return {
        _database_scope_ref(entry): entry
        for entry in _all_database_entries_unchecked()
    }


def _table_scope_for_entry(entry: Optional[dict]) -> Optional[frozenset[str]]:
    """Return case-folded allowed tables, or None when all tables are allowed."""
    if not entry:
        return frozenset()
    scope = _current_database_scope()
    if scope["mode"] != "restricted":
        return None
    tables = scope["tableScopes"].get(_database_scope_ref(entry))
    if tables is None:
        return None
    return frozenset(str(table).casefold() for table in tables)


def _table_allowed(entry: Optional[dict], table: Any) -> bool:
    allowed = _table_scope_for_entry(entry)
    return allowed is None or str(table or "").casefold() in allowed


def _column_scopes_for_entry(entry: Optional[dict]) -> dict[str, tuple[str, ...]]:
    """Return stored column scopes for one database; missing tables mean all columns."""
    if not entry:
        return {}
    scope = _current_database_scope()
    if scope["mode"] != "restricted":
        return {}
    raw = scope["columnScopes"].get(_database_scope_ref(entry))
    if not isinstance(raw, dict):
        return {}
    return {
        str(table): tuple(str(column) for column in columns)
        for table, columns in raw.items()
        if isinstance(columns, list)
    }


def _column_scope_for_table(
    entry: Optional[dict], table: Any,
) -> Optional[frozenset[str]]:
    folded_table = str(table or "").casefold()
    for scoped_table, columns in _column_scopes_for_entry(entry).items():
        if scoped_table.casefold() == folded_table:
            return frozenset(column.casefold() for column in columns)
    return None


def _column_allowed(entry: Optional[dict], table: Any, column: Any) -> bool:
    allowed = _column_scope_for_table(entry, table)
    return allowed is None or str(column or "").casefold() in allowed


def _row_scopes_for_entry(entry: Optional[dict]) -> dict[str, list[dict]]:
    if not entry:
        return {}
    scope = _current_database_scope()
    if scope["mode"] != "restricted":
        return {}
    raw = scope["rowScopes"].get(_database_scope_ref(entry))
    if not isinstance(raw, dict):
        return {}
    return {
        str(table): [dict(item) for item in filters if isinstance(item, dict)]
        for table, filters in raw.items()
        if isinstance(filters, list)
    }


def _current_access_scope_ref(entry: Optional[dict]) -> str:
    allowed = _table_scope_for_entry(entry)
    if allowed is None:
        return "all"
    column_scopes = _column_scopes_for_entry(entry)
    row_scopes = _row_scopes_for_entry(entry)
    if not column_scopes and not row_scopes:
        # Preserve v1 refs so sessions/runs made by existing table-only
        # credentials remain accessible after the schema-v4 upgrade.
        payload = "\n".join(sorted(allowed))
        return hashlib.sha256(
            f"dbagent-table-access-v1:{_database_scope_ref(entry or {})}:{payload}".encode("utf-8")
        ).hexdigest()
    if not row_scopes:
        payload = json.dumps({
            "tables": sorted(allowed),
            "columns": {
                table.casefold(): sorted(column.casefold() for column in columns)
                for table, columns in sorted(
                    column_scopes.items(), key=lambda item: item[0].casefold(),
                )
            },
        }, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(
            f"dbagent-column-access-v1:{_database_scope_ref(entry or {})}:{payload}".encode("utf-8")
        ).hexdigest()
    payload = json.dumps({
        "tables": sorted(allowed),
        "columns": {
            table.casefold(): sorted(column.casefold() for column in columns)
            for table, columns in sorted(
                column_scopes.items(), key=lambda item: item[0].casefold(),
            )
        },
        "rows": {
            table.casefold(): filters
            for table, filters in sorted(
                row_scopes.items(), key=lambda item: item[0].casefold(),
            )
        },
    }, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(
        f"dbagent-row-access-v1:{_database_scope_ref(entry or {})}:{payload}".encode("utf-8")
    ).hexdigest()


def _stored_access_scope_allowed(db_id: Any, stored_scope_ref: Any) -> bool:
    normalized_db_id = str(db_id or "")
    if not normalized_db_id:
        return not _database_scope_is_restricted()
    entry = _db_entry_unchecked(normalized_db_id)
    if not _database_entry_allowed(entry):
        return False
    current = _current_access_scope_ref(entry)
    return current == "all" or current == str(stored_scope_ref or "all")


def _write_proposal_scope_allowed(entry: Optional[dict], proposal: Any) -> bool:
    if proposal is None:
        return True
    if _row_scopes_for_entry(entry):
        return False
    scoped_tables = _table_scope_for_entry(entry)
    if scoped_tables is None:
        return True
    proposal_table = str(getattr(proposal, "table", "") or "")
    proposal_kind = str(getattr(proposal, "kind", "") or "").upper()
    if (
        proposal_kind in {"CREATE", "ALTER", "DROP"}
        or proposal_table.casefold() not in scoped_tables
    ):
        return False
    current_scope_ref = _current_access_scope_ref(entry)
    return str(getattr(proposal, "access_scope_ref", "all") or "all") == current_scope_ref


def _register_database_entry(entry: dict) -> bool:
    if not _database_entry_allowed(entry):
        return False
    _DB_AGENT_DBS[str(entry["id"])] = entry
    return True


def _database_scope_summary() -> dict:
    scope = _current_database_scope()
    return {
        "mode": scope["mode"],
        "databaseCount": len(scope["databaseRefs"]),
        "tableScopeDatabaseCount": len(scope["tableScopes"]),
        "tableScopeTableCount": sum(
            len(tables) for tables in scope["tableScopes"].values()
        ),
        "columnScopeTableCount": sum(
            len(tables) for tables in scope["columnScopes"].values()
        ),
        "columnScopeColumnCount": sum(
            len(columns)
            for tables in scope["columnScopes"].values()
            for columns in tables.values()
        ),
        "rowScopeTableCount": sum(
            len(tables) for tables in scope["rowScopes"].values()
        ),
        "rowScopeFilterCount": sum(
            len(filters)
            for tables in scope["rowScopes"].values()
            for filters in tables.values()
        ),
    }


def _audit_database_scope_denial(
    request,
    db_id: str,
    *,
    global_operation: bool = False,
) -> None:
    scope = _current_database_scope()
    try:
        _audit_append(
            "" if global_operation else db_id,
            category="access_control",
            action="deny_database_scope",
            outcome="rejected",
            summary=(
                "受限数据库范围拒绝全局管理请求"
                if global_operation else "数据库不在当前凭据授权范围"
            ),
            risk="high" if global_operation else "medium",
            actor=_current_audit_actor(),
            details={
                "request_role": _current_role(),
                "route_ref": _audit_ref(request.path),
                "http_method": request.method.upper(),
                "database_scope_mode": scope["mode"],
                "database_scope_count": len(scope["databaseRefs"]),
                "table_scope_database_count": len(scope["tableScopes"]),
                "table_scope_table_count": sum(
                    len(tables) for tables in scope["tableScopes"].values()
                ),
                "column_scope_table_count": sum(
                    len(tables) for tables in scope["columnScopes"].values()
                ),
                "column_scope_column_count": sum(
                    len(columns)
                    for tables in scope["columnScopes"].values()
                    for columns in tables.values()
                ),
                "row_scope_table_count": sum(
                    len(tables) for tables in scope["rowScopes"].values()
                ),
                "row_scope_filter_count": sum(
                    len(filters)
                    for tables in scope["rowScopes"].values()
                    for filters in tables.values()
                ),
            },
        )
    except Exception:
        pass


def _session_database_id_unchecked(sid: str) -> str:
    hot = _DB_SESSIONS.get(str(sid or ""))
    if isinstance(hot, dict):
        return str(hot.get("dbId") or "")
    if _db_sess_store_ok():
        try:
            stored = _sess_store.get_session(
                str(sid or ""), include_messages=False,
            )
            if isinstance(stored, dict):
                return str(stored.get("dbId") or "")
        except Exception:
            pass
    return ""


def _session_record_unchecked(sid: str) -> Optional[dict]:
    hot = _DB_SESSIONS.get(str(sid or ""))
    if isinstance(hot, dict):
        return hot
    if _db_sess_store_ok():
        try:
            stored = _sess_store.get_session(
                str(sid or ""), include_messages=False,
            )
            return stored if isinstance(stored, dict) else None
        except Exception:
            pass
    return None


async def _request_database_scope_denial(request) -> Optional[str]:
    """Return an out-of-scope dbId referenced directly or through an object ID."""
    if not _database_scope_is_restricted() or not request.path.startswith("/db/"):
        return None
    db_ids: set[str] = set()
    query_db_id = str(request.query.get("dbId") or "").strip()
    if query_db_id:
        db_ids.add(query_db_id)
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.can_read_body:
        content_type = str(request.content_type or "").lower()
        if content_type == "application/json" and (request.content_length or 0) <= 1024 * 1024:
            try:
                body = await request.json()
            except Exception:
                body = None
            if isinstance(body, dict):
                body_db_id = str(body.get("dbId") or "").strip()
                if body_db_id:
                    db_ids.add(body_db_id)
                if request.path == "/db/ask":
                    body_sid = str(body.get("sessionId") or "").strip()
                    session_db_id = _session_database_id_unchecked(body_sid)
                    if session_db_id:
                        db_ids.add(session_db_id)

    match = request.match_info
    path = request.path
    if path.startswith("/db/databases/"):
        db_id = str(match.get("db_id") or "").strip()
        if db_id:
            db_ids.add(db_id)
    if path.startswith("/db/ask/"):
        run_id = str(match.get("run_id") or "").strip()
        run = _DB_RUNS.get(run_id)
        if isinstance(run, dict) and run.get("dbId"):
            db_ids.add(str(run["dbId"]))
    if path.startswith("/db/session/"):
        sid = str(match.get("sid") or "").strip()
        session_db_id = _session_database_id_unchecked(sid)
        if session_db_id:
            db_ids.add(session_db_id)
    if path.startswith("/db/schedules/") and path != "/db/schedules/logs":
        task_id = str(match.get("id") or "").strip()
        if task_id and _db_sched is not None:
            try:
                task = _db_sched.get_task(task_id)
            except Exception:
                task = None
            if isinstance(task, dict) and task.get("dbId"):
                db_ids.add(str(task["dbId"]))

    for db_id in sorted(db_ids):
        entry = _db_entry_unchecked(db_id)
        if entry is not None and not _database_entry_allowed(entry):
            return db_id
        if entry is not None and _table_scope_for_entry(entry) is not None and (
            path.startswith("/db/schedules") or path.startswith("/db/audit")
            or (request.method == "DELETE" and path.startswith("/db/databases/"))
        ):
            # Scheduled execution, database-wide audit views, and detaching
            # the shared database cannot be confined to selected tables.
            return db_id
    return None


def _semantic_portable_entry(entry: dict) -> dict:
    return {
        "kind": entry.get("kind"),
        "term": entry.get("term"),
        "table": entry.get("table"),
        "column": entry.get("column") or "",
        "value": entry.get("value") if entry.get("kind") == "enum_value" else None,
        "aggregation": entry.get("aggregation") or "",
        "filters": entry.get("filters") if entry.get("kind") in {"metric", "dimension"} else [],
        "formula": entry.get("formula") if entry.get("kind") == "ratio_metric" else None,
        "calendar": entry.get("calendar") if entry.get("kind") == "business_calendar" else None,
        "hierarchy": entry.get("hierarchy") if entry.get("kind") == "dimension" else None,
        "default_grain": entry.get("default_grain") if entry.get("kind") == "time_field" else "",
        "description": entry.get("description") or "",
    }


def _semantic_entry_signature(entry: dict) -> str:
    return json.dumps(
        _semantic_portable_entry(entry), ensure_ascii=False,
        separators=(",", ":"), sort_keys=True,
    )


def _semantic_import_cleanup_locked(now: float) -> None:
    expired = [token for token, item in _SEMANTIC_IMPORTS.items() if item["expires_at"] <= now]
    for token in expired:
        _SEMANTIC_IMPORTS.pop(token, None)
    if len(_SEMANTIC_IMPORTS) > _SEMANTIC_IMPORT_MAX_PENDING:
        oldest = sorted(_SEMANTIC_IMPORTS, key=lambda token: _SEMANTIC_IMPORTS[token]["created_at"])
        for token in oldest[:len(_SEMANTIC_IMPORTS) - _SEMANTIC_IMPORT_MAX_PENDING]:
            _SEMANTIC_IMPORTS.pop(token, None)


def _semantic_import_register(
    database_key: str,
    revision: str,
    entries: list[dict],
    access_scope_ref: str,
) -> tuple[str, float]:
    now = time.time()
    token = secrets.token_urlsafe(24)
    expires_at = now + _SEMANTIC_IMPORT_TTL_SECONDS
    with _SEMANTIC_IMPORTS_LOCK:
        _semantic_import_cleanup_locked(now)
        _SEMANTIC_IMPORTS[token] = {
            "database_key": database_key,
            "access_scope_ref": access_scope_ref,
            "revision": revision,
            "entries": entries,
            "created_at": now,
            "expires_at": expires_at,
        }
        _semantic_import_cleanup_locked(now)
    return token, expires_at


def _semantic_import_consume(
    token: str,
    database_key: str,
    access_scope_ref: str,
) -> dict:
    now = time.time()
    with _SEMANTIC_IMPORTS_LOCK:
        _semantic_import_cleanup_locked(now)
        pending = _SEMANTIC_IMPORTS.pop(token, None)
    if (
        not pending
        or pending["database_key"] != database_key
        or pending.get("access_scope_ref") != access_scope_ref
    ):
        raise ValueError("导入预检令牌无效或已过期，请重新预检")
    return pending


def _db_semantics(entry: dict, schema: Optional[Any] = None) -> list[dict]:
    if _semantic_store is None:
        return []
    visible = [
        item for item in _semantic_store.list_entries(_db_semantic_key(entry))
        if _table_allowed(entry, item.get("table"))
    ]
    if schema is None or not _column_scopes_for_entry(entry):
        return visible
    dc = _db_agent_core()
    safe = []
    for item in visible:
        try:
            dc.SemanticCatalog(schema, [item], strict=True)
            safe.append(item)
        except ValueError:
            # A semantic definition that references a hidden field or an
            # unauthorized holiday table must not be exposed to this principal.
            continue
    return safe


def _csv_to_sqlite(csv_path: str, db_path: str, table_name: str) -> list:
    """把 CSV 转成 sqlite 并返回表名列表（与 _db_validate_sqlite 返回结构一致）。
    自动嗅探编码(utf-8-sig/utf-8/gb18030)、分隔符(, \\t ; |)、列类型(int/float/text)。
    无表头 CSV（首行全数值）→ 列名 col_N；空/重复表头自动改名；NA/null/NaN/空 → NULL；分块导入。
    """
    import csv as _csv
    import sqlite3  # 注意：本模块 sqlite3 是函数级 import，这里必须自带

    def sniff_encoding(path, nbytes=65536):
        with open(path, "rb") as f:
            raw = f.read(nbytes)
        best = ("utf-8", float("-inf"))
        for enc in ("utf-8-sig", "utf-8", "gb18030", "latin-1"):
            try:
                txt = raw.decode(enc)
            except Exception:
                continue
            cjk = sum(1 for ch in txt if "\u4e00" <= ch <= "\u9fff")
            bad = sum(1 for ch in txt if ch in "\ufffd")
            printable = sum(1 for ch in txt if ch.isprintable()) / max(1, len(txt))
            score = cjk * 10 - bad * 100 + printable
            if score > best[1]:
                best = (enc, score)
        return best[0]

    def is_num(v):
        try:
            float(v)
            return True
        except ValueError:
            return False

    enc = sniff_encoding(csv_path)
    try:
        with open(csv_path, encoding=enc, newline="") as f:
            head = f.read(8192)
        delim = _csv.Sniffer().sniff(head, delimiters=",\t;|").delimiter
    except Exception:
        delim = ","
    with open(csv_path, encoding=enc, newline="") as f:
        reader = _csv.reader(f, delimiter=delim)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("empty csv")
    ncol = len(header)
    first_data = None
    if ncol and all(is_num((v or "").strip()) for v in header):
        # 首行全数值 -> 无表头 CSV，该行是数据
        cols = ["col_%d" % (i + 1) for i in range(ncol)]
        first_data = header
    else:
        if header and header[0].startswith("\ufeff"):
            header[0] = header[0][1:]
        cols, used = [], {}
        for i, h in enumerate(header):
            h = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff]+", "_", (h or "").strip()).strip("_")
            if not h:
                h = "col_%d" % (i + 1)
            if h in used:
                used[h] += 1
                h = "%s_%d" % (h, used[h])
            else:
                used[h] = 0
            cols.append(h)

    def data_iter():
        if first_data is not None:
            yield first_data
        with open(csv_path, encoding=enc, newline="") as f:
            reader = _csv.reader(f, delimiter=delim)
            next(reader, None)
            for row in reader:
                yield row

    # 类型采样（前 2000 行）
    cnt = [0] * ncol
    nint = [0] * ncol
    nflt = [0] * ncol
    n = 0
    for row in data_iter():
        if n >= 2000:
            break
        n += 1
        for i in range(min(ncol, len(row))):
            v = (row[i] or "").strip()
            if not v or v.upper() in ("NA", "NULL", "NAN", "NONE"):
                continue
            cnt[i] += 1
            if is_num(v):
                nflt[i] += 1
                sv = v
                if sv.isdigit() or (sv.startswith("-") and sv[1:].isdigit()) or (sv.startswith("+") and sv[1:].isdigit()):
                    nint[i] += 1
    types = []
    for i in range(ncol):
        if cnt[i] and nint[i] == cnt[i]:
            types.append("INTEGER")
        elif cnt[i] and nflt[i] == cnt[i]:
            types.append("REAL")
        else:
            types.append("TEXT")

    qname = table_name.replace('"', '""')
    col_defs = ", ".join('"%s" %s' % (c, t) for c, t in zip(cols, types))
    conn = sqlite3.connect(db_path)
    try:
        conn.execute('CREATE TABLE "%s" (%s)' % (qname, col_defs))
        ph = ", ".join("?" * ncol)
        ins = 'INSERT INTO "%s" VALUES (%s)' % (qname, ph)
        batch = []
        for row in data_iter():
            vals = []
            for i in range(ncol):
                v = (row[i] if i < len(row) else "").strip()
                if not v or v.upper() in ("NA", "NULL", "NAN", "NONE"):
                    vals.append(None)
                elif types[i] == "INTEGER":
                    try:
                        vals.append(int(v))
                    except ValueError:
                        try:
                            vals.append(int(float(v)))
                        except ValueError:
                            vals.append(None)
                elif types[i] == "REAL":
                    try:
                        vals.append(float(v))
                    except ValueError:
                        vals.append(None)
                else:
                    vals.append(v)
            batch.append(vals)
            if len(batch) >= 20000:
                conn.executemany(ins, batch)
                batch = []
        if batch:
            conn.executemany(ins, batch)
        conn.commit()
    finally:
        conn.close()
    return [table_name]

def _excel_to_sqlite(xlsx_path: str, db_path: str) -> list:
    """把 Excel(.xlsx) 转成 sqlite，多 sheet → 多表。openpyxl 标准库级可用（本模块须自带 import）。
    表头识别：首行全数值/空 → col_N；列类型采样(int/float/text)；空值/None → NULL；分块导入。"""
    import openpyxl
    import sqlite3

    def is_num(v):
        try:
            float(v)
            return True
        except (ValueError, TypeError):
            return False

    def sanitize(s):
        s = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff]+", "_", (s or "").strip()).strip("_")
        return s or None

    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    conn = sqlite3.connect(db_path)
    tables = []
    used_names = {}
    try:
        for ws in wb.worksheets:
            all_rows = [list(r) for r in ws.iter_rows(values_only=True)
                        if r is not None and any(v is not None and str(v).strip() != "" for v in r)]
            if not all_rows:
                continue
            header = all_rows[0]
            ncol = len(header)
            non_empty = [v for v in header if v is not None and str(v).strip() != ""]
            if not non_empty or all(is_num(v) for v in non_empty):
                # 无表头 sheet → col_N，首行是数据
                cols = ["col_%d" % (i + 1) for i in range(ncol)]
                first_data = header
            else:
                cols, used = [], {}
                for i, h in enumerate(header):
                    h = sanitize(h) or "col_%d" % (i + 1)
                    if h in used:
                        used[h] += 1
                        h = "%s_%d" % (h, used[h])
                    else:
                        used[h] = 0
                    cols.append(h)
                first_data = None

            def data_iter():
                if first_data is not None:
                    yield first_data
                for r in all_rows[1:]:
                    yield r

            # 类型采样（前 2000 行）
            cnt = [0] * ncol
            nint = [0] * ncol
            nflt = [0] * ncol
            n = 0
            for row in data_iter():
                if n >= 2000:
                    break
                n += 1
                for i in range(min(ncol, len(row))):
                    v = row[i]
                    if v is None or str(v).strip() == "":
                        continue
                    cnt[i] += 1
                    if is_num(v):
                        nflt[i] += 1
                        if isinstance(v, int) and not isinstance(v, bool):
                            nint[i] += 1
            types = []
            for i in range(ncol):
                if cnt[i] and nint[i] == cnt[i]:
                    types.append("INTEGER")
                elif cnt[i] and nflt[i] == cnt[i]:
                    types.append("REAL")
                else:
                    types.append("TEXT")

            tname = sanitize(ws.title) or ("sheet_%d" % (len(tables) + 1))
            if tname in used_names:
                used_names[tname] += 1
                tname = "%s_%d" % (tname, used_names[tname])
            else:
                used_names[tname] = 0
            qname = tname.replace('"', '""')
            col_defs = ", ".join('"%s" %s' % (c, t) for c, t in zip(cols, types))
            conn.execute('CREATE TABLE "%s" (%s)' % (qname, col_defs))
            ph = ", ".join("?" * ncol)
            ins = 'INSERT INTO "%s" VALUES (%s)' % (qname, ph)
            batch = []
            for row in data_iter():
                vals = []
                for i in range(ncol):
                    v = row[i] if i < len(row) else None
                    if v is None or str(v).strip() == "":
                        vals.append(None)
                    elif types[i] == "INTEGER":
                        if isinstance(v, bool):
                            vals.append(1 if v else 0)
                        else:
                            try:
                                vals.append(int(v))
                            except (ValueError, TypeError):
                                try:
                                    vals.append(int(float(v)))
                                except (ValueError, TypeError):
                                    vals.append(None)
                    elif types[i] == "REAL":
                        try:
                            vals.append(float(v))
                        except (ValueError, TypeError):
                            vals.append(None)
                    else:
                        vals.append(str(v))
                batch.append(vals)
                if len(batch) >= 20000:
                    conn.executemany(ins, batch)
                    batch = []
            if batch:
                conn.executemany(ins, batch)
            conn.commit()
            tables.append(tname)
    finally:
        try:
            wb.close()
        except Exception:
            pass
        conn.close()
    return tables

def _db_sqlite_business_objects(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return user-facing SQLite objects while excluding FTS shadow storage."""
    rows = conn.execute(
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','virtual','shadow')"
    ).fetchall()
    objects = {
        str(name): str(sql or "")
        for name, object_type, sql in rows
        if str(object_type or "").lower() != "shadow"
    }
    fts_roots = [
        name for name, sql in objects.items()
        if sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE")
        and "FTS" in sql.upper()
    ]
    for name in list(objects):
        for root in fts_roots:
            suffix = name[len(root) + 1:] if name.startswith(root + "_") else ""
            if suffix in {"data", "docsize", "idx", "config", "content"}:
                objects.pop(name, None)
                break
    return sorted(objects.items(), key=lambda item: item[0].casefold())


def _db_validate_sqlite(path: str) -> list:
    """校验路径为可只读打开的 sqlite，返回真实业务表名列表。"""
    import sqlite3
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        raise ValueError(f"数据库文件不存在: {p}")
    if p.suffix.lower() not in (".db", ".sqlite", ".sqlite3"):
        raise ValueError("仅支持 .db/.sqlite/.sqlite3 文件")
    conn = sqlite3.connect(f"{p.as_uri()}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        tables = [name for name, _ in _db_sqlite_business_objects(conn)]
        if not tables:
            raise ValueError("数据库中没有任何业务表")
        return tables
    finally:
        conn.close()


def _db_validate_and_summarize(path: str) -> list:
    """upload_handler 用的别名（校验+返回表名）。"""
    return _db_validate_sqlite(path)


def _db_sanitize(entry: dict) -> dict:
    """对外返回前脱敏：远程连接密码不泄露。"""
    v = dict(entry)
    v["databaseRef"] = _database_scope_ref(entry)
    allowed_tables = _table_scope_for_entry(entry)
    if allowed_tables is not None:
        v["tables"] = [
            table for table in (v.get("tables") or [])
            if str(table).casefold() in allowed_tables
        ]
        v["tableScopeRestricted"] = True
    if _column_scopes_for_entry(entry):
        v["columnScopeRestricted"] = True
    if _row_scopes_for_entry(entry):
        v["rowScopeRestricted"] = True
    if "conn" in v:
        v["writeEnabled"] = bool(v["conn"].get("write_enabled"))
        v["conn"] = {k: ("***" if k == "password" else val) for k, val in v["conn"].items()}
    if _current_role() == "viewer":
        v.pop("path", None)
        conn = v.get("conn")
        if isinstance(conn, dict):
            v["conn"] = {
                key: conn[key] for key in ("dialect", "database") if key in conn
            }
    return v


def _db_view(db_id: str) -> dict:
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    return _db_sanitize(entry)


def _db_get_agent(db_id: str, llm_cfg=None):
    """按 dbId 取（并缓存）DBQuillAgent 实例；llm_cfg 变化时自动重建。"""
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    cfg = (llm_cfg or "").strip() or _default_model_profile()
    allowed_tables = _table_scope_for_entry(entry)
    if allowed_tables is not None:
        allowed_columns = _column_scopes_for_entry(entry)
        row_filters = _row_scopes_for_entry(entry)
        dc = _db_agent_core()
        conn_kw = {
            "sample_rows": 3,
            "max_rows": 500,
            "timeout_s": 15.0,
            "allowed_tables": sorted(allowed_tables),
            "allowed_columns": {
                table: list(columns) for table, columns in allowed_columns.items()
            },
            "row_filters": row_filters,
        }
        if entry.get("conn"):
            raise ValueError("表级授权目前仅支持本地 SQLite 数据库")
        conn_kw["db_path"] = entry["path"]
        try:
            agent = dc.DBQuillAgent(
                llm_cfg=cfg,
                semantic_entries=_db_semantics(entry),
                **conn_kw,
            )
        except ValueError:
            cfg = _default_model_profile()
            agent = dc.DBQuillAgent(
                llm_cfg=cfg,
                semantic_entries=_db_semantics(entry),
                **conn_kw,
            )
        agent._llm_cfg = cfg
        return agent
    cur = _DB_AGENT_CACHE.get(db_id)
    if cur is None or getattr(cur, "_llm_cfg", None) != cfg:
        dc = _db_agent_core()
        conn_kw = {"sample_rows": 3, "max_rows": 500, "timeout_s": 15.0}
        if entry.get("conn"):
            conn_kw["connector"] = dc.RemoteDBConnector(entry["conn"])
        else:
            conn_kw["db_path"] = entry["path"]
        try:
            agent = dc.DBQuillAgent(llm_cfg=cfg, semantic_entries=_db_semantics(entry), **conn_kw)
        except ValueError:
            cfg = _default_model_profile()
            agent = dc.DBQuillAgent(llm_cfg=cfg, semantic_entries=_db_semantics(entry), **conn_kw)
        agent._llm_cfg = cfg
        _DB_AGENT_CACHE[db_id] = agent
    return _DB_AGENT_CACHE[db_id]


def _db_ensure_local_entry_attached(db_id: str) -> bool:
    """Return whether a local database is attached and visible to this request."""
    return db_id in _DB_AGENT_DBS and bool(_db_entry(db_id))


def _db_run_view(run_id: str) -> Optional[dict]:
    run = _DB_RUNS.get(run_id)
    if run is None or not _stored_access_scope_allowed(
        run.get("dbId"), run.get("accessScopeRef"),
    ):
        return None
    view = dict(run)
    if run.get("result") is not None:
        view["result"] = run["result"]
    return view


def _db_run_update(run_id: str, **kw) -> bool:
    run = _DB_RUNS.get(run_id)
    if run is None:
        return False
    run.update(kw)
    return True


def _db_answer_to_dict(ans: Any) -> dict:
    """DBAnswer dataclass → JSON 可序列化 dict。"""
    from dataclasses import asdict
    try:
        return asdict(ans)
    except Exception:
        return {
            "kind": getattr(ans, "kind", "error"),
            "narrative": getattr(ans, "narrative", ""),
            "sql": getattr(ans, "sql", None),
            "columns": list(getattr(ans, "columns", [])),
            "rows": list(getattr(ans, "rows", [])),
            "datasets": list(getattr(ans, "datasets", [])),
            "evidence": list(getattr(ans, "evidence", [])),
            "steps": list(getattr(ans, "steps", [])),
            "error": getattr(ans, "error", None),
            "confirm_id": getattr(ans, "confirm_id", None),
            "write": getattr(ans, "write", None),
            "operation": getattr(ans, "operation", None),
            "clarification": getattr(ans, "clarification", None),
            "graph": getattr(ans, "graph", None),
            "semantic": getattr(ans, "semantic", None),
            "calendar_plan": getattr(ans, "calendar_plan", None),
            "metric_plan": getattr(ans, "metric_plan", None),
            "dimension_plan": getattr(ans, "dimension_plan", None),
            "trend_plan": getattr(ans, "trend_plan", None),
            "relational_plan": getattr(ans, "relational_plan", None),
        }


def _audit_database_key(db_id: str) -> str:
    entry = _db_entry_unchecked(db_id)
    return _db_semantic_key(entry) if entry else f"detached:{db_id}"


def _audit_sha256(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _audit_ref(value: Any) -> str:
    return _audit_sha256(value)[:16]


class AuditGateError(RuntimeError):
    """审计先决写入失败，受保护的后续变更不得继续。"""


def _audit_append(
    db_id: str,
    *,
    category: str,
    action: str,
    outcome: str,
    summary: str,
    risk: str = "low",
    session_id: str = "",
    run_id: str = "",
    correlation_id: str = "",
    details: Optional[dict] = None,
    actor: Optional[str] = None,
    strict: bool = False,
) -> Optional[dict]:
    """追加脱敏审计事件；strict 用于写执行前的 fail-closed 门禁。"""
    try:
        if _audit_store is None:
            raise RuntimeError("审计账本不可用")
        if strict:
            integrity = _audit_store.verify_chain()
            if not integrity.get("ok"):
                raise RuntimeError(
                    f"审计账本完整性异常: sequence={integrity.get('error_sequence')}"
                )
        resolved_actor = actor or _current_audit_actor()
        safe_details = dict(details or {})
        principal = _current_principal()
        credential_ref = str(principal.get("credentialRef") or "")
        if resolved_actor.startswith("local_") and credential_ref:
            safe_details.setdefault("credential_ref", credential_ref)
        return _audit_store.append_event(
            category=category,
            action=action,
            outcome=outcome,
            summary=summary,
            risk=risk,
            actor=resolved_actor,
            database_key=_audit_database_key(db_id),
            session_id=session_id,
            run_id=run_id,
            correlation_id=correlation_id,
            details=safe_details,
        )
    except Exception as exc:
        if strict:
            raise AuditGateError(
                f"审计事件登记失败，操作未执行: {type(exc).__name__}"
            ) from exc
        print(f"[db-audit] append failed: {type(exc).__name__}", file=sys.stderr)
        return None


def _audit_answer_details(question: str, answer: Optional[dict] = None) -> dict:
    answer = answer or {}
    details: dict = {
        "question_sha256": _audit_sha256(question),
        "question_length": len(question or ""),
    }
    kind = str(answer.get("kind") or "").strip()
    if kind:
        details["answer_kind"] = kind
    sql = str(answer.get("sql") or "")
    if sql:
        details["sql_sha256"] = _audit_sha256(sql)
    rows = answer.get("rows")
    if isinstance(rows, list):
        details["result_rows"] = len(rows)
    datasets = answer.get("datasets")
    if isinstance(datasets, list):
        details["dataset_count"] = len(datasets)
    operation = answer.get("operation") if isinstance(answer.get("operation"), dict) else {}
    for source, target in (
        ("action", "operation_action"),
        ("mode", "operation_mode"),
        ("status", "operation_status"),
    ):
        value = str(operation.get(source) or "").strip()
        if value:
            details[target] = value
    targets = operation.get("target_tables")
    if isinstance(targets, list):
        refs = [_audit_ref(item) for item in targets[:32] if str(item or "").strip()]
        details["target_refs"] = refs
        details["target_count"] = len(targets)
    write = answer.get("write") if isinstance(answer.get("write"), dict) else {}
    affected = write.get("affected")
    if isinstance(affected, int) and not isinstance(affected, bool) and affected >= 0:
        details["affected_rows"] = affected
    return details


def _audit_nl_terminal(
    run_id: str,
    db_id: str,
    question: str,
    sid: str,
    *,
    answer: Optional[dict] = None,
    forced_outcome: str = "",
    error: Optional[BaseException] = None,
) -> None:
    answer = answer or {}
    operation = answer.get("operation") if isinstance(answer.get("operation"), dict) else {}
    action = str(operation.get("action") or answer.get("kind") or "process").strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", action):
        action = "process"
    risk = str(operation.get("risk") or "low").strip().lower()
    if risk not in {"low", "medium", "high"}:
        risk = "low"
    kind = str(answer.get("kind") or "")
    status = str(operation.get("status") or "")
    outcome = forced_outcome
    if not outcome:
        if kind == "error" or status == "failed":
            outcome = "failed"
        elif kind in {"clarification", "write_form", "write_pending"}:
            outcome = "pending"
        elif status == "cancelled":
            outcome = "cancelled"
        else:
            outcome = "succeeded"
    details = _audit_answer_details(question, answer)
    if error is not None:
        details["error_type"] = type(error).__name__
        details["error_sha256"] = _audit_sha256(str(error))
    _audit_append(
        db_id,
        category="nl_operation",
        action=action,
        outcome=outcome,
        summary="基础沟通" if kind == "conversation" else "自然语言数据库操作",
        risk=risk,
        session_id=sid,
        run_id=run_id,
        correlation_id=run_id,
        details=details,
    )


def _db_ask_workflow(run_id: str, db_id: str, question: str, sid: str = "", history: Optional[list] = None) -> None:
    cancel_event = _DB_RUN_CANCEL_EVENTS.get(run_id)
    try:
        run = _DB_RUNS.get(run_id, {})
        if cancel_event is not None and cancel_event.is_set():
            return
        clarification = run.get("clarification") or None
        core = _db_agent_core()
        with core.cancellation_scope(cancel_event):
            try:
                agent = _db_get_agent(db_id, run.get("llmCfg") or None)
                if cancel_event is not None and cancel_event.is_set():
                    return
                _db_run_update(run_id, percent=20, stage="intent", label="意图判断完成，执行中")
                ans = agent.ask(question, history=history, clarification=clarification)
            except ValueError as exc:
                if "model profile" not in str(exc).lower():
                    raise
                if cancel_event is not None and cancel_event.is_set():
                    return
                _db_run_update(run_id, llmCfg=None, label="配置不可用，已回退默认模型")
                agent = _db_get_agent(db_id, None)
                _db_run_update(run_id, percent=20, stage="intent", label="意图判断完成，执行中")
                ans = agent.ask(question, history=history, clarification=clarification)
        current_run = _DB_RUNS.get(run_id, {})
        if (cancel_event is not None and cancel_event.is_set()) \
                or current_run.get("cancelRequested") \
                or current_run.get("status") == "cancelled":
            if not current_run.get("cancelAuditRecorded"):
                _audit_nl_terminal(
                    run_id, db_id, question, sid, answer=_db_answer_to_dict(ans),
                    forced_outcome="cancelled",
                )
                _db_run_update(run_id, cancelAuditRecorded=True)
            _db_run_update(
                run_id, status="cancelled", percent=100,
                stage="done", label="已取消",
            )
            return
        answer = _db_answer_to_dict(ans)
        confirm_id = str(getattr(ans, "confirm_id", "") or "")
        if confirm_id:
            proposal = _db_agent_core().WRITE_REGISTRY.get(confirm_id)
            if proposal is not None:
                proposal.access_scope_ref = _current_access_scope_ref(
                    _db_entry_unchecked(db_id),
                )
        # A completed/expired run may be pruned while its daemon worker is still
        # unwinding. Do not resurrect it or write session/audit side effects into
        # a test/runtime context that has already been released.
        if not _db_run_update(run_id, result=answer):
            return
        _db_session_append(
            sid,
            "assistant",
            _db_history_content(ans),
            display_payload=_db_session_display_payload(ans),
        )
        # 基础沟通可以短暂打断数据库澄清，但不应把已经收集的
        # 结构化上下文清空；下一轮仍可直接回复待补充项。
        pending = clarification if getattr(ans, "kind", "") == "conversation" \
            else getattr(ans, "clarification", None)
        if sid:
            if _DB_SESSIONS.get(sid) is not None:
                _DB_SESSIONS[sid]["clarification"] = pending
            if _db_sess_store_ok():
                if pending:
                    _sess_store.set_pending_clarification(sid, pending)
                else:
                    _sess_store.clear_pending_clarification(sid)
        # 只有回答与待澄清状态都已写入会话后才发布 done；否则客户端可能
        # 在两者之间发起下一轮，导致短回复丢失结构化澄清上下文。
        _audit_nl_terminal(run_id, db_id, question, sid, answer=answer)
        _db_run_update(run_id, status="done", percent=100, stage="done", label="回答完成")
    except Exception as exc:
        current_run = _DB_RUNS.get(run_id, {})
        if (cancel_event is not None and cancel_event.is_set()) \
                or current_run.get("cancelRequested") \
                or current_run.get("status") == "cancelled":
            if not current_run.get("cancelAuditRecorded"):
                _audit_nl_terminal(
                    run_id, db_id, question, sid,
                    forced_outcome="cancelled", error=exc,
                )
                _db_run_update(run_id, cancelAuditRecorded=True)
            _db_run_update(
                run_id, status="cancelled", percent=100,
                stage="done", label="已取消", error="",
            )
        else:
            traceback.print_exc()
            _audit_nl_terminal(
                run_id, db_id, question, sid, forced_outcome="failed", error=exc,
            )
            _db_run_update(
                run_id, status="error", stage="error", label="执行失败",
                error=f"{type(exc).__name__}: {exc}",
            )
    finally:
        _DB_RUN_CANCEL_EVENTS.pop(run_id, None)


async def db_databases_handler(request):
    """列出已 attach 的 sqlite 数据库。"""
    dbs = [
        _db_sanitize(dict(entry) | {"preset": bool(entry.get("preset"))})
        for entry in _all_database_entries_unchecked()
        if _database_entry_allowed(entry)
    ]
    return json_ok({
        "ok": True,
        "databases": dbs,
        "databaseScope": _database_scope_summary(),
    })


async def db_attach_handler(request):
    data = await read_json(request)
    path = str(data.get("path") or "").strip()
    name = str(data.get("name") or "").strip()
    if not path:
        return json_ok({"ok": False, "error": "missing path"}, status=400)
    try:
        tables = _db_validate_sqlite(path)
    except Exception as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    db_id = uuid.uuid4().hex[:12]
    entry = {
        "id": db_id, "name": name or Path(path).name,
        "path": str(Path(path).expanduser().resolve()),
        "tables": tables, "attachedAt": time.time(),
    }
    if not _database_entry_allowed(entry):
        _audit_database_scope_denial(request, db_id)
        return json_ok({
            "ok": False,
            "error": "当前凭据未获授权接入该数据库",
        }, status=403)
    _DB_AGENT_DBS[db_id] = entry
    return json_ok({"ok": True, "db": _db_sanitize(entry)})


async def db_connect_handler(request):
    """连接远程数据库（mysql/postgresql）→ 发现 schema → 注册为可对话数据源。"""
    data = await read_json(request)
    dialect = str(data.get("dialect") or "").strip().lower()
    if dialect not in ("mysql", "postgresql"):
        return json_ok({"ok": False, "error": "dialect 仅支持 mysql / postgresql"}, status=400)
    cfg = {
        "dialect": dialect,
        "host": str(data.get("host") or "").strip(),
        "port": int(data.get("port") or (3306 if dialect == "mysql" else 5432)),
        "user": str(data.get("user") or "").strip(),
        "password": str(data.get("password") or ""),
        "database": str(data.get("database") or "").strip(),
        "write_enabled": data.get("writeEnabled") is True,
    }
    if not cfg["host"] or not cfg["user"] or not cfg["database"]:
        return json_ok({"ok": False, "error": "host / user / database 必填"}, status=400)
    name = str(data.get("name") or "").strip() or f"{dialect}-{cfg['database']}"
    try:
        dc = _db_agent_core()
        conn = dc.RemoteDBConnector(cfg)
        snapshot = dc.SchemaDiscovery(conn, sample_rows=3).discover()
        tables = sorted(snapshot.tables)
        if not tables:
            return json_ok({"ok": False, "error": "远程库无可用业务表"}, status=400)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"连接失败：{type(exc).__name__}: {exc}"}, status=400)
    db_id = uuid.uuid4().hex[:12]
    entry = {
        "id": db_id, "name": name,
        "path": f"{dialect}://{cfg['host']}:{cfg['port']}/{cfg['database']}",
        "tables": tables, "attachedAt": time.time(), "preset": False,
        "kind": dialect, "conn": cfg,  # conn 含 password，仅存内存；对外一律脱敏
    }
    if not _database_entry_allowed(entry):
        _audit_database_scope_denial(request, db_id)
        return json_ok({
            "ok": False,
            "error": "当前凭据未获授权接入该数据库",
        }, status=403)
    _DB_AGENT_DBS[db_id] = entry
    return json_ok({"ok": True, "db": _db_sanitize(entry)})




async def db_detach_handler(request):
    db_id = request.match_info["db_id"]
    if not _db_entry(db_id):
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    _DB_AGENT_DBS.pop(db_id, None)
    _DB_AGENT_CACHE.pop(db_id, None)
    return json_ok({"ok": True})


_DB_SESSIONS: Dict[str, dict] = {}  # sessionId -> 会话记录（内存热缓存；持久权威在 db_sessions_store）
_DB_SESSIONS_MAX = 100
_DB_SESSION_HISTORY_MAX = 14  # 与 dbquill_core._HISTORY_MAX_MSGS 对齐（约 7 轮）
_DB_SESSION_DISPLAY_ROWS_MAX = 500  # 与只读查询物化硬上限一致；桌面默认只展开 10 行
_DB_SESSION_DISPLAY_BYTES_MAX = 700 * 1024


def _db_sess_store_ok() -> bool:
    return _sess_store is not None


def _db_session_touch(sid: str, db_id: str, question: str) -> None:
    """提问前记录：更新最近使用 + 追加本轮 user 消息（内存 + 持久化）。"""
    if not sid:
        return
    prev = _DB_SESSIONS.get(sid)
    messages = list((prev or {}).get("messages", []))
    messages.append({"role": "user", "content": question[:2000]})
    created = (prev or {}).get("createdAt") or time.time()
    access_scope_ref = _current_access_scope_ref(_db_entry_unchecked(db_id))
    _DB_SESSIONS[sid] = {
        "id": sid,
        "dbId": db_id,
        "accessScopeRef": access_scope_ref,
        "lastQuestion": question[:200],
        "count": (prev or {}).get("count", 0) + 1,
        "createdAt": created,
        "updatedAt": time.time(),
        "messages": messages[-_DB_SESSION_HISTORY_MAX:],
        "clarification": (prev or {}).get("clarification"),
    }
    if _db_sess_store_ok():
        _sess_store.upsert_session(
            sid,
            db_id=db_id,
            last_question=question[:200],
            count=(prev or {}).get("count", 0) + 1,
            created_at=created,
            title=(prev or {}).get("title", "") or question[:30],
            access_scope_ref=access_scope_ref,
        )
        _sess_store.append_message(sid, "user", question[:2000])
    if len(_DB_SESSIONS) > _DB_SESSIONS_MAX:
        for old_sid in sorted(_DB_SESSIONS, key=lambda k: _DB_SESSIONS[k]["updatedAt"])[
            : len(_DB_SESSIONS) - _DB_SESSIONS_MAX
        ]:
            _DB_SESSIONS.pop(old_sid, None)


def _db_session_history(sid: str) -> Optional[list]:
    """取会话内多轮历史（最近 N 条 Q/A），无则 None（供 agent.ask 注入）。

    内存 miss 时回退持久化 store（bridge 重启后热缓存被裁剪时仍可续聊）。
    """
    if not sid:
        return None
    msgs = _DB_SESSIONS.get(sid, {}).get("messages") or []
    if not msgs and _db_sess_store_ok():
        msgs = _sess_store.get_history(sid) or []
    # UI-only display snapshots must never expand the model context.  The LLM
    # keeps receiving the deliberately compact text contract below.
    history = [
        {
            "role": str(message.get("role") or ""),
            "content": str(message.get("content") or ""),
        }
        for message in msgs[-_DB_SESSION_HISTORY_MAX:]
        if isinstance(message, dict)
    ]
    return history or None


def _db_session_clarification(sid: str) -> Optional[dict]:
    """读取结构化澄清上下文，避免连续短回复时丢失已补充信息。"""
    if not sid:
        return None
    pending = _DB_SESSIONS.get(sid, {}).get("clarification")
    if pending:
        return pending
    if _db_sess_store_ok():
        return _sess_store.get_pending_clarification(sid)
    return None


def _db_history_content(ans) -> str:
    """会话历史内容 = 叙述 + 紧凑数据预览。

    让“第一篇/上一篇/其中 X”这类自由追问能在 history 中引用上一轮的
    真实列名与值；预览有界（3 行、每值 60 字、整体 2000 字），与既有
    存储截断一致，不扩大敏感面（消息本就保存于本地会话库）。
    """
    narrative = (getattr(ans, "narrative", "") or "（无回答）")
    parts: list = []
    columns = getattr(ans, "columns", None)
    rows = getattr(ans, "rows", None)
    if columns and rows:
        parts.append("结果列：" + ",".join(str(c) for c in list(columns)[:6]))
        for row in list(rows)[:3]:
            parts.append("行：" + ",".join(
                (str(v)[:60] if v is not None else "") for v in list(row)[:6]
            ))
    else:
        for ev in list(getattr(ans, "evidence", None) or [])[:3]:
            matched = str(ev.get("matched") or "")
            row = ev.get("row") or []
            parts.append("证据[" + matched + "]：" + ",".join(
                (str(v)[:60] if v is not None else "") for v in list(row)[:6]
            ))
    if parts:
        return (narrative[:800] + "\n" + "\n".join(parts))[:2000]
    return narrative


def _db_session_display_value(value: Any, depth: int = 0) -> Any:
    """Bound a UI snapshot without turning it into model conversation state."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<BLOB {len(value)} bytes>"
    if isinstance(value, str):
        return value[:1000]
    if depth >= 7:
        return str(value)[:240]
    if isinstance(value, dict):
        return {
            str(key)[:120]: _db_session_display_value(item, depth + 1)
            for key, item in list(value.items())[:64]
        }
    if isinstance(value, (list, tuple)):
        return [
            _db_session_display_value(item, depth + 1)
            for item in list(value)[:64]
        ]
    return str(value)[:240]


def _db_session_display_rows(rows: Any, column_count: int) -> list:
    if not isinstance(rows, list):
        return []
    width = max(1, min(int(column_count or 0), 64))
    output = []
    for raw_row in rows[:_DB_SESSION_DISPLAY_ROWS_MAX]:
        row = list(raw_row) if isinstance(raw_row, (list, tuple)) else [raw_row]
        safe_row = []
        for value in row[:width]:
            safe_value = _db_session_display_value(value)
            safe_row.append(safe_value[:240] if isinstance(safe_value, str) else safe_value)
        output.append(safe_row)
    return output


def _db_session_display_payload(ans: Any) -> Optional[dict]:
    """Create a bounded, read-only card snapshot for chat switching.

    The old persistence contract kept only three text rows for LLM follow-up.
    This separate payload keeps the rows the desktop can actually display while
    deliberately excluding write forms, confirmation IDs and actionable DDL.
    """
    answer = _db_answer_to_dict(ans)
    kind = str(answer.get("kind") or "")
    if kind not in {"conversation", "schema", "query", "retrieve", "compose", "error"}:
        return None
    columns = [str(item)[:240] for item in list(answer.get("columns") or [])[:64]]
    source_rows = answer.get("rows") if isinstance(answer.get("rows"), list) else []
    payload: dict = {
        "kind": kind,
        "narrative": str(answer.get("narrative") or "")[:4000],
        "columns": columns,
        "rows": _db_session_display_rows(source_rows, len(columns)),
        "row_count": len(source_rows),
    }
    for key, limit in (("sql", 12000), ("error", 2000)):
        if answer.get(key):
            payload[key] = str(answer[key])[:limit]
    datasets = []
    for raw in list(answer.get("datasets") or [])[:6]:
        if not isinstance(raw, dict):
            continue
        dataset_columns = [
            str(item)[:240] for item in list(raw.get("columns") or [])[:64]
        ]
        dataset_rows = raw.get("rows") if isinstance(raw.get("rows"), list) else []
        datasets.append({
            "node_id": str(raw.get("node_id") or "")[:120],
            "label": str(raw.get("label") or "")[:240],
            "summary": str(raw.get("summary") or "")[:1000],
            "sql": str(raw.get("sql") or "")[:12000],
            "columns": dataset_columns,
            "rows": _db_session_display_rows(dataset_rows, len(dataset_columns)),
            "row_count": len(dataset_rows),
        })
    if datasets:
        payload["datasets"] = datasets
    for key in (
        "evidence", "steps", "operation", "graph", "semantic", "calendar_plan",
        "metric_plan", "dimension_plan", "trend_plan", "relational_plan",
    ):
        if answer.get(key):
            payload[key] = _db_session_display_value(answer[key])

    def payload_size() -> int:
        return len(json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), default=str,
        ).encode("utf-8"))

    # Normal query/schema cards stay well below this.  For unusually wide or
    # text-heavy results, reduce only the stored preview while retaining the
    # true row_count so the restored card remains honest about result size.
    while payload_size() > _DB_SESSION_DISPLAY_BYTES_MAX:
        holders = [payload] + list(payload.get("datasets") or [])
        reducible = [holder for holder in holders if len(holder.get("rows") or []) > 3]
        if reducible:
            for holder in reducible:
                rows = holder.get("rows") or []
                holder["rows"] = rows[:max(3, len(rows) // 2)]
            continue
        removed = False
        for key in ("graph", "steps", "relational_plan", "semantic", "sql"):
            if key in payload:
                payload.pop(key, None)
                removed = True
                break
        if not removed:
            return {
                "kind": kind,
                "narrative": payload["narrative"][:1000],
                "columns": columns,
                "rows": payload["rows"][:3],
                "row_count": len(source_rows),
                "snapshot_limited": True,
            }
    payload["snapshot_limited"] = len(payload["rows"]) < len(source_rows)
    return payload


def _db_session_append(
    sid: str,
    role: str,
    content: str,
    display_payload: Optional[dict] = None,
) -> None:
    """回答完成后追加 assistant 消息，并刷新最近使用时间（内存 + 持久化）。"""
    if not sid:
        return
    entry = _DB_SESSIONS.get(sid)
    if entry is not None:
        msgs = list(entry.get("messages", []))
        message = {"role": role, "content": str(content)[:2000]}
        if isinstance(display_payload, dict):
            message["display"] = display_payload
        msgs.append(message)
        entry["messages"] = msgs[-_DB_SESSION_HISTORY_MAX:]
        entry["updatedAt"] = time.time()
    if _db_sess_store_ok():
        _sess_store.append_message(
            sid, role, str(content)[:2000], display_payload=display_payload,
        )
        _sess_store.touch_updated_at(sid)


def _db_session_restore() -> None:
    """bridge 启动时从持久化 store 恢复热缓存（只恢复元信息，消息按需读）。"""
    if not _db_sess_store_ok():
        return
    try:
        for s in _sess_store.list_sessions(limit=_DB_SESSIONS_MAX):
            _DB_SESSIONS[s["id"]] = {
                "id": s["id"],
                "dbId": s.get("dbId") or "",
                "accessScopeRef": s.get("accessScopeRef") or "all",
                "lastQuestion": s.get("lastQuestion") or "",
                "count": s.get("count") or 0,
                "createdAt": s.get("createdAt") or time.time(),
                "updatedAt": s.get("updatedAt") or time.time(),
                "title": s.get("title") or "",
                "messages": [],
            }
    except Exception:
        pass


async def db_sessions_handler(request):
    q = str(request.query.get("q") or "").strip()
    sessions = []
    if _db_sess_store_ok():
        sessions = _sess_store.list_sessions(q=q)
        # 用热缓存补齐 messages 与 title（store 权威）
        hot = {s["id"]: s for s in _DB_SESSIONS.values()}
        for s in sessions:
            h = hot.get(s["id"])
            if h:
                s["messages"] = h.get("messages") or []
                if h.get("title"):
                    s["title"] = h["title"]
    else:
        sessions = sorted(_DB_SESSIONS.values(), key=lambda s: s["updatedAt"], reverse=True)
        if q:
            sessions = [
                s
                for s in sessions
                if q.lower() in (s.get("lastQuestion") or "").lower()
                or q.lower() in (s.get("title") or "").lower()
            ]
    sessions = [
        session for session in sessions
        if _stored_access_scope_allowed(
            session.get("dbId"), session.get("accessScopeRef"),
        )
    ]
    return json_ok({"ok": True, "sessions": sessions})


async def db_session_detail_handler(request):
    sid = request.match_info["sid"]
    if _db_sess_store_ok():
        s = _sess_store.get_session(sid, include_messages=False)
        if s is None:
            return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
        if not _stored_access_scope_allowed(s.get("dbId"), s.get("accessScopeRef")):
            return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
        s = _sess_store.get_session(sid)
        if s is None:
            return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
        # 合并内存热缓存中较新的 messages（未落盘前的瞬态）
        hot = _DB_SESSIONS.get(sid)
        if hot and hot.get("messages"):
            s["messages"] = hot["messages"]
        return json_ok({"ok": True, "session": s})
    s = _DB_SESSIONS.get(sid)
    if s is None or not _stored_access_scope_allowed(
        s.get("dbId"), s.get("accessScopeRef"),
    ):
        return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
    return json_ok({"ok": True, "session": s})


async def db_session_rename_handler(request):
    sid = request.match_info["sid"]
    session = _session_record_unchecked(sid)
    if session is None or not _stored_access_scope_allowed(
        session.get("dbId"), session.get("accessScopeRef"),
    ):
        return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
    data = await read_json(request)
    title = str(data.get("title") or "").strip()[:100]
    if not title:
        return json_ok({"ok": False, "error": "title required"}, status=400)
    ok = False
    if _db_sess_store_ok():
        ok = _sess_store.rename_session(sid, title)
    if _DB_SESSIONS.get(sid) is not None:
        _DB_SESSIONS[sid]["title"] = title
        _DB_SESSIONS[sid]["updatedAt"] = time.time()
        ok = True
    if not ok:
        return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
    return json_ok({"ok": True, "title": title})


async def db_session_delete_handler(request):
    sid = request.match_info["sid"]
    session = _session_record_unchecked(sid)
    if session is None or not _stored_access_scope_allowed(
        session.get("dbId"), session.get("accessScopeRef"),
    ):
        return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
    existed_in_memory = sid in _DB_SESSIONS
    ok = False
    if _db_sess_store_ok():
        ok = _sess_store.delete_session(sid)
    _DB_SESSIONS.pop(sid, None)
    if not ok and not existed_in_memory:
        return json_ok({"ok": False, "error": f"session not found: {sid}"}, status=404)
    return json_ok({"ok": True})


# ---------------------------------------------------------------------------
# DBQuill —— 图表数据 API（只读聚合，白名单校验，防注入）
# ---------------------------------------------------------------------------
_CHART_AGGS = {"count", "sum", "avg", "max", "min"}
_CHART_AGG_CN = {"count": "计数", "sum": "求和", "avg": "平均", "max": "最大", "min": "最小"}
_CHART_MAX_ROWS = 120
_CHART_MAX_TABLES = 128
_CHART_LABEL_MAX_CHARS = 120


def _db_quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _db_entry(db_id: str):
    """Resolve a database only when the current principal may access it."""
    entry = _db_entry_unchecked(db_id)
    return entry if _database_entry_allowed(entry) else None


def _install_sqlite_read_scope(conn: sqlite3.Connection, entry: dict) -> None:
    allowed_tables = _table_scope_for_entry(entry)
    column_scopes = {
        table.casefold(): frozenset(column.casefold() for column in columns)
        for table, columns in _column_scopes_for_entry(entry).items()
    }
    dc = _db_agent_core()
    row_filters = dc._normalize_row_scope(_row_scopes_for_entry(entry))
    if allowed_tables is None and not column_scopes and not row_filters:
        return
    row_internal = dc._prepare_sqlite_row_views(
        conn, row_filters, column_scopes,
    ) if row_filters else {}
    dc._install_sqlite_scope_authorizer(
        conn,
        allowed_tables=allowed_tables,
        allowed_columns=column_scopes,
        row_internal_columns=row_internal,
        allow_writes=False,
        unavailable_error="当前 SQLite 运行时不支持行级读取授权",
    )


def _db_chart_exec(db_id: str, table: str, x: str, y: str, agg: str) -> dict:
    """执行白名单聚合查询（只读连接，返回 labels/values + 中文表头映射）。"""
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    if not _table_allowed(entry, table):
        raise ValueError(f"table not found: {table}")
    path = entry.get("path")
    if not path or not os.path.isfile(path):
        raise ValueError(f"database file missing: {path}")
    if agg not in _CHART_AGGS:
        raise ValueError(f"agg must be one of {sorted(_CHART_AGGS)}")
    tables = {t for t in _db_validate_sqlite(path)}
    if table not in tables:
        raise ValueError(f"table not found: {table}")
    cols = list(_db_table_columns(path, table, entry))
    if x not in cols:
        raise ValueError(f"x column not found: {x}")
    if agg != "count" and y not in cols:
        raise ValueError(f"y column not found: {y}")

    qt, qx = _db_quote_ident(table), _db_quote_ident(x)
    if agg == "count":
        sql = f'SELECT {qx} AS label, COUNT(*) AS value FROM {qt} GROUP BY {qx} ORDER BY value DESC LIMIT {_CHART_MAX_ROWS}'
    else:
        qy = _db_quote_ident(y)
        sql = f'SELECT {qx} AS label, {agg}({qy}) AS value FROM {qt} GROUP BY {qx} ORDER BY value DESC LIMIT {_CHART_MAX_ROWS}'
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        _install_sqlite_read_scope(conn, entry)
        rows = [dict(r) for r in conn.execute(sql)]
    labels = [_db_chart_display_label(r["label"]) for r in rows]
    values = [r["value"] for r in rows]
    return {
        "labels": labels,
        "values": values,
        "meta": {
            "table": table,
            "x": x,
            "y": y if agg != "count" else "",
            "agg": agg,
            "aggCn": _CHART_AGG_CN[agg],
            "title": f"{table} 按 {x} 的 {_CHART_AGG_CN[agg]}" + (f"({y})" if agg != "count" else ""),
        },
    }


def _db_table_columns(path: str, table: str, entry: Optional[dict] = None) -> list:
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
        columns = [r[1] for r in conn.execute(f'PRAGMA table_info({_db_quote_ident(table)})')]
    if entry is not None:
        columns = [column for column in columns if _column_allowed(entry, table, column)]
    return columns


def _db_chart_display_label(value: Any) -> str:
    if value is None:
        return "未填写"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<二进制 {len(value)}B>"
    text = str(value).replace("\x00", "").strip() or "空字符串"
    if len(text) > _CHART_LABEL_MAX_CHARS:
        return text[: _CHART_LABEL_MAX_CHARS - 1] + "…"
    return text


def _db_chart_is_number(value: Any) -> bool:
    if value is None or value == "" or isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        number = float(str(value).strip())
        return number == number and number not in (float("inf"), float("-inf"))
    except (TypeError, ValueError):
        return False


def _db_chart_column_profiles(
    conn: sqlite3.Connection,
    table: str,
    columns: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Infer chart-safe roles from one bounded sample on the scoped connection."""
    projection = ", ".join(_db_quote_ident(item["name"]) for item in columns)
    rows = []
    if projection:
        try:
            rows = [dict(row) for row in conn.execute(
                f"SELECT {projection} FROM {_db_quote_ident(table)} LIMIT 40"
            )]
        except sqlite3.Error:
            rows = []
    profiles: dict[str, dict[str, Any]] = {}
    for position, column in enumerate(columns):
        name = str(column["name"])
        declared = str(column.get("declared") or "").upper()
        folded = name.casefold()
        values = [row.get(name) for row in rows if row.get(name) not in (None, "")]
        if "BLOB" in declared or any(isinstance(value, (bytes, bytearray, memoryview)) for value in values):
            kind = "blob"
        elif "BOOL" in declared:
            kind = "cat"
        elif any(token in declared for token in ("DATE", "TIME")) or any(
            token in folded for token in ("date", "time", "_at", "year", "month", "day")
        ):
            kind = "date"
        elif any(token in declared for token in ("INT", "REAL", "NUM", "DOUBLE", "FLOAT", "DEC")):
            kind = "num"
        elif values and all(_db_chart_is_number(value) for value in values[:16]):
            kind = "num"
        else:
            kind = "cat"
        text_values = [_db_chart_display_label(value) for value in values[:40]]
        profiles[name] = {
            "kind": kind,
            "position": position,
            "primaryKey": bool(column.get("primaryKey")),
            "sampleDistinct": len(set(text_values)),
            "sampleCount": len(text_values),
            "sampleMaxLength": max((len(value) for value in text_values), default=0),
        }
    return profiles


def _db_chart_category_score(name: str, profile: dict[str, Any]) -> tuple[int, int]:
    folded = name.casefold()
    score = 0
    if re.search(
        r"(^|_)(status|state|type|kind|category|region|city|country|province|"
        r"department|unit|field|level|role|gender|stage|source|channel)(_|$)",
        folded,
    ):
        score += 7
    if profile["sampleCount"]:
        distinct = profile["sampleDistinct"]
        ratio = distinct / profile["sampleCount"]
        score += 4 if 2 <= distinct <= 12 else 2 if distinct <= 30 else 0
        if ratio > 0.9 and profile["sampleCount"] >= 12:
            score -= 4
    if profile["sampleMaxLength"] > 80:
        score -= 8
    if re.search(
        r"(^|_)(body|content|text|description|summary|abstract|excerpt|note|reason|"
        r"url|path|json|sql|hash|email|phone)(_|$)",
        folded,
    ):
        score -= 12
    return score, -int(profile["position"])


def _db_chart_metric_agg(name: str) -> str:
    folded = name.casefold()
    if re.search(r"(^|_)(rate|ratio|percent|percentage|score|confidence|quality|avg|average)(_|$)", folded):
        return "avg"
    return "sum"


def _db_chart_number(value: Any) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return str(value if value is not None else "0")
    if isinstance(value, float) and not (value == value and abs(value) != float("inf")):
        return "0"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{float(value):,.2f}".rstrip("0").rstrip(".")


def _db_chart_stats(
    conn: sqlite3.Connection,
    table: str,
    dimension: str = "",
    measure: str = "",
    agg: str = "count",
) -> dict[str, Any]:
    qt = _db_quote_ident(table)
    if not dimension:
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {qt}").fetchone()[0] or 0)
        return {
            "rowCount": row_count, "dimensionNonNull": row_count,
            "measureNonNull": row_count, "groupCount": 1 if row_count else 0,
            "overallValue": row_count,
        }
    qx = _db_quote_ident(dimension)
    overall = "COUNT(*)"
    measure_non_null = f"COUNT({qx})"
    if measure:
        qy = _db_quote_ident(measure)
        measure_non_null = f"COUNT({qy})"
        overall = f"{agg.upper()}({qy})"
    row = conn.execute(
        f"SELECT COUNT(*) AS row_count, COUNT({qx}) AS dimension_non_null, "
        f"COUNT(DISTINCT {qx}) + CASE WHEN COUNT(*) > COUNT({qx}) THEN 1 ELSE 0 END AS group_count, "
        f"{measure_non_null} AS measure_non_null, {overall} AS overall_value FROM {qt}"
    ).fetchone()
    return {
        "rowCount": int(row[0] or 0),
        "dimensionNonNull": int(row[1] or 0),
        "groupCount": int(row[2] or 0),
        "measureNonNull": int(row[3] or 0),
        "overallValue": row[4],
    }


def _db_chart_grouped_rows(
    conn: sqlite3.Connection,
    table: str,
    dimension: str,
    measure: str,
    agg: str,
    *,
    chronological: bool,
) -> tuple[list[str], list[Any]]:
    qt, qx = _db_quote_ident(table), _db_quote_ident(dimension)
    value_sql = "COUNT(*)" if agg == "count" else f"{agg.upper()}({_db_quote_ident(measure)})"
    order_sql = f"{qx} DESC" if chronological else "value DESC, label ASC"
    rows = list(conn.execute(
        f"SELECT {qx} AS raw_label, SUBSTR(CAST({qx} AS TEXT), 1, {_CHART_LABEL_MAX_CHARS}) AS label, "
        f"{value_sql} AS value FROM {qt} GROUP BY {qx} "
        f"ORDER BY {order_sql} LIMIT {_CHART_MAX_ROWS}"
    ))
    if chronological:
        rows.reverse()  # show the latest bounded window in natural time order
    return (
        [_db_chart_display_label(row[0]) for row in rows],
        [row[2] if isinstance(row[2], (int, float)) else 0 for row in rows],
    )


def _db_chart_summary(
    profile: str,
    labels: list[str],
    values: list[Any],
    stats: dict[str, Any],
    value_label: str,
) -> str:
    row_count = int(stats.get("rowCount") or 0)
    if profile == "size":
        return f"当前可见范围共 {_db_chart_number(row_count)} 行；未发现适合分组的业务维度。"
    if not labels or not values:
        return "当前可见范围内没有可用数据。"
    if profile == "trend":
        first, last = values[0], values[-1]
        if len(values) == 1:
            return f"当前仅有 1 个时间点，{value_label}为 {_db_chart_number(last)}。"
        direction = "上升" if last > first else "下降" if last < first else "持平"
        return (
            f"最新时间点 {labels[-1]} 的{value_label}为 {_db_chart_number(last)}，"
            f"较窗口起点{direction}。"
        )
    top_label = labels[0]
    top_value = values[0]
    overall = stats.get("overallValue")
    share = ""
    if isinstance(top_value, (int, float)) and isinstance(overall, (int, float)) and overall > 0:
        if value_label.startswith("记录数") or "求和" in value_label:
            share = f"，占整体 {top_value / overall:.1%}"
    return f"最高分组是“{top_label[:36]}”，{value_label}为 {_db_chart_number(top_value)}{share}。"


def _db_chart_source_fingerprint(path: str) -> str:
    """Fingerprint SQLite/WAL metadata so unchanged sources reuse charts."""
    database_path = Path(path).expanduser().resolve()
    files = []
    for role, candidate in (
        ("database", database_path),
        ("wal", Path(str(database_path) + "-wal")),
        ("journal", Path(str(database_path) + "-journal")),
    ):
        if not candidate.is_file():
            continue
        stat = candidate.stat()
        files.append({
            "role": role,
            "size": int(stat.st_size),
            "mtimeNs": int(stat.st_mtime_ns),
            "ctimeNs": int(stat.st_ctime_ns),
        })
    if not files or files[0]["role"] != "database":
        raise ValueError(f"database file missing: {database_path}")
    payload = json.dumps(files, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(
        f"dbagent-chart-source-v1:{payload}".encode("utf-8")
    ).hexdigest()


def _db_charts_auto(db_id: str, max_charts: int = _CHART_MAX_TABLES) -> list:
    """Generate one scoped, decision-useful chart per base table on one connection."""
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    path = entry.get("path")
    if not path or not os.path.isfile(path):
        raise ValueError(f"database file missing: {path}")
    cands: list[tuple[int, dict[str, Any]]] = []
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        table_specs = []
        for table, create_sql in _db_sqlite_business_objects(conn):
            if not _table_allowed(entry, table):
                continue
            # FTS virtual roots are search indexes, not independent business facts.
            if create_sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE"):
                continue
            info = list(conn.execute(f"PRAGMA table_info({_db_quote_ident(table)})"))
            columns = [
                {
                    "name": str(row[1]), "declared": str(row[2] or ""),
                    "primaryKey": bool(row[5]),
                }
                for row in info
                if _column_allowed(entry, table, str(row[1]))
            ]
            if columns:
                table_specs.append((table, columns))
        if len(table_specs) > max_charts:
            raise ValueError(f"可见业务表超过图表缓存上限 {max_charts} 张")

        _install_sqlite_read_scope(conn, entry)
        for table, columns in table_specs:
            try:
                profiles = _db_chart_column_profiles(conn, table, columns)
                date_columns = [
                    item["name"] for item in columns
                    if profiles[item["name"]]["kind"] == "date"
                ]
                numeric_columns = [
                    item["name"] for item in columns
                    if profiles[item["name"]]["kind"] == "num"
                    and not profiles[item["name"]]["primaryKey"]
                    and item["name"].casefold() != "id"
                    and not item["name"].casefold().endswith("_id")
                ]
                category_columns = [
                    item["name"] for item in columns
                    if profiles[item["name"]]["kind"] == "cat"
                    and not profiles[item["name"]]["primaryKey"]
                    and item["name"].casefold() not in {"id", "key"}
                ]
                category_columns.sort(
                    key=lambda name: _db_chart_category_score(name, profiles[name]),
                    reverse=True,
                )
                category_columns = [
                    name for name in category_columns
                    if _db_chart_category_score(name, profiles[name])[0] > -8
                ]

                dimension = ""
                measure = ""
                agg = "count"
                profile = "size"
                chart_type = "bar"
                priority = 3
                if date_columns and numeric_columns:
                    dimension, measure = date_columns[0], numeric_columns[0]
                    agg = _db_chart_metric_agg(measure)
                    profile, chart_type, priority = "trend", "line", 0
                elif category_columns and numeric_columns:
                    dimension, measure = category_columns[0], numeric_columns[0]
                    agg = _db_chart_metric_agg(measure)
                    profile, chart_type, priority = "breakdown", "bar", 1
                elif category_columns:
                    dimension = category_columns[0]
                    profile, chart_type, priority = "breakdown", "bar", 2

                stats = _db_chart_stats(conn, table, dimension, measure, agg)
                if profile == "size":
                    labels = ["记录数"]
                    values = [stats["rowCount"]]
                    value_label = "记录数"
                    title = f"{table} · 记录规模"
                else:
                    labels, values = _db_chart_grouped_rows(
                        conn, table, dimension, measure, agg,
                        chronological=profile == "trend",
                    )
                    value_label = (
                        "记录数" if agg == "count" else
                        f"{_CHART_AGG_CN[agg]}({measure})"
                    )
                    title = (
                        f"{table} · {measure} 趋势" if profile == "trend" else
                        f"{table} · 按 {dimension} {_CHART_AGG_CN[agg]}"
                        + (f" {measure}" if measure else "记录")
                    )
                    if (
                        profile == "breakdown"
                        and 2 <= stats["groupCount"] <= 8
                        and values
                        and all(isinstance(value, (int, float)) and value >= 0 for value in values)
                    ):
                        chart_type = "pie"

                point_count = len(labels)
                row_count = int(stats["rowCount"] or 0)
                dimension_non_null = int(stats["dimensionNonNull"] or 0)
                coverage = dimension_non_null / row_count if row_count else 0.0
                meta = {
                    "table": table,
                    "x": dimension,
                    "y": measure,
                    "agg": agg,
                    "aggCn": _CHART_AGG_CN[agg],
                    "title": title,
                    "type": chart_type,
                    "profile": profile,
                    "profileLabel": {
                        "trend": "时间趋势", "breakdown": "分类贡献", "size": "记录规模",
                    }[profile],
                    "dimensionLabel": dimension or "无分组维度",
                    "valueLabel": value_label,
                    "rowCount": row_count,
                    "groupCount": int(stats["groupCount"] or 0),
                    "pointCount": point_count,
                    "coverage": round(coverage, 4),
                    "truncated": int(stats["groupCount"] or 0) > point_count,
                    "overallValue": stats.get("overallValue"),
                    "summary": _db_chart_summary(profile, labels, values, stats, value_label),
                }
                cands.append((priority, {"labels": labels, "values": values, "meta": meta}))
            except (sqlite3.Error, ValueError, TypeError):
                # A single malformed table must not block the remaining dashboard.
                continue
    cands.sort(key=lambda item: (
        item[0], str(item[1]["meta"].get("table") or "").casefold(),
    ))
    return [chart for _, chart in cands]


def _db_chart_cache_context(db_id: str) -> tuple[dict, str, str, str]:
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    if entry.get("conn"):
        raise ValueError("远程数据库图表缓存尚未验证")
    path = str(entry.get("path") or "")
    if not path or not os.path.isfile(path):
        raise ValueError(f"database file missing: {path}")
    return (
        entry,
        _database_scope_ref(entry),
        _current_access_scope_ref(entry),
        _db_chart_source_fingerprint(path),
    )


def _db_charts_cached(db_id: str, *, force_refresh: bool = False) -> dict:
    """Load a durable chart snapshot or rebuild after source/manual invalidation."""
    entry, database_ref, access_scope_ref, source_fingerprint = _db_chart_cache_context(db_id)
    cached = None
    if _chart_cache is not None:
        cached = _chart_cache.load_snapshot(database_ref, access_scope_ref)
    if (
        not force_refresh
        and cached is not None
        and cached.get("sourceFingerprint") == source_fingerprint
    ):
        return {
            "charts": cached.get("charts") or [],
            "cache": {
                "status": "hit",
                "generatedAt": cached.get("generatedAt") or "",
                "sourceFingerprint": source_fingerprint[:16],
            },
        }

    previous_exists = cached is not None
    charts = []
    stable_fingerprint = source_fingerprint
    for attempt in range(2):
        before = _db_chart_source_fingerprint(str(entry.get("path") or ""))
        charts = _db_charts_auto(db_id)
        after = _db_chart_source_fingerprint(str(entry.get("path") or ""))
        if before == after:
            stable_fingerprint = after
            break
        if attempt == 1:
            raise RuntimeError("数据库在图表生成期间持续变化，请稍后重试")

    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    cache_status = (
        "manual_refresh" if force_refresh
        else "database_changed" if previous_exists
        else "generated"
    )
    if _chart_cache is not None:
        _chart_cache.replace_snapshot(
            database_ref,
            access_scope_ref,
            stable_fingerprint,
            charts,
            generated_at=generated_at,
        )
    else:
        cache_status = "uncached"
    return {
        "charts": charts,
        "cache": {
            "status": cache_status,
            "generatedAt": generated_at,
            "sourceFingerprint": stable_fingerprint[:16],
        },
    }


def _db_charts_cache_status(db_id: str) -> dict:
    """Return a cheap invalidation check without returning or rebuilding charts."""
    _, database_ref, access_scope_ref, source_fingerprint = _db_chart_cache_context(db_id)
    cached = None
    if _chart_cache is not None:
        cached = _chart_cache.load_snapshot(database_ref, access_scope_ref)
    return {
        "changed": cached is None or cached.get("sourceFingerprint") != source_fingerprint,
        "hasCache": cached is not None,
        "generatedAt": (cached or {}).get("generatedAt") or "",
        "sourceFingerprint": source_fingerprint[:16],
    }


async def db_charts_auto_handler(request):
    try:
        data = await read_json(request)
    except Exception:
        data = {}
    db_id = str(data.get("dbId") or "").strip()
    force_refresh = data.get("refresh") is True
    if not db_id:
        return json_ok({"ok": False, "error": "dbId required"}, status=400)
    try:
        # Chart discovery is read-only but can scan many tables on a cold cache;
        # keep it off aiohttp's event loop so the rest of the desktop stays responsive.
        result = await asyncio.to_thread(
            _db_charts_cached, db_id, force_refresh=force_refresh,
        )
        charts = result["charts"]
        return json_ok({
            "ok": True,
            "charts": charts,
            "count": len(charts),
            "cache": result["cache"],
        })
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"charts failed: {exc}"}, status=500)


async def db_charts_cache_status_handler(request):
    try:
        data = await read_json(request)
    except Exception:
        data = {}
    db_id = str(data.get("dbId") or "").strip()
    if not db_id:
        return json_ok({"ok": False, "error": "dbId required"}, status=400)
    try:
        return json_ok({
            "ok": True,
            "cache": await asyncio.to_thread(_db_charts_cache_status, db_id),
        })
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"chart cache check failed: {exc}"}, status=500)


async def db_chart_data_handler(request):
    try:
        data = await read_json(request)
    except Exception:
        data = {}
    db_id = str(data.get("dbId") or "").strip()
    table = str(data.get("table") or "").strip()
    x = str(data.get("x") or "").strip()
    y = str(data.get("y") or "").strip()
    agg = str(data.get("agg") or "count").strip().lower()
    if not db_id or not table or not x:
        return json_ok({"ok": False, "error": "dbId/table/x required"}, status=400)
    try:
        result = _db_chart_exec(db_id, table, x, y, agg)
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"chart query failed: {exc}"}, status=500)
    return json_ok({"ok": True, **result})


async def db_tables_handler(request):
    """GET /db/tables?dbId=xxx —— 返回表结构（供图表配置/定时任务 SQL 选择器用）。"""
    db_id = str(request.query.get("dbId") or "").strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    try:
        if entry.get("conn"):
            snapshot = _db_get_agent(db_id).schema
            tables = [{
                "name": table.name,
                "columns": [{"name": column.name, "type": column.type} for column in table.columns],
            } for table in snapshot.tables.values()]
        else:
            path = entry.get("path")
            if not path or not os.path.isfile(path):
                return json_ok({"ok": False, "error": f"database file missing: {path}"}, status=404)
            snapshot = _db_get_agent(db_id).schema
            tables = [{
                "name": table.name,
                "columns": [{"name": column.name, "type": column.type} for column in table.columns],
            } for table in snapshot.tables.values()]
    except Exception as exc:
        return json_ok({"ok": False, "error": f"tables failed: {exc}"}, status=500)
    return json_ok({"ok": True, "tables": tables})


async def db_semantics_handler(request):
    """GET/POST /db/semantics —— 管理当前数据源的持久语义目录。"""
    db_id = str(request.query.get("dbId") or "").strip()
    data = None
    if request.method == "POST":
        data = await read_json(request)
        db_id = str(data.get("dbId") or db_id).strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if _semantic_store is None:
        return json_ok({"ok": False, "error": "语义目录存储不可用"}, status=500)

    if request.method == "GET":
        try:
            dc = _db_agent_core()
            agent = _db_get_agent(db_id)
            entries = _db_semantics(entry, agent.schema)
            invalid = (
                [] if _column_scopes_for_entry(entry)
                else getattr(agent.semantic_catalog, "invalid_entries", [])
            )
            return json_ok({
                "ok": True,
                "entries": entries,
                "invalid": invalid,
                "timezone_runtime": dc.TimezoneRuntime.status(),
            })
        except Exception as exc:
            return json_ok({"ok": False, "error": f"读取语义目录失败：{exc}"}, status=500)

    raw_entry = data.get("entry") if isinstance(data, dict) else None
    audit_operation_ref = uuid.uuid4().hex[:20]
    try:
        dc = _db_agent_core()
        agent = _db_get_agent(db_id)
        current_entries = _db_semantics(entry, agent.schema)
        entry_id = str((raw_entry or {}).get("id") or "").strip()
        if entry_id and not any(
            str(item.get("id") or "") == entry_id for item in current_entries
        ):
            raise ValueError("语义定义不存在或不在当前授权范围")
        validation_entries = [item for item in current_entries if item.get("id") != entry_id]
        validation_entries.append(raw_entry)
        catalog = dc.SemanticCatalog(agent.schema, validation_entries, strict=True)
        validated = catalog.entries[-1]
        if entry_id:
            validated["id"] = entry_id
        semantic_ref = _audit_ref(validated.get("term") or "")
        audit_details = {
            "semantic_kind": str(validated.get("kind") or ""),
            "semantic_ref": semantic_ref,
            "target_count": 1,
            "target_refs": [_audit_ref(validated.get("table") or "")],
        }
        _audit_append(
            db_id,
            category="semantic_change",
            action="upsert",
            outcome="approved",
            summary="语义定义变更已通过校验",
            risk="medium",
            correlation_id=audit_operation_ref,
            details=audit_details,
            strict=True,
        )
        saved = _semantic_store.upsert_entry(_db_semantic_key(entry), validated)
        _DB_AGENT_CACHE.pop(db_id, None)
        _audit_append(
            db_id,
            category="semantic_change",
            action="upsert",
            outcome="succeeded",
            summary="语义定义已保存",
            risk="medium",
            correlation_id=audit_operation_ref,
            details=audit_details,
        )
        return json_ok({"ok": True, "entry": saved})
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except ValueError as exc:
        raw = raw_entry if isinstance(raw_entry, dict) else {}
        semantic_ref = _audit_ref(raw.get("term") or "")
        _audit_append(
            db_id,
            category="semantic_change",
            action="upsert",
            outcome="rejected",
            summary="语义定义校验未通过",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={
                "semantic_kind": str(raw.get("kind") or "unknown"),
                "semantic_ref": semantic_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        _audit_append(
            db_id,
            category="semantic_change",
            action="upsert",
            outcome="failed",
            summary="语义定义保存失败",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": f"保存语义定义失败：{exc}"}, status=500)


async def db_semantics_delete_handler(request):
    db_id = str(request.query.get("dbId") or "").strip()
    semantic_id = str(request.match_info.get("semantic_id") or "").strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if _semantic_store is None:
        return json_ok({"ok": False, "error": "语义目录存储不可用"}, status=500)
    semantic_ref = _audit_ref(semantic_id)
    audit_operation_ref = uuid.uuid4().hex[:20]
    try:
        visible_entry = next(
            (
                item for item in _db_semantics(entry, _db_get_agent(db_id).schema)
                if str(item.get("id") or "") == semantic_id
            ),
            None,
        )
        if visible_entry is None:
            return json_ok({
                "ok": False,
                "error": f"semantic entry not found: {semantic_id}",
            }, status=404)
        _audit_append(
            db_id,
            category="semantic_change",
            action="delete",
            outcome="approved",
            summary="语义定义删除请求已登记",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={"semantic_ref": semantic_ref},
            strict=True,
        )
        deleted = _semantic_store.delete_entry(_db_semantic_key(entry), semantic_id)
        if not deleted:
            _audit_append(
                db_id,
                category="semantic_change",
                action="delete",
                outcome="rejected",
                summary="语义定义不存在",
                risk="medium",
                correlation_id=audit_operation_ref,
                details={"semantic_ref": semantic_ref, "http_status": 404},
            )
            return json_ok({"ok": False, "error": f"semantic entry not found: {semantic_id}"}, status=404)
        _DB_AGENT_CACHE.pop(db_id, None)
        _audit_append(
            db_id,
            category="semantic_change",
            action="delete",
            outcome="succeeded",
            summary="语义定义已删除",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={"semantic_ref": semantic_ref},
        )
        return json_ok({"ok": True})
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except Exception as exc:
        _audit_append(
            db_id,
            category="semantic_change",
            action="delete",
            outcome="failed",
            summary="语义定义删除失败",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={
                "semantic_ref": semantic_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": f"删除语义定义失败：{exc}"}, status=500)


async def db_semantics_export_handler(request):
    """导出不含本地 ID、时间戳、路径或连接凭据的版本化语义配置。"""
    db_id = str(request.query.get("dbId") or "").strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if _semantic_store is None:
        return json_ok({"ok": False, "error": "语义目录存储不可用"}, status=500)
    try:
        # 通过当前 schema 重新构建后的目录导出，确保旧配置获得显式的
        # legacy_default 标记，且失效定义不会被包装成当前协议版本。
        agent = _db_get_agent(db_id)
        entries = [
            _semantic_portable_entry(item)
            for item in agent.semantic_catalog.entries
        ]
        catalog = {
            "format": _SEMANTIC_EXPORT_FORMAT,
            "schema_version": _SEMANTIC_EXPORT_SCHEMA_VERSION,
            "semantic_version": "2.8",
            "exported_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "source": {
                "name": str(entry.get("name") or ""),
                "kind": str(entry.get("kind") or (entry.get("conn") or {}).get("dialect") or "sqlite"),
            },
            "entries": entries,
        }
        return json_ok({"ok": True, "catalog": catalog})
    except Exception as exc:
        return json_ok({"ok": False, "error": f"导出语义配置失败：{exc}"}, status=500)


async def db_semantics_import_preflight_handler(request):
    """第一阶段：只解析并以当前 schema/目录预检，不执行写入。"""
    if (request.content_length or 0) > _SEMANTIC_IMPORT_REQUEST_MAX_BYTES:
        return json_ok({"ok": False, "error": "语义配置请求体过大"}, status=413)
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if _semantic_store is None:
        return json_ok({"ok": False, "error": "语义目录存储不可用"}, status=500)
    catalog_payload = data.get("catalog")
    try:
        if not isinstance(catalog_payload, dict):
            raise ValueError("语义配置必须是 JSON 对象")
        payload_bytes = json.dumps(
            catalog_payload, ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
        if len(payload_bytes) > _SEMANTIC_IMPORT_MAX_BYTES:
            raise ValueError("语义配置不能超过 512 KB")
        if catalog_payload.get("format") not in _SEMANTIC_IMPORT_FORMATS:
            raise ValueError("语义配置 format 不受支持")
        if catalog_payload.get("schema_version") not in _SEMANTIC_IMPORT_SCHEMA_VERSIONS:
            raise ValueError("语义配置 schema_version 不受支持")
        imported_raw = catalog_payload.get("entries")
        if not isinstance(imported_raw, list):
            raise ValueError("语义配置 entries 必须是列表")

        dc = _db_agent_core()
        schema = _db_get_agent(db_id).schema
        imported_catalog = dc.SemanticCatalog(schema, imported_raw, strict=True)
        imported = imported_catalog.entries
        database_key = _db_semantic_key(entry)
        all_current, revision = _semantic_store.list_entries_with_revision(database_key)
        current = _db_semantics(entry, schema)
        visible_ids = {str(item.get("id") or "") for item in current}
        invisible_term_keys = {
            str(item.get("term") or "").casefold()
            for item in all_current
            if str(item.get("id") or "") not in visible_ids
        }
        current_by_term = {str(item.get("term") or "").casefold(): item for item in current}
        merged_by_term = {str(item.get("term") or "").casefold(): item for item in current}
        preview = []
        changed = []
        counts = {"add": 0, "update": 0, "skip": 0}
        for item in imported:
            key = item["term"].casefold()
            if key in invisible_term_keys:
                raise ValueError("导入术语与当前授权范围外的既有定义冲突")
            old = current_by_term.get(key)
            if old is None:
                action = "add"
                changed.append(item)
            elif _semantic_entry_signature(old) == _semantic_entry_signature(item):
                action = "skip"
            else:
                action = "update"
                changed.append(item)
            counts[action] += 1
            merged_by_term[key] = item
            preview.append({
                "term": item["term"],
                "kind": item["kind"],
                "table": item["table"],
                "action": action,
            })

        dc.SemanticCatalog(schema, list(merged_by_term.values()), strict=True)
        token, expires_at = _semantic_import_register(
            database_key,
            revision,
            changed,
            _current_access_scope_ref(entry),
        )
        return json_ok({
            "ok": True,
            "preflight": {
                "token": token,
                "expiresAt": expires_at,
                "counts": counts,
                "entries": preview,
                "totalAfterImport": len(merged_by_term),
                "mode": "merge",
            },
        })
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        return json_ok({"ok": False, "error": f"预检语义配置失败：{exc}"}, status=500)


async def db_semantics_import_apply_handler(request):
    """第二阶段：消费一次性令牌，目录版本一致时原子合并预检条目。"""
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    token = str(data.get("token") or "").strip()
    entry = _db_entry(db_id)
    if not entry:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if _semantic_store is None:
        return json_ok({"ok": False, "error": "语义目录存储不可用"}, status=500)
    import_ref = _audit_ref(token)
    audit_operation_ref = uuid.uuid4().hex[:20]
    try:
        if not token:
            raise ValueError("缺少导入预检令牌")
        database_key = _db_semantic_key(entry)
        _audit_append(
            db_id,
            category="semantic_change",
            action="import",
            outcome="approved",
            summary="语义目录导入请求已登记",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={"semantic_ref": import_ref},
            strict=True,
        )
        pending = _semantic_import_consume(
            token, database_key, _current_access_scope_ref(entry),
        )
        saved = _semantic_store.import_entries(
            database_key, pending["entries"], pending["revision"],
        )
        _DB_AGENT_CACHE.pop(db_id, None)
        _audit_append(
            db_id,
            category="semantic_change",
            action="import",
            outcome="succeeded",
            summary="语义目录导入已应用",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={"semantic_ref": import_ref, "entry_count": len(saved)},
        )
        return json_ok({"ok": True, "imported": len(saved), "entries": saved})
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except ValueError as exc:
        _audit_append(
            db_id,
            category="semantic_change",
            action="import",
            outcome="rejected",
            summary="语义目录导入未通过",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={
                "semantic_ref": import_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        _audit_append(
            db_id,
            category="semantic_change",
            action="import",
            outcome="failed",
            summary="语义目录导入失败",
            risk="medium",
            correlation_id=audit_operation_ref,
            details={
                "semantic_ref": import_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": f"应用语义配置失败：{exc}"}, status=500)


def _audit_query_database_key(db_id: str) -> Optional[str]:
    if not db_id:
        return None
    entry = _db_entry(db_id)
    if not entry:
        raise ValueError(f"database not attached: {db_id}")
    return _db_semantic_key(entry)


async def db_audit_handler(request):
    """读取脱敏审计事件；完整性状态与事件列表一并返回。"""
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    try:
        db_id = str(request.query.get("dbId") or "").strip()
        if _database_scope_is_restricted() and not db_id:
            return json_ok({
                "ok": False,
                "error": "受限凭据读取审计记录时必须指定授权数据库",
            }, status=400)
        database_key = _audit_query_database_key(db_id)
        limit = int(request.query.get("limit") or 100)
        before_raw = str(request.query.get("before") or "").strip()
        before = int(before_raw) if before_raw else None
        category = str(request.query.get("category") or "").strip()
        outcome = str(request.query.get("outcome") or "").strip()
        events = _audit_store.list_events(
            limit=limit,
            before_sequence=before,
            category=category,
            outcome=outcome,
            database_key=database_key,
        )
        return json_ok({
            "ok": True,
            "events": events,
            "integrity": _audit_store.verify_chain(),
            "reconciliation": _audit_store.reconciliation_status(
                database_key=database_key if db_id else None,
            ),
            "retention": _audit_store.retention_status(365),
            "backups": (
                {"scope_restricted": True, "valid_count": 0, "invalid_count": 0}
                if _database_scope_is_restricted()
                else _audit_store.backup_status()
            ),
        })
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        return json_ok(
            {"ok": False, "error": f"读取审计账本失败: {type(exc).__name__}"}, status=500,
        )


async def db_audit_verify_handler(request):
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    try:
        db_id = str(request.query.get("dbId") or "").strip()
        if _database_scope_is_restricted() and not db_id:
            return json_ok({
                "ok": False,
                "error": "受限凭据校验审计记录时必须指定授权数据库",
            }, status=400)
        database_key = _audit_query_database_key(db_id)
        return json_ok({
            "ok": True,
            "integrity": _audit_store.verify_chain(),
            "reconciliation": _audit_store.reconciliation_status(
                database_key=database_key if db_id else None,
            ),
            "retention": _audit_store.retention_status(365),
            "backups": (
                {"scope_restricted": True, "valid_count": 0, "invalid_count": 0}
                if _database_scope_is_restricted()
                else _audit_store.backup_status()
            ),
        })
    except Exception as exc:
        return json_ok(
            {"ok": False, "error": f"校验审计账本失败: {type(exc).__name__}"}, status=500,
        )


async def db_audit_export_handler(request):
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    try:
        db_id = str(request.query.get("dbId") or "").strip()
        if _database_scope_is_restricted() and not db_id:
            return json_ok({
                "ok": False,
                "error": "受限凭据导出审计记录时必须指定授权数据库",
            }, status=400)
        database_key = _audit_query_database_key(db_id)
        payload = _audit_store.export_events(database_key=database_key, limit=200)
        return json_ok({"ok": True, "ledger": payload})
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except RuntimeError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=409)
    except Exception as exc:
        return json_ok(
            {"ok": False, "error": f"导出审计账本失败: {type(exc).__name__}"}, status=500,
        )


async def db_audit_reconciliation_resolve_handler(request):
    """Append an admin disposition for one unresolved audit operation."""
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    try:
        data = await read_json(request)
        db_id = str(data.get("dbId") or "").strip()
        if not db_id:
            return json_ok({"ok": False, "error": "missing dbId"}, status=400)
        database_key = _audit_query_database_key(db_id)
        sequence = data.get("sequence")
        if isinstance(sequence, bool):
            raise ValueError("sequence 必须是正整数")
        evidence_ref = str(data.get("evidenceRef") or "").strip()
        if not evidence_ref or len(evidence_ref) > 240 \
                or any(ord(char) < 32 for char in evidence_ref):
            raise ValueError("evidenceRef 必须是 1–240 个可打印字符")
        resolution = _audit_store.resolve_pending_event(
            sequence,
            disposition=data.get("disposition"),
            evidence_sha256=_audit_sha256(evidence_ref),
            expected_database_key=database_key,
            actor="local_admin",
        )
        return json_ok({
            "ok": True,
            "resolution": resolution,
            "reconciliation": _audit_store.reconciliation_status(
                database_key=database_key,
            ),
        })
    except KeyError:
        return json_ok({
            "ok": False,
            "error": "pending audit event not found",
        }, status=404)
    except ValueError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except RuntimeError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=409)
    except Exception as exc:
        return json_ok({
            "ok": False,
            "error": f"登记人工处置失败: {type(exc).__name__}",
        }, status=500)


async def db_audit_backups_handler(request):
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    try:
        return json_ok({
            "ok": True,
            "backups": _audit_store.list_backups(),
            "status": _audit_store.backup_status(),
        })
    except Exception as exc:
        return json_ok(
            {"ok": False, "error": f"读取审计备份失败: {type(exc).__name__}"}, status=500,
        )


async def db_audit_backup_create_handler(request):
    if _audit_store is None:
        return json_ok({"ok": False, "error": "审计账本不可用"}, status=503)
    operation_ref = uuid.uuid4().hex[:20]
    db_id = ""
    try:
        data = await read_json(request)
        db_id = str(data.get("dbId") or "").strip()
        if db_id and not _db_entry(db_id):
            return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
        _audit_append(
            db_id,
            category="audit_backup",
            action="create",
            outcome="approved",
            summary="审计账本备份请求已登记",
            risk="medium",
            correlation_id=operation_ref,
            strict=True,
        )
        backup = _audit_store.create_backup(reason="manual")
        _audit_append(
            db_id,
            category="audit_backup",
            action="create",
            outcome="succeeded",
            summary="审计账本备份已创建并校验",
            risk="medium",
            correlation_id=operation_ref,
            details={
                "backup_ref": _audit_ref(backup["backup_id"]),
                "backup_count": int(backup["count"]),
            },
        )
        return json_ok({"ok": True, "backup": backup})
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except Exception as exc:
        _audit_append(
            db_id,
            category="audit_backup",
            action="create",
            outcome="failed",
            summary="审计账本备份创建失败",
            risk="medium",
            correlation_id=operation_ref,
            details={
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok(
            {"ok": False, "error": f"创建审计备份失败: {type(exc).__name__}"}, status=500,
        )


# ---------------------------------------------------------------------------
# DBQuill —— 定时操作（db_scheduler 模块的 HTTP 封装）
# ---------------------------------------------------------------------------
def _db_sched_resolver(db_id: str):
    """把 dbId 解析为 db_scheduler 可用的 {path, conn}。"""
    entry = _db_entry(db_id)
    if not entry:
        return None
    return {"path": entry.get("path"), "conn": entry.get("conn")}


def _db_sched_require():
    if _db_sched is None:
        raise RuntimeError("db_scheduler 模块不可用")
    return _db_sched


def _db_sched_audit_sink(**event):
    db_id = str(event.pop("db_id", "") or "")
    return _audit_append(db_id, **event)


def _db_sched_mutation_details(task_type: str, schedule_ref: str) -> dict:
    return {
        "task_type": "nl" if str(task_type) == "nl" else "sql",
        "schedule_ref": schedule_ref,
        "target_count": 1,
    }


async def db_schedules_handler(request):
    try:
        tasks = [
            task for task in _db_sched_require().list_tasks()
            if _database_id_allowed(str(task.get("dbId") or ""))
            and _table_scope_for_entry(
                _db_entry_unchecked(str(task.get("dbId") or ""))
            ) is None
        ]
        return json_ok({
            "ok": True,
            "tasks": tasks,
            "databaseScope": _database_scope_summary(),
        })
    except Exception as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_schedules_create_handler(request):
    operation_ref = uuid.uuid4().hex[:20]
    db_id = ""
    details = _db_sched_mutation_details("sql", _audit_ref(operation_ref))
    try:
        data = await read_json(request)
        db_id = str(data.get("dbId") or "").strip()
        if not _db_entry(db_id):
            return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
        details = _db_sched_mutation_details(
            str(data.get("type") or "sql"), _audit_ref(operation_ref),
        )
        _audit_append(
            db_id,
            category="schedule_change", action="create", outcome="approved",
            summary="定时任务创建请求已登记", risk="medium",
            correlation_id=operation_ref, details=details, strict=True,
        )
        task = _db_sched_require().create_task(data)
        details = _db_sched_mutation_details(task.get("type"), _audit_ref(task["id"]))
        result_event = _audit_append(
            db_id,
            category="schedule_change", action="create", outcome="succeeded",
            summary="只读定时任务已创建", risk="medium",
            correlation_id=operation_ref, details=details,
        )
        return json_ok({
            "ok": True, "task": task,
            "audit": {"intent_recorded": True, "result_recorded": result_event is not None},
        })
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except ValueError as exc:
        _audit_append(
            db_id,
            category="schedule_change", action="create", outcome="rejected",
            summary="定时任务创建未通过安全校验", risk="medium",
            correlation_id=operation_ref,
            details={
                **details,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        _audit_append(
            db_id,
            category="schedule_change", action="create", outcome="failed",
            summary="定时任务创建失败", risk="medium",
            correlation_id=operation_ref,
            details={
                **details,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_schedules_update_handler(request):
    tid = request.match_info.get("id", "")
    operation_ref = uuid.uuid4().hex[:20]
    db_id = ""
    details = _db_sched_mutation_details("sql", _audit_ref(tid or operation_ref))
    try:
        data = await read_json(request)
        scheduler = _db_sched_require()
        existing = scheduler.get_task(tid)
        if existing is None:
            return json_ok({"ok": False, "error": f"task not found: {tid}"}, status=404)
        db_id = str(data.get("dbId") or existing.get("dbId") or "").strip()
        if not _db_entry(db_id):
            return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
        details = _db_sched_mutation_details(
            str(data.get("type") or existing.get("type") or "sql"), _audit_ref(tid),
        )
        _audit_append(
            db_id,
            category="schedule_change", action="update", outcome="approved",
            summary="定时任务修改请求已登记", risk="medium",
            correlation_id=operation_ref, details=details, strict=True,
        )
        task = scheduler.update_task(tid, data)
        result_event = _audit_append(
            db_id,
            category="schedule_change", action="update", outcome="succeeded",
            summary="只读定时任务已更新", risk="medium",
            correlation_id=operation_ref, details=details,
        )
        return json_ok({
            "ok": True, "task": task,
            "audit": {"intent_recorded": True, "result_recorded": result_event is not None},
        })
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except ValueError as exc:
        _audit_append(
            db_id,
            category="schedule_change", action="update", outcome="rejected",
            summary="定时任务修改未通过安全校验", risk="medium",
            correlation_id=operation_ref,
            details={
                **details,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        _audit_append(
            db_id,
            category="schedule_change", action="update", outcome="failed",
            summary="定时任务修改失败", risk="medium",
            correlation_id=operation_ref,
            details={
                **details,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_schedules_delete_handler(request):
    tid = request.match_info.get("id", "")
    operation_ref = uuid.uuid4().hex[:20]
    db_id = ""
    details = _db_sched_mutation_details("sql", _audit_ref(tid or operation_ref))
    try:
        scheduler = _db_sched_require()
        existing = scheduler.get_task(tid)
        if existing is None:
            return json_ok({"ok": False, "error": f"task not found: {tid}"}, status=404)
        db_id = str(existing.get("dbId") or "").strip()
        details = _db_sched_mutation_details(existing.get("type"), _audit_ref(tid))
        _audit_append(
            db_id,
            category="schedule_change", action="delete", outcome="approved",
            summary="定时任务删除请求已登记", risk="medium",
            correlation_id=operation_ref, details=details, strict=True,
        )
        ok = scheduler.delete_task(tid)
        if not ok:
            raise RuntimeError("定时任务在删除前已不存在")
        result_event = _audit_append(
            db_id,
            category="schedule_change", action="delete", outcome="succeeded",
            summary="定时任务已删除", risk="medium",
            correlation_id=operation_ref, details=details,
        )
        return json_ok({
            "ok": True,
            "audit": {"intent_recorded": True, "result_recorded": result_event is not None},
        })
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    except Exception as exc:
        _audit_append(
            db_id,
            category="schedule_change", action="delete", outcome="failed",
            summary="定时任务删除失败", risk="medium",
            correlation_id=operation_ref,
            details={
                **details,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_schedules_run_handler(request):
    tid = request.match_info.get("id", "")
    try:
        scheduler = _db_sched_require()
        existing = scheduler.get_task(tid)
        if existing is None or not _database_id_allowed(str(existing.get("dbId") or "")):
            return json_ok({"ok": False, "error": f"task not found: {tid}"}, status=404)
        result = scheduler.run_now(tid)
        if result is None:
            return json_ok({"ok": False, "error": f"task not found: {tid}"}, status=404)
        return json_ok({"ok": True, "result": result})
    except Exception as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_schedules_logs_handler(request):
    try:
        if _database_scope_is_restricted():
            return json_ok({"ok": True, "logs": [], "scopeRestricted": True})
        logs = _db_sched_require().list_logs()
        return json_ok({"ok": True, "logs": logs})
    except Exception as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=500)


async def db_ask_handler(request):
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    question = str(data.get("question") or "").strip()[:4000]
    sid = str(data.get("sessionId") or "").strip()[:64]
    llm_cfg = str(data.get("llmCfg") or "").strip()[:64]
    if not db_id or not question:
        return json_ok({"ok": False, "error": "missing dbId or question"}, status=400)
    if not _db_entry(db_id):
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    if not sid:
        sid = uuid.uuid4().hex[:12]
    else:
        existing_session = _session_record_unchecked(sid)
        existing_db_id = str((existing_session or {}).get("dbId") or "")
        if existing_db_id and existing_db_id != db_id:
            return json_ok({
                "ok": False,
                "error": "session is bound to a different database",
            }, status=409)
        if existing_session is not None and not _stored_access_scope_allowed(
            existing_db_id, existing_session.get("accessScopeRef"),
        ):
            return json_ok({
                "ok": False,
                "error": f"session not found: {sid}",
            }, status=404)
    history = _db_session_history(sid)  # 必须在 touch 前取，避免把本轮问题算进历史
    clarification = _db_session_clarification(sid)
    _db_session_touch(sid, db_id, question)
    run_id = uuid.uuid4().hex[:12]
    _DB_RUNS[run_id] = {
        "id": run_id, "dbId": db_id, "question": question, "sessionId": sid, "llmCfg": llm_cfg,
        "accessScopeRef": _current_access_scope_ref(_db_entry_unchecked(db_id)),
        "status": "running", "percent": 5, "stage": "intent",
        "label": "正在判断意图", "error": "", "result": None,
        "cancelRequested": False, "createdAt": time.time(),
        "clarification": clarification,
    }
    _DB_RUN_CANCEL_EVENTS[run_id] = threading.Event()
    if len(_DB_RUNS) > _DB_RUNS_MAX:
        for old_id in list(_DB_RUNS)[: len(_DB_RUNS) - _DB_RUNS_MAX]:
            if _DB_RUNS[old_id].get("status") in ("done", "error", "cancelled"):
                _DB_RUNS.pop(old_id, None)
                _DB_RUN_CANCEL_EVENTS.pop(old_id, None)
    request_context = contextvars.copy_context()
    threading.Thread(
        target=lambda: request_context.run(
            _db_ask_workflow, run_id, db_id, question, sid, history,
        ),
        daemon=True,
        name=f"DBAsk-{run_id}",
    ).start()
    return json_ok({"ok": True, "run": _db_run_view(run_id)}, status=202)


async def db_progress_handler(request):
    run_id = request.match_info["run_id"]
    view = _db_run_view(run_id)
    if view is None:
        return json_ok({"ok": False, "error": f"run not found: {run_id}"}, status=404)
    return json_ok({"ok": True, "run": view})


async def db_cancel_handler(request):
    run_id = request.match_info["run_id"]
    if _db_run_view(run_id) is None:
        return json_ok({"ok": False, "error": f"run not found: {run_id}"}, status=404)
    run = _DB_RUNS[run_id]
    if run.get("status") in ("done", "error", "cancelled"):
        return json_ok({"ok": True, "run": _db_run_view(run_id)})
    cancel_event = _DB_RUN_CANCEL_EVENTS.get(run_id)
    if cancel_event is not None:
        cancel_event.set()
    _db_run_update(
        run_id,
        cancelRequested=True,
        cancelAuditRecorded=True,
        status="cancelled",
        percent=100,
        stage="done",
        label="已取消",
        error="",
    )
    _audit_append(
        str(run.get("dbId") or ""),
        category="nl_operation",
        action="cancel",
        outcome="pending",
        summary="请求取消自然语言数据库操作",
        risk="low",
        session_id=str(run.get("sessionId") or ""),
        run_id=run_id,
        correlation_id=run_id,
        details={
            "question_sha256": _audit_sha256(run.get("question") or ""),
            "question_length": len(str(run.get("question") or "")),
            "cancel_requested": True,
        },
    )
    _audit_nl_terminal(
        run_id,
        str(run.get("dbId") or ""),
        str(run.get("question") or ""),
        str(run.get("sessionId") or ""),
        forced_outcome="cancelled",
    )
    return json_ok({"ok": True, "run": _db_run_view(run_id)})


async def db_write_form_handler(request):
    """返回当前授权范围内的可写表、字段与一行只读示例。"""
    db_id = str(request.query.get("dbId") or "").strip()
    table_name = str(request.query.get("table") or "").strip()
    if not db_id:
        return json_ok({"ok": False, "error": "missing dbId"}, status=400)
    if not _db_ensure_local_entry_attached(db_id):
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    try:
        answer = _db_get_agent(db_id).write_form(table_name)
    except Exception as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    payload = _db_answer_to_dict(answer)
    if answer.kind == "error":
        return json_ok({
            "ok": False,
            "error": answer.error or answer.narrative,
            "answer": payload,
        }, status=400)
    return json_ok({"ok": True, "answer": payload})


async def db_write_prepare_insert_handler(request):
    """把结构化单行表单转为可确认的回滚预览，此路由不落库。"""
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    table_name = str(data.get("table") or "").strip()
    fields = data.get("fields")
    if not db_id or not table_name:
        return json_ok({"ok": False, "error": "missing dbId or table"}, status=400)
    if not _db_ensure_local_entry_attached(db_id):
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    try:
        answer = _db_get_agent(db_id).prepare_structured_insert(table_name, fields)
    except Exception as exc:
        _audit_append(
            db_id,
            category="nl_operation",
            action="insert",
            outcome="failed",
            summary="结构化写入预览生成失败",
            risk="medium",
            details={
                "target_refs": [_audit_ref(table_name)],
                "target_count": 1,
                "answer_kind": "error",
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    payload = _db_answer_to_dict(answer)
    if answer.kind == "error":
        _audit_append(
            db_id,
            category="nl_operation",
            action="insert",
            outcome="rejected",
            summary="结构化写入未通过安全校验",
            risk="medium",
            details={
                "target_refs": [_audit_ref(table_name)],
                "target_count": 1,
                "answer_kind": "error",
                "error_sha256": _audit_sha256(answer.error or answer.narrative),
            },
        )
        return json_ok({
            "ok": False,
            "error": answer.error or answer.narrative,
            "answer": payload,
        }, status=400)
    confirm_id = str(answer.confirm_id or "")
    proposal = _db_agent_core().WRITE_REGISTRY.get(confirm_id)
    if proposal is not None:
        proposal.access_scope_ref = _current_access_scope_ref(
            _db_entry_unchecked(db_id),
        )
    confirm_ref = _audit_ref(confirm_id)
    _audit_append(
        db_id,
        category="nl_operation",
        action="insert",
        outcome="pending",
        summary="结构化写入已生成回滚预览，等待确认",
        risk="medium",
        correlation_id=confirm_ref,
        details={
            "target_refs": [_audit_ref(table_name)],
            "target_count": 1,
            "answer_kind": "write_pending",
            "confirm_ref": confirm_ref,
        },
    )
    return json_ok({"ok": True, "answer": payload})


async def db_write_prepare_create_table_handler(request):
    """把受控字段表单转为 CREATE TABLE 回滚预览，不直接落库。"""
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    table_name = str(data.get("table") or "").strip()
    columns = data.get("columns")
    if not db_id or not table_name:
        return json_ok({"ok": False, "error": "missing dbId or table"}, status=400)
    if not _db_ensure_local_entry_attached(db_id):
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    try:
        answer = _db_get_agent(db_id).prepare_structured_create_table(table_name, columns)
    except Exception as exc:
        _audit_append(
            db_id,
            category="nl_operation",
            action="create",
            outcome="failed",
            summary="自定义建表预览生成失败",
            risk="high",
            details={
                "target_refs": [_audit_ref(table_name)],
                "target_count": 1,
                "answer_kind": "error",
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": str(exc)}, status=400)
    payload = _db_answer_to_dict(answer)
    if answer.kind == "error":
        _audit_append(
            db_id,
            category="nl_operation",
            action="create",
            outcome="rejected",
            summary="自定义建表未通过安全校验",
            risk="high",
            details={
                "target_refs": [_audit_ref(table_name)],
                "target_count": 1,
                "answer_kind": "error",
                "error_sha256": _audit_sha256(answer.error or answer.narrative),
            },
        )
        return json_ok({
            "ok": False,
            "error": answer.error or answer.narrative,
            "answer": payload,
        }, status=400)
    confirm_id = str(answer.confirm_id or "")
    proposal = _db_agent_core().WRITE_REGISTRY.get(confirm_id)
    if proposal is not None:
        proposal.access_scope_ref = _current_access_scope_ref(
            _db_entry_unchecked(db_id),
        )
    confirm_ref = _audit_ref(confirm_id)
    _audit_append(
        db_id,
        category="nl_operation",
        action="create",
        outcome="pending",
        summary="自定义建表已生成回滚预览，等待确认",
        risk="high",
        correlation_id=confirm_ref,
        details={
            "target_refs": [_audit_ref(table_name)],
            "target_count": 1,
            "answer_kind": "write_pending",
            "confirm_ref": confirm_ref,
            "column_count": len(columns) if isinstance(columns, list) else 0,
        },
    )
    return json_ok({"ok": True, "answer": payload})


async def db_write_confirm_handler(request):
    """用户对写提案表态（Human-in-the-loop）：approved=True 执行落库；False 作废。"""
    data = await read_json(request)
    db_id = str(data.get("dbId") or "").strip()
    confirm_id = str(data.get("confirmId") or "").strip()
    approved = bool(data.get("approved"))
    if not db_id or not confirm_id:
        return json_ok({"ok": False, "error": "missing dbId or confirmId"}, status=400)
    if not _db_entry(db_id) or db_id not in _DB_AGENT_DBS:
        return json_ok({"ok": False, "error": f"database not attached: {db_id}"}, status=404)
    dc = _db_agent_core()
    proposal = dc.WRITE_REGISTRY.get(confirm_id)
    entry = _db_entry_unchecked(db_id)
    if not _write_proposal_scope_allowed(entry, proposal):
        return json_ok({
            "ok": False,
            "error": "write confirmation not found",
        }, status=404)
    confirm_ref = _audit_ref(confirm_id)
    approval_policy = "reject"
    if approved:
        required_role = "operator"
        approval_policy = "bounded_dml"
        if proposal is not None:
            kind = str(getattr(proposal, "kind", "") or "").upper()
            preview = getattr(proposal, "preview", None)
            affected = preview.get("affected") if isinstance(preview, dict) else None
            high_risk = (
                bool(getattr(proposal, "dangerous", False))
                or kind in {"DELETE", "CREATE", "ALTER", "DROP"}
                or not isinstance(affected, int)
                or isinstance(affected, bool)
                or affected < 0
                or affected > 100
            )
            if high_risk:
                required_role = "admin"
                approval_policy = "high_risk"
        role = _current_role()
        if not _access_control.role_allows(role, required_role):
            _audit_append(
                db_id,
                category="access_control", action="deny_write_approval",
                outcome="rejected", summary="本地角色无权批准该写操作",
                risk="high", actor=_current_audit_actor(),
                correlation_id=confirm_ref,
                details={
                    "confirm_ref": confirm_ref,
                    "required_role": required_role,
                    "request_role": role,
                    "route_ref": _audit_ref(request.path),
                    "http_method": request.method.upper(),
                    "approval_policy": approval_policy,
                },
            )
            return json_ok({
                "ok": False,
                "error": "当前角色无权批准该风险级别的写操作",
                "role": role,
                "requiredRole": required_role,
                "approvalPolicy": approval_policy,
            }, status=403)
    try:
        _audit_append(
            db_id,
            category="write_confirmation",
            action="approve" if approved else "reject",
            outcome="approved" if approved else "rejected",
            summary="用户批准写操作" if approved else "用户拒绝写操作",
            risk="high",
            correlation_id=confirm_ref,
            details={"confirm_ref": confirm_ref, "approval_policy": approval_policy},
            strict=True,
        )
    except AuditGateError as exc:
        return json_ok({"ok": False, "error": str(exc)}, status=503)
    try:
        agent = _db_get_agent(db_id)
    except Exception as exc:
        _audit_append(
            db_id,
            category="write_execution",
            action="execute" if approved else "cancel",
            outcome="failed",
            summary="写操作确认处理失败",
            risk="high",
            correlation_id=confirm_ref,
            details={
                "confirm_ref": confirm_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok({"ok": False, "error": f"agent init failed: {exc}"}, status=400)
    try:
        ans = agent.confirm_write(confirm_id, approve=approved)
    except Exception as exc:
        traceback.print_exc()
        _audit_append(
            db_id,
            category="write_execution",
            action="execute" if approved else "cancel",
            outcome="failed",
            summary="写操作确认处理失败",
            risk="high",
            correlation_id=confirm_ref,
            details={
                "confirm_ref": confirm_ref,
                "error_type": type(exc).__name__,
                "error_sha256": _audit_sha256(str(exc)),
            },
        )
        return json_ok(
            {"ok": False, "error": f"confirm failed: {type(exc).__name__}: {exc}"}, status=500
        )
    answer = _db_answer_to_dict(ans)
    operation = answer.get("operation") if isinstance(answer.get("operation"), dict) else {}
    operation_status = str(operation.get("status") or "")
    if operation_status == "cancelled":
        outcome = "cancelled"
    elif ans.kind == "write_result" and operation_status == "executed":
        outcome = "succeeded"
    else:
        outcome = "failed"
    details = _audit_answer_details("", answer)
    details.pop("question_sha256", None)
    details.pop("question_length", None)
    details["confirm_ref"] = confirm_ref
    result_event = _audit_append(
        db_id,
        category="write_execution",
        action="execute" if approved else "cancel",
        outcome=outcome,
        summary="写操作执行结果" if approved else "写操作已取消",
        risk=str(operation.get("risk") or "high"),
        correlation_id=confirm_ref,
        details=details,
    )
    if (
        outcome == "succeeded"
        and proposal is not None
        and str(getattr(proposal, "kind", "") or "").upper() in {"CREATE", "ALTER", "DROP"}
    ):
        # DDL changes the schema contract. Evict the old Agent snapshot and
        # refresh the public table list before the next form/chart request.
        _DB_AGENT_CACHE.pop(db_id, None)
        current_entry = _DB_AGENT_DBS.get(db_id)
        if current_entry is not None and not current_entry.get("conn"):
            try:
                current_entry["tables"] = _db_validate_sqlite(str(current_entry.get("path") or ""))
            except Exception:
                pass
    return json_ok({
        "ok": ans.kind == "write_result",
        "answer": answer,
        "audit": {"intent_recorded": True, "result_recorded": result_event is not None},
    })


def create_app():
    _db_session_restore()  # bridge 启动时从持久化 store 恢复 DB 会话热缓存
    if _db_sched is not None:
        try:
            _db_sched.start(_db_sched_resolver, _db_sched_audit_sink)
        except Exception:
            traceback.print_exc()
    app = web.Application(middlewares=[cors_middleware], client_max_size=500 * 1024 * 1024)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/status", status_handler)
    app.router.add_get("/model-profiles", model_profiles_handler)
    app.router.add_post("/model-profiles", model_profiles_handler)
    app.router.add_post("/model-profiles/test", model_profile_test_handler)
    app.router.add_get("/model-profiles/{id}", model_profiles_handler)
    app.router.add_put("/model-profiles/{id}", model_profiles_handler)
    app.router.add_delete("/model-profiles/{id}", model_profiles_handler)
    app.router.add_post("/upload", upload_handler)
    app.router.add_get("/db/auth/context", db_auth_context_handler)
    app.router.add_get("/db/auth/credentials", db_auth_credentials_handler)
    app.router.add_post("/db/auth/credentials", db_auth_credentials_handler)
    app.router.add_post(
        "/db/auth/credentials/{credential_id}/revoke",
        db_auth_credential_revoke_handler,
    )
    app.router.add_get("/db/databases", db_databases_handler)
    app.router.add_post("/db/attach", db_attach_handler)
    app.router.add_post("/db/connect", db_connect_handler)
    app.router.add_delete("/db/databases/{db_id}", db_detach_handler)
    app.router.add_post("/db/ask", db_ask_handler)
    app.router.add_get("/db/write/form", db_write_form_handler)
    app.router.add_post("/db/write/prepare-insert", db_write_prepare_insert_handler)
    app.router.add_post(
        "/db/write/prepare-create-table", db_write_prepare_create_table_handler,
    )
    app.router.add_post("/db/write/confirm", db_write_confirm_handler)
    app.router.add_get("/db/ask/{run_id}/progress", db_progress_handler)
    app.router.add_post("/db/ask/{run_id}/cancel", db_cancel_handler)
    app.router.add_get("/db/sessions", db_sessions_handler)
    app.router.add_get("/db/session/{sid}", db_session_detail_handler)
    app.router.add_patch("/db/session/{sid}", db_session_rename_handler)
    app.router.add_delete("/db/session/{sid}", db_session_delete_handler)
    app.router.add_post("/db/chart-data", db_chart_data_handler)
    app.router.add_post("/db/charts-auto", db_charts_auto_handler)
    app.router.add_post("/db/charts-cache-status", db_charts_cache_status_handler)
    app.router.add_get("/db/tables", db_tables_handler)
    app.router.add_get("/db/semantics", db_semantics_handler)
    app.router.add_post("/db/semantics", db_semantics_handler)
    app.router.add_delete("/db/semantics/{semantic_id}", db_semantics_delete_handler)
    app.router.add_get("/db/semantics/export", db_semantics_export_handler)
    app.router.add_post("/db/semantics/import/preflight", db_semantics_import_preflight_handler)
    app.router.add_post("/db/semantics/import/apply", db_semantics_import_apply_handler)
    app.router.add_get("/db/audit", db_audit_handler)
    app.router.add_get("/db/audit/verify", db_audit_verify_handler)
    app.router.add_get("/db/audit/export", db_audit_export_handler)
    app.router.add_post(
        "/db/audit/reconciliation/resolve",
        db_audit_reconciliation_resolve_handler,
    )
    app.router.add_get("/db/audit/backups", db_audit_backups_handler)
    app.router.add_post("/db/audit/backups", db_audit_backup_create_handler)
    app.router.add_get("/db/schedules", db_schedules_handler)
    app.router.add_post("/db/schedules", db_schedules_create_handler)
    app.router.add_patch("/db/schedules/{id}", db_schedules_update_handler)
    app.router.add_delete("/db/schedules/{id}", db_schedules_delete_handler)
    app.router.add_post("/db/schedules/{id}/run", db_schedules_run_handler)
    app.router.add_get("/db/schedules/logs", db_schedules_logs_handler)
    # Serve static frontend (desktop/static/)
    static_dir = APP_DIR / "desktop" / "static"

    async def db_handler(request):
        return web.FileResponse(
            static_dir / "db.html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    app.router.add_get("/", db_handler)
    app.router.add_get("/db", db_handler)
    app.router.add_static("/", static_dir, show_index=False)

    async def on_shutdown(app):
        if _db_sched is not None:
            try:
                _db_sched.stop()
            except Exception:
                pass
    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    host = os.environ.get("BRIDGE_HOST", "127.0.0.1")
    port = int(os.environ.get("BRIDGE_PORT", "14169"))
    print(f"DBQuill bridge: http://{host}:{port}  ws://{host}:{port}/ws", file=sys.stderr)
    web.run_app(create_app(), host=host, port=port, print=None)
