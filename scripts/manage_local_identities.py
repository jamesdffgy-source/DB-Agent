#!/usr/bin/env python3
"""Offline administration for expiring DBQuill local credentials."""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
sys.path.insert(0, str(FRONTENDS))

import db_identity_store as store  # noqa: E402
import db_audit_store as audit  # noqa: E402


for stream in (sys.stdout, sys.stderr):
    try:
        stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _audit_gate(*, action: str, correlation_id: str, details: dict) -> None:
    integrity = audit.verify_chain()
    if not integrity.get("ok"):
        raise RuntimeError("审计账本完整性异常，凭据变更已阻断")
    audit.append_event(
        category="access_control", action=action, outcome="approved",
        summary="离线管理员批准本地凭据变更", risk="high",
        actor="local_admin", database_key="local_access_control",
        correlation_id=correlation_id, details=details,
    )


def _audit_result(
    *, action: str, correlation_id: str, outcome: str, details: dict,
) -> None:
    audit.append_event(
        category="access_control", action=action, outcome=outcome,
        summary="离线本地凭据变更完成" if outcome == "succeeded" else "离线本地凭据变更失败",
        risk="high", actor="local_admin", database_key="local_access_control",
        correlation_id=correlation_id, details=details,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="管理 DBQuill 本地个人凭据")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="列出脱敏凭据状态，不输出 token 或 token 哈希")

    issue = sub.add_parser("issue", help="发行一次性显示的本地凭据")
    issue.add_argument("--label", required=True, help="凭据显示名称（1–64 字符）")
    issue.add_argument("--role", required=True, choices=("viewer", "operator", "admin"))
    issue.add_argument("--ttl-hours", type=int, default=store.DEFAULT_TTL_HOURS)
    scope = issue.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--all-databases", action="store_true",
        help="显式授予当前及后续接入的全部数据库",
    )
    scope.add_argument(
        "--database-ref", action="append", dest="database_refs",
        help="授予一个数据库 SHA-256 引用；可重复，最多 64 项",
    )
    issue.add_argument(
        "--table", action="append", dest="table_scopes", metavar="DATABASE_REF:TABLE",
        help="限制某个已授权数据库到指定表；可重复，未指定的授权库仍为全表",
    )
    issue.add_argument(
        "--column", action="append", dest="column_scopes",
        metavar="DATABASE_REF:TABLE:COLUMN",
        help="把已限定表进一步限制到指定字段；可重复，未指定字段的限定表仍为全字段",
    )

    revoke = sub.add_parser("revoke", help="不可逆吊销一条凭据")
    issue.add_argument(
        "--row-filter", action="append", dest="row_filters", metavar="JSON",
        help=(
            "Repeatable structured row filter JSON with databaseRef, table, "
            "column, operator and value. Row-scoped credentials are read-only."
        ),
    )

    revoke.add_argument("credential_id")
    revoke.add_argument("--confirm", required=True, help="必须为 REVOKE_LOCAL_CREDENTIAL")

    args = parser.parse_args()
    store.init_db()
    audit.init_db()
    try:
        if args.command == "list":
            _print({"ok": True, "credentials": store.list_credentials()})
            return 0
        if args.command == "issue":
            scope_mode = "all" if args.all_databases else "restricted"
            table_scopes = {}
            for raw in args.table_scopes or []:
                database_ref, separator, table = str(raw).partition(":")
                if not separator or not database_ref or not table:
                    raise ValueError("--table 必须使用 DATABASE_REF:TABLE 格式")
                table_scopes.setdefault(database_ref, []).append(table)
            column_scopes = {}
            for raw in args.column_scopes or []:
                parts = str(raw).split(":", 2)
                if len(parts) != 3 or not all(parts):
                    raise ValueError("--column 必须使用 DATABASE_REF:TABLE:COLUMN 格式")
                database_ref, table, column = parts
                column_scopes.setdefault(database_ref, {}).setdefault(table, []).append(column)
            row_scopes = {}
            for raw in args.row_filters or []:
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise ValueError("--row-filter must be valid JSON") from exc
                if not isinstance(item, dict):
                    raise ValueError("--row-filter JSON must be an object")
                database_ref = str(item.pop("databaseRef", "") or "").strip()
                table = str(item.pop("table", "") or "").strip()
                if not database_ref or not table:
                    raise ValueError("--row-filter requires databaseRef and table")
                row_scopes.setdefault(database_ref, {}).setdefault(table, []).append(item)
            normalized_scope = store.validate_database_scope(
                scope_mode, args.database_refs or [],
                table_scopes, column_scopes, row_scopes,
            )
            correlation_id = f"credential-issue-{uuid.uuid4().hex[:16]}"
            details = {
                "request_role": "admin", "credential_role": args.role,
                "expires_in_hours": args.ttl_hours,
                "database_scope_mode": normalized_scope["mode"],
                "database_scope_count": len(normalized_scope["databaseRefs"]),
                "table_scope_database_count": len(normalized_scope["tableScopes"]),
                "table_scope_table_count": sum(
                    len(tables) for tables in normalized_scope["tableScopes"].values()
                ),
                "column_scope_table_count": sum(
                    len(tables) for tables in normalized_scope["columnScopes"].values()
                ),
                "column_scope_column_count": sum(
                    len(columns)
                    for tables in normalized_scope["columnScopes"].values()
                    for columns in tables.values()
                ),
            }
            details.update({
                "row_scope_table_count": sum(
                    len(tables) for tables in normalized_scope["rowScopes"].values()
                ),
                "row_scope_filter_count": sum(
                    len(filters)
                    for tables in normalized_scope["rowScopes"].values()
                    for filters in tables.values()
                ),
            })
            _audit_gate(
                action="credential_issue", correlation_id=correlation_id,
                details=details,
            )
            try:
                result = store.issue_credential(
                    label=args.label,
                    role=args.role,
                    ttl_hours=args.ttl_hours,
                    scope_mode=normalized_scope["mode"],
                    database_refs=normalized_scope["databaseRefs"],
                    table_scopes=normalized_scope["tableScopes"],
                    column_scopes=normalized_scope["columnScopes"],
                    row_scopes=normalized_scope["rowScopes"],
                )
            except Exception as exc:
                _audit_result(
                    action="credential_issue", correlation_id=correlation_id,
                    outcome="failed", details={**details, "error_type": type(exc).__name__},
                )
                raise
            _audit_result(
                action="credential_issue", correlation_id=correlation_id,
                outcome="succeeded",
                details={**details, "credential_ref": result["credentialRef"]},
            )
            _print({
                "ok": True,
                "warning": "token 只显示本次；请安全保存，日志和审计不会保存原文。",
                "credential": result,
            })
            return 0
        if args.confirm != "REVOKE_LOCAL_CREDENTIAL":
            raise ValueError("确认短语必须是 REVOKE_LOCAL_CREDENTIAL")
        current = store.get_credential(args.credential_id)
        if current is None:
            raise KeyError("凭据不存在")
        correlation_id = f"credential-revoke-{uuid.uuid4().hex[:16]}"
        details = {
            "request_role": "admin", "credential_role": current["role"],
            "credential_ref": current["credentialRef"],
        }
        _audit_gate(
            action="credential_revoke", correlation_id=correlation_id,
            details=details,
        )
        try:
            result = store.revoke_credential(args.credential_id)
        except Exception as exc:
            _audit_result(
                action="credential_revoke", correlation_id=correlation_id,
                outcome="failed", details={**details, "error_type": type(exc).__name__},
            )
            raise
        _audit_result(
            action="credential_revoke", correlation_id=correlation_id,
            outcome="succeeded", details=details,
        )
        _print({"ok": True, "credential": result})
        return 0
    except (ValueError, KeyError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
