#!/usr/bin/env python3
"""Inspect or issue deterministic local DBQuill role tokens."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime/app/frontends"
TOKEN_FILE = ROOT / "runtime/app/temp/bridge.token"
sys.path.insert(0, str(FRONTENDS))

import db_access_control as access  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def _tokens() -> dict[str, str]:
    if not TOKEN_FILE.is_file():
        raise FileNotFoundError("bridge.token 不存在；请先启动一次 DBQuill")
    return access.derive_role_tokens(TOKEN_FILE.read_text(encoding="ascii").strip())


def status(_args: argparse.Namespace) -> dict:
    tokens = _tokens()
    return {
        "policy_version": access.POLICY_VERSION,
        "roles": [{
            "role": role,
            "label": access.ROLE_LABELS[role],
            "token_fingerprint": access.token_fingerprint(tokens[role]),
            "capabilities": access.capabilities(role),
        } for role in ("viewer", "operator", "admin")],
        "note": "派生令牌不会写入额外文件；轮换管理员 token 会同时撤销全部派生令牌。",
    }


def issue(args: argparse.Namespace) -> dict:
    tokens = _tokens()
    role = args.role
    return {
        "policy_version": access.POLICY_VERSION,
        "role": role,
        "label": access.ROLE_LABELS[role],
        "token": tokens[role],
        "token_fingerprint": access.token_fingerprint(tokens[role]),
        "warning": "此输出是本地 API 凭据；只交给对应角色并通过安全渠道传递。",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DBQuill 本地角色令牌管理")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="只显示角色、能力和令牌指纹").set_defaults(func=status)
    issue_cmd = commands.add_parser("issue", help="显式输出指定角色令牌")
    issue_cmd.add_argument("role", choices=("viewer", "operator"))
    issue_cmd.set_defaults(func=issue)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        payload = {"ok": True, "action": args.command, **args.func(args)}
    except Exception as exc:
        payload = {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
        print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
