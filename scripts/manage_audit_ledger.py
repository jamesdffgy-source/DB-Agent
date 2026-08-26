#!/usr/bin/env python3
"""Inspect, back up, verify, or restore the local DBQuill audit ledger."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime/app/frontends"
sys.path.insert(0, str(FRONTENDS))

import db_audit_store as ledger  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def status(_args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {
        "action": "status",
        "integrity": ledger.verify_chain(),
        "reconciliation": ledger.reconciliation_status(),
        "backups": ledger.backup_status(),
    }


def backup(_args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {"action": "backup", "backup": ledger.create_backup(reason="manual")}


def verify_backup(args: argparse.Namespace) -> dict:
    return {
        "action": "verify-backup",
        "backup": ledger.verify_backup(args.backup_id),
    }


def export_backup(args: argparse.Namespace) -> dict:
    if args.confirm != "EXPORT_AUDIT_BACKUP":
        raise ValueError("外部备份必须提供 --confirm EXPORT_AUDIT_BACKUP")
    ledger.init_db()
    return {
        "action": "export-backup",
        "backup": ledger.create_external_backup(args.backup_id, args.output_dir),
        "warning": "备份包没有数字签名；必须存入独立受保护位置才具备异地恢复价值。",
    }


def verify_external_backup(args: argparse.Namespace) -> dict:
    return {
        "action": "verify-external-backup",
        "backup": ledger.verify_external_backup(args.bundle_file),
    }


def configure_target(args: argparse.Namespace) -> dict:
    return {
        "action": "configure-target",
        "target": ledger.configure_external_backup_target(
            args.directory,
            expected_current_target_id=args.expect_current_target_id,
            confirmation=args.confirm,
        ),
    }


def target_status(_args: argparse.Namespace) -> dict:
    return {
        "action": "target-status",
        "target": ledger.external_backup_target_status(),
    }


def probe_target(args: argparse.Namespace) -> dict:
    return {
        "action": "probe-target",
        "probe": ledger.probe_external_backup_target(confirmation=args.confirm),
        "warning": "探测会创建、原子改名、读回并清理一个随机临时文件。",
    }


def sync_target(args: argparse.Namespace) -> dict:
    return {
        "action": "sync-target",
        "sync": ledger.synchronize_external_backup_target(
            confirmation=args.confirm,
        ),
        "warning": "同步会新增本地备份和外部包，不会删除目标中的既有文件。",
    }


def verify_target_latest(_args: argparse.Namespace) -> dict:
    return {
        "action": "verify-target-latest",
        "verification": ledger.verify_latest_external_target_backup(),
    }


def target_history(args: argparse.Namespace) -> dict:
    return {
        "action": "target-history",
        "history": ledger.external_backup_target_history(
            limit=args.limit,
            current_target_only=not args.all_targets,
        ),
    }


def check_target_health(args: argparse.Namespace) -> dict:
    return {
        "action": "check-target-health",
        "health": ledger.check_external_backup_target_health(
            max_age_hours=args.max_age_hours,
        ),
    }


def assess_current(args: argparse.Namespace) -> dict:
    return {
        "action": "assess-current",
        "assessment": ledger.assess_current_ledger(),
        "warning": "这是只读现场指纹；灾备恢复前必须完全退出桌面应用。",
    }


def preserve_corrupt(args: argparse.Namespace) -> dict:
    result = ledger.create_corrupt_ledger_evidence(
        args.output_dir,
        expected_assessment_token=args.expect_assessment_token,
        confirmation=args.confirm,
    )
    return {
        "action": "preserve-corrupt",
        "evidence": result,
        "warning": "证据包没有数字签名；请复制到独立、只读或受保护的位置。",
    }


def verify_corrupt_evidence(args: argparse.Namespace) -> dict:
    return {
        "action": "verify-corrupt-evidence",
        "evidence": ledger.verify_corrupt_ledger_evidence(args.evidence_file),
    }


def drill_restore(args: argparse.Namespace) -> dict:
    if args.confirm != "DRILL_AUDIT_RESTORE":
        raise ValueError("恢复演练必须提供 --confirm DRILL_AUDIT_RESTORE")
    ledger.init_db()
    return {
        "action": "drill-restore",
        "drill": ledger.run_restore_drill(args.backup_id, args.output_dir),
        "warning": "演练不替换当前账本，临时恢复库校验后立即清除；报告不是数字签名。",
    }


def drill_external_restore(args: argparse.Namespace) -> dict:
    if args.confirm != "DRILL_EXTERNAL_AUDIT_BACKUP":
        raise ValueError(
            "外部备份恢复演练必须提供 --confirm DRILL_EXTERNAL_AUDIT_BACKUP"
        )
    ledger.init_db()
    return {
        "action": "drill-external-restore",
        "drill": ledger.run_external_restore_drill(
            args.bundle_file, args.output_dir,
        ),
        "warning": "演练不替换当前账本；临时恢复库校验后立即清除。",
    }


def verify_drill(args: argparse.Namespace) -> dict:
    return {
        "action": "verify-drill",
        "drill": ledger.verify_restore_drill(
            args.report_file,
            external_backup_file=args.external_backup,
        ),
    }


def anchor(args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {
        "action": "anchor",
        "anchor": ledger.create_external_anchor(args.output_dir),
        "warning": "锚点只有保存在独立受保护位置时才增加证据价值；它不是数字签名或 WORM。",
    }


def verify_anchor(args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {
        "action": "verify-anchor",
        "anchor": ledger.verify_external_anchor(args.anchor_file),
    }


def retention_status(args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {
        "action": "retention-status",
        "retention": ledger.retention_status(args.days),
    }


def archive(args: argparse.Namespace) -> dict:
    if args.confirm != "ARCHIVE_AUDIT_PREFIX":
        raise ValueError("归档必须提供 --confirm ARCHIVE_AUDIT_PREFIX")
    ledger.init_db()
    result = ledger.create_external_archive(
        args.output_dir,
        through_sequence=args.through_sequence,
    )
    return {
        "action": "archive",
        "archive": result,
        "warning": "归档不删除当前账本；必须复制到独立受保护位置才增加外部证据价值。",
    }


def verify_archive(args: argparse.Namespace) -> dict:
    ledger.init_db()
    return {
        "action": "verify-archive",
        "archive": ledger.verify_external_archive(args.archive_file),
    }


def resolve_pending(args: argparse.Namespace) -> dict:
    if args.confirm != "RESOLVE_AUDIT_PENDING":
        raise ValueError("人工处置必须提供 --confirm RESOLVE_AUDIT_PENDING")
    evidence_ref = str(args.evidence_ref or "").strip()
    if not evidence_ref or len(evidence_ref) > 240 \
            or any(ord(char) < 32 for char in evidence_ref):
        raise ValueError("--evidence-ref 必须是 1–240 个可打印字符")
    ledger.init_db()
    resolution = ledger.resolve_pending_event(
        args.sequence,
        disposition=args.disposition,
        evidence_sha256=ledger.sha256_text(evidence_ref),
        actor="local_admin",
    )
    return {
        "action": "resolve-pending",
        "resolution": resolution,
        "reconciliation": ledger.reconciliation_status(),
    }


def restore(args: argparse.Namespace) -> dict:
    if not args.confirm_restore:
        raise ValueError("恢复必须显式提供 --confirm-restore，并确保桌面应用已完全退出")
    result = ledger.restore_backup(
        args.backup_id,
        expected_current_head=args.expect_current_head,
        confirmation="RESTORE_AUDIT_LEDGER",
    )
    return {"action": "restore", **result}


def restore_external(args: argparse.Namespace) -> dict:
    if args.confirm != "RESTORE_EXTERNAL_AUDIT_BACKUP":
        raise ValueError(
            "外部恢复必须提供 --confirm RESTORE_EXTERNAL_AUDIT_BACKUP，"
            "并确保桌面应用已完全退出"
        )
    result = ledger.restore_external_backup(
        args.bundle_file,
        expected_current_head=args.expect_current_head,
        confirmation=args.confirm,
    )
    return {"action": "restore-external", **result}


def restore_corrupt_external(args: argparse.Namespace) -> dict:
    if args.confirm != "RESTORE_CORRUPT_AUDIT_LEDGER":
        raise ValueError(
            "损坏账本灾备恢复必须提供 --confirm RESTORE_CORRUPT_AUDIT_LEDGER，"
            "并确保桌面应用已完全退出"
        )
    result = ledger.restore_external_backup_over_corrupt_ledger(
        args.bundle_file,
        expected_assessment_token=args.expect_assessment_token,
        evidence_output_dir=args.evidence_output_dir,
        confirmation=args.confirm,
    )
    return {
        "action": "restore-corrupt-external",
        **result,
        "warning": "损坏现场已先写入并复验外部证据包；请立即转存到受保护位置。",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DBQuill 审计账本离线管理；restore 前必须完全退出桌面应用。",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="校验当前事件链、未决对账和备份状态").set_defaults(func=status)
    commands.add_parser("backup", help="创建并校验本地审计备份").set_defaults(func=backup)
    verify = commands.add_parser("verify-backup", help="验证指定备份的文件哈希和事件链")
    verify.add_argument("backup_id")
    verify.set_defaults(func=verify_backup)
    export_backup_cmd = commands.add_parser(
        "export-backup", help="把本地备份原子封装到用户指定的非受管目录",
    )
    export_backup_cmd.add_argument("backup_id")
    export_backup_cmd.add_argument("--output-dir", required=True)
    export_backup_cmd.add_argument("--confirm", required=True)
    export_backup_cmd.set_defaults(func=export_backup)
    verify_external_backup_cmd = commands.add_parser(
        "verify-external-backup", help="独立验证外部审计备份包",
    )
    verify_external_backup_cmd.add_argument("bundle_file")
    verify_external_backup_cmd.set_defaults(func=verify_external_backup)
    configure_target_cmd = commands.add_parser(
        "configure-target",
        help="配置一个显式存在的外部文件系统备份目标",
    )
    configure_target_cmd.add_argument("directory")
    configure_target_cmd.add_argument("--expect-current-target-id")
    configure_target_cmd.add_argument("--confirm", required=True)
    configure_target_cmd.set_defaults(func=configure_target)
    commands.add_parser(
        "target-status",
        help="只读查看外部目标可用性和最近成功同步",
    ).set_defaults(func=target_status)
    probe_target_cmd = commands.add_parser(
        "probe-target",
        help="显式探测目标写入、原子替换、读回和临时清理能力",
    )
    probe_target_cmd.add_argument("--confirm", required=True)
    probe_target_cmd.set_defaults(func=probe_target)
    sync_target_cmd = commands.add_parser(
        "sync-target",
        help="创建本地备份、复制到已配置目标、复验并登记最近成功状态",
    )
    sync_target_cmd.add_argument("--confirm", required=True)
    sync_target_cmd.set_defaults(func=sync_target)
    commands.add_parser(
        "verify-target-latest",
        help="独立复验当前目标最近一次成功同步的外部包",
    ).set_defaults(func=verify_target_latest)
    target_history_cmd = commands.add_parser(
        "target-history",
        help="验证并查看脱敏同步尝试历史，默认仅当前 target ID",
    )
    target_history_cmd.add_argument("--limit", type=int, default=50)
    target_history_cmd.add_argument("--all-targets", action="store_true")
    target_history_cmd.set_defaults(func=target_history)
    target_health_cmd = commands.add_parser(
        "check-target-health",
        help="以退出码检查最近尝试、成功时效和最新外部包完整性",
    )
    target_health_cmd.add_argument("--max-age-hours", type=int, default=25)
    target_health_cmd.set_defaults(func=check_target_health)
    commands.add_parser(
        "assess-current",
        help="只读评估当前账本文件现场并生成短期绑定令牌",
    ).set_defaults(func=assess_current)
    preserve_corrupt_cmd = commands.add_parser(
        "preserve-corrupt",
        help="把已确认损坏且未变化的账本现场封装为外部证据包",
    )
    preserve_corrupt_cmd.add_argument("--output-dir", required=True)
    preserve_corrupt_cmd.add_argument("--expect-assessment-token", required=True)
    preserve_corrupt_cmd.add_argument("--confirm", required=True)
    preserve_corrupt_cmd.set_defaults(func=preserve_corrupt)
    verify_corrupt_evidence_cmd = commands.add_parser(
        "verify-corrupt-evidence",
        help="独立复验损坏账本证据包及每个现场文件哈希",
    )
    verify_corrupt_evidence_cmd.add_argument("evidence_file")
    verify_corrupt_evidence_cmd.set_defaults(func=verify_corrupt_evidence)
    drill_cmd = commands.add_parser(
        "drill-restore", help="在外部隔离目录演练恢复；不替换当前账本",
    )
    drill_cmd.add_argument("backup_id")
    drill_cmd.add_argument("--output-dir", required=True)
    drill_cmd.add_argument("--confirm", required=True)
    drill_cmd.set_defaults(func=drill_restore)
    drill_external_cmd = commands.add_parser(
        "drill-external-restore",
        help="从外部备份包执行隔离恢复演练；不替换当前账本",
    )
    drill_external_cmd.add_argument("bundle_file")
    drill_external_cmd.add_argument("--output-dir", required=True)
    drill_external_cmd.add_argument("--confirm", required=True)
    drill_external_cmd.set_defaults(func=drill_external_restore)
    verify_drill_cmd = commands.add_parser(
        "verify-drill", help="复验恢复演练报告及其源备份",
    )
    verify_drill_cmd.add_argument("report_file")
    verify_drill_cmd.add_argument("--external-backup")
    verify_drill_cmd.set_defaults(func=verify_drill)
    anchor_cmd = commands.add_parser("anchor", help="把当前账本 head 写入用户指定目录")
    anchor_cmd.add_argument("--output-dir", required=True)
    anchor_cmd.set_defaults(func=anchor)
    verify_anchor_cmd = commands.add_parser(
        "verify-anchor", help="验证外部锚点及当前账本对应历史前缀",
    )
    verify_anchor_cmd.add_argument("anchor_file")
    verify_anchor_cmd.set_defaults(func=verify_anchor)
    retention_cmd = commands.add_parser(
        "retention-status", help="评估超过保留期的连续历史前缀；不删除事件",
    )
    retention_cmd.add_argument("--days", type=int, default=365)
    retention_cmd.set_defaults(func=retention_status)
    archive_cmd = commands.add_parser(
        "archive", help="把历史前缀原子写到用户指定的非受管目录",
    )
    archive_cmd.add_argument("--output-dir", required=True)
    archive_cmd.add_argument("--through-sequence", type=int)
    archive_cmd.add_argument("--confirm", required=True)
    archive_cmd.set_defaults(func=archive)
    verify_archive_cmd = commands.add_parser(
        "verify-archive", help="验证外部归档内部链及当前账本对应前缀",
    )
    verify_archive_cmd.add_argument("archive_file")
    verify_archive_cmd.set_defaults(func=verify_archive)
    resolve_cmd = commands.add_parser(
        "resolve-pending",
        help="追加管理员未决处置；不修改原事件，也不证明数据库原子终态",
    )
    resolve_cmd.add_argument("sequence", type=int)
    resolve_cmd.add_argument(
        "--disposition", required=True,
        choices=("verified_no_change", "verified_completed", "superseded"),
    )
    resolve_cmd.add_argument(
        "--evidence-ref", required=True,
        help="外部工单或核验引用；账本只保存其 SHA-256",
    )
    resolve_cmd.add_argument("--confirm", required=True)
    resolve_cmd.set_defaults(func=resolve_pending)
    restore_cmd = commands.add_parser("restore", help="离线恢复已验证备份")
    restore_cmd.add_argument("backup_id")
    restore_cmd.add_argument("--expect-current-head", required=True)
    restore_cmd.add_argument("--confirm-restore", action="store_true")
    restore_cmd.set_defaults(func=restore)
    restore_external_cmd = commands.add_parser(
        "restore-external", help="离线恢复已验证的外部审计备份包",
    )
    restore_external_cmd.add_argument("bundle_file")
    restore_external_cmd.add_argument("--expect-current-head", required=True)
    restore_external_cmd.add_argument("--confirm", required=True)
    restore_external_cmd.set_defaults(func=restore_external)
    restore_corrupt_external_cmd = commands.add_parser(
        "restore-corrupt-external",
        help="证据保全后，从外部备份离线恢复已损坏的当前账本",
    )
    restore_corrupt_external_cmd.add_argument("bundle_file")
    restore_corrupt_external_cmd.add_argument(
        "--expect-assessment-token", required=True,
    )
    restore_corrupt_external_cmd.add_argument(
        "--evidence-output-dir", required=True,
    )
    restore_corrupt_external_cmd.add_argument("--confirm", required=True)
    restore_corrupt_external_cmd.set_defaults(func=restore_corrupt_external)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = args.func(args)
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
