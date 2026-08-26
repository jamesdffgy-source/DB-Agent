#!/usr/bin/env python3
"""Run the same read-only DBQuill contract against MySQL and PostgreSQL.

The database servers and fixture schema are intentionally external to the
repository. Credentials are read from ``DBQUILL_REMOTE_TEST_PASSWORD`` so a
test password never becomes project configuration or source history.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FRONTENDS = ROOT / "runtime" / "app" / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

import dbquill_core as core  # noqa: E402


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _scalar(connector: core.RemoteDBConnector, sql: str) -> Any:
    connection = connector.connect()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()
    finally:
        connector.close(connection)


def _assert_read_only(connector: core.RemoteDBConnector) -> str:
    connection = connector.connect()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("DELETE FROM orders WHERE id = -1")
        except Exception as exc:  # vendor-specific exception classes
            return type(exc).__name__
        finally:
            cursor.close()
    finally:
        connector.close(connection)
    raise AssertionError("read-only connection unexpectedly accepted DELETE")


def _assert_bad_connection(cfg: dict) -> str:
    connector = core.RemoteDBConnector(cfg)
    try:
        connection = connector.connect()
    except Exception as exc:  # vendor-specific exception classes
        return type(exc).__name__
    connector.close(connection)
    raise AssertionError("invalid remote connection unexpectedly succeeded")


def run_one(name: str, cfg: dict, closed_port: int) -> dict:
    started = time.monotonic()
    connector = core.RemoteDBConnector(cfg)
    schema = core.SchemaDiscovery(connector, sample_rows=3).discover()
    _expect(
        {"customers", "orders", "sequence_rows"}.issubset(schema.tables),
        f"{name}: fixture tables were not discovered",
    )
    customer_id = next(
        column for column in schema.tables["customers"].columns
        if column.name == "id"
    )
    order_customer_id = next(
        column for column in schema.tables["orders"].columns
        if column.name == "customer_id"
    )
    _expect(customer_id.pk, f"{name}: primary key metadata missing")
    _expect(
        (order_customer_id.fk_table, order_customer_id.fk_column)
        == ("customers", "id"),
        f"{name}: foreign key metadata missing",
    )
    _expect(schema.tables["orders"].row_count == 5, f"{name}: row count is wrong")
    _expect(
        schema.tables["sequence_rows"].row_count == 650,
        f"{name}: large fixture row count is wrong",
    )

    security = core.SQLSecurity(connector, max_rows=500, timeout_s=1.0)
    bounded = security.execute("SELECT id, label FROM sequence_rows ORDER BY id")
    _expect(not bounded.error, f"{name}: bounded query failed: {bounded.error}")
    _expect(len(bounded.rows) == 500, f"{name}: result cap was not enforced")
    _expect("LIMIT 500" in bounded.sql.upper(), f"{name}: LIMIT was not injected")

    executor = core.NL2SQLExecutor(security, schema)
    same_table = executor.answer(
        "按 status 统计 orders 的订单数和 total_amount 合计，按 status 排序。"
    )
    _expect(same_table.kind == "query", f"{name}: grouped metrics did not execute")
    _expect(
        same_table.relational_plan
        and same_table.relational_plan.get("kind") == "grouped_metrics",
        f"{name}: grouped metrics bypassed typed local planning",
    )
    _expect(
        _plain(same_table.rows)
        == [["cancelled", 1, 25.0], ["paid", 3, 225.0], ["pending", 1, 20.0]],
        f"{name}: grouped metrics returned wrong values: {same_table.rows}",
    )

    joined = executor.answer(
        "通过 customers.id = orders.customer_id，按客户 region 统计 "
        "status='paid' 的订单数和 total_amount，按金额降序。"
    )
    _expect(joined.kind == "query", f"{name}: joined metrics did not execute")
    _expect(
        _plain(joined.rows) == [["east", 2, 175.0], ["west", 1, 50.0]],
        f"{name}: joined metrics returned wrong values: {joined.rows}",
    )
    _expect(
        joined.relational_plan["joins"][0]["source"] == "foreign_key",
        f"{name}: explicit equality was not normalized to the declared FK",
    )

    dangerous = security.execute("SELECT COUNT(*) FROM orders; DROP TABLE orders")
    _expect(bool(dangerous.error), f"{name}: multi-statement SQL was not rejected")
    _expect(_scalar(connector, "SELECT COUNT(*) FROM orders") == 5, f"{name}: orders changed")
    read_only_error = _assert_read_only(connector)

    sleep_sql = "SELECT SLEEP(3)" if name == "mysql" else "SELECT pg_sleep(3)"
    timeout_security = core.SQLSecurity(connector, timeout_s=0.25)
    timeout_started = time.monotonic()
    timed = timeout_security.execute(sleep_sql)
    timeout_elapsed = time.monotonic() - timeout_started
    _expect(bool(timed.error), f"{name}: long query was not interrupted")
    _expect(timeout_elapsed < 2.5, f"{name}: timeout took too long: {timeout_elapsed:.2f}s")

    bad_password = dict(cfg)
    bad_password["password"] = "definitely-wrong"
    bad_password_error = _assert_bad_connection(bad_password)
    closed = dict(cfg)
    closed["port"] = closed_port
    closed_port_error = _assert_bad_connection(closed)

    return {
        "dialect": name,
        "server_version": str(_scalar(connector, "SELECT VERSION()")),
        "driver": (
            __import__("pymysql").VERSION_STRING
            if name == "mysql"
            else __import__("psycopg2").__version__
        ),
        "tables": sorted(schema.tables),
        "orders_rows": schema.tables["orders"].row_count,
        "large_fixture_rows": schema.tables["sequence_rows"].row_count,
        "bounded_result_rows": len(bounded.rows),
        "grouped_metrics_rows": _plain(same_table.rows),
        "joined_metrics_rows": _plain(joined.rows),
        "join_evidence": joined.relational_plan["joins"][0]["source"],
        "read_only_error": read_only_error,
        "timeout_error": timed.error,
        "timeout_seconds": round(timeout_elapsed, 3),
        "bad_password_error": bad_password_error,
        "closed_port_error": closed_port_error,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=13306)
    parser.add_argument("--postgresql-port", type=int, default=15432)
    parser.add_argument("--database", default="dbquill_bench")
    parser.add_argument("--user", default="dbquill_ro")
    args = parser.parse_args()
    password = (
        os.environ.get("DBQUILL_REMOTE_TEST_PASSWORD")
        or os.environ.get("DBAGENT_REMOTE_TEST_PASSWORD")
        or ""
    )
    if not password:
        raise SystemExit("DBQUILL_REMOTE_TEST_PASSWORD is required")
    base = {
        "host": args.host,
        "database": args.database,
        "user": args.user,
        "password": password,
    }
    report = {
        "contract": "remote-read-only-e2e-v1",
        "results": [
            run_one(
                "mysql",
                {**base, "dialect": "mysql", "port": args.mysql_port},
                args.mysql_port + 1,
            ),
            run_one(
                "postgresql",
                {**base, "dialect": "postgresql", "port": args.postgresql_port},
                args.postgresql_port + 1,
            ),
        ],
    }
    # Keep the report portable across Windows console code pages and CI log collectors.
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
