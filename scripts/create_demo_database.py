"""Create the deterministic SQLite database used for manual DB-Agent testing."""

from __future__ import annotations

import argparse
import os
import sqlite3
from contextlib import closing
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "demo_data" / "dbagent_demo.sqlite"
DATASET_VERSION = "2026.08.18-v1"


CUSTOMERS = [
    (1, "陈晨", "上海", "华东", "gold", "2025-01-10", 1),
    (2, "李明", "北京", "华北", "silver", "2025-02-15", 1),
    (3, "王芳", "广州", "华南", "gold", "2025-03-20", 1),
    (4, "赵强", "成都", "西南", "bronze", "2025-04-05", 1),
    (5, "周敏", "杭州", "华东", "silver", "2025-05-12", 1),
    (6, "吴涛", "深圳", "华南", "gold", "2025-06-01", 1),
    (7, "郑洁", "武汉", "华中", "silver", "2025-07-09", 1),
    (8, "孙浩", "西安", "西北", "bronze", "2025-08-18", 1),
    (9, "刘洋", "南京", "华东", "gold", "2025-09-03", 1),
    (10, "何静", "天津", "华北", "silver", "2026-01-11", 1),
    (11, "高远", "苏州", "华东", "gold", "2026-04-22", 1),
    (12, "林雪", "青岛", "华北", "bronze", "2026-07-01", 1),
]


PRODUCTS = [
    (1, "专业笔记本电脑", "电子产品", 6999.00, 5200.00, 1),
    (2, "机械键盘", "电子产品", 499.00, 280.00, 1),
    (3, "27英寸显示器", "电子产品", 1599.00, 1100.00, 1),
    (4, "人体工学椅", "办公家具", 1299.00, 800.00, 1),
    (5, "升降办公桌", "办公家具", 2399.00, 1500.00, 1),
    (6, "精品咖啡豆", "食品饮料", 99.00, 45.00, 1),
    (7, "茶叶礼盒", "食品饮料", 199.00, 80.00, 1),
    (8, "缓震跑鞋", "运动户外", 699.00, 350.00, 1),
    (9, "瑜伽垫", "运动户外", 199.00, 80.00, 1),
    (10, "通勤双肩包", "箱包服饰", 399.00, 160.00, 1),
]


# id, customer_id, order_date, status, channel, line items(product_id, quantity, discount), note
ORDER_SPECS = [
    (1001, 1, "2026-05-03", "paid", "web", [(1, 1, 0.05), (2, 1, 0.00)], "企业采购，加急配送"),
    (1002, 2, "2026-05-07", "paid", "store", [(4, 1, 0.00), (5, 1, 0.00)], "新办公室布置"),
    (1003, 3, "2026-05-12", "cancelled", "web", [(8, 1, 0.00), (9, 1, 0.00)], "客户主动取消"),
    (1004, 4, "2026-05-19", "paid", "partner", [(3, 1, 0.00), (2, 1, 0.00)], "渠道合作订单"),
    (1005, 5, "2026-05-27", "refunded", "web", [(6, 5, 0.00), (7, 2, 0.00)], "包装破损后退款"),
    (1006, 6, "2026-06-02", "paid", "web", [(5, 1, 0.05), (4, 1, 0.05)], "深圳办公室升级"),
    (1007, 7, "2026-06-05", "paid", "store", [(7, 3, 0.00), (6, 4, 0.00)], "端午员工礼品"),
    (1008, 8, "2026-06-11", "pending", "web", [(1, 1, 0.00)], "等待企业转账"),
    (1009, 9, "2026-06-18", "paid", "partner", [(3, 2, 0.10), (2, 1, 0.00)], "设计团队批量采购"),
    (1010, 10, "2026-06-25", "paid", "web", [(8, 2, 0.00), (10, 1, 0.00)], "客户要求周末送达"),
    (1011, 1, "2026-07-01", "paid", "web", [(5, 1, 0.10), (9, 1, 0.00)], "家庭办公空间改造"),
    (1012, 2, "2026-07-04", "cancelled", "store", [(4, 1, 0.00)], "门店缺货取消"),
    (1013, 3, "2026-07-08", "paid", "web", [(1, 1, 0.00), (3, 1, 0.05)], "直播团队设备，加急配送"),
    (1014, 4, "2026-07-12", "paid", "partner", [(6, 10, 0.00), (7, 4, 0.00)], "客户答谢礼品"),
    (1015, 5, "2026-07-16", "pending", "web", [(5, 1, 0.00)], "等待付款"),
    (1016, 6, "2026-07-20", "paid", "store", [(8, 1, 0.00), (9, 1, 0.00), (10, 1, 0.00)], "健身新人套装"),
    (1017, 7, "2026-07-24", "refunded", "web", [(3, 1, 0.00)], "显示器色差退款"),
    (1018, 8, "2026-07-28", "paid", "partner", [(4, 2, 0.05)], "共享办公室采购"),
    (1019, 9, "2026-08-02", "paid", "web", [(1, 1, 0.10), (2, 1, 0.00)], "老客户升级设备"),
    (1020, 10, "2026-08-05", "paid", "store", [(6, 6, 0.00), (7, 2, 0.00)], "门店自提"),
    (1021, 1, "2026-08-08", "pending", "web", [(3, 1, 0.00), (2, 1, 0.00)], "等待信用审核"),
    (1022, 3, "2026-08-11", "paid", "partner", [(5, 1, 0.00), (4, 1, 0.10)], "新分公司办公采购"),
    (1023, 6, "2026-08-14", "cancelled", "web", [(8, 2, 0.00)], "尺码选择错误"),
    (1024, 8, "2026-08-17", "paid", "store", [(2, 2, 0.00), (3, 1, 0.05)], "开学季设备采购，加急配送"),
]


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE demo_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    tier TEXT NOT NULL CHECK (tier IN ('bronze', 'silver', 'gold')),
    signup_date DATE NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    list_price REAL NOT NULL CHECK (list_price >= 0),
    unit_cost REAL NOT NULL CHECK (unit_cost >= 0),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1))
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    paid_at DATETIME,
    status TEXT NOT NULL CHECK (status IN ('paid', 'pending', 'cancelled', 'refunded')),
    channel TEXT NOT NULL CHECK (channel IN ('web', 'store', 'partner')),
    merchandise_amount REAL NOT NULL CHECK (merchandise_amount >= 0),
    shipping_fee REAL NOT NULL CHECK (shipping_fee >= 0),
    total_amount REAL NOT NULL CHECK (total_amount >= 0),
    note TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price >= 0),
    discount_rate REAL NOT NULL DEFAULT 0 CHECK (discount_rate >= 0 AND discount_rate < 1),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE calendar_exceptions (
    event_date DATE PRIMARY KEY,
    event_name TEXT NOT NULL,
    is_workday INTEGER NOT NULL CHECK (is_workday IN (0, 1))
);

CREATE TABLE regional_targets (
    month_start DATE NOT NULL,
    region TEXT NOT NULL,
    target_amount REAL NOT NULL CHECK (target_amount >= 0),
    PRIMARY KEY (month_start, region)
);

CREATE TABLE inventory_snapshots (
    product_id INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    PRIMARY KEY (product_id, snapshot_date),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date_status ON orders(order_date, status);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
"""


def create_database(output: Path, *, force: bool = False) -> dict[str, int | str]:
    output = output.resolve()
    if output.exists() and not force:
        raise FileExistsError(f"output already exists: {output}; use --force to reset it")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    if temporary.exists():
        temporary.unlink()

    prices = {product[0]: float(product[3]) for product in PRODUCTS}
    order_rows = []
    item_rows = []
    item_id = 1
    for order_id, customer_id, order_date, status, channel, lines, note in ORDER_SPECS:
        merchandise_amount = 0.0
        for product_id, quantity, discount_rate in lines:
            unit_price = prices[product_id]
            merchandise_amount += unit_price * quantity * (1.0 - discount_rate)
            item_rows.append(
                (item_id, order_id, product_id, quantity, unit_price, discount_rate)
            )
            item_id += 1
        merchandise_amount = round(merchandise_amount, 2)
        shipping_fee = 0.0 if merchandise_amount >= 500 else 20.0
        total_amount = round(merchandise_amount + shipping_fee, 2)
        paid_at = f"{order_date} 10:00:00" if status in {"paid", "refunded"} else None
        order_rows.append(
            (
                order_id,
                customer_id,
                order_date,
                paid_at,
                status,
                channel,
                merchandise_amount,
                shipping_fee,
                total_amount,
                note,
            )
        )

    target_rows = []
    target_base = {
        "华东": 16000,
        "华北": 9000,
        "华南": 13000,
        "华中": 4000,
        "西南": 5000,
        "西北": 3500,
    }
    for month_index, month_start in enumerate(
        ("2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01")
    ):
        for region, base in target_base.items():
            target_rows.append((month_start, region, float(base + month_index * 500)))

    inventory_rows = []
    for product_id in range(1, 11):
        inventory_rows.append((product_id, "2026-08-01", 120 - product_id * 5))
        inventory_rows.append((product_id, "2026-08-17", 96 - product_id * 4))

    try:
        with closing(sqlite3.connect(temporary)) as conn:
            conn.executescript(SCHEMA_SQL)
            conn.executemany(
                "INSERT INTO demo_metadata(key, value) VALUES (?, ?)",
                [
                    ("dataset_version", DATASET_VERSION),
                    ("reference_date", "2026-08-18"),
                    ("currency", "CNY"),
                    ("purpose", "DB-Agent manual NL-to-Database validation"),
                ],
            )
            conn.executemany(
                "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?)", CUSTOMERS
            )
            conn.executemany(
                "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)", PRODUCTS
            )
            conn.executemany(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                order_rows,
            )
            conn.executemany(
                "INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)", item_rows
            )
            conn.executemany(
                "INSERT INTO calendar_exceptions VALUES (?, ?, ?)",
                [
                    ("2026-05-01", "劳动节", 0),
                    ("2026-06-19", "公司培训日", 0),
                    ("2026-08-15", "周六调休上班", 1),
                    ("2026-08-17", "公司周年活动", 0),
                ],
            )
            conn.executemany(
                "INSERT INTO regional_targets VALUES (?, ?, ?)", target_rows
            )
            conn.executemany(
                "INSERT INTO inventory_snapshots VALUES (?, ?, ?)", inventory_rows
            )
            foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
            if foreign_key_errors:
                raise RuntimeError(f"foreign key validation failed: {foreign_key_errors}")
            quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
            if quick_check != "ok":
                raise RuntimeError(f"SQLite quick_check failed: {quick_check}")
            conn.commit()
        os.replace(temporary, output)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise

    return {
        "path": str(output),
        "dataset_version": DATASET_VERSION,
        "customers": len(CUSTOMERS),
        "products": len(PRODUCTS),
        "orders": len(order_rows),
        "order_items": len(item_rows),
        "tables": 8,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"output SQLite path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace the exact output file so a modified demo can be reset",
    )
    args = parser.parse_args()
    result = create_database(args.output, force=args.force)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
