"""Sinh và áp dụng 5 đợt giá/khuyến mãi; không có kết nối hay Spark khi import."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from contextlib import suppress
from dataclasses import asdict, astuple, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

DEPT_CENTS = {4: (20, 400), 5: (800, 4000), 7: (100, 600)}


@dataclass(frozen=True, order=True)
class Product:
    product_id: int
    department_id: int


@dataclass(frozen=True)
class Promotion:
    promo_id: str
    product_id: int
    promo_type: str
    discount_pct: int
    start_date: date
    end_date: date
    updated_at: datetime


@dataclass(frozen=True)
class Wave:
    wave: int
    seed: int
    product_count: int
    products_hash: str
    price_inserts: tuple
    price_updates: tuple
    promo_inserts: tuple[Promotion, ...]
    promo_extensions: tuple
    promo_deletes: tuple[str, ...]
    previous_plan_hashes: tuple[str, ...] = ()

    @property
    def fingerprint(self) -> str:
        # Metadata lịch sử không làm đổi fingerprint đã ghi của chính wave này.
        payload = asdict(self)
        payload.pop("previous_plan_hashes")
        return _digest(payload)


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def load_products(path: str | Path) -> tuple[Product, ...]:
    """Chỉ đọc hai cột cần thiết; CSV chứa dấu phẩy trong tên sản phẩm vẫn đọc đúng."""
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not {"product_id", "department_id"}.issubset(reader.fieldnames or []):
            raise ValueError("CSV thiếu product_id hoặc department_id")
        products = tuple(Product(int(row["product_id"]), int(row["department_id"])) for row in reader)
    return _canonical_products(products)


def _canonical_products(products) -> tuple[Product, ...]:
    result = tuple(sorted(products))
    if not result or any(
        type(p.product_id) is not int or type(p.department_id) is not int or p.product_id < 1 or p.department_id < 1
        for p in result
    ):
        raise ValueError("Danh mục phải có mã sản phẩm/ngành hàng là số nguyên dương")
    if len({p.product_id for p in result}) != len(result):
        raise ValueError("Mã sản phẩm bị trùng")
    return result


def generate_wave(products, wave: int, seed: int = 42) -> Wave:
    """Hàm thuần: cùng danh mục/seed/wave cho cùng kế hoạch, không phụ thuộc thứ tự CSV."""
    if type(wave) is not int or wave not in range(5) or type(seed) is not int:
        raise ValueError("wave phải từ 0 đến 4, seed phải là số nguyên")
    products = _canonical_products(products)
    products_hash = _digest([astuple(p) for p in products])
    active: dict[str, Promotion] = {}
    previous_hashes = []
    for current in range(wave + 1):
        rng = random.Random(seed + current)
        ts = datetime(2015, 1, 1) if current == 0 else datetime(2026, 2, 22) + timedelta(days=7 * current)
        prices, updates, extensions, deletes = (), (), (), ()
        if current == 0:
            prices = tuple(
                (p.product_id, Decimal(rng.randint(*DEPT_CENTS.get(p.department_id, (100, 1000)))) / 100, "USD", ts)
                for p in products
            )
            selected = rng.sample(products, min(3000, len(products)))
            offset = 0
        else:
            selected_prices = rng.sample(products, len(products) // 20)
            updates = tuple(
                (rng.choice((Decimal("0.85"), Decimal("0.90"), Decimal("1.05"), Decimal("1.10"))), ts, p.product_id)
                for p in selected_prices
            )
            deletes = tuple(rng.sample(sorted(active), min(30, len(active))))
            for key in deletes:
                del active[key]
            extended = rng.sample(sorted(active), min(50, len(active)))
            extensions = tuple((active[key].end_date + timedelta(days=7), ts, key) for key in extended)
            for end, _, key in extensions:
                old = active[key]
                active[key] = Promotion(
                    old.promo_id, old.product_id, old.promo_type, old.discount_pct, old.start_date, end, ts
                )
            selected = [rng.choice(products) for _ in range(300)]
            offset = min(3000, len(products)) + (current - 1) * 300
        promos = []
        for index, p in enumerate(selected, 1):
            start = date(2026, 3, 1) + timedelta(days=rng.randrange(21)) if current == 0 else ts.date()
            kind = rng.choice(("percent_off", "percent_off", "percent_off", "bogo"))
            discount = rng.randrange(5, 55, 5) if kind == "percent_off" else 0
            promo = Promotion(
                f"P{offset + index:05d}",
                p.product_id,
                kind,
                discount,
                start,
                start + timedelta(days=rng.randint(3, 14)),
                ts,
            )
            promos.append(promo)
            active[promo.promo_id] = promo
        plan = Wave(
            current,
            seed,
            len(products),
            products_hash,
            prices,
            updates,
            tuple(promos),
            extensions,
            deletes,
            tuple(previous_hashes),
        )
        previous_hashes.append(plan.fingerprint)
    return plan


def apply_wave(connection, plan: Wave, source_mode: str = "cdc") -> bool:
    """Dùng connection riêng có autocommit=False; commit/rollback cả đợt, trả False khi đã áp dụng."""
    if source_mode not in ("cdc", "soft-delete"):
        raise ValueError("source_mode phải là cdc hoặc soft-delete")
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SET XACT_ABORT ON;
            DECLARE @result int;
            EXEC @result = sys.sp_getapplock @Resource = N'etl.seed_retail_db',
                @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
            IF @result < 0 THROW 51100, N'Không lấy được khóa seed; thử lại sau.', 1;
            IF SCHEMA_ID(N'etl') IS NULL THROW 51100, N'Chạy script AD-DE-04 trước.', 1;
            IF OBJECT_ID(N'etl.seed_wave', N'U') IS NULL
                CREATE TABLE etl.seed_wave (
                    wave int NOT NULL PRIMARY KEY,
                    seed int NOT NULL,
                    products_hash char(64) NOT NULL,
                    plan_hash char(64) NOT NULL,
                    source_mode varchar(12) NOT NULL,
                    applied_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME()
                );
        """)
        cursor.execute("SELECT wave, seed, products_hash, plan_hash, source_mode FROM etl.seed_wave ORDER BY wave")
        history = cursor.fetchall()
        if [row[0] for row in history] != list(range(len(history))):
            raise ValueError("Lịch sử wave không liên tiếp; cần đối soát")
        already_applied = False
        for previous in history:
            if previous[1] != plan.seed or previous[2] != plan.products_hash or previous[4] != source_mode:
                raise ValueError("Seed, danh mục hoặc chế độ nguồn khác các đợt đã áp dụng")
            if previous[0] < plan.wave:
                if (
                    previous[0] >= len(plan.previous_plan_hashes)
                    or previous[3] != plan.previous_plan_hashes[previous[0]]
                ):
                    raise ValueError("Fingerprint wave trước khác kế hoạch tái lập; không áp dụng đợt mới")
            elif previous[0] == plan.wave:
                if previous[3] != plan.fingerprint:
                    raise ValueError("Kế hoạch khác đợt đã áp dụng; không chạy lại bằng phiên bản khác")
                already_applied = True
        if already_applied:
            connection.rollback()
            return False
        if len(history) != plan.wave:
            raise ValueError("Phải áp dụng tuần tự wave 0, 1, 2, 3, 4")
        cursor.execute("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()")
        cdc = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COL_LENGTH('dbo.product_prices', 'is_deleted'), COL_LENGTH('dbo.promotions', 'is_deleted')"
        )
        soft_columns = cursor.fetchone()
        if source_mode == "cdc":
            if not cdc or any(soft_columns):
                raise ValueError("Nguồn chưa sẵn sàng cho CDC hoặc đang dùng soft delete")
            cursor.execute(
                "SELECT COUNT(*) FROM cdc.change_tables "
                "WHERE capture_instance IN ('dbo_product_prices', 'dbo_promotions')"
            )
            if cursor.fetchone()[0] != 2:
                raise ValueError("Thiếu capture instance; chạy script AD-DE-04")
        elif cdc or not all(soft_columns):
            raise ValueError("Nguồn chưa sẵn sàng cho soft delete hoặc đang bật CDC")
        if plan.wave == 0:
            cursor.execute("SELECT (SELECT COUNT(*) FROM dbo.product_prices), (SELECT COUNT(*) FROM dbo.promotions)")
            if cursor.fetchone() != (0, 0):
                raise ValueError("Wave 0 cần bảng nguồn trắng; không tự xóa hoặc ghi đè dữ liệu cũ")
        else:
            cursor.execute("SELECT COUNT(*) FROM dbo.product_prices")
            if cursor.fetchone()[0] != plan.product_count:
                raise ValueError("Số sản phẩm nguồn không khớp kế hoạch seed")
        if plan.wave == 1:
            # Snapshot đọc bảng hiện hành. Sau UPDATE đầu tiên, giá wave 0 không còn ở bảng nguồn.
            run_log = "etl.run_log" if source_mode == "cdc" else "etl.ts_run_log"
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {run_log} AS log
                CROSS JOIN etl.seed_wave AS seed
                WHERE seed.wave = 0 AND log.capture_instance = N'dbo_product_prices'
                  AND log.mode = 'snapshot' AND log.rows_copied = %s
                  AND log.finished_at >= seed.applied_at;
                """,
                (plan.product_count,),
            )
            if cursor.fetchone()[0] == 0:
                raise ValueError("Phải công bố snapshot giá đầy đủ của wave 0 trước khi chạy wave 1")
        _execute_batches(
            cursor,
            "INSERT INTO dbo.product_prices(product_id, unit_price, currency, updated_at) VALUES (%s, %s, %s, %s)",
            plan.price_inserts,
        )
        _execute_batches(
            cursor,
            """
            UPDATE dbo.product_prices SET unit_price = ROUND(unit_price * %s, 2), updated_at = %s WHERE product_id = %s;
            IF @@ROWCOUNT <> 1 THROW 51101, N'Thiếu sản phẩm cần đổi giá.', 1;
        """,
            plan.price_updates,
        )
        _execute_batches(
            cursor,
            "INSERT INTO dbo.promotions(promo_id, product_id, promo_type, discount_pct, "
            "start_date, end_date, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [astuple(p) for p in plan.promo_inserts],
        )
        _execute_batches(
            cursor,
            """
            UPDATE dbo.promotions SET end_date = %s, updated_at = %s WHERE promo_id = %s;
            IF @@ROWCOUNT <> 1 THROW 51101, N'Thiếu khuyến mãi cần gia hạn.', 1;
        """,
            plan.promo_extensions,
        )
        if source_mode == "cdc":
            _execute_batches(
                cursor,
                """
                DELETE FROM dbo.promotions WHERE promo_id = %s;
                IF @@ROWCOUNT <> 1 THROW 51101, N'Thiếu khuyến mãi cần hủy.', 1;
            """,
                [(key,) for key in plan.promo_deletes],
            )
        else:
            ts = datetime(2026, 2, 22) + timedelta(days=7 * plan.wave)
            _execute_batches(
                cursor,
                """
                UPDATE dbo.promotions SET is_deleted = 1, updated_at = %s WHERE promo_id = %s AND is_deleted = 0;
                IF @@ROWCOUNT <> 1 THROW 51101, N'Thiếu khuyến mãi cần hủy.', 1;
            """,
                [(ts, key) for key in plan.promo_deletes],
            )
        cursor.execute(
            "INSERT INTO etl.seed_wave(wave, seed, products_hash, plan_hash, source_mode) VALUES (%s, %s, %s, %s, %s)",
            (plan.wave, plan.seed, plan.products_hash, plan.fingerprint, source_mode),
        )
        connection.commit()
        return True
    except BaseException:
        # Kết nối đứt có thể làm rollback/close lỗi; giữ nguyên exception gốc để debug.
        with suppress(Exception):
            connection.rollback()
        raise
    finally:
        with suppress(Exception):
            cursor.close()


def _execute_batches(cursor, statement, rows, batch_size=500):
    for start in range(0, len(rows), batch_size):
        cursor.executemany(statement, rows[start : start + batch_size], batch_size=batch_size)


def connect_from_secret(server, database, user, dbutils):
    """Đường chạy thật chỉ đọc mật khẩu từ Key Vault-backed scope, không đọc env/file."""
    import pymssql

    password = dbutils.secrets.get("kv", "sql-admin-password")
    return pymssql.connect(
        server=server,
        database=database,
        user=user,
        password=password,
        autocommit=False,
        encryption="require",
        tds_version="7.4",
        use_datetime2=True,
        login_timeout=30,
        timeout=120,
    )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wave", type=int, choices=range(5), required=True)
    parser.add_argument("--products-csv", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--expected-products", type=int, default=49688)
    parser.add_argument("--source-mode", choices=("cdc", "soft-delete"), default="cdc")
    parser.add_argument("--server", default="")
    parser.add_argument("--database", default="retail_db")
    parser.add_argument("--user", default="sqladmin")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    products = load_products(args.products_csv)
    if len(products) != args.expected_products:
        parser.error(f"Danh mục có {len(products)} sản phẩm, cần {args.expected_products}")
    plan = generate_wave(products, args.wave, args.seed)
    if args.dry_run:
        print(
            json.dumps(
                {
                    "wave": plan.wave,
                    "products": len(products),
                    "price_inserts": len(plan.price_inserts),
                    "price_updates": len(plan.price_updates),
                    "promo_inserts": len(plan.promo_inserts),
                    "promo_extensions": len(plan.promo_extensions),
                    "promo_deletes": len(plan.promo_deletes),
                    "plan_hash": plan.fingerprint,
                },
                ensure_ascii=False,
            )
        )
        return
    if not args.server.strip() or not args.database.strip() or not args.user.strip():
        parser.error("Phải cung cấp server, database và user trước khi đọc secret")
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    connection = connect_from_secret(args.server, args.database, args.user, DBUtils(SparkSession.builder.getOrCreate()))
    try:
        applied = apply_wave(connection, plan, args.source_mode)
    finally:
        with suppress(Exception):
            connection.close()
    print(f"wave {args.wave}: áp dụng thành công" if applied else f"wave {args.wave} đã áp dụng, bỏ qua")


if __name__ == "__main__":
    main()
