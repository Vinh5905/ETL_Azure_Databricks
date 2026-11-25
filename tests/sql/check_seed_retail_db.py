"""Regression seed trên hai database Docker trắng; thông tin kết nối chỉ nhận qua stdin."""

from __future__ import annotations

import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pymssql

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ingest.seed_retail_db import Product, apply_wave, generate_wave  # noqa: E402

PRODUCTS = tuple(Product(i, (i - 1) % 21 + 1) for i in range(1, 49689))


def check_database(credentials, mode):
    database = "retail_db_test_seed_cdc" if mode == "cdc" else "retail_db_test_seed_fallback"

    def connect(*, autocommit=False):
        return pymssql.connect(
            **credentials,
            database=database,
            autocommit=autocommit,
            encryption="require",
            tds_version="7.4",
            use_datetime2=True,
            timeout=120,
        )

    observer = connect(autocommit=True)
    writer = connect()
    cur = observer.cursor()
    try:
        cur.execute("SELECT DB_NAME()")
        assert cur.fetchone()[0].startswith("retail_db_test_"), "Chỉ dùng database test"
        cur.execute("SELECT COUNT(*) FROM dbo.product_prices")
        assert cur.fetchone()[0] == 0, "Cần database nguồn trắng"

        def snapshot():
            cur.execute("SELECT * FROM dbo.product_prices ORDER BY product_id")
            prices = hashlib.sha256(repr(cur.fetchall()).encode()).hexdigest()
            cur.execute("SELECT * FROM dbo.promotions ORDER BY promo_id")
            promos = hashlib.sha256(repr(cur.fetchall()).encode()).hexdigest()
            return prices, promos

        try:
            apply_wave(writer, generate_wave(PRODUCTS, 1), mode)
            raise AssertionError("Không được bỏ qua wave 0")
        except ValueError:
            pass
        first = generate_wave(PRODUCTS, 0)
        assert apply_wave(writer, first, mode)
        cur.execute("SELECT COUNT(*) FROM dbo.product_prices")
        assert cur.fetchone()[0] == 49688
        cur.execute("SELECT COUNT(*) FROM dbo.promotions")
        assert cur.fetchone()[0] == 3000
        original = snapshot()
        assert not apply_wave(writer, first, mode)
        assert snapshot() == original
        print(f"PASS {mode}: 49.688 giá, 3.000 khuyến mãi, retry wave 0 không đổi dữ liệu", flush=True)

        second = generate_wave(PRODUCTS, 1)
        try:
            apply_wave(writer, second, mode)
            raise AssertionError("Không được đổi giá trước khi snapshot wave 0 đã công bố")
        except ValueError as error:
            assert "Phải công bố snapshot giá đầy đủ của wave 0" in str(error)
        assert snapshot() == original
        cur.execute("SELECT COUNT(*) FROM etl.seed_wave")
        assert cur.fetchone()[0] == 1

        # Mô phỏng ADF xác nhận Copy/publish sau wave 0, qua đúng setter của mode đang chạy.
        if mode == "cdc":
            cur.execute("EXEC sys.sp_cdc_scan @continuous = 0")
            cur.execute("""
                DECLARE @lsn varchar(22) = CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1);
                EXEC etl.usp_set_cdc_watermark @capture_instance = N'dbo_product_prices', @to_lsn = @lsn,
                    @adf_run_id = 'test-wave-0', @mode = 'snapshot', @rows = 49688;
            """)
        else:
            cur.execute("""
                EXEC etl.usp_set_ts_watermark @capture_instance = N'dbo_product_prices', @to_ts = '2015-01-01',
                    @adf_run_id = 'test-wave-0', @mode = 'snapshot', @rows = 49688;
            """)
        print(f"PASS {mode}: wave 1 phải chờ snapshot giá wave 0 đã công bố đủ", flush=True)

        # Gây lỗi sau khi giá đã UPDATE để kiểm rollback dữ liệu và marker của cả đợt.
        cur.execute("""CREATE TRIGGER dbo.test_seed_reject ON dbo.promotions AFTER INSERT AS
                       THROW 51998, N'Lỗi tự dựng để kiểm rollback seed', 1;""")
        try:
            apply_wave(writer, second, mode)
            raise AssertionError("Phải nhận lỗi trigger")
        except Exception as error:
            # executemany có thể trả exception từ lớp DB-Lib thay vì wrapper DB-API.
            assert getattr(error, "number", None) == 51998, "Phải đúng lỗi trigger tự dựng"
        assert snapshot() == original
        cur.execute("SELECT COUNT(*) FROM etl.seed_wave")
        assert cur.fetchone()[0] == 1
        cur.execute("DROP TRIGGER dbo.test_seed_reject")
        print(f"PASS {mode}: lỗi giữa đợt rollback giá/khuyến mãi/marker", flush=True)

        if mode == "cdc":
            cur.execute("EXEC sys.sp_cdc_scan @continuous = 0")
            cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
            last = cur.fetchone()[0]
        assert apply_wave(writer, second, mode)
        if mode == "cdc":
            cur.execute("EXEC sys.sp_cdc_scan @continuous = 0")
            cur.execute(
                """DECLARE @from binary(10) = sys.fn_cdc_increment_lsn(%s),
                                  @to binary(10) = sys.fn_cdc_get_max_lsn();
                SELECT __$operation, COUNT(*) FROM cdc.fn_cdc_get_all_changes_dbo_product_prices(@from, @to, N'all')
                GROUP BY __$operation;""",
                (last,),
            )
            assert dict(cur.fetchall()) == {4: 2484}
            cur.execute(
                """DECLARE @from binary(10) = sys.fn_cdc_increment_lsn(%s),
                                  @to binary(10) = sys.fn_cdc_get_max_lsn();
                SELECT __$operation, COUNT(*) FROM cdc.fn_cdc_get_all_changes_dbo_promotions(@from, @to, N'all')
                GROUP BY __$operation;""",
                (last,),
            )
            assert dict(cur.fetchall()) == {1: 30, 2: 300, 4: 50}
        else:
            cur.execute("SELECT COUNT(*) FROM dbo.promotions WHERE is_deleted = 1 AND updated_at = '2026-03-01'")
            assert cur.fetchone()[0] == 30
        after_second = snapshot()
        assert not apply_wave(writer, second, mode)
        assert snapshot() == after_second
        for bad in (generate_wave(PRODUCTS, 2, seed=43), replace(second, price_updates=())):
            try:
                apply_wave(writer, bad, mode)
                raise AssertionError("Phải từ chối seed/kế hoạch khác")
            except ValueError:
                pass
        print(f"PASS {mode}: wave 1 đúng 2.484 giá/300 thêm/50 gia hạn/30 hủy, retry và kiểm fingerprint", flush=True)

        # Hai connection cùng áp dụng một đợt: chỉ một connection thực sự ghi.
        third = generate_wave(PRODUCTS, 2)
        # Mô phỏng marker từ thuật toán wave trước khác phiên bản hiện tại.
        for previous in (first, second):
            cur.execute("UPDATE etl.seed_wave SET plan_hash = %s WHERE wave = %s", ("0" * 64, previous.wave))
            try:
                apply_wave(writer, third, mode)
                raise AssertionError("Phải kiểm fingerprint từng wave trước")
            except ValueError as error:
                assert "Fingerprint wave trước" in str(error)
            assert snapshot() == after_second
            cur.execute("SELECT COUNT(*) FROM etl.seed_wave")
            assert cur.fetchone()[0] == 2
            cur.execute(
                "UPDATE etl.seed_wave SET plan_hash = %s WHERE wave = %s", (previous.fingerprint, previous.wave)
            )
        print(f"PASS {mode}: sai fingerprint wave 0 hoặc 1 chặn wave 2 trước khi ghi nguồn", flush=True)

        def parallel_seed(_):
            connection = connect()
            try:
                return apply_wave(connection, third, mode)
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(parallel_seed, range(2))) == [False, True]
        for wave in (3, 4):
            assert apply_wave(writer, generate_wave(PRODUCTS, wave), mode)
        assert not apply_wave(writer, first, mode)
        cur.execute("SELECT wave FROM etl.seed_wave ORDER BY wave")
        assert [r[0] for r in cur.fetchall()] == list(range(5))
        cur.execute("SELECT COUNT(*) FROM dbo.promotions" + (" WHERE is_deleted = 0" if mode == "soft-delete" else ""))
        assert cur.fetchone()[0] == 4080
        print(f"PASS {mode}: đồng thời chỉ áp dụng một lần; đủ 5 đợt, 4.080 khuyến mãi chưa hủy", flush=True)
    finally:
        cur.close()
        writer.close()
        observer.close()


if __name__ == "__main__":
    credentials = json.load(sys.stdin)
    check_database(credentials, "cdc")
    check_database(credentials, "soft-delete")
