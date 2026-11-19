"""Kiểm chọn lô bằng Spark thật, bootstrap CSV và cú pháp/lifecycle của adapter trạng thái."""

import datetime as dt
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingest.off_state import STATE_SCHEMA, SparkState, landing_frames, pick_batch


def test_pick_batch_ranks_prior_with_stable_ties_and_skips_terminal_or_exhausted(spark):
    products = spark.createDataFrame(
        [(pid, f"Tea {pid}") for pid in range(1, 7)] + [(3, "Tea 3")], "product_id INT, product_name STRING"
    )
    items = spark.createDataFrame(
        [(pid,) for pid, count in [(1, 6), (2, 5), (3, 4), (4, 3), (5, 2), (6, 4)] for _ in range(count)],
        "product_id INT",
    )
    state = spark.createDataFrame(
        [(1, "done", 1), (2, "no_result", 1), (3, "retry", 2), (4, "retry", 5), (5, "failed", 1)],
        "product_id INT, status STRING, attempts INT",
    )
    rows = pick_batch(products, items, state, n=2, top=6).collect()
    assert [(row.product_id, row.attempts) for row in rows] == [(3, 2), (6, 0)]
    # Top là tập sản phẩm cố định theo phổ biến, không mở rộng để bù sản phẩm đã hoàn thành.
    assert not pick_batch(products, items, state, n=1, top=2).collect()


def test_bootstrap_csv_reads_published_prior_only_and_handles_escaped_product_names(spark, tmp_path):
    fixtures = Path(__file__).resolve().parent / "fixtures/instacart"
    for name in ("products", "order_products__prior"):
        target = tmp_path / "instacart" / name / "release=2017" / f"{name}.csv"
        target.parent.mkdir(parents=True)
        shutil.copyfile(fixtures / f"{name}.csv", target)
    products, items = landing_frames(spark, str(tmp_path))
    names = {row.product_id: row.product_name for row in products.collect()}
    assert names[2] == "Tea, Green" and names[4] == 'Cookies "Classic"'
    assert items.count() == 24
    assert "release" not in products.columns


def test_merge_sql_parses_and_failed_merge_cleans_temp_view_without_changing_attempts(spark):
    class GrammarSpark:
        def __init__(self):
            self.catalog = spark.catalog
            self.merges = []

        def createDataFrame(self, rows, schema):
            assert schema == STATE_SCHEMA
            assert rows[0]["attempts"] == 3
            # Giữ múi giờ để encoder Spark không diễn giải UTC thành giờ local của Python host.
            assert rows[0]["updated_at"].utcoffset() == dt.timedelta(0)
            return spark.createDataFrame(rows, schema)

        def sql(self, text):
            # Parse SQL bằng Spark thật; không mô phỏng thực thi Delta MERGE.
            spark._jsparkSession.sessionState().sqlParser().parsePlan(text)
            if text.lstrip().startswith("MERGE"):
                self.merges.append(text)
                raise RuntimeError("lỗi Delta tự dựng")
            return SimpleNamespace(collect=lambda: [])

    adapter = GrammarSpark()
    store = SparkState(adapter, "test_catalog", "qa_bronze", "qa_ops")
    before = {table.name for table in spark.catalog.listTables() if table.isTemporary}
    row = dict(
        product_id=1,
        status="done",
        attempts=3,
        off_code="00123",
        batch_file="/Volumes/x/products/x.json",
        last_error=None,
        updated_at="2026-11-17T00:00:00+00:00",
        batch_id="abc",
    )
    with pytest.raises(RuntimeError, match="Delta"):
        store.merge([row])
    assert adapter.merges
    assert before == {table.name for table in spark.catalog.listTables() if table.isTemporary}
