"""Kiểm corpus nhỏ qua cả bốn định dạng và khóa MERGE của bảng kết quả."""

from types import SimpleNamespace
from uuid import uuid4

from pyspark.sql import functions as F

from tasks.benchmark_formats import FORMATS, fingerprint, measure_format, save_results


def test_measures_same_rows_across_formats_and_updates_on_repair(spark, tmp_path):
    data = spark.createDataFrame(
        [(1, 24852, 1, 0), (2, 24852, 3, 1), (3, 2, 2, 0)],
        "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
    ).repartition(2)

    def ls(path):
        return [SimpleNamespace(size=file.stat().st_size, name=file.name) for file in tmp_path.joinpath(path).iterdir()]

    fs = SimpleNamespace(ls=ls)
    expected = fingerprint(data)
    matching = fingerprint(data.where(F.col("product_id") == 24852))
    records = []
    for fmt in FORMATS:
        path = str(tmp_path / fmt)
        result = measure_format(spark, fs, data, path, fmt, 24852, expected, matching)
        assert result[:3] == (3, 2, 24852)
        assert result[3] > 0 and result[-1] > 0
        assert (result[4] > 0) == (fmt == "delta_optimized")
        records.append(("run-a", 1, fmt, *result, "silver.order_items_prior", "local", spark.version, 2))

    table = f"`benchmark_{uuid4().hex}`"
    try:
        save_results(spark, table, records)
        save_results(spark, table, records)
        assert {row.format for row in spark.table(table).collect()} == set(FORMATS)
        assert spark.table(table).count() == 4
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
