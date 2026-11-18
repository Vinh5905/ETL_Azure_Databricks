"""Đọc và chuẩn bị giao dịch cho các thuật toán DS."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TX = "instacart.ds_data.transactions"


def load_transactions(
    spark: SparkSession,
    *,
    grain: str,
    scale_pct: int,
    top_n: int | None,
    data_version: str = "v1",
    split: str = "prior",
) -> DataFrame:
    """Đọc giao dịch theo cấp, quy mô lồng nhau và giới hạn top-N item."""
    if not grain or not data_version or not split:
        raise ValueError("grain, data_version và split không được để trống")
    if scale_pct not in {10, 25, 50, 100, 200}:
        raise ValueError("scale_pct phải là 10, 25, 50, 100 hoặc 200")
    if top_n is not None and top_n < 1:
        raise ValueError("top_n phải >= 1 hoặc None")
    df = (
        spark.table(TX)
        .where((F.col("data_version") == data_version) & (F.col("grain") == grain) & (F.col("split") == split))
        .where(F.col("scale_bucket") < min(scale_pct, 100))
        .select("order_id", "items")
    )
    if top_n is not None:
        df = df.withColumn("items", F.filter("items", lambda item: item < F.lit(top_n))).where(F.size("items") > 0)
    if scale_pct == 200:
        duplicate = df.withColumn("order_id", -F.col("order_id"))
        df = df.unionByName(duplicate)
    return df


def to_rdd(df: DataFrame):
    """Chuyển DataFrame sang RDD tuple; không cache để mỗi thuật toán tự đo bộ nhớ."""
    return df.select("items").rdd.map(lambda row: tuple(row[0]))
