"""Đọc dữ liệu giao dịch dùng chung cho các thực nghiệm DS."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TX = "instacart.ds_data.transactions"
_SCALE_PCTS = {10, 25, 50, 100, 200}


def load_transactions(
    spark: SparkSession,
    *,
    grain: str,
    scale_pct: int,
    top_n: int | None,
    data_version: str = "v1",
    split: str = "prior",
) -> DataFrame:
    """Đọc và lọc giao dịch theo cấp item, quy mô, split và top-N.

    Tập dữ liệu ở các mức 10–100% lồng nhau theo ``scale_bucket``. Mức 200%
    lặp lại 100% với ``order_id`` âm để giữ khóa đơn hàng duy nhất. Hàm chỉ
    dựng DataFrame, không cache hoặc kích hoạt Spark action.
    """
    if scale_pct not in _SCALE_PCTS:
        raise ValueError(f"scale_pct phải thuộc {sorted(_SCALE_PCTS)}")
    if top_n is not None and top_n < 1:
        raise ValueError("top_n phải >= 1 hoặc None")

    df = (
        spark.table(TX)
        .where(
            (F.col("data_version") == data_version)
            & (F.col("grain") == grain)
            & (F.col("split") == split)
            & (F.col("scale_bucket") < min(scale_pct, 100))
        )
        .select("order_id", "items")
    )
    if top_n is not None:
        df = df.withColumn("items", F.filter("items", lambda item: item < F.lit(top_n))).where(F.size("items") > 0)
    if scale_pct == 200:
        df = df.unionByName(df.withColumn("order_id", -F.col("order_id")))
    return df


def to_rdd(df: DataFrame):
    """Chuyển ``(order_id, items)`` thành RDD các tuple item đã sắp xếp."""
    return df.select("items").rdd.map(lambda row: tuple(row[0]))
