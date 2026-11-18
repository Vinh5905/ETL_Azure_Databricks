"""Đếm cặp phổ biến bằng Spark SQL/DataFrame trên YARN."""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("Cách dùng: pair_count_df.py <input> <output> <min_count>")
    input_path, output_path, min_count_raw = sys.argv[1:]
    min_count = int(min_count_raw)
    if min_count < 1:
        raise ValueError("min_count phải >= 1")
    spark = SparkSession.builder.appName("pair-count-dataframe").getOrCreate()
    transactions = (
        spark.read.text(input_path).select(F.split(F.trim("value"), r"\s+").cast("array<int>").alias("items")).cache()
    )
    try:
        item_counts = transactions.select(F.explode("items").alias("item")).groupBy("item").count()
        frequent = [row.item for row in item_counts.where(F.col("count") >= min_count).select("item").collect()]
        if not frequent:
            spark.createDataFrame([], "value string").write.mode("overwrite").text(output_path)
            return
        kept = transactions.select(
            F.array_sort(F.array_intersect("items", F.array(*[F.lit(item) for item in frequent]))).alias("items")
        )
        indexed = kept.select(F.monotonically_increasing_id().alias("tx_id"), "items")
        left = indexed.select("tx_id", F.posexplode("items").alias("left_pos", "left_item"))
        right = indexed.select("tx_id", F.posexplode("items").alias("right_pos", "right_item"))
        pairs = (
            left.join(right, ["tx_id", "left_pos"])
            .where(F.col("right_pos") > F.col("left_pos"))
            .groupBy("left_item", "right_item")
            .count()
            .where(F.col("count") >= min_count)
            .select(F.concat_ws(",", "left_item", "right_item", "count").alias("value"))
        )
        pairs.write.mode("overwrite").text(output_path)
    finally:
        transactions.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
