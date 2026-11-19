"""Giỏ hàng DS từ batch prior và stream train; không đọc view để tránh vòng bootstrap."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASKET_RULES = {
    "valid_basket_keys": "order_id IS NOT NULL AND order_id > 0 AND user_id IS NOT NULL AND user_id > 0",
    "valid_basket_items": "_items_valid",
    "disjoint_sources": "_source_disjoint",
    "unique_basket_order": "dup_cnt = 1",
}


COMPLETE_BASKETS = "split = 'prior' OR n_items > 0 OR NOT _items_valid"


def _items_by_order(items: DataFrame) -> DataFrame:
    parsed = items.select("order_id", F.expr("try_cast(product_id AS INT)").alias("product_id"))
    valid_item = F.col("product_id").isNotNull() & (F.col("product_id") > 0)
    return parsed.groupBy("order_id").agg(
        F.array_sort(F.collect_set("product_id")).alias("items"),
        F.min(valid_item.cast("int")).eqNullSafe(1).alias("_items_valid"),
    )


def build_baskets(
    orders_batch: DataFrame,
    order_items_prior: DataFrame,
    stream_orders: DataFrame,
    stream_order_items: DataFrame,
    *,
    include_unready_holdout: bool = False,
) -> DataFrame:
    """Gom món trước khi join header. Header holdout đang chờ món chỉ vào view kiểm cảnh báo."""
    batch_ids = orders_batch.select("order_id").distinct().withColumn("_in_batch", F.lit(True))
    prior = orders_batch.where(F.col("eval_set") == "prior").select("order_id", "user_id").distinct()
    holdout = stream_orders.select("order_id", "user_id").distinct().join(batch_ids, "order_id", "left")
    prior_baskets = (
        prior.join(_items_by_order(order_items_prior), "order_id", "left")
        .withColumn("split", F.lit("prior"))
        .withColumn("_source_disjoint", F.lit(True))
        .withColumn("_items_valid", F.coalesce("_items_valid", F.lit(False)))
    )
    holdout_baskets = (
        holdout.join(_items_by_order(stream_order_items), "order_id", "left" if include_unready_holdout else "inner")
        .withColumn("split", F.lit("holdout"))
        .withColumn("_source_disjoint", F.col("_in_batch").isNull())
        .withColumn("_items_valid", F.coalesce("_items_valid", F.lit(True)))
    )
    columns = ["order_id", "user_id", "items", "split", "_items_valid", "_source_disjoint"]
    typed = (
        prior_baskets.select(*columns)
        .unionByName(holdout_baskets.select(*columns))
        .withColumn("order_id", F.expr("try_cast(order_id AS INT)"))
        .withColumn("user_id", F.expr("try_cast(user_id AS INT)"))
        .withColumn("items", F.coalesce("items", F.array().cast("array<int>")))
        .withColumn("n_items", F.size("items"))
    )
    # Không để header holdout chưa có món gây dup_cnt cho một giỏ prior hợp lệ.
    return typed.withColumn(
        "dup_cnt", F.sum(F.expr(COMPLETE_BASKETS).cast("long")).over(Window.partitionBy("order_id"))
    )
