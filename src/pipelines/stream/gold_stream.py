"""Tổng hợp Gold theo thời điểm Event Hubs nhận bản tin và đo độ trễ từng chặng."""

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.dq import EVENT_RULES

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
GOLD = spark.conf.get("gold")
OPS = spark.conf.get("ops")


def table_name(schema: str, name: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, name))


def deduped_events() -> DataFrame:
    """Gộp bản tin trùng trong một lượt phát, giữ lượt phát lại riêng."""
    return (
        spark.readStream.table("events_valid")
        .withWatermark("_enqueued_ts", "2 minutes")
        .dropDuplicatesWithinWatermark(["event_id", "replay_run_id"])
    )


@dp.table(
    name=table_name(GOLD, "stream_kpi_1m"),
    comment="Số đơn, số món và số khách ước tính theo phút Event Hubs nhận bản tin.",
    table_properties={"quality": "gold"},
)
def stream_kpi_1m() -> DataFrame:
    return (
        deduped_events()
        .groupBy(F.window("_enqueued_ts", "1 minute"))
        .agg(
            F.count("*").alias("orders"),
            F.sum(F.size("items")).alias("items"),
            F.approx_count_distinct("user_id").alias("users"),
        )
        .select(F.col("window.start").alias("minute"), "orders", "items", "users")
    )


@dp.table(
    name=table_name(GOLD, "stream_pair_counts_10m"),
    comment="Số lần mua cùng nhau của mỗi cặp sản phẩm trong cửa sổ nhận 10 phút đã chốt.",
    table_properties={"quality": "gold"},
)
def stream_pair_counts_10m() -> DataFrame:
    # Mỗi món chỉ ghép với các món đứng sau nó. Sắp mã trong cặp để gộp cả hai thứ tự.
    pairs = F.expr("""
        flatten(transform(items, (a, i) ->
            transform(slice(items, i + 2, size(items)), b ->
                named_struct('p1', least(a.product_id, b.product_id),
                             'p2', greatest(a.product_id, b.product_id)))))
        """)
    return (
        deduped_events()
        .select("_enqueued_ts", F.explode(pairs).alias("pair"))
        .groupBy(F.window("_enqueued_ts", "10 minutes"), "pair.p1", "pair.p2")
        .agg(F.count("*").alias("pair_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "p1",
            "p2",
            "pair_count",
        )
    )


def elapsed_ms(start: str, end: str) -> Column:
    return (F.unix_micros(end) - F.unix_micros(start)) / 1000


@dp.table(
    name=table_name(OPS, "streaming_latency"),
    comment="Độ trễ producer, Event Hubs, Bronze và Silver cho từng lần nhận sự kiện đã parse.",
)
def streaming_latency() -> DataFrame:
    return (
        spark.readStream.table("events_parsed")
        .where(EVENT_RULES["parsed"])
        .select(
            "event_id",
            "replay_run_id",
            "producer_location",
            "produced_at",
            "_enqueued_ts",
            "_ingest_ts",
            "_silver_ts",
            elapsed_ms("produced_at", "_enqueued_ts").alias("ms_producer_to_eh"),
            elapsed_ms("_enqueued_ts", "_ingest_ts").alias("ms_eh_to_bronze"),
            elapsed_ms("produced_at", "_silver_ts").alias("ms_end_to_end"),
            F.lit(spark.conf.get("eh_region", "unknown")).alias("eh_region"),
            F.lit(spark.conf.get("dbx_region", "unknown")).alias("dbx_region"),
        )
    )
