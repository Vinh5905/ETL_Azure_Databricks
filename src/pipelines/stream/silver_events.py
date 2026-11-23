"""Parse, kiểm chất lượng và upsert đơn hàng từ luồng Kafka."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.dq import EVENT_RULES, dq_failed

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze")
SILVER = spark.conf.get("silver")

ITEM = T.StructType(
    [
        T.StructField("product_id", T.IntegerType()),
        T.StructField("add_to_cart_order", T.IntegerType()),
        T.StructField("reordered", T.IntegerType()),
    ]
)
EVENT_V1 = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("order_id", T.IntegerType()),
        T.StructField("user_id", T.IntegerType()),
        T.StructField("order_number", T.IntegerType()),
        T.StructField("order_dow", T.IntegerType()),
        T.StructField("order_hour_of_day", T.IntegerType()),
        T.StructField("days_since_prior_order", T.DoubleType()),
        T.StructField("event_time", T.TimestampType()),
        T.StructField("replay_run_id", T.StringType()),
        T.StructField("produced_at", T.TimestampType()),
        T.StructField("producer_location", T.StringType()),
        T.StructField("items", T.ArrayType(ITEM)),
    ]
)
PARSE_SCHEMA = T.StructType([*EVENT_V1.fields, T.StructField("_corrupt_event", T.StringType())])


def table_name(schema: str, table: str) -> str:
    """Quote tên catalog và schema để giữ nguyên prefix dev."""
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, table))


@dp.temporary_view(comment="Sự kiện đã parse, nguồn chung cho Silver và Gold stream.")
def events_parsed():
    return (
        spark.readStream.table(table_name(BRONZE, "order_events"))
        .withColumn("_event", F.from_json("value_json", PARSE_SCHEMA, {"columnNameOfCorruptRecord": "_corrupt_event"}))
        # Spark có thể parse được một phần bản tin hỏng. Đưa cả bản tin đó vào quarantine.
        .withColumn("_event", F.when(F.col("_event._corrupt_event").isNull(), F.col("_event")))
        .select(
            "_event.*",
            "value_json",
            "_enqueued_ts",
            "_ingest_ts",
            "_kafka_partition",
            "_kafka_offset",
            "_eh_generation",
        )
        .drop("_corrupt_event")
        .withColumn("_silver_ts", F.current_timestamp())
    )


@dp.temporary_view()
@dp.expect_all_or_drop(EVENT_RULES)
def events_valid():
    return spark.readStream.table("events_parsed").drop("value_json")


@dp.table(
    name=table_name(SILVER, "quarantine_order_events"),
    comment="Sự kiện lỗi, payload Bronze và danh sách luật không đạt.",
    table_properties={"quality": "silver"},
)
def quarantine_order_events():
    return (
        spark.readStream.table("events_parsed")
        .withColumn("_dq_failed", dq_failed(EVENT_RULES))
        .where(F.size("_dq_failed") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )


@dp.temporary_view()
def event_items():
    return (
        spark.readStream.table("events_valid")
        .select("order_id", "_enqueued_ts", "_kafka_partition", "_kafka_offset", F.explode("items").alias("item"))
        .select("order_id", "item.*", "_enqueued_ts", "_kafka_partition", "_kafka_offset")
    )


dp.create_streaming_table(
    name=table_name(SILVER, "stream_orders"),
    comment="Đơn train đến qua stream, mỗi order_id giữ bản mới nhất.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    name="stream_orders_scd1",
    target=table_name(SILVER, "stream_orders"),
    source="events_valid",
    keys=["order_id"],
    sequence_by=F.struct("_enqueued_ts", "_kafka_partition", "_kafka_offset"),
    except_column_list=["items", "event_type", "schema_version", "_kafka_partition", "_kafka_offset", "_eh_generation"],
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name=table_name(SILVER, "stream_order_items"),
    comment="Món trong đơn stream, một dòng mỗi order_id và product_id.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    name="stream_order_items_scd1",
    target=table_name(SILVER, "stream_order_items"),
    source="event_items",
    keys=["order_id", "product_id"],
    sequence_by=F.struct("_enqueued_ts", "_kafka_partition", "_kafka_offset"),
    except_column_list=["_kafka_partition", "_kafka_offset"],
    stored_as_scd_type=1,
)
