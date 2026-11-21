"""Nạp sự kiện Kafka vào Bronze. Pipeline quản lý checkpoint cho từng flow."""

import json

from pyspark import pipelines as dp
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.event_hubs import sas_endpoint

spark = SparkSession.active()
GENERATION = spark.conf.get("eh_generation")
TOPIC = spark.conf.get("eh_topic")
TABLE_NAME = ".".join(
    "`" + part.replace("`", "``") + "`"
    for part in (spark.conf.get("catalog"), spark.conf.get("bronze"), "order_events")
)


def _kafka_options(connection: str) -> dict[str, str]:
    """Cấu hình Kafka cho SAS Listen, giữ lỗi cấu hình không chứa secret."""
    connection = connection.strip()
    bootstrap_server, entity_path = sas_endpoint(connection)
    if entity_path is not None and entity_path != TOPIC:
        raise ValueError("Secret eh-listen-connection-string phải cấp quyền cho hub được cấu hình")

    return {
        "kafka.bootstrap.servers": bootstrap_server,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="$ConnectionString" password={json.dumps(connection, ensure_ascii=False)};'
        ),
        "subscribe": TOPIC,
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": "20000",
    }


dp.create_streaming_table(
    name=TABLE_NAME,
    comment="Bản tin Kafka nguyên vẹn và metadata Event Hubs.",
    table_properties={"pipelines.reset.allowed": "false", "quality": "bronze"},
)


@dp.append_flow(target=TABLE_NAME, name=f"kafka_{GENERATION}")
def kafka_to_bronze():
    """Đọc từ checkpoint của thế hệ hiện tại và giữ nguyên dữ liệu nguồn."""
    dbutils = DBUtils(spark)
    connection = dbutils.secrets.get("kv", "eh-listen-connection-string")
    return (
        spark.readStream.format("kafka")
        .options(**_kafka_options(connection))
        .load()
        .select(
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("value_json"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_enqueued_ts"),
            F.lit(GENERATION).alias("_eh_generation"),
            F.current_timestamp().alias("_ingest_ts"),
        )
    )
