"""Đồ thị smoke cho AD-ST-03 trước khi pipeline có nguồn Kafka."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dp.materialized_view(
    name="_stream_bootstrap",
    private=True,
    comment="Kiểm tra cấu hình và event log của pipeline stream.",
)
def stream_bootstrap():
    """Trả một dòng cấu hình trong phạm vi pipeline."""
    spark = SparkSession.active()
    keys = ("catalog", "bronze", "silver", "gold", "ops", "eh_topic", "eh_generation", "eh_region", "dbx_region")
    return spark.range(1).select(*(F.lit(spark.conf.get(key)).alias(key) for key in keys))
