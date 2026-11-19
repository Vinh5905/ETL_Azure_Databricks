# Databricks notebook source
# MAGIC %pip install rapidfuzz==3.14.6

# COMMAND ----------

"""OFF, bảng cầu nối tên và danh mục sản phẩm đã làm giàu, không nhân dòng."""

from pyspark import pipelines as dp
from pyspark.cloudpickle import register_pickle_by_value
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import off, text
from common.catalog_rows import VALID_CATALOG_PRODUCT_ID
from common.off import (
    MATCH_RULES,
    assess_off,
    clean_off,
    product_matches,
    quarantine_off,
    score_series,
)
from common.product import build_dim_product

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze")
SILVER = spark.conf.get("silver")
OFF_ENABLED = spark.conf.get("enable_off", "false").lower() == "true"


def table_name(schema: str, name: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, name))


@dp.temporary_view(name="catalog_products_assessed", comment="Ghi metrics cho khóa sản phẩm nguồn không hợp lệ.")
@dp.expect("valid_catalog_product_id", VALID_CATALOG_PRODUCT_ID)
def catalog_products_assessed():
    return spark.read.table(table_name(BRONZE, "products"))


if OFF_ENABLED:
    # Worker có RapidFuzz từ môi trường pipeline, nhưng có thể không import được src/common.
    register_pickle_by_value(off)
    register_pickle_by_value(text)
    match_score = F.pandas_udf(score_series, "double")

    @dp.materialized_view(
        name=table_name(SILVER, "off_products_clean"),
        comment="OFF đã làm phẳng; bản hợp lệ mới nhất mỗi cặp product_id/off_code.",
        cluster_by=["product_id"],
    )
    @dp.expect("has_name", "off_product_name IS NOT NULL AND length(trim(off_product_name)) > 0")
    def off_products_clean():
        return clean_off(assess_off(spark.read.table(table_name(BRONZE, "off_products"))))

    @dp.materialized_view(
        name=table_name(SILVER, "quarantine_off_products"),
        comment="Mọi bản OFF sai khóa/ingest/rescue, giữ raw và các tên luật lỗi trước khử trùng.",
    )
    def quarantine_off_products():
        return quarantine_off(assess_off(spark.read.table(table_name(BRONZE, "off_products"))))

    @dp.materialized_view(
        name=table_name(SILVER, "product_off_match"),
        comment="Một ứng viên tốt nhất mỗi sản phẩm; chưa crawl/không có ứng viên Mỹ nhận no_match.",
        cluster_by=["product_id"],
    )
    @dp.expect_all_or_fail(MATCH_RULES)
    def product_off_match():
        return product_matches(
            spark.read.table("catalog_products_assessed"),
            spark.read.table(table_name(SILVER, "off_products_clean")),
            match_score,
        )


@dp.materialized_view(
    name=table_name(SILVER, "dim_product"),
    comment="Danh mục + giá hiện hành + KM tại as_of_date + OFF/ảnh matched; một dòng mỗi sản phẩm.",
    cluster_by=["product_id"],
)
@dp.expect_or_fail("no_fanout", "dup_cnt = 1")
@dp.expect_or_fail("unique_sources", "_join_sources_unique")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND product_id > 0")
@dp.expect("has_price", "current_price IS NOT NULL")
@dp.expect("catalog_payload_unique", "_catalog_sources_unique")
@dp.expect("catalog_unrescued", "_catalog_sources_unrescued")
def dim_product():
    off_sources = None
    if OFF_ENABLED:
        off_sources = (
            spark.read.table(table_name(SILVER, "product_off_match")),
            spark.read.table(table_name(SILVER, "off_products_clean")),
            spark.read.table(table_name(BRONZE, "product_images")),
        )
    return build_dim_product(
        spark.read.table("catalog_products_assessed"),
        spark.read.table(table_name(BRONZE, "aisles")),
        spark.read.table(table_name(BRONZE, "departments")),
        spark.read.table(table_name(SILVER, "product_prices_history")),
        spark.read.table(table_name(SILVER, "promotions")),
        spark.conf.get("as_of_date", "2026-03-30"),
        off_sources=off_sources,
    )
