"""Silver đơn hàng: clock đủ split, batch prior/test và AUTO CDC món prior."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.baskets import BASKET_RULES, COMPLETE_BASKETS, build_baskets
from common.instacart import (
    ITEM_COLUMNS,
    ITEM_INPUT_RULES,
    ORDER_INPUT_RULES,
    assess_instacart,
    batch_orders,
    canonical_headers,
    quarantine_instacart,
)
from common.sim_time import order_time_sim_sql

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze")
SILVER = spark.conf.get("silver")


def table_name(schema: str, table: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, table))


@dp.temporary_view(name="order_headers_assessed", comment="Ghi metrics DQ trên mọi bản header nguồn.")
@dp.expect_all(ORDER_INPUT_RULES)
def order_headers_assessed():
    return assess_instacart(spark.read.table(table_name(BRONZE, "orders")), "orders")


@dp.temporary_view(name="order_headers_canonical", comment="Header đủ split, DQ trước chọn phiên bản.")
@dp.expect_all_or_fail(ORDER_INPUT_RULES)
def order_headers_canonical():
    # Không bỏ hẳn một header lỗi rồi tính clock như thể lịch sử khách vẫn đầy đủ.
    return canonical_headers(spark.read.table("order_headers_assessed"))


@dp.materialized_view(name=table_name(SILVER, "order_time_sim"), table_properties={"quality": "silver"})
@dp.expect_or_fail("has_ts", "order_ts_sim IS NOT NULL")
def order_time_sim():
    return spark.sql(order_time_sim_sql("order_headers_canonical", canonical=True))


@dp.materialized_view(
    name=table_name(SILVER, "orders_batch"),
    cluster_by=["user_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("has_ts", "order_ts_sim IS NOT NULL")
def orders_batch():
    return batch_orders(spark.read.table(table_name(SILVER, "order_time_sim")))


@dp.temporary_view(name="order_items_prior_valid", comment="DQ món prior trước AUTO CDC.")
@dp.expect_all_or_drop(ITEM_INPUT_RULES)
def order_items_prior_valid():
    return assess_instacart(spark.readStream.table(table_name(BRONZE, "order_products_prior")), "items")


@dp.temporary_view(name="order_items_prior_src")
def order_items_prior_src():
    return spark.readStream.table("order_items_prior_valid").select(*ITEM_COLUMNS)


@dp.table(name=table_name(SILVER, "quarantine_orders"), table_properties={"quality": "silver"})
def quarantine_orders():
    return quarantine_instacart(assess_instacart(spark.readStream.table(table_name(BRONZE, "orders")), "orders"))


@dp.table(name=table_name(SILVER, "quarantine_order_items_prior"), table_properties={"quality": "silver"})
def quarantine_order_items_prior():
    return quarantine_instacart(
        assess_instacart(spark.readStream.table(table_name(BRONZE, "order_products_prior")), "items")
    )


dp.create_streaming_table(
    name=table_name(SILVER, "order_items_prior"),
    cluster_by=["order_id"],
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    name="order_items_prior_scd1",
    target=table_name(SILVER, "order_items_prior"),
    source="order_items_prior_src",
    keys=["order_id", "product_id"],
    sequence_by=F.struct("_ingest_ts", "_file_mtime", "_source_file"),
    except_column_list=["_rescued_data", "_ingest_ts", "_source_file", "_file_mtime"],
    stored_as_scd_type=1,
)


if spark.conf.get("enable_baskets", "false").lower() == "true":

    @dp.temporary_view(name="basket_candidates")
    @dp.expect("holdout_items_present", "split <> 'holdout' OR n_items > 0")
    def basket_candidates():
        return build_baskets(
            spark.read.table(table_name(SILVER, "orders_batch")),
            spark.read.table(table_name(SILVER, "order_items_prior")),
            spark.read.table(table_name(SILVER, "stream_orders")),
            spark.read.table(table_name(SILVER, "stream_order_items")),
            include_unready_holdout=True,
        )

    @dp.materialized_view(
        name=table_name(SILVER, "baskets"),
        cluster_by=["split"],
        table_properties={"quality": "silver"},
        comment="Giỏ DS: batch prior để học, stream train làm holdout; không có đơn test.",
    )
    @dp.expect_or_fail("non_empty_prior_basket", "split <> 'prior' OR n_items > 0")
    @dp.expect_all_or_fail(BASKET_RULES)
    def baskets():
        return spark.read.table("basket_candidates").where(COMPLETE_BASKETS)
