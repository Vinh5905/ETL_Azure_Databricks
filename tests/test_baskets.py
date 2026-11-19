"""Kiểm baskets và chạy nguyên SQL view trên catalog Spark local, không giả lập SQL parser."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from common.baskets import BASKET_RULES, build_baskets

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "src/tasks/create_views.sql").read_text()


@pytest.fixture
def basket_sources(spark):
    root = ROOT / "tests/fixtures/instacart"
    headers = (
        spark.read.schema(
            "order_id BIGINT, user_id BIGINT, eval_set STRING, order_number INT, order_dow INT, "
            "order_hour_of_day INT, days_since_prior_order DOUBLE"
        )
        .option("header", "true")
        .csv(str(root / "orders.csv"))
    )
    stamp = F.lit("2026-03-30T12:00:00Z").cast("timestamp_ltz")
    item_schema = "order_id BIGINT, product_id BIGINT, add_to_cart_order INT, reordered INT"
    return {
        "orders_batch": headers.where("eval_set IN ('prior', 'test')")
        .withColumn("order_ts_sim", stamp)
        .withColumn("source", F.lit("batch"))
        .withColumn("is_test", F.col("eval_set") == "test"),
        "order_items_prior": spark.read.schema(item_schema)
        .option("header", "true")
        .csv(str(root / "order_products__prior.csv")),
        "stream_orders": headers.where("eval_set = 'train'").drop("eval_set").withColumn("event_time", stamp),
        "stream_order_items": spark.read.schema(item_schema)
        .option("header", "true")
        .csv(str(root / "order_products__train.csv")),
    }


def valid(frame):
    expression = " AND ".join(f"({rule})" for rule in ["n_items >= 1", *BASKET_RULES.values()])
    return frame.where(F.coalesce(F.expr(expression), F.lit(False)))


def test_baskets_keep_only_prior_and_stream_holdout_with_sorted_unique_items(basket_sources):
    output = build_baskets(**basket_sources)
    rows = output.collect()
    assert len(rows) == valid(output).count() == 15
    assert len({r.order_id for r in rows}) == 15
    assert sum(r.split == "prior" for r in rows) == 12
    assert sum(r.split == "holdout" for r in rows) == 3
    assert not {23, 43, 63}.intersection(r.order_id for r in rows)
    assert all(r.items == sorted(set(r.items)) and r.n_items == len(r.items) == 2 for r in rows)
    assert output.schema["items"].dataType.simpleString() == "array<int>"
    assert output.schema["order_id"].dataType.simpleString() == "int"


def test_duplicate_replay_does_not_inflate_items_or_item_count(basket_sources):
    expected = build_baskets(**basket_sources).orderBy("order_id").collect()
    replay = {name: frame.unionByName(frame).repartition(2) for name, frame in basket_sources.items()}
    assert build_baskets(**replay).orderBy("order_id").collect() == expected


@pytest.mark.parametrize("source,order", [("order_items_prior", 11), ("stream_order_items", 13)])
def test_missing_items_fail_prior_and_only_warn_holdout(basket_sources, source, order):
    data = basket_sources.copy()
    data[source] = data[source].where(F.col("order_id") != order)
    output = build_baskets(**data)
    row = output.where(F.col("order_id") == order).first()
    if source == "order_items_prior":
        assert row.items == [] and row.n_items == 0 and not row._items_valid
    else:
        assert row is None
        pending = build_baskets(**data, include_unready_holdout=True).where(F.col("order_id") == order).first()
        assert pending.n_items == 0 and pending._items_valid
    assert valid(output).count() == 14


@pytest.mark.parametrize("bad_product", [None, -1])
def test_invalid_item_is_not_silently_hidden_by_collect_set(basket_sources, bad_product):
    data = basket_sources.copy()
    bad = (
        data["order_items_prior"]
        .where("order_id = 11")
        .limit(1)
        .withColumn("product_id", F.lit(bad_product).cast("long"))
    )
    data["order_items_prior"] = data["order_items_prior"].unionByName(bad)
    output = build_baskets(**data)
    assert not output.where("order_id = 11").first()._items_valid
    assert valid(output).count() == 14


@pytest.mark.parametrize("overlap", [11, 23])
def test_stream_collision_with_batch_prior_or_test_is_rejected(basket_sources, overlap):
    data = basket_sources.copy()
    data["stream_orders"] = data["stream_orders"].withColumn(
        "order_id", F.when(F.col("order_id") == 13, F.lit(overlap)).otherwise(F.col("order_id"))
    )
    data["stream_order_items"] = data["stream_order_items"].withColumn(
        "order_id", F.when(F.col("order_id") == 13, F.lit(overlap)).otherwise(F.col("order_id"))
    )
    output = build_baskets(**data)
    collisions = output.where(f"order_id = {overlap}").collect()
    assert any(not r._source_disjoint for r in collisions)
    assert not valid(output).where(f"order_id = {overlap} AND split = 'holdout'").count()
    if overlap == 11:
        assert all(r.dup_cnt == 2 for r in collisions)


def test_conflicting_header_users_are_flagged_instead_of_two_baskets(basket_sources):
    data = basket_sources.copy()
    other = data["orders_batch"].where("order_id = 11").withColumn("user_id", F.lit(99).cast("long"))
    data["orders_batch"] = data["orders_batch"].unionByName(other)
    output = build_baskets(**data)
    assert all(r.dup_cnt == 2 for r in output.where("order_id = 11").collect())
    assert valid(output).where("order_id = 11").count() == 0


def test_empty_stream_produces_only_prior_and_batch_train_cannot_leak(basket_sources):
    data = basket_sources.copy()
    data["stream_orders"] = data["stream_orders"].limit(0)
    data["stream_order_items"] = data["stream_order_items"].limit(0)
    train = basket_sources["stream_orders"].drop("event_time").withColumn("eval_set", F.lit("train"))
    data["orders_batch"] = data["orders_batch"].unionByName(
        train.withColumn("order_ts_sim", F.lit(None).cast("timestamp_ltz"))
        .withColumn("source", F.lit("batch"))
        .withColumn("is_test", F.lit(False))
    )
    result = build_baskets(**data)
    assert result.count() == valid(result).count() == 12
    assert {r.split for r in result.select("split").distinct().collect()} == {"prior"}


@pytest.fixture
def view_catalog(spark, basket_sources):
    previous = spark.catalog.currentDatabase()
    schema = "baskets_" + uuid.uuid4().hex
    spark.sql(f"CREATE DATABASE `{schema}`")
    try:
        for name, frame in basket_sources.items():
            frame.write.saveAsTable(f"`spark_catalog`.`{schema}`.`{name}`")
        yield schema
    finally:
        spark.catalog.setCurrentDatabase(previous)
        spark.sql(f"DROP DATABASE `{schema}` CASCADE")


def create_views(spark, schema):
    statements = [part.strip() for part in SQL.split(";") if part.strip()]
    assert len(statements) == 3
    for statement in statements:
        spark.sql(statement, args={"catalog": "spark_catalog", "silver_schema": schema})


def test_parameterized_sql_creates_both_views_twice_and_preserves_split_flags(spark, view_catalog):
    create_views(spark, view_catalog)
    create_views(spark, view_catalog)
    spark.catalog.setCurrentDatabase("default")
    orders = spark.table(f"`{view_catalog}`.orders")
    items = spark.table(f"`{view_catalog}`.order_items")
    assert orders.count() == orders.select("order_id").distinct().count() == 18
    assert orders.where("is_test").count() == 3
    assert orders.where("source = 'stream' AND eval_set = 'train' AND NOT is_test").count() == 3
    assert orders.where("source = 'stream' AND order_ts_sim IS NOT NULL").count() == 3
    assert items.count() == 30 and items.where("source = 'batch'").count() == 24
    assert items.where("source = 'stream'").count() == 6
    assert items.columns == ["order_id", "product_id", "add_to_cart_order", "reordered", "source"]
    # View là live query: thay nguồn không cần chạy lại DDL, UNION ALL không che khóa trùng.
    extra = spark.table(f"`{view_catalog}`.orders_batch").where("order_id = 11").collect()
    spark.createDataFrame(extra, orders.schema).write.mode("append").saveAsTable(f"`{view_catalog}`.orders_batch")
    assert orders.count() == 19


def test_sql_requires_explicit_parameters_and_does_not_write_current_schema(spark, view_catalog):
    statement = SQL.split(";")[0]
    with pytest.raises(Exception, match="UNBOUND_SQL_PARAMETER"):
        spark.sql(statement)
    create_views(spark, view_catalog)
    assert spark.catalog.tableExists(f"{view_catalog}.orders")
    assert spark.catalog.currentDatabase() == view_catalog
