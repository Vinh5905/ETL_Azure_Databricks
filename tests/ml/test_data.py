from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

import ml.data as data
from ml.data import load_transactions, to_rdd

FIXTURE = Path(__file__).parents[1] / "fixtures" / "ds_transactions.jsonl"

SCHEMA = StructType(
    [
        StructField("data_version", StringType(), False),
        StructField("grain", StringType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("split", StringType(), False),
        StructField("items", ArrayType(IntegerType(), False), False),
        StructField("scale_bucket", IntegerType(), False),
    ]
)


@pytest.fixture(autouse=True)
def transactions_table(spark, monkeypatch):
    table_name = "ds_transactions_fixture"
    monkeypatch.setattr(data, "TX", table_name)
    spark.read.schema(SCHEMA).json(str(FIXTURE)).createOrReplaceTempView(table_name)


def test_load_transactions_filters_and_uses_nested_scales(spark):
    scale_10 = load_transactions(spark, grain="product", scale_pct=10, top_n=None)
    scale_25 = load_transactions(spark, grain="product", scale_pct=25, top_n=None)

    ids_10 = {row.order_id for row in scale_10.collect()}
    ids_25 = {row.order_id for row in scale_25.collect()}

    assert ids_10 == {10, 11, 12}
    assert ids_10 <= ids_25
    assert 17 not in ids_25  # grain khác
    assert 18 not in ids_25  # split khác prior
    assert 19 not in ids_25  # data_version khác v1
    assert scale_10.storageLevel.useMemory is False
    assert scale_10.storageLevel.useDisk is False


def test_scale_200_duplicates_scale_100_with_negative_ids(spark):
    scale_100 = load_transactions(spark, grain="product", scale_pct=100, top_n=None)
    scale_200 = load_transactions(spark, grain="product", scale_pct=200, top_n=None)

    rows_100 = scale_100.collect()
    rows_200 = scale_200.collect()
    assert len(rows_200) == 2 * len(rows_100)
    assert {row.order_id for row in rows_200} == (
        {row.order_id for row in rows_100} | {-row.order_id for row in rows_100}
    )


def test_top_n_filters_items_and_drops_empty_baskets(spark):
    rows = load_transactions(spark, grain="product", scale_pct=100, top_n=2).collect()

    assert {row.order_id for row in rows} == {10, 11, 12, 13, 15}
    assert all(all(item < 2 for item in row.items) for row in rows)
    assert {row.order_id: row.items for row in rows}[13] == [0]


def test_to_rdd_returns_item_tuples(spark):
    df = load_transactions(spark, grain="product", scale_pct=10, top_n=None)

    assert set(to_rdd(df).collect()) == {(0, 1, 3), (0, 2), (1, 2, 4)}


@pytest.mark.parametrize("scale_pct", [0, 5, 15, 101, 2000])
def test_load_transactions_rejects_unsupported_scale(spark, scale_pct):
    with pytest.raises(ValueError, match="scale_pct"):
        load_transactions(spark, grain="product", scale_pct=scale_pct, top_n=None)


def test_load_transactions_rejects_nonpositive_top_n(spark):
    with pytest.raises(ValueError, match="top_n"):
        load_transactions(spark, grain="product", scale_pct=100, top_n=0)
