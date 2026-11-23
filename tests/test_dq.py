from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.dq import EVENT_RULES, ITEM_RULES, ORDER_RULES, PRICE_RULES, PROMO_RULES, dq_failed

ITEM_SCHEMA = "order_id int, product_id int, add_to_cart_order int, reordered int, _rescued_data string"
EVENT_NULL_FAILURES = ["parsed", "known_type", "schema_v1", "has_items", "id_matches"]

# Mỗi ca chỉ thay các cột được ghi; kết quả kỳ vọng được viết tường minh.
RULE_CASES = [
    pytest.param(
        ORDER_RULES,
        "order_dow int, order_hour_of_day int, days_since_prior_order double, _rescued_data string",
        {"order_dow": 0, "order_hour_of_day": 0, "days_since_prior_order": None, "_rescued_data": None},
        [
            ({}, []),
            ({"order_dow": 6, "order_hour_of_day": 23, "days_since_prior_order": 30.0}, []),
            ({"days_since_prior_order": 0.0}, []),
            ({"order_dow": -1}, ["valid_dow"]),
            ({"order_dow": 7}, ["valid_dow"]),
            ({"order_dow": None}, ["valid_dow"]),
            ({"order_hour_of_day": -1}, ["valid_hour"]),
            ({"order_hour_of_day": 24}, ["valid_hour"]),
            ({"order_hour_of_day": None}, ["valid_hour"]),
            ({"days_since_prior_order": -1.0}, ["valid_dsp"]),
            ({"days_since_prior_order": 31.0}, ["valid_dsp"]),
            ({"days_since_prior_order": float("nan")}, ["valid_dsp"]),
            ({"_rescued_data": '{"bad":"value"}'}, ["no_rescued"]),
            ({"order_dow": None, "order_hour_of_day": None}, ["valid_dow", "valid_hour"]),
        ],
        id="orders",
    ),
    pytest.param(
        ITEM_RULES,
        ITEM_SCHEMA,
        {"order_id": 1, "product_id": 2, "add_to_cart_order": 1, "reordered": 0, "_rescued_data": None},
        [
            ({}, []),
            ({"add_to_cart_order": 10, "reordered": 1}, []),
            ({"order_id": None}, ["has_keys"]),
            ({"product_id": None}, ["has_keys"]),
            ({"add_to_cart_order": 0}, ["cart_order_pos"]),
            ({"add_to_cart_order": -1}, ["cart_order_pos"]),
            ({"add_to_cart_order": None}, ["cart_order_pos"]),
            ({"reordered": -1}, ["reordered_flag"]),
            ({"reordered": 2}, ["reordered_flag"]),
            ({"reordered": None}, ["reordered_flag"]),
            ({"_rescued_data": "invalid"}, ["no_rescued"]),
            (
                {"order_id": None, "product_id": None, "add_to_cart_order": None, "reordered": None},
                ["has_keys", "cart_order_pos", "reordered_flag"],
            ),
        ],
        id="items",
    ),
    pytest.param(
        PRICE_RULES,
        "unit_price decimal(10,2), currency string, product_id int",
        {"unit_price": Decimal("0.01"), "currency": "USD", "product_id": 1},
        [
            ({}, []),
            ({"unit_price": Decimal("0.00")}, ["price_positive"]),
            ({"unit_price": Decimal("-0.01")}, ["price_positive"]),
            ({"unit_price": None}, ["price_positive"]),
            ({"currency": "EUR"}, ["currency_usd"]),
            ({"currency": "usd"}, ["currency_usd"]),
            ({"currency": None}, ["currency_usd"]),
            ({"product_id": None}, ["has_product_id"]),
            ({"unit_price": None, "currency": None, "product_id": None}, list(PRICE_RULES)),
        ],
        id="prices",
    ),
    pytest.param(
        PROMO_RULES,
        "discount_pct int, start_date date, end_date date",
        {"discount_pct": 0, "start_date": date(2026, 3, 1), "end_date": date(2026, 3, 1)},
        [
            ({}, []),
            ({"discount_pct": 90, "end_date": date(2026, 3, 30)}, []),
            ({"discount_pct": -1}, ["discount_range"]),
            ({"discount_pct": 91}, ["discount_range"]),
            ({"discount_pct": None}, ["discount_range"]),
            ({"end_date": date(2026, 2, 28)}, ["valid_window"]),
            ({"start_date": None}, ["valid_window"]),
            ({"end_date": None}, ["valid_window"]),
            ({"discount_pct": None, "start_date": None, "end_date": None}, list(PROMO_RULES)),
        ],
        id="promotions",
    ),
    pytest.param(
        EVENT_RULES,
        "event_id string, event_type string, schema_version int, order_id int, "
        "items array<struct<product_id:int,add_to_cart_order:int,reordered:int>>",
        {"event_id": "order-1", "event_type": "order_placed", "schema_version": 1, "order_id": 1, "items": [(2, 1, 0)]},
        [
            ({}, []),
            ({"event_id": None}, ["parsed", "id_matches"]),
            ({"event_id": "order-2"}, ["id_matches"]),
            ({"order_id": None}, ["id_matches"]),
            ({"event_type": "other"}, ["known_type"]),
            ({"event_type": None}, ["known_type"]),
            ({"schema_version": 2}, ["schema_v1"]),
            ({"schema_version": None}, ["schema_v1"]),
            ({"items": []}, ["has_items"]),
            ({"items": None}, ["has_items"]),
            ({"items": [(None, 1, 0)]}, ["valid_item_keys"]),
            (
                {"event_id": None, "event_type": None, "schema_version": None, "order_id": None, "items": None},
                EVENT_NULL_FAILURES,
            ),
        ],
        id="events",
    ),
]


@pytest.mark.parametrize("rules,schema,valid,cases", RULE_CASES)
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_rule_sets_never_return_null_and_report_exact_failures(spark, rules, schema, valid, cases, ansi):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    original_size = spark.conf.get("spark.sql.legacy.sizeOfNull")
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.conf.set("spark.sql.legacy.sizeOfNull", "false")
        # Schema tường minh để cột NULL và giá decimal/date giữ đúng kiểu nguồn.
        fields = list(valid)
        rows = [(i, *((valid | override)[field] for field in fields)) for i, (override, _) in enumerate(cases)]
        df = spark.createDataFrame(rows, "case_id int, " + schema)
        result = df.select(
            "case_id", dq_failed(rules).alias("_dq_failed"), *[F.expr(expr).alias(name) for name, expr in rules.items()]
        )
        assert result.schema["_dq_failed"].dataType == T.ArrayType(T.StringType(), containsNull=True)
        actual = result.orderBy("case_id").collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.conf.set("spark.sql.legacy.sizeOfNull", original_size)

    assert len(actual) == len(cases)
    for row, (_, expected) in zip(actual, cases, strict=True):
        assert row._dq_failed == expected
        for name in rules:
            assert row[name] is not None, (row.case_id, name)
            assert row[name] is (name not in expected)
    # Mỗi luật đều được thử cả đạt và lỗi, không chỉ kiểm tra một dòng chuẩn.
    for name in rules:
        assert {row[name] for row in actual} == {False, True}


def test_custom_nullable_rule_is_a_failure_and_empty_rules_have_string_array_type(spark):
    df = spark.createDataFrame([(None,), (0,), (1,)], "amount int")
    actual = df.withColumn("_dq_failed", dq_failed({"positive": "amount > 0"})).collect()
    assert [row._dq_failed for row in actual] == [["positive"], ["positive"], []]
    empty = df.withColumn("_dq_failed", dq_failed({}))
    assert empty.schema["_dq_failed"].dataType.elementType == T.StringType()
    assert [row._dq_failed for row in empty.collect()] == [[], [], []]


def test_invalid_rule_configuration_is_not_silently_ignored(spark):
    with pytest.raises(AnalysisException):
        spark.range(1).select(dq_failed({"missing": "unknown_column > 0"})).collect()
    with pytest.raises(AnalysisException):
        spark.range(1).select(dq_failed({"not_boolean": "id"})).collect()


def test_quarantine_retains_original_row_and_all_failure_names(spark):
    df = spark.createDataFrame([(1, 2, 1, 0, None), (2, None, 0, 2, "bad")], ITEM_SCHEMA).withColumn(
        "source_payload", F.lit("giữ để điều tra")
    )
    quarantined = df.withColumn("_dq_failed", dq_failed(ITEM_RULES)).where(F.size("_dq_failed") > 0).collect()
    assert len(quarantined) == 1
    row = quarantined[0]
    assert row.order_id == 2 and row.product_id is None and row.source_payload == "giữ để điều tra"
    assert row._dq_failed == ["has_keys", "cart_order_pos", "reordered_flag", "no_rescued"]


def test_malformed_event_is_quarantined_with_original_payload(spark):
    schema = (
        "event_id string, event_type string, schema_version int, order_id int, "
        "items array<struct<product_id:int,add_to_cart_order:int,reordered:int>>"
    )
    good = json.dumps(
        {
            "event_id": "order-1",
            "event_type": "order_placed",
            "schema_version": 1,
            "order_id": 1,
            "items": [{"product_id": 2, "add_to_cart_order": 1, "reordered": 0}],
        }
    )
    raw = spark.createDataFrame([(good,), ("{bad json",)], "value_json string")
    parsed = raw.withColumn("e", F.from_json("value_json", schema)).select("value_json", "e.*")
    rows = parsed.withColumn("_dq_failed", dq_failed(EVENT_RULES)).collect()
    assert rows[0]._dq_failed == []
    assert rows[1].value_json == "{bad json"
    assert rows[1]._dq_failed == EVENT_NULL_FAILURES


def test_dq_expression_works_in_local_stream(spark, tmp_path):
    source = tmp_path / "events"
    source.mkdir()
    (source / "items.json").write_text(
        json.dumps({"order_id": 1, "product_id": 2, "add_to_cart_order": 1, "reordered": 0})
        + "\n"
        + json.dumps({"order_id": 2, "product_id": None, "add_to_cart_order": 0, "reordered": 1})
        + "\n"
    )
    df = spark.readStream.schema(ITEM_SCHEMA).json(str(source)).withColumn("_dq_failed", dq_failed(ITEM_RULES))
    query = (
        df.writeStream.format("memory")
        .queryName("dq_test_stream")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    try:
        assert query.awaitTermination(30)
        rows = spark.table("dq_test_stream").orderBy("order_id").collect()
        assert [row._dq_failed for row in rows] == [[], ["has_keys", "cart_order_pos"]]
    finally:
        query.stop()
        spark.catalog.dropTempView("dq_test_stream")
