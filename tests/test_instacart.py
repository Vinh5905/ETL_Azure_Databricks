"""Kiểm silver đơn hàng bằng Spark thật; dp chỉ giả lập wiring/expectations, không phải AUTO CDC."""

from __future__ import annotations

import csv
import json
import sys
import uuid
from functools import wraps
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from pyspark.sql import functions as F

from common.baskets import BASKET_RULES
from common.instacart import (
    ITEM_COLUMNS,
    ITEM_INPUT_RULES,
    ORDER_INPUT_RULES,
    assess_instacart,
    canonical_headers,
    quarantine_instacart,
)
from common.sim_time import order_time_sim_sql, order_ts_sim

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def strict_session(spark):
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.timestampType": "TIMESTAMP_NTZ",
    }
    previous = {key: spark.conf.get(key) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def metadata(frame):
    return (
        frame.withColumn("_rescued_data", F.lit(None).cast("string"))
        .withColumn("_source_file", F.lit("fixture/release=2017/data.csv"))
        .withColumn("_ingest_ts", F.lit("2026-11-17T00:00:00Z").cast("timestamp_ltz"))
        .withColumn("_file_mtime", F.lit("2026-11-16T00:00:00Z").cast("timestamp_ltz"))
    )


@pytest.fixture
def inputs(spark):
    return {
        name: metadata(
            spark.read.option("header", "true").csv(str(ROOT / "tests/fixtures/instacart" / f"{filename}.csv"))
        )
        for name, filename in [("orders", "orders"), ("items", "order_products__prior")]
    }


@pytest.mark.parametrize(
    "column,value,failed",
    [
        ("order_hour_of_day", "24", ["valid_hour"]),
        ("order_dow", "7", ["valid_dow"]),
        ("days_since_prior_order", "bad", ["parsed_gap"]),
        ("days_since_prior_order", "-1", ["valid_dsp"]),
        ("order_id", "bad", ["has_header_keys"]),
        ("eval_set", "unknown", ["valid_split"]),
        ("_ingest_ts", "bad", ["has_ingest_ts"]),
        ("_rescued_data", '{"extra":1}', ["no_rescued"]),
    ],
)
def test_bad_headers_keep_original_payload_and_exact_reasons(inputs, column, value, failed):
    raw = inputs["orders"].where("order_id = '11'").withColumn(column, F.lit(value))
    result = quarantine_instacart(assess_instacart(raw, "orders")).first()
    assert result._dq_failed == failed and result[column] == value
    assert result._quarantined_at is not None


def test_missing_gap_after_first_order_is_rejected(inputs):
    raw = inputs["orders"].where("order_id = '12'").withColumn("days_since_prior_order", F.lit(None).cast("string"))
    assert assess_instacart(raw, "orders").first()._dq_failed == ["has_history_gap"]


@pytest.mark.parametrize(
    "column,value,failed",
    [
        ("order_id", None, ["has_keys", "positive_keys"]),
        ("product_id", "-1", ["positive_keys"]),
        ("add_to_cart_order", "0", ["cart_order_pos"]),
        ("reordered", "2", ["reordered_flag"]),
        ("_ingest_ts", "bad", ["has_ingest_ts"]),
        ("_rescued_data", '{"extra":1}', ["no_rescued"]),
    ],
)
def test_bad_items_keep_source_value_and_all_failed_rules(inputs, column, value, failed):
    raw = inputs["items"].limit(1).withColumn(column, F.lit(value).cast("string"))
    result = quarantine_instacart(assess_instacart(raw, "items")).first()
    assert result._dq_failed == failed and result[column] == value


def test_canonical_headers_prefer_valid_then_latest_and_keep_unrecoverable_header(inputs):
    base = inputs["orders"]
    bad = (
        base.where("order_id = '11'")
        .withColumn("order_hour_of_day", F.lit("99"))
        .withColumn("_ingest_ts", F.lit("2026-11-18T00:00:00Z").cast("timestamp_ltz"))
    )
    replay = base.withColumn("_ingest_ts", F.lit("2026-11-17T01:00:00Z").cast("timestamp_ltz"))
    source = base.unionByName(bad).unionByName(replay)
    checked = assess_instacart(source, "orders")
    canonical = canonical_headers(checked)
    assert canonical.count() == 18
    row = canonical.where("order_id = 11").first()
    assert row.order_hour_of_day == 8 and row._dq_failed == []
    assert quarantine_instacart(checked).count() == 1
    only_bad = canonical_headers(assess_instacart(bad, "orders")).first()
    assert only_bad._dq_failed == ["valid_hour"]


@pytest.fixture
def pipeline(spark, monkeypatch, inputs, request):
    datasets, flows, targets, queries = {}, [], {}, []

    def register(kind):
        def decorator(**kwargs):
            def decorate(function):
                datasets[kwargs["name"]] = (function, kind, kwargs)
                return function

            return decorate

        return decorator

    def expectation(action, rules):
        def decorate(function):
            @wraps(function)
            def execute():
                frame = function()
                valid = F.lit(True)
                for condition in rules.values():
                    valid = valid & F.coalesce(F.expr(condition), F.lit(False))
                if action == "fail" and frame.where(~valid).limit(1).count():
                    raise ValueError("expectation_failed")
                return frame.where(valid) if action == "drop" else frame

            execute.expectations = [*getattr(function, "expectations", []), (action, rules)]
            return execute

        return decorate

    def create_streaming_table(**kwargs):
        targets[kwargs["name"]] = kwargs

    def create_auto_cdc_flow(**kwargs):
        assert kwargs["target"] in targets
        flows.append(kwargs)

    monkeypatch.setitem(
        sys.modules,
        "pyspark.pipelines",
        SimpleNamespace(
            temporary_view=register("view"),
            materialized_view=register("mv"),
            table=register("stream"),
            expect_all_or_fail=lambda rules: expectation("fail", rules),
            expect_all=lambda rules: expectation("warn", rules),
            expect_all_or_drop=lambda rules: expectation("drop", rules),
            expect_or_fail=lambda name, condition: expectation("fail", {name: condition}),
            expect=lambda name, condition: expectation("warn", {name: condition}),
            create_streaming_table=create_streaming_table,
            create_auto_cdc_flow=create_auto_cdc_flow,
        ),
    )
    configuration = {
        "catalog": "test_catalog",
        "bronze": "p2_bronze",
        "silver": "p2_silver",
        "enable_baskets": str(getattr(request, "param", True)).lower(),
    }
    previous = {key: spark.conf.get(key, None) for key in configuration}
    try:
        for key, value in configuration.items():
            spark.conf.set(key, value)
        module = ModuleType("silver_instacart_under_test")
        source = ROOT / "src/pipelines/batch/silver_instacart.py"
        exec(compile(source.read_text(), str(source), "exec"), module.__dict__)
        raw = {
            "`test_catalog`.`p2_bronze`.`orders`": inputs["orders"],
            "`test_catalog`.`p2_bronze`.`order_products_prior`": inputs["items"],
        }

        def read_table(name):
            return raw[name] if name in raw else datasets[name][0]()

        def sql(query):
            queries.append(query)
            read_table("order_headers_canonical").createOrReplaceTempView("order_headers_canonical")
            try:
                return spark.sql(query)
            finally:
                spark.catalog.dropTempView("order_headers_canonical")

        module.spark = SimpleNamespace(
            read=SimpleNamespace(table=read_table), readStream=SimpleNamespace(table=read_table), sql=sql
        )
        yield SimpleNamespace(
            datasets=datasets, flows=flows, targets=targets, raw=raw, read=read_table, queries=queries
        )
    finally:
        for key, value in previous.items():
            spark.conf.unset(key) if value is None else spark.conf.set(key, value)


def test_pipeline_clock_reuses_shared_sql_and_matches_python_for_every_split(pipeline):
    clock = pipeline.read("`test_catalog`.`p2_silver`.`order_time_sim`")
    actual = {
        row.order_id: row.epoch for row in clock.selectExpr("order_id", "unix_seconds(order_ts_sim) AS epoch").collect()
    }
    assert len(actual) == 18
    assert pipeline.queries == [order_time_sim_sql("order_headers_canonical", canonical=True)]
    assert "row_number" not in pipeline.queries[0]
    with (ROOT / "tests/fixtures/instacart/orders.csv").open(newline="") as handle:
        original = list(csv.DictReader(handle))
    for user in {row["user_id"] for row in original}:
        history = sorted(
            (row for row in original if row["user_id"] == user), key=lambda row: int(row["order_number"]), reverse=True
        )
        days_back = 0
        for row in history:
            expected = order_ts_sim(int(user), int(row["order_hour_of_day"]), days_back)
            assert actual[int(row["order_id"])] == int(expected.timestamp())
            days_back += int(float(row["days_since_prior_order"] or 0))


def test_orders_batch_keeps_prior_test_flags_unique_keys_and_clock(pipeline):
    frame = pipeline.read("`test_catalog`.`p2_silver`.`orders_batch`")
    rows = frame.collect()
    assert len(rows) == len({row.order_id for row in rows}) == 15
    assert {row.eval_set for row in rows} == {"prior", "test"}
    assert sum(row.is_test for row in rows) == 3
    assert all(row.source == "batch" and row.order_ts_sim is not None for row in rows)
    assert not {"_raw", "_rescued_data", "_dq_failed", "_source_file", "_file_mtime"}.intersection(frame.columns)
    definition = pipeline.datasets["`test_catalog`.`p2_silver`.`orders_batch`"]
    assert definition[1] == "mv" and definition[2]["cluster_by"] == ["user_id"]


def test_irrecoverable_header_blocks_clock_and_is_preserved_in_quarantine(pipeline):
    key = "`test_catalog`.`p2_bronze`.`orders`"
    pipeline.raw[key] = pipeline.raw[key].withColumn(
        "order_hour_of_day", F.when(F.col("order_id") == "11", F.lit("99")).otherwise(F.col("order_hour_of_day"))
    )
    with pytest.raises(ValueError, match="expectation_failed"):
        pipeline.read("`test_catalog`.`p2_silver`.`order_time_sim`")
    bad = pipeline.read("`test_catalog`.`p2_silver`.`quarantine_orders`").collect()
    assert len(bad) == 1 and bad[0].order_hour_of_day == "99"
    assert bad[0]._dq_failed == ["valid_hour"]


def test_bad_replay_header_is_visible_to_dq_but_does_not_replace_valid_history(pipeline):
    key = "`test_catalog`.`p2_bronze`.`orders`"
    bad = pipeline.raw[key].where("order_id = '11'").withColumn("order_hour_of_day", F.lit("99"))
    pipeline.raw[key] = pipeline.raw[key].unionByName(bad)
    assert ("warn", ORDER_INPUT_RULES) in pipeline.datasets["order_headers_assessed"][0].expectations
    assert pipeline.read("`test_catalog`.`p2_silver`.`order_time_sim`").count() == 18
    assert pipeline.read("`test_catalog`.`p2_silver`.`quarantine_orders`").count() == 1


def test_item_flow_has_composite_keys_ingest_sequence_and_prior_source_only(pipeline):
    assert len(pipeline.flows) == 1
    flow = pipeline.flows[0]
    assert flow["keys"] == ["order_id", "product_id"] and flow["stored_as_scd_type"] == 1
    assert pipeline.targets[flow["target"]]["cluster_by"] == ["order_id"]
    assert flow["except_column_list"] == ["_rescued_data", "_ingest_ts", "_source_file", "_file_mtime"]
    valid = pipeline.read(flow["source"])
    assert valid.columns == ITEM_COLUMNS and valid.count() == 24
    sequence = valid.select(flow["sequence_by"].alias("sequence")).schema["sequence"].dataType
    assert sequence.fieldNames() == ["_ingest_ts", "_file_mtime", "_source_file"]
    assert ("drop", ITEM_INPUT_RULES) in pipeline.datasets["order_items_prior_valid"][0].expectations
    assert ("fail", ORDER_INPUT_RULES) in pipeline.datasets["order_headers_canonical"][0].expectations


def test_invalid_item_is_quarantined_and_not_sent_to_cdc(pipeline):
    key = "`test_catalog`.`p2_bronze`.`order_products_prior`"
    invalid = pipeline.raw[key].limit(1).withColumn("add_to_cart_order", F.lit("0"))
    pipeline.raw[key] = pipeline.raw[key].unionByName(invalid)
    assert pipeline.read("order_items_prior_src").count() == 24
    bad = pipeline.read("`test_catalog`.`p2_silver`.`quarantine_order_items_prior`").first()
    assert bad.add_to_cart_order == "0" and bad._dq_failed == ["cart_order_pos"]


def test_item_preprocessing_remains_streaming_and_preserves_later_replay_events(spark, inputs, tmp_path):
    source = tmp_path / "incoming"
    source.mkdir()
    with (ROOT / "tests/fixtures/instacart/order_products__prior.csv").open(newline="") as handle:
        original = list(csv.DictReader(handle))[:2]
    events = [
        {
            **row,
            "_rescued_data": None,
            "_source_file": "original.csv",
            "_ingest_ts": "2026-11-17T00:00:00Z",
            "_file_mtime": "2026-11-16T00:00:00Z",
        }
        for row in original
    ]
    (source / "01.json").write_text(
        "\n".join(json.dumps(row) for row in [*events, {**events[0], "add_to_cart_order": "0"}])
    )
    raw = spark.readStream.schema(inputs["items"].schema).json(str(source))
    assessed = assess_instacart(raw, "items")
    assert assessed.isStreaming and quarantine_instacart(assessed).isStreaming
    valid = assessed.where(F.size("_dq_failed") == 0).select(*ITEM_COLUMNS)
    name = "instacart_events_" + uuid.uuid4().hex
    query = (
        valid.writeStream.format("memory")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )
    try:
        query.processAllAvailable()
        assert spark.table(name).count() == 2
        replay = [{**row, "_source_file": "replay.csv", "_ingest_ts": "2026-11-18T00:00:00Z"} for row in events]
        (source / "02.json").write_text("\n".join(json.dumps(row) for row in replay))
        query.processAllAvailable()
        assert spark.table(name).count() == 4
        assert spark.table(name).select("order_id", "product_id").distinct().count() == 2
        # Đây là nguồn cho AUTO CDC: giữ cả event mới, engine SCD1 mới chịu trách nhiệm upsert target.
        assert spark.table(name).select("_ingest_ts").distinct().count() == 2
    finally:
        query.stop()
        spark.catalog.dropTempView(name)


@pytest.fixture
def baskets_pipeline(pipeline, spark):
    silver = "`test_catalog`.`p2_silver`"
    headers = assess_instacart(pipeline.raw["`test_catalog`.`p2_bronze`.`orders`"], "orders")
    pipeline.raw[f"{silver}.`stream_orders`"] = headers.where("eval_set = 'train'").select("order_id", "user_id")
    pipeline.raw[f"{silver}.`order_items_prior`"] = assess_instacart(
        pipeline.raw["`test_catalog`.`p2_bronze`.`order_products_prior`"], "items"
    ).select("order_id", "product_id")
    pipeline.raw[f"{silver}.`stream_order_items`"] = (
        spark.read.schema("order_id BIGINT, product_id BIGINT, add_to_cart_order INT, reordered INT")
        .option("header", "true")
        .csv(str(ROOT / "tests/fixtures/instacart/order_products__train.csv"))
    )
    return pipeline


def test_pipeline_baskets_reads_silver_tables_and_applies_failure_rules(baskets_pipeline):
    name = "`test_catalog`.`p2_silver`.`baskets`"
    output = baskets_pipeline.read(name)
    assert output.count() == 15
    fn, kind, definition = baskets_pipeline.datasets[name]
    assert kind == "mv" and definition["cluster_by"] == ["split"]
    assert ("fail", {"non_empty_prior_basket": "split <> 'prior' OR n_items > 0"}) in fn.expectations
    assert ("fail", BASKET_RULES) in fn.expectations
    # Task views không thuộc graph LDP, và batch không nhận quyền sở hữu target stream.
    assert not any("stream_orders" in target or "stream_order_items" in target for target in baskets_pipeline.targets)


@pytest.mark.parametrize("source,order", [("order_items_prior", 11), ("stream_order_items", 13)])
def test_pipeline_baskets_rejects_header_without_items(baskets_pipeline, source, order):
    key = f"`test_catalog`.`p2_silver`.`{source}`"
    baskets_pipeline.raw[key] = baskets_pipeline.raw[key].where(F.col("order_id") != order)
    if source == "order_items_prior":
        with pytest.raises(ValueError, match="expectation_failed"):
            baskets_pipeline.read("`test_catalog`.`p2_silver`.`baskets`")
    else:
        assert baskets_pipeline.read("`test_catalog`.`p2_silver`.`baskets`").count() == 14
        warn = ("warn", {"holdout_items_present": "split <> 'holdout' OR n_items > 0"})
        assert warn in baskets_pipeline.datasets["basket_candidates"][0].expectations


@pytest.mark.parametrize("pipeline", [False], indirect=True)
def test_pipeline_can_run_without_stream_baskets_enabled(pipeline):
    assert "basket_candidates" not in pipeline.datasets
    assert "`test_catalog`.`p2_silver`.`baskets`" not in pipeline.datasets
