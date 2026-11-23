"""Kiểm schema, quarantine và hợp đồng AUTO CDC của Silver stream."""

import json
import runpy
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from common.dq import EVENT_RULES

PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/stream/silver_events.py"
BRONZE_TABLE = "`instacart`.`dev-stream_bronze`.`order_events`"
ENQUEUED = datetime(2026, 11, 22, 12)
BRONZE_SCHEMA = (
    "value_json STRING, _enqueued_ts TIMESTAMP, _ingest_ts TIMESTAMP, "
    "_kafka_partition INT, _kafka_offset LONG, _eh_generation STRING"
)
VALID_EVENT = {
    "event_id": "order-123",
    "event_type": "order_placed",
    "schema_version": 1,
    "order_id": 123,
    "user_id": 17,
    "order_number": 1,
    "order_dow": 0,
    "order_hour_of_day": 12,
    "days_since_prior_order": None,
    "event_time": "2026-03-04T12:00:00Z",
    "replay_run_id": "run-01",
    "produced_at": "2026-11-22T05:00:00Z",
    "producer_location": "laptop",
    "items": [{"product_id": 31, "add_to_cart_order": 1, "reordered": 0}],
}


@pytest.fixture
def pipeline(monkeypatch, spark):
    session = Mock()
    schemas = {"catalog": "instacart", "bronze": "dev-stream_bronze", "silver": "dev-stream_silver"}
    session.conf.get.side_effect = schemas.__getitem__
    tables = {}
    session.readStream.table.side_effect = tables.__getitem__
    dp = Mock()
    dp.temporary_view.return_value = lambda view: view
    dp.table.return_value = lambda view: view

    def expect_all_or_drop(rules):
        def decorate(view):
            return lambda: view().where(" AND ".join(f"({condition})" for condition in rules.values()))

        return decorate

    dp.expect_all_or_drop.side_effect = expect_all_or_drop
    monkeypatch.setattr(SparkSession, "active", lambda: session)
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
    return runpy.run_path(str(PIPELINE)), tables, dp


def test_parse_routes_malformed_version_and_unusable_items_to_quarantine(pipeline, spark):
    silver, tables, dp = pipeline
    good = json.dumps(VALID_EVENT)
    malformed = good[:-1]
    future_version = json.dumps({**VALID_EVENT, "schema_version": 2})
    missing_product_id = json.dumps({**VALID_EVENT, "items": [{"add_to_cart_order": 1}]})
    duplicate_product_id = json.dumps({**VALID_EVENT, "items": VALID_EVENT["items"] * 2})
    tables[BRONZE_TABLE] = spark.createDataFrame(
        [
            (payload, ENQUEUED, ENQUEUED, 0, offset, "g2")
            for offset, payload in enumerate(
                (good, malformed, future_version, missing_product_id, duplicate_product_id)
            )
        ],
        BRONZE_SCHEMA,
    )
    tables["events_parsed"] = silver["events_parsed"]()

    [row] = silver["events_valid"]().collect()
    assert "value_json" not in row.asDict()
    assert (row.order_id, row.replay_run_id, row.days_since_prior_order) == (123, "run-01", None)
    assert row.event_time is not None and row.produced_at is not None
    assert row.items[0].product_id == 31
    assert row._silver_ts is not None

    quarantine = {row._kafka_offset: row for row in silver["quarantine_order_events"]().collect()}
    assert set(quarantine) == {1, 2, 3, 4}
    assert quarantine[1].value_json == malformed and "parsed" in quarantine[1]._dq_failed
    assert quarantine[2].value_json == future_version and quarantine[2]._dq_failed == ["schema_v1"]
    assert quarantine[3]._dq_failed == quarantine[4]._dq_failed == ["valid_item_keys"]
    assert quarantine[1]._eh_generation == "g2"
    assert quarantine[1]._quarantined_at is not None
    dp.expect_all_or_drop.assert_called_once_with(EVENT_RULES)


def test_explodes_each_item_and_registers_scd1_with_kafka_sequence(pipeline, spark):
    silver, tables, dp = pipeline
    other_item = {"product_id": 32, "add_to_cart_order": 2, "reordered": 1}
    payload = json.dumps({**VALID_EVENT, "items": [*VALID_EVENT["items"], other_item]})
    tables[BRONZE_TABLE] = spark.createDataFrame([(payload, ENQUEUED, ENQUEUED, 1, 45, "g2")], BRONZE_SCHEMA)
    tables["events_parsed"] = silver["events_parsed"]()
    tables["events_valid"] = silver["events_valid"]()

    rows = silver["event_items"]().orderBy("add_to_cart_order").collect()
    assert [(row.order_id, row.product_id, row.add_to_cart_order, row.reordered) for row in rows] == [
        (123, 31, 1, 0),
        (123, 32, 2, 1),
    ]

    orders, items = [call.kwargs for call in dp.create_auto_cdc_flow.call_args_list]
    assert orders["target"] == "`instacart`.`dev-stream_silver`.`stream_orders`"
    assert items["target"] == "`instacart`.`dev-stream_silver`.`stream_order_items`"
    assert orders["source"] == "events_valid" and orders["keys"] == ["order_id"]
    assert items["source"] == "event_items" and items["keys"] == ["order_id", "product_id"]
    assert "replay_run_id" not in orders["except_column_list"]
    for flow in orders, items:
        assert flow["stored_as_scd_type"] == 1
        sequence = tables["events_valid"].select(flow["sequence_by"].alias("sequence")).first().sequence
        assert tuple(sequence.asDict()) == ("_enqueued_ts", "_kafka_partition", "_kafka_offset")
        assert tuple(sequence) == (ENQUEUED, 1, 45)
