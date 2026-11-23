"""Kiểm KPI, cửa sổ chốt theo watermark và độ trễ của Gold stream."""

import json
import runpy
import sys
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/stream/gold_stream.py"
VALID_SCHEMA = (
    "event_id STRING, replay_run_id STRING, user_id INT, event_time TIMESTAMP, "
    "_enqueued_ts TIMESTAMP, items ARRAY<STRUCT<product_id: INT>>"
)
PARSED_SCHEMA = (
    "event_id STRING, replay_run_id STRING, producer_location STRING, schema_version INT, "
    "produced_at TIMESTAMP, _enqueued_ts TIMESTAMP, _ingest_ts TIMESTAMP, _silver_ts TIMESTAMP"
)


@pytest.fixture
def pipeline(monkeypatch):
    session = Mock()
    config = {
        "catalog": "instacart",
        "gold": "dev-stream_gold",
        "ops": "dev-stream_ops",
        "eh_region": "koreacentral",
        "dbx_region": "eastasia",
    }
    session.conf.get.side_effect = config.get
    tables = {}
    session.readStream.table.side_effect = tables.__getitem__
    dp = Mock()
    dp.table.return_value = lambda definition: definition
    with monkeypatch.context() as patch:
        patch.setattr(SparkSession, "active", lambda: session)
        patch.setitem(sys.modules, "pyspark.pipelines", dp)
        definitions = runpy.run_path(str(PIPELINE))
    return definitions, tables, dp


def event(order, enqueued, products, *, run="first", user=17):
    return {
        "event_id": f"order-{order}",
        "replay_run_id": run,
        "user_id": user,
        "event_time": "2026-03-01T08:05:00",
        "_enqueued_ts": f"2026-11-22T{enqueued}",
        "items": [{"product_id": product} for product in products],
    }


def test_kpi_and_pairs_skip_same_run_duplicates_and_close_by_arrival_time(pipeline, spark, tmp_path):
    definitions, tables, dp = pipeline
    input_dir = tmp_path / "events"
    input_dir.mkdir()
    tables["events_valid"] = spark.readStream.schema(VALID_SCHEMA).json(str(input_dir))

    def feed(batch, events):
        path = tmp_path / f"batch-{batch}.json"
        path.write_text("\n".join(map(json.dumps, events)), encoding="utf-8")
        path.rename(input_dir / path.name)
        for query in queries:
            query.processAllAvailable()

    with ExitStack() as stack:
        queries = []
        for table in ("stream_kpi_1m", "stream_pair_counts_10m"):
            query = definitions[table]().writeStream.format("memory").queryName(table).outputMode("append").start()
            stack.callback(spark.catalog.dropTempView, table)
            stack.callback(query.stop)
            queries.append(query)

        feed(
            0,
            [
                event(1, "12:00:10", [7, 2, 5]),
                event(1, "12:00:15", [7, 2, 5]),
                event(2, "12:00:20", [2, 7]),
                event(1, "12:00:30", [7, 2, 5], run="replay"),
                event(3, "12:00:45", [2], user=18),
            ],
        )
        assert spark.table("stream_kpi_1m").count() == 0
        assert spark.table("stream_pair_counts_10m").count() == 0

        feed(1, [event(4, "12:11:59", [2])])
        [kpi] = spark.table("stream_kpi_1m").selectExpr("CAST(minute AS STRING)", "orders", "items", "users").collect()
        assert tuple(kpi) == ("2026-11-22 12:00:00", 4, 9, 2)
        assert spark.table("stream_pair_counts_10m").count() == 0

        # Đơn chỉ có một món vẫn đẩy watermark. Bản tin quá trễ không đổi cửa sổ cũ.
        feed(2, [event(5, "12:12:01", [2]), event(6, "12:00:50", [2, 7])])
        pairs = spark.table("stream_pair_counts_10m").selectExpr(
            "CAST(window_start AS STRING)", "CAST(window_end AS STRING)", "p1", "p2", "pair_count"
        )
        window = ("2026-11-22 12:00:00", "2026-11-22 12:10:00")
        assert [tuple(row) for row in pairs.orderBy("p1", "p2").collect()] == [
            (*window, 2, 5, 2),
            (*window, 2, 7, 3),
            (*window, 5, 7, 2),
        ]

    assert [call.kwargs["name"] for call in dp.table.call_args_list] == [
        "`instacart`.`dev-stream_gold`.`stream_kpi_1m`",
        "`instacart`.`dev-stream_gold`.`stream_pair_counts_10m`",
        "`instacart`.`dev-stream_ops`.`streaming_latency`",
    ]


def test_latency_keeps_parsed_repeats_and_labels_regions(pipeline, spark):
    definitions, tables, _ = pipeline
    produced = datetime(2026, 11, 22, 12)
    enqueued = produced + timedelta(microseconds=12345)
    ingested = enqueued + timedelta(milliseconds=80)
    silver = produced + timedelta(milliseconds=150)
    tables["events_parsed"] = spark.createDataFrame(
        [
            ("order-1", "first", "laptop", 1, produced, enqueued, ingested, silver),
            ("order-1", "first", "laptop", 1, produced, enqueued, ingested, silver),
            ("order-1", "replay", "laptop", 2, None, enqueued, ingested, silver),
            (None, None, None, None, None, enqueued, ingested, silver),
        ],
        PARSED_SCHEMA,
    )
    rows = definitions["streaming_latency"]().collect()
    assert len(rows) == 3
    assert {(row.eh_region, row.dbx_region, row.producer_location) for row in rows} == {
        ("koreacentral", "eastasia", "laptop")
    }
    assert [row.ms_producer_to_eh for row in rows if row.replay_run_id == "first"] == [12.345, 12.345]
    assert [row.ms_end_to_end for row in rows if row.replay_run_id == "first"] == [150, 150]
    assert {row.ms_eh_to_bronze for row in rows} == {80}
    [replay] = [row for row in rows if row.replay_run_id == "replay"]
    assert replay.ms_producer_to_eh is None and replay.ms_end_to_end is None
