from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from common.sim_time import order_time_sim_sql
from streaming.simulator.simulate import build_events, main

FIXTURE = Path(__file__).parent / "fixtures" / "instacart"


def test_build_events_joins_train_orders_and_sorts_ties(tmp_path):
    (tmp_path / "orders.csv").write_text(
        "order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order\n"
        "9,31,train,2,4,8,NaN\n"
        "7,30,train,2,1,0,\n"
        "8,1,train,2,4,8,14.0\n"
        "1,1,prior,1,3,8,\n"
        "10,2,test,2,4,9,1.0\n"
        "12,3,train,2,4,9,1.0\n",
        encoding="utf-8-sig",
    )
    (tmp_path / "order_products__train.csv").write_text(
        "order_id,product_id,add_to_cart_order,reordered\n"
        "8,196,2,1\n"
        "1,1,1,0\n"
        "9,3,1,0\n"
        "7,4,1,1\n"
        "8,5,1,0\n"
        "10,2,1,1\n"
        "99,2,1,1\n",
        encoding="utf-8",
    )

    events = build_events(tmp_path)

    assert [event["event_id"] for event in events] == ["order-7", "order-8", "order-9"]
    assert [event["event_time"] for event in events] == [
        "2026-03-01T00:00:00Z",
        "2026-03-30T08:00:00Z",
        "2026-03-30T08:00:00Z",
    ]
    assert [event["days_since_prior_order"] for event in events] == [None, 14.0, None]
    assert events[1] == {
        "event_id": "order-8",
        "event_type": "order_placed",
        "schema_version": 1,
        "order_id": 8,
        "user_id": 1,
        "order_number": 2,
        "order_dow": 4,
        "order_hour_of_day": 8,
        "days_since_prior_order": 14.0,
        "event_time": "2026-03-30T08:00:00Z",
        "items": [
            {"product_id": 5, "add_to_cart_order": 1, "reordered": 0},
            {"product_id": 196, "add_to_cart_order": 2, "reordered": 1},
        ],
    }


def test_dry_run_splits_adjacent_ranges_without_overlap(tmp_path):
    first = tmp_path / "first.jsonl"
    last = tmp_path / "nested" / "last.jsonl"
    args = ["--data-dir", str(FIXTURE), "--run-id", "run-01", "--location", "laptop-hcm"]
    main([*args, "--to-frac", "0.67", "--dry-run", str(first)])
    main([*args, "--from-frac", "0.67", "--dry-run", str(last)])

    first_events = [json.loads(line) for line in first.read_text(encoding="utf-8").splitlines()]
    last_events = [json.loads(line) for line in last.read_text(encoding="utf-8").splitlines()]
    assert len(first_events) == 2
    assert len(last_events) == 1
    events = first_events + last_events
    assert [event["event_id"] for event in events] == [event["event_id"] for event in build_events(FIXTURE)]
    for event in events:
        assert event["replay_run_id"] == "run-01"
        assert event["producer_location"] == "laptop-hcm"
        assert event["produced_at"].endswith("Z")
        assert datetime.fromisoformat(event["produced_at"]).tzinfo is UTC


@pytest.mark.parametrize(
    "selection", [["--from-frac", "1.1"], ["--to-frac", "NaN"], ["--from-frac", "0.9", "--to-frac", "0.1"]]
)
def test_invalid_fraction_does_not_create_file(tmp_path, selection):
    output = tmp_path / "events.jsonl"
    with pytest.raises(SystemExit) as error:
        main(["--data-dir", str(FIXTURE), "--run-id", "run-01", "--dry-run", str(output), *selection])
    assert error.value.code == 2
    assert not output.exists()


def test_event_time_matches_silver_sql(spark):
    view = "simulator_order_headers"
    orders = spark.read.option("header", True).csv(str(FIXTURE / "orders.csv"))
    orders.selectExpr("*", "current_timestamp() AS _ingest_ts").createOrReplaceTempView(view)
    try:
        silver = (
            spark.sql(order_time_sim_sql(view, canonical=True))
            .where("eval_set = 'train'")
            .selectExpr("order_id", "unix_seconds(order_ts_sim) AS epoch")
        )
        expected = {row.order_id: row.epoch for row in silver.collect()}
    finally:
        spark.catalog.dropTempView(view)

    assert {
        event["order_id"]: int(datetime.fromisoformat(event["event_time"]).timestamp())
        for event in build_events(FIXTURE)
    } == expected
