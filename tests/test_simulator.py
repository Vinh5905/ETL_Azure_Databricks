from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from common.sim_time import order_time_sim_sql
from streaming.simulator import kafka_producer, simulate
from streaming.simulator.simulate import build_events, main

FIXTURE = Path(__file__).parent / "fixtures" / "instacart"
SAS_TEST = "Endpoint=sb://unit-test.servicebus.windows.net/;SharedAccessKeyName=sim-send;SharedAccessKey=test-only=="


class FakeKafkaException(Exception):
    pass


@pytest.fixture
def kafka_client(monkeypatch):
    producer = Mock()
    producer.flush.return_value = 0
    factory = Mock(return_value=producer)
    monkeypatch.setitem(
        sys.modules, "confluent_kafka", SimpleNamespace(Producer=factory, KafkaException=FakeKafkaException)
    )
    monkeypatch.setenv("EH_SEND_CONN", SAS_TEST)
    monkeypatch.setattr(kafka_producer, "sleep", lambda _: None)
    return factory, producer


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


def test_kafka_caps_replay_at_1000_and_counts_broker_acks(monkeypatch, capsys, kafka_client):
    factory, producer = kafka_client
    template = build_events(FIXTURE)[0]
    events = [{**template, "order_id": order_id, "event_id": f"order-{order_id}"} for order_id in range(1, 1003)]
    monkeypatch.setattr(simulate, "build_events", lambda _: events)

    def acknowledge(_timeout):
        for call in producer.produce.call_args_list:
            call.kwargs["on_delivery"](None, None)
        return 0

    producer.flush.side_effect = acknowledge
    main(["--data-dir", str(FIXTURE), "--run-id", "smoke-01", "--location", "codespace", "--limit", "1000", "--send"])

    config = factory.call_args.args[0]
    assert config["bootstrap.servers"] == "unit-test.servicebus.windows.net:9093"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "$ConnectionString"
    assert config["sasl.password"] == SAS_TEST
    assert config["enable.idempotence"] is True
    assert config["compression.type"] == "none"
    assert [call.kwargs["key"] for call in producer.produce.call_args_list] == list(map(str, range(1, 1001)))
    for call in producer.produce.call_args_list:
        assert call.args == ("orders-stream",)
        payload = json.loads(call.kwargs["value"])
        assert payload["order_id"] == int(call.kwargs["key"])
        assert payload["replay_run_id"] == "smoke-01"
        assert payload["producer_location"] == "codespace"
        assert datetime.fromisoformat(payload["produced_at"]).tzinfo is UTC
    captured = capsys.readouterr()
    assert captured.out == "events_sent=1000 total_events=1002\n"
    assert not captured.err


def test_kafka_retries_full_queue_and_allows_gzip(kafka_client):
    factory, producer = kafka_client
    producer.produce.side_effect = [BufferError(), None]

    def poll(_timeout):
        producer.produce.call_args.kwargs["on_delivery"](None, None)

    producer.poll.side_effect = poll
    assert kafka_producer.publish_events([("7", "{}")], rate=200, compression="gzip") == 1
    assert factory.call_args.args[0]["compression.type"] == "gzip"
    assert producer.produce.call_count == 2
    assert producer.flush.call_count == 2


@pytest.mark.parametrize("pending", [0, 1])
def test_kafka_does_not_report_success_for_failed_or_pending_delivery(capsys, kafka_client, pending):
    _, producer = kafka_client
    producer.flush.return_value = pending
    failure = SimpleNamespace(code=lambda: 10)
    if not pending:
        producer.poll.side_effect = lambda _: producer.produce.call_args.kwargs["on_delivery"](failure, None)

    with pytest.raises(SystemExit) as error:
        main(["--data-dir", str(FIXTURE), "--run-id", "smoke-02", "--limit", "1", "--send"])

    assert error.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert "Broker xác nhận 0 sự kiện" in captured.err
    assert f"{pending} chưa được xác nhận" in captured.err
    assert SAS_TEST not in captured.err


@pytest.mark.parametrize("connection", [None, "malformed-test-only", SAS_TEST + ";EntityPath=another-hub"])
def test_kafka_rejects_bad_connection_without_logging_it(monkeypatch, capsys, connection):
    monkeypatch.delenv("EH_SEND_CONN", raising=False)
    if connection is not None:
        monkeypatch.setenv("EH_SEND_CONN", connection)

    with pytest.raises(SystemExit) as error:
        main(["--data-dir", str(FIXTURE), "--run-id", "smoke-03", "--send"])

    assert error.value.code == 1
    captured = capsys.readouterr()
    assert "EH_SEND_CONN" in captured.err
    assert "test-only" not in captured.err


def test_kafka_suppresses_credentials_from_native_errors(kafka_client, capsys):
    factory, _ = kafka_client
    factory.side_effect = FakeKafkaException(SAS_TEST)

    with pytest.raises(SystemExit) as error:
        main(["--data-dir", str(FIXTURE), "--run-id", "smoke-04", "--send"])

    assert error.value.code == 1
    captured = capsys.readouterr()
    assert "Kafka producer gặp lỗi" in captured.err
    assert "test-only" not in captured.err


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
