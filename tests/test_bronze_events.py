"""Kiểm flow Bronze với Kafka và Databricks được mô phỏng."""

import runpy
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/stream/bronze_events.py"
SAS = "Endpoint=sb://orders.example.invalid/;SharedAccessKeyName=test-listen;SharedAccessKey=fake=="
TABLE = "`instacart`.`p3-dev_bronze`.`order_events`"


@pytest.fixture
def pipeline(monkeypatch):
    def load(generation):
        session = Mock()
        session.conf.get.side_effect = {
            "catalog": "instacart",
            "bronze": "p3-dev_bronze",
            "eh_generation": generation,
            "eh_topic": "orders-stream",
        }.__getitem__
        reader = session.readStream.format.return_value
        reader.options.return_value = reader
        dbutils = Mock()
        dbutils.secrets.get.return_value = SAS
        dp = Mock()
        dp.append_flow.return_value = lambda flow: flow
        monkeypatch.setattr(SparkSession, "active", lambda: session)
        monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
        monkeypatch.setitem(sys.modules, "pyspark.dbutils", SimpleNamespace(DBUtils=Mock(return_value=dbutils)))
        flow = runpy.run_path(str(PIPELINE))["kafka_to_bronze"]
        return SimpleNamespace(dp=dp, session=session, reader=reader, dbutils=dbutils, flow=flow)

    return load


@pytest.mark.parametrize("generation", ["g1", "g2"])
def test_flow_name_follows_hub_generation_and_table_disables_reset(pipeline, generation):
    bronze = pipeline(generation)

    bronze.dp.append_flow.assert_called_once_with(target=TABLE, name=f"kafka_{generation}")
    table = bronze.dp.create_streaming_table.call_args.kwargs
    assert table["name"] == TABLE
    assert table["table_properties"] == {"pipelines.reset.allowed": "false", "quality": "bronze"}


@pytest.mark.parametrize("connection", ["invalid-sas", SAS + ";EntityPath=other-hub"])
def test_flow_rejects_invalid_secret_without_echoing_it(pipeline, connection):
    bronze = pipeline("g1")
    bronze.dbutils.secrets.get.return_value = connection

    with pytest.raises(ValueError) as error:
        bronze.flow()

    assert connection not in str(error.value)
    bronze.reader.load.assert_not_called()


def test_flow_reads_listen_secret_and_preserves_kafka_payload(pipeline, spark):
    bronze = pipeline("g2")
    payload = '{"schema_version": 42, "new": "sữa"}'
    enqueued_at = datetime(2026, 11, 22, 1, 2, 3)
    bronze.dbutils.secrets.get.return_value = SAS + ";EntityPath=orders-stream"
    bronze.reader.load.return_value = spark.createDataFrame(
        [(b"123", payload.encode(), 2, 445, enqueued_at)],
        "key BINARY, value BINARY, partition INT, offset LONG, timestamp TIMESTAMP",
    )

    row = bronze.flow().first().asDict()

    bronze.dbutils.secrets.get.assert_called_once_with("kv", "eh-listen-connection-string")
    bronze.session.readStream.format.assert_called_once_with("kafka")
    options = bronze.reader.options.call_args.kwargs
    assert options["kafka.bootstrap.servers"] == "orders.example.invalid:9093"
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.sasl.mechanism"] == "PLAIN"
    assert options["kafka.sasl.jaas.config"].endswith(f'password="{SAS};EntityPath=orders-stream";')
    assert options["subscribe"] == "orders-stream"
    assert options["startingOffsets"] == "earliest"
    assert options["maxOffsetsPerTrigger"] == "20000"
    assert row.pop("_ingest_ts") is not None
    assert row == {
        "event_key": "123",
        "value_json": payload,
        "_kafka_partition": 2,
        "_kafka_offset": 445,
        "_enqueued_ts": enqueued_at,
        "_eh_generation": "g2",
    }
