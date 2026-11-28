"""Kiểm đăng ký sink tùy chọn của pipeline Gold."""

import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from common import power_bi_push

PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/stream/gold_stream.py"


@pytest.fixture
def load_pipeline(monkeypatch):
    def load(**overrides):
        spark = Mock()
        spark.conf.get.side_effect = (
            {"catalog": "instacart", "gold": "p3-dev_gold", "ops": "p3-dev_ops"} | overrides
        ).get
        dp = Mock()
        for decorator in (dp.table, dp.foreach_batch_sink, dp.append_flow):
            decorator.return_value = lambda definition: definition
        monkeypatch.setattr(SparkSession, "active", lambda: spark)
        monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
        return runpy.run_path(str(PIPELINE)), spark, dp

    return load


@pytest.mark.parametrize("options", [{}, {"enable_power_bi_push": "false"}])
def test_pipeline_does_not_register_sink_when_disabled(load_pipeline, options):
    _, _, dp = load_pipeline(**options)
    dp.foreach_batch_sink.assert_not_called()
    dp.append_flow.assert_not_called()


def test_sink_reads_new_kpis_and_loads_secret_at_batch_time(load_pipeline, monkeypatch):
    secrets = Mock()
    push = Mock()
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", SimpleNamespace(DBUtils=Mock(return_value=secrets)))
    monkeypatch.setattr(power_bi_push, "push_kpi_rows", push)
    definitions, spark, dp = load_pipeline(enable_power_bi_push="true")
    dp.foreach_batch_sink.assert_called_once_with(name="pbi_push")
    dp.append_flow.assert_called_once_with(target="pbi_push", name="kpi_to_pbi")
    secrets.secrets.get.assert_not_called()

    definitions["kpi_to_pbi"]()

    spark.readStream.option.assert_called_once_with("startingVersion", "latest")
    spark.readStream.option.return_value.table.assert_called_once_with("`instacart`.`p3-dev_gold`.`stream_kpi_1m`")

    batch = Mock()
    iterator = batch.selectExpr.return_value.orderBy.return_value.toLocalIterator.return_value
    definitions["pbi_push"](batch, 3)
    batch.selectExpr.assert_called_once_with("unix_seconds(minute) AS minute_epoch", "orders", "items")
    batch.selectExpr.return_value.orderBy.assert_called_once_with("minute_epoch")
    secrets.secrets.get.assert_called_once_with("kv", "pbi-push-url")
    push.assert_called_once_with(iterator, secrets.secrets.get.return_value)
