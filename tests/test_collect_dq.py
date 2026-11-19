"""Chạy nguyên collect_dq.sql bằng Spark + Delta local, bao gồm MERGE/transaction thật."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from pathlib import Path

import pytest
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "src/tasks/collect_dq.sql").read_text()
EVENT_SCHEMA = "id STRING, timestamp TIMESTAMP, origin STRING, details STRING, event_type STRING, maturity_level STRING"
ORIGIN_SCHEMA = "pipeline_id STRING,pipeline_name STRING,update_id STRING,flow_name STRING,dataset_name STRING"
DETAIL_SCHEMA = (
    "flow_progress STRUCT<status:STRING,data_quality:STRUCT<expectations:"
    "ARRAY<STRUCT<name:STRING,dataset:STRING,passed_records:BIGINT,failed_records:BIGINT>>>>"
)


def metric(name="has_keys", dataset="silver.header", passed=10, failed=1):
    return {"name": name, "dataset": dataset, "passed_records": passed, "failed_records": failed}


def event(
    eid,
    metrics=None,
    *,
    pipeline="pipe-batch",
    pipeline_name="display-name",
    update="u-1",
    flow="flow-1",
    day=1,
    status="RUNNING",
    maturity="STABLE",
    event_type="flow_progress",
):
    origin = {
        "pipeline_id": pipeline,
        "pipeline_name": pipeline_name,
        "update_id": update,
        "flow_name": flow,
        "dataset_name": "silver.header",
    }
    details = {"flow_progress": {"status": status}}
    if metrics is not None:
        details["flow_progress"]["data_quality"] = {"expectations": metrics}
    return (eid, dt.datetime(2020, 1, day), json.dumps(origin), json.dumps(details), event_type, maturity)


@pytest.fixture
def log(spark):
    schema = "dq_" + uuid.uuid4().hex
    previous = spark.catalog.currentDatabase()
    spark.sql(f"CREATE DATABASE `{schema}`")
    rows = [
        event(
            "a1",
            [metric(), metric(dataset="silver.canonical", passed=8, failed=2), metric("positive", passed=9, failed=2)],
        ),
        event("a2", [metric(passed=5, failed=2), metric("empty", passed=0, failed=0)], day=2),
        event("a3", status="COMPLETED"),
        event("a4", [metric("has_price", passed=3)], flow="flow-2", status="COMPLETED"),
        event("b1", [metric(passed=7, failed=0)], pipeline="pipe-other"),
        event("fallback", [metric(dataset=None, passed=4, failed=0)], pipeline=None, flow="fallback-flow"),
        event("old", [metric(passed=1000)], update="different-update"),
        event("evolving", [metric(passed=1000)], maturity="EVOLVING"),
        event("ignored", [metric(passed=1000)], event_type="create_update"),
    ]
    rows.append(rows[0])
    frame = spark.createDataFrame(rows, EVENT_SCHEMA)
    frame.write.format("delta").saveAsTable(f"`{schema}`.event_log_batch")
    try:
        yield schema
    finally:
        spark.catalog.setCurrentDatabase(previous)
        spark.sql(f"DROP DATABASE `{schema}` CASCADE")


def collect(spark, schema, source="event_log_batch", update="u-1"):
    args = {"catalog": "spark_catalog", "ops_schema": schema, "event_log_table": source, "update_id": update}
    statements = [statement.strip() for statement in SQL.split(";") if statement.strip()]
    assert len(statements) == 5
    for statement in statements:
        spark.sql(statement, args=args).collect()
    return spark.table(f"`{schema}`.dq_results")


def results(frame):
    return {(r.pipeline, r.flow, r.dataset, r.expectation): (r.passed, r.failed, r.at) for r in frame.collect()}


def test_real_delta_merge_twice_keeps_event_totals_and_dataset_pipeline_identity(spark, log):
    first = results(collect(spark, log))
    assert results(collect(spark, log)) == first
    assert len(first) == 7
    assert collect(spark, log).where("pipeline = 'pipe-batch'").first().pipeline_name == "display-name"
    assert first[("pipe-batch", "flow-1", "silver.header", "has_keys")][:2] == (15, 3)
    assert first[("pipe-batch", "flow-1", "silver.canonical", "has_keys")][:2] == (8, 2)
    assert first[("pipe-other", "flow-1", "silver.header", "has_keys")][:2] == (7, 0)
    assert first[("display-name", "fallback-flow", "silver.header", "has_keys")][:2] == (4, 0)
    assert first[("pipe-batch", "flow-1", "silver.header", "empty")][:2] == (0, 0)
    # Dữ liệu 2020 vẫn được thu, không cắt cửa sổ thời gian khi chỉ định update_id.
    assert first[("pipe-batch", "flow-1", "silver.header", "has_keys")][2] == dt.datetime(2020, 1, 2)


def test_late_metric_increases_totals_without_new_target_key(spark, log):
    before = results(collect(spark, log))
    spark.createDataFrame(
        [event("late", [metric(passed=2)], day=3, pipeline_name="renamed-pipeline")], EVENT_SCHEMA
    ).write.format("delta").mode("append").saveAsTable(f"`{log}`.event_log_batch")
    after = results(collect(spark, log))
    assert len(after) == len(before)
    assert after[("pipe-batch", "flow-1", "silver.header", "has_keys")][:2] == (17, 4)
    target = spark.table(f"`{log}`.dq_results")
    assert (
        target.where("dataset = 'silver.header' AND pipeline = 'pipe-batch' AND expectation = 'has_keys'")
        .first()
        .pipeline_name
        == "renamed-pipeline"
    )


def test_truncated_log_cannot_lower_previously_collected_counts(spark, log):
    before = results(collect(spark, log))
    spark.sql(f"DELETE FROM `{log}`.event_log_batch WHERE id = 'a1'")
    with pytest.raises(Exception, match="source totals regressed"):
        collect(spark, log)
    assert results(spark.table(f"`{log}`.dq_results")) == before


def test_same_sql_reads_stream_struct_origin_and_details_without_requiring_batch_union(spark, log):
    before = results(collect(spark, log))
    stream = spark.createDataFrame([event("s1", [metric()], pipeline="pipe-stream", maturity=None)], EVENT_SCHEMA)
    stream = stream.withColumn("origin", F.from_json("origin", ORIGIN_SCHEMA)).withColumn(
        "details", F.from_json("details", DETAIL_SCHEMA)
    )
    stream.write.format("delta").saveAsTable(f"`{log}`.event_log_stream")
    after = results(collect(spark, log, "event_log_stream"))
    assert len(after) == len(before) + 1
    assert after[("pipe-stream", "flow-1", "silver.header", "has_keys")][:2] == (10, 1)
    assert all(after[key] == value for key, value in before.items())


@pytest.mark.parametrize("bad", [metric(passed=-1), metric(failed=None), metric(name=""), metric(passed="bad")])
def test_bad_metrics_fail_merge_atomically_and_keep_existing_rows(spark, log, bad):
    before = results(collect(spark, log))
    spark.createDataFrame([event("bad", [bad], update="bad-update")], EVENT_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(f"`{log}`.event_log_batch")
    with pytest.raises(Exception, match="collect_dq|MALFORMED|malformed"):
        collect(spark, log, update="bad-update")
    assert results(spark.table(f"`{log}`.dq_results")) == before


def test_conflicting_event_id_fails_instead_of_double_counting(spark, log):
    before = results(collect(spark, log))
    spark.createDataFrame([event("a1", [metric(passed=999)])], EVENT_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(f"`{log}`.event_log_batch")
    with pytest.raises(Exception, match="conflicting event id"):
        collect(spark, log)
    assert results(spark.table(f"`{log}`.dq_results")) == before


def test_update_without_metrics_does_not_fabricate_zero_rows(spark, log):
    assert collect(spark, log, update="absent-update").count() == 0


def test_unknown_log_parameter_is_rejected(spark, log):
    with pytest.raises(Exception, match="collect_dq requires"):
        collect(spark, log, source="unknown")
