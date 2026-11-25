# Databricks notebook source
"""Chạy một bước SQL của job batch trên Spark serverless."""

import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils, spark

PARAMETERS = {
    "resolve_pipeline_update": ("pipeline_id", "pipeline_task_run_id"),
    "collect_dq": ("event_log_table", "update_id"),
    "init_reconciliation": (),
    "reconcile": ("bronze_schema", "silver_schema", "gold_schema", "profile", "run_id", "check_date"),
}

task = dbutils.widgets.get("sql_step")
if task not in PARAMETERS:
    raise ValueError(f"Bước SQL batch không hợp lệ: {task}")

sql_dir = Path(dbutils.widgets.get("sql_dir"))
if not sql_dir.is_absolute():
    raise ValueError("sql_dir phải là đường dẫn tuyệt đối do bundle cấp")
sys.path.insert(0, str(sql_dir))
from ds_prepare.run_sql import split_sql  # noqa: E402

args = {name: dbutils.widgets.get(name) for name in ("catalog", "ops_schema", *PARAMETERS[task])}
script = sql_dir / f"{task}.sql"
statements = split_sql(script.read_text(encoding="utf-8"))
if not statements:
    raise ValueError(f"File không có câu SQL: {script.name}")

# Collect từng câu để các SELECT assert_true phải chạy trước câu tiếp theo.
for index, statement in enumerate(statements, start=1):
    try:
        rows = spark.sql(statement, args=args).collect()
    except Exception as exc:
        raise RuntimeError(f"{script.name}, câu {index}: {exc}") from exc

if task == "resolve_pipeline_update":
    # Collector cần update_id mới khi pipeline task được repair.
    dbutils.jobs.taskValues.set(key="update_id", value=rows[0]["update_id"])
