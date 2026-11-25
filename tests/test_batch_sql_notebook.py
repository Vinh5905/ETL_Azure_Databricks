"""Kiểm tham số Jobs, thứ tự chạy SQL và task value khi repair batch."""

import runpy
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "src/tasks"
NOTEBOOK = SQL_DIR / "run_batch_sql.py"


def test_batch_job_keeps_repair_reference_without_warehouse_tasks():
    bundle = yaml.safe_load((ROOT / "bundles/resources/jobs_batch.yml").read_text())
    tasks = {task["task_key"]: task for task in bundle["resources"]["jobs"]["batch_medallion"]["tasks"]}
    for name in ("resolve_pipeline_update", "collect_dq", "init_reconciliation", "reconcile"):
        task = tasks[name]
        assert "sql_task" not in task
        assert task["notebook_task"]["base_parameters"]["sql_step"] == name
        assert task["notebook_task"]["notebook_path"].endswith("/run_batch_sql.py")
    resolve = tasks["resolve_pipeline_update"]
    assert resolve["run_if"] == "ALL_DONE"
    assert resolve["notebook_task"]["base_parameters"]["pipeline_task_run_id"] == "{{tasks.pipeline_batch.run_id}}"
    collect = tasks["collect_dq"]["notebook_task"]["base_parameters"]
    assert collect["update_id"] == "{{tasks.resolve_pipeline_update.values.update_id}}"


@pytest.fixture
def notebook(monkeypatch):
    spark = Mock()
    dbutils = Mock()
    monkeypatch.setattr(sys, "path", sys.path[:])

    def run(step, **params):
        dbutils.widgets.get.side_effect = {
            "sql_step": step,
            "sql_dir": str(SQL_DIR),
            "catalog": "instacart",
            "ops_schema": "p2_ops",
            **params,
        }.__getitem__
        runpy.run_path(str(NOTEBOOK), init_globals={"dbutils": dbutils, "spark": spark})

    return run, spark, dbutils


def test_resolver_sets_update_id_for_downstream(notebook):
    run, spark, dbutils = notebook
    spark.sql.return_value.collect.return_value = [{"update_id": "new-pipeline-update"}]
    run("resolve_pipeline_update", pipeline_id="pipeline", pipeline_task_run_id="repair-task-run")
    assert spark.sql.call_args.kwargs["args"]["pipeline_task_run_id"] == "repair-task-run"
    dbutils.jobs.taskValues.set.assert_called_once_with(key="update_id", value="new-pipeline-update")


def test_collector_stops_before_writes_when_assertion_fails(notebook):
    run, spark, dbutils = notebook
    spark.sql.return_value.collect.side_effect = ValueError("bad parameters")
    with pytest.raises(RuntimeError, match="collect_dq.sql, câu 1"):
        run("collect_dq", event_log_table="event_log_batch", update_id="id")
    spark.sql.assert_called_once()
    dbutils.jobs.taskValues.set.assert_not_called()
