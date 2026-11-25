from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

from ml import run_experiment
from ml.run_experiment import _calculate, execute_run

TOYS = [([1, 2, 3],), ([1, 2],), ([1, 3],), ([1, 2, 3],), ([2, 3],)]


@pytest.fixture(scope="module")
def toy_df(spark):
    return spark.createDataFrame(TOYS, "items array<int>")


def config(algo: str) -> dict:
    return {
        "run_key": f"toy-{algo}",
        "algo_version": "fim-v1",
        "experiment": "E1",
        "platform": "databricks",
        "algo": algo,
        "grain": "product",
        "scale_pct": 10,
        "top_n": None,
        "min_support": 0.4,
        "min_confidence": 0.5,
        "max_k": 3,
        "cores": 1,
        "rep": 1,
        "data_version": "test",
        "timeout_min": 5,
        "persist": False,
        "sweep": None,
    }


@pytest.mark.parametrize("algo", ["dapriori", "datid", "fpgrowth", "all"])
def test_calculate_one_algorithm_or_all_matches_on_toy_data(spark, toy_df, algo):
    result = _calculate(spark, toy_df, config(algo), len(TOYS))

    assert result["metrics"]["n_itemsets"] == 7
    assert json.loads(result["metrics"]["n_itemsets_by_k"]) == {"1": 3, "2": 3, "3": 1}
    assert result["metrics"]["n_rules_single"] > 0
    if algo == "all":
        assert result["metrics"]["check_passed"] is True


def test_timeout_is_recorded_without_raising(monkeypatch):
    expired = threading.Event()

    class FakeFrame:
        def count(self):
            return 1

    class FakeSpark:
        sparkContext = object()

    @contextmanager
    def fake_timeout(_sc, _minutes, _group):
        yield expired
        expired.set()

    class FakePeak:
        delta_mb = 1.0

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    monkeypatch.setattr(
        "ml.run_experiment._calculate",
        lambda *_args: {"freq": {}, "rules": None, "metrics": {"n_itemsets": 0}},
    )
    row, freq, rules = execute_run(
        FakeSpark(),
        config("dapriori"),
        FakeFrame(),
        timeout_factory=fake_timeout,
        peak_factory=FakePeak,
    )

    assert row["status"] == "timeout"
    assert row["error"] == "Vượt quá timeout 5 phút"
    assert freq is None
    assert rules is None


def _create_ml_tables(spark, database: str) -> None:
    ddl = (Path(__file__).parents[2] / "src/ml/sql/ddl_ml.sql").read_text(encoding="utf-8")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    for statement in ddl.split(";"):
        if any(name in statement for name in ("experiment_runs (", "frequent_itemsets (", "association_rules (")):
            spark.sql(statement.replace("instacart.ml.", f"{database}."))


def test_run_config_merges_full_row_and_persists_idempotently(spark, toy_df, monkeypatch):
    # Chạy hết đường ghi thật (MERGE experiment_runs, replaceWhere itemsets/rules) trên Delta local, chạy 2 lần.
    database = "ml_run_test"
    monkeypatch.setattr(run_experiment, "load_transactions", lambda *_args, **_kwargs: toy_df)
    monkeypatch.setattr(run_experiment, "RUNS", f"{database}.experiment_runs")
    monkeypatch.setattr(run_experiment, "ITEMSETS", f"{database}.frequent_itemsets")
    monkeypatch.setattr(run_experiment, "RULES", f"{database}.association_rules")
    cfg = {**config("datid"), "persist": True}
    spark.catalog.clearCache()  # các test _calculate ở trên để lại cache luật
    try:
        _create_ml_tables(spark, database)
        for _ in range(2):
            run_experiment.run_config(spark, cfg, log_mlflow=False)

        (row,) = spark.table(f"{database}.experiment_runs").collect()
        for column in (
            "run_key", "status", "n_tx", "n_itemsets", "n_itemsets_by_k", "n_rules_single", "n_rules_all",
            "wall_s", "rules_s", "peak_mem_mb", "started_at", "finished_at", "min_support", "timeout_min",
        ):  # fmt: skip
            assert row[column] is not None, column
        assert row.status == "ok"
        assert row.n_tx == len(TOYS)
        assert row.n_itemsets == 7
        assert spark.table(f"{database}.frequent_itemsets").count() == 7
        assert spark.table(f"{database}.association_rules").count() == row.n_rules_all
        # Cache của luật phải được gỡ; chỉ còn cache trạng thái Delta log do Delta tự quản.
        storage = spark.sparkContext._jsc.sc().getRDDStorageInfo()
        assert [info.name() for info in storage if not info.name().startswith("Delta Table State")] == []
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
